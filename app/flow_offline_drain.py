"""Offline-only maintenance using the existing Validation range-proof ledger."""
import hashlib
import json
import math
import time

from app.flow_providers import provider
from app.flow_shadow import ShadowReconciler, ShadowRpc, SHADOW_SPAN
from app.flow_worker import RECOVERY_MAX_BLOCKS, FlowBudget


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


class OfflineAbort(RuntimeError):
    """Do not let the reconciler treat concurrency loss as a retryable RPC error."""


class GuardedConnection:
    """Check external service state before statements and before transaction commit."""
    def __init__(self, conn, guard):
        self.raw, self.guard = conn, guard

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def execute(self, *args):
        self.guard()
        return self.raw.execute(*args)

    def executescript(self, *args):
        self.guard()
        return self.raw.executescript(*args)

    def commit(self):
        try:
            self.guard()
        except BaseException:
            self.raw.rollback()
            raise
        return self.raw.commit()

    def __enter__(self):
        self.guard()
        return self

    def __exit__(self, typ, value, tb):
        if typ is None:
            try:
                self.guard()
            except BaseException:
                self.raw.rollback()
                raise
        return self.raw.__exit__(typ, value, tb)


class OfflineRpc(ShadowRpc):
    async def request(self, payload, method):
        members = payload if isinstance(payload, list) else [payload]
        if any(x['method'] not in ('eth_chainId', 'eth_blockNumber',
                                  'eth_getBlockByNumber', 'eth_getLogs') for x in members):
            raise OfflineAbort('Maintenance RPC method forbidden')
        self.guard()
        result = await super().request(payload, method)
        self.guard()
        return result

    async def _send(self, payload, method):
        # The pacing wait and retry backoff occur before this final guard.
        self.guard()
        if provider(self.config.rpc_http) != 'validation' or self.config.fallback_http:
            raise OfflineAbort('Maintenance HTTP routing changed')
        return await super()._send(payload, method)


class OfflineDrain(ShadowReconciler):
    def __init__(self, worker, guard, generation, revision, source_pid):
        if not worker.settings.split_enabled or provider(worker.providers.http) != 'validation':
            raise OfflineAbort('Offline drain requires split Validation routing')
        self.worker, self.db, self.session_id = worker, worker.db, None
        self.guard, self.generation = guard, generation
        self.revision, self.source_pid = revision, source_pid
        self.prefix = 'offline_drain:' + generation
        self.raw = self.db.conn
        self.db.conn = GuardedConnection(self.raw, self.check_offline)
        self.worker.rpc = OfflineRpc(worker.config, worker.settings, self.db, worker.providers)
        self.worker.rpc.guard = self.check_offline
        required = {'flow_shadow_meta', 'flow_shadow_jobs', 'flow_shadow_ranges', 'flow_bootstrap_identity'}
        tables = {r[0] for r in self.db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not required.issubset(tables):
            raise OfflineAbort('Existing proof schema required; no implicit migration')

    def check_offline(self):
        self.guard()
        rpc = self.worker.rpc
        if getattr(rpc, "job", None) or getattr(self, "promoting_launch", None):
            launch = rpc.job[1] if rpc.job else self.promoting_launch
            target = self.raw.execute("SELECT * FROM flow_tracking_targets WHERE launch_id=?", (launch,)).fetchone()
            if target is None or not self.live(target):
                raise OfflineAbort("Maintenance target expired; rerun to reassess")

    def skip_target(self, target):
        return not self.live(target)

    def complete(self, stage):
        return not self.db.conn.execute("""SELECT 1 FROM flow_shadow_jobs j JOIN flow_tracking_targets t USING(launch_id)
            WHERE j.stage=? AND j.completion_status!='complete'
              AND t.status NOT IN ('completed','partial') AND t.tracking_end_at>? LIMIT 1""",
            (stage, time.time())).fetchone()

    def live(self, target):
        return target['status'] not in ('completed', 'partial') and target['tracking_end_at'] > time.time()

    def snapshot(self):
        # One consistent generation snapshot, including all obligations and identities.
        with self.db.conn:
            self.db.conn.execute('BEGIN')
            targets = [dict(r) for r in self.db.conn.execute(
                "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') ORDER BY launch_id")
                if self.live(r)]
            rows = []
            for target in targets:
                filters = []
                for kind, query, base, end in self.periods(target, 2**63-1):
                    bootstrap = self.db.conn.execute('SELECT * FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                                    (target['launch_id'], kind)).fetchone()
                    cursor = self.db.state(f'recovery:{target["launch_id"]}:{kind}')
                    filters.append({'kind': kind, 'query': query, 'safe_start': base, 'end': end,
                                    'cursor': int(cursor) if cursor is not None else None,
                                    'bootstrap': dict(bootstrap) if bootstrap else None})
                gaps = [dict(r) for r in self.db.conn.execute(
                    'SELECT * FROM flow_gaps WHERE launch_id=? AND resolved=0 ORDER BY id', (target['launch_id'],))]
                safe = {key: target[key] for key in ('launch_id', 'token_address', 'curve_address',
                    'launch_block', 'launch_log_index', 'current_phase', 'tracking_start_at', 'tracking_end_at', 'status')}
                safe['graduation_digest'] = hashlib.sha256((target['graduation_json'] or '').encode()).hexdigest()
                for gap in gaps:
                    gap['reason_digest'] = hashlib.sha256(gap['reason'].encode()).hexdigest()
                    if '://' in gap['reason']:gap['reason'] = 'redacted_unknown_obligation'
                rows.append({'target': safe, 'filters': filters, 'gaps': gaps})
            return rows

    def manifest(self):
        value = self.meta(self.prefix + ':manifest')
        return json.loads(value) if value else None

    def save_manifest(self, value):
        self.set_meta(self.prefix + ':manifest', canonical(value))

    def check_snapshot(self, manifest):
        saved = {r['target']['launch_id']: r['target'] for r in manifest['snapshot']}
        fields = ('token_address', 'curve_address', 'launch_block', 'launch_log_index',
                  'current_phase', 'tracking_end_at', 'status')
        current = [dict(r) for r in self.db.conn.execute(
            "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')")]
        for target in current:
            old = saved.get(target['launch_id'])
            if self.live(target) and old is None:
                raise OfflineAbort('Maintenance target set changed')
            if old and (any(target[k] != old[k] for k in fields) or
                old['graduation_digest'] != hashlib.sha256((target['graduation_json'] or '').encode()).hexdigest()):
                raise OfflineAbort('Maintenance target identity changed')

    def semantics(self, launch):
        target = self.db.target(launch)
        return canonical({'launch_block': target['launch_block'], 'launch_log_index': target['launch_log_index'],
                          'graduation_digest': hashlib.sha256((target['graduation_json'] or '').encode()).hexdigest()})

    def reusable(self, launch, kind, query, first, last):
        # Identity-free recovery/switch ranges and WSS cursors are not reusable proof.
        identity = canonical(query)
        return [dict(r) for r in self.db.conn.execute("""SELECT r.* FROM flow_shadow_ranges r
            JOIN flow_bootstrap_identity i USING(stage,launch_id,kind)
            JOIN flow_shadow_jobs j USING(stage,launch_id,kind)
            JOIN flow_shadow_meta m ON m.key=r.stage||':semantics:'||r.launch_id||':'||r.kind
            WHERE m.value=? AND r.launch_id=? AND r.kind=? AND i.query_json=?
              AND r.first_block<=? AND r.last_block>=?
              AND r.last_block<=j.highest_contiguous_verified_block ORDER BY r.first_block,r.last_block""",
            (self.semantics(launch), launch, kind, identity, last, first))]

    def start_block(self, row, filt):
        base, cursor = filt['safe_start'], self.db.state(
            f'recovery:{row["target"]["launch_id"]}:{filt["kind"]}')
        state = self.db.conn.execute('SELECT status FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                    (row['target']['launch_id'], filt['kind'])).fetchone()
        if cursor is None or not state or state['status'] != 'complete':
            return base
        first = max(base, int(cursor)+1)
        gaps = self.db.conn.execute('SELECT first_block FROM flow_gaps WHERE launch_id=? AND resolved=0',
                                    (row['target']['launch_id'],))
        for gap in gaps:
            if gap['first_block'] is not None:
                first = min(first, max(base, gap['first_block']))
        return first

    def needs_work(self, launch, filt, head):
        cursor = self.db.state(f'recovery:{launch}:{filt["kind"]}')
        state = self.db.conn.execute('SELECT status FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                    (launch, filt['kind'])).fetchone()
        gaps = self.db.conn.execute('SELECT 1 FROM flow_gaps WHERE launch_id=? AND resolved=0 LIMIT 1', (launch,)).fetchone()
        end = min(head, filt['end'])
        return (cursor is None or state is None or state['status'] != 'complete' or bool(gaps) or
                end-max(filt['safe_start'], int(cursor)-2)+1 > RECOVERY_MAX_BLOCKS)

    def plan(self, manifest, head, head_at, stage):
        self.check_snapshot(manifest)
        with self.db.conn:
            for row in manifest['snapshot']:
                target = self.db.target(row['target']['launch_id'])
                if not self.live(target):
                    continue
                for filt in row['filters']:
                    launch, kind = target['launch_id'], filt['kind']
                    if not self.needs_work(launch, filt, head):
                        continue
                    first, last = self.start_block(row, filt), min(head, filt['end'])
                    if first > last:
                        continue
                    key = (stage, launch, kind)
                    if self.db.conn.execute('SELECT 1 FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?', key).fetchone():
                        continue
                    self.db.conn.execute("""INSERT INTO flow_shadow_jobs
                        (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
                         next_unverified_block,highest_contiguous_verified_block,span)
                        VALUES(?,?,?,?,?,?,?,?)""", (*key, first, last, first, first-1, SHADOW_SPAN))
                    self.db.conn.execute('INSERT INTO flow_bootstrap_identity VALUES(?,?,?,?,?)',
                                         (*key, canonical(filt['query']), head_at))
                    self.db.conn.execute('INSERT INTO flow_shadow_meta VALUES(?,?)',
                        (stage+f':semantics:{launch}:{kind}', self.semantics(launch)))
                    covered = first-1
                    for proof in self.reusable(launch, kind, filt['query'], first, last):
                        if proof['first_block'] > covered+1:
                            break
                        end = min(last, proof['last_block'])
                        if end <= covered:
                            continue
                        self.db.conn.execute('INSERT OR IGNORE INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)',
                                             (*key, covered+1, end, int(end == last)))
                        manifest['reused'].append({'stage': stage, 'launch_id': launch, 'kind': kind,
                                                   'from': covered+1, 'to': end, 'source_stage': proof['stage']})
                        covered = end
                    self.db.conn.execute("""UPDATE flow_shadow_jobs SET next_unverified_block=?,
                        highest_contiguous_verified_block=?,completion_status=?
                        WHERE stage=? AND launch_id=? AND kind=?""",
                        (covered+1, covered, 'complete' if covered == last else 'pending', *key))
            self.save_manifest(manifest)

    def _query(self, job, target):
        if not self.live(target):
            raise OfflineAbort('Maintenance target expired; rerun to reassess')
        saved = self.db.conn.execute('SELECT query_json FROM flow_bootstrap_identity WHERE stage=? AND launch_id=? AND kind=?',
                                    (job['stage'], job['launch_id'], job['kind'])).fetchone()
        query = super()._query(job, target)
        if not saved or saved[0] != canonical(query):
            raise OfflineAbort('Maintenance filter identity changed')
        return query

    def promote_offline(self, manifest, stage, head_at):
        self.check_snapshot(manifest)
        jobs = self.db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=?', (stage,)).fetchall()
        from app.flow_bootstrap import CursorBootstrap
        verifier = CursorBootstrap(self)
        checked = [(j, *verifier._verify_job(stage, j)) for j in jobs if self.live(self.db.target(j['launch_id']))]
        with self.db.conn:
            for job, target, _ in checked:
                launch, kind, head = job['launch_id'], job['kind'], job['reconciliation_upper_bound']
                self.promoting_launch = launch
                base = next(b for k, _, b, _ in self.periods(target, head) if k == kind)
                state = self.db.conn.execute('SELECT * FROM flow_bootstrap WHERE launch_id=? AND kind=?', (launch, kind)).fetchone()
                cursor = self.db.state(f'recovery:{launch}:{kind}')
                if cursor is None or not state or state['status'] != 'complete':
                    if job['original_safe_start'] != base or (state and state['safe_start'] != base):
                        raise OfflineAbort('Activation proof incomplete')
                    self.db.conn.execute("""INSERT INTO flow_bootstrap VALUES(?,?,?,'complete',?,?,?)
                        ON CONFLICT(launch_id,kind) DO UPDATE SET status='complete',completed_head=excluded.completed_head,
                        completed_at=excluded.completed_at""", (launch, kind, base, head, time.time(), time.time()))
                self.db.conn.execute("""INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
                    DO UPDATE SET value=max(cast(value AS INTEGER),cast(excluded.value AS INTEGER))""",
                    (f'recovery:{launch}:{kind}', str(head)))
            self.promoting_launch = None
            # All relevant filters must cover a gap; do not clear unknown/reorg obligations.
            for row in manifest['snapshot']:
                target = self.db.target(row['target']['launch_id'])
                if not self.live(target):
                    continue
                launch = target['launch_id']
                self.promoting_launch = launch
                for gap in self.db.conn.execute('SELECT * FROM flow_gaps WHERE launch_id=? AND resolved=0', (launch,)).fetchall():
                    reason = gap['reason']
                    kinds = [f for f in row['filters'] if reason == 'bootstrap_required:'+f['kind']] if reason.startswith('bootstrap_required:') else row['filters']
                    known = reason.startswith('bootstrap_required:') or reason in ('ws_gap', 'reconnect_recovery_incomplete', 'recovery_rejection')
                    proved = bool(kinds) and known and gap['first_block'] is not None and gap['first_block'] <= manifest['heads'][-1]['head'] and gap['end_at'] <= head_at
                    if reason.startswith('bootstrap_required:'):
                        proved = proved and all(gap['first_block'] == f['safe_start'] for f in kinds)
                    for filt in kinds:
                        last = min(manifest['heads'][-1]['head'], filt['end'])
                        first = max(filt['safe_start'], gap['first_block'] or filt['safe_start'])
                        proofs = self.reusable(launch, filt['kind'], filt['query'], first, last)
                        covered = first-1
                        for proof in proofs:
                            if proof['first_block'] > covered+1:
                                break
                            covered = max(covered, proof['last_block'])
                        proved = proved and covered >= last
                    if proved:
                        self.db.conn.execute('UPDATE flow_gaps SET resolved=1 WHERE id=?', (gap['id'],))
                        manifest['gaps_resolved'].append({'id': gap['id'], 'stage': stage, 'at': time.time(), 'original': dict(gap)})
                # Only active targets receive mutable feature rebuilds; actual wall-clock PIT append.
                self.db.conn.execute("""UPDATE flow_tracking_targets SET coverage_start_at=coalesce(coverage_start_at,tracking_start_at),
                    coverage_end_at=max(coalesce(coverage_end_at,0),?),updated_at=? WHERE launch_id=?""",
                    (min(head_at, target['tracking_end_at']), time.time(), launch))
            self.promoting_launch = None
            self.save_manifest(manifest)
        for row in manifest['snapshot']:
            target = self.db.target(row['target']['launch_id'])
            if self.live(target):
                self.promoting_launch = target['launch_id']
                self.db.rebuild(target)
                self.promoting_launch = None

    def assessment(self, head):
        filters, gaps, bootstrap = [], [], []
        for row in self.snapshot():
            target = row['target']
            gaps.extend(g['id'] for g in row['gaps'])
            for f in row['filters']:
                cursor = f['cursor']
                end = min(head, f['end']) if head is not None else None
                retired = f['kind']=='curve' and target['graduation_digest'] != hashlib.sha256(b'').hexdigest() and cursor is not None and end is not None and cursor >= end
                bootstrap.append(f['bootstrap'] is None or f['bootstrap']['status'] != 'complete')
                filters.append({'launch_id': target['launch_id'], 'kind': f['kind'], 'cursor': cursor,
                                'lag': (max(0,end-cursor) if retired else end-cursor) if end is not None and cursor is not None and cursor<=head+2 else None,
                                'normal_blocks': (0 if retired else end-max(f['safe_start'], cursor-2)+1) if end is not None and cursor is not None else None})
        switches_clear = not self.db.conn.execute("SELECT 1 FROM flow_provider_switches WHERE state!='HEALTHY' LIMIT 1").fetchone()
        safe = switches_clear and not gaps and not any(bootstrap) and all(
            f['lag'] is not None and 0 <= f['lag'] <= 100 and f['normal_blocks'] <= RECOVERY_MAX_BLOCKS for f in filters)
        return {'restart_safe_now': safe, 'head': head, 'filters': filters, 'active_gap_ids': gaps,
                'active_bootstrap_incomplete': sum(bootstrap), 'provider_switch_clear': switches_clear}

    async def run(self, max_rounds=4):
        try:
            return await self._run(max_rounds)
        except FlowBudget:
            manifest = self.manifest()
            if manifest is None:raise
            return self.finish(manifest, self.assessment(None), 'OFFLINE_DRAIN_BUDGET_PENDING')

    async def _run(self, max_rounds):
        manifest = self.manifest()
        if manifest is None:
            snapshot = self.snapshot()
            manifest = {'generation_id': self.generation, 'started_at': time.time(), 'revision': self.revision,
                        'source_flow_pid': self.source_pid, 'snapshot': snapshot,
                        'snapshot_digest': hashlib.sha256(canonical(snapshot).encode()).hexdigest(),
                        'heads': [], 'reused': [], 'gaps_resolved': [], 'completed_at': None}
            self.save_manifest(manifest)
        elif manifest['revision'] != self.revision or manifest['source_flow_pid'] != self.source_pid:
            raise OfflineAbort('Maintenance generation revision/process changed')
        self.check_snapshot(manifest)
        if not self.snapshot():
            return self.finish(manifest, self.assessment(None))
        for _ in range(max_rounds):
            # A rerun of a completed generation still checks a fresh restart head.
            pending = manifest['heads'] and not manifest['heads'][-1].get('promoted')
            if pending:
                frozen = manifest['heads'][-1]
            else:
                if not self.snapshot():
                    return self.finish(manifest, self.assessment(None))
                head = int(await self.worker.rpc.call('eth_blockNumber', []), 16)
                header = await self.worker.rpc.call('eth_getBlockByNumber', [hex(head), False])
                if int(header['number'],16) != head:
                    raise OfflineAbort('Maintenance head mismatch')
                frozen = {'head': head, 'at': int(header['timestamp'],16), 'stage': self.prefix+':'+str(len(manifest['heads']))}
                manifest['heads'].append(frozen)
                self.save_manifest(manifest)
            self.plan(manifest, frozen['head'], frozen['at'], frozen['stage'])
            has_jobs = self.db.conn.execute('SELECT 1 FROM flow_shadow_jobs WHERE stage=?', (frozen['stage'],)).fetchone()
            if has_jobs and not await self.run_stage(frozen['stage']):
                return self.finish(manifest, self.assessment(frozen['head']), 'OFFLINE_DRAIN_BUDGET_PENDING' if self.pause_scope else 'OFFLINE_DRAIN_PROOF_PENDING')
            if has_jobs:
                self.promote_offline(manifest, frozen['stage'], frozen['at'])
            frozen['promoted'] = True
            self.save_manifest(manifest)
            head = int(await self.worker.rpc.call('eth_blockNumber', []), 16)
            result = self.assessment(head)
            if result['restart_safe_now']:
                return self.finish(manifest, result)
        return self.finish(manifest, self.assessment(head), 'OFFLINE_DRAIN_HEAD_PENDING')

    def finish(self, manifest, assessment, gate=None):
        self.guard()
        if gate is not None:assessment = dict(assessment, restart_safe_now=False)
        manifest['assessment'] = assessment
        manifest['cursors_after'] = assessment['filters']
        manifest['proof'] = [self.summary(h['stage']) for h in manifest['heads']]
        manifest['completed_ranges'] = [dict(r) for h in manifest['heads'] for r in self.db.conn.execute(
            'SELECT * FROM flow_shadow_ranges WHERE stage=?', (h['stage'],))]
        manifest['completed_at'] = time.time() if assessment['restart_safe_now'] else None
        self.save_manifest(manifest)
        return {'gate': gate or ('OFFLINE_DRAIN_COMPLETE' if assessment['restart_safe_now'] else 'OFFLINE_DRAIN_PROOF_PENDING'),
                **assessment, 'manifest': manifest}


def dry_plan(worker):
    """Read-only inspection; no reconciler construction, head RPC, schema or writes."""
    runner = object.__new__(OfflineDrain)
    runner.worker, runner.db = worker, worker.db
    head = worker.db.state('last_connected_block')
    head = int(head) if head else None
    rows = runner.snapshot()
    filters = []
    for row in rows:
        for f in row['filters']:
            last = min(head, f['end']) if head is not None else None
            first = runner.start_block(row, f)
            proofs = runner.reusable(row['target']['launch_id'], f['kind'], f['query'], first, last) if last is not None else []
            covered = first-1
            for proof in proofs:
                if proof['first_block'] > covered+1:break
                covered = max(covered, proof['last_block'])
            positions = max(0, last-max(first, covered+1)+1) if last is not None else None
            if head is not None and not runner.needs_work(row['target']['launch_id'], f, head):positions = 0
            filters.append({'launch_id': row['target']['launch_id'], 'kind': f['kind'], 'query': f['query'], 'safe_start': f['safe_start'],
                            'cursor': f['cursor'], 'active_gap_ids': [g['id'] for g in row['gaps']],
                            'estimated_filter_block_positions': positions, 'reusable_proof': proofs,
                            'estimated_getlogs': math.ceil(positions/SHADOW_SPAN) if positions is not None else None})
    day, minute = int(time.time())//86400*86400, int(time.time())//60*60
    budget = {k: worker.db.used(k, day) for k in ('flow_eth_getLogs','flow_rpc_members')}
    budget['rpc_minute'] = worker.db.used('flow_rpc_members', minute)
    calls = sum(f['estimated_getlogs'] or 0 for f in filters)
    return {'dry_run': True, 'rpc_calls': 0, 'db_mutations': 0, 'service_actions': 0,
            'head_estimate': head, 'head_is_live': False, 'filters': filters, 'budget': budget,
            'estimated_getlogs': calls, 'reserve_likely_preserved': head is not None and
            budget['flow_eth_getLogs']+calls*3 <= worker.settings.daily_getlogs-50,
            'estimate_warning': 'Stored head only; adaptive reductions and moving head can increase work'}
