"""Resumable Validation-only reconciliation before a flow provider cutover."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import time

from app.config import ROOT
from app.flow_data import BUY, SELL
from app.flow_providers import provider
from app.flow_worker import FlowBudget, FlowRpc, FlowWorker
from app.rpc import LogRangeError, Rpc, RpcError

SHADOW_SPAN = 2000
CUTOVER_RESERVE = 50
REQUIRED_COMMITS = ('6fa1d28', 'ab4d2d6', '70c18e5')


def verified_checkout(root=ROOT):
    """The deployment source must be one exact, clean Git revision."""
    root = Path(root).resolve()
    def git(*args):
        return subprocess.run(('git', '-C', str(root), *args), capture_output=True, text=True)
    top = git('rev-parse', '--show-toplevel')
    if top.returncode or Path(top.stdout.strip()).resolve() != root:
        raise RuntimeError('Deployment source is not a Git checkout')
    head = git('rev-parse', 'HEAD')
    status = git('status', '--porcelain=v1')
    if head.returncode or status.returncode or status.stdout.strip():
        raise RuntimeError('Deployment checkout is not clean')
    for commit in REQUIRED_COMMITS:
        if git('merge-base', '--is-ancestor', commit, 'HEAD').returncode:
            raise RuntimeError('Required migration commit is absent')
    return head.stdout.strip()


class ShadowRpc(FlowRpc):
    """Account every Validation attempt before sending; share the 400/day guard."""
    def __init__(self, config, settings, db, providers):
        super().__init__(config, replace(settings,split_enabled=True), db, providers)
        if provider(self.config.rpc_http) != 'validation':
            raise ValueError('Shadow RPC must be Validation HTTP')
        self.job = None
        self.attempt = 0

    def add(self, metric, n=1):
        super().add(metric, n)
        self.db.count('flow_shadow_rpc_' + metric, n)

    async def call(self, method, params):
        self.attempt = 0
        return await super().call(method, params)

    async def _send(self, payload, method):
        members = payload if isinstance(payload, list) else [payload]
        n = len(members)
        logs = sum(x['method'] == 'eth_getLogs' for x in members)
        now = int(time.time())
        day, minute = now // 86400 * 86400, now // 60 * 60
        self.db.conn.execute('BEGIN IMMEDIATE')
        try:
            if self.db.used('flow_rpc_members', day) + n > self.settings.daily_calls:
                raise FlowBudget('Shadow daily HTTP budget exhausted')
            if self.db.used('flow_rpc_members', minute) + n > self.settings.minute_calls:
                raise FlowBudget('Shadow minute HTTP budget exhausted')
            if logs and self.db.used('flow_eth_getLogs', day) + logs > self.settings.daily_getlogs - CUTOVER_RESERVE:
                raise FlowBudget('Shadow getLogs reserve reached')
            self.attempt += 1
            metrics = [('flow_rpc_members', n), ('flow_http_calls', 1),
                       ('flow_http_calls_validation', 1), ('flow_rpc_members_validation', n),
                       ('flow_shadow_http_calls_validation', 1), ('flow_shadow_rpc_members', n)]
            if logs:
                metrics += [('flow_eth_getLogs_validation', logs),
                            ('flow_shadow_eth_getLogs_validation', logs)]
            for item in members:
                metrics.append(('flow_' + item['method'], 1))
            for name, count in metrics:
                self.db.conn.execute('INSERT INTO flow_usage VALUES(?,?,?) ON CONFLICT(minute,metric) DO UPDATE SET count=count+excluded.count',
                                     (minute, name, count))
            if self.job and logs:
                self.db.conn.execute('''UPDATE flow_shadow_jobs SET actual_getlogs_calls=actual_getlogs_calls+?,
                  retries=retries+? WHERE stage=? AND launch_id=? AND kind=?''',
                  (logs, int(self.attempt > 1), *self.job))
            self.db.conn.commit()
        except BaseException:
            self.db.conn.rollback()
            raise
        return await Rpc._send(self, payload, method)


class ShadowReconciler:
    def __init__(self, worker):
        self.worker = worker
        self.db = worker.db
        self.worker.rpc = ShadowRpc(worker.config, worker.settings, self.db, worker.providers)
        self._schema()

    def _schema(self):
        self.db.conn.executescript('''
          CREATE TABLE IF NOT EXISTS flow_shadow_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS flow_shadow_jobs(
            stage TEXT NOT NULL,launch_id INTEGER NOT NULL,kind TEXT NOT NULL,
            original_safe_start INTEGER NOT NULL,reconciliation_upper_bound INTEGER NOT NULL,
            next_unverified_block INTEGER NOT NULL,highest_contiguous_verified_block INTEGER NOT NULL,
            span INTEGER NOT NULL DEFAULT 2000,actual_getlogs_calls INTEGER NOT NULL DEFAULT 0,
            retries INTEGER NOT NULL DEFAULT 0,range_reductions INTEGER NOT NULL DEFAULT 0,
            recovered_raw_events INTEGER NOT NULL DEFAULT 0,duplicates_ignored INTEGER NOT NULL DEFAULT 0,
            failed_from INTEGER,failed_to INTEGER,failed_error TEXT,
            completion_status TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY(stage,launch_id,kind));
          CREATE TABLE IF NOT EXISTS flow_shadow_ranges(
            stage TEXT NOT NULL,launch_id INTEGER NOT NULL,kind TEXT NOT NULL,
            first_block INTEGER NOT NULL,last_block INTEGER NOT NULL,was_terminal INTEGER NOT NULL,
            PRIMARY KEY(stage,launch_id,kind,first_block,last_block));
        ''')

    def meta(self, key):
        row = self.db.conn.execute('SELECT value FROM flow_shadow_meta WHERE key=?', (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key, value):
        with self.db.conn:
            self.db.conn.execute('INSERT INTO flow_shadow_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                 (key, str(value)))

    def periods(self, target, head):
        filters = self.worker.filters(target)
        graduation = json.loads(target['graduation_json']) if target['graduation_json'] else None
        if graduation:
            filters['curve'] = {'address': target['curve_address'], 'topics': [[BUY, SELL]]}
        for kind, query in filters.items():
            start = graduation['block_number'] if graduation and kind != 'curve' else target['launch_block']
            end = min(head, graduation['block_number']) if graduation and kind == 'curve' else head
            yield kind, query, start, end

    def historical_start(self,target,kind,base,head):
        cursor=self.db.state(f'recovery:{target["launch_id"]}:{kind}')
        if cursor is not None and int(cursor)>head+2:
            raise RpcError('Persisted recovery cursor exceeds validated head')
        start=max(base,int(cursor)-2) if cursor is not None else base
        gap=self.db.conn.execute('''SELECT min(first_block) FROM flow_gaps WHERE launch_id=? AND resolved=0
          AND reason IN ('ws_gap','reconnect_recovery_incomplete') AND first_block IS NOT NULL''',
          (target['launch_id'],)).fetchone()[0]
        return min(start,max(base,gap)) if gap is not None else start

    def add_jobs(self, stage, first, head):
        targets = [dict(row) for row in self.db.conn.execute(
            "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') ORDER BY launch_id")]
        with self.db.conn:
            for target in targets:
                for kind, _, base, end in self.periods(target, head):
                    start=max(first,self.historical_start(target,kind,base,head)) if stage=='historical' else max(first,base)
                    self.db.conn.execute('''INSERT OR IGNORE INTO flow_shadow_jobs
                      (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
                       next_unverified_block,highest_contiguous_verified_block,completion_status)
                      VALUES(?,?,?,?,?,?,?,?)''',
                      (stage,target['launch_id'],kind,start,end,start,start-1,
                       'complete' if start > end else 'pending'))
                    row = self.db.conn.execute('''SELECT original_safe_start,reconciliation_upper_bound FROM flow_shadow_jobs
                      WHERE stage=? AND launch_id=? AND kind=?''',(stage,target['launch_id'],kind)).fetchone()
                    if row[1] != end or row[0] > start:
                        raise RpcError('Shadow job boundaries changed; operator review required')
        return len(targets)

    async def start_historical(self):
        head = self.meta('H_prefetch')
        if head is None:
            head = int(await self.worker.rpc.call('eth_blockNumber', []), 16)
            self.set_meta('H_prefetch', head)
        elif self.complete('historical'):
            newer=int(await self.worker.rpc.call('eth_blockNumber', []),16)
            if newer>int(head):
                targets=[dict(row) for row in self.db.conn.execute(
                    "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')")]
                with self.db.conn:
                    for target in targets:
                        for kind,_,base,end in self.periods(target,newer):
                            row=self.db.conn.execute('''SELECT original_safe_start,reconciliation_upper_bound,next_unverified_block
                              FROM flow_shadow_jobs WHERE stage='historical' AND launch_id=? AND kind=?''',
                              (target['launch_id'],kind)).fetchone()
                            if row:
                                if row[1]>end or row[2]!=row[1]+1:
                                    raise RpcError('Historical filter changed before head extension')
                                if end>row[1]:
                                    self.db.conn.execute('''UPDATE flow_shadow_jobs SET reconciliation_upper_bound=?,
                                      completion_status='pending' WHERE stage='historical' AND launch_id=? AND kind=?''',
                                      (end,target['launch_id'],kind))
                            else:
                                start=self.historical_start(target,kind,base,newer)
                                self.db.conn.execute('''INSERT INTO flow_shadow_jobs
                                  (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
                                   next_unverified_block,highest_contiguous_verified_block,completion_status)
                                  VALUES('historical',?,?,?,?,?,?,?)''',
                                  (target['launch_id'],kind,start,end,start,start-1,
                                   'complete' if start>end else 'pending'))
                    self.db.conn.execute('''UPDATE flow_shadow_meta SET value=? WHERE key='H_prefetch' ''',(str(newer),))
                head=newer
        head = int(head)
        # Existing jobs may have aged out; newly active filters are added here.
        self.add_jobs('historical', 0, head)
        return head

    def complete(self, stage):
        if not self.db.conn.execute('SELECT 1 FROM flow_shadow_jobs WHERE stage=? LIMIT 1',(stage,)).fetchone():
            return False
        return not self.db.conn.execute('''SELECT 1 FROM flow_shadow_jobs
          WHERE stage=? AND completion_status!='complete' LIMIT 1''',(stage,)).fetchone()

    def fail_range(self, key, first, last, exc):
        with self.db.conn:
            self.db.conn.execute('''UPDATE flow_shadow_jobs SET failed_from=?,failed_to=?,failed_error=?
              WHERE stage=? AND launch_id=? AND kind=?''',(first,last,type(exc).__name__,*key))

    def _query(self, job, target):
        for kind, query, _, _ in self.periods(target, job['reconciliation_upper_bound']):
            if kind == job['kind']:
                return query
        raise RpcError('Shadow target filter changed')

    async def run_stage(self, stage):
        for job in self.db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? ORDER BY launch_id,kind',(stage,)).fetchall():
            key=(stage,job['launch_id'],job['kind'])
            target=self.db.target(job['launch_id'])
            if not target:raise RpcError('Shadow target disappeared')
            query=self._query(job,target)
            current=job['next_unverified_block']
            span=job['span']
            last=job['reconciliation_upper_bound']
            graduation=json.loads(target['graduation_json']) if target['graduation_json'] else None
            while current<=last:
                end=min(last,current+span-1)
                self.worker.rpc.job=key
                try:
                    rows=await self.worker.rpc.call('eth_getLogs',[dict(query,fromBlock=hex(current),toBlock=hex(end))])
                except LogRangeError:
                    if span==1:
                        self.fail_range(key,current,end,LogRangeError('range one rejected'))
                        return False
                    span=max(1,(end-current+1)//2)
                    with self.db.conn:
                        self.db.conn.execute('''UPDATE flow_shadow_jobs SET span=?,range_reductions=range_reductions+1,
                          failed_from=?,failed_to=?,failed_error='LogRangeError' WHERE stage=? AND launch_id=? AND kind=?''',
                          (span,current,end,*key))
                    continue
                except FlowBudget as exc:
                    if 'minute' in str(exc):
                        await asyncio.sleep(60-time.time()%60+.05)
                        continue
                    return False
                except RpcError as exc:
                    self.fail_range(key,current,end,exc)
                    return False
                finally:
                    self.worker.rpc.job=None
                try:
                    if not isinstance(rows,list):raise RpcError('Invalid shadow log response')
                    for item in rows:
                        if not isinstance(item,dict) or item.get('removed'):raise RpcError('Invalid shadow log')
                        try:block=int(item['blockNumber'],16);position=(block,int(item['logIndex'],16))
                        except (KeyError,TypeError,ValueError):raise RpcError('Invalid shadow log position') from None
                        if not current<=block<=end:raise RpcError('Shadow log outside query range')
                        if job['kind']=='curve' and position<(target['launch_block'],target['launch_log_index']):continue
                        if graduation:
                            boundary=(graduation['block_number'],graduation['log_index'])
                            if (job['kind']=='curve' and position>=boundary) or (job['kind']!='curve' and position<=boundary):continue
                        if not item.get('blockTimestamp'):
                            item['blockTimestamp']=hex(await self.worker.header(block))
                        if self.worker.ingest(target,item,shadow=stage) is False:
                            raise RpcError('Shadow event rejected')
                except (RpcError,FlowBudget) as exc:
                    self.fail_range(key,current,end,exc)
                    return False
                # Event writes commit first. A crash here replays and deduplicates.
                with self.db.conn:
                    self.db.conn.execute('''INSERT OR IGNORE INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)''',(*key,current,end,int(end==last)))
                    self.db.conn.execute('''UPDATE flow_shadow_jobs SET next_unverified_block=?,highest_contiguous_verified_block=?,
                      failed_from=NULL,failed_to=NULL,failed_error=NULL,
                      completion_status=?,recovered_raw_events=?,duplicates_ignored=?
                      WHERE stage=? AND launch_id=? AND kind=?''',
                      (end+1,end,'complete' if end==last else 'pending',
                       self.db.used(f'flow_shadow_events_stored:{stage}:{job["launch_id"]}:{job["kind"]}',0),
                       self.db.used(f'flow_shadow_duplicates:{stage}:{job["launch_id"]}:{job["kind"]}',0),*key))
                current=end+1
            self.worker.rpc.job=None
        return self.complete(stage)

    def summary(self, stage):
        jobs=[dict(row) for row in self.db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? ORDER BY launch_id,kind',(stage,))]
        sizes=[r[0]-r[1]+1 for r in self.db.conn.execute('SELECT last_block,first_block FROM flow_shadow_ranges WHERE stage=?',(stage,))]
        by_type={kind:sum(j['recovered_raw_events'] for j in jobs if j['kind']==kind)
                 for kind in ('curve','v4','hook')}
        return {'stage':stage,'jobs':jobs,'complete':self.complete(stage),'blocks_verified':sum(
            max(0,j['highest_contiguous_verified_block']-j['original_safe_start']+1) for j in jobs),
            'successful_getlogs_calls':len(sizes),'actual_getlogs_calls':sum(j['actual_getlogs_calls'] for j in jobs),
            'retries':sum(j['retries'] for j in jobs),'reductions':sum(j['range_reductions'] for j in jobs),
            'min_successful_chunk':min(sizes,default=None),'max_successful_chunk':max(sizes,default=None),
            'recovered_events':sum(j['recovered_raw_events'] for j in jobs),
            'recovered_events_by_type':by_type,
            'duplicates':sum(j['duplicates_ignored'] for j in jobs),
            'unresolved_ranges':[(j['launch_id'],j['kind'],j['next_unverified_block'],j['reconciliation_upper_bound'])
                                 for j in jobs if j['completion_status']!='complete']}

    def tail_plan(self, head):
        if not self.complete('historical'):raise RpcError('Historical shadow reconciliation incomplete')
        first=int(self.meta('H_prefetch'))+1
        sizes=[r[0]-r[1]+1 for r in self.db.conn.execute('''SELECT last_block,first_block
          FROM flow_shadow_ranges WHERE stage='historical' AND was_terminal=0''')]
        if not sizes:
            sizes=[r[0]-r[1]+1 for r in self.db.conn.execute(
                "SELECT last_block,first_block FROM flow_shadow_ranges WHERE stage='historical'")]
        if not sizes:raise RpcError('No measured successful recovery chunk')
        span=min(sizes)
        ranges=[(t['launch_id'],kind,max(first,base),end) for t in
                (dict(r) for r in self.db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"))
                for kind,_,base,end in self.periods(t,head) if max(first,base)<=end]
        calls=sum((end-start)//span+1 for _,_,start,end in ranges)
        day=int(time.time())//86400*86400
        remaining=self.worker.settings.daily_getlogs-self.db.used('flow_eth_getLogs',day)
        return {'H_prefetch':first-1,'H_pre_stop':head,'span':span,'ranges':ranges,
                'base_queries':calls,'reserved_attempts':calls*self.worker.rpc.config.retry_attempts,
                'remaining':remaining,'ready':remaining>=calls*self.worker.rpc.config.retry_attempts+CUTOVER_RESERVE}

    def promote(self, stage):
        if not self.complete(stage):raise RpcError('Cannot promote incomplete shadow stage')
        with self.db.conn:
            for job in self.db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=?',(stage,)):
                if job['highest_contiguous_verified_block']>=job['original_safe_start']:
                    self.db.conn.execute('''INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
                      DO UPDATE SET value=max(cast(value AS INTEGER),cast(excluded.value AS INTEGER))''',
                                         (f'recovery:{job["launch_id"]}:{job["kind"]}',str(job['highest_contiguous_verified_block'])))
            if stage=='wss_ready_tail':
                if not self.complete('historical') or not self.complete('stop_tail'):
                    raise RpcError('Shadow handoff has incomplete prior stage')
                head=int(self.meta('H_live'));at=int(self.meta('H_live_at'))
                for (launch,) in self.db.conn.execute("SELECT DISTINCT launch_id FROM flow_shadow_jobs WHERE stage='wss_ready_tail'"):
                    target=self.db.target(launch)
                    if not target:continue
                    gaps=self.db.conn.execute('''SELECT id,first_block,end_at FROM flow_gaps WHERE launch_id=?
                      AND resolved=0 AND reason IN ('ws_gap','reconnect_recovery_incomplete')''',(launch,)).fetchall()
                    for gap in gaps:
                        first=gap['first_block']
                        if first is None or gap['end_at']>at or first>head:continue
                        proved=True
                        for kind,_,base,end in self.periods(target,head):
                            start=max(first,base)
                            if start>end:continue
                            covered=start-1
                            ranges=self.db.conn.execute('''SELECT original_safe_start,highest_contiguous_verified_block
                              FROM flow_shadow_jobs WHERE launch_id=? AND kind=? AND completion_status='complete'
                              AND stage IN ('historical','stop_tail','wss_ready_tail') ORDER BY original_safe_start''',
                              (launch,kind)).fetchall()
                            for low,high in ranges:
                                if low<=covered+1:covered=max(covered,high)
                            if covered<end:proved=False;break
                        if proved:self.db.conn.execute('UPDATE flow_gaps SET resolved=1 WHERE id=?',(gap['id'],))
                    outstanding=self.db.conn.execute('''SELECT 1 FROM flow_gaps WHERE launch_id=? AND resolved=0
                      AND reason IN ('ws_gap','reconnect_recovery_incomplete') LIMIT 1''',(launch,)).fetchone()
                    if not outstanding:
                        self.db.conn.execute('''INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
                          DO UPDATE SET value=excluded.value''',(f'flow_shadow_handoff:{launch}',f'{head}:{at}'))


def make_reconciler(config, settings, db, providers):
    worker=FlowWorker(config,settings,db,providers)
    old=worker.rpc
    runner=ShadowReconciler(worker)
    return runner,old
