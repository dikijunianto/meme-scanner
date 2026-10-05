"""Proof-backed, resumable Validation HTTP bootstrap while legacy flow stays online."""
import json
import time

from app.flow_data import iso, stamp
from app.flow_shadow import SHADOW_SPAN
from app.rpc import RpcError
from app.flow_identity import query_identity


def required_filters(worker, target):
    """Every filter that can affect this target's due feature windows."""
    return list(worker.filters(target)) + (['curve'] if target['graduation_json'] else [])


class CursorBootstrap:
    def __init__(self, runner):
        self.runner=runner
        self.worker=runner.worker
        self.db=runner.db

    def _targets(self, ids):
        result=[]
        for launch in ids:
            target=self.db.target(launch)
            if not target:raise RpcError('Bootstrap target disappeared')
            result.append(target)
        return result

    def _expiry_cap(self, target, head, head_at):
        if target['tracking_end_at']>=head_at:return head,head_at
        row=self.worker.main.execute('''SELECT block_number,block_timestamp FROM launches
          WHERE block_timestamp>=? ORDER BY block_number LIMIT 1''',
          (iso(target['tracking_end_at']),)).fetchone()
        if row and row['block_number']<=head:
            return row['block_number'],stamp(row['block_timestamp'])
        return head,head_at

    def _plan(self, stage, ids, head, head_at, kinds=None):
        for target in self._targets(ids):
            cap,cap_at=self._expiry_cap(target,head,head_at)
            for kind,query,base,end in self.runner.periods(target,cap):
                if kinds is not None and kind not in kinds:continue
                if base>cap:continue
                end=min(end,cap)
                if end<base:continue
                cursor=self.db.state(f'recovery:{target["launch_id"]}:{kind}')
                start=self.runner.historical_start(target,kind,base,end)
                if target['graduation_json'] and (self.db.epoch() or {}).get('status')=='ACTIVATING' and stage.startswith('live_graduation:'):
                    start=base  # Independent full lifecycle proof while research remains sealed.
                if self.db.needs_bootstrap(target['launch_id'],kind):
                    self.db.require_bootstrap(target,kind,base)
                    if start!=base:raise RpcError('Missing-cursor bootstrap must start at activation')
                if start>end:continue
                identity=json.dumps(query,sort_keys=True,separators=(',',':'))
                key=(stage,target['launch_id'],kind)
                with self.db.conn:
                    self.db.conn.execute('''INSERT OR IGNORE INTO flow_shadow_jobs
                      (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
                       next_unverified_block,highest_contiguous_verified_block,span,completion_status)
                      VALUES(?,?,?,?,?,?,?,?,?)''',
                      (*key,start,end,start,start-1,SHADOW_SPAN,'pending'))
                    self.db.conn.execute('''INSERT OR IGNORE INTO flow_bootstrap_identity VALUES(?,?,?,?,?)''',
                                         (*key,identity,cap_at))
                    job=self.db.conn.execute('''SELECT original_safe_start,reconciliation_upper_bound
                      FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?''',key).fetchone()
                    saved=self.db.conn.execute('''SELECT query_json,upper_at FROM flow_bootstrap_identity
                      WHERE stage=? AND launch_id=? AND kind=?''',key).fetchone()
                    if (not job or job['original_safe_start']>start or
                        job['reconciliation_upper_bound']!=end or not saved or
                        query_identity(json.loads(saved['query_json']))!=query_identity(query) or saved['upper_at']!=cap_at):
                        raise RpcError('Bootstrap proof identity changed')
                    if cursor is None:
                        self.db.conn.execute("UPDATE flow_bootstrap SET status='in_progress' WHERE launch_id=? AND kind=? AND status='required'",
                                             (target['launch_id'],kind))

    async def freeze(self, stage, ids, kinds=None):
        saved=self.runner.meta(stage+':head')
        if saved is None:
            head=int(await self.worker.rpc.call('eth_blockNumber',[]),16)
            header=await self.worker.rpc.call('eth_getBlockByNumber',[hex(head),False])
            if int(header['number'],16)!=head:raise RpcError('Bootstrap head identity mismatch')
            head_at=int(header['timestamp'],16)
            with self.db.conn:
                for key,value in ((stage+':head',head),(stage+':head_at',head_at),
                                  (stage+':ids',json.dumps(sorted(set(ids))))):
                    self.db.conn.execute('INSERT INTO flow_shadow_meta VALUES(?,?)',(key,str(value)))
        else:
            head=int(saved);head_at=int(self.runner.meta(stage+':head_at'))
            ids=json.loads(self.runner.meta(stage+':ids'))
        self._plan(stage,ids,head,head_at,kinds)
        return head,head_at,ids

    def _verify_job(self, stage, job):
        if (job['completion_status']!='complete' or
            job['next_unverified_block']!=job['reconciliation_upper_bound']+1 or
            job['highest_contiguous_verified_block']!=job['reconciliation_upper_bound']):
            raise RpcError('Bootstrap job incomplete')
        covered=job['original_safe_start']-1
        for first,last in self.db.conn.execute('''SELECT first_block,last_block FROM flow_shadow_ranges
          WHERE stage=? AND launch_id=? AND kind=? ORDER BY first_block''',
          (stage,job['launch_id'],job['kind'])):
            if first!=covered+1 or last<first:raise RpcError('Bootstrap proof has a hole')
            covered=last
        if covered!=job['reconciliation_upper_bound']:raise RpcError('Bootstrap proof is not contiguous')
        target=self.db.target(job['launch_id'])
        if not target:raise RpcError('Bootstrap target missing at promotion')
        query=next((q for kind,q,_,_ in self.runner.periods(target,job['reconciliation_upper_bound'])
                    if kind==job['kind']),None)
        identity=self.db.conn.execute('''SELECT query_json,upper_at FROM flow_bootstrap_identity
          WHERE stage=? AND launch_id=? AND kind=?''',(stage,job['launch_id'],job['kind'])).fetchone()
        if not identity or query_identity(json.loads(identity['query_json']))!=query_identity(query):
            raise RpcError('Bootstrap filter changed')
        return target,identity['upper_at']

    def promote(self, stage):
        jobs=self.db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? ORDER BY launch_id,kind',(stage,)).fetchall()
        if not jobs:raise RpcError('Bootstrap has no jobs')
        checked=[(job,*self._verify_job(stage,job)) for job in jobs]
        affected=set()
        with self.db.conn:
            for job,target,upper_at in checked:
                launch,kind,head=job['launch_id'],job['kind'],job['reconciliation_upper_bound']
                state=self.db.conn.execute('SELECT safe_start,status,completed_head FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                           (launch,kind)).fetchone()
                cursor=self.db.state(f'recovery:{launch}:{kind}')
                if (self.db.needs_bootstrap(launch,kind) or (state and state['status']!='complete') or
                    (state and job['original_safe_start']==state['safe_start'] and head>state['completed_head'])):
                    graduation=json.loads(target['graduation_json']) if target['graduation_json'] else None
                    base=graduation['block_number'] if graduation and kind!='curve' else target['launch_block']
                    if not state or state['safe_start']!=base or job['original_safe_start']!=base:
                        raise RpcError('Missing cursor lacks activation-to-head proof')
                    # Completion is durable before the normal runtime cursor appears.
                    self.db.conn.execute("UPDATE flow_bootstrap SET status='complete',completed_head=?,completed_at=? WHERE launch_id=? AND kind=?",
                                         (head,time.time(),launch,kind))
                self.db.conn.execute('''INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
                  DO UPDATE SET value=max(cast(value AS INTEGER),cast(excluded.value AS INTEGER))''',
                                     (f'recovery:{launch}:{kind}',str(head)))
                if state:
                    self.db.conn.execute('''UPDATE flow_gaps SET resolved=1 WHERE launch_id=? AND reason=?
                      AND first_block=?''',(launch,f'bootstrap_required:{kind}',state['safe_start']))
                self.db.conn.execute('''UPDATE flow_gaps SET resolved=1 WHERE launch_id=? AND resolved=0
                  AND reason IN ('ws_gap','reconnect_recovery_incomplete')
                  AND first_block BETWEEN ? AND ? AND end_at<=?''',
                                     (launch,job['original_safe_start'],head,upper_at))
                coverage_end=min(target['tracking_end_at'],upper_at)
                self.db.conn.execute('''UPDATE flow_tracking_targets SET
                  coverage_start_at=CASE WHEN ?=launch_block THEN coalesce(coverage_start_at,tracking_start_at)
                    ELSE coverage_start_at END,
                  coverage_end_at=max(coalesce(coverage_end_at,0),?),updated_at=? WHERE launch_id=?''',
                                     (job['original_safe_start'],coverage_end,time.time(),launch))
                affected.add(launch)
            head=int(self.runner.meta(stage+':head'))
            sample=self.db.conn.execute('SELECT at,subscriptions FROM flow_samples ORDER BY at DESC LIMIT 1').fetchone()
            if (self.db.state('current_wss_provider')=='alchemy' and sample and
                time.time()-sample['at']<=65):
                for launch in affected:
                    target=self.db.target(launch)
                    if target['status'] in ('completed','partial'):continue
                    if self.db.conn.execute('SELECT 1 FROM flow_gaps WHERE launch_id=? AND resolved=0 LIMIT 1',(launch,)).fetchone():
                        continue
                    kinds=self.worker.filters(target)
                    if sample['subscriptions']<len(kinds):continue
                    if all(int(self.db.state(f'recovery:{launch}:{kind}',-1))>=head for kind in kinds):
                        self.db.conn.execute('''INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
                          DO UPDATE SET value=excluded.value''',
                                             (f'flow_shadow_handoff:{launch}',f'{head}:{int(time.time())}'))
        before=self.db.conn.execute('''SELECT coverage_quality,count(*) FROM flow_features
          WHERE launch_id IN (%s) GROUP BY coverage_quality'''%','.join('?' for _ in affected),tuple(affected)).fetchall()
        for launch in affected:self.db.rebuild(self.db.target(launch))
        after=self.db.conn.execute('''SELECT coverage_quality,count(*) FROM flow_features
          WHERE launch_id IN (%s) GROUP BY coverage_quality'''%','.join('?' for _ in affected),tuple(affected)).fetchall()
        return {'targets':sorted(affected),'features_before':dict(before),'features_after':dict(after)}

    async def run(self, stage, ids, kinds=None, active_only=False,max_chunks=None):
        head,head_at,ids=await self.freeze(stage,ids,kinds)
        complete=await self.runner.run_stage(stage,max_chunks=max_chunks)
        result={'stage':stage,'head':head,'head_at':head_at,'ids':ids,
                'pause_scope':self.runner.pause_scope,**self.runner.summary(stage)}
        if not complete:
            return {'gate':'MISSING_CURSOR_BOOTSTRAP_PENDING',**result}
        if active_only and any((t['status'] in ('completed','partial') or
                                t['tracking_end_at']+10<time.time()) for t in self._targets(ids)):
            return {'gate':'MISSING_CURSOR_BOOTSTRAP_PENDING','expired_incomplete':True,**result}
        result['promotion']=self.promote(stage)
        return {'gate':'BOOTSTRAP_PROOF_COMPLETE',**result}

    async def restart_plan(self):
        head=int(await self.worker.rpc.call('eth_blockNumber',[]),16)
        targets=[dict(r) for r in self.db.conn.execute(
            "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') ORDER BY launch_id")]
        rows=[]
        for target in targets:
            for kind,_,base,end in self.runner.periods(target,head):
                if base>end:continue
                cursor=self.db.state(f'recovery:{target["launch_id"]}:{kind}')
                if kind=='curve' and target['graduation_json'] and cursor is not None and int(cursor)>=end:
                    continue
                start=max(base,int(cursor)-2) if cursor is not None else base
                rows.append({'launch_id':target['launch_id'],'kind':kind,'safe_start':base,
                             'cursor':int(cursor) if cursor is not None else None,
                             'head':end,'delta':end-int(cursor) if cursor is not None else None,
                             'normal_recovery_blocks':end-start+1,
                             'bootstrap_required':cursor is None})
        return {'head':head,'filters':rows,
                'safe_now':all(not r['bootstrap_required'] and r['normal_recovery_blocks']<=100
                               and -2<=r['delta']<=60 for r in rows)}
