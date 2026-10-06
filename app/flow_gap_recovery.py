"""Bounded current-partition gap proof using the existing durable range runner."""
import json
import time

from app.flow_budget import FlowBudget
from app.flow_identity import compatible_ranges
from app.rpc import RpcError


def obligations(db):
    result=[]
    for row in db.conn.execute('SELECT * FROM flow_gaps WHERE resolved=0 ORDER BY id'):
        gap=dict(row);target=db.target(gap['launch_id'])
        saved=json.loads(db.state(f'gap_recovery:{gap["id"]}','{}'))
        state=saved.get('state','queued');reason=saved.get('reason')
        if not target or target['status'] in ('completed','partial') or target['tracking_end_at']+10<time.time():
            state,reason='operator_blocked','target_expired_required_proof_retained'
        elif gap['reason'] not in ('ws_gap','reconnect_recovery_incomplete'):
            # Bootstrap/tail/switch debt has its own existing durable scheduler.
            if gap['reason'].startswith('bootstrap_required:') or gap['reason']=='epoch_activation_tail_required':
                state,reason='queued',gap['reason']
            else:state,reason='operator_blocked','unsupported_gap_reason:'+gap['reason']
        elif gap['first_block'] is None:state,reason='operator_blocked','missing_required_block_boundary'
        result.append({**gap,**saved,'state':state,'reason_detail':reason})
    return result


def save(db,gap,**values):
    key=f'gap_recovery:{gap["id"]}'
    state=json.loads(db.state(key,'{}'));state.update(values)
    db.set_state(key,json.dumps(state,sort_keys=True))


async def recover(worker,target,gap):
    from app.flow_bootstrap import CursorBootstrap
    from app.flow_shadow import ShadowReconciler
    from app.flow_switch_recovery import semantics
    db=worker.db;stage=f'live_gap:{gap["id"]}'
    state=json.loads(db.state(f'gap_recovery:{gap["id"]}','{}'))
    if state.get('retry_at',0)>time.time():return False
    if gap['state']=='operator_blocked':return False
    original=worker.rpc;runner=ShadowReconciler(worker,reserve=50)
    bootstrap=CursorBootstrap(runner)
    save(db,gap,state='recovering',last_attempt_at=time.time(),attempts=state.get('attempts',0)+1)
    try:
        # A complete, identity-bound proof after the uncertain end needs no RPC.
        if runner.meta(stage+':head') is None:
            candidates=db.conn.execute('SELECT DISTINCT reconciliation_upper_bound,upper_at '
                'FROM flow_shadow_jobs JOIN flow_bootstrap_identity USING(stage,launch_id,kind) '
                "WHERE launch_id=? AND completion_status='complete' AND upper_at>=? ORDER BY upper_at",
                (target['launch_id'],gap['end_at'])).fetchall()
            for head,at in candidates:
                floor=max([gap['first_block']]+[base for _,_,base,_ in runner.periods(target,head)])
                if head>=floor and all(_coverage(db,target,kind,query,max(base,gap['first_block']),end,gap['end_at'])[1]==0
                       for kind,query,base,end in runner.periods(target,head)):
                    runner.set_meta(stage+':head',head);runner.set_meta(stage+':head_at',int(at))
                    runner.set_meta(stage+':ids',json.dumps([target['launch_id']]))
                    break
        # Persist the head before a separately budgeted header request.
        if runner.meta(stage+':head') is None:
            head=runner.meta(stage+':captured_head')
            if head is None:
                head=int(await worker.rpc.call('eth_blockNumber',[]),16)
                runner.set_meta(stage+':captured_head',head)
            head=int(head)
            header=await worker.rpc.call('eth_getBlockByNumber',[hex(head),False])
            if int(header['number'],16)!=head:raise RpcError('Gap head identity mismatch')
            runner.set_meta(stage+':head',head);runner.set_meta(stage+':head_at',int(header['timestamp'],16))
            runner.set_meta(stage+':ids',json.dumps([target['launch_id']]))
        head,at,_=await bootstrap.freeze(stage,[target['launch_id']])
        if at<gap['end_at']:raise RpcError('Pinned proof head precedes uncertain end')
        missing=0
        for job in db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=?',(stage,)).fetchall():
            query=runner._query(job,target);key=(stage,target['launch_id'],job['kind'])
            lifecycle=semantics(db,{'launch_id':target['launch_id']})
            saved=runner.meta(stage+f':semantics:{target["launch_id"]}:{job["kind"]}')
            if saved is not None and saved!=lifecycle:raise RpcError('Gap lifecycle changed')
            runner.set_meta(stage+f':semantics:{target["launch_id"]}:{job["kind"]}',lifecycle)
            ranges,unproved=_coverage(db,target,job['kind'],query,job['original_safe_start'],
                                      job['reconciliation_upper_bound'],gap['end_at'])
            missing+=unproved
            with db.conn:
                for first,last in ranges:
                    db.conn.execute('INSERT OR IGNORE INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)',
                                     (*key,first,last,int(last==job['reconciliation_upper_bound'])))
        # Explicit debt does not authorize unbounded initialized-cursor recovery.
        if missing>100:
            save(db,gap,state='operator_blocked',reason='unproved_recovery_range_exceeds_100',unproved_blocks=missing)
            return False
        if not await runner.run_stage(stage,max_chunks=1):
            wait=runner.pause_budget
            save(db,gap,state='budget_wait' if wait else 'queued',
                 reason=wait.scope if wait else 'proof_pending',retry_at=(wait.reset_at or time.time()+30) if wait else time.time()+30)
            return False
        jobs=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=?',(stage,)).fetchall()
        for job in jobs:bootstrap._verify_job(stage,job)
        # Check every lifecycle filter together; a single kind cannot resolve debt.
        periods=list(runner.periods(target,head))
        if {j['kind'] for j in jobs}!={k for k,_,_,_ in periods}:raise RpcError('Gap required filter missing')
        with db.conn:
            for job in jobs:
                key=f'recovery:{target["launch_id"]}:{job["kind"]}'
                db.conn.execute('INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key) '
                    'DO UPDATE SET value=max(cast(value AS INTEGER),cast(excluded.value AS INTEGER))',
                    (key,str(job['reconciliation_upper_bound'])))
            db.conn.execute('UPDATE flow_gaps SET resolved=1 WHERE id=?',(gap['id'],))
            save(db,gap,state='resolved',completed_at=time.time(),reason='identity_bound_contiguous_http_proof')
        worker.dirty.add(target['launch_id'])
        return True
    except FlowBudget as wait:
        save(db,gap,state='budget_wait',reason=wait.scope,retry_at=wait.reset_at or time.time()+30)
        return False
    except (RpcError,ValueError) as exc:
        save(db,gap,state='operator_blocked',reason=str(exc))
        return False
    finally:
        shadow=worker.rpc;worker.rpc=original
        await shadow.close()


def _coverage(db,target,kind,query,first,last,before):
    from app.flow_switch_recovery import semantics
    ranges=compatible_ranges(db,target['launch_id'],kind,query,
                             semantics(db,{'launch_id':target['launch_id']}),first,last,
                             legacy_before=None if target['graduation_json'] else before)
    merged=[]
    for row in ranges:
        low,high=max(first,row['first_block']),min(last,row['last_block'])
        if merged and low<=merged[-1][1]+1:merged[-1][1]=max(merged[-1][1],high)
        else:merged.append([low,high])
    return merged,max(0,last-first+1)-sum(high-low+1 for low,high in merged)
