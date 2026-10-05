"""Opt-in later-head superset proof. Importing this module performs no work."""
import hashlib
import json
import time
import math
from datetime import datetime, timezone
from dataclasses import replace

from app import flow_provider_switch as switch
from app.flow_data import BUY, SELL
from app.flow_budget import BudgetWait
from app.flow_identity import canonical_address, query_identity, compatible_ranges
from app.flow_shadow import SHADOW_SPAN


def plan(db,main,switch_id):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['state']!='FAILED' or switch.latest(db,row['session_id'])['id']!=switch_id:
        raise ValueError('Latest FAILED switch required')
    item=switch.value(row)
    if item.get('frozen_head') is not None:raise ValueError('Use exact-original-head recovery')
    if not item.get('filters'):raise ValueError('MISSING_FILTER_SNAPSHOT_BLOCKED')
    filters=[]
    for f in item['filters']:
        base=f.get('base')
        if type(base) is not int or base<=0:raise ValueError('MISSING_LOWER_BOUND_BLOCKED')
        target=db.target(f.get('launch_id'))
        launch=main.execute('SELECT * FROM launches WHERE id=?',(f.get('launch_id'),)).fetchone()
        if (not target or not launch or f.get('kind')!='curve' or target['graduation_json'] or
            main.execute('SELECT 1 FROM graduations WHERE lower(token_address)=?',
                         (canonical_address(target['token_address']),)).fetchone() or
            target['launch_block']!=base or launch['block_number']!=base or
            target['launch_log_index']!=launch['log_index'] or
            any(canonical_address(target[k])!=canonical_address(launch[k])
                for k in ('token_address','quote_asset_address','curve_address'))):
            raise ValueError('FILTER_IDENTITY_BLOCKED')
        query={'address':canonical_address(target['curve_address']),'topics':[[BUY,SELL]]}
        if (any(k in f and canonical_address(f[k])!=canonical_address(target[t])
                for k,t in (('token','token_address'),('quote','quote_asset_address'))) or
            ('launch_log_index' in f and f['launch_log_index']!=target['launch_log_index']) or
            ('lifecycle' in f and f['lifecycle']!='curve_ungraduated')):
            raise ValueError('FILTER_SNAPSHOT_IDENTITY_BLOCKED')
        if f.get('query') is not None and query_identity(f['query'])!=query:raise ValueError('FILTER_QUERY_BLOCKED')
        if f.get('query') is None:
            original=db.conn.execute('SELECT query_json,upper_at FROM flow_bootstrap_identity '
                'WHERE stage=? AND launch_id=? AND kind=?',
                (f'live_bootstrap:{f["launch_id"]}',f['launch_id'],'curve')).fetchone()
            if (not original or query_identity(json.loads(original['query_json']))!=query or
                original['upper_at']>item['filter_snapshot_at']):raise ValueError('FILTER_QUERY_PROVENANCE_BLOCKED')
        filters.append({'launch_id':f['launch_id'],'kind':'curve','safe_start':base,
                        'lower_bound_source':'durable_switch_filter_base_verified_against_launch',
                        'query':query,'token':canonical_address(target['token_address']),
                        'quote':canonical_address(target['quote_asset_address']),'launch_log_index':target['launch_log_index'],
                        'lifecycle':'curve_ungraduated'})
    if len({f['launch_id'] for f in filters})!=len(filters):raise ValueError('Duplicate filter identity')
    return row,item,filters


def semantics(db, filt):
    target=db.target(filt['launch_id'])
    return json.dumps({'launch_block':target['launch_block'],'launch_log_index':target['launch_log_index'],
        'graduation_digest':hashlib.sha256((target['graduation_json'] or '').encode()).hexdigest()},
        sort_keys=True,separators=(',',':'))


def workload(db, item, filters, head):
    """Offline cost calculation against an explicitly supplied comparison endpoint."""
    if type(head) is not int or head<max(f['safe_start'] for f in filters):
        raise ValueError('Invalid workload endpoint')
    result=[]
    for f in filters:
        first=f['safe_start']
        proofs=compatible_ranges(db,f['launch_id'],f['kind'],f['query'],semantics(db,f),
                                 first,head,legacy_before=item['filter_snapshot_at'])
        merged=[]
        for r in proofs:
            low,high=max(first,r['first_block']),min(head,r['last_block'])
            if merged and low<=merged[-1][1]+1:merged[-1][1]=max(merged[-1][1],high)
            else:merged.append([low,high])
        missing=[];current=first
        for low,high in merged:
            if current<low:missing.append([current,low-1])
            current=max(current,high+1)
        if current<=head:missing.append([current,head])
        result.append({'launch_id':f['launch_id'],'kind':f['kind'],'reusable_proof':proofs,
            'completed_ranges':merged,'pending_ranges':missing,
            'filter_block_positions':sum(b-a+1 for a,b in missing),
            'estimated_getlogs':sum(math.ceil((b-a+1)/SHADOW_SPAN) for a,b in missing)})
    return result


def budget(runner, now):
    rpc=runner.worker.rpc;day=int(now)//86400*86400;minute=int(now)//60*60
    return {'utc_day':day,'getlogs_used':runner.db.used('flow_eth_getLogs',day),
        'rpc_used':runner.db.used('flow_rpc_members',day),'minute_used':runner.db.used('flow_rpc_members',minute),
        'safe_getlogs':max(0,rpc.settings.daily_getlogs-rpc.reserve-runner.db.used('flow_eth_getLogs',day)),
        'remaining_rpc':max(0,rpc.settings.daily_calls-runner.db.used('flow_rpc_members',day)),
        'remaining_minute':max(0,rpc.settings.minute_calls-runner.db.used('flow_rpc_members',minute))}


def seed_jobs(runner,item,filters,recovery):
    db=runner.db;stage=runner.stage(recovery['stage']);head=recovery['reconciliation_head']
    with db.conn:
        for f,work in zip(filters,workload(db,item,filters,head)):
            key=(stage,f['launch_id'],f['kind']);first=f['safe_start']
            exists=db.conn.execute('SELECT 1 FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',key).fetchone()
            db.conn.execute('''INSERT OR IGNORE INTO flow_shadow_jobs
              (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
               next_unverified_block,highest_contiguous_verified_block,span)
              VALUES(?,?,?,?,?,?,?,?)''',(*key,first,head,first,first-1,SHADOW_SPAN))
            job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',key).fetchone()
            if job['original_safe_start']!=first or job['reconciliation_upper_bound']!=head:
                raise ValueError('Pinned proof obligation changed')
            saved=db.conn.execute('SELECT query_json FROM flow_bootstrap_identity WHERE stage=? AND launch_id=? AND kind=?',key).fetchone()
            if saved and query_identity(json.loads(saved[0]))!=f['query']:
                raise ValueError('Pinned proof query changed')
            if exists:continue
            db.conn.execute('INSERT OR IGNORE INTO flow_bootstrap_identity VALUES(?,?,?,?,?)',
                (*key,json.dumps(f['query'],sort_keys=True,separators=(',',':')),recovery['reconciliation_head_timestamp']))
            db.conn.execute('INSERT OR IGNORE INTO flow_shadow_meta VALUES(?,?)',
                (stage+f':semantics:{f["launch_id"]}:{f["kind"]}',semantics(db,f)))
            for low,high in work['completed_ranges']:
                # Preserve original source evidence; only copy clipped proof into this obligation.
                if not db.conn.execute('SELECT 1 FROM flow_shadow_ranges WHERE stage=? AND launch_id=? AND kind=? AND first_block<=? AND last_block>=?',
                    (*key,low,high)).fetchone():
                    db.conn.execute('INSERT OR IGNORE INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)',(*key,low,high,int(high==head)))


def save_progress(runner,switch_id,attempt,stop=None):
    db=runner.db
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    item=switch.value(row);recovery=item['conservative_recovery']
    recovery['progress']=runner.summary(recovery['stage'])
    recovery['completed_ranges']=[dict(r) for r in db.conn.execute(
        'SELECT * FROM flow_shadow_ranges WHERE stage=? ORDER BY launch_id,kind,first_block,last_block',
        (runner.stage(recovery['stage']),))]
    recovery['pending_ranges']=recovery['progress']['unresolved_ranges']
    recovery['recovered_events']=recovery['progress']['recovered_events']
    recovery['duplicates']=recovery['progress']['duplicates']
    attempt.update(ended_at=time.time(),budget_after=budget(runner,time.time()),stop_reason=stop)
    recovery.setdefault('daily_attempts',[]).append(attempt)
    if stop:recovery.setdefault('budget_stop_reasons',[]).append(stop)
    persist(db,row,item)


def transport(db,row,now):
    connection=db.conn.execute('SELECT * FROM flow_provider_connections WHERE session_id IS ? '
                              'ORDER BY id DESC LIMIT 1',(row['session_id'],)).fetchone()
    if (switch.pending(db,row['session_id']) or db.state('connection_state')!='connected' or
        db.state('current_wss_provider')!='validation' or not connection or
        connection['provider']!='validation' or connection['disconnected_at'] is not None or
        not 0<=now-connection['last_seen_at']<=65 or
        not 0<=now-float(db.state('heartbeat',0))<=65):
        raise ValueError('Fresh matching Validation transport required')


def persist(db,row,item):
    with db.conn:
        result=db.conn.execute("UPDATE flow_provider_switches SET payload=? WHERE id=? AND state='FAILED' AND payload=?",
                              (json.dumps(item,sort_keys=True),row['id'],row['payload']))
        if result.rowcount!=1:raise ValueError('Failed switch changed concurrently')


async def recover(runner,switch_id,revision,handoff=False,unchanged=lambda: True):
    """Reuse one pinned plan and its chunks across waits; never resurrect expired targets."""
    db=runner.db;now=time.time()
    if len(revision)!=40 or any(c not in '0123456789abcdef' for c in revision):
        raise ValueError('Exact source revision required')
    if not runner.worker.settings.split_enabled:raise ValueError('Validation split required')
    # Operational recovery cannot loosen the established shared allowance/reserve.
    settings=runner.worker.rpc.settings
    runner.worker.rpc.settings=replace(settings,daily_calls=min(1000,settings.daily_calls),
        minute_calls=min(12,settings.minute_calls),daily_getlogs=min(400,settings.daily_getlogs))
    runner.worker.rpc.reserve=max(50,runner.worker.rpc.reserve)
    row,item,filters=plan(db,runner.worker.main,switch_id)
    transport(db,row,now)
    if not unchanged():raise ValueError('Service identity changed')
    recovery=item.get('conservative_recovery')
    allowance=budget(runner,now)
    required_rpc=1 if recovery else 4  # chain/head/header plus at least one proof request
    needs_rpc=not recovery or not runner.complete(recovery['stage'])
    if needs_rpc and (allowance['safe_getlogs']<1 or allowance['remaining_rpc']<required_rpc or allowance['remaining_minute']<required_rpc):
        if recovery:
            save_progress(runner,switch_id,{'started_at':now,'budget_before':allowance},'insufficient_current_budget')
        return {'gate':'RECOVERY_BUDGET_PAUSED','complete':False,'waiting_for_budget':True,'budget':allowance}
    if recovery:
        if recovery['filters']!=filters:raise ValueError('Pinned filter identity changed')
    else:
        # Validate identity and lower bounds before any RPC. Never synthesize frozen_head.
        raw=row['payload']
        history=item.get('failure_history') or {'state':'FAILED','payload':json.loads(raw),'payload_raw':raw,
                 'payload_sha256':hashlib.sha256(raw.encode()).hexdigest()}
        item['failure_history']=history
        try:
            capture=item.get('conservative_head_capture')
            if capture is None:
                await runner.worker.rpc.check_chain()
                head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
                if head<=0:raise ValueError('INVALID_RECONCILIATION_HEAD_BLOCKED')
                # Pin even if the following header call is interrupted/budget-paused.
                item['conservative_head_capture']={'head':head,'captured_at':time.time(),'revision':revision}
                persist(db,row,item)
                row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
            else:head=capture['head']
            header=await runner.worker.rpc.call('eth_getBlockByNumber',[hex(head),False])
        except BudgetWait as wait:
            item['conservative_budget_wait']={'scope':wait.scope,'used':wait.used,'limit':wait.limit,
                                              'retry_after':wait.reset_at or time.time()+30}
            persist(db,row,item)
            return {'gate':'RECOVERY_BUDGET_PAUSED','complete':False,'waiting_for_budget':True}
        floor=max([f['safe_start'] for f in filters]+
                  [int(f['cursor']) for f in item['filters'] if f.get('cursor') is not None]+
                  [int(item.get('last_old_block') or 0)])
        if (type(head) is not int or head<=0 or head<floor or not isinstance(header,dict) or
            int(header.get('number','0x0'),16)!=head or
            int(header.get('timestamp','0x0'),16)<max(item['failed_at'],item['first_disconnect_at'])):
            raise ValueError('INVALID_RECONCILIATION_HEAD_BLOCKED')
        recovery={'method':'conservative_later_head','original_frozen_head':None,
                  'reconciliation_head':head,'reconciliation_head_timestamp':int(header['timestamp'],16),
                  'captured_at':item['conservative_head_capture']['captured_at'],'revision':revision,'filters':filters,
                  'stage':f'provider_switch:{switch_id}:conservative','retry_after':0,
                  'switch_id':switch_id,'created_at':time.time(),
                  'reconciliation_head_utc':datetime.fromtimestamp(int(header['timestamp'],16),timezone.utc).isoformat(),
                  'display_addresses':{str(f['launch_id']):db.target(f['launch_id'])['curve_address'] for f in filters},
                  'canonical_comparison_identity':{str(f['launch_id']):f['query'] for f in filters},
                  'proof_obligations':workload(db,item,filters,head),'daily_attempts':[],
                  'budget_stop_reasons':[],'completed_at':None}
        item.update(failure_history=history,conservative_recovery=recovery)
        persist(db,row,item)
    stage=recovery['stage'];head=recovery['reconciliation_head']
    seed_jobs(runner,item,filters,recovery)
    def guard():
        current_row,current_item,current_filters=plan(db,runner.worker.main,switch_id)
        transport(db,current_row,time.time())
        if (not unchanged() or current_filters!=filters or
            current_item['conservative_recovery']['reconciliation_head']!=head):
            raise ValueError('Concurrent service/filter/recovery change')
    runner.worker.rpc.recovery_guard=guard
    try:
        complete=await runner.run_stage(stage)
    finally:
        runner.worker.rpc.recovery_guard=None
    wait=runner.pause_budget
    stop=wait.scope if isinstance(wait,BudgetWait) else None
    save_progress(runner,switch_id,{'started_at':now,'budget_before':allowance},stop)
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    item=switch.value(row);recovery=item['conservative_recovery']
    if not complete:
        wait=runner.pause_budget
        if isinstance(wait,BudgetWait):
            recovery['budget_wait']={'scope':wait.scope,'used':wait.used,'limit':wait.limit}
            recovery['retry_after']=wait.reset_at or time.time()+30
            persist(db,row,item)
        return {'gate':'RECOVERY_BUDGET_PAUSED' if isinstance(wait,BudgetWait) else 'RECOVERY_PROOF_PENDING',
                'complete':False,'proof':runner.summary(stage),'waiting_for_budget':isinstance(wait,BudgetWait)}
    if not handoff:return {'complete':True,'proof':runner.summary(stage),'handoff':False}
    if not unchanged():raise ValueError('Service identity changed')
    return finish(runner,switch_id,filters)


def finish(runner,switch_id,filters):
    db=runner.db;now=time.time()
    with db.conn:
        db.conn.execute('BEGIN IMMEDIATE')
        row,item,current=plan(db,runner.worker.main,switch_id)
        transport(db,row,now)
        recovery=item['conservative_recovery']
        if current!=filters or recovery['filters']!=filters:raise ValueError('Filter snapshot changed')
        stage=recovery['stage'];head=recovery['reconciliation_head']
        jobs={(j['launch_id'],j['kind']):dict(j) for j in db.conn.execute(
            'SELECT * FROM flow_shadow_jobs WHERE stage=?',(runner.stage(stage),))}
        if set(jobs)!={(f['launch_id'],f['kind']) for f in filters}:raise ValueError('Exact proof set required')
        proof=[]
        for f in filters:
            job=jobs[f['launch_id'],f['kind']];first=f['safe_start']
            identity=db.conn.execute('SELECT query_json FROM flow_bootstrap_identity WHERE stage=? AND launch_id=? AND kind=?',
                (runner.stage(stage),f['launch_id'],f['kind'])).fetchone()
            lifecycle=runner.meta(stage+f':semantics:{f["launch_id"]}:{f["kind"]}')
            if not identity or query_identity(json.loads(identity[0]))!=f['query'] or lifecycle!=semantics(db,f):
                raise ValueError('Final proof identity changed')
            if (job['original_safe_start']!=first or job['reconciliation_upper_bound']!=head or
                job['completion_status']!='complete' or job['next_unverified_block']!=head+1 or
                job['highest_contiguous_verified_block']!=head):raise ValueError('Incomplete superset proof')
            ranges=[dict(r) for r in db.conn.execute('SELECT first_block,last_block FROM flow_shadow_ranges '
                'WHERE stage=? AND launch_id=? AND kind=? ORDER BY first_block,last_block',
                (runner.stage(stage),f['launch_id'],f['kind']))]
            covered=first-1
            for r in ranges:
                if r['first_block']>covered+1 or r['first_block']<first or r['last_block']>head:
                    raise ValueError('Noncontiguous range proof')
                covered=max(covered,r['last_block'])
            if covered!=head:raise ValueError('Missing range proof')
            proof.append(dict(f,ranges=ranges))
        required={f['launch_id']:f['safe_start'] for f in filters}
        for gap_id in item['gap_ids']:
            gap=db.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gap_id,)).fetchone()
            if (not gap or gap['launch_id'] not in required or gap['reason']!='ws_gap' or
                gap['first_block'] is None or not required[gap['launch_id']]<=gap['first_block']<=head or
                gap['end_at']>recovery['reconciliation_head_timestamp']):raise ValueError('Gap outside superset proof')
        # Raw ingestion/dedupe and successful chunk evidence are durable before closure.
        summary=runner.summary(stage)
        recovery.update(proved_at=now,proof=proof,recovered_events=summary['recovered_events'],
                        duplicates=summary['duplicates'],unresolved_ranges=[],completed_at=now)
        item.update(recovery=recovery,resolved_gap_ids=item['gap_ids'],healthy_at=now)
        for gap_id in item['gap_ids']:db.conn.execute('UPDATE flow_gaps SET resolved=1 WHERE id=?',(gap_id,))
        incident={'PIT_COLLECTION_REGRESSION_START':item['first_disconnect_at'],
                  'PIT_COLLECTION_RECOVERY_END':None,'proof_reconciled_at':now,
                  'switch_id':switch_id,'original_start_block':None,'anchor_block':item.get('last_old_block'),
                  'reconciliation_head':head}
        db.conn.execute('INSERT INTO flow_state VALUES(?,?)',(f'pit_collection_incident:{switch_id}',json.dumps(incident)))
        db.conn.execute("UPDATE flow_provider_switches SET state='HEALTHY',payload=? WHERE id=? AND state='FAILED'",
                        (json.dumps(item,sort_keys=True),switch_id))
    return {'complete':True,'handoff':True,'recovery':recovery,'worker_runtime_untouched':True}
