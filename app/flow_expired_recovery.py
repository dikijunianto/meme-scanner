"""Exact, opt-in expired-current-gap proof. Never a live tracking lifecycle."""
import hashlib
import json
import math
import time

from app.flow_budget import BudgetWait
from app.flow_identity import compatible_ranges, query_identity, canonical_address
from app.flow_shadow import SHADOW_SPAN, ShadowReconciler
from app.flow_switch_recovery import semantics
from app.rpc import RpcError

MODE='EXPIRED_GAP_FORENSIC_RECOVERY'
CLASSES=('ws_gap','reconnect_recovery_incomplete','bootstrap_required:curve',
         'bootstrap_required:v4','bootstrap_required:hook')


def expired(target, now=None):
    return target['status'] in ('completed','partial') or target['tracking_end_at']+10<(time.time() if now is None else now)


def target_identity(target):
    return {**{k:canonical_address(target[k]) for k in
               ('token_address','quote_asset_address','curve_address','creator_address')},
            **{k:target[k] for k in ('launch_id','launch_block','launch_log_index','quote_decimals','tracking_start_at')}}


def periods(target, first, last):
    from app.flow_worker import filter_queries
    from app.flow_data import BUY,SELL
    graduation=json.loads(target['graduation_json']) if target['graduation_json'] else None
    queries=filter_queries(target)
    if graduation:queries['curve']={'address':target['curve_address'],'topics':[[BUY,SELL]]}
    for kind,query in sorted(queries.items()):
        base=graduation['block_number'] if graduation and kind!='curve' else target['launch_block']
        end=min(last,graduation['block_number']) if graduation and kind=='curve' else last
        if max(first,base)<=end:
            yield {'kind':kind,'query':query_identity(query),'first':max(first,base),'last':end}


def header_pair(gap, target, upper, following):
    """Two verified adjacent headers bracket the fixed uncertain end, not now."""
    try:
        number=int(upper['number'],16);at=int(upper['timestamp'],16)
        nxt=int(following['number'],16);later=int(following['timestamp'],16)
        hashes=[upper['hash'],following['hash'],following['parentHash']]
        if any(not isinstance(h,str) or len(h)!=66 or not h.startswith('0x') for h in hashes):raise ValueError()
        if any(int(h[2:],16)<0 for h in hashes):raise ValueError()
    except (ValueError,KeyError,TypeError):raise ValueError('Missing verified adjacent boundary headers') from None
    end=min(gap['end_at'],target['tracking_end_at'])
    if nxt!=number+1 or following['parentHash'].lower()!=upper['hash'].lower() or not at<=end<later:
        raise ValueError('Headers do not bracket the fixed gap end')
    if type(gap['first_block']) is not int or not 0<gap['first_block']<=number:
        raise ValueError('Missing exact safe lower boundary')
    return number


def retain_verified_bounds(db, gap_id, upper, following):
    """Only call after Validation independently verified these exact header numbers."""
    gap=dict(db.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gap_id,)).fetchone() or {})
    target=db.target(gap.get('launch_id'));epoch=db.epoch()
    if not epoch or epoch['status']!='ACTIVE' or not gap or gap['resolved'] or not target or not expired(target):
        raise ValueError('Unresolved expired CURRENT ACTIVE epoch gap required')
    if gap['reason'] not in CLASSES:raise ValueError('Unsupported gap class')
    last=header_pair(gap,target,upper,following)
    value={'epoch_id':epoch['epoch_id'],'gap':gap,'target_status':target['status'],
           'tracking_end_at':target['tracking_end_at'],'target_identity':target_identity(target),'lifecycle':semantics(db,gap),
           'filters':list(periods(target,gap['first_block'],last)),
           'upper_header':upper,'next_header':following}
    if not value['filters']:raise ValueError('Missing required filter interval')
    key=f'expired_gap_bounds:{gap_id}';prior=db.state(key)
    encoded=json.dumps(value,sort_keys=True,separators=(',',':'))
    if prior and json.loads(prior)!=value:raise ValueError('Forensic bounds are immutable')
    db.set_state(key,encoded)
    # Re-enqueue only this durable existing gap, never the expired target.
    from app.flow_gap_recovery import save
    save(db,gap,state='queued',mode=MODE,reason='verified_fixed_bounds',retry_at=0)
    return value


def bounds(db, gap):
    target=db.target(gap['launch_id']);epoch=db.epoch()
    if not epoch or epoch['status']!='ACTIVE' or not target or not expired(target):
        raise ValueError('Expired CURRENT ACTIVE epoch target required')
    encoded=db.state(f'expired_gap_bounds:{gap["id"]}')
    if not encoded:raise ValueError('missing_exact_expired_gap_bounds')
    value=json.loads(encoded)
    original=dict(db.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gap['id'],)).fetchone())
    last=header_pair(original,target,value['upper_header'],value['next_header'])
    if (original['reason'] not in CLASSES or
        value['epoch_id']!=epoch['epoch_id'] or value['gap']!=original or
        value['target_status']!=target['status'] or value['tracking_end_at']!=target['tracking_end_at'] or
        value['target_identity']!=target_identity(target) or
        value['lifecycle']!=semantics(db,original) or
        value['filters']!=list(periods(target,original['first_block'],last))):
        raise ValueError('Forensic epoch/gap/lifecycle/filter identity changed')
    if original['reason'].startswith('bootstrap_required:'):
        kind=original['reason'].split(':',1)[1]
        state=db.conn.execute('SELECT safe_start FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                             (gap['launch_id'],kind)).fetchone()
        filt=next((f for f in value['filters'] if f['kind']==kind),None)
        if not state or not filt or state['safe_start']!=filt['first']:
            raise ValueError('Required bootstrap safe boundary mismatch')
    return value


def uncovered(first,last,ranges):
    merged=[]
    for low,high in sorted((max(first,r['first_block']),min(last,r['last_block'])) for r in ranges):
        if low>high:continue
        if merged and low<=merged[-1][1]+1:merged[-1][1]=max(merged[-1][1],high)
        else:merged.append([low,high])
    missing=[];cursor=first
    for low,high in merged:
        if cursor<low:missing.append([cursor,low-1])
        cursor=high+1
    if cursor<=last:missing.append([cursor,last])
    return merged,missing


def coverage(db, gap, bound, filt):
    ranges=compatible_ranges(db,gap['launch_id'],filt['kind'],filt['query'],bound['lifecycle'],
                             filt['first'],filt['last'],
                             legacy_before=gap['end_at'] if not db.target(gap['launch_id'])['graduation_json'] else None)
    return uncovered(filt['first'],filt['last'],ranges)


def inventory(db):
    """Read-only; costs UNKNOWN for missing bounds, never fabricated zero."""
    rows=[];groups={}
    for row in db.conn.execute('SELECT * FROM flow_gaps WHERE resolved=0 ORDER BY id'):
        gap=dict(row);target=db.target(gap['launch_id'])
        item={'gap':gap,'target_status':target['status'] if target else None,
              'expired':expired(target) if target else None,'filters':[],
              'affected_pit_windows':[r[0] for r in db.conn.execute('SELECT window_seconds FROM flow_feature_versions WHERE launch_id=? AND feature_cutoff_at>=? GROUP BY window_seconds',(gap['launch_id'],gap['start_at']))]}
        try:
            bound=bounds(db,gap)
            for filt in bound['filters']:
                reused,missing=coverage(db,gap,bound,filt)
                item['filters'].append({**filt,'reused':reused,'missing':missing,
                     'missing_positions':sum(b-a+1 for a,b in missing)})
                key=json.dumps([bound['epoch_id'],gap['launch_id'],filt['kind'],filt['query'],bound['lifecycle']],sort_keys=True)
                groups.setdefault(key,[]).extend(missing)
            item['state']='bounded'
        except (ValueError,KeyError,TypeError) as exc:
            item.update(state='blocked',reason=str(exc),estimated_getlogs=None)
            if target and type(gap['first_block']) is int:
                # A retained prefix can be reported even when the exact end of
                # the obligation is absent. It cannot become a total-cost guess.
                upper=db.conn.execute('SELECT max(reconciliation_upper_bound) FROM flow_shadow_jobs WHERE launch_id=?',
                                      (gap['launch_id'],)).fetchone()[0]
                for filt in periods(target,gap['first_block'],max(gap['first_block'],upper or gap['first_block'])):
                    reused=compatible_ranges(db,gap['launch_id'],filt['kind'],filt['query'],semantics(db,gap),
                        gap['first_block'],upper or gap['first_block'],
                        legacy_before=gap['end_at'] if not target['graduation_json'] else None)
                    item['filters'].append({'kind':filt['kind'],'query':filt['query'],
                        'from_block':filt['first'],'to_block':None,'block_length':None,
                        'retained_compatible_proof':reused,'missing_positions':None})
        rows.append(item)
    unique=[]
    for identity,ranges in sorted(groups.items()):
        if not ranges:continue
        merged,_=uncovered(min(a for a,b in ranges),max(b for a,b in ranges),
                           [{'first_block':a,'last_block':b} for a,b in ranges])
        unique.extend({'identity':identity,'first':a,'last':b,'positions':b-a+1,
                       'estimated_getlogs':math.ceil((b-a+1)/SHADOW_SPAN)} for a,b in merged)
    known=sum(u['estimated_getlogs'] for u in unique);blocked=sum(r['state']=='blocked' for r in rows)
    return {'epoch_id':(db.epoch() or {}).get('epoch_id'),'raw_gap_count':len(rows),
        'target_count':len({r['gap']['launch_id'] for r in rows}),
        'expired_target_count':len({r['gap']['launch_id'] for r in rows if r['expired']}),
        'active_target_count':len({r['gap']['launch_id'] for r in rows if not r['expired']}),
        'gaps':rows,'unique_proof_obligations':unique,'bounded_missing_positions':sum(u['positions'] for u in unique),
        'blocked_gap_count':blocked,'remaining_unique_filter_block_positions':None if blocked else sum(u['positions'] for u in unique),
        'unique_proof_obligation_count':None if blocked else len(unique),
        'estimated_getlogs':None if blocked else known,
        'estimated_rpc_members_minimum':None if blocked else known,
        'cost_note':'Base minimum; retries, reductions and absent log timestamp headers add shared-budget RPC. Unknown bounds forbid total cost estimate.',
        'minimum_utc_budget_days':None if blocked else math.ceil(known/350),'reserve':50}


async def recover(worker, gap):
    """One low-priority exact proof chunk; no cursor/subscription/target mutation."""
    from app.flow_gap_recovery import save
    from app.flow_bootstrap import CursorBootstrap
    db=worker.db
    try:bound=bounds(db,gap)
    except (ValueError,KeyError,TypeError) as exc:
        save(db,gap,state='operator_blocked',mode=MODE,reason=str(exc));return False
    saved=json.loads(db.state(f'gap_recovery:{gap["id"]}','{}'))
    if saved.get('retry_at',0)>time.time():return False
    stage=f'expired_gap:{gap["id"]}';target=db.target(gap['launch_id'])
    original=worker.rpc;runner=ShadowReconciler(worker,reserve=50)
    try:
        save(db,gap,state='recovering',mode=MODE,last_attempt_at=time.time())
        # Existing immutable versions remain untouched. New forensic diagnostics
        # for this expired lifecycle can never become retrospective first eligible.
        db.set_state(f'expired_forensic_target:{gap["launch_id"]}',1)
        for filt in bound['filters']:
            key=(stage,gap['launch_id'],filt['kind'])
            runner.set_meta(stage+f':semantics:{gap["launch_id"]}:{filt["kind"]}',bound['lifecycle'])
            reused,missing=coverage(db,gap,bound,filt)
            with db.conn:
                db.conn.execute('''INSERT OR IGNORE INTO flow_shadow_jobs
                  (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,next_unverified_block,
                   highest_contiguous_verified_block,span) VALUES(?,?,?,?,?,?,?,?)''',
                  (*key,filt['first'],filt['last'],filt['first'],filt['first']-1,SHADOW_SPAN))
                db.conn.execute('INSERT OR IGNORE INTO flow_bootstrap_identity VALUES(?,?,?,?,?)',
                    (*key,json.dumps(filt['query'],sort_keys=True,separators=(',',':')),int(bound['upper_header']['timestamp'],16)))
                job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',key).fetchone()
                identity=db.conn.execute('SELECT query_json,upper_at FROM flow_bootstrap_identity WHERE stage=? AND launch_id=? AND kind=?',key).fetchone()
                if job['original_safe_start']!=filt['first'] or job['reconciliation_upper_bound']!=filt['last']:
                    raise ValueError('Forensic job bounds changed')
                if query_identity(json.loads(identity['query_json']))!=filt['query'] or identity['upper_at']!=int(bound['upper_header']['timestamp'],16):
                    raise ValueError('Forensic job provenance changed')
                # Fresh compatible proof from another overlapping obligation may
                # arrive between retries. Seed only holes, preserving old ranges.
                existing=[dict(r) for r in db.conn.execute('SELECT first_block,last_block FROM flow_shadow_ranges WHERE stage=? AND launch_id=? AND kind=?',key)]
                for first,last in reused:
                    _,holes=uncovered(first,last,existing)
                    for low,high in holes:
                        db.conn.execute('INSERT OR IGNORE INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)',(*key,low,high,int(high==filt['last'])))
        if not await runner.run_stage(stage,max_chunks=1):
            wait=runner.pause_budget
            save(db,gap,state='budget_wait' if wait else 'queued',mode=MODE,
                 reason=wait.scope if wait else 'exact_proof_pending',
                 retry_at=(wait.reset_at or time.time()+30) if wait else 0)
            return False
        jobs=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=?',(stage,)).fetchall()
        if {j['kind'] for j in jobs}!={f['kind'] for f in bound['filters']}:raise ValueError('Forensic filter set changed')
        for job in jobs:
            CursorBootstrap(runner)._verify_job(stage,job)
        # Recheck immutable binding after provider work; event commits precede proof.
        current=db.target(gap['launch_id'])
        if bounds(db,gap)!=bound or any(current[k]!=target[k] for k in
            ('status','tracking_start_at','tracking_end_at','completed_at','graduation_json','pool_id','current_phase')):
            raise ValueError('Expired lifecycle changed during proof')
        with db.conn:
            if gap['reason'].startswith('bootstrap_required:'):
                kind=gap['reason'].split(':',1)[1]
                filt=next(f for f in bound['filters'] if f['kind']==kind)
                # Only the proved expired bootstrap metadata; no live cursor,
                # subscriptions or unrelated gap rows are promoted.
                db.conn.execute("UPDATE flow_bootstrap SET status='complete',completed_head=?,completed_at=? WHERE launch_id=? AND kind=?",
                                (filt['last'],time.time(),gap['launch_id'],kind))
            db.conn.execute('UPDATE flow_gaps SET resolved=1 WHERE id=?',(gap['id'],))
            save(db,gap,state='resolved',mode=MODE,reason='exact_expired_interval_proved',completed_at=time.time())
        return True
    except (ValueError,RpcError) as exc:
        save(db,gap,state='operator_blocked',mode=MODE,reason=str(exc));return False
    finally:
        await worker.rpc.close();worker.rpc=original


async def tick(worker, debts, live_pending):
    """Fresh work first; deterministic oldest eligible debt, one chunk per tick."""
    from app.flow_gap_recovery import save
    items=[d for d in debts if d.get('mode')==MODE]
    for state in ('queued','recovering','budget_wait','operator_blocked'):
        name={'queued':'pending','recovering':'in_progress','budget_wait':'budget_wait','operator_blocked':'blocked'}[state]
        worker.db.set_state('expired_forensic_'+name,sum(d['state']==state for d in items))
    if live_pending:return
    for gap in sorted(items,key=lambda d:(d['start_at'],d['id'])):
        if gap['state']=='operator_blocked':continue
        saved=json.loads(worker.db.state(f'gap_recovery:{gap["id"]}','{}'))
        if saved.get('retry_at',0)>time.time():continue
        day=int(time.time())//86400*86400
        if worker.db.used('flow_eth_getLogs',day)>=worker.settings.daily_getlogs-50:
            save(worker.db,gap,state='budget_wait',mode=MODE,reason='daily_getlogs',retry_at=day+86400)
            continue
        await recover(worker,gap);break
