"""Durable WSS connection and provider-switch evidence for the split flow worker."""
import json
import hashlib
import time

from app.flow_data import BUY,SELL


def pending(db,session_id=None):
    row=db.conn.execute("SELECT * FROM flow_provider_switches WHERE state NOT IN ('HEALTHY','FAILED') "
                        "AND session_id IS ? ORDER BY id DESC LIMIT 1",(session_id,)).fetchone()
    return dict(row) if row else None


def latest(db,session_id=None):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE session_id IS ? '
                        'ORDER BY id DESC LIMIT 1',(session_id,)).fetchone()
    return dict(row) if row else None


def blocked(db,session_id=None):
    row=latest(db,session_id)
    return bool(row and row['state']=='FAILED')


def value(row):
    return json.loads(row['payload']) if row else None


def connection_open(db,provider,now=None,session_id=None):
    now=time.time() if now is None else now
    with db.conn:
        cursor=db.conn.execute('INSERT INTO flow_provider_connections(session_id,provider,connected_at,last_seen_at) '
                               'VALUES(?,?,?,?)',(session_id,provider,now,now))
    return cursor.lastrowid


def connection_seen(db,connection_id,now=None):
    if connection_id is None:return
    now=time.time() if now is None else now
    with db.conn:db.conn.execute('UPDATE flow_provider_connections SET last_seen_at=? WHERE id=? '
                                 'AND disconnected_at IS NULL',(now,connection_id))


def connection_close(db,connection_id,now=None):
    if connection_id is None:return
    now=time.time() if now is None else now
    with db.conn:db.conn.execute('UPDATE flow_provider_connections SET last_seen_at=?,disconnected_at=? '
                                 'WHERE id=? AND disconnected_at IS NULL',(now,now,connection_id))


def start(db,old_provider,filters,gap_ids,*,session_id=None,ready_at=None,last_block=None,now=None):
    now=time.time() if now is None else now
    row=pending(db,session_id)
    if row:
        item=value(row)
        item['last_disconnect_at']=now
        item['disconnect_attempts']+=1
        save(db,row['id'],row['state'],item)
        return row['id'],item
    item={'old_provider':old_provider,'new_provider':None,'first_disconnect_at':now,
          'created_at':now,'filter_snapshot_at':now,
          'last_disconnect_at':now,'disconnect_attempts':1,
          'old_subscription_ready_at':ready_at,'last_old_block':last_block,
          'filters':filters,'gap_ids':gap_ids,'new_connected_at':None,
          'subscriptions_ready_at':None,'frozen_head':None,'uncertain_from':None,
          'uncertain_to':None,'recovery':None,'failure':None}
    with db.conn:
        cursor=db.conn.execute('INSERT INTO flow_provider_switches(session_id,state,payload) '
                               'VALUES(?,?,?)',(session_id,'PRIMARY_DISCONNECTED' if old_provider=='publicnode'
                                                  else 'FALLBACK_DISCONNECTED',json.dumps(item,sort_keys=True)))
    return cursor.lastrowid,item


def save(db,switch_id,state,item):
    with db.conn:
        db.conn.execute('UPDATE flow_provider_switches SET state=?,payload=? WHERE id=? '
                        "AND state NOT IN ('HEALTHY','FAILED')",
                        (state,json.dumps(item,sort_keys=True),switch_id))
        if db.conn.execute('SELECT changes()').fetchone()[0]!=1:
            raise ValueError('Provider switch changed concurrently')
    return item


def connected(db,switch_id,provider,connection_id,now=None):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['id']!=switch_id:raise ValueError('Provider switch is no longer pending')
    item=value(row);item.update(new_provider=provider,new_connected_at=time.time() if now is None else now,
                                new_connection_id=connection_id)
    save(db,switch_id,'VALIDATION_CONNECTED' if provider=='validation' else 'PRIMARY_CONNECTED',item)
    if not item['filters']:
        return complete_zero_filter(db,switch_id)
    return item


def complete_zero_filter(db,switch_id):
    """Transport establishes vacuous snapshot coverage, not an invented ACK/head."""
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    item=zero_filter_evidence(db,switch_id,value(row))
    return save(db,switch_id,'HEALTHY',item)


def zero_filter_evidence(db,switch_id,item):
    if (not item or item['filters']!=[] or item['gap_ids']!=[] or
        not item['new_connected_at'] or item['new_provider'] not in ('publicnode','validation') or
        item.get('recovery') is not None or
        any(item.get(k) is not None for k in ('subscriptions_ready_at','frozen_head',
                                             'uncertain_from','uncertain_to'))):
        raise ValueError('Unproved zero-filter transport or unexpected proof')
    for table in ('flow_shadow_jobs','flow_shadow_ranges'):
        row=db.conn.execute('SELECT session_id FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
        stage=f'provider_switch:{switch_id}'
        if (db.conn.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',('table',table)).fetchone()
            and db.conn.execute(f'SELECT 1 FROM {table} WHERE stage IN (?,?) LIMIT 1',
                                (stage,f'cutover:{row[0]}:{stage}')).fetchone()):
            raise ValueError('Zero-filter switch has range work')
    item.update(zero_filter_switch=True,no_subscriptions_required=True,
                subscriptions_required=0,recovery_required=False,unresolved_ranges=0,
                recovery={'calls':0,'recovered_events':0,'duplicates':0,
                          'unresolved_ranges':[],'zero_active_filters':True},
                resolved_gap_ids=[],healthy_at=time.time())
    return item


def reconcile_zero_filter_failure(db,switch_id,revision,now=None):
    """Preserve the failure and close empty coverage; runtime is worker-owned."""
    now=time.time() if now is None else now
    if len(revision)!=40 or any(c not in '0123456789abcdef' for c in revision):
        raise ValueError('Exact source revision required')
    with db.conn:
        db.conn.execute('BEGIN IMMEDIATE')
        row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
        if not row or row['state']!='FAILED' or latest(db,row['session_id'])['id']!=switch_id:
            raise ValueError('Latest failed switch required')
        item=value(row)
        connection=db.conn.execute('SELECT * FROM flow_provider_connections WHERE session_id IS ? '
                                   'ORDER BY id DESC LIMIT 1',(row['session_id'],)).fetchone()
        if (pending(db,row['session_id']) or db.state('connection_state')!='connected' or
            db.state('current_wss_provider')!=item['new_provider'] or
            not connection or connection['provider']!=item['new_provider'] or
            connection['disconnected_at'] is not None or
            not 0<=now-connection['last_seen_at']<=65 or
            not 0<=now-float(db.state('heartbeat',0))<=65):
            raise ValueError('Fresh matching transport required')
        original=json.dumps(item,sort_keys=True,separators=(',',':'))
        item['failure_history']={'state':'FAILED','payload':json.loads(original),
                                 'payload_sha256':hashlib.sha256(original.encode()).hexdigest()}
        item['recovery_after_failure']={'proved_at':now,'revision':revision,
            'zero_filter_snapshot':True,'current_connection_id':connection['id'],
            'current_provider':connection['provider'],'rpc_calls':0,
            'state_transitions':['FAILED','PROVIDER_SWITCH_RECOVERY','HEALTHY']}
        item=zero_filter_evidence(db,switch_id,item)
        # Both transitions commit atomically; an old worker cannot invent an ACK between them.
        db.conn.execute("UPDATE flow_provider_switches SET state='PROVIDER_SWITCH_RECOVERY',payload=? "
                        "WHERE id=? AND state='FAILED'",(json.dumps(item,sort_keys=True),switch_id))
        db.conn.execute("UPDATE flow_provider_switches SET state='HEALTHY' WHERE id=?",(switch_id,))
    return item


def failover_pending(db,switch_id,reason):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['state'] in ('HEALTHY','FAILED'):
        raise ValueError('Provider switch is not pending')
    item=value(row);item.update(new_provider='validation',failover_reason=reason,
                                failover_started_at=time.time())
    save(db,switch_id,'FAILOVER_PENDING',item)
    return item


def acknowledged(db,switch_id,now=None):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['id']!=switch_id or not value(row)['new_connected_at']:
        raise ValueError('Provider connection is not ready')
    item=value(row);item['subscriptions_ready_at']=time.time() if now is None else now
    save(db,switch_id,'FALLBACK_SUBSCRIPTIONS_READY' if item['new_provider']=='validation'
         else 'PRIMARY_SUBSCRIPTIONS_READY',item)
    return item


def frozen(db,switch_id,head,first,now=None):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['id']!=switch_id or not value(row)['subscriptions_ready_at']:
        raise ValueError('Provider subscriptions are not acknowledged')
    item=value(row);item.update(frozen_head=head,uncertain_from=first,uncertain_to=head,
                                head_at=time.time() if now is None else now,
                                stage=f'provider_switch:{switch_id}')
    save(db,switch_id,'PROVIDER_SWITCH_RECOVERY',item)
    return item


def healthy(db,switch_id,recovery,resolved_gap_ids):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['id']!=switch_id:raise ValueError('Provider switch is not pending')
    item=value(row)
    if recovery['unresolved_ranges'] or len(resolved_gap_ids)!=len(item['gap_ids']):
        raise ValueError('Provider switch has unproved ranges')
    item.update(recovery=recovery,resolved_gap_ids=resolved_gap_ids,healthy_at=time.time())
    save(db,switch_id,'HEALTHY',item)
    return item


def failed(db,switch_id,reason,*,connection_id=None,provider=None):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['id']!=switch_id:raise ValueError('Provider switch is not pending')
    item=value(row)
    if connection_id is not None and (item.get('new_connection_id')!=connection_id or
                                      item.get('new_provider')!=provider):
        return None
    item.update(failure=reason,failed_at=time.time())
    save(db,switch_id,'FAILED',item)
    return item


def resume_proved_failure(db,switch_id,revision,now=None):
    """Hand an already-proved failure back to the worker; never clear runtime state."""
    now=time.time() if now is None else now
    if len(revision)!=40 or any(c not in '0123456789abcdef' for c in revision):
        raise ValueError('Exact source revision required')
    with db.conn:
        db.conn.execute('BEGIN IMMEDIATE')
        row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
        if not row or row['state']!='FAILED' or latest(db,row['session_id'])['id']!=switch_id:
            raise ValueError('Latest failed provider switch required')
        item=value(row)
        if (pending(db,row['session_id']) or db.state('connection_state')!='connected' or
            db.state('current_wss_provider')!=item['new_provider'] or item['new_provider']!='validation' or
            not 0<=now-float(db.state('heartbeat',0))<=65):
            raise ValueError('Fresh matching Validation connection required')
        if db.conn.execute("SELECT 1 FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') LIMIT 1").fetchone():
            raise ValueError('Zero active targets required for failed-switch handoff')
        if not item.get('subscriptions_ready_at') or item.get('frozen_head') is None:
            raise ValueError('Frozen acknowledged switch proof required')
        stage=f'provider_switch:{switch_id}'
        jobs={(r['launch_id'],r['kind']):dict(r) for r in db.conn.execute(
            'SELECT * FROM flow_shadow_jobs WHERE stage=?',(stage,))}
        required={(f['launch_id'],f['kind']) for f in item['filters']}
        if not required or set(jobs)!=required:
            raise ValueError('Exact switch filter proof required')
        proof=[]
        for f in item['filters']:
            target=db.target(f['launch_id'])
            if (not target or f['kind']!='curve' or target['graduation_json'] or
                target['launch_block']!=f['base']):
                raise ValueError('Frozen curve filter identity changed')
            job=jobs[f['launch_id'],f['kind']]
            first=max(item['uncertain_from'],f['base']);head=item['frozen_head']
            if (job['original_safe_start']!=first or job['reconciliation_upper_bound']!=head or
                job['completion_status']!='complete' or job['next_unverified_block']!=head+1 or
                job['highest_contiguous_verified_block']!=head):
                raise ValueError('Switch filter has incomplete proof')
            ranges=[dict(r) for r in db.conn.execute('SELECT first_block,last_block FROM flow_shadow_ranges '
                'WHERE stage=? AND launch_id=? AND kind=? ORDER BY first_block,last_block',
                (stage,f['launch_id'],f['kind']))]
            covered=first-1
            for r in ranges:
                if r['first_block']>covered+1 or r['first_block']<first or r['last_block']>head:
                    raise ValueError('Switch range provenance is not contiguous')
                covered=max(covered,r['last_block'])
            if covered!=head:raise ValueError('Switch range provenance is incomplete')
            query={'address':target['curve_address'].lower(),'topics':[[BUY,SELL]]}
            proof.append({'launch_id':f['launch_id'],'kind':f['kind'],'query':query,'ranges':ranges})
        for gap_id in item['gap_ids']:
            gap=db.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gap_id,)).fetchone()
            if (not gap or gap['launch_id'] not in {f['launch_id'] for f in item['filters']} or
                gap['reason']!='ws_gap' or gap['first_block'] is None or
                not item['uncertain_from']<=gap['first_block']<=item['frozen_head'] or
                gap['end_at']>item['head_at']):
                raise ValueError('Switch gap identity changed')
        original=json.dumps(item,sort_keys=True,separators=(',',':'))
        item['failure_history']={'state':'FAILED','payload':json.loads(original),
                                 'payload_sha256':hashlib.sha256(original.encode()).hexdigest()}
        item['recovery_after_failure']={'proved_at':now,'revision':revision,'stage':stage,'proof':proof}
        db.conn.execute("UPDATE flow_provider_switches SET state='PROVIDER_SWITCH_RECOVERY',payload=? "
                        "WHERE id=? AND state='FAILED'",(json.dumps(item,sort_keys=True),switch_id))
    return item


def report(db,now=None,session_id=None):
    now=time.time() if now is None else now
    if not db.conn.execute("SELECT 1 FROM sqlite_master WHERE name='flow_provider_switches'").fetchone():
        return {'switches':[],'provider_switch_pending':False,'provider_switch_unresolved_ranges':0,
                'time_on_primary':0.0,'time_on_fallback':0.0,'primary_disconnect_count':0,
                'fallback_activation_count':0,'failback_count':0,'provider_switch_failed':False,
                'provider_flapping':False}
    rows=[dict(r) for r in db.conn.execute('SELECT * FROM flow_provider_switches WHERE session_id IS ? '
                                           'ORDER BY id',(session_id,))]
    switches=[dict(id=r['id'],session_id=r['session_id'],state=r['state'],**value(r)) for r in rows]
    duration={'publicnode':0.0,'validation':0.0}
    for row in db.conn.execute('SELECT provider,connected_at,last_seen_at,disconnected_at '
                               'FROM flow_provider_connections WHERE session_id IS ?',(session_id,)):
        if row['provider'] in duration:
            duration[row['provider']]+=max(0,(row['disconnected_at'] or row['last_seen_at'])-row['connected_at'])
    times=[x['first_disconnect_at'] for x in switches]
    flapping=any(sum(start<=at<start+1800 for at in times)>=4 for start in times)
    return {'switches':switches,'provider_switch_pending':any(r['state'] not in ('HEALTHY','FAILED') for r in rows),
            'provider_switch_failed':any(r['state']=='FAILED' for r in rows),
            'provider_flapping':flapping,
            'provider_switch_unresolved_ranges':sum(len(x.get('recovery',{}).get('unresolved_ranges',[]))
                                                    if x.get('recovery') else len(x['filters']) for x in switches
                                                    if x['state']!='HEALTHY'),
            'time_on_primary':duration['publicnode'],'time_on_fallback':duration['validation'],
            'primary_disconnect_count':db.conn.execute("SELECT count(*) FROM flow_provider_connections "
                "WHERE session_id IS ? AND provider='publicnode' AND disconnected_at IS NOT NULL",
                (session_id,)).fetchone()[0],
            'fallback_activation_count':sum(x['old_provider']=='publicnode' and x['new_provider']=='validation'
                                            and x['new_connected_at'] is not None for x in switches),
            'failback_count':sum(x['old_provider']=='validation' and x['new_provider']=='publicnode' and x['new_connected_at'] is not None
                                  for x in switches)}


def acceptable_route(db,session_id):
    """A proved Validation fallback is connected and healthy, not an error state."""
    if db.state('connection_state')!='connected' or db.state('service_status')!='connected':
        return False
    provider=db.state('current_wss_provider')
    evidence=report(db,session_id=session_id)
    if (provider not in ('publicnode','validation') or evidence['provider_switch_pending'] or
        evidence['provider_switch_failed'] or evidence['provider_switch_unresolved_ranges'] or
        evidence['provider_flapping']):
        return False
    if provider=='publicnode':return True
    switches=[x for x in evidence['switches'] if x['new_provider']=='validation']
    return bool(switches and switches[-1]['state']=='HEALTHY' and
                any(x['old_provider']=='publicnode' and x.get('failover_reason') for x in switches) and
                (switches[-1]['subscriptions_ready_at'] or switches[-1].get('no_subscriptions_required')) and
                switches[-1]['recovery'] is not None)
