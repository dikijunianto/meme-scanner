"""Durable WSS connection and provider-switch evidence for the split flow worker."""
import json
import time


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


def failed(db,switch_id,reason):
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
    if not row or row['id']!=switch_id:raise ValueError('Provider switch is not pending')
    item=value(row);item.update(failure=reason,failed_at=time.time())
    save(db,switch_id,'FAILED',item)
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
            'fallback_activation_count':sum(x['new_provider']=='validation' and x['new_connected_at'] is not None for x in switches),
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
                switches[-1].get('failover_reason') and switches[-1]['subscriptions_ready_at'] and
                switches[-1]['recovery'] is not None)
