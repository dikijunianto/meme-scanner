"""Immutable current-segment gap bounds, derived only from retained capture evidence."""
import json
import re
import time

from app.flow_expired_recovery import periods,target_identity
from app.flow_switch_recovery import semantics


def failure(db,launch,start,end,reason,error):
    context=db.collection_context()
    key=f'unbounded_current_gap:{time.time_ns()}'
    db.conn.execute('INSERT INTO flow_state VALUES(?,?)',(key,json.dumps({
        'failure':'UNBOUNDED_CURRENT_GAP','epoch_id':context['epoch_id'],
        'segment_id':context['research_segment_id'],'launch_id':launch,'gap_class':reason,
        'start_at':start,'end_at':end,'created_at':time.time(),'reason':error},sort_keys=True)))


def build(db,launch,reason,first,last,provenance):
    context=db.collection_context();target=db.target(launch)
    if not target or not context.get('research_segment_id'):raise ValueError('Current segment target required')
    if type(first) is not int or type(last) is not int or not 0<first<=last:raise ValueError('Exact ordered required bounds missing')
    if not provenance:raise ValueError('Durable upper provenance missing')
    source=provenance['source'];identity=provenance['identity']
    if source=='shadow_job':
        job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',
                           (identity,launch,provenance['kind'])).fetchone()
        header=json.loads(db.conn.execute('SELECT value FROM flow_shadow_meta WHERE key=?',(identity+':header',)).fetchone()[0])
        if not job or job['original_safe_start']!=first or job['reconciliation_upper_bound']!=last:raise ValueError('Pinned job bounds mismatch')
        allowed=reason.startswith('bootstrap_required:') or reason=='epoch_activation_tail_required'
    elif source=='recovery_head':
        saved=json.loads(db.state(identity,'{}'));header=saved['header']
        allowed=reason in ('ws_gap','reconnect_recovery_incomplete','normal_recovery','provider_budget')
        if (saved['provider']!='validation' or saved['launch_id']!=launch or
            saved['segment_id']!=context['research_segment_id'] or first not in saved['lower_anchors'] or
            saved['required_head']!=last):
            raise ValueError('Recovery capture ownership/lower anchor mismatch')
    elif source=='provider_switch':
        from app.flow_provider_switch import value
        row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(int(identity),)).fetchone()
        saved=value(row);header=saved['boundary_header']
        allowed=(reason=='ws_gap' and saved['frozen_head']==last and first==saved['uncertain_from'] and
                 launch in {f['launch_id'] for f in saved['filters']})
    else:raise ValueError('Unsupported required gap class/source')
    head=int(header['number'],16)
    if int(header['timestamp'],16)>target['tracking_end_at']:
        raise ValueError('Capture does not prove the required expiry endpoint')
    clipped_curve=(source=='shadow_job' and provenance['kind']=='curve' and target['graduation_json'] and
                   last==json.loads(target['graduation_json'])['block_number'] and head>=last)
    if not allowed or (head!=last and not clipped_curve) or not re.fullmatch('0x[0-9a-fA-F]{64}',header.get('hash','')):
        raise ValueError('Required upper bound lacks matching capture provenance')
    filters=list(periods(target,first,last))
    if reason.startswith('bootstrap_required:') or source=='shadow_job':
        filters=[f for f in filters if f['kind']==provenance['kind']]
    if not filters:raise ValueError('Required filter identity missing')
    return {'epoch_id':context['epoch_id'],'segment_id':context['research_segment_id'],
        'launch_id':launch,'gap_class':reason,'required_from_block':first,'required_through_block':last,
        'lower_bound_provenance':{'source':source,'identity':identity,'block':first,
                                  'launch_block':target['launch_block']},
        'upper_bound_provenance':provenance,'upper_header':header,'filters':filters,
        'target_identity':target_identity(target),'tracking_end_at':target['tracking_end_at'],
        'lifecycle':semantics(db,{'launch_id':launch}),'created_at':time.time(),'recovery_state':'queued'}


def read(db,gap):
    value=json.loads(db.state(f'gap_contract:{gap["id"]}','null'))
    if value is None:raise ValueError('Required immutable gap contract missing')
    expected=build(db,gap['launch_id'],gap['reason'],gap['first_block'],value['required_through_block'],value['upper_bound_provenance'])
    for field in ('epoch_id','segment_id','launch_id','gap_class','required_from_block','required_through_block',
                  'lower_bound_provenance','upper_bound_provenance','upper_header','filters','target_identity','tracking_end_at','lifecycle'):
        if value[field]!=expected[field]:raise ValueError('Required gap contract identity changed: '+field)
    return value


def defer(db,target,start,end,reason,first):
    """Disconnect uncertainty is diagnostic until a reconnect head can be frozen."""
    context=db.collection_context();key=f'pending_uncertainty:{target["launch_id"]}:{time.time_ns()}'
    db.set_state(key,json.dumps({'segment_id':context['research_segment_id'],'launch_id':target['launch_id'],
        'start_at':start,'noticed_at':end,'reason':reason,'first_block':first,'state':'AWAITING_RECONNECT_HEAD'},sort_keys=True))


def bind_pending(db,target,identity):
    captured=json.loads(db.state(identity));head=int(captured['header']['number'],16);at=int(captured['header']['timestamp'],16)
    for row in db.conn.execute("SELECT key,value FROM flow_state WHERE key LIKE 'pending_uncertainty:%'").fetchall():
        value=json.loads(row['value'])
        if value['launch_id']!=target['launch_id'] or value['segment_id']!=db.collection_context()['research_segment_id']:continue
        if at>target['tracking_end_at']:
            with db.conn:failure(db,target['launch_id'],value['start_at'],value['noticed_at'],value['reason'],'Expiry precedes retained reconnect boundary')
            continue
        db.gap(target['launch_id'],value['start_at'],at,value['reason'],value['first_block'],through_block=head,
               provenance={'source':'recovery_head','identity':identity})
        with db.conn:db.conn.execute('DELETE FROM flow_state WHERE key=?',(row['key'],))


def pin_recovery(db,target,head):
    """Persist the block before a separately budgeted header call can pause."""
    launch=target['launch_id'];pointer=f'normal_recovery_capture_current:{launch}'
    if db.state(pointer):return db.state(pointer)
    graduation=json.loads(target['graduation_json']) if target['graduation_json'] else None
    anchors=[max(graduation['block_number'] if graduation and r[0].rsplit(':',1)[1]!='curve' else target['launch_block'],int(r[1])-2)
             for r in db.conn.execute("SELECT key,value FROM flow_state WHERE key LIKE ?",(f'recovery:{launch}:%',))]
    pending=[json.loads(r[0])['first_block'] for r in db.conn.execute("SELECT value FROM flow_state WHERE key LIKE ?",(f'pending_uncertainty:{launch}:%',))]
    key=f'normal_recovery_capture:{launch}:{time.time_ns()}'
    with db.conn:
        db.conn.execute('INSERT INTO flow_state VALUES(?,?)',(key,json.dumps({'launch_id':launch,'provider':'validation',
            'segment_id':db.collection_context()['research_segment_id'],'required_head':head,'header':None,
            'lower_anchors':sorted(set([target['launch_block']]+anchors+[x for x in pending if x is not None])),
            'captured_at':time.time()},sort_keys=True)))
        db.conn.execute('INSERT INTO flow_state VALUES(?,?)',(pointer,key))
    return key


async def capture_recovery(db,rpc,target):
    from app.rpc import RpcError
    key=db.state(f'normal_recovery_capture_current:{target["launch_id"]}')
    saved=json.loads(db.state(key))
    if saved['header'] is None:
        header=await rpc.call('eth_getBlockByNumber',[hex(saved['required_head']),False])
        if int(header['number'],16)!=saved['required_head'] or not re.fullmatch('0x[0-9a-fA-F]{64}',header.get('hash','')):
            raise RpcError('Fixed reconnect header mismatch')
        saved['header']=header;db.set_state(key,json.dumps(saved,sort_keys=True))
    bind_pending(db,target,key)
    return saved['required_head']
