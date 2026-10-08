"""RESEARCH_SEGMENT_ACTIVATION_EVIDENCE_CONTRACT: append-only evidence in flow_state."""
import hashlib,json,os,re,sqlite3,time,uuid
from types import SimpleNamespace
from app.flow_identity import query_identity
from app.rpc import RpcError

PREFIX='activation_v1:'

def load(db,key):
    value=db.state(PREFIX+key)
    return json.loads(value) if value is not None else None

def append(db,key,value):
    text=json.dumps(value,sort_keys=True,separators=(',',':'))
    prior=db.state(PREFIX+key)
    if prior is not None:
        if prior!=text:raise ValueError('Activation evidence identity changed')
        return
    db.conn.execute('INSERT INTO flow_state VALUES(?,?)',(PREFIX+key,text))

def pointer(db,key,value):
    db.conn.execute('INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                    (PREFIX+key,json.dumps(value)))

def session(worker,revision=None):
    from app.flow_segments import record
    segment=record(worker.db)
    if not segment:return None
    if revision is None:
        from app.flow_shadow import verified_checkout
        try:revision=verified_checkout()
        except RuntimeError as exc:raise ValueError('Activation startup checkout unverified') from exc
    if len(revision)!=40 or any(c not in '0123456789abcdef' for c in revision):raise ValueError('Startup revision required')
    identity=uuid.uuid4().hex
    value={'id':identity,'epoch_id':segment['epoch_id'],'segment_id':segment['segment_id'],
           'boundary_at':segment['start_at'],'boundary_block':segment['start_block'],
           'boundary_sha256':hashlib.sha256(segment['boundary_json'].encode()).hexdigest(),
           'provider':worker.ws_provider,'connection_id':worker.connection_id,'pid':os.getpid(),
           'revision':revision,'source_path':os.path.dirname(os.path.dirname(__file__)),
           'created_at':time.time()}
    with worker.db.conn:
        append(worker.db,'session:'+identity,value);pointer(worker.db,'current_session',identity)
        pointer(worker.db,'current_readiness',None)
    worker.activation_session=identity
    return identity

def ack(worker,target,kind,query,subscription,request_id,at):
    db=worker.db;sid=getattr(worker,'activation_session',None)
    if not sid or load(db,'current_session')!=sid:raise ValueError('Subscription lacks current activation session')
    if not isinstance(subscription,str) or not subscription:raise ValueError('Invalid subscription ACK identity')
    identity=f'{sid}:{request_id}'
    prior=load(db,'ack:'+identity)
    if prior:
        if (prior['subscription_id']!=subscription or prior['query']!=query_identity(query) or prior['acked_at']!=at):
            raise ValueError('Conflicting duplicate ACK')
        return prior
    value={'id':identity,'session_id':sid,'launch_id':target['launch_id'],'kind':kind,
           'query':query_identity(query),'subscription_id':subscription,'request_id':request_id,
           'acked_at':at,'created_at':time.time(),'provenance':'successful_eth_subscribe_response'}
    with db.conn:
        append(db,'ack:'+identity,value);pointer(db,f'ack_current:{sid}:{target["launch_id"]}:{kind}',identity)
        pointer(db,'current_readiness',None)
    return value

def required(worker,targets):
    from app.flow_bootstrap import required_filters
    from app.flow_segments import record
    from app.flow_switch_recovery import semantics
    from app.flow_shadow import ShadowReconciler
    segment=record(worker.db)
    members=[]
    for target in targets:
        # Required historical curve membership is retained after graduation.
        runner=object.__new__(ShadowReconciler);runner.worker=worker;runner.db=worker.db
        for kind,query,base,end in runner.periods(target,2**63-1):
            if kind in required_filters(worker,target):
                members.append({'launch_id':target['launch_id'],'kind':kind,'query':query_identity(query),
                                'base':max(base,segment['start_block']),
                                'lifecycle_end':None if end==2**63-1 else end,
                                'lifecycle':semantics(worker.db,{'launch_id':target['launch_id']})})
    return sorted(members,key=lambda v:(v['launch_id'],v['kind']))

async def readiness(worker,targets):
    db=worker.db;sid=getattr(worker,'activation_session',None)
    if not sid or load(db,'current_session')!=sid:return None
    members=required(worker,targets)
    if not members:return None  # An empty historical seal never authorizes primary PIT.
    acks=[]
    for member in members:
        aid=load(db,f'ack_current:{sid}:{member["launch_id"]}:{member["kind"]}')
        evidence=load(db,'ack:'+aid) if aid else None
        if not evidence or evidence['query']!=member['query']:return None
        acks.append(aid)
    digest=hashlib.sha256(json.dumps((members,acks),sort_keys=True).encode()).hexdigest()
    rid=sid+':'+digest;stage='research_tail:'+rid
    saved=load(db,'readiness:'+rid)
    if saved:
        with db.conn:pointer(db,'current_readiness',rid)
        return saved
    # Pin the head before the header call: budget interruption cannot move the bound.
    pin=load(db,'head:'+rid)
    if pin is None:
        raw_head=await worker.rpc.call('eth_blockNumber',[])
        try:head=int(raw_head,16)
        except (ValueError,TypeError):raise RpcError('Invalid readiness head') from None
        with db.conn:append(db,'head:'+rid,{'head':head,'captured_at':time.time()})
    else:head=pin['head']
    header=await worker.rpc.call('eth_getBlockByNumber',[hex(head),False])
    try:
        valid=(int(header['number'],16)==head and re.fullmatch('0x[0-9a-fA-F]{64}',header.get('hash',''))
               and int(header['timestamp'],16)<=time.time())
    except (KeyError,ValueError,TypeError):valid=False
    if not valid:raise RpcError('Readiness header identity mismatch')
    value={'id':rid,'session_id':sid,'members':members,'ack_ids':acks,'filter_set_hash':digest,
           'head':head,'header':header,'provider':'validation','ready_at':time.time(),'stage':stage}
    with db.conn:
        append(db,'readiness:'+rid,value);pointer(db,'current_readiness',rid)
        for key,val in ((stage+':head',head),(stage+':head_at',int(header['timestamp'],16)),
                        (stage+':ids',json.dumps(sorted({m['launch_id'] for m in members}))),
                        (stage+':header',json.dumps(header,sort_keys=True))):
            db.conn.execute('INSERT INTO flow_shadow_meta VALUES(?,?)',(key,str(val)))
    return value

def evidence(db, *, readiness_id=None):
    from app.flow_segments import record
    segment=record(db)
    if not segment:return {'complete':True,'activated_at':0,'id':None}
    rid=readiness_id or load(db,'current_readiness')
    ready=load(db,'readiness:'+rid) if rid else None
    sid=ready['session_id'] if readiness_id and ready else load(db,'current_session')
    ses=load(db,'session:'+sid) if sid else None;ready=load(db,'readiness:'+rid) if rid else None
    done=load(db,'complete:'+rid) if rid else None
    if not ses or not ready or not done:return {'complete':False,'reason':'activation_evidence_pending'}
    if (ready['session_id']!=sid or ses['segment_id']!=segment['segment_id'] or ses['epoch_id']!=segment['epoch_id']
        or ses['boundary_at']!=segment['start_at'] or ses['boundary_block']!=segment['start_block']
        or ses['boundary_sha256']!=hashlib.sha256(segment['boundary_json'].encode()).hexdigest()
        or ses['provider'] not in ('publicnode','validation') or type(ses['connection_id']) is not int
        or ready['provider']!='validation' or not ready['members']):return {'complete':False,'reason':'activation_identity_mismatch'}
    digest=hashlib.sha256(json.dumps((ready['members'],ready['ack_ids']),sort_keys=True).encode()).hexdigest()
    pin=load(db,'head:'+rid)
    try:meta=db.conn.execute('SELECT value FROM flow_shadow_meta WHERE key=?',(ready['stage']+':header',)).fetchone()
    except sqlite3.OperationalError:return {'complete':False,'reason':'activation_schema_incomplete'}
    if (ready['filter_set_hash']!=digest or rid!=sid+':'+digest or ready['stage']!='research_tail:'+rid or
        int(ready['header']['number'],16)!=ready['head'] or
        not re.fullmatch('0x[0-9a-fA-F]{64}',ready['header'].get('hash','')) or
        int(ready['header']['timestamp'],16)>ready['ready_at'] or not pin or pin['head']!=ready['head'] or
        pin['captured_at']>ready['ready_at'] or not meta or json.loads(meta[0])!=ready['header']):
        return {'complete':False,'reason':'activation_readiness_mismatch'}
    for member,aid in zip(ready['members'],ready['ack_ids']):
        a=load(db,'ack:'+aid)
        if (not a or a['session_id']!=sid or a['query']!=member['query'] or
            a['launch_id']!=member['launch_id'] or a['kind']!=member['kind'] or
            a['provenance']!='successful_eth_subscribe_response' or not a['subscription_id'] or
            not ses['created_at']<=a['acked_at']<=pin['captured_at']):
            return {'complete':False,'reason':'activation_ack_missing'}
    if len(ready['ack_ids'])!=len(ready['members']):return {'complete':False,'reason':'activation_filter_set_incomplete'}
    if not readiness_id:
        from app.flow_worker import filter_queries
        targets=[dict(r) for r in db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')")]
        if required(SimpleNamespace(db=db,filters=filter_queries),targets)!=ready['members']:
            return {'complete':False,'reason':'activation_filter_set_changed'}
    from app.flow_worker import filter_queries
    from app.flow_shadow import ShadowReconciler
    from app.flow_bootstrap import CursorBootstrap
    runner=object.__new__(ShadowReconciler);runner.db=db;runner.worker=SimpleNamespace(db=db,filters=filter_queries)
    try:
        for member in ready['members']:
            job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',
                (ready['stage'],member['launch_id'],member['kind'])).fetchone()
            expected_end=min(ready['head'],member['lifecycle_end'] or ready['head'])
            if not job or job['original_safe_start']!=member['base'] or job['reconciliation_upper_bound']!=expected_end:
                raise ValueError('Tail obligation changed')
            CursorBootstrap(runner)._verify_job(ready['stage'],job)
    except (ValueError,TypeError,KeyError,RpcError):
        return {'complete':False,'reason':'activation_tail_incomplete'}
    except sqlite3.OperationalError:
        return {'complete':False,'reason':'activation_schema_incomplete'}
    if (done['proved_at']<ready['ready_at'] or done['head']!=ready['head'] or
        done['filter_set_hash']!=ready['filter_set_hash'] or done['provider']!='validation' or done['stage']!=ready['stage']):
        return {'complete':False,'reason':'activation_completion_mismatch'}
    return {'complete':True,'id':rid,'activated_at':done['proved_at'],'session_id':sid}

async def drive(worker,targets,max_chunks=1):
    from app.flow_shadow import ShadowReconciler
    db=worker.db;original=worker.rpc;runner=ShadowReconciler(worker,reserve=50)
    try:
        ready=await readiness(worker,targets)
        if not ready:return False
        if evidence(db)['complete']:return True
        from app.flow_gap_contracts import bind_pending
        for target in targets:
            pending_rows=[json.loads(r[0]) for r in db.conn.execute(
                "SELECT value FROM flow_state WHERE key LIKE ?",(f'pending_uncertainty:{target["launch_id"]}:%',))]
            if pending_rows:
                anchors=sorted({p['first_block'] for p in pending_rows})
                if any(type(a) is not int or not target['launch_block']<=a<=ready['head'] for a in anchors):
                    raise ValueError('Pending uncertainty lacks a proven activation lower bound')
                key='reconnect:'+ready['id']+':'+str(target['launch_id'])
                with db.conn:append(db,key,{'provider':'validation','launch_id':target['launch_id'],
                    'segment_id':db.collection_context()['research_segment_id'],'lower_anchors':anchors,
                    'required_head':ready['head'],'header':ready['header']})
                bind_pending(db,target,PREFIX+key)
        # A restart preserves unfinished generations; a new ACK cannot move their head.
        generations=[json.loads(r[0]) for r in db.conn.execute(
            "SELECT value FROM flow_state WHERE key LIKE 'activation_v1:readiness:%' ORDER BY key")]
        generations=sorted(generations,key=lambda r:r['ready_at'])
        for old in generations:
            if load(db,'complete:'+old['id']) is not None or old['id']==ready['id']:continue
            if not await run_tail(runner,old,max_chunks):return False
            with db.conn:append(db,'complete:'+old['id'],completion(old))
        result=await run_tail(runner,ready,max_chunks)
        if not result:return False
        from app.flow_gap_recovery import obligations,recover
        for debt in obligations(db):
            if debt['state']!='operator_blocked':await recover(worker,db.target(debt['launch_id']),debt)
        from app.flow_provider_switch import pending,blocked
        if (pending(db) or blocked(db) or db.conn.execute('SELECT 1 FROM flow_gaps WHERE resolved=0 LIMIT 1').fetchone() or
            db.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'unbounded_current_gap:%' LIMIT 1").fetchone()):return False
        with db.conn:append(db,'complete:'+ready['id'],completion(ready))
        from app.flow_segments import record
        segment=record(db)
        if segment['status']=='SEALED':
            with db.catalog_conn:db.catalog_conn.execute("UPDATE flow_research_segments SET status='ACTIVE' WHERE segment_id=? AND status='SEALED'",(segment['segment_id'],))
        return evidence(db)['complete']
    finally:await worker.rpc.close();worker.rpc=original

def completion(ready):
    return {'proved_at':time.time(),'stage':ready['stage'],'provider':'validation',
            'head':ready['head'],'filter_set_hash':ready['filter_set_hash']}

async def run_tail(runner,ready,max_chunks):
    from app.flow_bootstrap import CursorBootstrap
    original=runner.periods
    for member in ready['members']:
        key=ready['stage']+f':semantics:{member["launch_id"]}:{member["kind"]}'
        prior=runner.db.conn.execute('SELECT value FROM flow_shadow_meta WHERE key=?',(key,)).fetchone()
        if prior and prior[0]!=member['lifecycle']:raise ValueError('Pinned tail lifecycle changed')
        with runner.db.conn:runner.db.conn.execute('INSERT OR IGNORE INTO flow_shadow_meta VALUES(?,?)',(key,member['lifecycle']))
    runner.periods=lambda target,head:((m['kind'],m['query'],m['base'],min(head,m['lifecycle_end'] or head))
        for m in ready['members'] if m['launch_id']==target['launch_id'])
    try:
        result=await CursorBootstrap(runner).run(ready['stage'],sorted({m['launch_id'] for m in ready['members']}),max_chunks=max_chunks)
        return result['gate']=='BOOTSTRAP_PROOF_COMPLETE'
    finally:runner.periods=original

def storage_preflight(db):
    """Exercise constraints/transactions on a private DB copy, never the live writer."""
    copy=sqlite3.connect(':memory:');db.conn.backup(copy)
    try:
        copy.execute('BEGIN IMMEDIATE')
        copy.execute('INSERT INTO flow_state VALUES(?,?)',(PREFIX+'local_probe','{}'))
        copy.execute('INSERT INTO flow_state VALUES(?,?)',(PREFIX+'local_filter_set',json.dumps({'members':[],'required_from_block':1,'required_through_block':2})))
        copy.execute('INSERT INTO flow_shadow_meta VALUES(?,?)',('activation_local_probe:head','2'))
        copy.execute('''INSERT INTO flow_shadow_jobs(stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
            next_unverified_block,highest_contiguous_verified_block,span,completion_status) VALUES(?,?,?,?,?,?,?,?,?)''',
            ('activation_local_probe',-1,'curve',1,2,1,0,2000,'pending'))
        copy.execute('INSERT INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)',('activation_local_probe',-1,'curve',1,2,1))
        try:copy.execute('INSERT INTO flow_state VALUES(?,?)',(PREFIX+'local_probe','{}'))
        except sqlite3.IntegrityError:pass
        else:raise ValueError('Activation storage lacks unique keys')
        copy.rollback()
        if copy.execute('SELECT 1 FROM flow_state WHERE key=?',(PREFIX+'local_probe',)).fetchone():raise ValueError('Activation rollback failed')
        if copy.execute("SELECT 1 FROM flow_shadow_jobs WHERE stage='activation_local_probe'").fetchone():raise ValueError('Tail rollback failed')
        evidence(db)
        return {'gate':'ACTIVATION_STORAGE_PREFLIGHT_PASS','schema_migration':'NO_PRODUCTION_SCHEMA_MIGRATION_REQUIRED','production_writes':0}
    finally:copy.close()

def joined_evidence(conn,epoch,segment,readiness_id):
    """The same verifier on an existing read-only joined audit transaction."""
    import re
    from pathlib import Path
    class Namespace:
        def __init__(self,name):self.name=name
        def execute(self,sql,params=()):
            return conn.execute(re.sub(r'\b(flow_[a-z_]+)\b',lambda m:self.name+'.'+m[1],sql),params)
    class ReadDB:
        def __init__(self):
            self.conn=Namespace('flow');self.catalog_conn=Namespace('collection');self.path=Path(segment['db_path'])
        def epoch(self):return epoch
        def state(self,key,default=None):
            row=self.conn.execute('SELECT value FROM flow_state WHERE key=?',(key,)).fetchone()
            return row[0] if row else default
        def target(self,launch):
            row=self.conn.execute('SELECT * FROM flow_tracking_targets WHERE launch_id=?',(launch,)).fetchone()
            return dict(row) if row else None
    return evidence(ReadDB(),readiness_id=readiness_id)
