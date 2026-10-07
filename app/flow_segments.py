"""Explicit research partitions within an operational epoch; never implicit rollover."""
import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path


def exists(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE name='flow_research_segments'").fetchone() is not None


def current(conn):
    if not exists(conn):return None
    cursor=conn.execute("SELECT * FROM flow_research_segments WHERE status IN ('SEALED','ACTIVE','VALIDATED')")
    row=cursor.fetchone()
    return dict(zip((c[0] for c in cursor.description),row)) if row else None


def record(db):
    row=current(db.catalog_conn)
    return row if row and Path(row['db_path']).resolve()==db.path.resolve() else None


def context(db):
    epoch=db.epoch();segment=record(db)
    if not segment:return epoch
    return {**epoch,'status':'ACTIVATING' if segment['status']=='SEALED' else 'ACTIVE',
            'pit_eligible':int(segment['status']!='SEALED'),'start_block':segment['start_block'],
            'start_block_timestamp':segment['start_at'],'source_revision':segment['source_revision'],
            'boundary_json':segment['boundary_json'],'research_segment_id':segment['segment_id']}


def debt_snapshot(db):
    rows=[dict(r) for r in db.conn.execute('SELECT * FROM flow_gaps WHERE resolved=0 ORDER BY id')]
    return {'classification':'PRE_CLEAN_INCIDENT_DEBT','epoch_id':db.epoch()['epoch_id'],
            'db_path':str(db.path.resolve()),'rows':rows,
            'sha256':hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
            'resolved':False,'upper_bounds_inferred':False}


async def prepare(runner, *, segment_id, revision, reason):
    """Future opt-in operation. Capture Validation boundary and publish SEALED ownership."""
    from app.flow_data import FlowDB,iso
    from app.flow_providers import provider
    db=runner.db;epoch=db.epoch();rpc=runner.worker.rpc
    if not epoch or epoch['status']!='ACTIVE' or current(db.catalog_conn):raise ValueError('One ACTIVE epoch without a research segment required')
    if not re.fullmatch('[A-Za-z0-9_-]+',segment_id) or not re.fullmatch('[0-9a-f]{40}',revision) or not reason:
        raise ValueError('Exact segment ID, full revision and reason required')
    if (provider(rpc.config.rpc_http)!='validation' or rpc.reserve<50 or rpc.settings.daily_getlogs>400 or
        rpc.settings.daily_calls>1000 or rpc.settings.minute_calls>12):raise ValueError('Budgeted Validation boundary required')
    key='segment_boundary_capture:'+segment_id
    from app.flow_budget import BudgetWait
    day=int(time.time())//86400*86400;used=db.used('flow_eth_getLogs',day)
    if used>rpc.settings.daily_getlogs-50:
        raise BudgetWait('daily_getlogs',used,rpc.settings.daily_getlogs-50,day+86400)
    saved=json.loads(db.state(key,'null'))
    if saved is None:
        if int(await rpc.call('eth_chainId',[]),16)!=4663:raise ValueError('Wrong chain')
        head=int(await rpc.call('eth_blockNumber',[]),16);captured=time.time()
        saved={'head':head,'captured_at':captured};db.set_state(key,json.dumps(saved))
    head=saved['head'];captured=saved['captured_at']
    header=await rpc.call('eth_getBlockByNumber',[hex(head),False]);at=int(header['timestamp'],16)
    if (int(header['number'],16)!=head or not re.fullmatch('0x[0-9a-fA-F]{64}',header.get('hash','')) or
        not 0<=time.time()-at<=60 or time.time()-captured>60):raise ValueError('Fresh matching fixed boundary required')
    debt=debt_snapshot(db)
    path=db.path.with_name(db.path.stem+'.segment-'+segment_id+db.path.suffix)
    with path.open('xb'):pass  # Never overwrite an interrupted partition.
    fresh=FlowDB(path,follow_epoch=False)
    try:
        fresh.migrate(shared_budget=True)
        from app.flow_partition_schema import validate_startup_contract
        if not validate_startup_contract(fresh.conn)['complete']:raise ValueError('SEGMENT_SCHEMA_INCOMPLETE before publication')
        fresh.activate_pit_ledger(revision,head,at)
        fresh.set_state('epoch_catalog_path',Path(db.catalog_conn.execute('PRAGMA database_list').fetchone()[2]).resolve())
        fresh.set_state('phase2b_coverage_start_at',at);fresh.set_state('recovery_state','bootstrap_required')
        with db.catalog_conn:
            db.catalog_conn.execute('BEGIN IMMEDIATE')
            db.catalog_conn.execute('''CREATE TABLE IF NOT EXISTS flow_research_segments(
                segment_id TEXT PRIMARY KEY,epoch_id TEXT NOT NULL,start_at REAL NOT NULL,start_block INTEGER NOT NULL,
                source_revision TEXT NOT NULL,reason TEXT NOT NULL,predecessor TEXT,status TEXT NOT NULL
                CHECK(status IN ('SEALED','ACTIVE','VALIDATED','CLOSED')),db_path TEXT NOT NULL UNIQUE,
                boundary_json TEXT NOT NULL,preclean_debt_json TEXT NOT NULL,validated_at REAL,first_pit_json TEXT)''')
            db.catalog_conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_current_research_segment ON flow_research_segments((1)) WHERE status IN ('SEALED','ACTIVE','VALIDATED')")
            if current(db.catalog_conn):raise ValueError('Research segment concurrently created')
            prospective={'segment_id':segment_id,'epoch_id':epoch['epoch_id'],'start_at':at,'start_block':head,
                'source_revision':revision,'status':'SEALED','db_path':str(path.resolve()),
                'boundary_json':json.dumps({'provider':'validation','chain_id':4663,'header':header}),
                'preclean_debt_json':json.dumps(debt)}
            check=validate_startup_contract(fresh.conn,segment=prospective,epoch=epoch,catalog_path=db.catalog_conn.execute('PRAGMA database_list').fetchone()[2],catalog_conn=db.catalog_conn)
            if not check['complete']:raise ValueError('SEGMENT_SCHEMA_INCOMPLETE: '+','.join(check['failures']))
            db.catalog_conn.execute('INSERT INTO flow_research_segments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (segment_id,epoch['epoch_id'],at,head,revision,reason,None,'SEALED',str(path.resolve()),
                 json.dumps({'provider':'validation','chain_id':4663,'captured_at':captured,'header':header,
                             'architecture':'SEALED_POSTSTART_BOOTSTRAP'},sort_keys=True),
                 json.dumps(debt,sort_keys=True),None,None))
        return current(db.catalog_conn)
    finally:fresh.close()


async def process_request(worker):
    """The live writer owns preparation; an operator only queues an exact request."""
    import os
    from app.flow_shadow import ShadowReconciler,verified_checkout
    from app.flow_budget import FlowBudget
    from app.rpc import RpcError
    db=worker.db
    row=db.catalog_conn.execute("SELECT value FROM flow_state WHERE key='research_segment_request'").fetchone()
    if not row:return
    request=json.loads(row[0])
    if request['status'] not in ('REQUESTED','BUDGET_WAIT') or request.get('retry_at',0)>time.time():return
    original=worker.rpc;runner=None
    try:
        if (request['flow_pid']!=os.getpid() or request['source_revision']!=verified_checkout() or
            request['epoch_id']!=db.epoch()['epoch_id']):raise ValueError('Requested process/checkout/epoch changed')
        runner=ShadowReconciler(worker,reserve=50)
        await prepare(runner,segment_id=request['segment_id'],revision=request['source_revision'],reason=request['reason'])
        worker.retired_segment_subscriptions.update(worker.routes)
        worker.routes={}  # Retire old-partition event writes before closing the temporary RPC client yields.
        request['status']='PREPARED'
    except FlowBudget as wait:
        request.update(status='BUDGET_WAIT',retry_at=wait.reset_at or time.time()+30)
    except (ValueError,RuntimeError,RpcError,OSError,sqlite3.Error) as exc:
        request.update(status='BLOCKED',failure=str(exc))
        db.set_state('research_segment_request_failure',str(exc))
    finally:
        if runner:await worker.rpc.close();worker.rpc=original
        with db.catalog_conn:db.catalog_conn.execute("UPDATE flow_state SET value=? WHERE key='research_segment_request'",(json.dumps(request,sort_keys=True),))


def validate_fresh(db,as_of=None):
    """Establish research start only from an actual immutable, post-seal PIT append."""
    from app.flow_epochs import sealed_proof_intact
    segment=record(db);now=time.time() if as_of is None else as_of
    if not segment or segment['status']=='SEALED' or db.current_health()!='healthy':return False
    seal=json.loads(segment['boundary_json']).get('live_seal')
    if not sealed_proof_intact(db,seal):return False
    for row in db.conn.execute('''SELECT v.*,t.launch_block,t.tracking_start_at FROM flow_feature_versions v
        JOIN flow_tracking_targets t USING(launch_id) WHERE version_number=1 AND feature_schema_version='v1'
        AND model_eligible_at IS NOT NULL ORDER BY model_eligible_at,launch_id,window_seconds'''):
        proof=json.loads(row['proof_json'])
        if (row['launch_block']<segment['start_block'] or row['tracking_start_at']<segment['start_at'] or
            row['feature_cutoff_at']<segment['start_at'] or row['materialized_at']<seal['proved_at'] or
            not row['materialized_at']<=row['model_eligible_at']<=now or
            proof.get('research_segment_id')!=segment['segment_id'] or row['coverage_quality']!='complete' or
            hashlib.sha256(row['payload'].encode()).hexdigest()!=row['payload_sha256']):continue
        filters=proof.get('filters',[])
        if not filters or not all(f.get('bootstrap_status')=='complete' and f.get('cursor') is not None and
                   f.get('completed_at') is not None and f['completed_at']<=row['model_eligible_at'] for f in filters):continue
        from app.flow_bootstrap import CursorBootstrap
        from app.flow_shadow import ShadowReconciler
        from app.rpc import RpcError
        from app.flow_worker import filter_queries
        from types import SimpleNamespace
        runner=object.__new__(ShadowReconciler);runner.db=db
        runner.worker=SimpleNamespace(db=db,filters=filter_queries)
        intact=True
        for filt in filters:
            state=db.conn.execute('SELECT * FROM flow_bootstrap WHERE launch_id=? AND kind=?',(row['launch_id'],filt['kind'])).fetchone()
            jobs=db.conn.execute("SELECT * FROM flow_shadow_jobs WHERE launch_id=? AND kind=? AND original_safe_start=? AND stage LIKE 'live_%' ORDER BY reconciliation_upper_bound DESC",
                (row['launch_id'],filt['kind'],state['safe_start'] if state else -1)).fetchall()
            valid=False
            for job in jobs:
                try:
                    _,upper_at=CursorBootstrap(runner)._verify_job(job['stage'],job)
                    if upper_at<=row['model_eligible_at']:valid=True;break
                except (RpcError,ValueError,TypeError):continue
            if not valid:intact=False;break
        if not intact:continue
        evidence={k:row[k] for k in ('launch_id','window_seconds','version_number','feature_cutoff_at',
                                   'materialized_at','model_eligible_at','payload_sha256')}
        if segment['status']=='VALIDATED':return True
        with db.catalog_conn:db.catalog_conn.execute("UPDATE flow_research_segments SET status='VALIDATED',validated_at=?,first_pit_json=? WHERE segment_id=? AND status='ACTIVE'",
            (now,json.dumps(evidence,sort_keys=True),segment['segment_id']))
        return True
    return False


def historical_debt(db):
    segment=record(db)
    if not segment:return None
    debt=json.loads(segment['preclean_debt_json'])
    conn=sqlite3.connect(Path(debt['db_path']).as_uri()+'?mode=ro',uri=True);conn.row_factory=sqlite3.Row
    try:
        retained=[dict(conn.execute('SELECT * FROM flow_gaps WHERE id=?',(r['id'],)).fetchone() or {}) for r in debt['rows']]
        digest=hashlib.sha256(json.dumps(retained,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        return {'classification':debt['classification'],'epoch_id':debt['epoch_id'],'gap_count':len(retained),
                'original_rows_unchanged':digest==debt['sha256'],'unresolved':sum(not r.get('resolved',True) for r in retained),
                'upper_bound':None,'proof_complete':False}
    finally:conn.close()
