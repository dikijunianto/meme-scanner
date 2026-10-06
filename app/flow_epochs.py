"""Collection partitions: historical proof is retained, never inherited.

The configured database remains the catalog, global ID and budget ledger.
Each new epoch has a fresh collector database with the existing schema.
Nothing is created by importing this module or opening a read-only database.
"""
import json
from pathlib import Path
import re
import sqlite3
import time


def exists(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE name='flow_collection_epochs'").fetchone() is not None


def schema(conn):
    if exists(conn):
        definition=conn.execute("SELECT sql FROM sqlite_master WHERE name='flow_collection_epochs'").fetchone()[0]
        index=conn.execute("SELECT sql FROM sqlite_master WHERE name='one_active_collection_epoch'").fetchone()
        if 'ACTIVATING' not in definition or (index and 'ACTIVATING' not in index[0]):
            raise ValueError('Existing epoch schema needs an explicit compatible migration')
    with conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('''CREATE TABLE IF NOT EXISTS flow_collection_epochs(
          epoch_id TEXT PRIMARY KEY,created_at_utc TEXT NOT NULL,start_block INTEGER NOT NULL,
          start_block_timestamp REAL NOT NULL,source_revision TEXT NOT NULL,reason TEXT NOT NULL,
          predecessor TEXT,status TEXT NOT NULL CHECK(status IN ('ACTIVATING','ACTIVE','CLOSED','QUARANTINED')),
          pit_eligible INTEGER NOT NULL CHECK(pit_eligible IN (0,1)),db_path TEXT NOT NULL UNIQUE,
          ended_at REAL,incident_json TEXT,boundary_json TEXT NOT NULL)''')
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_active_collection_epoch ON flow_collection_epochs((1)) WHERE status IN ('ACTIVATING','ACTIVE')")
        conn.execute('''CREATE TABLE IF NOT EXISTS flow_epoch_global_ids(
          kind TEXT PRIMARY KEY,next_id INTEGER NOT NULL)''')


def row(conn, path):
    if not exists(conn):return None
    value=conn.execute('SELECT * FROM flow_collection_epochs WHERE db_path=?',(str(Path(path).resolve()),)).fetchone()
    return dict(value) if value else None


def active_path(conn, default):
    if not exists(conn):return Path(default)
    value=conn.execute("SELECT db_path FROM flow_collection_epochs WHERE status IN ('ACTIVATING','ACTIVE')").fetchone()
    return Path(value[0]) if value else Path(default)


def allocate(db, kind):
    epoch=db.epoch()
    if epoch is None:return None  # Exact legacy behavior until explicit migration/registration.
    if epoch['status'] not in ('ACTIVATING','ACTIVE'):raise ValueError('Closed epoch cannot create current switches/connections')
    with db.catalog_conn:
        db.catalog_conn.execute('BEGIN IMMEDIATE')
        identity=db.catalog_conn.execute('SELECT next_id FROM flow_epoch_global_ids WHERE kind=?',(kind,)).fetchone()[0]
        db.catalog_conn.execute('UPDATE flow_epoch_global_ids SET next_id=next_id+1 WHERE kind=?',(kind,))
    return identity


def quarantine(db, *, epoch_id, start_block, start_at, revision, incident_at,
               switch_id, estimated_calls, reason):
    """Explicit offline registration; no original state, range or PIT writes."""
    from app.flow_data import iso
    if db.catalog_conn is not db.conn:raise ValueError('Open the historical database explicitly')
    if not reason or incident_at<=start_at or start_block<0 or not re.fullmatch('[0-9a-f]{40}',revision):
        raise ValueError('Retained historical boundaries and full revision required')
    schema(db.conn)
    with db.conn:
        db.conn.execute('BEGIN IMMEDIATE')
        failed=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(switch_id,)).fetchone()
        if not failed or failed['state']!='FAILED':raise ValueError('Historical failed switch required')
        payload=json.loads(failed['payload'])
        if payload['first_disconnect_at']!=incident_at:raise ValueError('Incident boundary must match retained switch')
        metadata={'switch_id':switch_id,'incident_start':incident_at,'incident_end':None,
                  'unresolved_ranges':len(payload['filters']), 'estimated_getlogs':estimated_calls,
                  'quarantine_reason':reason,'proof_completed':False}
        for kind,table in (('switch','flow_provider_switches'),('connection','flow_provider_connections')):
            next_id=db.conn.execute(f'SELECT coalesce(max(id),0)+1 FROM {table}').fetchone()[0]
            db.conn.execute('INSERT OR IGNORE INTO flow_epoch_global_ids VALUES(?,?)',(kind,next_id))
        existing=row(db.conn,db.path)
        if existing:
            if (existing['status'] in ('ACTIVATING','ACTIVE') and existing['epoch_id']==epoch_id and
                existing['start_block']==start_block and existing['start_block_timestamp']==start_at and
                existing['source_revision']==revision):
                db.conn.execute("UPDATE flow_collection_epochs SET status='QUARANTINED',pit_eligible=0,ended_at=?,incident_json=? WHERE epoch_id=?",
                                (incident_at,json.dumps(metadata,sort_keys=True),epoch_id))
                return row(db.conn,db.path)
            if (existing['epoch_id']!=epoch_id or existing['start_block']!=start_block or
                existing['start_block_timestamp']!=start_at or existing['ended_at']!=incident_at or
                existing['incident_json']!=json.dumps(metadata,sort_keys=True)):
                raise ValueError('Historical epoch registration changed')
            return existing
        db.conn.execute('INSERT INTO flow_collection_epochs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (epoch_id,iso(time.time()),start_block,start_at,revision,reason,None,'QUARANTINED',0,
             str(db.path.resolve()),incident_at,json.dumps(metadata,sort_keys=True),
             json.dumps({'retained_boundary':True,'literal_process_start_block':False},sort_keys=True)))
    return db.epoch()


async def prepare(runner, *, epoch_id, revision, reason, predecessor):
    """Offline operator primitive; fresh Validation RPC is budgeted by ShadowRpc.

ACTIVATING identifies current ownership without research eligibility. The worker seals eligibility only
after subscription ACKs and fresh bootstrap/tail proof. No old state is copied.
"""
    from app.flow_data import FlowDB, iso
    from app.flow_providers import provider
    catalog=runner.db
    if catalog.catalog_conn is not catalog.conn or not re.fullmatch('[A-Za-z0-9_-]+',epoch_id):
        raise ValueError('Historical catalog and simple epoch ID required')
    if runner.worker.settings.database.resolve()!=catalog.path.resolve():
        raise ValueError('Configured catalog must match the historical database')
    if not re.fullmatch('[0-9a-f]{40}',revision) or not reason:raise ValueError('Full revision and reason required')
    schema(catalog.conn)
    previous=catalog.conn.execute('SELECT * FROM flow_collection_epochs WHERE epoch_id=?',(predecessor,)).fetchone()
    if not previous or previous['status'] not in ('CLOSED','QUARANTINED'):
        raise ValueError('Explicitly closed predecessor required')
    if catalog.conn.execute("SELECT 1 FROM flow_collection_epochs WHERE status IN ('ACTIVATING','ACTIVE') OR epoch_id=?",(epoch_id,)).fetchone():
        raise ValueError('Epoch already exists; inspect and resume, never reset its boundary')
    rpc=runner.worker.rpc
    if (provider(rpc.config.rpc_http)!='validation' or rpc.reserve<50 or
        rpc.settings.daily_getlogs>400 or rpc.settings.daily_calls>1000 or rpc.settings.minute_calls>12):
        raise ValueError('Budgeted Validation HTTP with reserve >=50 required')
    captured=time.time()
    if int(await rpc.call('eth_chainId',[]),16)!=4663:raise ValueError('Wrong chain')
    head=int(await rpc.call('eth_blockNumber',[]),16)
    header=await rpc.call('eth_getBlockByNumber',[hex(head),False])
    at=int(header['timestamp'],16)
    if (int(header['number'],16)!=head or not re.fullmatch('0x[0-9a-fA-F]{64}',header.get('hash','')) or
        not 0<=time.time()-at<=60 or time.time()-captured>60):
        raise ValueError('Fresh matching boundary header required')
    path=catalog.path.with_name(catalog.path.stem+'.epoch-'+epoch_id+catalog.path.suffix)
    # Exclusive creation fails closed after interruption; never truncate an orphan.
    with path.open('xb'):pass
    fresh=FlowDB(path,follow_epoch=False)
    try:
        fresh.migrate()
        fresh.activate_pit_ledger(revision,head,at)
        fresh.set_state('schema_version',1)
        fresh.set_state('recovery_state','bootstrap_required')
        fresh.set_state('phase2b_coverage_start_at',at)
        fresh.set_state('epoch_catalog_path',catalog.path.resolve())
        with catalog.conn:
            catalog.conn.execute('BEGIN IMMEDIATE')
            catalog.conn.execute('INSERT INTO flow_collection_epochs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (epoch_id,iso(time.time()),head,at,revision,reason,predecessor,'ACTIVATING',0,
                 str(path.resolve()),None,None,json.dumps({'provider':'validation','chain_id':4663,
                   'captured_at':captured,'header':header,'proof_obligation':'fresh safe_start through fixed head, then acknowledged live tail'},sort_keys=True)))
        return dict(catalog.conn.execute('SELECT * FROM flow_collection_epochs WHERE epoch_id=?',(epoch_id,)).fetchone())
    finally:fresh.conn.close()


def sealed_proof_intact(db,seal):
    """Read-only verification of the retained activation proof, without RPC clients."""
    from types import SimpleNamespace
    from app.flow_bootstrap import CursorBootstrap
    from app.flow_shadow import ShadowReconciler
    from app.flow_worker import filter_queries
    from app.rpc import RpcError
    if not seal or not seal.get('zero_active_gaps') or 'filters' not in seal:return False
    runner=object.__new__(ShadowReconciler)
    runner.worker=SimpleNamespace(db=db,filters=filter_queries);runner.db=db;runner.session_id=None
    try:
        for filt in seal['filters']:
            tail=filt['acknowledged_tail']
            if tail and json.loads(db.state(f'epoch_tail_proof:{filt["launch_id"]}:{filt["kind"]}','null'))!=tail:
                return False
            for stage in [filt['bootstrap_stage'],*(tail['stages'] if tail else [])]:
                job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',
                                    (stage,filt['launch_id'],filt['kind'])).fetchone()
                if not job:return False
                CursorBootstrap(runner)._verify_job(stage,job)
    except (KeyError,TypeError,ValueError,RpcError):return False
    return True


def seal_live(worker):
    """Never treat an existing cursor or current COMPLETE flag as activation proof."""
    from app.flow_bootstrap import CursorBootstrap
    from app import flow_provider_switch as switch
    db=worker.db;epoch=db.epoch()
    if not epoch or epoch['status']!='ACTIVATING' or epoch['pit_eligible']:return False
    if (not worker.epoch_discovery_ready or db.state('connection_state')!='connected' or db.state('service_status')!='connected' or
        worker.pending_recovery or not worker.subscriptions_acknowledged() or
        db.conn.execute('SELECT 1 FROM flow_gaps WHERE resolved=0 LIMIT 1').fetchone() or
        switch.pending(db) or switch.blocked(db)):
        return False
    # Reuse the exact identity + contiguous-range verifier; fresh partitions
    # cannot contain the preceding epoch's jobs, cursors or gaps.
    proof=[]
    for target in db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"):
        target=dict(target)
        from app.flow_bootstrap import required_filters
        for kind in required_filters(worker,target):
            state=db.conn.execute('SELECT * FROM flow_bootstrap WHERE launch_id=? AND kind=?',(target['launch_id'],kind)).fetchone()
            minimum=(min(epoch['start_block'],json.loads(target['graduation_json'])['block_number'])
                     if kind=='curve' and target['graduation_json'] else epoch['start_block'])
            if not state or state['status']!='complete' or state['completed_head']<minimum:
                return False
            stage=('live_graduation' if target['graduation_json'] else 'live_bootstrap')+f':{target["launch_id"]}'
            job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',
                                (stage,target['launch_id'],kind)).fetchone()
            if not job or job['original_safe_start']!=state['safe_start']:return False
            # A verifier needs only periods and the database; no RPC client is constructed.
            from app.flow_shadow import ShadowReconciler
            runner=object.__new__(ShadowReconciler);runner.worker=worker;runner.db=db;runner.session_id=None
            CursorBootstrap(runner)._verify_job(stage,job)
            if int(db.state(f'recovery:{target["launch_id"]}:{kind}',-1))<job['reconciliation_upper_bound']:
                return False
            tail=None
            if kind in worker.filters(target):
                from app.flow_identity import query_identity
                tail=json.loads(db.state(f'epoch_tail_proof:{target["launch_id"]}:{kind}','null'))
                if (not tail or not tail.get('stages') or not tail['acknowledged'] or tail['subscription_ready_at'] is None or
                    tail['proved_at']<state['completed_at'] or
                    tail['subscription_ready_at']>tail['proved_at'] or
                    tail['first_block']>job['reconciliation_upper_bound']+1 or
                    tail['last_block']<tail['ready_head'] or
                    tail['ready_head']<job['reconciliation_upper_bound'] or
                    tail['subscription_ready_at']!=worker.subscription_ready_at or
                    tail['query']!=query_identity(worker.filters(target)[kind])):
                    return False
            if tail and tail.get('stages'):
                covered=job['reconciliation_upper_bound']
                for tail_stage in tail['stages']:
                    tail_job=db.conn.execute('SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=? AND kind=?',
                        (tail_stage,target['launch_id'],kind)).fetchone()
                    if not tail_job or tail_job['original_safe_start']>covered+1:return False
                    CursorBootstrap(runner)._verify_job(tail_stage,tail_job)
                    covered=max(covered,tail_job['reconciliation_upper_bound'])
                if covered<tail['ready_head']:return False
            proof.append({'launch_id':target['launch_id'],'kind':kind,'bootstrap_stage':stage,
                          'bootstrap_first':job['original_safe_start'],'bootstrap_head':job['reconciliation_upper_bound'],
                          'bootstrap_completed_at':state['completed_at'],'acknowledged_tail':tail})
    with db.catalog_conn:
        boundary=json.loads(epoch['boundary_json'])
        boundary['live_seal']={'proved_at':time.time(),'filters':proof,'zero_active_gaps':True}
        db.catalog_conn.execute("UPDATE flow_collection_epochs SET status='ACTIVE',pit_eligible=1,boundary_json=? WHERE epoch_id=? AND status='ACTIVATING'",
                                (json.dumps(boundary,sort_keys=True),epoch['epoch_id']))
    worker.dirty.update(r[0] for r in db.conn.execute('SELECT launch_id FROM flow_tracking_targets'))
    return True


def historical_debt(db):
    if not exists(db.catalog_conn):return []
    result=[]
    for epoch in db.catalog_conn.execute("SELECT * FROM flow_collection_epochs WHERE status NOT IN ('ACTIVATING','ACTIVE')"):
        record=dict(epoch);record['incident']=json.loads(record.pop('incident_json') or 'null')
        # Retained files are read only, including optional later forensic proof.
        conn=sqlite3.connect(Path(record['db_path']).as_uri()+'?mode=ro',uri=True)
        try:
            if record['incident']:
                saved=conn.execute('SELECT state FROM flow_provider_switches WHERE id=?',(record['incident']['switch_id'],)).fetchone()
                record['retained_switch_state']=saved[0] if saved else None
            record['immutable_versions']=conn.execute('SELECT count(*) FROM flow_feature_versions').fetchone()[0]
        finally:conn.close()
        result.append(record)
    return result
