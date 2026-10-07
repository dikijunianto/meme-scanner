"""Canonical proof schema and read-only contract for collector partitions."""
import sqlite3
import json
import math
import re
from pathlib import Path
from functools import lru_cache

VERSION = 1  # Derived contract version; never a persisted completeness flag.
MANDATORY_TABLES = frozenset((
    'flow_state','flow_tracking_targets','flow_events','flow_gaps','flow_features',
    'flow_samples','flow_bootstrap','flow_bootstrap_identity','flow_feature_ledger_start',
    'flow_feature_versions','flow_cutover_sessions','flow_cutover_legacy_proof',
    'flow_cutover_outcomes','flow_provider_connections','flow_provider_switches',
    'flow_shadow_meta','flow_shadow_jobs','flow_shadow_ranges'))
GLOBAL_TABLES = frozenset(('flow_usage','flow_collection_epochs','flow_epoch_global_ids','flow_research_segments'))
REQUIRED_OBJECTS = frozenset((
    'curve_trade_events','v4_swap_events','v4_hook_fee_events','flow_events_launch_time',
    'flow_events_block','flow_events_caller','flow_events_recipient','flow_events_sender',
    'flow_feature_versions_eligible','flow_feature_versions_no_update','flow_feature_versions_no_delete',
    'one_active_flow_cutover','one_active_flow_cutover_v2','one_active_flow_cutover_v3',
    'one_pending_provider_switch'))
SHADOW_DDL = (
    '''CREATE TABLE IF NOT EXISTS flow_shadow_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS flow_shadow_jobs(
      stage TEXT NOT NULL,launch_id INTEGER NOT NULL,kind TEXT NOT NULL,
      original_safe_start INTEGER NOT NULL,reconciliation_upper_bound INTEGER NOT NULL,
      next_unverified_block INTEGER NOT NULL,highest_contiguous_verified_block INTEGER NOT NULL,
      span INTEGER NOT NULL DEFAULT 2000,actual_getlogs_calls INTEGER NOT NULL DEFAULT 0,
      retries INTEGER NOT NULL DEFAULT 0,range_reductions INTEGER NOT NULL DEFAULT 0,
      recovered_raw_events INTEGER NOT NULL DEFAULT 0,duplicates_ignored INTEGER NOT NULL DEFAULT 0,
      failed_from INTEGER,failed_to INTEGER,failed_error TEXT,
      completion_status TEXT NOT NULL DEFAULT 'pending',
      PRIMARY KEY(stage,launch_id,kind))''',
    '''CREATE TABLE IF NOT EXISTS flow_shadow_ranges(
      stage TEXT NOT NULL,launch_id INTEGER NOT NULL,kind TEXT NOT NULL,
      first_block INTEGER NOT NULL,last_block INTEGER NOT NULL,was_terminal INTEGER NOT NULL,
      PRIMARY KEY(stage,launch_id,kind,first_block,last_block))''')
SHADOW_TABLES = ('flow_shadow_meta','flow_shadow_jobs','flow_shadow_ranges')


def create_shadow(conn):
    """Caller owns the transaction; no executescript implicit commit."""
    for statement in SHADOW_DDL:conn.execute(statement)


@lru_cache(maxsize=1)
def shadow_columns():
    expected=sqlite3.connect(':memory:')
    try:
        create_shadow(expected)
        return {t:[tuple(r) for r in expected.execute('PRAGMA table_info('+t+')')] for t in SHADOW_TABLES}
    finally:expected.close()


def missing(conn,namespace='main'):
    if namespace not in ('main','flow'):raise ValueError('Unsupported schema namespace')
    objects={r[0] for r in conn.execute('SELECT name FROM '+namespace+'.sqlite_master')}
    tables={r[0] for r in conn.execute("SELECT name FROM "+namespace+".sqlite_master WHERE type='table'")}
    result=sorted((MANDATORY_TABLES|REQUIRED_OBJECTS)-objects)
    result.extend(sorted((MANDATORY_TABLES & objects)-tables))
    # Existing wrong columns/defaults/PKs must not be mistaken for a complete migration.
    for table,columns in shadow_columns().items():
        if table in tables and [tuple(r) for r in conn.execute('PRAGMA '+namespace+'.table_info('+table+')')]!=columns:
            result.append(table+':incompatible_columns')
    return result


class SegmentSchemaIncomplete(ValueError):pass


def require_segment(db, *, integrity=False):
    from app.flow_segments import record
    if record(db) and (absent:=validate_startup_contract(db.conn, segment=record(db), epoch=db.epoch(),
            catalog_path=db.catalog_conn.execute('PRAGMA database_list').fetchone()[2],catalog_conn=db.catalog_conn,integrity=integrity)['failures']):
        raise SegmentSchemaIncomplete('SEGMENT_SCHEMA_INCOMPLETE: '+','.join(absent))


def initialize_metadata(conn):
    """Structural version belongs to canonical schema creation, never chain proof."""
    conn.execute("INSERT OR IGNORE INTO flow_state VALUES('schema_version','1')")


@lru_cache(maxsize=1)
def canonical_objects():
    from app.flow_data import FlowDB
    db=FlowDB(':memory:',follow_epoch=False)
    try:
        db.migrate(shared_budget=True)
        return {r['name']:{'type':r['type'],'sql':r['sql'],
                    'columns':[tuple(c) for c in db.conn.execute('PRAGMA table_xinfo('+r['name']+')')]
                    if r['type']=='table' else None,
                    'foreign_keys':[tuple(c) for c in db.conn.execute('PRAGMA foreign_key_list('+r['name']+')')]
                    if r['type']=='table' else None}
                for r in db.conn.execute("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL")
                if r['name'] in MANDATORY_TABLES|REQUIRED_OBJECTS}
    finally:db.close()


def _sql(value):
    # Preserve quoted literals: 'curve' and 'CURVE' are different view predicates.
    parts=re.findall(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|[^'\"]+",value or '')
    return ''.join(p if p[0] in ('\"',"'") else re.sub(r'\s+','',p).lower().replace('ifnotexists','') for p in parts)


def validate_startup_contract(conn, *, segment=None, epoch=None, catalog_path=None, catalog_conn=None, integrity=True):
    """One read-only local contract. Proof/connected health is a separate runtime gate."""
    failures=[]
    def fail(value):failures.append(value)
    try:
        objects={r[1]:(r[0],r[2]) for r in conn.execute('SELECT type,name,sql FROM sqlite_master')}
        for name,expected in sorted(canonical_objects().items()):
            actual=objects.get(name)
            if actual is None:fail('missing '+expected['type']+':'+name);continue
            if actual[0]!=expected['type']:fail('wrong object type:'+name);continue
            if _sql(actual[1])!=_sql(expected['sql']):fail('incompatible definition:'+name)
            if expected['type']=='table':
                if [tuple(c) for c in conn.execute('PRAGMA table_xinfo('+name+')')]!=expected['columns']:
                    fail('incompatible_columns:'+name)
                if [tuple(c) for c in conn.execute('PRAGMA foreign_key_list('+name+')')]!=expected['foreign_keys']:
                    fail('incompatible foreign keys:'+name)
        if 'flow_state' in objects and objects['flow_state'][0]=='table':
            version=conn.execute("SELECT value,typeof(value) FROM flow_state WHERE key='schema_version'").fetchall()
            if not version:fail('missing metadata:schema_version')
            elif len(version)!=1 or tuple(version[0])!=('1','text'):fail('wrong schema_version:value/type')
        if integrity and conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':fail('integrity_check')
        if integrity and conn.execute('PRAGMA foreign_key_check').fetchone():fail('foreign_key_check')
        if catalog_conn is not None and segment is not None:
            for table,columns in (('flow_usage',('minute','metric','count')),
                ('flow_epoch_global_ids',('kind','next_id')),
                ('flow_collection_epochs',('epoch_id','status','db_path')),
                ('flow_research_segments',('segment_id','epoch_id','status','db_path'))):
                found={c[1] for c in catalog_conn.execute('PRAGMA table_info('+table+')')}
                if not set(columns)<=found:fail('catalog missing columns:'+table)
            for kind in ('connection','switch'):
                row=catalog_conn.execute('SELECT next_id,typeof(next_id) FROM flow_epoch_global_ids WHERE kind=?',(kind,)).fetchone()
                if not row or row[1]!='integer' or row[0]<1:fail('catalog allocator:'+kind)
        if segment is not None:
            if not epoch or epoch['epoch_id']!=segment['epoch_id']:fail('segment epoch mismatch')
            if not epoch or epoch['status'] not in ('ACTIVATING','ACTIVE'):fail('closed parent epoch')
            if segment['status'] not in ('SEALED','ACTIVE','VALIDATED'):fail('segment state')
            if not re.fullmatch('[A-Za-z0-9_-]+',segment['segment_id']):fail('segment identity')
            actual_path=conn.execute('PRAGMA database_list').fetchone()[2]
            if Path(actual_path).resolve()!=Path(segment['db_path']).resolve():fail('segment partition mismatch')
            boundary=json.loads(segment['boundary_json']);h=boundary['header']
            if (boundary['provider']!='validation' or boundary['chain_id']!=4663 or
                int(h['number'],16)!=segment['start_block'] or int(h['timestamp'],16)!=segment['start_at'] or
                not re.fullmatch('0x[0-9a-fA-F]{64}',h['hash']) or not math.isfinite(segment['start_at'])):
                fail('boundary identity mismatch')
            if not re.fullmatch('[0-9a-f]{40}',segment['source_revision']):fail('boundary revision')
            state=dict(conn.execute("SELECT key,value FROM flow_state WHERE key IN ('epoch_catalog_path','phase2b_coverage_start_at')"))
            for key in ('epoch_catalog_path','phase2b_coverage_start_at'):
                if key not in state:fail('missing metadata:'+key)
            if 'epoch_catalog_path' in state and catalog_path is not None and Path(state['epoch_catalog_path']).resolve()!=Path(catalog_path).resolve():
                fail('catalog path mismatch')
            if 'phase2b_coverage_start_at' in state and float(state['phase2b_coverage_start_at'])!=segment['start_at']:
                fail('coverage boundary mismatch')
            ledger=conn.execute('SELECT start_at,start_block,deploy_revision FROM flow_feature_ledger_start WHERE id=1').fetchone()
            if not ledger or tuple(ledger)!=(segment['start_at'],segment['start_block'],segment['source_revision']):
                fail('ledger boundary mismatch')
            debt=json.loads(segment['preclean_debt_json'])
            if debt['epoch_id']!=segment['epoch_id'] or debt['classification']!='PRE_CLEAN_INCIDENT_DEBT':fail('preclean identity mismatch')
    except (sqlite3.Error,KeyError,ValueError,TypeError,OverflowError) as exc:
        fail('malformed local state:'+type(exc).__name__)
    failures=sorted(set(failures))
    return {'gate':'SEGMENT_SCHEMA_INCOMPLETE' if failures else 'SEGMENT_PARTITION_STARTUP_CONTRACT_PASS',
            'contract_version':VERSION,'complete':not failures,'failures':failures}


def repair(conn, *, segment=None, epoch=None, catalog_path=None, catalog_conn=None):
    """Atomic structural expansion only; preserve all chain/proof/operational rows."""
    if conn.in_transaction:raise ValueError('Schema repair requires its own transaction')
    before=missing(conn)
    conn.execute('BEGIN IMMEDIATE')
    try:
        create_shadow(conn)
        for name,obj in canonical_objects().items():
            # Table definitions must already be compatible; never silently ALTER data.
            conn.execute(obj['sql'].replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ',1)
                         .replace('CREATE INDEX ', 'CREATE INDEX IF NOT EXISTS ',1)
                         .replace('CREATE UNIQUE INDEX ', 'CREATE UNIQUE INDEX IF NOT EXISTS ',1)
                         .replace('CREATE TRIGGER ', 'CREATE TRIGGER IF NOT EXISTS ',1)
                         .replace('CREATE VIEW ', 'CREATE VIEW IF NOT EXISTS ',1)
                         if 'IF NOT EXISTS' not in obj['sql'].upper() else obj['sql'])
        initialize_metadata(conn)
        result=validate_startup_contract(conn,segment=segment,epoch=epoch,catalog_path=catalog_path,catalog_conn=catalog_conn)
        if not result['complete']:raise ValueError('SEGMENT_SCHEMA_INCOMPLETE: '+','.join(result['failures']))
        conn.commit()
    except BaseException:
        conn.rollback();raise
    return {**result,'added_tables':[t for t in SHADOW_TABLES if t in before],
            'proof_rows_seeded':0,'structural_metadata':{'schema_version':'1'}}
