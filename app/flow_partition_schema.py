"""Canonical proof schema and read-only contract for collector partitions."""
import sqlite3
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


def require_segment(db):
    from app.flow_segments import record
    if record(db) and (absent:=missing(db.conn)):
        raise SegmentSchemaIncomplete('SEGMENT_SCHEMA_INCOMPLETE: '+','.join(absent))


def repair(conn):
    """Add only missing canonical proof tables, atomically; never seed data."""
    if conn.in_transaction:raise ValueError('Schema repair requires its own transaction')
    before=missing(conn)
    conn.execute('BEGIN IMMEDIATE')
    try:
        create_shadow(conn)
        absent=missing(conn)
        if absent:raise ValueError('SEGMENT_SCHEMA_INCOMPLETE: '+','.join(absent))
        if conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise ValueError('Partition integrity failed')
        conn.commit()
    except BaseException:
        conn.rollback();raise
    return {'contract_version':VERSION,'added_tables':[t for t in SHADOW_TABLES if t in before],
            'complete':True,'proof_rows_seeded':0}
