"""Durable, explicit Phase 2B.2 cutover sessions."""
import hashlib
import json
from pathlib import Path
import time
from datetime import datetime, timezone
from uuid import uuid4

KEY = 'phase2b2_cutover_session'  # Old singleton; never rewritten.
PENDING = {'SOURCE_STOPPED', 'STOP_TAIL_VERIFIED', 'SPLIT_CONFIGURED',
           'SPLIT_WSS_CONNECTING', 'SUBSCRIPTIONS_READY',
           'READY_TAIL_PENDING'}
PRE_STOP = {'CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED','STOP_AUTHORIZED'}
ACTIVE = {*PRE_STOP, *PENDING,
          'READY_TAIL_VERIFIED', 'POST_CUTOVER_VALIDATING', 'SOAKING'}
TERMINAL = {'COMPLETE', 'FAILED', 'ROLLED_BACK', 'ABORTED_PRE_STOP', 'ARCHIVED'}


def source_process_gone(pid):
    """An inactive unit is insufficient if the bound process survived elsewhere."""
    return not (Path('/proc') / str(pid)).exists()


def utc(at=None):
    return datetime.fromtimestamp(at if at is not None else time.time(), timezone.utc).isoformat()


def schema(db):
    """Additive registry; old shadow proof tables are left untouched."""
    with db.conn:
        db.conn.execute('''CREATE TABLE IF NOT EXISTS flow_cutover_sessions(
        id TEXT PRIMARY KEY,created_at_utc TEXT,deploy_git_revision TEXT NOT NULL,
        source_route TEXT NOT NULL,source_legacy_pid TEXT NOT NULL,
        source_legacy_start_time TEXT,split_role_fingerprint TEXT,
        status TEXT NOT NULL,terminal_reason TEXT,archived_at_utc TEXT,
        split_pid TEXT,rollback_pid TEXT,payload TEXT NOT NULL)''')
        db.conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_active_flow_cutover
        ON flow_cutover_sessions((1)) WHERE status IN
        ('CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED','STOP_TAIL_VERIFIED',
         'SPLIT_WSS_CONNECTING','SUBSCRIPTIONS_READY','READY_TAIL_PENDING',
         'READY_TAIL_VERIFIED')''')
        db.conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_active_flow_cutover_v2
        ON flow_cutover_sessions((1)) WHERE status IN
        ('CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED','STOP_AUTHORIZED',
         'SOURCE_STOPPED','STOP_TAIL_VERIFIED','SPLIT_CONFIGURED',
         'SPLIT_WSS_CONNECTING','SUBSCRIPTIONS_READY','READY_TAIL_PENDING',
         'READY_TAIL_VERIFIED')''')
        db.conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_active_flow_cutover_v3
        ON flow_cutover_sessions((1)) WHERE status IN
        ('CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED','STOP_AUTHORIZED',
         'SOURCE_STOPPED','STOP_TAIL_VERIFIED','SPLIT_CONFIGURED',
         'SPLIT_WSS_CONNECTING','SUBSCRIPTIONS_READY','READY_TAIL_PENDING',
         'READY_TAIL_VERIFIED','POST_CUTOVER_VALIDATING','SOAKING')''')
        db.conn.execute('''CREATE TABLE IF NOT EXISTS flow_cutover_legacy_proof(
          session_id TEXT NOT NULL,proof_table TEXT NOT NULL,stage TEXT NOT NULL,
          launch_id INTEGER NOT NULL,kind TEXT NOT NULL,first_block INTEGER NOT NULL,
          last_block INTEGER NOT NULL,
          PRIMARY KEY(proof_table,stage,launch_id,kind,first_block,last_block))''')
        db.conn.execute('''CREATE TABLE IF NOT EXISTS flow_cutover_outcomes(
          session_id TEXT PRIMARY KEY,recorded_at_utc TEXT NOT NULL,
          handoff_completed INTEGER NOT NULL,later_operational_rollback INTEGER NOT NULL,
          rollback_reason TEXT NOT NULL,rollback_pid TEXT NOT NULL,
          rollback_route TEXT NOT NULL,provider_at_failure TEXT NOT NULL,
          source_pid TEXT NOT NULL,split_pid TEXT,proof_payload_sha256 TEXT NOT NULL,
          reconciliation_proof TEXT NOT NULL)''')
        db.conn.execute('''CREATE TABLE IF NOT EXISTS flow_provider_connections(
          id INTEGER PRIMARY KEY,session_id TEXT,provider TEXT NOT NULL,connected_at REAL NOT NULL,
          last_seen_at REAL NOT NULL,disconnected_at REAL)''')
        db.conn.execute('''CREATE TABLE IF NOT EXISTS flow_provider_switches(
          id INTEGER PRIMARY KEY,session_id TEXT,state TEXT NOT NULL,payload TEXT NOT NULL)''')
        db.conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_pending_provider_switch
          ON flow_provider_switches((1)) WHERE state NOT IN ('HEALTHY','FAILED')''')


def _exists(db):
    return bool(db.conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flow_cutover_sessions'").fetchone())


def _row(db, where, args=()):
    if not _exists(db):return None
    row=db.conn.execute('SELECT * FROM flow_cutover_sessions WHERE '+where,args).fetchone()
    return dict(row) if row else None


def current(db):
    return _row(db, "status IN ('CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED',"
                "'STOP_AUTHORIZED','SOURCE_STOPPED','STOP_TAIL_VERIFIED',"
                "'SPLIT_CONFIGURED','SPLIT_WSS_CONNECTING','SUBSCRIPTIONS_READY',"
                "'READY_TAIL_PENDING','READY_TAIL_VERIFIED','POST_CUTOVER_VALIDATING','SOAKING')")


def latest_terminal(db):
    return _row(db, "status IN ('COMPLETE','FAILED','ROLLED_BACK','ABORTED_PRE_STOP','ARCHIVED') "
                "ORDER BY rowid DESC LIMIT 1")


def session(db):
    row=current(db) or latest_terminal(db)
    return json.loads(row['payload']) if row else None


def status(db):
    """Read-only; stale terminal revisions never block inspection."""
    outcomes={r['session_id']:dict(r) for r in db.conn.execute('SELECT * FROM flow_cutover_outcomes')} \
        if db.conn.execute("SELECT 1 FROM sqlite_master WHERE name='flow_cutover_outcomes'").fetchone() else {}
    def view(row):
        if not row:return None
        value=json.loads(row['payload'])
        return {**{key:row[key] for key in ('id','created_at_utc','deploy_git_revision',
            'source_route','source_legacy_pid','source_legacy_start_time',
            'split_role_fingerprint','status','terminal_reason','archived_at_utc',
            'split_pid','rollback_pid')},
            'H_prefetch':value.get('H_prefetch'),'H_pre_stop':value.get('H_pre_stop'),
            'H_stop':value.get('H_stop'),'H_live':value.get('H_live'),
            'target_filters':value.get('targets',[]),
            'shadow_proof':value.get('shadow_proof'),
            'stop_tail_proof':value.get('stop_tail_proof'),
            'subscription_ack':value.get('subscription_ack'),
            'ready_tail_proof':value.get('ready_tail_proof'),
            'rollback_state':value.get('rollback_state'),
            'legacy_proof_digest':value.get('legacy_proof_digest'),
            'legacy_jobs':value.get('legacy_jobs'),
            'legacy_ranges':value.get('legacy_ranges'),
            'stop_authorized_at':value.get('stop_authorized_at'),
            'stop_command_issued_at':value.get('stop_command_issued_at'),
            'source_stopped_at':value.get('source_stopped_at'),
            'candidate_config_fingerprint':value.get('candidate_config_fingerprint'),
            'session_phase':row['status'],
            'operational_outcome':outcomes.get(row['id']),
            'handoff_completed':value.get('ready_tail_proof')=='verified',
            'validation_started_at':value.get('validation_started_at'),
            'soak_started_at':value.get('soak_started_at'),
            'authorization':value.get('authorization')}
    history=[view(dict(row)) for row in db.conn.execute(
        "SELECT * FROM flow_cutover_sessions WHERE status IN "
        "('COMPLETE','FAILED','ROLLED_BACK','ABORTED_PRE_STOP','ARCHIVED') ORDER BY rowid DESC"
    )] if _exists(db) else []
    active=view(current(db))
    from app.flow_provider_switch import report as switch_report
    switches=switch_report(db,session_id=(active or (history[0] if history else {})).get('id'))
    return {'historical_latest_terminal':history[0] if history else None,
            'historical_sessions':history,'current_active':active,
            'connection_state':db.state('connection_state'),
            'current_wss_provider':db.state('current_wss_provider'),
            'primary_provider':'publicnode','fallback_provider':'validation',
            'provider_switch':switches,
            **{key:switches[key] for key in ('primary_disconnect_count','fallback_activation_count',
                'failback_count','provider_switch_pending','provider_switch_unresolved_ranges',
                'time_on_primary','time_on_fallback')}}


def legacy_digest(db):
    stages=('historical','stop_tail','wss_ready_tail','rollback_tail',
            'rollback_restart_tail','failed_cutover_cleanup_tail')
    args=','.join('?' for _ in stages)
    jobs=[dict(r) for r in db.conn.execute(
        f'SELECT * FROM flow_shadow_jobs WHERE stage IN ({args}) ORDER BY stage,launch_id,kind',stages)]
    ranges=[dict(r) for r in db.conn.execute(
        f'SELECT * FROM flow_shadow_ranges WHERE stage IN ({args}) '
        'ORDER BY stage,launch_id,kind,first_block,last_block',stages)]
    proof=json.dumps({'jobs':jobs,'ranges':ranges},sort_keys=True,separators=(',',':'))
    return hashlib.sha256(proof.encode()).hexdigest(),len(jobs),len(ranges)


def import_rolled_back_legacy(db, current_pid):
    """One explicit, proof-backed import; no old metadata or proof is updated."""
    schema(db)
    meta=dict(db.conn.execute("SELECT key,value FROM flow_shadow_meta WHERE key IN "
                              "('git_revision','old_flow_pid','H_prefetch','H_pre_stop','H_stop','H_stop_at','H_live')"))
    if not {'git_revision','old_flow_pid','H_prefetch','H_stop'} <= meta.keys():
        raise ValueError('No complete legacy cutover identity')
    if (meta['old_flow_pid']==str(current_pid) or
        db.state('current_wss_provider')!='alchemy' or current(db)):
        raise ValueError('Legacy rollback outcome is not established')
    for stage in ('historical','stop_tail'):
        jobs=db.conn.execute('SELECT completion_status FROM flow_shadow_jobs WHERE stage=?',
                             (stage,)).fetchall()
        if not jobs or any(row[0]!='complete' for row in jobs):
            raise ValueError('Legacy shadow or stop-tail proof is incomplete')
    digest,jobs,ranges=legacy_digest(db)
    identity='legacy-'+hashlib.sha256(
        '|'.join(meta[k] for k in ('git_revision','old_flow_pid','H_prefetch','H_stop')).encode()
    ).hexdigest()[:24]
    existing=_row(db,'id=?',(identity,))
    if existing:
        if json.loads(existing['payload']).get('legacy_proof_digest')!=digest:
            raise ValueError('Legacy proof changed after import')
        return json.loads(existing['payload'])
    value={'id':identity,'state':'ROLLED_BACK','revision':meta['git_revision'],
           'source_legacy_pid':meta['old_flow_pid'],'H_prefetch':int(meta['H_prefetch']),
           'H_pre_stop':int(meta['H_pre_stop']) if meta.get('H_pre_stop') else None,
           'H_stop':int(meta['H_stop']),
           'H_live':int(meta['H_live']) if meta.get('H_live') else None,'targets':[],
           'proof_stages':['historical','stop_tail'],
           'legacy_proof_digest':digest,'legacy_jobs':jobs,'legacy_ranges':ranges,
           'failure':'Prior cutover rolled back to active Alchemy flow'}
    with db.conn:
        db.conn.execute('''INSERT INTO flow_cutover_sessions
          (id,created_at_utc,deploy_git_revision,source_route,source_legacy_pid,
           status,terminal_reason,archived_at_utc,payload)
          VALUES(?,?,?,?,?,?,?,?,?)''',
          (identity,None,
           meta['git_revision'],'alchemy',meta['old_flow_pid'],'ROLLED_BACK',
           value['failure'],utc(),json.dumps(value,sort_keys=True)))
        stages=('historical','stop_tail','wss_ready_tail','rollback_tail',
                'rollback_restart_tail','failed_cutover_cleanup_tail')
        placeholders=','.join('?' for _ in stages)
        db.conn.execute(f'''INSERT INTO flow_cutover_legacy_proof
          SELECT ?,'job',stage,launch_id,kind,-1,-1 FROM flow_shadow_jobs
          WHERE stage IN ({placeholders})''',(identity,*stages))
        db.conn.execute(f'''INSERT INTO flow_cutover_legacy_proof
          SELECT ?,'range',stage,launch_id,kind,first_block,last_block
          FROM flow_shadow_ranges WHERE stage IN ({placeholders})''',(identity,*stages))
    return value


def create(db, *, revision, source_pid, source_start, main_pid, roles_fingerprint):
    """Caller completes all external preconditions before this atomic insert."""
    if not _exists(db):raise ValueError('Legacy ledger must be imported first')
    db.conn.execute('BEGIN IMMEDIATE')
    try:
        if current(db):raise ValueError('A nonterminal cutover session already exists')
        if not latest_terminal(db):raise ValueError('Prior ledger is not terminal')
        now=utc()
        value={'id':uuid4().hex,'state':'CREATED','revision':revision,
               'source_legacy_pid':str(source_pid),'source_legacy_start_time':source_start,
               'main_pid':str(main_pid),
               'source_route':'alchemy','split_role_fingerprint':roles_fingerprint,
               'created_at_utc':now,'H_prefetch':None,'H_pre_stop':None,
               'H_stop':None,'H_live':None,'targets':[],
               'shadow_proof':'not_started','stop_tail_proof':'not_started',
               'subscription_ack':None,'ready_tail_proof':'not_started',
               'rollback_state':'none','history':[['CREATED',time.time()]]}
        db.conn.execute('''INSERT INTO flow_cutover_sessions
          (id,created_at_utc,deploy_git_revision,source_route,source_legacy_pid,
           source_legacy_start_time,split_role_fingerprint,status,payload)
          VALUES(?,?,?,?,?,?,?,?,?)''',
          (value['id'],now,revision,'alchemy',str(source_pid),source_start,
           roles_fingerprint,'CREATED',json.dumps(value,sort_keys=True)))
        db.conn.commit()
        return value
    except BaseException:
        db.conn.rollback()
        raise


def require_phase_pid(value, mode, pid, start=None):
    """Bind the legacy process before stop, then the new process separately."""
    if mode in ('prefetch','preflight','authorize-stop','stop-flow'):
        if (value['source_legacy_pid']!=str(pid) or
            (start is not None and value.get('source_legacy_start_time') not in (None,start))):
            raise ValueError('Legacy source process changed before stop')
    elif mode=='stop-tail':
        if str(pid)==value['source_legacy_pid']:
            raise ValueError('Legacy source process has not stopped')
    elif mode=='ready-tail':
        if str(pid)==value['source_legacy_pid'] or value.get('split_pid')!=str(pid):
            raise ValueError('Split process identity changed')


def save(db, value):
    live=current(db)
    if not live or live['id']!=value['id']:raise ValueError('Cutover session is not active')
    original=json.loads(live['payload'])
    if any(value.get(key)!=original.get(key) for key in
           ('id','revision','source_legacy_pid','source_legacy_start_time',
            'source_route','split_role_fingerprint','created_at_utc','main_pid')):
        raise ValueError('Cutover identity is immutable')
    state=value['state']
    status=state
    if status not in ACTIVE|TERMINAL:raise ValueError('Unknown cutover state')
    db.conn.execute('''UPDATE flow_cutover_sessions SET status=?,terminal_reason=?,
      archived_at_utc=?,split_pid=?,rollback_pid=?,payload=? WHERE id=? AND status=?''',
      (status,value.get('failure'),utc() if status in TERMINAL else None,
       value.get('split_pid'),value.get('rollback_pid'),json.dumps(value,sort_keys=True),
       value['id'],live['status']))
    if db.conn.execute('SELECT changes()').fetchone()[0]!=1:
        raise ValueError('Cutover state changed concurrently')


def advance(db, value, state, **fields):
    value=dict(value,**fields,state=state)
    value['history']=[*value.get('history',[]),[state,time.time()]]
    with db.conn:
        save(db,value)
        db.conn.execute('''INSERT INTO flow_state VALUES('cutover_state',?) ON CONFLICT(key)
          DO UPDATE SET value=excluded.value''',(state.lower(),))
    return value


def begin_validation(db, value):
    if value['state']!='READY_TAIL_VERIFIED' or value.get('ready_tail_proof')!='verified':
        raise ValueError('Verified ready-tail is required')
    return advance(db,value,'POST_CUTOVER_VALIDATING',validation_started_at=time.time())


ACCEPTANCE = ('main_unchanged','db_integrity','security_clean','tx_receipt_zero',
              'phase1_6_healthy','phase2a_healthy','raw_when_present','features_due',
              'no_alchemy','no_active_gaps','provider_switches_proven','no_flap')


def accept_validation(db, value, evidence, *, now=None):
    now=time.time() if now is None else now
    if value['state']!='POST_CUTOVER_VALIDATING' or now-value['validation_started_at']<1800:
        raise ValueError('Thirty-minute validation has not completed')
    if not all(evidence.get(key) is True for key in ACCEPTANCE):
        raise ValueError('Validation acceptance evidence is incomplete')
    return advance(db,value,'SOAKING',soak_started_at=now,validation_evidence=evidence)


def complete_soak(db, value, evidence, *, now=None):
    now=time.time() if now is None else now
    if value['state']!='SOAKING' or now-value['soak_started_at']<86400:
        raise ValueError('Twenty-four-hour soak has not completed')
    if not all(evidence.get(key) is True for key in ACCEPTANCE):
        raise ValueError('Soak acceptance evidence is incomplete')
    return advance(db,value,'COMPLETE',soak_evidence=evidence,soak_completed_at=now)


def rollback_intent(db, value, *, reason, provider, unresolved_ranges):
    if value['state'] not in ('POST_CUTOVER_VALIDATING','SOAKING'):
        raise ValueError('Post-handoff rollback requires validation or soak state')
    if not reason or len(reason)>160 or provider not in ('publicnode','validation'):
        raise ValueError('Rollback reason or provider is invalid')
    if value.get('rollback_intent'):
        raise ValueError('Rollback intent already recorded')
    value=dict(value,rollback_intent={'at':utc(),'reason':reason,'provider':provider,
                                      'unresolved_ranges':unresolved_ranges})
    with db.conn:save(db,value)
    return value


def record_operational_outcome(db, value, *, reason, rollback_pid, provider,
                               reconciliation_proof):
    """Append a later outcome without rewriting an old COMPLETE handoff."""
    row=_row(db,'id=?',(value['id'],))
    if not row or row['status'] not in ('COMPLETE','ROLLED_BACK'):
        raise ValueError('Only a terminal handoff can receive an outcome')
    if row['status']=='COMPLETE' and value.get('ready_tail_proof')!='verified':
        raise ValueError('Completed handoff proof is missing')
    if (not reason or len(reason)>160 or provider not in ('publicnode','validation') or
        not str(rollback_pid).isdigit() or not reconciliation_proof):
        raise ValueError('Operational rollback evidence is incomplete')
    proof_digest=hashlib.sha256(row['payload'].encode()).hexdigest()
    handoff_completed=int(value.get('ready_tail_proof')=='verified')
    outcome={'session_id':value['id'],'handoff_completed':handoff_completed,
             'later_operational_rollback':handoff_completed,
             'rollback_reason':reason,'rollback_pid':str(rollback_pid),
             'rollback_route':'alchemy','provider_at_failure':provider,
             'source_pid':value['source_legacy_pid'],'split_pid':value.get('split_pid'),
             'proof_payload_sha256':proof_digest,
             'reconciliation_proof':json.dumps(reconciliation_proof,sort_keys=True)}
    existing=db.conn.execute('SELECT * FROM flow_cutover_outcomes WHERE session_id=?',
                             (value['id'],)).fetchone()
    if existing:
        if any(existing[key]!=val for key,val in outcome.items()):
            raise ValueError('Operational outcome already differs')
        return dict(existing)
    with db.conn:
        db.conn.execute('''INSERT INTO flow_cutover_outcomes
          (session_id,recorded_at_utc,handoff_completed,later_operational_rollback,
           rollback_reason,rollback_pid,rollback_route,provider_at_failure,
           source_pid,split_pid,proof_payload_sha256,reconciliation_proof)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
          (value['id'],utc(),*(outcome[key] for key in (
             'handoff_completed','later_operational_rollback','rollback_reason',
             'rollback_pid','rollback_route','provider_at_failure','source_pid',
             'split_pid','proof_payload_sha256','reconciliation_proof'))))
    return dict(db.conn.execute('SELECT * FROM flow_cutover_outcomes WHERE session_id=?',
                                (value['id'],)).fetchone())


def authorize_stop(db,value,proof):
    if value['state']!='SHADOW_VERIFIED' or value['H_prefetch'] is None:
        raise ValueError('Fresh shadow proof is required before stop authorization')
    if proof['H_pre_stop']<value['H_prefetch'] or not proof['candidate_config_fingerprint']:
        raise ValueError('Stop authorization is incomplete')
    return advance(db,value,'STOP_AUTHORIZED',H_pre_stop=proof['H_pre_stop'],
                   stop_authorized_at=utc(),candidate_config_fingerprint=proof['candidate_config_fingerprint'],
                   authorization=proof)


def mark_stop_issued(db,value):
    if value['state']!='STOP_AUTHORIZED' or value.get('stop_command_issued_at'):
        raise ValueError('Stop command was already issued or unauthorized')
    value=dict(value,stop_command_issued_at=utc())
    with db.conn:save(db,value)
    return value


def mark_source_stopped(db,value):
    if value['state']!='STOP_AUTHORIZED' or not value.get('stop_command_issued_at'):
        raise ValueError('Source stop has no durable command intent')
    return advance(db,value,'SOURCE_STOPPED',source_stopped_at=utc())


def mark_split_configured(db,value,fingerprint):
    if value['state']!='STOP_TAIL_VERIFIED' or fingerprint!=value.get('candidate_config_fingerprint'):
        raise ValueError('Split config does not match authorized candidate')
    return advance(db,value,'SPLIT_CONFIGURED',split_configured_at=utc())


def abort_pre_stop(db,value,reason,*,source_pid,source_start,route,split):
    if (value['state'] not in PRE_STOP or value.get('source_stopped_at') or
        value.get('split_pid') or split or route!='alchemy'):
        raise ValueError('Source stop or split prevents pre-stop abort')
    require_phase_pid(value,'stop-flow',source_pid,source_start)
    if not reason or len(reason)>160:raise ValueError('Explicit short abort reason required')
    return advance(db,value,'ABORTED_PRE_STOP',failure=reason,abort_reason=reason)


def new(db, h_prefetch, h_stop, targets):
    """Promote an already explicit, freshly shadowed session."""
    row=current(db)
    value=json.loads(row['payload']) if row else None
    if not value or value['state'] not in ('SOURCE_STOPPED','STOP_TAIL_VERIFIED'):
        raise ValueError('Durable source stop is required')
    if value['state']=='STOP_TAIL_VERIFIED':
        if (value['H_prefetch'],value['H_stop'],value['targets'])==(h_prefetch,h_stop,targets):
            return value
        raise ValueError('Stop-tail identity changed')
    if (value['H_prefetch']!=h_prefetch or value['shadow_proof']!='verified' or
        not value.get('source_stopped_at')):
        raise ValueError('Shadow head is not bound to this session')
    for stage in ('historical','stop_tail'):
        scoped=f'cutover:{value["id"]}:{stage}'
        rows=db.conn.execute('''SELECT completion_status FROM flow_shadow_jobs
          WHERE stage=?''',(scoped,)).fetchall()
        if not rows or any(row[0]!='complete' for row in rows):
            raise ValueError('Fresh stage proof is incomplete')
    return advance(db,value,'STOP_TAIL_VERIFIED',H_prefetch=h_prefetch,H_stop=h_stop,
                   targets=targets,stop_tail_proof='verified',
                   gap_id_before=db.conn.execute('SELECT coalesce(max(id),0) FROM flow_gaps').fetchone()[0],
                   expected={'wss':'publicnode','fallback_wss':'validation','http':'validation'},
                   subscription_ready_at=None,subscription_ready_provider=None,
                   ready_tail_verified=False,H_live=None)


def abort_empty(db):
    row=current(db)
    value=json.loads(row['payload']) if row else None
    if not value or value['state']!='CREATED':
        raise ValueError('Only an empty CREATED session may be aborted')
    prefix='cutover:'+value['id']+':%'
    if (db.conn.execute('SELECT 1 FROM flow_shadow_jobs WHERE stage LIKE ? LIMIT 1',(prefix,)).fetchone()
        or db.conn.execute('SELECT 1 FROM flow_shadow_ranges WHERE stage LIKE ? LIMIT 1',(prefix,)).fetchone()
        or any(value.get(k) is not None for k in ('H_prefetch','H_stop','H_live'))):
        raise ValueError('Created session already has proof')
    return advance(db,value,'ABORTED_PRE_STOP',failure='Empty test session aborted')


def record_rollback(db, value, pid, *, proof=None):
    """Record the connected legacy PID after a failed or cancelled split run."""
    row=_row(db,'id=?',(value['id'],))
    if not row or row['status'] not in PENDING|{'FAILED','POST_CUTOVER_VALIDATING','SOAKING'}:
        raise ValueError('No rollback-eligible session')
    intent=value.get('rollback_intent')
    if row['status'] in ('POST_CUTOVER_VALIDATING','SOAKING') and not intent:
        raise ValueError('Post-handoff rollback intent is missing')
    if row['status'] in ('POST_CUTOVER_VALIDATING','SOAKING') and not (
        proof and proof.get('legacy_route_connected') is True and
        proof.get('active_unresolved_gaps')==0 and proof.get('recovery_state')=='healthy'):
        raise ValueError('Post-handoff rollback reconciliation is unproved')
    reason=intent['reason'] if intent else value.get('failure','Handoff failed')
    provider=intent['provider'] if intent else value.get('subscription_ready_provider','publicnode')
    value=dict(value,state='ROLLED_BACK',rollback_pid=str(pid),
               rollback_state='legacy_active',rollback_at=utc(),rollback_reason=reason,
               provider_at_failure=provider,
               unresolved_provider_switch_ranges=(intent or {}).get('unresolved_ranges',[]),
               rollback_reconciliation_proof=proof)
    value['history']=[*value.get('history',[]),['ROLLED_BACK',time.time()]]
    with db.conn:
        db.conn.execute('''UPDATE flow_cutover_sessions SET status='ROLLED_BACK',
          rollback_pid=?,archived_at_utc=?,payload=? WHERE id=? AND status=?''',
          (str(pid),utc(),json.dumps(value,sort_keys=True),value['id'],row['status']))
        if db.conn.execute('SELECT changes()').fetchone()[0]!=1:
            raise ValueError('Rollback session changed concurrently')
        db.conn.execute("INSERT INTO flow_state VALUES('cutover_state','rolled_back') "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value")
    if proof:
        record_operational_outcome(db,value,reason=reason,rollback_pid=pid,provider=provider,
                                   reconciliation_proof=proof)
    return value


def gap_counts(db):
    active=db.conn.execute('''SELECT count(*) FROM flow_gaps g JOIN flow_tracking_targets t
      ON t.launch_id=g.launch_id WHERE g.resolved=0 AND t.status NOT IN ('completed','partial')''').fetchone()[0]
    historical=db.conn.execute('''SELECT count(*) FROM flow_gaps g JOIN flow_tracking_targets t
      ON t.launch_id=g.launch_id WHERE g.resolved=0 AND t.status IN ('completed','partial')''').fetchone()[0]
    return active,historical


def unexpected_gap(db, value):
    ids={t['launch_id'] for t in value['targets']}
    return any(r['launch_id'] in ids or r['status'] not in ('completed','partial')
               for r in db.conn.execute('''SELECT g.launch_id,t.status FROM flow_gaps g
                 JOIN flow_tracking_targets t ON t.launch_id=g.launch_id
                 WHERE g.resolved=0 AND g.id>?''',(value['gap_id_before'],)))
