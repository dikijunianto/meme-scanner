"""Operator-controlled, fail-closed Phase 2B.2 shadow and tail reconciliation."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import hashlib
import json
import sqlite3
import subprocess
import time
from contextlib import closing

import httpx
from websockets.asyncio.client import connect

from app.config import ROOT, Config
from app.flow_cutover import (advance as advance_cutover,
                              abort_pre_stop, authorize_stop,
                              create as create_cutover, current as active_cutover,
                              gap_counts, import_rolled_back_legacy,
                              mark_source_stopped, mark_stop_issued,
                              new as new_cutover, save as save_cutover,
                              schema as cutover_schema,
                              source_process_gone,
                              require_phase_pid,
                              session as cutover_session, status as cutover_status,
                              unexpected_gap)
from app.flow_config import (candidate_split_preflight,candidate_split_status,
                             prestart_check,safe_split_fingerprint)
from app.flow_data import BUY, SELL, FlowDB
from app.flow_providers import FlowProviders
from app.flow_shadow import make_reconciler, verified_checkout
from app.flow_worker import FlowSettings, filter_queries
from app.rpc import RpcError


def service(name):
    result=subprocess.run(('systemctl','show',name,'-p','ActiveState','-p','MainPID',
                           '-p','NRestarts','-p','ExecMainStartTimestampMonotonic'),
                          capture_output=True,text=True,check=True)
    return dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line)


def filter_snapshot(db):
    targets=[dict(row) for row in db.conn.execute(
        "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') ORDER BY launch_id")]
    result=[]
    for target in targets:
        graduation=json.loads(target['graduation_json']) if target['graduation_json'] else None
        filters=filter_queries(target)
        if graduation:filters['curve']={'address':target['curve_address'],'topics':[[BUY,SELL]]}
        for kind,query in filters.items():
            base=graduation['block_number'] if graduation and kind!='curve' else target['launch_block']
            cursor=db.state(f'recovery:{target["launch_id"]}:{kind}')
            bootstrap=db.conn.execute('''SELECT status FROM flow_bootstrap
              WHERE launch_id=? AND kind=?''',(target['launch_id'],kind)).fetchone()
            if cursor is None or (bootstrap and bootstrap['status']!='complete'):
                raise RpcError('Active filter lacks completed bootstrap and recovery cursor')
            result.append({'launch_id':target['launch_id'],'kind':kind,'base':base,
                           'query':query})
    if gap_counts(db)[0]:raise RpcError('Active recovery gap blocks shadow')
    return result


def snapshot_digest(snapshot):
    return hashlib.sha256(json.dumps(snapshot,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def authorization_reasons(db,value,revision,main,flow,settings,providers):
    """No network or writes; status and guarded stop share the same invalidators."""
    auth=value.get('authorization') or {}
    reasons=[]
    if value['revision']!=revision:reasons.append('revision_changed')
    if value.get('main_pid')!=main['MainPID'] or main['ActiveState']!='active':
        reasons.append('main_changed')
    if (flow['ActiveState']!='active' or value['source_legacy_pid']!=flow['MainPID'] or
        value.get('source_legacy_start_time')!=flow.get('ExecMainStartTimestampMonotonic')):
        reasons.append('source_process_changed')
    if settings.split_enabled:reasons.append('split_enabled_before_source_stop')
    if db.state('current_wss_provider')!='alchemy' or db.state('recovery_state')!='healthy':
        reasons.append('legacy_route_or_recovery_changed')
    try:
        snapshot=filter_snapshot(db)
        if snapshot!=value.get('targets') or snapshot_digest(snapshot)!=auth.get('snapshot_digest'):
            reasons.append('active_filter_snapshot_changed')
    except Exception:reasons.append('active_filter_or_bootstrap_invalid')
    if gap_counts(db)[0]:reasons.append('active_gap')
    try:
        candidate=candidate_split_status()
        if (candidate['candidate_fingerprint']!=value.get('candidate_config_fingerprint') or
            candidate['legacy_file_digest']!=auth.get('legacy_file_digest') or
            value.get('split_role_fingerprint')!=auth.get('role_fingerprint')):
            reasons.append('candidate_config_changed')
    except Exception:reasons.append('candidate_config_invalid')
    if safe_split_fingerprint(settings,providers)!=auth.get('actual_legacy_fingerprint'):
        reasons.append('production_config_changed')
    if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':reasons.append('flow_db_integrity')
    day=int(time.time())//86400*86400
    remaining=settings.daily_getlogs-db.used('flow_eth_getLogs',day)
    if remaining<auth.get('minimum_getlogs_remaining',settings.daily_getlogs+1):
        reasons.append('getlogs_reserve_insufficient')
    if db.used('flow_rpc_members',day)>=settings.daily_calls:reasons.append('rpc_daily_exhausted')
    return reasons


async def chain_ids(config,providers):
    async with httpx.AsyncClient(timeout=20) as client:
        async def http(url):
            response=await client.post(url,json={'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]})
            response.raise_for_status()
            return int(response.json()['result'],16)
        # The old live flow already validates Alchemy; migration never calls it.
        result={'alchemy_configured':config.chain_id,'validation_http':await http(providers.http)}
    for name in ('publicnode','validation'):
        async with connect(providers.ws(name),open_timeout=20,max_size=65536,compression=None) as socket:
            await socket.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]}))
            result[name+'_wss']=int(json.loads(await asyncio.wait_for(socket.recv(),20))['result'],16)
    if set(result.values())!={4663}:raise RpcError('Provider chain identity mismatch')
    return result


async def operate(mode,reason=None):
    if mode=='status':
        # Status must not construct a writer, create schema, or contact a provider.
        settings=FlowSettings.load()
        db=FlowDB(settings.database,readonly=True)
        try:
            revision=subprocess.check_output(('git','-C',str(ROOT),'rev-parse','HEAD'),text=True).strip()
            report=cutover_status(db)
            active=report['current_active']
            providers=None;actual_fingerprint=None
            try:
                providers=FlowProviders.load()
                actual_fingerprint=safe_split_fingerprint(settings,providers)
            except Exception:pass
            proof={}
            if active:
                prefix='cutover:'+active['id']+':%'
                proof={'jobs':db.conn.execute('SELECT count(*) FROM flow_shadow_jobs WHERE stage LIKE ?',
                                              (prefix,)).fetchone()[0],
                       'ranges':db.conn.execute('SELECT count(*) FROM flow_shadow_ranges WHERE stage LIKE ?',
                                                (prefix,)).fetchone()[0]}
            main=service('meme-scanner.service');flow=service('meme-scanner-flow.service')
            phase=active['status'] if active else None
            expected_split=(False if phase in ('CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED',
                                             'STOP_AUTHORIZED','SOURCE_STOPPED','STOP_TAIL_VERIFIED')
                            else True if phase in ('SPLIT_CONFIGURED','SPLIT_WSS_CONNECTING',
                                                   'SUBSCRIPTIONS_READY','READY_TAIL_PENDING',
                                                   'READY_TAIL_VERIFIED') else None)
            reasons=[]
            if providers is None:reasons.append('provider_config_invalid')
            if expected_split is not None and settings.split_enabled!=expected_split:
                reasons.append('phase_split_mismatch')
            if active and phase=='STOP_AUTHORIZED' and providers is not None:
                value=json.loads(db.conn.execute('SELECT payload FROM flow_cutover_sessions WHERE id=?',
                                                 (active['id'],)).fetchone()[0])
                reasons+=authorization_reasons(db,value,revision,main,flow,settings,providers)
            return {'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                    'production':{'revision':revision,'checkout_clean':not subprocess.check_output(
                        ('git','-C',str(ROOT),'status','--porcelain'),text=True).strip(),
                        'main':main,'flow':flow,
                        'split':settings.split_enabled,'current_wss_provider':db.state('current_wss_provider')},
                    **report,'current_proof':proof,
                    'actual_safe_config_fingerprint':actual_fingerprint,
                    'expected_split_for_phase':expected_split,
                    'authorization_still_valid':not reasons if phase=='STOP_AUTHORIZED' else None,
                    'invalidation_reasons':reasons,
                    'historical_session_revision_mismatch':any(
                        old['deploy_git_revision']!=revision for old in report['historical_sessions']),
                    'active_session_revision_mismatch':bool(active and active['deploy_git_revision']!=revision)}
        finally:db.conn.close()
    revision=verified_checkout()
    main=service('meme-scanner.service')
    flow=service('meme-scanner-flow.service')
    if main['ActiveState']!='active':raise RpcError('Main service is not active')
    if mode in ('prefetch','preflight','authorize-stop') and flow['ActiveState']!='active':
        raise RpcError('Old flow must remain active')
    if mode=='stop-tail' and flow['ActiveState']!='inactive':
        raise RpcError('Stop only flow after preflight')
    if mode=='ready-tail' and flow['ActiveState']!='active':
        raise RpcError('New flow is not active')
    settings=FlowSettings.load()
    if mode in ('archive-legacy','new-session','abort-session','migrate-session-schema'):
        if (main['ActiveState']!='active' or flow['ActiveState']!='active' or
            settings.split_enabled):
            raise RpcError('Legacy services and routing required')
        db=FlowDB(settings.database)
        try:
            config=Config.load();providers=FlowProviders.load()
            if (db.state('current_wss_provider')!='alchemy' or
                db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok'):
                raise RpcError('Legacy flow or flow DB is not healthy')
            with closing(sqlite3.connect(config.database.resolve().as_uri()+'?mode=ro',uri=True)) as source:
                if source.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                    raise RpcError('Main DB integrity failed')
            if mode=='migrate-session-schema':
                cutover_schema(db)
                return {'gate':'SESSION_SCHEMA_READY','active_session':bool(active_cutover(db)),
                        'flow_integrity':db.conn.execute('PRAGMA integrity_check').fetchone()[0]}
            if mode=='abort-session':
                row=active_cutover(db)
                if not row:raise RpcError('No active session to abort')
                value=abort_pre_stop(db,json.loads(row['payload']),reason or 'operator_cancelled',
                    source_pid=flow['MainPID'],source_start=flow.get('ExecMainStartTimestampMonotonic'),
                    route=db.state('current_wss_provider'),split=settings.split_enabled)
                return {'gate':'PRE_STOP_SESSION_ABORTED','id':value['id'],
                        'reason':value['abort_reason']}
            await prestart_check()
            if mode=='archive-legacy':
                old=import_rolled_back_legacy(db,flow['MainPID'])
                return {'gate':'LEGACY_LEDGER_ARCHIVED','historical_id':old['id'],
                        'historical_revision':old['revision'],
                        'historical_source_pid':old['source_legacy_pid'],
                        'proof_digest':old['legacy_proof_digest']}
            if (db.state('recovery_state') in ('unrecoverable_gap','cutover_failed') or
                db.state('service_status')!='connected' or
                db.state('connection_state')!='connected' or
                gap_counts(db)[0] or
                db.conn.execute('''SELECT 1 FROM flow_bootstrap b JOIN flow_tracking_targets t
                  ON t.launch_id=b.launch_id WHERE t.status NOT IN ('completed','partial')
                  AND b.status!='complete' LIMIT 1''').fetchone() or
                db.state('cutover_state') in ('stop_tail_verified','split_wss_connecting',
                                            'subscriptions_ready','ready_tail_pending',
                                            'ready_tail_verified')):
                raise RpcError('Cutover creation preflight failed')
            roles=json.dumps({'primary':providers.fingerprints()['ws_primary'],
                              'fallback':providers.fingerprints()['ws_fallback'],
                              'http':providers.fingerprints()['http'],
                              'candidate_split':True},sort_keys=True)
            cutover_schema(db)
            value=create_cutover(db,revision=revision,source_pid=flow['MainPID'],
                                 source_start=flow.get('ExecMainStartTimestampMonotonic'),
                                 main_pid=main['MainPID'],
                                 roles_fingerprint=hashlib.sha256(roles.encode()).hexdigest())
            return {'gate':'CUTOVER_SESSION_CREATED','id':value['id'],'revision':revision,
                    'source_legacy_pid':value['source_legacy_pid'],
                    'H_prefetch':None,'H_stop':None,'H_live':None,'target_filters':0,
                    'fresh_jobs':0,'fresh_ranges':0}
        finally:db.conn.close()
    if mode in ('prefetch','preflight','authorize-stop','stop-flow','stop-tail') and settings.split_enabled:
        raise RpcError('Old flow routing flag unexpectedly enabled')
    if mode=='ready-tail' and not settings.split_enabled:
        raise RpcError('New flow routing flag is not enabled')
    db=FlowDB(settings.database)
    config=Config.load();providers=FlowProviders.load()
    active=active_cutover(db)
    if not active:raise RpcError('Explicit active cutover session required')
    cutover=json.loads(active['payload'])
    if cutover['revision']!=revision:raise RpcError('Active cutover revision changed')
    if cutover.get('main_pid')!=main['MainPID']:raise RpcError('Main PID changed during cutover')
    if not (mode=='stop-flow' and flow['ActiveState']=='inactive'):
        require_phase_pid(cutover,mode,flow['MainPID'],flow.get('ExecMainStartTimestampMonotonic'))
    runner,old_rpc=make_reconciler(config,settings,db,providers,session_id=cutover['id'])
    try:
        await old_rpc.close()
        pinned=runner.meta('git_revision')
        if pinned and pinned!=revision:raise RpcError('Migration source revision changed')
        if mode=='prefetch' and not pinned:runner.set_meta('git_revision',revision)
        previous=runner.meta('main_pid')
        if previous and previous!=main['MainPID']:raise RpcError('Main PID changed during migration')
        if mode=='prefetch':
            if flow['ActiveState']!='active':raise RpcError('Old flow must remain active')
            if cutover['state'] not in ('CREATED','SHADOW_IN_PROGRESS','SHADOW_VERIFIED'):
                raise RpcError('Shadow session is past pre-stop phase')
            prior_flow=runner.meta('old_flow_pid')
            if prior_flow and prior_flow!=flow['MainPID']:raise RpcError('Old flow PID changed during shadow')
            snapshot=filter_snapshot(db)
            if not snapshot:
                return {'revision':revision,'cutover_session_id':cutover['id'],
                        'target_filters':0,'gate':'MIGRATION_BLOCKED','reason':'No active filter to shadow'}
            if cutover['state']!='SHADOW_IN_PROGRESS':
                cutover=advance_cutover(db,cutover,'SHADOW_IN_PROGRESS',shadow_proof='in_progress')
            runner.set_meta('main_pid',main['MainPID'])
            runner.set_meta('old_flow_pid',flow['MainPID'])
            head=await runner.start_historical()
            complete=await runner.run_stage('historical')
            if complete:
                if filter_snapshot(db)!=snapshot:
                    raise RpcError('Active filter snapshot changed during shadow')
                cutover=advance_cutover(db,cutover,'SHADOW_VERIFIED',H_prefetch=head,
                                        targets=snapshot,shadow_proof='verified')
            return {'revision':revision,'H_prefetch':head,**runner.summary('historical'),
                    'gate':'SHADOW_COMPLETE' if complete else 'MIGRATION_BLOCKED'}
        if mode=='preflight':
            if cutover['state']!='SHADOW_VERIFIED' or filter_snapshot(db)!=cutover['targets']:
                raise RpcError('Fresh shadow target snapshot changed')
            if flow['ActiveState']!='active' or flow['MainPID']!=runner.meta('old_flow_pid'):
                raise RpcError('Old flow is not continuously active')
            runner.add_jobs('historical',0,int(runner.meta('H_prefetch')))
            if not runner.complete('historical'):raise RpcError('Historical shadow reconciliation incomplete')
            prestart=await prestart_check()
            if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Flow database integrity failed')
            if runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Main database integrity failed')
            if gap_counts(db)[0]:raise RpcError('Preexisting active flow gap blocks cutover')
            identities=await chain_ids(config,providers)
            runner.set_meta('provider_chain_ids_verified',json.dumps(identities,sort_keys=True))
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            if plan['ready']:
                runner.set_meta('H_pre_stop',head)
                cutover=advance_cutover(db,cutover,'SHADOW_VERIFIED',H_pre_stop=head)
            return {'revision':revision,'chain_ids':identities,'databases':'ok',
                    'service_user_readable':prestart['readable_by_service_user'],**plan,
                    'gate':'CUTOVER_PREFLIGHT_PASS' if plan['ready'] else 'MIGRATION_BLOCKED'}
        if mode=='authorize-stop':
            if (cutover['state']!='SHADOW_VERIFIED' or settings.split_enabled or
                db.state('current_wss_provider')!='alchemy' or
                db.state('service_status')!='connected' or db.state('recovery_state')!='healthy' or
                filter_snapshot(db)!=cutover['targets'] or not runner.complete('historical') or
                not runner.meta('provider_chain_ids_verified')):
                raise RpcError('Fresh shadow, legacy route, or provider identity gate failed')
            if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok' or \
               runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                raise RpcError('Database integrity failed')
            candidate=await candidate_split_preflight()
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            ready_allowance=plan['reserved_attempts']
            required=plan['reserved_attempts']+ready_allowance+50
            day=int(time.time())//86400*86400;minute=int(time.time())//60*60
            if (plan['remaining']<required or
                db.used('flow_rpc_members',day)>=settings.daily_calls or
                db.used('flow_rpc_members',minute)>=settings.minute_calls or
                runner.worker.rpc.config.rpc_rps>.5):
                return {'gate':'MIGRATION_BLOCKED','reason':'Tail budget or envelope limiter insufficient',
                        **plan,'projected_ready_tail_attempts':ready_allowance,
                        'required_getlogs':required}
            proof={'H_pre_stop':head,'snapshot_digest':snapshot_digest(cutover['targets']),
                   'source_legacy_pid':cutover['source_legacy_pid'],
                   'source_legacy_start_time':cutover.get('source_legacy_start_time'),
                   'revision':revision,'role_fingerprint':cutover['split_role_fingerprint'],
                   'candidate_config_fingerprint':candidate['candidate_fingerprint'],
                   'actual_legacy_fingerprint':candidate['actual_fingerprint'],
                   'legacy_file_digest':candidate['legacy_file_digest'],
                   'service_user_readable':candidate['readable_by_service_user'],
                   'config_owner_uid':candidate['owner_uid'],'config_owner_gid':candidate['owner_gid'],
                   'config_mode':candidate['mode'],'config_parent_mode':candidate['parent_mode'],
                   'db_integrity':'ok','projected_stop_tail_attempts':plan['reserved_attempts'],
                   'projected_ready_tail_attempts':ready_allowance,
                   'minimum_getlogs_remaining':required,
                   'budget_at_authorization':{'getlogs_remaining':plan['remaining'],
                        'rpc_daily_used':db.used('flow_rpc_members',day),
                        'rpc_minute_used':db.used('flow_rpc_members',minute)},
                   'envelope_rps':runner.worker.rpc.config.rpc_rps}
            runner.set_meta('H_pre_stop',head)
            cutover=authorize_stop(db,cutover,proof)
            return {'gate':'STOP_AUTHORIZED','session_id':cutover['id'],
                    'authorized_at':cutover['stop_authorized_at'],
                    'H_pre_stop':head,'projected_stop_tail_attempts':plan['reserved_attempts'],
                    'projected_ready_tail_attempts':ready_allowance,
                    'remaining':plan['remaining'],'required_getlogs':required,
                    'candidate_fingerprint':candidate['candidate_fingerprint']}
        if mode=='stop-flow':
            if cutover['state']!='STOP_AUTHORIZED' or settings.split_enabled:
                raise RpcError('Durable stop authorization and legacy config required')
            if flow['ActiveState']=='inactive':
                if (not cutover.get('stop_command_issued_at') or
                    flow['MainPID']==cutover['source_legacy_pid'] or
                    not source_process_gone(cutover['source_legacy_pid'])):
                    raise RpcError('Source absence lacks authorized stop intent')
                cutover=mark_source_stopped(db,cutover)
                return {'gate':'SOURCE_STOPPED','session_id':cutover['id'],
                        'source_stopped_at':cutover['source_stopped_at'],'recovered_after_crash':True}
            if (flow['ActiveState']!='active' or
                flow['MainPID']!=runner.meta('old_flow_pid') or
                not runner.meta('H_pre_stop') or
                cutover['H_pre_stop']!=int(runner.meta('H_pre_stop')) or
                not runner.complete('historical')):
                raise RpcError('Flow stop proof is incomplete')
            reasons=authorization_reasons(db,cutover,revision,main,flow,settings,providers)
            if reasons:raise RpcError('Stop authorization invalidated: '+','.join(reasons))
            candidate=await candidate_split_preflight()
            if (candidate['candidate_fingerprint']!=cutover['candidate_config_fingerprint'] or
                candidate['legacy_file_digest']!=cutover['authorization']['legacy_file_digest']):
                raise RpcError('Candidate config changed after authorization')
            if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Flow database integrity failed')
            if runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Main database integrity failed')
            if gap_counts(db)[0]:raise RpcError('New active flow gap blocks cutover')
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            required=2*plan['reserved_attempts']+50
            minute=int(time.time())//60*60
            if plan['remaining']<required or db.used('flow_rpc_members',minute)>=settings.minute_calls:
                return {'revision':revision,**plan,'gate':'MIGRATION_BLOCKED'}
            if not cutover.get('stop_command_issued_at'):
                cutover=mark_stop_issued(db,cutover)
            subprocess.run(('sudo','-n','systemctl','stop','meme-scanner-flow.service'),check=True)
            stopped=service('meme-scanner-flow.service')
            if (stopped['ActiveState']!='inactive' or
                stopped['MainPID']==cutover['source_legacy_pid'] or
                not source_process_gone(cutover['source_legacy_pid'])):
                raise RpcError('Bound source process remains active after stop command')
            cutover=mark_source_stopped(db,cutover)
            return {'revision':revision,**plan,'gate':'SOURCE_STOPPED',
                    'session_id':cutover['id'],'source_stopped_at':cutover['source_stopped_at']}
        if mode=='stop-tail':
            if flow['ActiveState']!='inactive':raise RpcError('Stop only flow after preflight; main must remain active')
            if (cutover['state']!='SOURCE_STOPPED' or settings.split_enabled or
                not runner.meta('H_pre_stop') or
                cutover.get('H_pre_stop')!=int(runner.meta('H_pre_stop'))):
                raise RpcError('No passed pre-stop gate')
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            if not runner.tail_plan(head)['ready']:
                return {'revision':revision,'H_stop':head,'gate':'ROLLBACK_REQUIRED','reason':'Tail budget insufficient'}
            runner.set_meta('H_stop',head)
            runner.set_meta('H_stop_at',int(time.time()))
            runner.add_jobs('stop_tail',int(runner.meta('H_prefetch'))+1,head)
            complete=await runner.run_stage('stop_tail')
            if complete:
                runner.promote('historical')
                runner.promote('stop_tail')
                if gap_counts(db)[0]:raise RpcError('Active flow gap appeared during stop tail')
                targets=[]
                for row in db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"):
                    target=dict(row)
                    for kind,query,base,_ in runner.periods(target,head):
                        targets.append({'launch_id':target['launch_id'],'kind':kind,
                                        'base':base,'query':query})
                cutover=new_cutover(db,int(runner.meta('H_prefetch')),head,targets)
                runner.set_meta('cutover_session_id',cutover['id'])
            return {'revision':revision,'H_stop':head,**runner.summary('stop_tail'),
                    'cutover_session_id':cutover['id'] if complete else None,
                    'gate':'STOP_TAIL_COMPLETE' if complete else 'ROLLBACK_REQUIRED'}
        if mode=='ready-tail':
            if flow['ActiveState']!='active' or flow['MainPID']==runner.meta('old_flow_pid'):
                raise RpcError('New flow service is not active')
            if not runner.complete('stop_tail'):raise RpcError('Stop tail incomplete')
            cutover=cutover_session(db)
            if (not cutover or cutover['id']!=runner.meta('cutover_session_id') or
                cutover['state']!='READY_TAIL_PENDING' or
                cutover.get('split_pid')!=flow['MainPID'] or
                cutover['expected']!={'wss':'publicnode','fallback_wss':'validation','http':'validation'} or
                cutover['H_stop']!=int(runner.meta('H_stop')) or
                not cutover['subscription_ready_at'] or
                cutover['subscription_ready_provider']!='publicnode' or
                runner.db.state('service_status')!='cutover_handoff_pending' or
                runner.db.state('current_wss_provider')!='publicnode' or
                unexpected_gap(db,cutover)):
                raise RpcError('Exact cutover handoff is not ready')
            await prestart_check(require_split=True)
            active=[dict(row) for row in db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')")]
            required=sum(1 for t in cutover['targets'] if any(t['launch_id']==a['launch_id'] for a in active))
            for _ in range(36):
                sample=runner.db.conn.execute('SELECT subscriptions FROM flow_samples WHERE at>=? ORDER BY at DESC LIMIT 1',
                                              (int(cutover['subscription_ready_at'])//30*30+30,)).fetchone()
                if sample and sample[0]>=required:break
                await asyncio.sleep(1)
            else:
                raise RpcError('Required WSS subscriptions are not acknowledged')
            latest=cutover_session(db)
            if (not latest or latest['id']!=cutover['id'] or latest['state']!='READY_TAIL_PENDING'
                or unexpected_gap(db,latest) or db.state('current_wss_provider')!='publicnode'):
                raise RpcError('Cutover state changed while awaiting subscriptions')
            cutover=latest
            snapshot={(x['launch_id'],x['kind']):x for x in cutover['targets']}
            for t in active:
                for kind,query,base,_ in runner.periods(t,int(runner.meta('H_stop'))):
                    saved=snapshot.get((t['launch_id'],kind))
                    if not saved or saved['base']!=base or saved['query']!=query:
                        raise RpcError('Active filter changed outside cutover snapshot')
            head=cutover['H_live']
            if head is None:
                head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
                if head<int(runner.meta('H_stop')):raise RpcError('Validation head moved behind stop proof')
                db.conn.execute('BEGIN IMMEDIATE')
                try:
                    fresh=cutover_session(db)
                    if (fresh['id']!=cutover['id'] or fresh['state']!='READY_TAIL_PENDING' or
                        fresh['targets']!=cutover['targets'] or fresh['H_live'] is not None):
                        raise RpcError('Cutover session changed while pinning live head')
                    cutover=dict(fresh,H_live=head,H_live_at=time.time())
                    save_cutover(db,cutover)
                    db.conn.commit()
                except BaseException:
                    db.conn.rollback()
                    raise
            runner.set_meta('H_live',head)
            runner.set_meta('H_live_at',int(cutover['H_live_at']))
            targets={x['launch_id'] for x in cutover['targets']}
            runner.add_jobs('wss_ready_tail',int(runner.meta('H_stop'))+1,head,targets)
            jobs=runner.summary('wss_ready_tail')['jobs']
            span=runner.summary('historical')['min_successful_chunk'] or 1
            needed=sum(max(0,(j['reconciliation_upper_bound']-j['next_unverified_block'])//span+1)
                       for j in jobs)
            remaining=settings.daily_getlogs-db.used('flow_eth_getLogs',int(time.time())//86400*86400)
            if remaining<needed*runner.worker.rpc.config.retry_attempts+50:
                return {'revision':revision,'H_live':head,'remaining':remaining,
                        'reserved_attempts':needed*runner.worker.rpc.config.retry_attempts,
                        'gate':'ROLLBACK_REQUIRED','reason':'Ready-tail budget insufficient'}
            complete=await runner.run_stage('wss_ready_tail')
            if complete:runner.promote('wss_ready_tail')
            return {'revision':revision,'H_live':head,**runner.summary('wss_ready_tail'),
                    'gate':'READY_FOR_30_MIN_VALIDATION' if complete else 'ROLLBACK_REQUIRED'}
        raise ValueError('Unknown mode')
    finally:
        await runner.worker.rpc.close()
        runner.worker.main.close()
        db.conn.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('archive-legacy','migrate-session-schema','new-session',
                                        'abort-session','prefetch','preflight','authorize-stop',
                                        'stop-flow','stop-tail','ready-tail','status'))
    parser.add_argument('--reason',help='Short reason when aborting a pre-stop session')
    args=parser.parse_args()
    try:print(json.dumps(asyncio.run(operate(args.mode,args.reason)),indent=2))
    except Exception as exc:
        # Never print exception text: provider errors can contain credentials.
        print(json.dumps({'gate':'MIGRATION_BLOCKED','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
