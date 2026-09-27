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
from app.flow_cutover import (abort_empty, advance as advance_cutover,
                              create as create_cutover, current as active_cutover,
                              gap_counts, import_rolled_back_legacy,
                              new as new_cutover, save as save_cutover,
                              require_phase_pid,
                              session as cutover_session, status as cutover_status,
                              unexpected_gap)
from app.flow_config import prestart_check
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders
from app.flow_shadow import make_reconciler, verified_checkout
from app.flow_worker import FlowSettings
from app.rpc import RpcError


def service(name):
    result=subprocess.run(('systemctl','show',name,'-p','ActiveState','-p','MainPID',
                           '-p','NRestarts','-p','ExecMainStartTimestampMonotonic'),
                          capture_output=True,text=True,check=True)
    return dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line)


def filter_snapshot(runner):
    targets=[dict(row) for row in runner.db.conn.execute(
        "SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') ORDER BY launch_id")]
    result=[]
    for target in targets:
        for kind,query,base,_ in runner.periods(target,0):
            cursor=runner.db.state(f'recovery:{target["launch_id"]}:{kind}')
            bootstrap=runner.db.conn.execute('''SELECT status FROM flow_bootstrap
              WHERE launch_id=? AND kind=?''',(target['launch_id'],kind)).fetchone()
            if cursor is None or (bootstrap and bootstrap['status']!='complete'):
                raise RpcError('Active filter lacks completed bootstrap and recovery cursor')
            result.append({'launch_id':target['launch_id'],'kind':kind,'base':base,
                           'query':query})
    if gap_counts(runner.db)[0]:raise RpcError('Active recovery gap blocks shadow')
    return result


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


async def operate(mode):
    if mode=='status':
        # Status must not construct a writer, create schema, or contact a provider.
        settings=FlowSettings.load()
        db=FlowDB(settings.database,readonly=True)
        try:
            revision=subprocess.check_output(('git','-C',str(ROOT),'rev-parse','HEAD'),text=True).strip()
            report=cutover_status(db)
            active=report['current_active']
            proof={}
            if active:
                prefix='cutover:'+active['id']+':%'
                proof={'jobs':db.conn.execute('SELECT count(*) FROM flow_shadow_jobs WHERE stage LIKE ?',
                                              (prefix,)).fetchone()[0],
                       'ranges':db.conn.execute('SELECT count(*) FROM flow_shadow_ranges WHERE stage LIKE ?',
                                                (prefix,)).fetchone()[0]}
            return {'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                    'production':{'revision':revision,'checkout_clean':not subprocess.check_output(
                        ('git','-C',str(ROOT),'status','--porcelain'),text=True).strip(),
                        'main':service('meme-scanner.service'),'flow':service('meme-scanner-flow.service'),
                        'split':settings.split_enabled,'current_wss_provider':db.state('current_wss_provider')},
                    **report,'current_proof':proof,
                    'historical_session_revision_mismatch':any(
                        old['deploy_git_revision']!=revision for old in report['historical_sessions']),
                    'active_session_revision_mismatch':bool(active and active['deploy_git_revision']!=revision)}
        finally:db.conn.close()
    revision=verified_checkout()
    main=service('meme-scanner.service')
    flow=service('meme-scanner-flow.service')
    if main['ActiveState']!='active':raise RpcError('Main service is not active')
    if mode in ('prefetch','preflight','stop-flow') and flow['ActiveState']!='active':
        raise RpcError('Old flow must remain active')
    if mode=='stop-tail' and flow['ActiveState']!='inactive':
        raise RpcError('Stop only flow after preflight')
    if mode=='ready-tail' and flow['ActiveState']!='active':
        raise RpcError('New flow is not active')
    settings=FlowSettings.load()
    if mode in ('archive-legacy','new-session','abort-session'):
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
            await prestart_check()
            if mode=='archive-legacy':
                old=import_rolled_back_legacy(db,flow['MainPID'])
                return {'gate':'LEGACY_LEDGER_ARCHIVED','historical_id':old['id'],
                        'historical_revision':old['revision'],
                        'historical_source_pid':old['source_legacy_pid'],
                        'proof_digest':old['legacy_proof_digest']}
            if mode=='abort-session':
                value=abort_empty(db)
                return {'gate':'EMPTY_SESSION_ABORTED','id':value['id']}
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
            value=create_cutover(db,revision=revision,source_pid=flow['MainPID'],
                                 source_start=flow.get('ExecMainStartTimestampMonotonic'),
                                 main_pid=main['MainPID'],
                                 roles_fingerprint=hashlib.sha256(roles.encode()).hexdigest())
            return {'gate':'CUTOVER_SESSION_CREATED','id':value['id'],'revision':revision,
                    'source_legacy_pid':value['source_legacy_pid'],
                    'H_prefetch':None,'H_stop':None,'H_live':None,'target_filters':0,
                    'fresh_jobs':0,'fresh_ranges':0}
        finally:db.conn.close()
    if mode in ('prefetch','preflight') and settings.split_enabled:
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
            snapshot=filter_snapshot(runner)
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
                if filter_snapshot(runner)!=snapshot:
                    raise RpcError('Active filter snapshot changed during shadow')
                cutover=advance_cutover(db,cutover,'SHADOW_VERIFIED',H_prefetch=head,
                                        targets=snapshot,shadow_proof='verified')
            return {'revision':revision,'H_prefetch':head,**runner.summary('historical'),
                    'gate':'SHADOW_COMPLETE' if complete else 'MIGRATION_BLOCKED'}
        if mode=='preflight':
            if cutover['state']!='SHADOW_VERIFIED' or filter_snapshot(runner)!=cutover['targets']:
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
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            if plan['ready']:
                runner.set_meta('H_pre_stop',head)
                cutover=advance_cutover(db,cutover,'SHADOW_VERIFIED',H_pre_stop=head)
            return {'revision':revision,'chain_ids':identities,'databases':'ok',
                    'service_user_readable':prestart['readable_by_service_user'],**plan,
                    'gate':'CUTOVER_PREFLIGHT_PASS' if plan['ready'] else 'MIGRATION_BLOCKED'}
        if mode=='stop-flow':
            if (cutover['state']!='SHADOW_VERIFIED' or
                filter_snapshot(runner)!=cutover['targets'] or
                not settings.split_enabled or flow['MainPID']!=runner.meta('old_flow_pid') or
                not runner.meta('H_pre_stop') or not runner.complete('historical')):
                raise RpcError('Flow stop gate is incomplete')
            await prestart_check(require_split=True)
            if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Flow database integrity failed')
            if runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Main database integrity failed')
            if gap_counts(db)[0]:raise RpcError('New active flow gap blocks cutover')
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            if not plan['ready']:
                return {'revision':revision,**plan,'gate':'MIGRATION_BLOCKED'}
            subprocess.run(('sudo','-n','systemctl','stop','meme-scanner-flow.service'),check=True)
            return {'revision':revision,**plan,'gate':'FLOW_STOPPED_FOR_CUTOVER'}
        if mode=='stop-tail':
            if flow['ActiveState']!='inactive':raise RpcError('Stop only flow after preflight; main must remain active')
            if (cutover['state']!='SHADOW_VERIFIED' or not runner.meta('H_pre_stop') or
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
    parser.add_argument('mode',choices=('archive-legacy','new-session','abort-session',
                                        'prefetch','preflight','stop-flow','stop-tail','ready-tail','status'))
    args=parser.parse_args()
    try:print(json.dumps(asyncio.run(operate(args.mode)),indent=2))
    except Exception as exc:
        # Never print exception text: provider errors can contain credentials.
        print(json.dumps({'gate':'MIGRATION_BLOCKED','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
