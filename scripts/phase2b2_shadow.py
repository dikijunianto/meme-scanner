"""Operator-controlled, fail-closed Phase 2B.2 shadow and tail reconciliation."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
import subprocess
import time

import httpx
from websockets.asyncio.client import connect

from app.config import Config
from app.flow_config import prestart_check
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders
from app.flow_shadow import make_reconciler, verified_checkout
from app.flow_worker import FlowSettings
from app.rpc import RpcError


def service(name):
    result=subprocess.run(('systemctl','show',name,'-p','ActiveState','-p','MainPID','-p','NRestarts'),
                          capture_output=True,text=True,check=True)
    return dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line)


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
    if mode in ('prefetch','preflight') and settings.split_enabled:
        raise RpcError('Old flow routing flag unexpectedly enabled')
    if mode=='ready-tail' and not settings.split_enabled:
        raise RpcError('New flow routing flag is not enabled')
    db=FlowDB(settings.database)
    config=Config.load();providers=FlowProviders.load()
    runner,old_rpc=make_reconciler(config,settings,db,providers)
    try:
        await old_rpc.close()
        pinned=runner.meta('git_revision')
        if pinned and pinned!=revision:raise RpcError('Migration source revision changed')
        if mode=='prefetch' and not pinned:runner.set_meta('git_revision',revision)
        previous=runner.meta('main_pid')
        if previous and previous!=main['MainPID']:raise RpcError('Main PID changed during migration')
        if mode=='prefetch':
            if flow['ActiveState']!='active':raise RpcError('Old flow must remain active')
            prior_flow=runner.meta('old_flow_pid')
            if prior_flow and prior_flow!=flow['MainPID']:raise RpcError('Old flow PID changed during shadow')
            runner.set_meta('main_pid',main['MainPID'])
            runner.set_meta('old_flow_pid',flow['MainPID'])
            head=await runner.start_historical()
            complete=await runner.run_stage('historical')
            return {'revision':revision,'H_prefetch':head,**runner.summary('historical'),
                    'gate':'SHADOW_COMPLETE' if complete else 'MIGRATION_BLOCKED'}
        if mode=='preflight':
            if flow['ActiveState']!='active' or flow['MainPID']!=runner.meta('old_flow_pid'):
                raise RpcError('Old flow is not continuously active')
            runner.add_jobs('historical',0,int(runner.meta('H_prefetch')))
            if not runner.complete('historical'):raise RpcError('Historical shadow reconciliation incomplete')
            prestart=await prestart_check()
            if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Flow database integrity failed')
            if runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Main database integrity failed')
            identities=await chain_ids(config,providers)
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            if plan['ready']:runner.set_meta('H_pre_stop',head)
            return {'revision':revision,'chain_ids':identities,'databases':'ok',
                    'service_user_readable':prestart['readable_by_service_user'],**plan,
                    'gate':'CUTOVER_PREFLIGHT_PASS' if plan['ready'] else 'MIGRATION_BLOCKED'}
        if mode=='stop-flow':
            if (not settings.split_enabled or flow['MainPID']!=runner.meta('old_flow_pid') or
                not runner.meta('H_pre_stop') or not runner.complete('historical')):
                raise RpcError('Flow stop gate is incomplete')
            await prestart_check(require_split=True)
            if db.conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Flow database integrity failed')
            if runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RpcError('Main database integrity failed')
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            plan=runner.tail_plan(head)
            if not plan['ready']:
                return {'revision':revision,**plan,'gate':'MIGRATION_BLOCKED'}
            subprocess.run(('sudo','-n','systemctl','stop','meme-scanner-flow.service'),check=True)
            return {'revision':revision,**plan,'gate':'FLOW_STOPPED_FOR_CUTOVER'}
        if mode=='stop-tail':
            if flow['ActiveState']!='inactive':raise RpcError('Stop only flow after preflight; main must remain active')
            if not runner.meta('H_pre_stop'):raise RpcError('No passed pre-stop gate')
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
            return {'revision':revision,'H_stop':head,**runner.summary('stop_tail'),
                    'gate':'STOP_TAIL_COMPLETE' if complete else 'ROLLBACK_REQUIRED'}
        if mode=='ready-tail':
            if flow['ActiveState']!='active' or flow['MainPID']==runner.meta('old_flow_pid'):
                raise RpcError('New flow service is not active')
            if not runner.complete('stop_tail'):raise RpcError('Stop tail incomplete')
            if (runner.db.state('service_status') not in ('connected','connected_http_budget') or
                runner.db.state('current_wss_provider')!='publicnode'):
                raise RpcError('PublicNode WSS is not confirmed ready')
            sample=runner.db.conn.execute('SELECT subscriptions FROM flow_samples WHERE at>=? ORDER BY at DESC LIMIT 1',
                                          (int(runner.meta('H_stop_at'))//30*30+30,)).fetchone()
            if not sample or sample[0]<1:raise RpcError('Active WSS subscriptions are not acknowledged')
            head=int(await runner.worker.rpc.call('eth_blockNumber',[]),16)
            runner.set_meta('H_live',head)
            runner.set_meta('H_live_at',int(time.time()))
            runner.add_jobs('wss_ready_tail',int(runner.meta('H_stop'))+1,head)
            complete=await runner.run_stage('wss_ready_tail')
            if complete:runner.promote('wss_ready_tail')
            return {'revision':revision,'H_live':head,**runner.summary('wss_ready_tail'),
                    'gate':'READY_FOR_30_MIN_VALIDATION' if complete else 'ROLLBACK_REQUIRED'}
        if mode=='status':
            return {'revision':revision,'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                    'H_prefetch':runner.meta('H_prefetch'),'H_pre_stop':runner.meta('H_pre_stop'),
                    'H_stop':runner.meta('H_stop'),'H_live':runner.meta('H_live'),
                    'historical':runner.summary('historical'),'stop_tail':runner.summary('stop_tail'),
                    'wss_ready_tail':runner.summary('wss_ready_tail')}
        raise ValueError('Unknown mode')
    finally:
        await runner.worker.rpc.close()
        runner.worker.main.close()
        db.conn.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('prefetch','preflight','stop-flow','stop-tail','ready-tail','status'))
    args=parser.parse_args()
    try:print(json.dumps(asyncio.run(operate(args.mode)),indent=2))
    except Exception as exc:
        # Never print exception text: provider errors can contain credentials.
        print(json.dumps({'gate':'MIGRATION_BLOCKED','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
