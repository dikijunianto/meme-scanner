"""The planned handoff is durable; ordinary recovery still owns every other gap."""
import asyncio
from contextlib import ExitStack
import copy
import json
from dataclasses import replace
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from app.config import Config
from app.flow_config import safe_split_fingerprint
from app.flow_cutover import advance, create, gap_counts, new, save, schema, session
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders
from app.flow_shadow import make_reconciler
from app.flow_worker import FlowBudget, FlowSettings, FlowWorker
from app.rpc import Rpc, RpcError
from tests.test_flow import event, insert_target, main_schema, target


class CutoverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)
        self.main=main_schema(self.path/'main.db')
        self.db=FlowDB(self.path/'flow.db');self.db.migrate()
        now=time.time();t=target(start=now-60)
        t['coverage_end_at']=now-60
        self.t=insert_target(self.db,t)
        self.base=self.t['launch_block']
        self.config=Config('https://alchemy.invalid',4663,(),'',self.path/'main.db',self.path/'log')
        self.settings=FlowSettings(database=self.path/'flow.db',minute_calls=100)
        self.providers=FlowProviders('https://mainnet.robinhood.validationcloud.io/v1/test',
                                     'wss://mainnet.robinhood.validationcloud.io/v1/test')
        self.runner,self.old_rpc=make_reconciler(self.config,self.settings,self.db,self.providers)
        self.runner.worker.rpc.config=replace(self.runner.worker.rpc.config,rpc_rps=1000)
        self.worker=self.runner.worker

    async def asyncTearDown(self):
        await self.old_rpc.close();await self.worker.rpc.close()
        self.worker.main.close();self.main.close();self.db.conn.close();self.tmp.cleanup()

    def rpc(self,head,logs=(),fail_first=False):
        calls=[]
        async def answer(rpc,payload,method):
            if method=='eth_blockNumber':return {'jsonrpc':'2.0','id':payload['id'],'result':hex(head)}
            self.assertEqual(method,'eth_getLogs')
            q=payload['params'][0];first,last=int(q['fromBlock'],16),int(q['toBlock'],16)
            calls.append((first,last))
            if fail_first and len(calls)==1:raise RpcError('isolated failure')
            return {'jsonrpc':'2.0','id':payload['id'],'result':[
                copy.deepcopy(x) for x in logs if first<=int(x['blockNumber'],16)<=last]}
        p=patch.object(Rpc,'_send',answer);p.start();self.addCleanup(p.stop);self.patch=p
        return calls

    async def shadowed_pre_stop(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        from scripts import phase2b2_shadow as script
        self.rpc(self.base+8)
        schema(self.db)
        with self.db.conn:self.db.conn.execute('''INSERT INTO flow_cutover_sessions
          (id,created_at_utc,deploy_git_revision,source_route,source_legacy_pid,status,payload)
          VALUES('old','2026-09-01T00:00:00Z','old-revision','alchemy','old-pid','ROLLED_BACK',?)''',
          (json.dumps({'id':'old','state':'ROLLED_BACK','revision':'old-revision'}),))
        created=create(self.db,revision='test',source_pid='old',source_start='100',
                       main_pid='main',roles_fingerprint='roles')
        self.runner.session_id=created['id']
        self.db.set_state('recovery:1:curve',self.base+5)
        for key,value in (('git_revision','test'),('main_pid','main'),('old_flow_pid','old'),
                          ('provider_chain_ids_verified','{}'),('H_prefetch',self.base+5)):
            self.runner.set_meta(key,value)
        self.runner.add_jobs('historical',0,self.base+5)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.db.set_state('current_wss_provider','alchemy')
        self.db.set_state('service_status','connected')
        self.db.set_state('connection_state','connected')
        self.db.set_state('recovery_state','healthy')
        snapshot=script.filter_snapshot(self.db)
        advance(self.db,created,'SHADOW_VERIFIED',H_prefetch=self.base+5,
                targets=snapshot,shadow_proof='verified')
        return script

    def operator_patches(self,script,state):
        stack=ExitStack()
        settings=replace(self.settings,split_enabled=False)
        candidate={'candidate_fingerprint':'candidate',
                   'actual_fingerprint':safe_split_fingerprint(settings,self.providers),
                   'legacy_file_digest':'legacy-digest','readable_by_service_user':True,
                   'owner_uid':1000,'owner_gid':1000,'mode':'0600','parent_mode':'0700'}
        def service(name):
            if name=='meme-scanner.service':return {'ActiveState':'active','MainPID':'main',
                                                     'ExecMainStartTimestampMonotonic':'1'}
            return {'ActiveState':'active' if state['active'] else 'inactive',
                    'MainPID':'old' if state['active'] else '0',
                    'ExecMainStartTimestampMonotonic':'100' if state['active'] else '0'}
        for target,name,value in ((script,'verified_checkout','test'),
                                  (script.FlowSettings,'load',settings),
                                  (script.Config,'load',self.config),
                                  (script.FlowProviders,'load',self.providers)):
            stack.enter_context(patch.object(target,name,return_value=value))
        stack.enter_context(patch.object(script,'service',side_effect=service))
        stack.enter_context(patch.object(script,'FlowDB',side_effect=lambda _:FlowDB(self.path/'flow.db')))
        stack.enter_context(patch.object(script,'candidate_split_preflight',
                                         AsyncMock(return_value=candidate)))
        stack.enter_context(patch.object(script,'candidate_split_status',return_value=candidate))
        stack.enter_context(patch.object(script,'prestart_check',
                                         AsyncMock(return_value={'no_network':True})))
        return stack

    async def prepared(self,head=None,logs=()):
        head=head or self.base+184
        calls=self.rpc(head,logs)
        schema(self.db)
        with self.db.conn:self.db.conn.execute('''INSERT INTO flow_cutover_sessions
          (id,created_at_utc,deploy_git_revision,source_route,source_legacy_pid,status,payload)
          VALUES('old','2026-09-01T00:00:00Z','old-revision','alchemy','old-pid','ROLLED_BACK',?)''',
          (json.dumps({'id':'old','state':'ROLLED_BACK','revision':'old-revision'}),))
        created=create(self.db,revision='test',source_pid='old',source_start='1',
                       main_pid='main',roles_fingerprint='roles')
        self.runner.session_id=created['id']
        self.runner.set_meta('H_prefetch',self.base+5)
        self.runner.add_jobs('historical',0,self.base+5)
        self.assertTrue(await self.runner.run_stage('historical'))
        advance(self.db,created,'SHADOW_VERIFIED',H_prefetch=self.base+5,shadow_proof='verified')
        self.runner.set_meta('H_stop',self.base+8)
        self.runner.add_jobs('stop_tail',self.base+6,self.base+8)
        self.assertTrue(await self.runner.run_stage('stop_tail'))
        self.runner.promote('historical');self.runner.promote('stop_tail')
        self.db.set_state('recovery:1:curve',self.base+8)
        advance(self.db,session(self.db),'SOURCE_STOPPED',source_stopped_at='2026-09-27T00:00:00Z')
        value=new(self.db,self.base+5,self.base+8,[{'launch_id':1,'kind':'curve',
            'base':self.base,'query':self.worker.filters(self.t)['curve']}])
        advance(self.db,value,'SPLIT_CONFIGURED')
        self.runner.set_meta('cutover_session_id',value['id'])
        self.worker.command=AsyncMock(return_value='sub')
        self.worker.discover=AsyncMock()
        self.worker.connection_started_at=time.time()-5
        self.db.set_state('service_status','cutover_handoff_pending')
        self.db.set_state('current_wss_provider','publicnode')
        return calls

    async def test_oversized_startup_waits_for_proof_then_connects_with_wss_overlap(self):
        row=event(self.t,at=int(time.time()),index=2);row['blockNumber']=hex(self.base+9)
        calls=await self.prepared(logs=[row])
        with self.assertRaises(FlowBudget) as error:self.worker.recovery_plan(self.t,self.base+184)
        self.assertEqual(error.exception.scope,'recovery_range')
        self.assertEqual(error.exception.used,179)
        before=len(calls)
        await self.worker.reconcile();self.worker.finalize(True)
        self.assertEqual(len(calls),before)
        self.assertEqual(self.db.state('service_status'),'cutover_handoff_pending')
        self.assertEqual(session(self.db)['state'],'READY_TAIL_PENDING')
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+8))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_features').fetchone()[0],0)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_gaps').fetchone()[0],0)
        self.worker.ingest(self.t,copy.deepcopy(row))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+8))
        value=session(self.db);value['H_live']=self.base+184;value['H_live_at']=time.time()
        with self.db.conn:save(self.db,value)
        self.runner.set_meta('H_live',self.base+184);self.runner.set_meta('H_live_at',int(time.time()))
        self.runner.add_jobs('wss_ready_tail',self.base+9,self.base+184,{1})
        self.assertTrue(await self.runner.run_stage('wss_ready_tail'))
        self.assertEqual(self.runner.summary('wss_ready_tail')['duplicates'],1)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.runner.promote('wss_ready_tail')
        self.assertEqual(session(self.db)['state'],'READY_TAIL_VERIFIED')
        await self.worker.reconcile()
        self.assertEqual(session(self.db)['state'],'NORMAL_CONNECTED')
        self.assertEqual(self.db.state('service_status'),'connected')
        self.assertEqual(len(calls),before+1)
        self.assertEqual(self.db.used('flow_eth_getTransactionByHash',0),0)
        self.assertEqual(self.db.used('flow_eth_getTransactionReceipt',0),0)

    async def test_unexpected_active_gap_fails_pending_cutover(self):
        await self.prepared()
        self.db.gap(1,time.time()-2,time.time(),'ws_gap',self.base+9)
        await self.worker.reconcile()
        self.assertEqual(session(self.db)['state'],'FAILED')
        self.assertEqual(self.db.state('service_status'),'cutover_failed')
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+8))

    async def test_preexisting_active_gap_blocks_but_expired_gap_does_not_poison_connection(self):
        old=target(launch=2,start=time.time()-5000)
        old['token_address']='0x'+'12'*20;old['curve_address']='0x'+'13'*20
        insert_target(self.db,old)
        with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=2")
        self.db.gap(2,time.time()-40,time.time()-20,'ws_gap',self.base)
        self.assertEqual(gap_counts(self.db),(0,1))
        await self.prepared()
        await self.worker.reconcile()
        self.assertEqual(session(self.db)['state'],'READY_TAIL_PENDING')
        self.assertEqual(self.db.state('historical_unresolved_gap_count'),'1')
        self.db.gap(1,time.time()-5,time.time()-2,'ws_gap',self.base+9)
        self.assertEqual(gap_counts(self.db),(1,1))

    async def test_partial_ready_tail_restarts_at_first_unverified_chunk(self):
        await self.prepared(head=self.base+4008)
        await self.worker.reconcile()
        value=session(self.db);value['H_live']=self.base+4008;value['H_live_at']=time.time()
        with self.db.conn:save(self.db,value)
        self.runner.set_meta('H_live',self.base+4008)
        self.runner.set_meta('H_live_at',int(time.time()))
        self.runner.add_jobs('wss_ready_tail',self.base+9,self.base+4008,{1})
        original=self.worker.ingest
        row=event(self.t,at=int(time.time()),index=4);row['blockNumber']=hex(self.base+2009)
        self.patch.stop()
        calls=self.rpc(self.base+4008,[row])
        def crash(*args,**kwargs):
            original(*args,**kwargs)
            raise OSError('isolated crash after event commit')
        with patch.object(self.worker,'ingest',side_effect=crash):
            with self.assertRaises(OSError):await self.runner.run_stage('wss_ready_tail')
        self.assertEqual(self.runner.summary('wss_ready_tail')['jobs'][0]['next_unverified_block'],self.base+2009)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.assertTrue(await self.runner.run_stage('wss_ready_tail'))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.assertEqual(calls[1][0],calls[2][0])
        self.assertEqual(session(self.db)['state'],'READY_TAIL_PENDING')
        self.runner.promote('wss_ready_tail')
        self.assertEqual(session(self.db)['state'],'READY_TAIL_VERIFIED')

    async def test_operator_ready_tail_runs_while_status_is_handoff_pending(self):
        await self.prepared()
        await self.worker.reconcile();self.worker.finalize(True)
        next_sample=int(session(self.db)['subscription_ready_at'])//30*30+30
        with self.db.conn:self.db.conn.execute('''INSERT OR REPLACE INTO flow_samples(at,active,subscriptions,
          curve_subscriptions,v4_subscriptions,hook_subscriptions,db_bytes) VALUES(?,1,1,1,0,0,0)''',(next_sample,))
        self.runner.set_meta('main_pid','main')
        self.runner.set_meta('old_flow_pid','old')
        advance(self.db,session(self.db),'READY_TAIL_PENDING',split_pid='new')
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        from scripts import phase2b2_shadow
        def service(name):
            return {'ActiveState':'active','MainPID':'main' if name=='meme-scanner.service' else 'new'}
        with (patch.object(phase2b2_shadow,'verified_checkout',return_value='test'),
              patch.object(phase2b2_shadow,'service',side_effect=service),
              patch.object(phase2b2_shadow.FlowSettings,'load',return_value=self.settings),
              patch.object(phase2b2_shadow.Config,'load',return_value=self.config),
              patch.object(phase2b2_shadow.FlowProviders,'load',return_value=self.providers),
              patch.object(phase2b2_shadow,'FlowDB',side_effect=lambda _:FlowDB(self.path/'flow.db')),
              patch.object(phase2b2_shadow,'prestart_check',AsyncMock(return_value={'no_network':True}))):
            result=await phase2b2_shadow.operate('ready-tail')
        self.assertEqual(result['gate'],'READY_FOR_30_MIN_VALIDATION')
        self.assertEqual(result['H_live'],self.base+184)
        self.assertEqual(session(self.db)['state'],'READY_TAIL_VERIFIED')

    async def test_session_survives_restart_and_legacy_rollback_archives_it(self):
        await self.prepared()
        await self.worker.reconcile()
        self.assertEqual(session(self.db)['state'],'READY_TAIL_PENDING')
        restarted=FlowWorker(self.config,self.settings,self.db,self.providers)
        try:
            restarted.command=AsyncMock(return_value='new-sub')
            restarted.discover=AsyncMock()
            await restarted.reconcile()
            self.assertEqual(session(self.db)['state'],'READY_TAIL_PENDING')
            self.assertEqual(self.db.state('service_status'),'cutover_handoff_pending')
            self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_gaps').fetchone()[0],0)
        finally:
            await restarted.rpc.close();restarted.main.close()
        legacy=FlowWorker(self.config,replace(self.settings,split_enabled=False),self.db,self.providers)
        class Cancel:
            def __aenter__(self):raise asyncio.CancelledError
            async def __aexit__(self,*args):pass
        try:
            with patch('app.flow_worker.connect',return_value=Cancel()):
                with self.assertRaises(asyncio.CancelledError):await legacy.run()
            self.assertEqual(session(self.db)['state'],'ROLLED_BACK')
        finally:
            await legacy.rpc.close();legacy.main.close()

    async def test_crash_before_ack_keeps_session_and_cursor_unadvanced(self):
        await self.prepared()
        before=self.db.state('recovery:1:curve')
        replacement=FlowWorker(self.config,self.settings,self.db,self.providers)
        class Cancel:
            def __aenter__(self):raise asyncio.CancelledError
            async def __aexit__(self,*args):pass
        try:
            with patch('app.flow_worker.connect',return_value=Cancel()):
                with self.assertRaises(asyncio.CancelledError):await replacement.run()
            self.assertEqual(session(self.db)['state'],'SPLIT_WSS_CONNECTING')
            self.assertEqual(self.db.state('recovery:1:curve'),before)
            self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_gaps').fetchone()[0],0)
        finally:
            await replacement.rpc.close();replacement.main.close()

    async def test_verified_proof_survives_crash_before_worker_transition(self):
        await self.prepared()
        await self.worker.reconcile()
        value=session(self.db);value['H_live']=self.base+10;value['H_live_at']=time.time()
        with self.db.conn:save(self.db,value)
        self.runner.set_meta('H_live',self.base+10);self.runner.set_meta('H_live_at',int(time.time()))
        self.runner.add_jobs('wss_ready_tail',self.base+9,self.base+10,{1})
        self.assertTrue(await self.runner.run_stage('wss_ready_tail'))
        self.runner.promote('wss_ready_tail')
        self.assertEqual(session(self.db)['state'],'READY_TAIL_VERIFIED')
        with self.assertRaises(RpcError):self.runner.promote('wss_ready_tail')
        replacement=FlowWorker(self.config,self.settings,self.db,self.providers)
        try:
            replacement.subscriptions[(1,'curve')]='ack'
            replacement.discover=AsyncMock()
            await replacement.reconcile()
            self.assertEqual(session(self.db)['state'],'NORMAL_CONNECTED')
            self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+10))
        finally:
            await replacement.rpc.close();replacement.main.close()

    async def test_named_expired_gaps_need_contiguous_proof(self):
        await self.prepared(head=self.base+12)
        now=time.time()
        with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=1")
        self.db.gap(1,now-5,now-2,'ws_gap',self.base+2)
        gap_id=self.db.conn.execute('SELECT max(id) FROM flow_gaps').fetchone()[0]
        self.runner.add_jobs('failed_cutover_cleanup_tail',self.base+9,self.base+12,{1})
        with self.assertRaises(RpcError):self.runner.resolve_failed_cutover_gaps([gap_id],self.base+12,now)
        self.assertTrue(await self.runner.run_stage('failed_cutover_cleanup_tail'))
        self.assertEqual(self.runner.resolve_failed_cutover_gaps([gap_id],self.base+12,now),[gap_id])
        self.assertEqual(self.runner.resolve_failed_cutover_gaps([gap_id],self.base+12,now),[])
        self.assertEqual(self.db.conn.execute('SELECT resolved FROM flow_gaps WHERE id=?',(gap_id,)).fetchone()[0],1)

    async def test_stop_authorization_accepts_legacy_split_false_then_records_source_stop(self):
        script=await self.shadowed_pre_stop();state={'active':True}
        with self.operator_patches(script,state):
            authorized=await script.operate('authorize-stop')
            self.assertEqual(authorized['gate'],'STOP_AUTHORIZED')
            self.assertTrue(state['active'])
            self.assertEqual(session(self.db)['state'],'STOP_AUTHORIZED')
            self.assertIsNone(session(self.db).get('source_stopped_at'))
            def stop(*args,**kwargs):
                state['active']=False
                return None
            with patch.object(script.subprocess,'run',side_effect=stop) as command:
                stopped=await script.operate('stop-flow')
                command.assert_called_once()
            self.assertEqual(stopped['gate'],'SOURCE_STOPPED')
            self.assertEqual(session(self.db)['source_legacy_pid'],'old')
            self.assertIsNotNone(session(self.db)['source_stopped_at'])

    async def test_target_expiry_refuses_stop_and_shadowed_authorization_aborts(self):
        script=await self.shadowed_pre_stop();state={'active':True}
        with self.operator_patches(script,state):
            self.assertEqual((await script.operate('authorize-stop'))['gate'],'STOP_AUTHORIZED')
            with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=1")
            with patch.object(script.subprocess,'run') as stop:
                with self.assertRaises(RpcError):await script.operate('stop-flow')
                stop.assert_not_called()
            aborted=await script.operate('abort-session','target_expired')
            self.assertEqual(aborted['gate'],'PRE_STOP_SESSION_ABORTED')
            self.assertEqual(session(self.db)['state'],'ABORTED_PRE_STOP')
            self.assertGreater(self.db.conn.execute('SELECT count(*) FROM flow_shadow_ranges WHERE stage LIKE ?',
                (f'cutover:{aborted["id"]}:%',)).fetchone()[0],0)
            fresh=await script.operate('new-session')
            self.assertNotEqual(fresh['id'],aborted['id'])
            self.assertIsNone(fresh['H_prefetch'])

    async def test_stop_failure_and_crash_recovery_preserve_exact_source(self):
        script=await self.shadowed_pre_stop();state={'active':True}
        with self.operator_patches(script,state):
            await script.operate('authorize-stop')
            with patch.object(script.subprocess,'run',side_effect=OSError('stop failed')):
                with self.assertRaises(OSError):await script.operate('stop-flow')
            self.assertEqual(session(self.db)['state'],'STOP_AUTHORIZED')
            self.assertTrue(state['active'])
            self.assertIsNotNone(session(self.db)['stop_command_issued_at'])
            state['active']=False
            with patch.object(script,'source_process_gone',return_value=False), \
                 patch.object(script.subprocess,'run') as stop:
                with self.assertRaises(RpcError):await script.operate('stop-flow')
                stop.assert_not_called()
            with patch.object(script,'source_process_gone',return_value=True), \
                 patch.object(script.subprocess,'run') as stop:
                recovered=await script.operate('stop-flow')
                stop.assert_not_called()
            self.assertTrue(recovered['recovered_after_crash'])
            self.assertEqual(session(self.db)['state'],'SOURCE_STOPPED')

    async def test_split_true_refuses_pre_stop_without_service_change(self):
        script=await self.shadowed_pre_stop();state={'active':True}
        with self.operator_patches(script,state):
            await script.operate('authorize-stop')
            with patch.object(script.FlowSettings,'load',return_value=replace(self.settings,split_enabled=True)), \
                 patch.object(script.subprocess,'run') as stop:
                with self.assertRaises(RpcError):await script.operate('stop-flow')
                stop.assert_not_called()
            self.assertEqual(session(self.db)['state'],'STOP_AUTHORIZED')
            self.assertTrue(state['active'])


if __name__=='__main__':unittest.main()
