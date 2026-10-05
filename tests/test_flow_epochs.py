import hashlib
import json
import time
import unittest
from dataclasses import replace
from unittest.mock import patch

from app import flow_epochs as epochs,flow_provider_switch as switch
from app.flow_bootstrap import CursorBootstrap
from app.flow_data import FlowDB
from app.flow_shadow import make_reconciler
from app.rpc import Rpc,RpcError
from tests import test_flow_bootstrap as fixtures
from tests.test_flow import target,insert_target,fixture


class EpochTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=fixtures.BootstrapTests.asyncSetUp
    asyncTearDown=fixtures.BootstrapTests.asyncTearDown

    def failure(self):
        self.db.activate_pit_ledger('a'*40,self.base,900)
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.base+5)
        with patch('app.flow_data.time.time',return_value=1035):self.db.rebuild(self.t,1035)
        self.immutable=[dict(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')]
        self.assertTrue(any(r['model_eligible_at'] for r in self.immutable))
        self.db.gap(1,1040,1100,'ws_gap',self.base+5)
        self.db.gap(1,1040,1100,'ws_gap',self.base+6)
        self.sid,_=switch.start(self.db,'validation',
            [{'launch_id':1,'kind':'curve','base':self.base,'cursor':str(self.base+5)}]*2,[2,3],now=1100)
        switch.failed(self.db,self.sid,'FlowBudget')
        self.raw=switch.latest(self.db)['payload']
        self.before=[dict(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')]

    def quarantine(self):
        return epochs.quarantine(self.db,epoch_id='epoch1',start_block=self.base,start_at=1000,
            revision='a'*40,incident_at=1100,switch_id=self.sid,estimated_calls=2514,
            reason='historical_collection_epoch_closed')

    def fake_rpc(self,head=None,at=1200,bad_hash=False):
        self.runner.worker.rpc.settings=replace(self.settings,minute_calls=12)
        head=head or self.base+20;self.calls=[]
        async def answer(rpc,payload,method):
            self.calls.append(method)
            if method=='eth_chainId':value=hex(4663)
            elif method=='eth_blockNumber':value=hex(head)
            elif method=='eth_getBlockByNumber':value={'number':hex(head),'timestamp':hex(at),'hash':'bad' if bad_hash else '0x'+'11'*32}
            elif method=='eth_getLogs':value=[]
            else:raise AssertionError(method)
            return {'jsonrpc':'2.0','id':payload['id'],'result':value}
        p=patch.object(Rpc,'_send',answer);p.start();self.addCleanup(p.stop)

    async def rollover(self):
        self.failure();self.quarantine();self.fake_rpc()
        with patch('app.flow_epochs.time.time',return_value=1200):
            await epochs.prepare(self.runner,epoch_id='epoch2',revision='b'*40,reason='new clean attempt',predecessor='epoch1')
        self.fresh=FlowDB(self.db.path)
        self.addCleanup(self.fresh.close)
        runner,old=make_reconciler(self.config,self.settings,self.fresh,self.providers)
        await old.close();self.addAsyncCleanup(runner.worker.rpc.close);self.addCleanup(runner.worker.main.close)
        runner.worker.rpc.config=replace(runner.worker.rpc.config,rpc_rps=100000)
        return runner

    async def test_active_failed_blocks_quarantine_preserves_history_and_new_ids(self):
        runner=await self.rollover()
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_gaps WHERE resolved=0').fetchone()[0],2)
        self.assertFalse(switch.blocked(self.fresh));self.assertIsNone(switch.pending(self.fresh))
        self.assertEqual(epochs.historical_debt(self.fresh)[0]['retained_switch_state'],'FAILED')
        sid,_=switch.start(self.fresh,'publicnode',[{'launch_id':2}],[1],now=1300)
        self.assertGreater(sid,self.sid);switch.failed(self.fresh,sid,'real failure')
        self.assertTrue(switch.blocked(self.fresh))
        self.assertEqual(switch.report(self.fresh)['provider_switch_unresolved_ranges'],1)
        self.assertEqual(switch.report(self.db)['provider_switch_unresolved_ranges'],2)
        with self.assertRaises(ValueError):switch.start(self.db,'validation',[],[])

    async def test_fresh_boundary_required_and_no_cursor_inheritance(self):
        self.failure();self.assertTrue(switch.blocked(self.db));self.quarantine()
        self.fake_rpc(bad_hash=True)
        with patch('app.flow_epochs.time.time',return_value=1200),self.assertRaisesRegex(ValueError,'boundary'):
            await epochs.prepare(self.runner,epoch_id='epoch2',revision='b'*40,reason='fresh',predecessor='epoch1')
        self.assertFalse(self.db.conn.execute("SELECT 1 FROM flow_collection_epochs WHERE status='ACTIVE'").fetchone())
        self.assertEqual(self.calls,['eth_chainId','eth_blockNumber','eth_getBlockByNumber'])
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+5))

    async def test_partition_null_bootstrap_proof_before_cursor_and_false_complete(self):
        runner=await self.rollover();t=insert_target(self.fresh,target(start=1210))
        with self.fresh.conn:self.fresh.conn.execute('UPDATE flow_tracking_targets SET launch_block=? WHERE launch_id=1',(self.base+21,))
        t=self.fresh.target(1);worker=runner.worker;worker.epoch_discovery_ready=True
        self.assertIsNone(self.fresh.state('recovery:1:curve'))
        with self.assertRaisesRegex(RpcError,'explicit bootstrap'):worker.recovery_plan(t,self.base+5000)
        worker.ensure_bootstrap_state(t)
        self.assertEqual(self.fresh.conn.execute('SELECT status FROM flow_bootstrap').fetchone()[0],'required')
        self.fake_rpc(self.base+5020,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            result=await CursorBootstrap(runner).run('live_bootstrap:1',[1],max_chunks=1)
        self.assertNotEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE');self.assertIsNone(self.fresh.state('recovery:1:curve'))
        with self.assertRaises(RpcError):CursorBootstrap(runner).promote('live_bootstrap:1')
        self.assertFalse(self.fresh.epoch()['pit_eligible'])
        self.assertGreater(self.fresh.used('flow_eth_getLogs',0),0)
        self.assertEqual(self.fresh.used('flow_eth_getLogs',0),self.db.used('flow_eth_getLogs',0))

    async def test_materializer_rollover_old_missing_stays_missing_actual_availability(self):
        runner=await self.rollover();worker=runner.worker;worker.epoch_discovery_ready=True
        t=target(start=1210);t['launch_block']=self.base+21
        t=insert_target(self.fresh,t);worker.ensure_bootstrap_state(t)
        self.fake_rpc(self.base+50,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            result=await CursorBootstrap(runner).run('live_bootstrap:1',[1])
        self.assertEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        worker.subscriptions={(1,'curve'):'ACK'};worker.pending_recovery.add(1)
        self.assertFalse(epochs.seal_live(worker))  # Bootstrap alone cannot seal the subscription/recovery tail.
        worker.pending_recovery.clear()
        self.assertFalse(epochs.seal_live(worker))  # No durable acknowledged tail yet.
        worker.subscription_ready_at=1300
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(self.fresh.target(1)))
        first_tail=self.fresh.state('epoch_tail_proof:1:curve')
        with patch('app.flow_epochs.time.time',return_value=1306):
            await worker.recover_plans(worker.recovery_plan(self.fresh.target(1),self.base+60))
        self.assertEqual(self.fresh.state('epoch_tail_proof:1:curve'),first_tail)
        with patch('app.flow_epochs.time.time',return_value=1306):self.assertTrue(epochs.seal_live(worker))
        with patch('app.flow_data.time.time',return_value=1310):self.fresh.rebuild(self.fresh.target(1),1300)
        current=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL')]
        self.assertTrue(current)
        self.assertTrue(all(r['model_eligible_at']==1310 and json.loads(r['proof_json'])['collection_epoch_id']=='epoch2' for r in current))
        self.assertEqual([dict(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')],self.before)
        for r in self.before:self.assertEqual(hashlib.sha256(r['payload'].encode()).hexdigest(),r['payload_sha256'])
        # Historical gaps do not contaminate the fresh partition, but current gaps do.
        self.fresh.gap(1,1210,1300,'current_gap',self.base+21)
        self.fresh.rebuild(self.fresh.target(1),1300)
        latest=self.fresh.conn.execute('SELECT model_eligible_at FROM flow_feature_versions ORDER BY version_number DESC LIMIT 1').fetchone()
        self.assertIsNone(latest[0])

    async def test_current_failed_switch_prevents_pit_even_after_initial_seal(self):
        runner=await self.rollover();worker=runner.worker
        t=await self.bootstrap_epoch_target(runner)
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(t))
            self.assertTrue(epochs.seal_live(worker))
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVE')
        sid,_=switch.start(self.fresh,'publicnode',[{'launch_id':1}],[]);switch.failed(self.fresh,sid,'real_failure')
        self.fresh.rebuild(t,1300)
        self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL').fetchone())

    async def test_epoch_graduation_explicit_curve_v4_hook_and_spanning_proof(self):
        runner=await self.rollover();worker=runner.worker;worker.epoch_discovery_ready=True
        t=target(start=1210);t['launch_block']=self.base+21
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+30;g['block_timestamp']='1970-01-01T00:20:40+00:00'
        t['graduation_json']=json.dumps(g);t['pool_id']=g['pool_id']
        t=insert_target(self.fresh,t);worker.ensure_bootstrap_state(t)
        self.assertEqual(worker.bootstrap_kinds(t),{'curve','v4','hook'})
        self.fake_rpc(self.base+500,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            result=await CursorBootstrap(runner).run('live_graduation:1',[1])
        self.assertEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        self.assertEqual({j['kind']:j['original_safe_start'] for j in result['jobs']},
                         {'curve':self.base+21,'v4':self.base+30,'hook':self.base+30})
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        worker.subscriptions={(1,k):'ACK' for k in ('v4','hook')}
        worker.subscription_ready_at=1300
        with patch('app.flow_epochs.time.time',return_value=1305):
            for _ in range(5):
                if await worker.activation_tail(self.fresh.target(1)):break
            else:self.fail('Graduation tail did not complete')
        self.fresh.set_state('service_status','connected')
        with patch('app.flow_epochs.time.time',return_value=1306):self.assertTrue(epochs.seal_live(worker))
        with patch('app.flow_data.time.time',return_value=1310):self.fresh.rebuild(self.fresh.target(1),1300)
        row=self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=60 ORDER BY version_number DESC').fetchone()
        self.assertEqual(row['model_eligible_at'],1310)
        self.assertEqual({p['kind'] for p in json.loads(row['proof_json'])['filters']},{'curve','v4','hook'})
        with self.assertRaises(Exception):worker.recovery_plan(self.fresh.target(1),self.base+1000)

    async def test_idempotent_schema_quarantine_and_readonly_partition_selection(self):
        runner=await self.rollover()
        epochs.schema(self.db.conn);self.quarantine()
        read=FlowDB(self.db.path,readonly=True)
        try:
            self.assertEqual(read.epoch()['epoch_id'],'epoch2')
            with self.assertRaises(Exception):read.set_state('attempted_write',1)
        finally:read.close()
        with self.assertRaisesRegex(ValueError,'already exists'):
            await epochs.prepare(self.runner,epoch_id='epoch2',revision='b'*40,reason='reset',predecessor='epoch1')

    async def test_shared_budget_cannot_reset_at_epoch_rollover(self):
        from app.flow_budget import BudgetWait
        runner=await self.rollover()
        self.db.count('flow_eth_getLogs',350,now=1300)
        self.fake_rpc(self.base+100,1300)
        with patch('app.flow_epochs.time.time',return_value=1300),self.assertRaises(BudgetWait):
            await runner.worker.rpc.call('eth_getLogs',[{}])
        self.assertEqual(self.calls,[])
        self.assertEqual(self.fresh.used('flow_eth_getLogs',0),350)

    async def test_maturity_counts_one_epoch_only_with_real_label_availability(self):
        from scripts.phase2c_dataset_audit import open_readonly
        from scripts.phase2c_point_in_time_audit import ledger_audit
        from app.flow_data import iso
        await self.test_materializer_rollover_old_missing_stays_missing_actual_availability()
        t=self.fresh.target(1)
        with self.main:
            self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',
                (1,t['token_address'],t['quote_asset_address'],t['curve_address'],t['creator_address'],iso(1210),t['launch_block'],0,1))
            self.main.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',(1,86400,'random_long',iso(1210+86400)))
            for age,at,price in ((0,1211,'1'),(86400,87700,'2')):
                self.main.execute('INSERT INTO market_snapshots VALUES(?,?,?,?,?,?)',(1,age,t['quote_asset_address'],iso(at),'verified',price))
        with open_readonly(self.path/'main.db',self.db.path) as joined:
            result=ledger_audit(joined,100000)
        self.assertEqual(result['collection_epoch']['epoch_id'],'epoch2')
        self.assertEqual(result['boundary']['start_block'],self.base+20)
        self.assertEqual(result['model_readiness']['current_point_in_time_safe_n'],1)
        self.assertIn('clean_epoch_elapsed_days_below_60',result['model_readiness']['unmet_conditions'])
        self.assertIn('current_epoch_unresolved_proof',result['model_readiness']['unmet_conditions'])
        self.assertEqual(result['model_readiness']['status'],'MODEL_DATA_NOT_MATURE')
        self.assertGreater(epochs.historical_debt(self.fresh)[0]['immutable_versions'],0)

    async def test_current_complete_and_cursor_without_jobs_cannot_seal(self):
        runner=await self.rollover();worker=runner.worker;worker.epoch_discovery_ready=True
        t=target(start=1210);t['launch_block']=self.base+21;t=insert_target(self.fresh,t)
        self.fresh.require_bootstrap(t,'curve',t['launch_block']);self.fresh.complete_bootstrap(1,'curve',self.base+50)
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        worker.subscriptions={(1,'curve'):'ACK'}
        self.assertFalse(epochs.seal_live(worker))

    async def test_registered_active_failure_blocks_until_explicit_quarantine(self):
        self.failure();epochs.schema(self.db.conn)
        with self.db.conn:
            self.db.conn.execute('INSERT INTO flow_collection_epochs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                ('epoch1','retained fixture time',self.base,1000,'a'*40,'fixture',None,'ACTIVE',1,
                 str(self.db.path.resolve()),None,None,'{}'))
        self.assertTrue(switch.blocked(self.db,session_id='different_session'))
        self.quarantine()
        self.assertFalse(switch.blocked(self.db))
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)
        self.assertEqual(self.db.epoch()['ended_at'],1100)

    async def test_imports_have_no_database_network_or_service_side_effects(self):
        import importlib
        with patch('sqlite3.connect',side_effect=AssertionError('import database access')), \
             patch.object(Rpc,'call',side_effect=AssertionError('import RPC')), \
             patch('subprocess.run',side_effect=AssertionError('import service access')):
            for name in ('app.flow_epochs','scripts.flow_epoch_prepare','scripts.flow_epoch_status'):
                importlib.reload(importlib.import_module(name))

    async def test_expired_current_epoch_gap_still_blocks_new_pit(self):
        runner=await self.rollover();worker=runner.worker;worker.epoch_discovery_ready=True
        t=target(start=1210);t['launch_block']=self.base+21;t=insert_target(self.fresh,t)
        self.fake_rpc(self.base+50,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            await CursorBootstrap(runner).run('live_bootstrap:1',[1])
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        worker.subscriptions={(1,'curve'):'ACK'};worker.subscription_ready_at=1300
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(self.fresh.target(1)))
        other=target(launch=2,start=1100);other['token_address']='0x'+'cc'*20
        other=insert_target(self.fresh,other)
        self.fresh.gap(2,1100,1150,'unresolved_expired_epoch_gap',self.base)
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=2")
        self.assertFalse(epochs.seal_live(worker))

    async def test_preboundary_discovery_requires_real_bootstrap_but_never_primary_pit(self):
        from app.flow_data import iso
        runner=await self.rollover();worker=runner.worker;worker.epoch_discovery_ready=True
        with self.main:
            self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',(2,'0x'+'cc'*20,
                self.t['quote_asset_address'],self.t['curve_address'],self.t['creator_address'],iso(1190),self.base+19,0,1))
            self.main.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',(2,0,'random_initial',iso(1190)))
        self.fresh.set_state('quote_decimals:'+self.t['quote_asset_address'],18)
        from unittest.mock import AsyncMock
        worker.header=AsyncMock(return_value=1190)
        with patch('app.flow_worker.time.time',return_value=1210):await worker.discover()
        self.assertEqual(worker.bootstrap_kinds(self.fresh.target(2)),{'curve'})
        self.assertFalse(self.fresh.conn.execute("SELECT 1 FROM flow_gaps WHERE reason='service_started_late'").fetchone())
        self.fake_rpc(self.base+50,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            await CursorBootstrap(runner).run('live_bootstrap:2',[2])
        self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM flow_gaps WHERE resolved=0').fetchone()[0],1)
        self.fresh.rebuild(self.fresh.target(2),1400)
        self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions').fetchone())

    def reopened_worker(self):
        read=FlowDB(self.db.path)
        self.addCleanup(read.close)
        runner,old=make_reconciler(self.config,self.settings,read,self.providers)
        self.addAsyncCleanup(old.close);self.addAsyncCleanup(runner.worker.rpc.close)
        self.addCleanup(runner.worker.main.close)
        runner.worker.rpc.config=replace(runner.worker.rpc.config,rpc_rps=100000)
        return runner.worker

    async def bootstrap_epoch_target(self,runner,head=None):
        worker=runner.worker
        t=target(start=1210);t['launch_block']=self.base+21
        t=insert_target(self.fresh,t);worker.ensure_bootstrap_state(t)
        self.fake_rpc(head or self.base+50,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            await CursorBootstrap(runner).run('live_bootstrap:1',[1])
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        worker.epoch_discovery_ready=True;worker.subscriptions={(1,'curve'):'ACK'}
        worker.subscription_ready_at=1300
        return self.fresh.target(1)

    async def test_creation_and_service_start_crash_keep_same_sealed_epoch(self):
        runner=await self.rollover();boundary=self.fresh.epoch()['boundary_json']
        for checkpoint in ('after_creation','after_service_start_before_bootstrap'):
            with self.subTest(checkpoint=checkpoint):
                worker=self.reopened_worker()
                self.assertEqual(worker.db.epoch()['status'],'ACTIVATING')
                self.assertEqual(worker.db.epoch()['boundary_json'],boundary)
                worker.db.set_state('recovery_state','healthy')
                self.assertEqual(worker.db.state('recovery_state'),'bootstrap_required')
                self.assertFalse(epochs.seal_live(worker))
                with self.assertRaisesRegex(ValueError,'already exists'):
                    await epochs.prepare(self.runner,epoch_id='epoch2',revision='b'*40,reason='retry',predecessor='epoch1')
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)

    async def test_partial_bootstrap_crash_resumes_fixed_head_without_eligibility(self):
        runner=await self.rollover();worker=runner.worker
        t=target(start=1210);t['launch_block']=self.base+21;t=insert_target(self.fresh,t)
        worker.ensure_bootstrap_state(t);self.fake_rpc(self.base+5020,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            result=await CursorBootstrap(runner).run('live_bootstrap:1',[1],max_chunks=1)
        self.assertEqual(result['gate'],'MISSING_CURSOR_BOOTSTRAP_PENDING')
        first=self.fresh.conn.execute('SELECT next_unverified_block FROM flow_shadow_jobs').fetchone()[0]
        self.assertGreater(first,t['launch_block']);self.assertIsNone(self.fresh.state('recovery:1:curve'))
        worker=self.reopened_worker()
        from app.flow_shadow import ShadowReconciler
        old=worker.rpc;resume=ShadowReconciler(worker,reserve=50)
        try:
            with patch('app.flow_epochs.time.time',return_value=1300):
                result=await CursorBootstrap(resume).run('live_bootstrap:1',[1])
        finally:
            await worker.rpc.close();worker.rpc=old
        self.assertEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        self.assertEqual(result['head'],self.base+5020)
        self.assertEqual(worker.db.epoch()['status'],'ACTIVATING')
        worker.db.rebuild(worker.db.target(1),1300)
        self.assertFalse(worker.db.conn.execute('SELECT 1 FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL').fetchone())

    async def test_ack_without_tail_and_pretransition_crash_never_eligible(self):
        runner=await self.rollover();t=await self.bootstrap_epoch_target(runner)
        worker=runner.worker
        self.fresh.rebuild(t,1300)
        self.assertTrue(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions').fetchone())
        self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL').fetchone())
        self.assertFalse(epochs.seal_live(worker))
        worker=self.reopened_worker();worker.epoch_discovery_ready=True
        worker.subscriptions={(1,'curve'):'ACK'};worker.subscription_ready_at=1301
        self.fake_rpc(self.base+500,1305)
        with patch('app.flow_epochs.time.time',return_value=1305):
            while not await worker.activation_tail(worker.db.target(1)):pass
        self.assertEqual(worker.db.epoch()['status'],'ACTIVATING')
        # Crash immediately before transition: prior ACK cannot seal a new connection.
        restarted=self.reopened_worker();restarted.epoch_discovery_ready=True
        restarted.subscriptions={(1,'curve'):'ACK'};restarted.subscription_ready_at=1310
        self.assertFalse(epochs.seal_live(restarted))
        self.fake_rpc(self.base+550,1315)
        with patch('app.flow_epochs.time.time',return_value=1315):
            while not await restarted.activation_tail(restarted.db.target(1)):pass
            self.assertTrue(epochs.seal_live(restarted))
        # Crash immediately after atomic transition: reopening retains ACTIVE + exact seal.
        active=self.reopened_worker();self.assertEqual(active.db.epoch()['status'],'ACTIVE')
        seal=json.loads(active.db.epoch()['boundary_json'])['live_seal']
        self.assertEqual(seal['proved_at'],1315)
        with patch('app.flow_data.time.time',return_value=1320):active.db.rebuild(active.db.target(1),1320)
        rows=list(active.db.conn.execute('SELECT * FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL'))
        self.assertTrue(rows);self.assertTrue(all(r['model_eligible_at']>=1320 for r in rows))
        self.assertEqual([dict(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')],self.before)

    async def test_tail_budget_wait_is_resumable_and_not_normal_over100(self):
        runner=await self.rollover();t=await self.bootstrap_epoch_target(runner);worker=runner.worker
        self.fake_rpc(self.base+5050,1310)
        with patch('app.flow_epochs.time.time',return_value=1310):
            self.assertFalse(await worker.activation_tail(t))
        job=self.fresh.conn.execute("SELECT * FROM flow_shadow_jobs WHERE stage='epoch_tail:1:0'").fetchone()
        next_block=job['next_unverified_block'];self.assertGreater(next_block,job['original_safe_start'])
        # Consume the available shared budget in the fixture only.
        self.db.count('flow_eth_getLogs',350-self.db.used('flow_eth_getLogs',0),now=1310)
        with patch('app.flow_epochs.time.time',return_value=1310):
            self.assertFalse(await worker.activation_tail(t))
        self.assertEqual(self.fresh.conn.execute("SELECT next_unverified_block FROM flow_shadow_jobs WHERE stage='epoch_tail:1:0'").fetchone()[0],next_block)
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVATING');self.assertFalse(switch.blocked(self.fresh))
        self.assertFalse(epochs.seal_live(worker))
        # Next UTC-day budget resumes original fixed stage/head; never resets H_epoch2.
        worker=self.reopened_worker();worker.subscriptions={(1,'curve'):'ACK'};worker.subscription_ready_at=86500
        self.fake_rpc(self.base+5050,86500)
        with patch('app.flow_epochs.time.time',return_value=86500):
            while not await worker.activation_tail(worker.db.target(1)):pass
        self.assertEqual(worker.db.conn.execute("SELECT reconciliation_upper_bound FROM flow_shadow_jobs WHERE stage='epoch_tail:1:0'").fetchone()[0],self.base+5050)
        self.assertEqual(worker.db.epoch()['start_block'],self.base+20)
        with self.assertRaises(Exception):worker.recovery_plan(worker.db.target(1),self.base+6000)
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)

    async def test_failed_discovery_cannot_vacuously_activate_empty_epoch(self):
        runner=await self.rollover();worker=runner.worker
        from unittest.mock import AsyncMock
        from app.flow_budget import BudgetWait
        worker.discover=AsyncMock(side_effect=BudgetWait('daily_getlogs',350,350,86400))
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        with patch('app.flow_worker.time.time',return_value=1300):await worker.reconcile()
        self.assertFalse(worker.epoch_discovery_ready)
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVATING')
        self.assertNotEqual(self.fresh.state('recovery_state'),'healthy')

    async def test_downtime_launch_positions_discovered_and_proved_before_pit(self):
        from app.flow_data import iso
        from unittest.mock import AsyncMock
        runner=await self.rollover();worker=runner.worker
        # Before stop, during stop before boundary, boundary-to-ACK, immediately after ACK.
        positions=[(2,1190,self.base+19),(3,1199,self.base+19),(4,1210,self.base+21),(5,1231,self.base+31)]
        with self.main:
            for launch,at,block in positions:
                self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',(launch,'0x'+format(launch,'040x'),
                    self.t['quote_asset_address'],self.t['curve_address'],self.t['creator_address'],iso(at),block,0,1))
                self.main.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',(launch,0,'random_initial',iso(at)))
        worker.header=AsyncMock(side_effect=lambda block:1190 if block==self.base+19 else 1210 if block==self.base+21 else 1231)
        self.fresh.set_state('quote_decimals:'+self.t['quote_asset_address'],18)
        with patch('app.flow_worker.time.time',return_value=1250):await worker.discover()
        self.assertEqual({r[0] for r in self.fresh.conn.execute('SELECT launch_id FROM flow_tracking_targets')},{2,3,4,5})
        self.assertTrue(all(worker.bootstrap_kinds(self.fresh.target(i))=={'curve'} for i,_,_ in positions))
        self.fake_rpc(self.base+50,1300)
        with patch('app.flow_epochs.time.time',return_value=1300):
            for i,_,_ in positions:await CursorBootstrap(runner).run(f'live_bootstrap:{i}',[i])
        worker.epoch_discovery_ready=True;worker.subscription_ready_at=1300
        worker.subscriptions={(i,'curve'):'ACK' for i,_,_ in positions}
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        with patch('app.flow_epochs.time.time',return_value=1305):
            for i,_,_ in positions:
                self.assertTrue(await worker.activation_tail(self.fresh.target(i)))
            self.assertTrue(epochs.seal_live(worker))
        with patch('app.flow_data.time.time',return_value=1310):
            for i,_,_ in positions:self.fresh.rebuild(self.fresh.target(i),1310)
        eligible={r[0] for r in self.fresh.conn.execute('SELECT DISTINCT launch_id FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL')}
        self.assertEqual(eligible,{4,5})  # Whole-target boundary excludes pre-boundary launches.

    async def test_actual_reconcile_subscribe_bootstrap_tail_seal_without_early_health(self):
        from unittest.mock import AsyncMock
        runner=await self.rollover();worker=runner.worker
        t=target(start=1210);t['launch_block']=self.base+21;insert_target(self.fresh,t)
        worker.discover=AsyncMock();worker.command=AsyncMock(return_value='ACK')
        self.fake_rpc(self.base+50,1300)
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        with patch('app.flow_worker.time.time',return_value=1300):
            await worker.reconcile()
            self.assertEqual(self.fresh.epoch()['status'],'ACTIVATING')
            self.assertEqual(self.fresh.state('recovery_state'),'bootstrap_required')
            self.assertEqual(worker.subscription_ready_at,1300)
            await worker.reconcile()
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVE')
        self.assertEqual(self.fresh.state('recovery_state'),'healthy')
        self.assertEqual(worker.latest_block,self.base+50)
        with patch('app.flow_worker.time.time',return_value=1305):worker.finalize(True)
        self.assertEqual(int(self.fresh.state('last_connected_block')),self.base+50)
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')

    async def test_old_catalog_constraint_rejected_before_quarantine_write(self):
        self.failure()
        # Retain the actual old table/index contract in a fixture database.
        with self.db.conn:
            self.db.conn.execute('''CREATE TABLE flow_collection_epochs(
                epoch_id TEXT PRIMARY KEY,status TEXT CHECK(status IN ('ACTIVE','CLOSED','QUARANTINED')))''')
        with self.assertRaisesRegex(ValueError,'explicit compatible migration'):self.quarantine()
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_collection_epochs').fetchone()[0],0)
        self.assertFalse(self.db.conn.execute("SELECT 1 FROM sqlite_master WHERE name='flow_epoch_global_ids'").fetchone())

    async def test_new_ack_during_activation_requeues_existing_filter_tail(self):
        from unittest.mock import AsyncMock
        runner=await self.rollover();worker=runner.worker
        await self.bootstrap_epoch_target(runner)
        self.fake_rpc(self.base+50,1305)
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(self.fresh.target(1)))
        other=target(launch=2,start=1220);other['launch_block']=self.base+22;other['token_address']='0x'+'cc'*20;insert_target(self.fresh,other)
        worker.discover=AsyncMock();worker.command=AsyncMock(return_value='ACK2')
        with patch('app.flow_worker.time.time',return_value=1310):await worker.reconcile()
        self.assertEqual(worker.subscription_ready_at,1310)
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVATING')
        self.assertIn(1,worker.pending_recovery)
        self.assertFalse(epochs.seal_live(worker))

    async def test_graduation_during_sealed_activation_proves_terminal_curve_and_new_filters(self):
        runner=await self.rollover();worker=runner.worker
        t=await self.bootstrap_epoch_target(runner)
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(t))
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+60;g['block_timestamp']='1970-01-01T00:20:40+00:00'
        with self.fresh.conn:self.fresh.conn.execute('UPDATE flow_tracking_targets SET graduation_json=?,pool_id=? WHERE launch_id=1',(json.dumps(g),g['pool_id']))
        t=self.fresh.target(1);worker.ensure_bootstrap_state(t)
        self.assertEqual(worker.bootstrap_kinds(t),{'curve','v4','hook'})
        self.fake_rpc(self.base+70,1310)
        with patch('app.flow_epochs.time.time',return_value=1310):
            self.assertTrue(await worker.bootstrap_missing(t))
        self.assertEqual(self.fresh.conn.execute("SELECT completed_head FROM flow_bootstrap WHERE kind='curve'").fetchone()[0],self.base+60)
        worker.subscriptions={(1,k):'ACK' for k in ('v4','hook')};worker.subscription_ready_at=1311
        with patch('app.flow_epochs.time.time',return_value=1315):
            for _ in range(5):
                if await worker.activation_tail(self.fresh.target(1)):break
            else:self.fail('Transition tail incomplete')
        self.fresh.set_state('service_status','connected')
        self.assertTrue(epochs.seal_live(worker))
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVE')
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)

    async def test_expired_unproved_activation_tail_still_blocks_seal(self):
        runner=await self.rollover();worker=runner.worker
        await self.bootstrap_epoch_target(runner)
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=1")
        worker.subscriptions={};worker.pending_recovery.clear()
        self.assertFalse(epochs.seal_live(worker))
        self.assertEqual(self.fresh.epoch()['status'],'ACTIVATING')
        self.assertEqual(self.fresh.conn.execute("SELECT resolved FROM flow_gaps WHERE reason='epoch_activation_tail_required'").fetchone()[0],0)
