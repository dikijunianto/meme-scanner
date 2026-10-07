"""Same-epoch segmentation preserves failed history and fixes future obligations."""
import json
import unittest
from dataclasses import replace
from unittest.mock import patch,AsyncMock

from tests import test_flow_epochs as fixture
from tests.test_flow import target,insert_target,fixture as log_fixture
from app.flow_data import FlowDB
from app.flow_bootstrap import CursorBootstrap
from app import flow_epochs,flow_segments
from app.flow_shadow import make_reconciler


class SegmentTests(unittest.IsolatedAsyncioTestCase):
    async def segment(self):
        runner=await self.rollover();worker=runner.worker
        t=await self.bootstrap_epoch_target(runner)
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(t))
            self.assertTrue(flow_epochs.seal_live(worker))
        self.parent=self.fresh
        self.presegment_worker=worker
        self.parent_epoch=self.parent.epoch()
        for i in range(14):
            t=target(launch=100+i,start=1210);t['launch_block']=self.base+21+i
            t['token_address']='0x'+format(100+i,'040x')
            insert_target(self.parent,t)
            with self.parent.conn:self.parent.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=?",(100+i,))
        for i in range(66):self.parent.gap(100+i%14,1220,1280,'ws_gap',self.base+21+i%14)
        self.debt=flow_segments.debt_snapshot(self.parent)
        self.fake_rpc(self.base+200,1400)
        runner.worker.rpc.settings=replace(runner.worker.rpc.settings,minute_calls=12)
        with patch('app.flow_segments.time.time',return_value=1400):
            await flow_segments.prepare(runner,segment_id='clean1',revision='c'*40,reason='preclean incident')
        self.fresh=FlowDB(self.db.path);self.addCleanup(self.fresh.close)
        new,old=make_reconciler(self.config,self.settings,self.fresh,self.providers)
        await old.close();self.addAsyncCleanup(new.worker.rpc.close);self.addCleanup(new.worker.main.close)
        return new

    async def active_segment(self):
        runner=await self.segment();worker=runner.worker
        t=target(launch=200,start=1410);t['launch_block']=self.base+201
        t=insert_target(self.fresh,t);worker.ensure_bootstrap_state(t)
        self.fake_rpc(self.base+220,1420)
        with patch('app.flow_data.time.time',return_value=1420):
            await CursorBootstrap(runner).run('live_bootstrap:200',[200])
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        worker.epoch_discovery_ready=True;worker.subscriptions={(200,'curve'):'ACK'};worker.subscription_ready_at=1420
        with patch('app.flow_data.time.time',return_value=1425):
            self.assertTrue(await worker.activation_tail(self.fresh.target(200)))
            self.assertTrue(flow_epochs.seal_live(worker))
        return worker,self.fresh.target(200)

    def captured_gap(self,t,reason='ws_gap',last=None):
        first=t['launch_block'];last=last or first+30
        key='capture:'+reason
        self.fresh.set_state(key,json.dumps({'provider':'validation','launch_id':t['launch_id'],
            'segment_id':'clean1','lower_anchors':[first],'required_head':last,
            'header':{'number':hex(last),'timestamp':hex(1440),'hash':'0x'+'33'*32}}))
        return self.fresh.gap(t['launch_id'],1420,1440,reason,first,through_block=last,
                             provenance={'source':'recovery_head','identity':key})

    async def test_66_gap_14_target_fixture_same_epoch_preserved(self):
        await self.segment()
        debt=flow_segments.historical_debt(self.fresh)
        self.assertEqual((debt['gap_count'],debt['unresolved']),(66,66))
        self.assertTrue(debt['original_rows_unchanged']);self.assertIsNone(debt['upper_bound'])
        self.assertEqual(len({r['launch_id'] for r in self.debt['rows']}),14)
        self.assertEqual(self.parent.epoch(),self.parent_epoch)
        self.assertEqual(self.fresh.epoch()['epoch_id'],'epoch2')
        self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM flow_gaps').fetchone()[0],0)
        self.assertEqual(flow_segments.record(self.fresh)['status'],'SEALED')
        self.assertIsNone(flow_segments.record(self.fresh)['validated_at'])
        self.assertEqual(self.db.conn.execute("SELECT status FROM flow_collection_epochs WHERE epoch_id='epoch1'").fetchone()[0],'QUARANTINED')

    async def test_writer_adopts_same_epoch_and_retires_old_notifications(self):
        await self.segment();worker=self.presegment_worker
        worker.routes={'oldACK':(1,'curve')};worker.subscriptions={(1,'curve'):'oldACK'}
        worker.queue.put_nowait({'subscription':'oldACK'})
        worker.command=AsyncMock(return_value=True);worker.discover=AsyncMock()
        worker.connection_started_at=1300
        with patch('app.flow_data.time.time',return_value=1405):await worker.reconcile()
        worker.drain()
        self.assertEqual(worker.db.path,self.fresh.path)
        self.assertEqual(worker.db.epoch()['epoch_id'],'epoch2')
        self.assertEqual(flow_segments.historical_debt(worker.db)['unresolved'],66)
        worker.command.assert_awaited_once_with('eth_unsubscribe',['oldACK'])
        worker.db.close()

    async def test_health_matrix_and_historical_visibility(self):
        worker,t=await self.active_segment()
        self.assertEqual(self.fresh.current_health(),'healthy')
        gid=self.captured_gap(t)
        self.assertIsNotNone(gid);self.assertNotEqual(self.fresh.current_health(),'healthy')
        from app.flow_expired_recovery import recover
        gap=dict(self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gid,)).fetchone())
        self.fake_rpc(self.base+999,1440)
        with patch('app.flow_data.time.time',return_value=1440):self.assertTrue(await recover(worker,gap))
        self.assertEqual(self.fresh.current_health(),'healthy')
        self.fresh.conn.execute("UPDATE flow_bootstrap SET status='required'");self.fresh.conn.commit()
        self.assertNotEqual(self.fresh.current_health(),'healthy')
        self.fresh.conn.execute("UPDATE flow_bootstrap SET status='complete'");self.fresh.conn.commit()
        seal=flow_segments.record(self.fresh)['boundary_json']
        with self.fresh.catalog_conn:self.fresh.catalog_conn.execute("UPDATE flow_research_segments SET boundary_json='{}'")
        self.assertNotEqual(self.fresh.current_health(),'healthy')
        with self.fresh.catalog_conn:self.fresh.catalog_conn.execute('UPDATE flow_research_segments SET boundary_json=?',(seal,))
        self.assertIsNone(self.fresh.gap(200,1440,1441,'ws_gap',t['launch_block']))
        self.assertEqual(self.fresh.current_health(),'UNBOUNDED_CURRENT_GAP')
        self.assertEqual(flow_segments.historical_debt(self.fresh)['unresolved'],66)

    async def test_header_budget_pause_keeps_reconnect_generation(self):
        worker,t=await self.active_segment()
        from app.flow_gap_contracts import defer,pin_recovery,capture_recovery,read
        from app.flow_budget import BudgetWait
        defer(self.fresh,t,1430,1440,'ws_gap',t['launch_block'])
        key=pin_recovery(self.fresh,t,t['launch_block']+40)
        with patch.object(worker.rpc,'call',side_effect=BudgetWait('minute_rpc',12,12,1445)):
            with self.assertRaises(BudgetWait):await capture_recovery(self.fresh,worker.rpc,t)
        self.assertEqual(json.loads(self.fresh.state(key))['required_head'],t['launch_block']+40)
        reopened=self.reopened_worker();self.fake_rpc(t['launch_block']+40,1445)
        with patch('app.flow_data.time.time',return_value=1445):
            self.assertEqual(await capture_recovery(reopened.db,reopened.rpc,t),t['launch_block']+40)
        self.assertEqual(self.calls,['eth_getBlockByNumber'])
        gap=self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE resolved=0').fetchone()
        self.assertEqual(read(self.fresh,gap)['required_through_block'],t['launch_block']+40)

    async def test_all_recovery_classes_pause_restart_then_expire(self):
        worker,t=await self.active_segment()
        from app.flow_gap_recovery import obligations,recover
        from app.flow_gap_contracts import read
        from app.flow_budget import BudgetWait
        for reason in ('ws_gap','reconnect_recovery_incomplete','normal_recovery','provider_budget'):
            self.assertIsNotNone(self.captured_gap(t,reason))
        before=[read(self.fresh,r) for r in self.fresh.conn.execute('SELECT * FROM flow_gaps')]
        with patch('app.flow_data.time.time',return_value=1440),patch('app.flow_shadow.ShadowRpc.call',side_effect=BudgetWait('minute_rpc',12,12,1445)):
            for gap in obligations(self.fresh):self.assertFalse(await recover(worker,t,gap))
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial',completed_at=? WHERE launch_id=200",(t['tracking_end_at']+11,))
        reopened=self.reopened_worker();self.fake_rpc(self.base+99999,3000)
        with patch('app.flow_data.time.time',return_value=3000):
            for gap in obligations(reopened.db):self.assertTrue(await recover(reopened,reopened.db.target(200),gap))
        self.assertEqual(self.fresh.target(200)['status'],'partial')
        self.assertEqual([read(self.fresh,r) for r in self.fresh.conn.execute('SELECT * FROM flow_gaps')],before)
        self.assertEqual(set(self.calls),{'eth_getLogs'})

    async def test_bounds_restart_budget_resume_expiry_and_immutable_pit(self):
        worker,t=await self.active_segment()
        from app.flow_gap_contracts import read
        from app.flow_gap_recovery import obligations,recover
        from app.flow_budget import BudgetWait
        with self.fresh.conn:self.fresh.conn.execute('UPDATE flow_tracking_targets SET coverage_end_at=1425 WHERE launch_id=200')
        with patch('app.flow_data.time.time',return_value=1445):self.fresh.rebuild(self.fresh.target(200),1445)
        gid=self.captured_gap(t,'reconnect_recovery_incomplete')
        before=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')]
        self.assertTrue(before);self.assertTrue(all(r['model_eligible_at'] is None for r in before))
        bound=read(self.fresh,self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gid,)).fetchone())
        with patch('app.flow_shadow.ShadowRpc.call',side_effect=BudgetWait('minute_rpc',12,12,1441)),patch('app.flow_data.time.time',return_value=1440):
            self.assertFalse(await recover(worker,t,obligations(self.fresh)[0]))
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial',completed_at=? WHERE launch_id=200",(t['tracking_end_at']+11,))
        reopened=self.reopened_worker();self.fake_rpc(self.base+99999,3000)
        with patch('app.flow_data.time.time',return_value=3000):
            self.assertTrue(await recover(reopened,self.fresh.target(200),obligations(reopened.db)[0]))
        self.assertEqual(self.calls,['eth_getLogs'])
        self.assertEqual(self.fresh.target(200)['status'],'partial')
        self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')],before)
        self.assertEqual(read(self.fresh,self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gid,)).fetchone()),bound)
        self.assertEqual(flow_segments.historical_debt(self.fresh)['unresolved'],66)

    async def test_supported_and_unsupported_class_matrix(self):
        worker,t=await self.active_segment()
        from app.flow_gap_contracts import read
        for reason in ('ws_gap','reconnect_recovery_incomplete','normal_recovery','provider_budget'):
            with self.subTest(reason=reason):
                gid=self.captured_gap(t,reason)
                gap=self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gid,)).fetchone()
                contract=read(self.fresh,gap)
                self.assertEqual(contract['required_from_block'],t['launch_block'])
                self.assertEqual(contract['required_through_block'],t['launch_block']+30)
                saved=json.loads(self.fresh.state('capture:'+reason));saved['header']['number']=hex(t['launch_block']+31)
                self.fresh.set_state('capture:'+reason,json.dumps(saved))
                with self.assertRaises(ValueError):read(self.fresh,gap)
        for reason in ('reorg_unresolved','unknown_timestamp','unsupported_semantics','service_started_late','target_expiry'):
            with self.subTest(reason=reason):self.assertIsNone(self.captured_gap(t,reason))
        self.assertEqual(self.fresh.current_health(),'UNBOUNDED_CURRENT_GAP')

    async def test_curve_v4_hook_and_startup_tail_fixed_contracts(self):
        runner=await self.segment();worker=runner.worker
        t=target(launch=200,start=1410);t['launch_block']=self.base+201
        g=log_fixture('v4_buy')['launch'];g['block_number']=self.base+210
        t['graduation_json']=json.dumps(g);t=insert_target(self.fresh,t)
        worker.ensure_bootstrap_state(t);self.fake_rpc(self.base+250,1450)
        from app.flow_budget import BudgetWait
        original=runner.worker.rpc.call
        async def pause(method,params):
            if method=='eth_getLogs':raise BudgetWait('minute_rpc',12,12,1460)
            return await original(method,params)
        with patch.object(runner.worker.rpc,'call',side_effect=pause),patch('app.flow_data.time.time',return_value=1450):
            result=await CursorBootstrap(runner).run('live_graduation:200',[200])
            self.assertNotEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        from app.flow_gap_contracts import read
        frozen=[read(self.fresh,r) for r in self.fresh.conn.execute('SELECT * FROM flow_gaps')]
        self.assertEqual({r['filters'][0]['kind'] for r in frozen},{'curve','v4','hook'})
        self.assertEqual({r['required_through_block'] for r in frozen},{self.base+210,self.base+250})
        reopened=self.reopened_worker();self.fake_rpc(self.base+99999,1460)
        with patch('app.flow_data.time.time',return_value=1460):
            self.assertTrue(await reopened.bootstrap_missing(self.fresh.target(200)))
        self.assertNotIn('eth_blockNumber',self.calls)
        self.assertEqual([read(self.fresh,r) for r in self.fresh.conn.execute('SELECT * FROM flow_gaps')],frozen)
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        reopened.subscriptions={(200,k):'ACK'+k for k in ('v4','hook')};reopened.subscription_ready_at=1460
        self.fake_rpc(self.base+260,1465)
        with patch('app.flow_data.time.time',return_value=1465):
            complete=await reopened.activation_tail(self.fresh.target(200))
            if not complete:complete=await reopened.activation_tail(self.fresh.target(200))
            self.assertTrue(complete)
        tails=[read(self.fresh,r) for r in self.fresh.conn.execute("SELECT * FROM flow_gaps WHERE reason='epoch_activation_tail_required'")]
        self.assertTrue(tails);self.assertTrue(all(r['required_through_block']==self.base+260 for r in tails))
        from app.flow_gap_contracts import pin_recovery
        for kind in ('v4','hook'):self.fresh.set_state('recovery:200:'+kind,g['block_number'])
        key=pin_recovery(self.fresh,self.fresh.target(200),self.base+280)
        self.assertIn(g['block_number'],json.loads(self.fresh.state(key))['lower_anchors'])

    async def test_disconnect_pending_bound_and_provider_switch_head(self):
        worker,t=await self.active_segment()
        from app.flow_gap_contracts import defer,bind_pending,read
        from app import flow_provider_switch as switch
        defer(self.fresh,t,1430,1440,'ws_gap',t['launch_block'])
        self.assertEqual(self.fresh.current_health(),'recovering')
        gid=self.captured_gap(t);bind_pending(self.fresh,t,'capture:ws_gap')
        gaps=self.fresh.conn.execute("SELECT * FROM flow_gaps WHERE resolved=0").fetchall()
        self.assertEqual(len(gaps),2)
        self.assertTrue(all(read(self.fresh,r)['required_through_block']==t['launch_block']+30 for r in gaps))
        sid,item=switch.start(self.fresh,'publicnode',[{'launch_id':200,'kind':'curve','base':t['launch_block'],'cursor':str(t['launch_block']+10)}],[])
        item.update(new_connected_at=1440,new_provider='validation',subscriptions_ready_at=1440)
        switch.save(self.fresh,sid,'PRIMARY_SUBSCRIPTIONS_READY',item)
        header={'number':hex(t['launch_block']+40),'timestamp':hex(1445),'hash':'0x'+'44'*32}
        switch.frozen(self.fresh,sid,t['launch_block']+40,t['launch_block'],header=header)
        gid=self.fresh.gap(200,1430,1445,'ws_gap',t['launch_block'],through_block=t['launch_block']+40,
            provenance={'source':'provider_switch','identity':str(sid)})
        self.assertIsNotNone(gid)
        with self.assertRaises(ValueError):switch.frozen(self.fresh,sid,t['launch_block']+41,t['launch_block'],header=header)

    async def test_actual_fresh_pit_sets_clock_and_maturity_segment_scope(self):
        worker,t=await self.active_segment()
        self.assertFalse(flow_segments.validate_fresh(self.fresh,1445))
        with patch('app.flow_data.time.time',return_value=1445):self.fresh.rebuild(t,1445)
        self.assertTrue(flow_segments.validate_fresh(self.fresh,1445))
        segment=flow_segments.record(self.fresh)
        self.assertEqual(segment['status'],'VALIDATED');self.assertEqual(segment['start_at'],1400)
        from scripts.phase2c_dataset_audit import open_readonly
        from scripts.phase2c_point_in_time_audit import ledger_audit
        with open_readonly(self.config.database,self.settings.database) as joined:
            audit=ledger_audit(joined,1445)
        self.assertEqual(audit['research_clean_start'],1400)
        self.assertEqual(audit['research_segment']['segment_id'],'clean1')
        self.assertEqual(audit['model_readiness']['status'],'MODEL_DATA_NOT_MATURE')
        self.assertEqual(flow_segments.historical_debt(self.fresh)['unresolved'],66)

    async def test_expired_bootstrap_curve_v4_hook_finishes_original_jobs_without_cursors(self):
        runner=await self.segment()
        t=target(launch=200,start=1410,long=False);t['launch_block']=self.base+201
        g=log_fixture('v4_buy')['launch'];g['block_number']=self.base+210;t['graduation_json']=json.dumps(g)
        t=insert_target(self.fresh,t);runner.worker.ensure_bootstrap_state(t)
        self.fake_rpc(self.base+250,1450)
        from app.flow_budget import BudgetWait
        original=runner.worker.rpc.call
        async def pause(method,params):
            if method=='eth_getLogs':raise BudgetWait('minute_rpc',12,12,1460)
            return await original(method,params)
        with patch.object(runner.worker.rpc,'call',side_effect=pause),patch('app.flow_data.time.time',return_value=1450):
            await CursorBootstrap(runner).run('live_graduation:200',[200])
        with patch('app.flow_data.time.time',return_value=1460):self.fresh.rebuild(self.fresh.target(200),1460)
        before=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')]
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial',completed_at=2321 WHERE launch_id=200")
        reopened=self.reopened_worker();self.fake_rpc(self.base+9999,3000)
        from app.flow_gap_recovery import obligations,recover
        with patch('app.flow_data.time.time',return_value=3000):
            for gap in obligations(reopened.db):self.assertTrue(await recover(reopened,reopened.db.target(200),gap))
        self.assertEqual(self.fresh.target(200)['status'],'partial')
        self.assertTrue(all(self.fresh.state('recovery:200:'+k) is None for k in ('curve','v4','hook')))
        self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')],before)
        self.assertFalse(self.fresh.conn.execute("SELECT 1 FROM flow_shadow_jobs WHERE completion_status!='complete'").fetchone())


for _name in ('asyncSetUp','asyncTearDown','failure','quarantine','fake_rpc','rollover','bootstrap_epoch_target','reopened_worker'):
    setattr(SegmentTests,_name,getattr(fixture.EpochTests,_name))
