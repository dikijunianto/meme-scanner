"""Real subscription protocol, persistence, fixed HTTP jobs and resume paths offline."""
import asyncio,json,sqlite3,unittest
from dataclasses import replace
from unittest.mock import patch,AsyncMock
from app import flow_activation as activation,flow_segments
from app.flow_data import FlowDB
from app.flow_worker import FlowWorker
from app.rpc import RpcError
from tests import test_flow_research_segments as segment_fixtures
from tests.test_flow import target,insert_target

class FakeWSS:
    def __init__(self):self.queue=asyncio.Queue();self.reject=False;self.duplicate=False;self.silent=False;self.bad_ack=False
    def __aiter__(self):return self
    async def __aenter__(self):return self
    async def __aexit__(self,*args):return False
    async def __anext__(self):return await self.queue.get()
    async def send(self,raw):
        msg=json.loads(raw)
        if self.silent:return
        response={'jsonrpc':'2.0','id':msg['id']}
        if self.reject:response['error']={'code':-32000,'message':'subscribe rejected'}
        else:response['result']='ACK:'+str(msg['id']) if msg['method']=='eth_subscribe' else hex(4663)
        if self.bad_ack:response['result']=True
        await self.queue.put(json.dumps(response))
        if self.duplicate:await self.queue.put(json.dumps(response))

class ActivationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await segment_fixtures.SegmentTests.asyncSetUp(self)
        self.config=replace(self.config,rpc_rps=100000)
        self.runner.worker.config=self.config
        self.runner.worker.rpc.config=replace(self.runner.worker.rpc.config,rpc_rps=100000)
    asyncTearDown=segment_fixtures.SegmentTests.asyncTearDown
    segment=segment_fixtures.SegmentTests.segment
    failure=segment_fixtures.SegmentTests.failure;quarantine=segment_fixtures.SegmentTests.quarantine;fake_rpc=segment_fixtures.SegmentTests.fake_rpc
    rollover=segment_fixtures.SegmentTests.rollover;bootstrap_epoch_target=segment_fixtures.SegmentTests.bootstrap_epoch_target

    async def existing(self,multiple=False):
        runner=await self.segment();worker=runner.worker
        worker.settings=replace(worker.settings,split_enabled=True,minute_calls=12)
        worker.config=replace(worker.config,rpc_rps=100000);worker.ws_provider='validation';worker.connection_id=123
        t=target(launch=200,start=1410);t['launch_block']=self.base+201
        t=insert_target(self.fresh,t);worker.ensure_bootstrap_state(t)
        targets=[t]
        if multiple:
            t2=target(launch=201,start=1410);t2['launch_block']=self.base+202;t2['token_address']='0x'+'ab'*20
            t2=insert_target(self.fresh,t2);worker.ensure_bootstrap_state(t2);targets.append(t2)
        worker.socket=FakeWSS();reader=asyncio.create_task(worker.read_socket())
        self.protocol_reader=reader
        async def close():reader.cancel();await asyncio.gather(reader,return_exceptions=True)
        self.addAsyncCleanup(close)
        self.fake_rpc(self.base+220,1420)
        self.fresh.set_state('connection_state','connected');self.fresh.set_state('service_status','connected')
        with patch('app.flow_activation.time.time',return_value=1420):activation.session(worker,'c'*40)
        return worker,targets

    async def test_offline_full_activation_real_protocol_to_fresh_pit(self):
        worker,targets=await self.existing()
        with patch('app.flow_data.time.time',return_value=1420):
            await worker.command('eth_chainId',[])
            await worker.subscribe(targets[0])
            worker.discover=AsyncMock();worker.epoch_discovery_ready=True
            await worker.reconcile()
        self.assertTrue(activation.evidence(self.fresh)['complete'])
        self.assertEqual(self.fresh.current_health(),'healthy')
        with patch('app.flow_data.time.time',return_value=1445):self.fresh.rebuild(self.fresh.target(200),1445)
        row=self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=30 AND model_eligible_at IS NOT NULL').fetchone()
        self.assertIsNotNone(row);self.assertGreaterEqual(row['model_eligible_at'],row['materialized_at'])
        self.assertTrue(flow_segments.validate_fresh(self.fresh,1445))
        self.assertEqual(flow_segments.record(self.fresh)['start_at'],1400)
        self.assertEqual(flow_segments.historical_debt(self.fresh)['unresolved'],66)
        self.assertEqual(json.loads(row['proof_json'])['activation_evidence_id'],activation.evidence(self.fresh)['id'])

    async def test_real_worker_run_connects_and_activates_before_new_pit(self):
        worker,targets=await self.existing();self.protocol_reader.cancel()
        await asyncio.gather(self.protocol_reader,return_exceptions=True)
        worker.discover=AsyncMock();now=[1420];ticks=[0];original_sleep=asyncio.sleep
        async def advance(delay):
            if delay==2:
                ticks[0]+=1;now[0]=1445
                if ticks[0]==2:
                    self.assertTrue(activation.evidence(self.fresh)['complete'])
                    self.assertEqual(self.fresh.current_health(),'healthy')
                    self.assertTrue(flow_segments.validate_fresh(self.fresh,1445))
                    raise asyncio.CancelledError()
            await original_sleep(0)
        with patch('app.flow_worker.connect',return_value=worker.socket) as connected,\
             patch('app.flow_shadow.verified_checkout',return_value='c'*40),\
             patch('app.flow_worker.time.time',side_effect=lambda:now[0]),\
             patch('app.flow_worker.asyncio.sleep',side_effect=advance),\
             patch.object(worker,'pressure',return_value=None):
            with self.assertRaises(asyncio.CancelledError):await worker.run()
        connected.assert_called_once()
        self.assertEqual(ticks[0],2)

    async def test_multiple_out_of_order_missing_and_duplicate_ack(self):
        worker,targets=await self.existing(True);worker.socket.duplicate=True
        with patch('app.flow_data.time.time',return_value=1420):
            await worker.subscribe(targets[1])
            self.assertFalse(await activation.drive(worker,targets))
            self.assertNotEqual(self.fresh.current_health(),'healthy')
            await worker.subscribe(targets[0])
            self.assertTrue(await activation.drive(worker,targets,max_chunks=None))
        ready=activation.load(self.fresh,'readiness:'+activation.load(self.fresh,'current_readiness'))
        self.assertEqual(len(ready['ack_ids']),2);self.assertEqual(len(ready['members']),2)

    async def test_remote_rejection_has_no_ack_or_local_write(self):
        worker,targets=await self.existing();worker.socket.reject=True
        with self.assertRaises(RpcError):await worker.subscribe(targets[0])
        self.assertFalse(self.fresh.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'activation_v1:ack:%'").fetchone())
        self.assertFalse(activation.evidence(self.fresh)['complete'])
        worker.socket.reject=False;worker.socket.bad_ack=True
        with self.assertRaises(RpcError):await worker.subscribe(targets[0])

    async def test_local_writer_failure_rolls_back_ack_and_keeps_protocol_distinct(self):
        worker,targets=await self.existing()
        with patch('app.flow_activation.append',side_effect=sqlite3.OperationalError('local writer')):
            with self.assertRaises(sqlite3.OperationalError):await worker.subscribe(targets[0])
        self.assertFalse(self.fresh.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'activation_v1:ack:%'").fetchone())
        self.assertEqual(worker.subscriptions,{})

    async def test_crash_after_ack_and_partial_tail_resume_bound_unchanged(self):
        worker,targets=await self.existing();self.fake_rpc(self.base+4200,1420)
        with patch('app.flow_data.time.time',return_value=1420):
            await worker.subscribe(targets[0])
            self.assertFalse(await activation.drive(worker,targets,max_chunks=1))
        rid=activation.load(self.fresh,'current_readiness');ready=activation.load(self.fresh,'readiness:'+rid)
        db=FlowDB(self.db.path);self.addCleanup(db.close)
        reopened=FlowWorker(self.config,worker.settings,db,self.providers)
        self.addAsyncCleanup(reopened.rpc.close);self.addCleanup(reopened.main.close)
        reopened.activation_session=worker.activation_session;reopened.ws_provider='validation';reopened.connection_id=123
        with patch('app.flow_data.time.time',return_value=1430):
            self.assertTrue(await activation.drive(reopened,[db.target(200)],max_chunks=None))
        self.assertEqual(activation.load(db,'readiness:'+rid),ready)
        self.assertTrue(activation.evidence(db)['complete'])
        self.assertEqual(db.conn.execute('SELECT count(*) FROM flow_shadow_ranges').fetchone()[0],2)
        self.assertEqual(len(activation.load(db,'readiness:'+rid)['ack_ids']),1)
        # Reconnection after process reopen requires a fresh ACK chain, preserving history.
        worker.db=db;worker.subscriptions={};worker.connection_id=124
        with patch('app.flow_data.time.time',return_value=1440):
            activation.session(worker,'c'*40)
            await worker.subscribe(db.target(200))
            self.assertTrue(await activation.drive(worker,[db.target(200)],max_chunks=None))
        self.assertTrue(activation.evidence(db,readiness_id=rid)['complete'])
        self.assertNotEqual(rid,activation.load(db,'current_readiness'))

    async def test_reconnect_old_ack_never_satisfies_current_session(self):
        worker,targets=await self.existing()
        with patch('app.flow_data.time.time',return_value=1420):
            await worker.subscribe(targets[0]);self.assertTrue(await activation.drive(worker,targets,max_chunks=None))
        old=activation.load(self.fresh,'current_readiness')
        with patch('app.flow_data.time.time',return_value=1440):activation.session(worker,'c'*40)
        worker.subscriptions={};worker.connection_id=124
        self.assertFalse(activation.evidence(self.fresh)['complete'])
        self.assertTrue(activation.evidence(self.fresh,readiness_id=old)['complete'])
        with patch('app.flow_data.time.time',return_value=1440):
            self.assertFalse(await activation.drive(worker,targets))
            await worker.subscribe(targets[0]);self.assertTrue(await activation.drive(worker,targets,max_chunks=None))
        self.assertNotEqual(old,activation.load(self.fresh,'current_readiness'))
        self.assertEqual(self.fresh.conn.execute("SELECT count(*) FROM flow_state WHERE key LIKE 'activation_v1:ack:%'").fetchone()[0],2)

    async def test_race_ack_and_proof_and_completion_survive_reopen(self):
        worker,targets=await self.existing()
        self.assertFalse(await activation.drive(worker,targets))
        with patch('app.flow_data.time.time',return_value=1420):
            await worker.subscribe(targets[0])
            self.assertFalse(activation.evidence(self.fresh)['complete'])
            self.assertTrue(await activation.drive(worker,targets,max_chunks=None))
        rid=activation.load(self.fresh,'current_readiness')
        db=FlowDB(self.db.path);self.addCleanup(db.close)
        self.assertTrue(activation.evidence(db)['complete'])
        ready=activation.load(db,'readiness:'+rid);header_key=ready['stage']+':header'
        raw=db.conn.execute('SELECT value FROM flow_shadow_meta WHERE key=?',(header_key,)).fetchone()[0]
        altered=json.loads(raw);altered['hash']='0x'+'99'*32
        with db.conn:db.conn.execute('UPDATE flow_shadow_meta SET value=? WHERE key=?',(json.dumps(altered),header_key))
        self.assertFalse(activation.evidence(db)['complete'])
        with db.conn:db.conn.execute('UPDATE flow_shadow_meta SET value=? WHERE key=?',(raw,header_key))
        # Missing committed ACK cannot be substituted by an already completed HTTP job.
        aid=activation.load(db,'readiness:'+rid)['ack_ids'][0]
        with db.conn:db.conn.execute('DELETE FROM flow_state WHERE key=?',(activation.PREFIX+'ack:'+aid,))
        self.assertFalse(activation.evidence(db)['complete'])

    async def test_old_incident_window_never_promoted_and_unbounded_blocks_activation(self):
        worker,targets=await self.existing()
        with patch('app.flow_data.time.time',return_value=1445):self.fresh.rebuild(targets[0],1445)
        old=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')]
        with patch('app.flow_data.time.time',return_value=1450):
            await worker.subscribe(targets[0]);self.assertTrue(await activation.drive(worker,targets,max_chunks=None))
            self.fresh.rebuild(self.fresh.target(200),1450)
        self.assertTrue(all(r['model_eligible_at'] is None for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')))
        self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions ORDER BY launch_id,window_seconds,version_number')][:len(old)],old)
        self.fresh.set_state('unbounded_current_gap:injected','{}')
        self.assertNotEqual(self.fresh.current_health(),'healthy')
        self.assertEqual(activation.storage_preflight(self.fresh)['schema_migration'],'NO_PRODUCTION_SCHEMA_MIGRATION_REQUIRED')

    async def test_timeout_and_malformed_local_state_remain_distinct(self):
        worker,targets=await self.existing();worker.socket.silent=True
        original=asyncio.wait_for
        async def fast(awaitable,timeout):return await original(awaitable,.01)
        with patch('app.flow_worker.asyncio.wait_for',side_effect=fast):
            with self.assertRaises(asyncio.TimeoutError):await worker.subscribe(targets[0])
        self.assertFalse(activation.evidence(self.fresh)['complete'])
        self.fresh.set_state(activation.PREFIX+'current_readiness','malformed JSON')
        with self.assertRaises(ValueError):activation.evidence(self.fresh)

    async def test_graduated_three_filter_ack_set_and_bounded_reconnect(self):
        from tests.test_flow import fixture
        worker,targets=await self.existing();g=fixture('v4_buy')['launch'];g['block_number']=self.base+210
        with self.fresh.conn:self.fresh.conn.execute('UPDATE flow_tracking_targets SET graduation_json=?,pool_id=? WHERE launch_id=200',
            (json.dumps(g),g['pool_id']))
        target=self.fresh.target(200);worker.ensure_bootstrap_state(target)
        with patch('app.flow_data.time.time',return_value=1420):
            await worker.subscribe(target);self.assertTrue(await activation.drive(worker,[target],max_chunks=None))
        ready=activation.load(self.fresh,'readiness:'+activation.load(self.fresh,'current_readiness'))
        self.assertEqual({m['kind'] for m in ready['members']},{'curve','v4','hook'})
        worker.uncertainty(target,1420,1421,'ws_gap',self.base+201)
        worker.subscriptions={};worker.connection_id=124;self.fake_rpc(self.base+225,1440)
        with patch('app.flow_data.time.time',return_value=1440):
            activation.session(worker,'c'*40);await worker.subscribe(target)
            self.assertTrue(await activation.drive(worker,[target],max_chunks=None))
        self.assertEqual(self.fresh.current_health(),'healthy')
        self.assertFalse(self.fresh.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'unbounded_current_gap:%'").fetchone())

    async def test_budget_pause_pins_head_across_day_and_completion_before_active(self):
        from app.flow_budget import BudgetWait
        worker,targets=await self.existing()
        with patch('app.flow_data.time.time',return_value=1420):await worker.subscribe(targets[0])
        from app.flow_shadow import ShadowRpc
        original=ShadowRpc.call
        async def pause(rpc,method,params):
            if method=='eth_getBlockByNumber':raise BudgetWait('daily_rpc',1000,1000,86400)
            return await original(rpc,method,params)
        with patch.object(ShadowRpc,'call',pause),patch('app.flow_data.time.time',return_value=1420):
            with self.assertRaises(BudgetWait):await activation.drive(worker,targets)
        pins=[r[0] for r in self.fresh.conn.execute("SELECT value FROM flow_state WHERE key LIKE 'activation_v1:head:%'")]
        self.fake_rpc(self.base+220,1420)
        with patch('app.flow_data.time.time',return_value=86410),patch('app.flow_segments.record',wraps=flow_segments.record):
            self.assertTrue(await activation.drive(worker,targets,max_chunks=None))
        self.assertEqual([r[0] for r in self.fresh.conn.execute("SELECT value FROM flow_state WHERE key LIKE 'activation_v1:head:%'")],pins)
        with self.fresh.catalog_conn:self.fresh.catalog_conn.execute("UPDATE flow_research_segments SET status='SEALED' WHERE segment_id='clean1'")
        reopened=FlowDB(self.db.path);self.addCleanup(reopened.close)
        self.assertTrue(activation.evidence(reopened)['complete'])
        self.assertTrue(reopened.collection_context()['pit_eligible'])
