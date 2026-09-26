import asyncio
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from app.config import Config
from app.flow_data import FlowDB, decode_event
from app.flow_providers import FlowProviders, provider
from app.flow_shadow import CUTOVER_RESERVE, SHADOW_SPAN, make_reconciler, verified_checkout
from app.flow_worker import FlowSettings
from app.rpc import Rpc, RpcError, RetryableRpcError
from tests.test_flow import event, insert_target, main_schema, target


class ShadowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)
        self.main=main_schema(self.path/'main.db')
        self.db=FlowDB(self.path/'flow.db');self.db.migrate()
        self.t=insert_target(self.db,target())
        self.config=Config('https://alchemy.invalid',4663,(),'',self.path/'main.db',self.path/'log')
        self.settings=FlowSettings(database=self.path/'flow.db',minute_calls=100)
        self.providers=FlowProviders('https://mainnet.robinhood.validationcloud.io/v1/test',
                                     'wss://mainnet.robinhood.validationcloud.io/v1/test')
        self.runner,self.old_rpc=make_reconciler(self.config,self.settings,self.db,self.providers)
        self.runner.worker.rpc.config=replace(self.runner.worker.rpc.config,rpc_rps=1000)
        self.base=self.t['launch_block']

    async def asyncTearDown(self):
        await self.old_rpc.close();await self.runner.worker.rpc.close()
        self.runner.worker.main.close();self.main.close();self.db.conn.close();self.tmp.cleanup()

    def mock_rpc(self,head,logs=(),reject_above=None,fail_at=None):
        calls=[]
        async def answer(rpc,payload,method):
            self.assertEqual(provider(rpc.config.rpc_http),'validation')
            self.assertEqual(rpc.config.fallback_http,'')
            if method=='eth_blockNumber':return {'jsonrpc':'2.0','id':payload['id'],'result':hex(head)}
            self.assertEqual(method,'eth_getLogs')
            q=payload['params'][0]
            first,last=int(q['fromBlock'],16),int(q['toBlock'],16)
            calls.append((first,last))
            if fail_at is not None and first>=fail_at:raise RpcError('provider failure')
            if reject_above and last-first+1>reject_above:
                return {'jsonrpc':'2.0','id':payload['id'],'error':{'code':-32005,'message':'block range'}}
            return {'jsonrpc':'2.0','id':payload['id'],'result':[
                copy.deepcopy(x) for x in logs if first<=int(x['blockNumber'],16)<=last]}
        self.patch=patch.object(Rpc,'_send',answer)
        self.patch.start();self.addCleanup(self.patch.stop)
        return calls

    async def test_354_block_shadow_replay_deduplicates_and_keeps_runtime_limit(self):
        head=self.base+354
        rows=[]
        for i,block in enumerate((self.base,self.base+99,self.base+100,head),1):
            row=event(self.t,index=i);row['blockNumber']=hex(block);rows.extend((row,copy.deepcopy(row)))
        calls=self.mock_rpc(head,rows)
        self.assertEqual(await self.runner.start_historical(),head)
        self.assertTrue(await self.runner.run_stage('historical'))
        report=self.runner.summary('historical')
        self.assertEqual(report['blocks_verified'],355)
        self.assertEqual(report['successful_getlogs_calls'],1)
        self.assertEqual(report['recovered_events'],4)
        self.assertEqual(report['recovered_events_by_type']['curve'],4)
        self.assertEqual(report['duplicates'],4)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],4)
        self.assertEqual(report['unresolved_ranges'],[])
        self.assertEqual(self.db.state('recovery:1:curve'),None)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(len(calls),1)
        self.assertLessEqual(max(last-first+1 for first,last in calls),SHADOW_SPAN)
        self.assertEqual(self.db.used('flow_shadow_eth_getLogs_validation',0),1)
        self.assertEqual(self.db.used('flow_eth_getLogs',0),1)

    async def test_adaptive_reduction_and_successful_empty_range(self):
        calls=self.mock_rpc(self.base+354,reject_above=100)
        await self.runner.start_historical()
        self.assertTrue(await self.runner.run_stage('historical'))
        report=self.runner.summary('historical')
        self.assertGreater(report['reductions'],0)
        self.assertGreater(report['successful_getlogs_calls'],1)
        self.assertEqual(report['recovered_events'],0)
        self.assertEqual(report['blocks_verified'],355)
        self.assertEqual(report['unresolved_ranges'],[])
        self.assertLessEqual(report['max_successful_chunk'],100)
        self.assertEqual(report['actual_getlogs_calls'],len(calls))

    async def test_nine_targets_keep_independent_safe_starts(self):
        head=self.base+354
        for i,gap in enumerate((0,1,9,10,50,99,100,354),2):
            t=target(launch=i);t['token_address']='0x'+format(i,'040x')
            t['curve_address']='0x'+format(i+100,'040x')
            insert_target(self.db,t)
            self.db.set_state(f'recovery:{i}:curve',head-gap)
        self.mock_rpc(head)
        await self.runner.start_historical()
        jobs=self.runner.summary('historical')['jobs']
        self.assertEqual(len(jobs),9)
        self.assertEqual(jobs[0]['original_safe_start'],self.base)
        self.assertEqual([j['original_safe_start'] for j in jobs[1:]],
                         [max(self.base,head-gap-2) for gap in (0,1,9,10,50,99,100,354)])

    async def test_unresolved_gap_widens_shadow_start_before_runtime_cursor(self):
        self.db.set_state('recovery:1:curve',self.base+100)
        self.db.gap(1,1000,1010,'ws_gap',self.base+5)
        self.mock_rpc(self.base+110)
        await self.runner.start_historical()
        self.assertEqual(self.runner.summary('historical')['jobs'][0]['original_safe_start'],self.base+5)

    async def test_rate_limit_retry_is_counted_per_actual_send(self):
        attempts=0
        async def answer(rpc,payload,method):
            nonlocal attempts
            if method=='eth_blockNumber':return {'jsonrpc':'2.0','id':payload['id'],'result':hex(self.base)}
            attempts+=1
            if attempts==1:raise RetryableRpcError('HTTP 429')
            return {'jsonrpc':'2.0','id':payload['id'],'result':[]}
        with patch.object(Rpc,'_send',answer),patch('app.rpc.asyncio.sleep',AsyncMock()):
            await self.runner.start_historical()
            self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(attempts,2)
        job=self.runner.summary('historical')['jobs'][0]
        self.assertEqual(job['actual_getlogs_calls'],2)
        self.assertEqual(job['retries'],1)
        self.assertEqual(self.db.used('flow_shadow_eth_getLogs_validation',0),2)

    async def test_invalid_provider_log_records_failed_range_without_cursor(self):
        bad=event(self.t);bad['address']='0x'+'99'*20
        self.mock_rpc(self.base,[bad])
        await self.runner.start_historical()
        self.assertFalse(await self.runner.run_stage('historical'))
        job=self.runner.summary('historical')['jobs'][0]
        self.assertEqual(job['failed_from'],self.base)
        self.assertEqual(job['next_unverified_block'],self.base)
        self.assertEqual(job['highest_contiguous_verified_block'],self.base-1)

    async def test_failed_range_preserves_cursor_and_next_run_resumes(self):
        head=self.base+SHADOW_SPAN+2
        calls=self.mock_rpc(head,fail_at=self.base+SHADOW_SPAN)
        await self.runner.start_historical()
        self.assertFalse(await self.runner.run_stage('historical'))
        report=self.runner.summary('historical')
        job=report['jobs'][0]
        self.assertEqual(job['highest_contiguous_verified_block'],self.base+SHADOW_SPAN-1)
        self.assertEqual(job['failed_from'],self.base+SHADOW_SPAN)
        self.assertEqual(report['unresolved_ranges'],[(1,'curve',self.base+SHADOW_SPAN,head)])
        self.assertEqual(self.db.state('recovery:1:curve'),None)
        self.patch.stop();calls2=self.mock_rpc(head)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(calls2[0][0],self.base+SHADOW_SPAN)

    async def test_budget_reserve_pause_and_next_day_resume(self):
        head=self.base+SHADOW_SPAN+2
        calls=self.mock_rpc(head)
        self.db.count('flow_eth_getLogs',self.settings.daily_getlogs-CUTOVER_RESERVE-1)
        await self.runner.start_historical()
        self.assertFalse(await self.runner.run_stage('historical'))
        self.assertEqual(len(calls),1)
        self.assertEqual(self.runner.summary('historical')['unresolved_ranges'][0][2],self.base+SHADOW_SPAN)
        self.assertEqual(self.db.used('flow_eth_getLogs',int(__import__('time').time())//86400*86400),350)
        with self.db.conn:self.db.conn.execute('UPDATE flow_usage SET minute=minute-86400')
        self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(calls[-1][0],self.base+SHADOW_SPAN)
        self.assertEqual(self.runner.summary('historical')['unresolved_ranges'],[])

    async def test_preexisting_live_event_is_ignored_and_main_unchanged(self):
        row=event(self.t)
        self.runner.worker.ingest(self.t,copy.deepcopy(row))
        main_before=self.main.execute('SELECT count(*) FROM launches').fetchone()[0]
        self.mock_rpc(self.base,[row])
        await self.runner.start_historical()
        self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.assertEqual(self.runner.summary('historical')['duplicates'],1)
        self.assertEqual(self.main.execute('SELECT count(*) FROM launches').fetchone()[0],main_before)

    async def test_old_shadow_event_does_not_regress_live_target_position(self):
        later=event(self.t,index=2)
        later['blockNumber']=hex(self.base+9)
        self.runner.worker.ingest(self.t,later)
        older=event(self.t,index=1)
        self.mock_rpc(self.base+9,[older])
        await self.runner.start_historical()
        self.assertTrue(await self.runner.run_stage('historical'))
        row=self.db.target(self.t['launch_id'])
        self.assertEqual(row['last_event_block'],self.base+9)

    async def test_concurrent_live_and_shadow_writers_keep_one_canonical_row(self):
        row=event(self.t)
        def write(shadow):
            connection=FlowDB(self.path/'flow.db')
            try:return connection.store(self.t,copy.deepcopy(row),decode_event(row,self.t),shadow=shadow)
            finally:connection.conn.close()
        await asyncio.gather(asyncio.to_thread(write,False),asyncio.to_thread(write,True))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.assertEqual(self.db.conn.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    async def test_crash_after_event_commit_replays_without_duplicate(self):
        row=event(self.t)
        self.mock_rpc(self.base,[row])
        await self.runner.start_historical()
        original=self.runner.worker.ingest
        def crash(*args,**kwargs):
            original(*args,**kwargs)
            raise OSError('simulated crash')
        with patch.object(self.runner.worker,'ingest',side_effect=crash):
            with self.assertRaises(OSError):await self.runner.run_stage('historical')
        self.assertEqual(self.runner.summary('historical')['jobs'][0]['next_unverified_block'],self.base)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)

    async def test_small_stop_and_wss_ready_tails_promote_only_verified_cursor(self):
        self.mock_rpc(self.base+5)
        await self.runner.start_historical()
        self.assertTrue(await self.runner.run_stage('historical'))
        plan=self.runner.tail_plan(self.base+7)
        self.assertEqual(plan['ranges'],[(1,'curve',self.base+6,self.base+7)])
        self.assertTrue(plan['ready'])
        self.runner.add_jobs('stop_tail',self.base+6,self.base+8)
        self.assertTrue(await self.runner.run_stage('stop_tail'))
        self.runner.promote('stop_tail')
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+8))
        self.runner.add_jobs('wss_ready_tail',self.base+9,self.base+10)
        self.assertTrue(await self.runner.run_stage('wss_ready_tail'))
        self.runner.set_meta('H_live',self.base+10)
        self.runner.set_meta('H_live_at',int(__import__('time').time()))
        self.runner.promote('wss_ready_tail')
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+10))

    async def test_completed_shadow_can_extend_to_new_common_head(self):
        self.mock_rpc(self.base+5)
        await self.runner.start_historical()
        self.assertTrue(await self.runner.run_stage('historical'))
        self.patch.stop();self.mock_rpc(self.base+12)
        self.assertEqual(await self.runner.start_historical(),self.base+12)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.assertEqual(self.runner.summary('historical')['blocks_verified'],13)
        self.assertEqual(self.runner.meta('H_prefetch'),str(self.base+12))

    async def test_late_target_cursor_does_not_invalidate_old_prefetch(self):
        head=self.base+5
        self.mock_rpc(head)
        await self.runner.start_historical()
        self.assertTrue(await self.runner.run_stage('historical'))
        for launch,offset in ((2,20),(3,22)):
            t=target(launch=launch);t['launch_block']=self.base+offset
            t['token_address']='0x'+format(launch,'040x')
            t['curve_address']='0x'+format(launch+100,'040x')
            insert_target(self.db,t)
            self.db.set_state(f'recovery:{launch}:curve',self.base+25)
        # Reproduce the empty, complete job written by the earlier preflight.
        with self.db.conn:self.db.conn.execute('''INSERT INTO flow_shadow_jobs
          (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
           next_unverified_block,highest_contiguous_verified_block,completion_status)
          VALUES('historical',2,'curve',?,?,?,?,'complete')''',
          (self.base+20,head,self.base+20,self.base+19))
        self.runner.add_jobs('historical',0,head)
        self.assertEqual({j['launch_id'] for j in self.runner.summary('historical')['jobs']},{1,2})
        self.assertIn((3,'curve',self.base+22,self.base+30),self.runner.tail_plan(self.base+30)['ranges'])
        self.patch.stop();calls=self.mock_rpc(self.base+30)
        self.assertEqual(await self.runner.start_historical(),self.base+30)
        self.assertTrue(await self.runner.run_stage('historical'))
        jobs={j['launch_id']:j for j in self.runner.summary('historical')['jobs']}
        self.assertEqual(jobs[2]['highest_contiguous_verified_block'],self.base+30)
        self.assertIn((self.base+20,self.base+30),calls)

    async def test_proved_gap_handoff_resolves_and_stops_redundant_recovery(self):
        self.mock_rpc(self.base+10)
        self.runner.set_meta('H_prefetch',self.base+5)
        self.runner.add_jobs('historical',0,self.base+5)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.runner.add_jobs('stop_tail',self.base+6,self.base+8)
        self.assertTrue(await self.runner.run_stage('stop_tail'))
        self.runner.add_jobs('wss_ready_tail',self.base+9,self.base+10)
        self.assertTrue(await self.runner.run_stage('wss_ready_tail'))
        self.db.gap(1,1000,1001,'ws_gap',self.base+1)
        now=int(__import__('time').time())
        self.runner.set_meta('H_live',self.base+10);self.runner.set_meta('H_live_at',now)
        self.runner.promote('wss_ready_tail')
        self.assertEqual(self.db.conn.execute('SELECT resolved FROM flow_gaps').fetchone()[0],1)
        self.assertEqual(self.db.state('flow_shadow_handoff:1'),f'{self.base+10}:{now}')
        worker=self.runner.worker;worker.connection_started_at=now-5
        worker.subscriptions[(1,'curve')]='ack';worker.pending_recovery.add(1)
        self.assertTrue(worker.accept_shadow_handoff(self.db.target(1)))
        self.assertNotIn(1,worker.pending_recovery)

    async def test_interrupted_or_unproved_handoff_keeps_gap(self):
        self.mock_rpc(self.base+10)
        self.db.set_state('recovery:1:curve',self.base+4)
        self.runner.set_meta('H_prefetch',self.base+5)
        self.runner.add_jobs('historical',0,self.base+5)
        self.assertTrue(await self.runner.run_stage('historical'))
        self.runner.add_jobs('stop_tail',self.base+6,self.base+8)
        self.assertTrue(await self.runner.run_stage('stop_tail'))
        self.runner.add_jobs('wss_ready_tail',self.base+9,self.base+10)
        self.db.gap(1,1000,1001,'ws_gap',self.base+1)
        self.runner.set_meta('H_live',self.base+10)
        self.runner.set_meta('H_live_at',int(__import__('time').time()))
        with self.assertRaises(RpcError):self.runner.promote('wss_ready_tail')
        self.assertEqual(self.db.conn.execute('SELECT resolved FROM flow_gaps').fetchone()[0],0)
        self.assertTrue(await self.runner.run_stage('wss_ready_tail'))
        self.runner.promote('wss_ready_tail')
        self.assertEqual(self.db.conn.execute('SELECT resolved FROM flow_gaps').fetchone()[0],0)
        self.assertIsNone(self.db.state('flow_shadow_handoff:1'))


class ProvenanceTests(unittest.TestCase):
    def test_unverifiable_staging_directory_cannot_authorize_cutover(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError,'not a Git checkout'):
                verified_checkout(directory)

    def test_legacy_flow_config_keeps_alchemy_route_and_eight_mb_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'flow.env'
            path.write_text('FLOW_TRACKING_ENABLED=true\nFLOW_MAX_WS_BYTES_PER_DAY=8000000\nFLOW_RECOVERY_MAX_BLOCKS=100\n')
            path.chmod(0o600)
            with patch.dict(os.environ,{'FLOW_ENV':str(path)}):settings=FlowSettings.load()
        self.assertFalse(settings.split_enabled)
        self.assertEqual(settings.daily_ws_bytes,8_000_000)
