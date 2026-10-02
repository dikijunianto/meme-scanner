import copy
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, AsyncMock

from app.config import Config
from app.flow_data import FlowDB
from app.flow_offline_drain import OfflineDrain, OfflineAbort, dry_plan
from app.flow_providers import FlowProviders, provider
from app.flow_worker import FlowSettings, FlowWorker, FlowBudget
from app.flow_shadow import ShadowReconciler, ShadowRpc
from app.rpc import Rpc, RpcError, LogRangeError
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"scripts"))
from scripts.flow_offline_drain import offline_guard, database_preflight, parser, main as cli_main
from tests.test_flow import target, insert_target, main_schema, event, fixture


class OfflineDrainTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.path = Path(self.tmp.name)
        self.main = main_schema(self.path/'main.db')
        self.db = FlowDB(self.path/'flow.db'); self.db.migrate()
        self.t = insert_target(self.db, target()); self.base = self.t['launch_block']
        self.clock = patch('app.flow_offline_drain.time.time', return_value=1100); self.clock.start()
        self.addCleanup(self.clock.stop)
        self.config = Config('https://alchemy.invalid/SECRET', 4663, (), '', self.path/'main.db', self.path/'log')
        self.settings = FlowSettings(database=self.path/'flow.db', split_enabled=True, minute_calls=100)
        self.providers = FlowProviders('https://mainnet.robinhood.validationcloud.io/v1/SECRET',
                                      'wss://mainnet.robinhood.validationcloud.io/v1/SECRET')
        self.worker = FlowWorker(self.config, self.settings, self.db, self.providers)
        self.old = self.worker.rpc
        self.active = False
        ShadowReconciler._schema(type('Schema', (), {'db': self.db})())
        self.runner = OfflineDrain(self.worker, self.guard, 'test', 'a'*40, 123)
        self.worker.rpc.config = replace(self.worker.rpc.config, rpc_rps=100000)
        self.calls = []; self.logs = []; self.head = self.base+30; self.fail_from = None
        self.after_send = None; self.methods = []
        async def answer(rpc, payload, method):
            self.assertEqual(provider(rpc.config.rpc_http), 'validation')
            self.assertEqual(rpc.config.fallback_http, '')
            self.assertEqual(rpc.config.rpc_ws, '')
            self.methods.append(method)
            if method == 'eth_chainId': value = hex(4663)
            elif method == 'eth_blockNumber': value = hex(self.head)
            elif method == 'eth_getBlockByNumber':
                value = {'number':payload['params'][0], 'timestamp':hex(1100), 'hash':'0x'+'11'*32}
            elif method == 'eth_getLogs':
                q = payload['params'][0]; first, last = int(q['fromBlock'],16), int(q['toBlock'],16)
                self.calls.append((first,last,q))
                if self.fail_from is not None and first >= self.fail_from: raise RpcError('SECRET provider failure')
                value = [copy.deepcopy(x) for x in self.logs if first <= int(x['blockNumber'],16) <= last]
                if self.after_send: self.after_send()
            else: self.fail('Forbidden RPC method '+method)
            return {'jsonrpc':'2.0','id':payload['id'],'result':value}
        self.rpc_patch = patch.object(Rpc, '_send', answer); self.rpc_patch.start()
        self.addCleanup(self.rpc_patch.stop)

    async def asyncTearDown(self):
        await self.old.close(); await self.worker.rpc.close()
        self.worker.main.close(); self.main.close(); self.db.conn.close(); self.tmp.cleanup()

    def guard(self):
        if self.active: raise OfflineAbort('Flow active')

    async def test_split_large_production_debt_multiple_gaps_and_manifest(self):
        for launch, offset in ((2,10000),(3,25000)):
            t = target(launch=launch); t['launch_block'] = self.base+offset
            t['token_address'] = '0x'+format(launch,'040x'); t['curve_address'] = '0x'+format(launch+100,'040x')
            insert_target(self.db,t)
        self.head = self.base+35000
        for t in [self.db.target(i) for i in (1,2,3)]:
            self.db.require_bootstrap(t,'curve',t['launch_block'])
            self.db.gap(t['launch_id'],1000,1090,'reconnect_recovery_incomplete',t['launch_block'])
            self.db.gap(t['launch_id'],1000,1090,'recovery_rejection',t['launch_block'])
        before = [dict(r) for r in self.db.conn.execute('select * from flow_gaps')]
        with self.assertRaises(RpcError): self.worker.recovery_plan(self.t,self.head)
        result = await self.runner.run()
        self.assertTrue(result['restart_safe_now'])
        for launch in (1,2,3): self.assertEqual(self.db.state(f'recovery:{launch}:curve'),str(self.head))
        after = [dict(r) for r in self.db.conn.execute('select * from flow_gaps')]
        self.assertEqual([{**r,'resolved':1} for r in before],after)
        self.assertEqual(len(result['manifest']['gaps_resolved']),9)
        self.assertTrue(all(q['fromBlock'] >= hex(self.base) for _,_,q in self.calls))
        self.assertEqual(self.db.used('flow_eth_getTransactionReceipt',0),0)
        self.assertNotIn('SECRET',json.dumps(result))
        self.assertEqual(self.db.used('flow_http_calls_alchemy',0),0)
        self.assertEqual(self.worker.rpc.config.rpc_rps,100000)  # test-only pacing override

    async def test_offline_debt_then_new_worker_discovers_downtime_curve(self):
        from app.flow_data import iso
        await self.test_split_large_production_debt_multiple_gaps_and_manifest()
        t=target(launch=4);t['launch_block']=self.base+30000
        t['token_address']='0x'+format(4,'040x');t['curve_address']='0x'+format(104,'040x')
        self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',
                          (4,t['token_address'],t['quote_asset_address'],t['curve_address'],t['creator_address'],iso(1000),t['launch_block'],0,1))
        self.main.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',(4,0,'random_initial',iso(1000)));self.main.commit()
        new=FlowWorker(self.config,self.settings,self.db,self.providers)
        new.header=AsyncMock(return_value=1000)
        self.db.set_state('phase2b_coverage_start_at',900)
        self.db.set_state('quote_decimals:'+t['quote_asset_address'],18)
        def fast(*args,**kwargs):
            rpc=ShadowRpc(*args,**kwargs);rpc.config=replace(rpc.config,rpc_rps=100000);return rpc
        try:
            await new.discover()
            self.assertEqual(new.bootstrap_kinds(self.db.target(4)),{'curve'})
            self.assertIsNone(self.db.state('recovery:4:curve'))
            with patch('app.flow_shadow.ShadowRpc',side_effect=fast):
                new.subscribe=AsyncMock(return_value=['curve']);new.discover=AsyncMock()
                # Previously drained targets use normal proof-backed recovery; only4 bootstraps.
                new.rpc.config=replace(new.rpc.config,rpc_rps=100000)
                for _ in range(3):
                    new.bootstrap_retry_at.clear()
                    await new.reconcile()
            self.assertEqual(self.db.state('recovery:4:curve'),str(self.head))
            self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_shadow_ranges WHERE stage='live_bootstrap:4'").fetchone()[0],3)
            self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_gaps WHERE launch_id=4 AND reason='reconnect_recovery_incomplete'").fetchone()[0],0)
            self.assertTrue(self.runner.assessment(self.head)['restart_safe_now'])
        finally:
            await new.rpc.close();new.main.close()

    async def test_partial_chunks_hold_cursor_and_resume_first_unverified(self):
        self.head = self.base+5000; self.fail_from = self.base+2000
        result = await self.runner.run()
        self.assertFalse(result['restart_safe_now']); self.assertIsNone(self.db.state('recovery:1:curve'))
        self.assertEqual(self.db.conn.execute('select count(*) from flow_shadow_ranges').fetchone()[0],1)
        self.fail_from = None; before = len(self.calls)
        result = await self.runner.run()
        self.assertEqual(self.calls[before][0],self.base+2000)
        self.assertTrue(result['restart_safe_now'])

    async def test_zero_log_proof_and_idempotency(self):
        first = await self.runner.run(); n = len(self.calls)
        again = await self.runner.run()
        self.assertTrue(first['restart_safe_now']); self.assertTrue(again['restart_safe_now'])
        self.assertEqual(n,len(self.calls)); self.assertEqual(self.db.conn.execute('select count(*) from flow_events').fetchone()[0],0)
        self.assertEqual(self.db.conn.execute('select count(*) from flow_shadow_ranges').fetchone()[0],1)
        self.assertEqual(first['manifest']['snapshot_digest'],again['manifest']['snapshot_digest'])

    async def test_canonical_event_dedupe(self):
        row = event(self.t); self.logs = [row,row]
        result = await self.runner.run()
        self.assertEqual(result['manifest']['proof'][0]['recovered_events'],1)
        self.assertEqual(result['manifest']['proof'][0]['duplicates'],1)
        self.assertEqual(self.db.conn.execute('select count(*) from flow_events').fetchone()[0],1)

    async def test_dry_run_zero_rpc_and_database_mutation(self):
        self.db.set_state('last_connected_block',self.head)
        before = '\n'.join(self.db.conn.iterdump()); changes = self.db.conn.total_changes
        result = dry_plan(self.worker)
        self.assertEqual(before,'\n'.join(self.db.conn.iterdump()))
        self.assertEqual(changes,self.db.conn.total_changes); self.assertEqual(self.methods,[])
        self.assertEqual(result['rpc_calls'],0); self.assertNotIn('SECRET',json.dumps(result))
        self.assertEqual(result['filters'][0]['safe_start'],self.base)

    async def test_flow_active_refuses_before_rpc_or_write(self):
        before = self.db.conn.total_changes; self.active = True
        with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertEqual(self.methods,[]); self.assertEqual(self.db.conn.total_changes,before)

    async def test_mid_request_worker_start_discards_uncommitted_proof(self):
        self.after_send = lambda: setattr(self,'active',True)
        with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertEqual(self.runner.raw.execute('select count(*) from flow_shadow_ranges').fetchone()[0],0)
        self.assertIsNone(self.runner.raw.execute("select value from flow_state where key='recovery:1:curve'").fetchone())

    async def test_no_tx_receipt_or_other_provider_method_possible(self):
        for method in ('eth_getTransactionByHash','eth_getTransactionReceipt','eth_call','eth_subscribe'):
            with self.assertRaises((OfflineAbort,ValueError)): await self.worker.rpc.call(method,[])
        self.assertEqual(self.methods,[])
        self.worker.rpc.config = replace(self.worker.rpc.config,rpc_http='https://robinhood-rpc.publicnode.com')
        with self.assertRaises(OfflineAbort): await self.worker.rpc.call('eth_blockNumber',[])
        self.worker.rpc.config = replace(self.worker.rpc.config,rpc_http='https://robinhood-mainnet.g.alchemy.com/v2/SECRET')
        with self.assertRaises(OfflineAbort): await self.worker.rpc.call('eth_blockNumber',[])
        self.assertEqual(self.methods,[])

    async def test_reserve_pending_then_resume_after_reset(self):
        self.head = self.base+4000
        self.db.count('flow_eth_getLogs',349)
        result = await self.runner.run()
        self.assertEqual(result['gate'],'OFFLINE_DRAIN_BUDGET_PENDING')
        self.assertEqual(len(self.calls),1); self.assertIsNone(self.db.state('recovery:1:curve'))
        self.assertEqual(self.db.used('flow_eth_getLogs',0),350)
        with self.db.conn: self.db.conn.execute('update flow_usage set minute=minute-86400')
        self.assertTrue((await self.runner.run())['restart_safe_now'])

    async def test_interruption_after_proof_before_promotion(self):
        with patch.object(self.runner,'promote_offline',side_effect=OfflineAbort('interrupted')):
            with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertIsNone(self.db.state('recovery:1:curve')); n = len(self.calls)
        self.assertTrue((await self.runner.run())['restart_safe_now']); self.assertEqual(n,len(self.calls))

    async def test_interruption_after_cursor_before_report(self):
        with patch.object(self.runner,'finish',side_effect=OfflineAbort('interrupted')):
            with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.head)); n=len(self.calls)
        self.assertTrue((await self.runner.run())['restart_safe_now']); self.assertEqual(n,len(self.calls))

    async def test_expired_targets_are_not_reactivated_or_made_eligible(self):
        with self.db.conn: self.db.conn.execute('update flow_tracking_targets set tracking_end_at=1099')
        result = await self.runner.run()
        self.assertTrue(result['restart_safe_now']); self.assertEqual(self.methods,[])
        self.assertIsNone(self.db.state('recovery:1:curve'))
        self.assertEqual(self.db.conn.execute('select count(*) from flow_feature_versions').fetchone()[0],0)
        self.assertEqual(self.db.target(1)['status'],'scheduled')

    async def test_mid_drain_expiry_aborts_then_rerun_excludes_target(self):
        def expire():
            self.clock.stop()
            self.clock = patch('app.flow_offline_drain.time.time',return_value=4700)
            self.clock.start()
            self.addCleanup(self.clock.stop)
        self.after_send = expire
        with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertIsNone(self.db.state('recovery:1:curve'))
        self.assertTrue((await self.runner.run())['restart_safe_now'])
        self.assertEqual(self.db.conn.execute('select count(*) from flow_feature_versions').fetchone()[0],0)

    async def test_moving_head_uses_explicit_catchup_and_normal_guard_unchanged(self):
        self.head = self.base+500
        def move(): self.head = self.base+1000; self.after_send = None
        self.after_send = move
        result = await self.runner.run()
        self.assertTrue(result['restart_safe_now']); self.assertEqual(len(result['manifest']['heads']),2)
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+1000))
        with self.assertRaises(FlowBudget): self.worker.recovery_plan(self.t,self.base+1200)

    async def test_graduation_activation_positions_pit_and_immutability(self):
        self.db.activate_pit_ledger('b'*40,now=900)
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.base+20)
        self.db.rebuild(self.db.target(1))
        before = [dict(r) for r in self.db.conn.execute('select * from flow_feature_versions')]
        g = fixture('v4_buy')['launch']; g['block_number']=self.base+20; g['block_timestamp']='1970-01-01T00:17:30+00:00'
        with self.db.conn: self.db.conn.execute('update flow_tracking_targets set graduation_json=?,current_phase=? where launch_id=1',(json.dumps(g),'graduated'))
        self.head = self.base+400
        result = await self.runner.run()
        self.assertTrue(result['restart_safe_now'])
        jobs=result['manifest']['proof'][0]['jobs']
        self.assertEqual({j['kind']:j['original_safe_start'] for j in jobs}, {'v4':self.base+20,'hook':self.base+20})
        for kind in ('v4','hook'): self.assertEqual(self.db.state('recovery:1:'+kind),str(self.head))
        after = [dict(r) for r in self.db.conn.execute('select * from flow_feature_versions')]
        for row in before: self.assertIn(row,after)
        self.assertTrue(all(r['model_eligible_at'] is None or r['model_eligible_at']>=r['materialized_at'] for r in after))
        self.assertNotIn('SECRET',json.dumps(result))

    async def test_reuse_requires_exact_durable_filter_identity(self):
        await self.runner.run(); calls=len(self.calls)
        self.db.conn.execute("delete from flow_state where key='recovery:1:curve'"); self.db.conn.commit()
        self.runner.generation='resume'; self.runner.prefix='offline_drain:resume'
        result=await self.runner.run()
        self.assertEqual(calls,len(self.calls)); self.assertTrue(result['manifest']['reused'])
        self.db.conn.execute("delete from flow_state where key='recovery:1:curve'"); self.db.conn.commit()
        with self.db.conn: self.db.conn.execute("update flow_bootstrap_identity set query_json='{}'")
        self.runner.generation='different'; self.runner.prefix='offline_drain:different'
        await self.runner.run(); self.assertGreater(len(self.calls),calls)

    async def test_unknown_gap_and_future_gap_remain_blockers(self):
        self.db.gap(1,1000,1100,'reorg_unresolved',self.base)
        self.db.gap(1,1000,1200,'ws_gap',self.base+400)
        result = await self.runner.run(max_rounds=1)
        self.assertFalse(result['restart_safe_now']); self.assertEqual(len(result['active_gap_ids']),2)

    async def test_adaptive_ranges_are_durable(self):
        original = Rpc._send
        async def adaptive(rpc,payload,method):
            if method=='eth_getLogs':
                q=payload['params'][0]
                if int(q['toBlock'],16)-int(q['fromBlock'],16)>10: raise LogRangeError('too large')
            return await original(rpc,payload,method)
        with patch.object(Rpc,'_send',adaptive): result = await self.runner.run()
        self.assertTrue(result['restart_safe_now']); self.assertGreater(result['manifest']['proof'][0]['reductions'],0)

    def test_service_state_guards_and_cli_acknowledgement(self):
        good={'meme-scanner.service':{'ActiveState':'active','SubState':'running','MainPID':'10','NRestarts':'0'},
              'meme-scanner-flow.service':{'ActiveState':'inactive','SubState':'dead','MainPID':'0','NRestarts':'0'}}
        with patch('scripts.flow_offline_drain.services',return_value=good),patch('scripts.flow_offline_drain.Path.exists',return_value=False):
            self.assertEqual(offline_guard(123),good)
            for state in ('active','activating','deactivating','failed'):
                bad=copy.deepcopy(good);bad['meme-scanner-flow.service']['ActiveState']=state
                with patch('scripts.flow_offline_drain.services',return_value=bad):
                    with self.assertRaises(OfflineAbort): offline_guard(123)
            bad=copy.deepcopy(good);bad['meme-scanner.service']['ActiveState']='inactive'
            with patch('scripts.flow_offline_drain.services',return_value=bad):
                with self.assertRaises(OfflineAbort): offline_guard(123)
        self.assertFalse(parser().parse_args(['--expected-revision','a'*40,'--generation','test','--source-flow-pid','123']).acknowledge_flow_offline)

    def test_preflight_tx_budget_and_integrity_guards(self):
        database_preflight(self.db,self.main,self.settings)
        self.db.count('flow_eth_getTransactionReceipt')
        with self.assertRaises(OfflineAbort): database_preflight(self.db,self.main,self.settings)

    def test_secret_error_redaction(self):
        from unittest.mock import AsyncMock
        args=['tool','--expected-revision','a'*40,'--source-flow-pid','123','--generation','safe']
        with patch('sys.argv',args),patch('scripts.flow_offline_drain.run',new=AsyncMock(side_effect=RpcError('https://SECRET'))),patch('builtins.print') as output:
            with self.assertRaises(SystemExit): cli_main()
        self.assertNotIn('SECRET',str(output.call_args))

    async def test_complete_proof_with_hole_cannot_promote(self):
        with patch.object(self.runner,'promote_offline',side_effect=OfflineAbort('interrupted')):
            with self.assertRaises(OfflineAbort): await self.runner.run()
        with self.db.conn: self.db.conn.execute('delete from flow_shadow_ranges')
        with self.assertRaises(RpcError): await self.runner.run()
        self.assertIsNone(self.db.state('recovery:1:curve'))

    async def test_existing_in_progress_bootstrap_requires_activation_proof(self):
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.set_state('recovery:1:curve',self.base+10)
        with self.db.conn: self.db.conn.execute("update flow_bootstrap set status='in_progress'")
        await self.runner.run()
        self.assertEqual(self.calls[0][0],self.base)
        self.assertEqual(self.db.conn.execute('select status from flow_bootstrap').fetchone()[0],'complete')

    async def test_existing_cursor_tail_starts_at_next_block(self):
        self.head=self.base+300
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.base+10)
        await self.runner.run()
        self.assertEqual(self.calls[0][0],self.base+11)

    async def test_restart_assessment_obeys_actual_overlap_guard(self):
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.head)
        self.assertTrue(self.runner.assessment(self.head+97)['restart_safe_now'])
        self.assertFalse(self.runner.assessment(self.head+98)['restart_safe_now'])
        self.assertFalse(self.runner.assessment(self.head-1)['restart_safe_now'])

    async def test_snapshot_identity_change_aborts_without_cursor(self):
        self.head=self.base+3000; self.fail_from=self.base+2000
        await self.runner.run()
        with self.db.conn: self.db.conn.execute("update flow_tracking_targets set curve_address=?",('0x'+'33'*20,))
        self.fail_from=None; calls=len(self.calls)
        with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertEqual(len(self.calls),calls)
        self.assertIsNone(self.db.state('recovery:1:curve'))

    async def test_worker_start_before_promotion_prevents_cursor_and_gap_writes(self):
        self.db.require_bootstrap(self.t,'curve',self.base)
        original=self.runner.promote_offline
        def start(*args):
            self.active=True
            return original(*args)
        with patch.object(self.runner,'promote_offline',side_effect=start):
            with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertIsNone(self.runner.raw.execute("select value from flow_state where key='recovery:1:curve'").fetchone())
        self.assertEqual(self.runner.raw.execute('select resolved from flow_gaps').fetchone()[0],0)

    async def test_snapshot_error_strings_do_not_enter_manifest(self):
        with self.db.conn: self.db.conn.execute("update flow_tracking_targets set error_code='https://SECRET'")
        self.db.gap(1,1000,1100,'https://SECRET',self.base)
        result=await self.runner.run(max_rounds=1)
        self.assertNotIn('SECRET',json.dumps(result))
        self.assertFalse(result['restart_safe_now'])

    @unittest.skipUnless(__import__('os').name=='posix','Ubuntu operator CLI')
    async def test_cli_dry_run_zero_rpc_and_chain_preflight_before_manifest(self):
        from scripts.flow_offline_drain import run
        from unittest.mock import AsyncMock
        import os
        import pwd
        args=parser().parse_args(['--acknowledge-flow-offline','--expected-revision','a'*40,
                                 '--source-flow-pid','123','--generation','cli','--dry-run'])
        before='\n'.join(self.db.conn.iterdump())
        with (patch('scripts.flow_offline_drain.verified_checkout',return_value='a'*40),
              patch('scripts.flow_offline_drain.offline_guard',return_value={}),
              patch('scripts.flow_offline_drain.checked_file'),
              patch('scripts.flow_offline_drain.readable_as_service',return_value=True),
              patch('scripts.flow_offline_drain.no_network_probe',new=AsyncMock()),
              patch('scripts.flow_offline_drain.Config.load',return_value=self.config),
              patch('scripts.flow_offline_drain.FlowSettings.load',return_value=replace(self.settings,minute_calls=12,enabled=True)),
              patch('scripts.flow_offline_drain.FlowProviders.load',return_value=self.providers),
              patch('pwd.getpwnam',return_value=type('User',(),{'pw_uid':os.geteuid()})())):
            result=await run(args)
            self.assertEqual(result['rpc_calls'],0)
            self.assertEqual(before,'\n'.join(self.db.conn.iterdump()))
            args.dry_run=False
            # Fail chain verification: no maintenance manifest, target/cursor/gap mutation.
            with patch('app.flow_offline_drain.OfflineRpc.check_chain',new=AsyncMock(side_effect=ValueError('wrong chain'))):
                with self.assertRaises(ValueError): await run(args)
            self.assertIsNone(self.db.conn.execute("select value from flow_shadow_meta where key='offline_drain:cli:manifest'").fetchone())
            self.assertIsNone(self.db.state('recovery:1:curve'))

    @unittest.skipUnless(__import__('os').name=='posix','Linux shared writer lock')
    def test_shared_writer_lock_excludes_worker_and_operators(self):
        from app.flow_lock import flow_writer_lock
        with flow_writer_lock(self.path/'flow.db'):
            with self.assertRaises(RuntimeError):
                with flow_writer_lock(self.path/'flow.db'): self.fail('concurrent owner')
        with flow_writer_lock(self.path/'flow.db'): pass

    async def test_restart_safe_filters_skip_getlogs(self):
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.head-10)
        result=await self.runner.run()
        self.assertTrue(result['restart_safe_now']); self.assertEqual(self.calls,[])

    @unittest.skipUnless(__import__('os').name=='posix','Linux worker entrypoint writer lock')
    def test_worker_takes_lock_before_opening_database(self):
        from app.flow_lock import flow_writer_lock
        from app.flow_worker import main
        with (flow_writer_lock(self.path/'flow.db'),
              patch('app.flow_worker.FlowSettings.load',return_value=replace(self.settings,enabled=True)),
              patch('app.flow_worker.Config.load',return_value=self.config),
              patch('app.flow_worker.FlowProviders.load',return_value=self.providers),
              patch('app.flow_worker.FlowDB',side_effect=AssertionError('DB opened during maintenance'))):
            with self.assertRaises(RuntimeError): main()

    async def test_protected_272959_eligible_version_is_unchanged(self):
        self.db.activate_pit_ledger('b'*40,now=900)
        protected=target(launch=272959)
        protected['token_address']='0x'+format(272959,'040x')
        protected['curve_address']='0x'+format(272960,'040x')
        protected['tracking_end_at']=1099
        protected=insert_target(self.db,protected)
        with patch('app.flow_offline_drain.time.time',return_value=1090):
            self.db.require_bootstrap(protected,'curve',protected['launch_block'])
            self.db.complete_bootstrap(272959,'curve',protected['launch_block']+20)
            self.db.rebuild(self.db.target(272959))
        before=[dict(r) for r in self.db.conn.execute('select * from flow_feature_versions where launch_id=272959')]
        self.assertTrue(any(r['window_seconds']==30 and r['model_eligible_at'] is not None for r in before))
        await self.runner.run()
        self.assertEqual(before,[dict(r) for r in self.db.conn.execute('select * from flow_feature_versions where launch_id=272959')])

    async def test_mismatched_bootstrap_gap_activation_is_not_resolved(self):
        self.db.require_bootstrap(self.t,'curve',self.base)
        with self.db.conn: self.db.conn.execute('update flow_gaps set first_block=first_block+1')
        result=await self.runner.run(max_rounds=1)
        self.assertFalse(result['restart_safe_now'])
        self.assertEqual(self.db.conn.execute('select resolved from flow_gaps').fetchone()[0],0)

    async def test_interruption_before_gap_resolution_rolls_back_promotion(self):
        self.db.require_bootstrap(self.t,'curve',self.base)
        execute=self.db.conn.execute
        def interrupted(sql,*args):
            if sql.startswith('UPDATE flow_gaps SET resolved=1'):
                raise OfflineAbort('interrupted before gap reconciliation')
            return execute(sql,*args)
        with patch.object(self.db.conn,'execute',side_effect=interrupted):
            with self.assertRaises(OfflineAbort): await self.runner.run()
        self.assertIsNone(self.db.state('recovery:1:curve'))
        self.assertEqual(self.db.conn.execute('select resolved from flow_gaps').fetchone()[0],0)
        calls=len(self.calls)
        self.assertTrue((await self.runner.run())['restart_safe_now'])
        self.assertEqual(calls,len(self.calls))
