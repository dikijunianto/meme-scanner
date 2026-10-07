"""Expired debt is proof-only work, never target resurrection or retrospective PIT."""
import json
from unittest.mock import patch
from tests import test_flow_active_gaps as fixtures
import unittest


class ExpiredRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def bounded(self,last=81419346):
        worker,t=await self.incident()
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial',completed_at=1791268903.0168962 WHERE launch_id=309266")
        from app.flow_expired_recovery import retain_verified_bounds
        with patch('app.flow_data.time.time',return_value=1791269000):
            retain_verified_bounds(self.fresh,4,
                {'number':hex(last),'timestamp':hex(1791268015),'hash':'0x'+'12'*32},
                {'number':hex(last+1),'timestamp':hex(1791268016),'hash':'0x'+'13'*32,'parentHash':'0x'+'12'*32})
        return worker,self.fresh.target(309266)

    async def test_expired_309266_with_exact_bounds_is_scheduled(self):
        worker,t=await self.incident()
        gap=dict(self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE id=4').fetchone())
        from app.flow_switch_recovery import semantics
        from app.flow_identity import query_identity
        from app.flow_expired_recovery import target_identity
        bounds={'epoch_id':'epoch2','gap':gap,'target_status':'partial',
                'tracking_end_at':t['tracking_end_at'],'target_identity':target_identity(t),'lifecycle':semantics(self.fresh,gap),
                'filters':[{'kind':'curve','query':query_identity(worker.filters(t)['curve']),
                            'first':81419247,'last':81419346}],
                'upper_header':{'number':hex(81419346),'timestamp':hex(1791268015),
                                'hash':'0x'+'12'*32},
                'next_header':{'number':hex(81419347),'timestamp':hex(1791268016),
                               'hash':'0x'+'13'*32,'parentHash':'0x'+'12'*32}}
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial',completed_at=1791268903.0168962 WHERE launch_id=309266")
        self.fresh.set_state('expired_gap_bounds:4',json.dumps(bounds,sort_keys=True))
        with patch('app.flow_data.time.time',return_value=1791269000):
            from app.flow_gap_recovery import obligations
            debt=obligations(self.fresh)[0]
            self.assertEqual(debt['state'],'queued')
            self.assertEqual(debt['mode'],'EXPIRED_GAP_FORENSIC_RECOVERY')

    async def test_zero_logs_exact_proof_preserves_expiry_cursor_pit_and_historical_debt(self):
        worker,t=await self.bounded()
        from app.flow_expired_recovery import recover
        from app.flow_gap_recovery import obligations
        before=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=309266')]
        cursor=self.fresh.state('recovery:309266:curve');self.calls=[]
        worker.subscribe=__import__('unittest.mock',fromlist=['AsyncMock']).AsyncMock(side_effect=AssertionError('expired subscription'))
        with patch('app.flow_data.time.time',return_value=1791269000):
            self.assertNotEqual(self.fresh.current_health(),'healthy')
            self.assertTrue(await recover(worker,obligations(self.fresh)[0]))
            self.assertEqual(self.calls,['eth_getLogs'])
            self.assertEqual(self.fresh.target(309266),t)
            self.assertEqual(self.fresh.state('recovery:309266:curve'),cursor)
            self.assertEqual(self.fresh.current_health(),'healthy')
            self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=309266')],before)
            self.fresh.rebuild(t,1791269000)
            self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE launch_id=309266 AND model_eligible_at IS NOT NULL').fetchone())
        from app.flow_provider_switch import latest
        self.assertEqual(latest(self.db)['state'],'FAILED')
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_gaps WHERE resolved=0').fetchone()[0],2)
        worker.subscribe.assert_not_called()

    async def test_missing_changed_or_unproved_bounds_are_refused_without_rpc(self):
        worker,t=await self.incident()
        with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=309266")
        from app.flow_expired_recovery import recover,retain_verified_bounds
        from app.flow_gap_recovery import obligations
        from app.flow_shadow import ShadowRpc
        with patch('app.flow_data.time.time',return_value=1791269000),patch.object(ShadowRpc,'call',side_effect=AssertionError('missing bounds RPC')):
            self.assertFalse(await recover(worker,obligations(self.fresh)[0]))
            self.assertIn('missing_exact',obligations(self.fresh)[0]['reason_detail'])
            with self.assertRaises(ValueError):retain_verified_bounds(self.fresh,4,
                {'number':hex(81419346),'timestamp':hex(1791268015),'hash':'0x'+'12'*32},
                {'number':hex(81419348),'timestamp':hex(1791268016),'hash':'0x'+'13'*32,'parentHash':'0x'+'12'*32})
        self.assertTrue(self.fresh.conn.execute('SELECT 1 FROM flow_gaps WHERE id=4 AND resolved=0').fetchone())

    async def test_partial_proof_budget_next_day_process_resume_no_moving_head(self):
        worker,t=await self.bounded(81423346)
        from app.flow_expired_recovery import recover
        from app.flow_gap_recovery import obligations
        from app.flow_budget import BudgetWait
        from app.flow_shadow import ShadowRpc
        now=1791269000;self.calls=[]
        with patch('app.flow_data.time.time',return_value=now):
            self.assertFalse(await recover(worker,obligations(self.fresh)[0]))
        before=dict(self.fresh.conn.execute("SELECT * FROM flow_shadow_jobs WHERE stage='expired_gap:4'").fetchone())
        self.assertGreater(before['next_unverified_block'],81419247)
        with patch('app.flow_data.time.time',return_value=now+1),patch.object(ShadowRpc,'call',side_effect=BudgetWait('daily_getlogs',350,350,1791331200)):
            self.assertFalse(await recover(worker,obligations(self.fresh)[0]))
            self.assertEqual(obligations(self.fresh)[0]['state'],'budget_wait')
        resumed=self.reopened_worker();self.fake_rpc(99999999,1791331201)
        with patch('app.flow_data.time.time',return_value=1791331201):
            while obligations(resumed.db):await recover(resumed,obligations(resumed.db)[0])
        self.assertTrue(all(method=='eth_getLogs' for method in self.calls))
        after=dict(self.fresh.conn.execute("SELECT * FROM flow_shadow_jobs WHERE stage='expired_gap:4'").fetchone())
        self.assertEqual(after['reconciliation_upper_bound'],81423346)
        self.assertEqual(after['highest_contiguous_verified_block'],81423346)
        self.assertEqual(self.fresh.target(309266),t)

    async def test_dedupe_reuse_overlap_and_identity_isolation(self):
        worker,t=await self.bounded()
        from app.flow_expired_recovery import recover,retain_verified_bounds,inventory
        from app.flow_gap_recovery import obligations
        from tests.test_flow import event
        from app.rpc import Rpc
        item=event(t,at=1791268005,index=93);item['blockNumber']=hex(81419300)
        self.calls=[]
        async def answer(rpc,payload,method):
            self.calls.append(method);self.assertEqual(method,'eth_getLogs')
            return {'jsonrpc':'2.0','id':payload['id'],'result':[dict(item),dict(item)]}
        before={k:t[k] for k in ('status','tracking_start_at','tracking_end_at','completed_at')}
        with patch('app.flow_data.time.time',return_value=1791269000),patch.object(Rpc,'_send',answer):
            self.assertTrue(await recover(worker,obligations(self.fresh)[0]))
            self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM flow_events WHERE launch_id=309266').fetchone()[0],1)
            self.assertEqual({k:self.fresh.target(309266)[k] for k in before},before)
            self.fresh.gap(309266,1791268000,1791268015.026573,'ws_gap',81419260)
            gap=dict(self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE resolved=0').fetchone())
            retained=json.loads(self.fresh.state('expired_gap_bounds:4'))
            retain_verified_bounds(self.fresh,gap['id'],retained['upper_header'],retained['next_header'])
            self.assertEqual(inventory(self.fresh)['estimated_getlogs'],0)
            self.assertTrue(await recover(worker,obligations(self.fresh)[0]))
            self.assertEqual(len(self.calls),1)
            gaps_before=self.fresh.conn.execute('SELECT count(*) FROM flow_gaps').fetchone()[0]
            self.assertFalse(worker.ingest(t,dict(item,blockHash='0x'+'99'*32),shadow='expired_gap:4'))
            self.assertFalse(worker.ingest(t,dict(item,removed=True),shadow='expired_gap:4'))
            self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM flow_gaps').fetchone()[0],gaps_before)
        from app.flow_identity import compatible_ranges
        from app.flow_switch_recovery import semantics
        wrong=dict(worker.filters(t)['curve'],address='0x'+'aa'*20)
        self.assertFalse(compatible_ranges(self.fresh,309266,'curve',wrong,semantics(self.fresh,{'launch_id':309266}),81419247,81419346))

    async def test_coalesced_cost_different_identity_and_bound_immutability(self):
        worker,t=await self.bounded(81423346)
        from app.flow_expired_recovery import inventory,retain_verified_bounds
        retained=json.loads(self.fresh.state('expired_gap_bounds:4'))
        with patch('app.flow_data.time.time',return_value=1791269000):
            self.fresh.gap(309266,1791268002,1791268015.026573,'ws_gap',81419300)
            second=dict(self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE id>4 ORDER BY id DESC LIMIT 1').fetchone())
            retain_verified_bounds(self.fresh,second['id'],retained['upper_header'],retained['next_header'])
            plan=inventory(self.fresh)
            self.assertEqual(plan['raw_gap_count'],2);self.assertEqual(len(plan['unique_proof_obligations']),1)
            self.assertEqual(plan['estimated_getlogs'],3)
            with self.assertRaises(ValueError):retain_verified_bounds(self.fresh,4,
                dict(retained['upper_header'],number=hex(81423347)),dict(retained['next_header'],number=hex(81423348)))
            changed=json.loads(self.fresh.state('expired_gap_bounds:4'));changed['filters'][0]['query']['topics']=['wrong']
            self.fresh.set_state('expired_gap_bounds:4',json.dumps(changed))
            self.assertEqual(inventory(self.fresh)['blocked_gap_count'],1)

    async def test_live_work_priority_and_foreign_epoch_rejection(self):
        worker,t=await self.bounded()
        from app.flow_expired_recovery import tick,retain_verified_bounds
        from app.flow_gap_recovery import obligations
        from unittest.mock import AsyncMock
        worker.discover=AsyncMock();worker.subscribe=AsyncMock(return_value=[])
        with patch('app.flow_data.time.time',return_value=1791269000),patch('app.flow_expired_recovery.recover',new_callable=AsyncMock) as run:
            await tick(worker,obligations(self.fresh),live_pending=True);run.assert_not_called()
            await tick(worker,obligations(self.fresh),live_pending=False);run.assert_awaited_once()
            self.assertEqual(self.fresh.state('expired_forensic_pending'),'1')
            retained=json.loads(self.fresh.state('expired_gap_bounds:4'))
            with self.assertRaises(ValueError):retain_verified_bounds(self.db,2,retained['upper_header'],retained['next_header'])

    async def test_fresh_target_bootstrap_and_windows_continue_during_backlog(self):
        worker,t=await self.bounded()
        from tests.test_flow import target,insert_target
        from app.flow_bootstrap import CursorBootstrap
        from app.flow_shadow import ShadowReconciler
        from app.flow_expired_recovery import tick
        from app.flow_gap_recovery import obligations
        new=target(launch=309999,start=1791269000,long=False)
        new.update(launch_block=81425000,curve_address='0x'+'ab'*20,token_address='0x'+'ac'*20)
        new=insert_target(self.fresh,new);self.fake_rpc(81425035,1791269040)
        original=worker.rpc;runner=ShadowReconciler(worker,reserve=50)
        try:
            with patch('app.flow_data.time.time',return_value=1791269040):
                result=await CursorBootstrap(runner).run('live_bootstrap:309999',[309999])
                self.assertEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE')
                self.fresh.rebuild(new,1791269040)
                self.assertTrue(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE launch_id=309999 AND window_seconds=30').fetchone())
                self.assertTrue(obligations(self.fresh))
                self.assertNotEqual(self.fresh.current_health(),'healthy')
                with patch('app.flow_expired_recovery.recover',new_callable=__import__('unittest.mock',fromlist=['AsyncMock']).AsyncMock) as recover:
                    await tick(worker,obligations(self.fresh),live_pending=True);recover.assert_not_called()
        finally:await worker.rpc.close();worker.rpc=original

    async def test_multiday_cost_and_discretionary_reserve(self):
        worker,t=await self.bounded(82219346)
        from app.flow_expired_recovery import inventory,tick
        from app.flow_gap_recovery import obligations
        from unittest.mock import AsyncMock
        with patch('app.flow_data.time.time',return_value=1791269000):
            plan=inventory(self.fresh)
            self.assertGreater(plan['estimated_getlogs'],350)
            self.assertEqual(plan['minimum_utc_budget_days'],2)
            self.fresh.count('flow_eth_getLogs',350,now=1791269000)
            with patch('app.flow_expired_recovery.recover',new_callable=AsyncMock) as recover:
                await tick(worker,obligations(self.fresh),False);recover.assert_not_called()
                self.assertEqual(obligations(self.fresh)[0]['state'],'budget_wait')
                self.assertNotEqual(self.fresh.current_health(),'healthy')

    async def test_expired_required_bootstrap_proves_only_fixed_interval(self):
        worker,t=await self.bounded()
        from tests.test_flow import target,insert_target
        from app.flow_expired_recovery import retain_verified_bounds,recover
        from app.flow_gap_recovery import obligations
        new=target(launch=311273,start=1791268020,long=False)
        new.update(launch_block=81419400,status='partial',completed_at=1791268930,token_address='0x'+'ad'*20)
        new=insert_target(self.fresh,new)
        self.fresh.require_bootstrap(new,'curve',81419400)
        gap=dict(self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE launch_id=311273').fetchone())
        with patch('app.flow_data.time.time',return_value=1791269000):
            retain_verified_bounds(self.fresh,gap['id'],
                {'number':hex(81419402),'timestamp':hex(1791268020),'hash':'0x'+'14'*32},
                {'number':hex(81419403),'timestamp':hex(1791268021),'hash':'0x'+'15'*32,'parentHash':'0x'+'14'*32})
            debt=next(d for d in obligations(self.fresh) if d['id']==gap['id'])
            self.assertTrue(await recover(worker,debt))
            self.assertIsNone(self.fresh.state('recovery:311273:curve'))
            self.assertEqual(self.fresh.target(311273),new)
            state=self.fresh.conn.execute('SELECT * FROM flow_bootstrap WHERE launch_id=311273').fetchone()
            self.assertEqual(state['status'],'complete');self.assertEqual(state['completed_head'],81419402)
            self.fresh.rebuild(new,1791269000)
            self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE launch_id=311273 AND model_eligible_at IS NOT NULL').fetchone())

    async def test_different_targets_filters_never_coalesce(self):
        worker,t=await self.bounded()
        from tests.test_flow import target,insert_target
        from app.flow_expired_recovery import retain_verified_bounds,inventory
        new=target(launch=311274,start=t['tracking_start_at'],long=False)
        new.update(launch_block=t['launch_block'],status='partial',token_address='0x'+'ae'*20,curve_address='0x'+'af'*20)
        new=insert_target(self.fresh,new)
        with patch('app.flow_data.time.time',return_value=1791269000):
            self.fresh.gap(311274,1791268000,1791268015.026573,'ws_gap',81419247)
            gap=dict(self.fresh.conn.execute('SELECT * FROM flow_gaps WHERE launch_id=311274').fetchone())
            retained=json.loads(self.fresh.state('expired_gap_bounds:4'))
            retain_verified_bounds(self.fresh,gap['id'],retained['upper_header'],retained['next_header'])
            plan=inventory(self.fresh)
            self.assertEqual(plan['target_count'],2)
            self.assertEqual(plan['unique_proof_obligation_count'],2)


for name in ('asyncSetUp','asyncTearDown','incident','failure','quarantine','fake_rpc','rollover','bootstrap_epoch_target','reopened_worker'):
    setattr(ExpiredRecoveryTests,name,getattr(fixtures.ActiveGapTests,name))
