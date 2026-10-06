"""Current-epoch debt must survive connection health, retries and expiry."""
import json
import unittest
from unittest.mock import patch,AsyncMock

from tests import test_flow_epochs as epochs_fixture
from tests.test_flow import target, insert_target


class ActiveGapTests(unittest.IsolatedAsyncioTestCase):
    # Reuse the real sealed-epoch fixture, without rediscovering its parent tests.
    async def incident(self):
        runner=await self.rollover();worker=runner.worker
        t=await self.bootstrap_epoch_target(runner)
        with patch('app.flow_epochs.time.time',return_value=1305):
            self.assertTrue(await worker.activation_tail(t))
            from app.flow_epochs import seal_live
            self.assertTrue(seal_live(worker))
        t=target(launch=309266,start=1791267993,long=False)
        t.update(launch_block=81419247,launch_log_index=92,
                 curve_address='0x30a86cF4e791D49196776acE5F4A4127997E6321',
                 token_address='0x1116859Af7384c0412C305c2f3e277640ff1F35b')
        t=insert_target(self.fresh,t)
        self.fake_rpc(81419281,1791268000)
        from app.flow_bootstrap import CursorBootstrap
        with patch('app.flow_data.time.time',return_value=1791268000.4579334):
            await CursorBootstrap(runner).run('live_bootstrap:309266',[309266])
        self.fresh.set_state('recovery:309266:curve',81419346)
        with patch('app.flow_data.time.time',return_value=1791268015.026573):
            self.fresh.gap(309266,1791268000,1791268015.026573,
                           'reconnect_recovery_incomplete',81419247)
        for window in (30,60,300):
            with patch('app.flow_data.time.time',return_value=1791267993+window+4):
                self.fresh.rebuild(t,1791267993+window+4)
        return worker,t

    async def test_production_gap_cannot_report_healthy(self):
        worker,t=await self.incident()
        with patch('app.flow_data.time.time',return_value=1791268298):
            self.fresh.set_state('recovery_state','healthy')
            self.assertNotEqual(self.fresh.state('recovery_state'),'healthy')
        rows=self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=309266').fetchall()
        self.assertEqual({r['window_seconds'] for r in rows},{30,60,300})
        self.assertTrue(all(r['model_eligible_at'] is None for r in rows))

    async def test_health_matrix_and_expiry_remain_current_epoch_debt(self):
        worker,t=await self.incident()
        from app.flow_gap_recovery import obligations,save
        from app import flow_provider_switch as switch
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')

        with patch('app.flow_data.time.time',return_value=1791268298):
            debt=obligations(self.fresh)[0]
            self.assertEqual(debt['state'],'queued')
            save(self.fresh,debt,state='budget_wait',reason='minute_rpc')
            self.assertEqual(self.fresh.current_health(),'temporary_budget_wait')
            with self.fresh.conn:self.fresh.conn.execute('UPDATE flow_gaps SET resolved=1 WHERE id=?',(debt['id'],))
            self.assertEqual(self.fresh.current_health(),'healthy')  # A/F/H, old switch still FAILED.
            with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_bootstrap SET status='required' WHERE launch_id=309266")
            self.assertEqual(self.fresh.current_health(),'bootstrap_required')
            with self.fresh.conn:self.fresh.conn.execute("UPDATE flow_bootstrap SET status='complete' WHERE launch_id=309266")
            epoch=self.fresh.epoch();boundary=epoch['boundary_json']
            with self.fresh.catalog_conn:self.fresh.catalog_conn.execute("UPDATE flow_collection_epochs SET boundary_json='{}' WHERE epoch_id='epoch2'")
            self.assertEqual(self.fresh.current_health(),'bootstrap_required')
            with self.fresh.catalog_conn:self.fresh.catalog_conn.execute("UPDATE flow_collection_epochs SET boundary_json=? WHERE epoch_id='epoch2'",(boundary,))
            tail=self.fresh.state('epoch_tail_proof:1:curve')
            with self.fresh.conn:self.fresh.conn.execute("DELETE FROM flow_state WHERE key='epoch_tail_proof:1:curve'")
            self.assertEqual(self.fresh.current_health(),'bootstrap_required')
            self.fresh.set_state('epoch_tail_proof:1:curve',tail)
            with self.fresh.conn:self.fresh.conn.execute('UPDATE flow_gaps SET resolved=0 WHERE id=?',(debt['id'],))
        with patch('app.flow_data.time.time',return_value=1791269000):
            self.assertEqual(obligations(self.fresh)[0]['state'],'operator_blocked')
            self.assertNotEqual(self.fresh.current_health(),'healthy')
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')

    async def test_suffix_success_cannot_drop_required_gap(self):
        worker,t=await self.incident()
        worker.discover=AsyncMock();worker.subscribe=AsyncMock(return_value=[])
        worker.command=AsyncMock(return_value=True)
        worker.subscriptions={(309266,'curve'):'ACK'}
        worker.pending_recovery.add(309266)
        self.fake_rpc(81419360,1791268320)
        with patch('app.flow_data.time.time',return_value=1791268320):
            await worker.reconcile()
        self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_gaps WHERE launch_id=309266 AND resolved=0').fetchone())

    async def test_scheduler_resume_budget_proof_reuse_and_forward_pit(self):
        worker,t=await self.incident()
        from app.flow_gap_recovery import obligations,recover
        from app.flow_budget import BudgetWait
        before=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=309266')]
        self.fake_rpc(81419360,1791268300)
        with patch('app.flow_data.time.time',return_value=1791268300):
            debt=obligations(self.fresh)[0]
            with patch('app.flow_shadow.ShadowRpc.call',side_effect=BudgetWait('minute_rpc',12,12,1791268320)):
                self.assertFalse(await recover(worker,t,debt))
            self.assertEqual(obligations(self.fresh)[0]['state'],'budget_wait')
        reopened=self.reopened_worker()
        reopened.discover=AsyncMock();reopened.subscribe=AsyncMock(return_value=[])
        reopened.command=AsyncMock(return_value=True)
        reopened.subscriptions={(309266,'curve'):'ACK'}
        # No in-memory pending entry after a process restart. Durable gap requeues it.
        self.assertFalse(reopened.pending_recovery)
        with patch('app.flow_data.time.time',return_value=1791268321):
            await reopened.reconcile()
            self.assertFalse(obligations(self.fresh))
            self.assertEqual(self.fresh.current_health(),'healthy')
        logs=[r for r in self.fresh.conn.execute("SELECT first_block,last_block FROM flow_shadow_ranges WHERE stage LIKE 'live_gap:%'")]
        self.assertTrue(any(r['first_block']==81419247 and r['last_block']==81419281 for r in logs))
        self.assertEqual(self.calls.count('eth_getLogs'),1)
        self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=309266')],before)
        with patch('app.flow_data.time.time',return_value=1791268897):self.fresh.rebuild(t,1791268897)
        self.assertFalse(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE launch_id=309266 AND window_seconds IN (30,60,300) AND model_eligible_at IS NOT NULL').fetchone())
        later=self.fresh.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=309266 AND window_seconds=900').fetchone()
        self.assertEqual(later['model_eligible_at'],1791268897)
        self.assertEqual(later['completeness_proved_at'],1791268897)
        # Resolved work stays resolved and duplicate reconcile needs no repair RPC.
        with patch('app.flow_data.time.time',return_value=1791268897),patch.object(reopened.rpc,'call',side_effect=AssertionError('duplicate RPC')):
            await reopened.reconcile()

    async def test_complete_compatible_proof_resolves_without_rpc(self):
        worker,t=await self.incident()
        from app.flow_bootstrap import CursorBootstrap
        from app.flow_gap_recovery import obligations,recover
        from app.flow_switch_recovery import semantics
        self.fake_rpc(81419346,1791268300)
        with patch('app.flow_data.time.time',return_value=1791268300):
            from app.flow_shadow import ShadowReconciler
            original=worker.rpc;runner=ShadowReconciler(worker,reserve=50)
            try:
                runner.set_meta('existing:semantics:309266:curve',semantics(self.fresh,{'launch_id':309266}))
                await CursorBootstrap(runner).run('existing',[309266])
            finally:
                await worker.rpc.close();worker.rpc=original
            # Current debt cannot be resolved by promoting one bootstrap kind.
            self.assertTrue(obligations(self.fresh))
            with patch('app.flow_shadow.ShadowRpc.call',side_effect=AssertionError('unnecessary RPC')):
                self.assertTrue(await recover(worker,t,obligations(self.fresh)[0]),obligations(self.fresh))
            self.assertFalse(obligations(self.fresh))

    async def test_wrong_query_kind_or_lifecycle_never_reuses_proof(self):
        worker,t=await self.incident()
        from app.flow_gap_recovery import _coverage
        query=worker.filters(t)['curve']
        self.assertEqual(_coverage(self.fresh,t,'curve',query,81419247,81419281,1791268015)[1],0)
        for kind,changed in (('hook',query),('curve',dict(query,address='0x'+'ff'*20)),('curve',dict(query,topics=['wrong']))):
            self.assertEqual(_coverage(self.fresh,t,kind,changed,81419247,81419281,1791268015)[1],35)
        changed=dict(t,graduation_json='{}')
        self.assertEqual(_coverage(self.fresh,changed,'curve',query,81419247,81419281,1791268015)[1],35)

    async def test_oversized_or_unknown_debt_is_explicitly_blocked(self):
        worker,t=await self.incident()
        from app.flow_gap_recovery import obligations,recover
        self.fake_rpc(81419999,1791268320)
        with patch('app.flow_data.time.time',return_value=1791268320):
            self.assertFalse(await recover(worker,t,obligations(self.fresh)[0]))
            debt=obligations(self.fresh)[0]
            self.assertEqual(debt['state'],'operator_blocked')
            self.assertEqual(debt['reason_detail'],'unproved_recovery_range_exceeds_100')
            self.assertFalse('eth_getLogs' in self.calls)
        with patch('app.flow_data.time.time',return_value=1791269000),patch('app.flow_shadow.ShadowRpc.call',side_effect=AssertionError('expired RPC')):
            self.assertFalse(await recover(worker,t,obligations(self.fresh)[0]))

    async def test_budget_after_plan_preserves_head_job_and_restart(self):
        worker,t=await self.incident()
        from app.flow_gap_recovery import obligations,recover
        from app.flow_budget import BudgetWait
        from app.flow_shadow import ShadowRpc
        self.fake_rpc(81419360,1791268320)
        original=ShadowRpc.call
        async def wait_on_logs(rpc,method,params):
            if method=='eth_getLogs':raise BudgetWait('daily_getlogs',350,350,1791268350)
            return await original(rpc,method,params)
        with patch('app.flow_data.time.time',return_value=1791268320),patch.object(ShadowRpc,'call',wait_on_logs):
            self.assertFalse(await recover(worker,t,obligations(self.fresh)[0]))
            self.assertEqual(obligations(self.fresh)[0]['state'],'budget_wait')
        job=dict(self.fresh.conn.execute("SELECT * FROM flow_shadow_jobs WHERE stage='live_gap:4'").fetchone())
        self.assertEqual(job['reconciliation_upper_bound'],81419360)
        reopened=self.reopened_worker();self.calls=[]
        # Frozen head survives a later live head and a new worker instance.
        self.fake_rpc(81419999,1791268351)
        with patch('app.flow_data.time.time',return_value=1791268351):
            self.assertTrue(await recover(reopened,t,obligations(reopened.db)[0]))
        self.assertEqual(self.calls,['eth_getLogs'])
        self.assertEqual(self.fresh.conn.execute("SELECT reconciliation_upper_bound FROM flow_shadow_jobs WHERE stage='live_gap:4'").fetchone()[0],81419360)


# Helpers above reuse EpochTests; inherited test methods are exercised in their
# original module only, avoiding a second copy of the epoch suite.
for _name in ('asyncSetUp','asyncTearDown','failure','quarantine','fake_rpc','rollover','bootstrap_epoch_target','reopened_worker'):
    setattr(ActiveGapTests,_name,getattr(epochs_fixture.EpochTests,_name))
