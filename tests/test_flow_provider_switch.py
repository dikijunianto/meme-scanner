"""A fallback is accepted only after its durable coverage proof is complete."""
from pathlib import Path
import json
import tempfile
import time
import unittest

from app.flow_data import FlowDB
from app import flow_provider_switch as switch
from app.flow_cutover import ACCEPTANCE, accept_validation


class ProviderSwitchTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=FlowDB(Path(self.tmp.name)/'flow.db')
        self.db.migrate()
        self.db.set_state('connection_state','connected')
        self.db.set_state('service_status','connected')
        self.db.set_state('current_wss_provider','publicnode')

    def tearDown(self):
        self.db.conn.close();self.tmp.cleanup()

    def test_primary_disconnect_validation_ack_and_zero_filter_proof(self):
        old=switch.connection_open(self.db,'publicnode',100,session_id='s')
        switch.connection_close(self.db,old,110)
        identity,_=switch.start(self.db,'publicnode',[],[],session_id='s',ready_at=101,
                                last_block=12,now=110)
        self.assertFalse(switch.acceptable_route(self.db,'s'))
        switch.failover_pending(self.db,identity,'RpcError')
        new=switch.connection_open(self.db,'validation',120,session_id='s')
        switch.connected(self.db,identity,'validation',new,120)
        self.db.set_state('current_wss_provider','validation')
        self.assertEqual(switch.latest(self.db,'s')['state'],'HEALTHY')
        self.assertIsNone(switch.value(switch.latest(self.db,'s'))['subscriptions_ready_at'])
        switch.connection_seen(self.db,new,124)
        self.assertTrue(switch.acceptable_route(self.db,'s'))
        report=switch.report(self.db,now=125,session_id='s')
        self.assertEqual(report['primary_disconnect_count'],1)
        self.assertEqual(report['fallback_activation_count'],1)
        self.assertEqual(report['time_on_primary'],10)
        self.assertEqual(report['time_on_fallback'],4)
        self.assertEqual(report['provider_switch_unresolved_ranges'],0)
        self.assertEqual(self.db.used('flow_http_calls_alchemy',0),0)
        session={'id':'s','state':'POST_CUTOVER_VALIDATING','validation_started_at':100,
                 'source_legacy_pid':'10','source_route':'alchemy','revision':'test',
                 'created_at_utc':'2026-09-27T00:00:00Z','main_pid':'20',
                 'source_legacy_start_time':None,'split_role_fingerprint':'roles','history':[]}
        with self.db.conn:self.db.conn.execute('''INSERT INTO flow_cutover_sessions
            (id,created_at_utc,deploy_git_revision,source_route,source_legacy_pid,status,payload)
            VALUES(?,?,?,?,?,?,?)''',('s',session['created_at_utc'],'test','alchemy','10',
                                      'POST_CUTOVER_VALIDATING',json.dumps(session)))
        accepted=accept_validation(self.db,session,{key:True for key in ACCEPTANCE},now=1900)
        self.assertEqual(accepted['state'],'SOAKING')
        self.assertEqual(self.db.state('current_wss_provider'),'validation')

    def test_unproved_range_failure_and_flapping_block_acceptance(self):
        filters=[{'launch_id':1,'kind':'curve','base':10,'cursor':12}]
        identity,_=switch.start(self.db,'publicnode',filters,[7],session_id='s',now=100)
        switch.failover_pending(self.db,identity,'RpcError')
        switch.connected(self.db,identity,'validation',1,105)
        switch.acknowledged(self.db,identity,106)
        self.db.set_state('current_wss_provider','validation')
        self.assertEqual(switch.report(self.db,session_id='s')['provider_switch_unresolved_ranges'],1)
        self.assertFalse(switch.acceptable_route(self.db,'s'))
        switch.failed(self.db,identity,'unreconciled')
        self.assertFalse(switch.acceptable_route(self.db,'s'))
        self.assertTrue(switch.report(self.db,session_id='s')['provider_switch_failed'])

    def test_disconnected_or_forbidden_route_cannot_pass(self):
        self.db.set_state('connection_state','disconnected')
        self.assertFalse(switch.acceptable_route(self.db,'s'))
        self.db.set_state('connection_state','connected')
        self.db.set_state('current_wss_provider','alchemy')
        self.assertFalse(switch.acceptable_route(self.db,'s'))

    def test_four_rapid_switches_block_acceptance_without_auto_failback(self):
        provider='publicnode'
        for offset in range(4):
            identity,_=switch.start(self.db,provider,[],[],session_id='s',now=100+offset*60)
            other='validation' if provider=='publicnode' else 'publicnode'
            if other=='validation':switch.failover_pending(self.db,identity,'RpcError')
            switch.connected(self.db,identity,other,offset+1,101+offset*60)
            provider=other
        self.db.set_state('current_wss_provider',provider)
        report=switch.report(self.db,session_id='s')
        self.assertTrue(report['provider_flapping'])
        self.assertEqual(report['failback_count'],2)
        self.assertFalse(switch.acceptable_route(self.db,'s'))


    def zero_failure(self):
        identity,_=switch.start(self.db,'validation',[],[],session_id='s',now=100)
        old=switch.connection_open(self.db,'validation',101,session_id='s')
        row=switch.latest(self.db,'s');item=switch.value(row)
        item.update(new_provider='validation',new_connected_at=101,new_connection_id=old,
                    failure='connection_lost_before_switch_proof',failed_at=102)
        with self.db.conn:self.db.conn.execute("UPDATE flow_provider_switches SET state='FAILED',payload=? WHERE id=?",
                                               (json.dumps(item),identity))
        switch.connection_close(self.db,old,102)
        new=switch.connection_open(self.db,'validation',session_id='s')
        self.db.set_state('current_wss_provider','validation')
        self.db.set_state('heartbeat',time.time())
        self.db.set_state('recovery_state','provider_switch_failed')
        return identity,item,new

    def test_same_provider_zero_reconnect_needs_no_ack_head_or_rpc(self):
        identity,_=switch.start(self.db,'validation',[],[],session_id='s')
        connection=switch.connection_open(self.db,'validation',session_id='s')
        item=switch.connected(self.db,identity,'validation',connection)
        self.assertEqual(switch.latest(self.db,'s')['state'],'HEALTHY')
        self.assertTrue(item['no_subscriptions_required'])
        self.assertFalse(item['recovery_required'])
        for key in ('subscriptions_ready_at','frozen_head','uncertain_from','uncertain_to'):
            self.assertIsNone(item[key])
        self.assertEqual(item['recovery']['calls'],0)
        self.assertEqual(switch.report(self.db,session_id='s')['fallback_activation_count'],0)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_usage').fetchone()[0],0)

    def test_zero_failure_preserves_original_and_never_sets_runtime(self):
        identity,original,new=self.zero_failure()
        item=switch.reconcile_zero_filter_failure(self.db,identity,'a'*40)
        self.assertEqual(item['failure_history']['payload'],original)
        self.assertEqual(item['failure'],original['failure'])
        self.assertEqual(item['failed_at'],102)
        self.assertEqual(item['new_connection_id'],original['new_connection_id'])
        self.assertEqual(item['recovery_after_failure']['current_connection_id'],new)
        self.assertEqual(switch.latest(self.db,'s')['state'],'HEALTHY')
        self.assertEqual(self.db.state('recovery_state'),'provider_switch_failed')
        self.assertIsNone(item['subscriptions_ready_at'])
        with self.assertRaises(ValueError):switch.reconcile_zero_filter_failure(self.db,identity,'a'*40)

    def test_zero_failure_rejects_gaps_ranges_or_stale_transport_atomically(self):
        identity,original,new=self.zero_failure()
        for key,bad in (('gap_ids',[1]),('filters',[{'kind':'curve'}]),
                        ('frozen_head',100),('subscriptions_ready_at',100)):
            item=dict(original);item[key]=bad
            with self.db.conn:self.db.conn.execute('UPDATE flow_provider_switches SET payload=? WHERE id=?',
                                                   (json.dumps(item),identity))
            before=switch.latest(self.db,'s')
            with self.assertRaises(ValueError):switch.reconcile_zero_filter_failure(self.db,identity,'a'*40)
            self.assertEqual(switch.latest(self.db,'s'),before)
        with self.db.conn:self.db.conn.execute('UPDATE flow_provider_switches SET payload=? WHERE id=?',
                                               (json.dumps(original),identity))
        switch.connection_close(self.db,new)
        with self.assertRaises(ValueError):switch.reconcile_zero_filter_failure(self.db,identity,'a'*40)

    def test_stale_old_connection_loss_does_not_fail_new_generation(self):
        identity,_=switch.start(self.db,'validation',[{'kind':'curve'}],[],session_id='s')
        old=switch.connection_open(self.db,'validation',session_id='s')
        new=switch.connection_open(self.db,'validation',session_id='s')
        switch.connected(self.db,identity,'validation',new)
        before=switch.latest(self.db,'s')
        switch.connection_close(self.db,old)
        self.assertIsNone(switch.failed(self.db,identity,'stale',connection_id=old,provider='validation'))
        self.assertIsNone(switch.failed(self.db,identity,'wrong_provider',connection_id=new,provider='publicnode'))
        self.assertEqual(switch.latest(self.db,'s'),before)
        switch.failed(self.db,identity,'lost',connection_id=new,provider='validation')
        self.assertEqual(switch.latest(self.db,'s')['state'],'FAILED')

    def test_same_provider_nonzero_still_waits_for_ack_and_head(self):
        identity,_=switch.start(self.db,'validation',[{'kind':'curve'}],[],session_id='s')
        switch.connected(self.db,identity,'validation',1)
        self.assertEqual(switch.latest(self.db,'s')['state'],'VALIDATION_CONNECTED')
        with self.assertRaises(ValueError):switch.frozen(self.db,identity,100,10)
        switch.acknowledged(self.db,identity)
        switch.frozen(self.db,identity,100,10)
        self.assertEqual(switch.latest(self.db,'s')['state'],'PROVIDER_SWITCH_RECOVERY')
        with self.assertRaises(ValueError):switch.healthy(self.db,identity,{'unresolved_ranges':[1]},[])


if __name__=='__main__':unittest.main()
