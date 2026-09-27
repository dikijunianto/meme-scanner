"""A fallback is accepted only after its durable coverage proof is complete."""
from pathlib import Path
import json
import tempfile
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
        switch.acknowledged(self.db,identity,121)
        self.db.set_state('current_wss_provider','validation')
        self.assertFalse(switch.acceptable_route(self.db,'s'))
        switch.healthy(self.db,identity,{'calls':0,'recovered_events':0,'duplicates':0,
                       'unresolved_ranges':[],'zero_active_filters':True},[])
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
            switch.acknowledged(self.db,identity,102+offset*60)
            switch.healthy(self.db,identity,{'calls':0,'recovered_events':0,
                           'duplicates':0,'unresolved_ranges':[],'zero_active_filters':True},[])
            provider=other
        self.db.set_state('current_wss_provider',provider)
        report=switch.report(self.db,session_id='s')
        self.assertTrue(report['provider_flapping'])
        self.assertEqual(report['failback_count'],2)
        self.assertFalse(switch.acceptable_route(self.db,'s'))


if __name__=='__main__':unittest.main()
