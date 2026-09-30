"""Prospective feature versions keep the first eligible value and its real clock."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.flow_data import FlowDB, decode_event, iso, required_filter_intervals
from tests.test_flow import target, insert_target, event, fixture


class PitLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=FlowDB(Path(self.tmp.name)/'flow.db')
        self.db.migrate()
        self.revision='a'*40
        self.t=insert_target(self.db,target(start=1000))

    def tearDown(self):
        self.db.conn.close();self.tmp.cleanup()

    def versions(self):
        return [dict(r) for r in self.db.conn.execute('''SELECT * FROM flow_feature_versions
            WHERE launch_id=1 AND window_seconds=60 ORDER BY version_number''')]

    def test_late_recovery_correction_and_immutable_first_eligible(self):
        self.db.activate_pit_ledger(self.revision,123,now=900)
        self.db.require_bootstrap(self.t,'curve',self.t['launch_block'])
        first=event(self.t,at=1010,index=1)
        self.db.store(self.t,first,decode_event(first,self.t),observed=1020)
        with patch('app.flow_data.time.time',return_value=1063.18):
            self.db.rebuild(self.db.target(1),1060)
        v1=self.versions()[0]
        self.assertEqual(v1['coverage_quality'],'partial')
        self.assertEqual(v1['materialized_at'],1063.18)
        self.assertIsNone(v1['model_eligible_at'])
        self.assertEqual(v1['feature_cutoff_at'],1060)
        self.assertEqual(v1['source_revision'],self.revision)
        self.assertEqual(json.loads(v1['payload'])['curve_buy_count'],1)
        self.assertNotIn('economic_actor',v1['payload'])
        self.assertEqual(hashlib.sha256(v1['payload'].encode()).hexdigest(),v1['payload_sha256'])

        second=event(self.t,at=1020,index=2)
        self.db.store(self.t,second,decode_event(second,self.t),observed=1120)
        with patch('app.flow_data.time.time',return_value=1120):
            self.db.complete_bootstrap(1,'curve',125)
        with patch('app.flow_data.time.time',return_value=1121):
            self.db.rebuild(self.db.target(1),1118)
        v2=self.versions()[1]
        self.assertEqual(v2['coverage_quality'],'complete')
        self.assertEqual((v2['materialized_at'],v2['completeness_proved_at'],v2['model_eligible_at']),
                         (1121,1121,1121))
        self.assertEqual(json.loads(v2['payload'])['curve_buy_count'],2)
        self.assertEqual(json.loads(v2['proof_json'])['filters'][0]['completed_head'],125)
        self.assertFalse(v2['model_eligible_at']<1100)  # recovery after an early label excludes the row
        with patch('app.flow_data.time.time',return_value=1122):
            self.db.rebuild(self.db.target(1),1119)
        self.assertEqual(len(self.versions()),2)

        third=event(self.t,at=1030,index=3)
        self.db.store(self.t,third,decode_event(third,self.t),observed=1130)
        with patch('app.flow_data.time.time',return_value=1131):
            self.db.rebuild(self.db.target(1),1128)
        self.assertEqual(len(self.versions()),3)
        self.assertEqual(json.loads(self.versions()[2]['payload'])['curve_buy_count'],3)
        self.assertEqual(self.versions()[1],v2)
        with self.assertRaisesRegex(Exception,'immutable'):
            self.db.conn.execute('UPDATE flow_feature_versions SET payload=? WHERE launch_id=1',('{}',))
        self.db.conn.rollback()
        self.db.conn.close()
        reopened=FlowDB(Path(self.tmp.name)/'flow.db')
        self.db=reopened
        self.db.migrate()
        with patch('app.flow_data.time.time',return_value=1132):
            self.db.rebuild(self.db.target(1),1129)
        self.assertEqual(len(self.versions()),3)

    def test_boundary_no_backfill_unknown_proof_and_reorg_invalidation(self):
        self.db.rebuild(self.t,2000)
        self.db.activate_pit_ledger(self.revision,123,now=1100)
        self.db.activate_pit_ledger('b'*40,456,now=1200)
        self.assertEqual(self.db.conn.execute('SELECT start_at,start_block,deploy_revision FROM flow_feature_ledger_start').fetchone()[:],
                         (1100,123,self.revision))
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_feature_versions').fetchone()[0],0)
        newer=target(launch=2,start=1200)
        newer['token_address']='0x'+'ab'*20
        new=insert_target(self.db,newer)
        with patch('app.flow_data.time.time',return_value=1233):
            self.db.rebuild(new,1230)
        unknown=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=2 AND window_seconds=30').fetchone()
        self.assertEqual(unknown['coverage_quality'],'complete')
        self.assertIsNone(unknown['model_eligible_at'])  # no bootstrap/cursor proof
        with patch('app.flow_data.time.time',return_value=1240):
            self.db.gap(2,1200,1230,'reorg_unresolved')
        changed=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=2 AND window_seconds=30 AND version_number=2').fetchone()
        self.assertEqual(changed['write_reason'],'reorg_unresolved')
        self.assertEqual(changed['coverage_quality'],'partial')
        self.assertEqual(self.db.conn.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    def test_same_transaction_and_proof_transition_without_payload_change(self):
        self.db.activate_pit_ledger(self.revision,now=900)
        self.db.require_bootstrap(self.t,'curve',self.t['launch_block'])
        raw=event(self.t,at=1010,index=1)
        self.db.store(self.t,raw,decode_event(raw,self.t),observed=1020)
        with patch.object(self.db,'_append_feature_version',side_effect=RuntimeError('ledger write failed')):
            with self.assertRaisesRegex(RuntimeError,'ledger write failed'):
                self.db.rebuild(self.db.target(1),1030)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_features').fetchone()[0],0)
        with patch('app.flow_data.time.time',return_value=1033):
            self.db.rebuild(self.db.target(1),1030)
        first=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=30').fetchone()
        with patch('app.flow_data.time.time',return_value=1040):
            self.db.complete_bootstrap(1,'curve',125)
            self.db.rebuild(self.db.target(1),1037)
        later=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=30 AND version_number=2').fetchone()
        self.assertEqual(first['payload_sha256'],later['payload_sha256'])
        self.assertIsNone(first['model_eligible_at'])
        self.assertEqual(later['model_eligible_at'],1040)
        self.assertEqual(later['feature_schema_version'],'v1')

    def test_graduation_after_cutoff_preserves_first_eligible_and_requires_spanning_proof(self):
        self.db.activate_pit_ledger(self.revision,now=900)
        self.db.require_bootstrap(self.t,'curve',self.t['launch_block'])
        self.db.complete_bootstrap(1,'curve',self.t['launch_block']+20)
        raw=event(self.t,at=1010,index=1)
        self.db.store(self.t,raw,decode_event(raw,self.t),observed=1020)
        with patch('app.flow_data.time.time',return_value=1033):
            self.db.rebuild(self.db.target(1),1030)
        first=dict(self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=30').fetchone())
        self.assertEqual(first['model_eligible_at'],1033)
        g=fixture('v4_buy')['launch'];g['block_timestamp']=iso(1045)
        g['block_number']=self.t['launch_block']+20
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=? WHERE launch_id=1',(json.dumps(g),))
        for kind in ('v4','hook'):
            self.db.require_bootstrap(self.db.target(1),kind,g['block_number'])
        self.assertEqual(self.db.conn.execute('SELECT coverage_quality FROM flow_features WHERE window_seconds=30').fetchone()[0],'complete')
        with patch('app.flow_data.time.time',return_value=1063):
            self.db.rebuild(self.db.target(1),1060)
        partial=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=60').fetchone()
        self.assertEqual(partial['coverage_quality'],'partial')
        self.assertIsNone(partial['model_eligible_at'])
        proof=json.loads(partial['proof_json'])
        self.assertEqual(proof['lifecycle_state_at_cutoff'],'graduated')
        self.assertEqual([(f['kind'],f['start_at'],f['end_at']) for f in proof['filters']],
                         [('curve',1000,1045),('v4',1045,1060),('hook',1045,1060)])
        for kind in ('v4','hook'):
            self.db.complete_bootstrap(1,kind,g['block_number']+30)
        with patch('app.flow_data.time.time',return_value=1070):
            self.db.rebuild(self.db.target(1),1067)
        later=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=60 AND version_number=2').fetchone()
        self.assertEqual(later['coverage_quality'],'complete')
        self.assertEqual(later['model_eligible_at'],1070)
        self.assertEqual(dict(self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=30 AND version_number=1').fetchone()),first)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_feature_versions WHERE window_seconds=30').fetchone()[0],1)
        self.assertNotIn('graduation',first['payload'])
        self.assertEqual(json.loads(first['proof_json'])['lifecycle_state_at_cutoff'],'curve')
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_feature_versions WHERE window_seconds=60').fetchone()[0],2)
        self.assertEqual(json.loads(later['proof_json'])['required_filter_set_hash'],
                         json.loads(partial['proof_json'])['required_filter_set_hash'])

    def test_graduation_at_cutoff_requires_pool_proof_and_filter_identity_is_stable(self):
        self.db.activate_pit_ledger(self.revision,now=900)
        self.db.require_bootstrap(self.t,'curve',self.t['launch_block'])
        self.db.complete_bootstrap(1,'curve',self.t['launch_block']+20)
        g=fixture('v4_buy')['launch'];g['block_timestamp']=iso(1030)
        g['block_number']=self.t['launch_block']+20
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=? WHERE launch_id=1',(json.dumps(g),))
        for kind in ('v4','hook'):
            self.db.require_bootstrap(self.db.target(1),kind,g['block_number'])
        self.db.rebuild(self.db.target(1),1033)
        first=self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE window_seconds=30').fetchone()
        self.assertIsNone(first['model_eligible_at'])
        intervals=required_filter_intervals(self.db.target(1),1030)
        self.assertEqual([(i['kind'],i['start_at'],i['end_at']) for i in intervals],
                         [('curve',1000,1030),('v4',1030,1030),('hook',1030,1030)])
        proof=json.loads(first['proof_json'])
        self.assertEqual(proof['filters'][0]['end_before_position'],
                         proof['filters'][1]['start_after_position'])
        self.db.rebuild(self.db.target(1),1034)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_feature_versions WHERE window_seconds=30').fetchone()[0],1)
        self.assertEqual(json.loads(first['proof_json'])['required_filter_set_hash'],
                         json.loads(self.db.conn.execute('SELECT proof_json FROM flow_feature_versions WHERE window_seconds=30').fetchone()[0])['required_filter_set_hash'])
