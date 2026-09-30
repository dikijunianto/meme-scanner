import gc
import hashlib
import io
import json
from contextlib import redirect_stderr
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from scripts import phase2c_point_in_time_audit as pit


def iso(at):
    return datetime.fromtimestamp(at,timezone.utc).isoformat()


class PointInTimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(gc.collect)
        self.main = Path(self.tmp.name)/'main.db'
        self.flow = Path(self.tmp.name)/'flow.db'
        with sqlite3.connect(self.main) as db:
            db.executescript('''CREATE TABLE launches(id INTEGER,token_address TEXT,quote_asset_address TEXT,is_stock_quote INTEGER);
            CREATE TABLE outcome_targets(launch_id INTEGER,target_age_seconds INTEGER,due_at TEXT,sampling_group TEXT);
            CREATE TABLE market_snapshots(launch_id INTEGER,target_age_seconds INTEGER,observed_at TEXT,
              data_quality TEXT,price_quote TEXT,quote_asset_address TEXT,market_phase TEXT,
              quote_reserve TEXT,token_reserve TEXT);
            CREATE TABLE graduations(token_address TEXT,block_timestamp TEXT,block_number INTEGER,log_index INTEGER);''')
            for launch,start,price in ((1,900,'1'),(2,1200,'2')):
                db.execute('INSERT INTO launches VALUES(?,?,?,1)',(launch,f'token{launch}','quote'))
                db.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',
                           (launch,21600,iso(start+21600),'random_long'))
                db.execute('INSERT INTO market_snapshots VALUES(?,?,?,?,?,?,?,?,?)',
                           (launch,0,iso(start+1),'verified','1','quote','curve','10','20'))
                db.execute('INSERT INTO market_snapshots VALUES(?,?,?,?,?,?,?,?,?)',
                           (launch,21600,iso(start+21601),'verified',price,'quote','curve',
                            '10' if launch==1 else '20','20'))
        with sqlite3.connect(self.flow) as db:
            db.executescript('''CREATE TABLE flow_cutover_sessions(id TEXT,payload TEXT,status TEXT);
            CREATE TABLE flow_tracking_targets(launch_id INTEGER,token_address TEXT,quote_asset_address TEXT,
              tracking_start_at REAL,tracking_end_at REAL,cohort_initial INTEGER,cohort_long INTEGER,graduation_json TEXT);
            CREATE TABLE flow_features(launch_id INTEGER,window_seconds INTEGER,feature_cutoff_at REAL,
              finalized_at REAL,coverage_quality TEXT,metrics TEXT);
            CREATE TABLE flow_events(launch_id INTEGER,event_time REAL,observed_at REAL,removed INTEGER,
              event_time_source TEXT,phase TEXT,direction TEXT,recipient_address TEXT,economic_actor TEXT);
            CREATE TABLE flow_bootstrap(launch_id INTEGER,kind TEXT,status TEXT,completed_at REAL);''')
            db.execute('INSERT INTO flow_cutover_sessions VALUES(?,?,?)',
                       (pit.base.SESSION,json.dumps({'source_stopped_at':iso(1000),
                        'validation_started_at':1100,'H_live':1234}),'COMPLETE'))
            metrics = {key:1 for key in pit.base.PREDICTORS}
            for launch,start in ((1,900),(2,1200)):
                db.execute('INSERT INTO flow_tracking_targets VALUES(?,?,?,?,?,1,1,NULL)',
                           (launch,f'token{launch}','quote',start,start+3600))
                db.execute('INSERT INTO flow_features VALUES(?,?,?,?,?,?)',
                           (launch,60,start+60,start+65,'complete',json.dumps(metrics)))
                db.execute('INSERT INTO flow_events VALUES(?,?,?,?,?,?,?,?,?)',
                           (launch,start+60,start+60+2*(launch==2),0,'log_block_timestamp',
                            'curve','buy',f'recipient{launch}',None))
            db.execute('INSERT INTO flow_bootstrap VALUES(?,?,?,?)',(2,'curve','complete',1262))

    def test_availability_boundaries_late_and_unknown(self):
        self.assertTrue(pit.eligible_at(100,100,100))
        self.assertFalse(pit.eligible_at(101,100,100))
        self.assertFalse(pit.eligible_at(100,101,100))
        self.assertFalse(pit.eligible_at(None,100,100))
        at = pit.availability(100,100,0,None,0,100,'LEGACY_FLOW_ERA')
        self.assertEqual(at['usable_at_exact_cutoff'],'UNKNOWN')
        self.assertIsNone(at['feature_available_at'])
        late = pit.availability(100,101,1,102,1,100,'SPLIT_FLOW_ERA')
        self.assertEqual(late['usable_at_exact_cutoff'],'NO')
        self.assertEqual(late['minimum_delay_seconds'],2)
        self.assertIn('event_time_unverified',late['reasons'])
        self.assertEqual(pit.availability(100,None,0,101,0,100,'LEGACY_FLOW_ERA')
                         ['usable_at_exact_cutoff'],'NO')

    def test_read_only_era_separation_labels_and_recipient_semantics(self):
        with pit.base.open_readonly(self.main,self.flow) as db:
            self.assertEqual(db.execute('PRAGMA query_only').fetchone()[0],1)
            with patch('socket.socket',side_effect=AssertionError('network forbidden')):
                result = pit.audit(db,30000)
        pair = next(p for p in result['pairs_by_era'] if p['feature_cutoff_seconds']==60
                    and p['label_horizon_seconds']==21600)
        legacy,split = pair['eras']['LEGACY_FLOW_ERA'],pair['eras']['SPLIT_FLOW_ERA']
        self.assertEqual((legacy['descriptive_n'],split['descriptive_n']),(1,1))
        self.assertEqual((legacy['final_model_usable_n'],split['final_model_usable_n']),(0,0))
        self.assertEqual(legacy['label']['fraction_exactly_one'],1)
        self.assertEqual(legacy['label']['exact_one_curve_same_reserves'],1)
        self.assertEqual(split['point_in_time_safe_features_among_mature'],0)
        split_window = next(w for w in result['window_availability'] if
                            w['era']=='SPLIT_FLOW_ERA' and w['window_seconds']==60)
        self.assertEqual(split_window['with_late_retained_events'],1)
        self.assertEqual(split_window['exact_cutoff_NO'],1)
        self.assertFalse(result['pooled_inference_permitted'])
        self.assertEqual(result['recipient_semantics']['claimed_economic_actors'],0)
        self.assertNotIn('recipient1',json.dumps(result))
        self.assertEqual(result['model_readiness']['status'],'MODEL_DATA_NOT_MATURE')
        self.assertEqual(result['candidate_classification']['unique_buy_recipients'],
                         'RECONSTRUCTABLE_ONLY')
        del db
        gc.collect()

    def test_horizon_guard_partial_and_deterministic_export(self):
        with pit.base.open_readonly(self.main,self.flow) as db:
            with self.assertRaises(ValueError):
                pit.descriptive_row({},300,300,30000,{})
        with sqlite3.connect(self.flow) as db:
            db.execute("UPDATE flow_features SET coverage_quality='partial' WHERE launch_id=1")
        paths = [Path(self.tmp.name)/name for name in ('one.json','two.json')]
        for path in paths:
            argv = ['phase2c_point_in_time_audit','--main-db',str(self.main),
                    '--flow-db',str(self.flow),'--as-of',iso(30000),'--output',str(path)]
            with patch('sys.argv',argv),redirect_stderr(io.StringIO()):
                pit.main()
        self.assertEqual(paths[0].read_bytes(),paths[1].read_bytes())
        result = json.loads(paths[0].read_text())
        pair = next(p for p in result['pairs_by_era'] if p['feature_cutoff_seconds']==60
                    and p['label_horizon_seconds']==21600)
        self.assertEqual(pair['eras']['LEGACY_FLOW_ERA']['descriptive_n'],0)
        self.assertNotIn('recipient1',paths[0].read_text())
        source = Path(pit.__file__).read_text()
        self.assertNotIn('eth_getTransactionByHash',source)
        self.assertNotIn('eth_getTransactionReceipt',source)
        gc.collect()

    def test_fixed_chronological_threshold(self):
        rows = [{'tracking_start_at':i*86400/10,
                 'multiple':0.9 if i%2 else 1.1,
                 'label_relation':-1 if i%2 else 1,
                 'pit':{'usable_at_exact_cutoff':'YES'}} for i in range(600)]
        self.assertEqual(pit.chronological_readiness(rows)['status'],
                         'MODEL_DATA_NOT_MATURE')  # 59.9 days is below the fixed span.
        rows[-1]['tracking_start_at'] = 61*86400
        ready = pit.chronological_readiness(rows)
        self.assertEqual(ready['status'],'MODEL_DATA_MINIMUM_READY')
        self.assertEqual(ready['chronological_slices']['holdout']['n'],120)
        rows[0]['pit']['usable_at_exact_cutoff']='UNKNOWN'
        self.assertEqual(pit.chronological_readiness(rows)['status'],'MODEL_DATA_NOT_MATURE')
        self.assertEqual(pit.label_distribution([{'multiple':1.0,'label_relation':-1,
                                                   'reserve_state':'same_curve_reserves'}])
                         ['fraction_exactly_one'],0)

    def test_ledger_first_eligible_strictly_precedes_label(self):
        payload=json.dumps({'curve_buy_count':1},sort_keys=True,separators=(',',':'))
        with sqlite3.connect(self.flow) as db:
            db.executescript('''CREATE TABLE flow_feature_ledger_start(id INTEGER,start_at REAL,start_block INTEGER,deploy_revision TEXT);
                CREATE TABLE flow_feature_versions(launch_id INTEGER,window_seconds INTEGER,version_number INTEGER,
                  feature_schema_version TEXT,feature_cutoff_at REAL,materialized_at REAL,coverage_quality TEXT,
                  model_eligible_at REAL,payload TEXT,payload_sha256 TEXT,write_reason TEXT,
                  coverage_reason TEXT,proof_json TEXT);''')
            db.execute('INSERT INTO flow_feature_ledger_start VALUES(1,1100,1234,?)',('a'*40,))
            proof=json.dumps({'filters':[{'kind':kind,'bootstrap_status':'complete','cursor':1234}
                                         for kind in ('curve','v4','hook')]})
            for version,materialized,quality,eligible,reason in (
                    (1,1264,'partial',None,'bootstrap_required'),
                    (2,1265,'complete',1265,'complete'),
                    (3,1266,'complete',1266,'complete')):
                db.execute('INSERT INTO flow_feature_versions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (2,60,version,'v1',1260,materialized,quality,eligible,payload,
                            hashlib.sha256(payload.encode()).hexdigest(),'rebuild',reason,proof))
        with pit.base.open_readonly(self.main,self.flow) as db:
            result=pit.ledger_audit(db,30000)
        self.assertEqual(result['first_eligible_versions'],1)
        self.assertEqual(result['usable_by_pair']['60s_to_21600s'],1)
        self.assertEqual(result['launches'],1)
        with sqlite3.connect(self.flow) as db:
            db.execute('UPDATE flow_tracking_targets SET graduation_json=? WHERE launch_id=2',
                       (json.dumps({'block_timestamp':iso(1250)}),))
            full=db.execute('SELECT proof_json FROM flow_feature_versions WHERE launch_id=2 AND version_number=2').fetchone()[0]
            curve_only=json.dumps({'filters':[{'kind':'curve','bootstrap_status':'complete','cursor':1234}]})
            db.execute('UPDATE flow_feature_versions SET proof_json=? WHERE launch_id=2 AND version_number=2',(curve_only,))
        with pit.base.open_readonly(self.main,self.flow) as db:
            self.assertEqual(pit.ledger_audit(db,30000)['first_eligible_versions'],1)
        with sqlite3.connect(self.flow) as db:
            db.execute('UPDATE flow_feature_versions SET proof_json=? WHERE launch_id=2 AND version_number=3',(curve_only,))
        with pit.base.open_readonly(self.main,self.flow) as db:
            self.assertEqual(pit.ledger_audit(db,30000)['first_eligible_versions'],0)
        with sqlite3.connect(self.flow) as db:
            db.execute('UPDATE flow_feature_versions SET proof_json=? WHERE launch_id=2 AND version_number IN (2,3)',(full,))
        with pit.base.open_readonly(self.main,self.flow) as db:
            bias=pit.ledger_audit(db,30000)['graduation_selection_bias']
        self.assertEqual((bias['total_launches'],bias['graduated_during_tracking'],bias['spanning_windows']),
                         (1,1,4))
        self.assertEqual((bias['complete_spanning_windows'],bias['unavailable_spanning_windows'],
                          bias['pit_eligible_spanning_windows']),(1,3,1))
        self.assertEqual(bias['eligibility_rates']['graduated_during_window'],
                         {'due_windows':4,'pit_eligible_windows':1,'rate':.25})
        with sqlite3.connect(self.flow) as db:
            db.execute('UPDATE flow_feature_versions SET model_eligible_at=? WHERE launch_id=2 AND version_number=2',
                       (22801,))
            db.execute('UPDATE flow_feature_versions SET model_eligible_at=? WHERE launch_id=2 AND version_number=3',
                       (22802,))
        with pit.base.open_readonly(self.main,self.flow) as db:
            result=pit.ledger_audit(db,30000)
        self.assertEqual(result['usable_by_pair']['60s_to_21600s'],0)
        with sqlite3.connect(self.flow) as db:
            db.execute("UPDATE flow_feature_versions SET feature_schema_version='v2' WHERE launch_id=2")
        with pit.base.open_readonly(self.main,self.flow) as db:
            result=pit.ledger_audit(db,30000)
        self.assertEqual(result['first_eligible_versions'],0)


if __name__=='__main__':
    unittest.main()
