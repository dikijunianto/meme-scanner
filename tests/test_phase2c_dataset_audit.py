import json
import gc
import io
from contextlib import redirect_stderr
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from scripts import phase2c_dataset_audit as audit


def iso(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


class DatasetAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(gc.collect)
        self.main = Path(self.tmp.name) / 'main.db'
        self.flow = Path(self.tmp.name) / 'flow.db'
        with sqlite3.connect(self.main) as db:
            db.executescript('''CREATE TABLE launches(id INTEGER PRIMARY KEY,token_address TEXT,
                quote_asset_address TEXT,block_timestamp TEXT,is_stock_quote INTEGER);
                CREATE TABLE outcome_targets(launch_id INTEGER,target_age_seconds INTEGER,
                due_at TEXT,sampling_group TEXT);
                CREATE TABLE market_snapshots(launch_id INTEGER,target_age_seconds INTEGER,
                observed_at TEXT,data_quality TEXT,price_quote TEXT,fdv_quote TEXT,
                quote_asset_address TEXT,market_phase TEXT);
                CREATE TABLE graduations(token_address TEXT,block_timestamp TEXT,
                block_number INTEGER,log_index INTEGER);''')
            for launch in range(1, 6):
                start = 900 if launch == 1 else 1200 + launch * 100
                db.execute('INSERT INTO launches VALUES(?,?,?,?,1)',
                           (launch, f'token{launch}', 'quote', iso(start)))
                for h in (0, 900):
                    db.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',
                               (launch, h, iso(start + h), 'random_long'))
                    db.execute('INSERT INTO market_snapshots VALUES(?,?,?,?,?,?,?,?)',
                               (launch, h, iso(start + h + 2), 'verified',
                                '1' if h == 0 else '2', '10' if h == 0 else '20',
                                'quote', 'curve' if launch != 5 else 'v4'))
            db.execute('INSERT INTO graduations VALUES(?,?,?,?)', ('token5', iso(1600), 1, 0))
        with sqlite3.connect(self.flow) as db:
            db.executescript('''CREATE TABLE flow_tracking_targets(launch_id INTEGER PRIMARY KEY,
                token_address TEXT,quote_asset_address TEXT,tracking_start_at REAL,
                cohort_initial INTEGER,cohort_long INTEGER,graduation_json TEXT);
                CREATE TABLE flow_features(launch_id INTEGER,window_seconds INTEGER,
                feature_cutoff_at REAL,finalized_at REAL,coverage_quality TEXT,
                coverage_reason TEXT,metrics TEXT);
                CREATE TABLE flow_events(launch_id INTEGER,removed INTEGER,event_time REAL);
                CREATE TABLE flow_cutover_sessions(id TEXT,payload TEXT,status TEXT);''')
            session = {'source_stopped_at': iso(1000), 'validation_started_at': 1100,
                       'H_live': 1234}
            db.execute('INSERT INTO flow_cutover_sessions VALUES(?,?,?)',
                       (audit.SESSION, json.dumps(session), 'COMPLETE'))
            for launch in range(1, 6):
                start = 900 if launch == 1 else 1200 + launch * 100
                quality = {1: 'complete', 2: 'partial', 3: 'unavailable',
                           4: 'complete', 5: 'complete'}[launch]
                metrics = {'curve_buy_count': launch, 'curve_sell_count': 0,
                           'total_directional_event_count': launch,
                           'v4_core_swap_count': 1 if launch == 5 else 0,
                           'unique_buy_recipients': launch,
                           'top1_buy_recipient_token_share': '0.5',
                           'future_secret': 'top-secret'}
                db.execute('INSERT INTO flow_tracking_targets VALUES(?,?,?,?,1,1,NULL)',
                           (launch, f'token{launch}', 'quote', start))
                db.execute('INSERT INTO flow_features VALUES(?,?,?,?,?,?,?)',
                           (launch, 60, start + 60,
                            start + (1000 if launch == 4 else 65), quality,
                            quality, json.dumps(metrics)))
                db.execute('INSERT INTO flow_events VALUES(?,?,?)', (launch, 0, start + 10))

    def test_read_only_grain_coverage_and_leakage(self):
        with audit.open_readonly(self.main, self.flow) as db:
            cursor = db.execute('PRAGMA query_only')
            self.assertEqual(cursor.fetchone()[0], 1)
            cursor.close()
            with patch('socket.socket', side_effect=AssertionError('network forbidden')):
                result = audit.audit(db, 5000)
                pair = audit.audit_pair(db, 60, 900, 5000, 1000, 1100)
        self.assertEqual(pair['mature_denominator'], 5)
        self.assertEqual(pair['usable_n'], 2)
        self.assertEqual(pair['exclusive_exclusions']['flow_partial'], 1)
        self.assertEqual(pair['exclusive_exclusions']['flow_unavailable'], 1)
        self.assertEqual(pair['exclusive_exclusions']['feature_finalized_after_label'], 1)
        self.assertEqual(pair['usable_by_era'], {'LEGACY_FLOW_ERA': 1, 'SPLIT_FLOW_ERA': 1})
        self.assertEqual(pair['usable_by_state_at_cutoff'], {'curve_at_cutoff': 1, 'v4_at_cutoff': 1})
        self.assertNotIn('future_secret', json.dumps(result))
        self.assertNotIn('current_phase', audit.PREDICTORS)
        self.assertNotIn('graduation_json', audit.PREDICTORS)
        self.assertNotIn('target_status', audit.PREDICTORS)
        del db
        gc.collect()

    def test_alignment_eras_and_deterministic_summary(self):
        self.assertIn('spearman', audit.screen([(float(i), float(i)) for i in range(1, 31)], 1))
        self.assertIn('spearman', audit.screen([(0.0 if i < 5 else float(i), float(i))
                                                for i in range(1, 31)], 1))
        self.assertEqual(audit.screen([(0.0, float(i)) for i in range(30)], 1)['note'],
                         'constant_feature_no_correlation')
        for window, horizon in ((300, 300), (900, 300), (60, 123)):
            with audit.open_readonly(self.main, self.flow) as db:
                with self.assertRaises(ValueError):
                    audit.audit_pair(db, window, horizon, 5000, 1000, 1100)
        self.assertEqual(audit.era(900, 960, 1000, 1100), 'LEGACY_FLOW_ERA')
        self.assertEqual(audit.era(900, 1050, 1000, 1100), 'TRANSITION_ERA')
        self.assertEqual(audit.era(1100, 1160, 1000, 1100), 'SPLIT_FLOW_ERA')
        with audit.open_readonly(self.main, self.flow) as db:
            first = json.dumps(audit.audit(db, 5000), sort_keys=True)
        with audit.open_readonly(self.main, self.flow) as db:
            second = json.dumps(audit.audit(db, 5000), sort_keys=True)
        self.assertEqual(first, second)
        self.assertNotIn('top-secret', first)
        source = Path(audit.__file__).read_text()
        self.assertNotIn('eth_getTransactionByHash', source)
        self.assertNotIn('eth_getTransactionReceipt', source)

    def test_deterministic_export_and_input_protection(self):
        outputs = [Path(self.tmp.name) / 'first.json', Path(self.tmp.name) / 'second.json']
        for output in outputs:
            argv = ['phase2c_dataset_audit.py', '--main-db', str(self.main),
                    '--flow-db', str(self.flow), '--as-of', iso(5000), '--output', str(output)]
            with patch('sys.argv', argv), redirect_stderr(io.StringIO()):
                audit.main()
        self.assertEqual(outputs[0].read_bytes(), outputs[1].read_bytes())
        self.assertNotIn(b'top-secret', outputs[0].read_bytes())
        argv[-1] = str(self.flow)
        with patch('sys.argv', argv), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                audit.main()
        gc.collect()


if __name__ == '__main__':
    unittest.main()
