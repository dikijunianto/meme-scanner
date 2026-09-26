"""The passive gate triggers only for canonical sampled Phase 2B work."""
import sqlite3
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from recovery_fix_live_gate import new_targets  # noqa: E402


class LiveGateTests(unittest.TestCase):
    def test_new_target_requires_persisted_initial_cohort_flag(self):
        flow, main = sqlite3.connect(':memory:'), sqlite3.connect(':memory:')
        flow.row_factory = main.row_factory = sqlite3.Row
        flow.execute('CREATE TABLE flow_tracking_targets(launch_id INTEGER,token_address TEXT,'
                     'curve_address TEXT,cohort_initial INTEGER,cohort_long INTEGER,'
                     'created_at REAL,current_phase TEXT,launch_block INTEGER,status TEXT)')
        main.execute('CREATE TABLE outcome_targets(launch_id INTEGER,sampling_group TEXT)')
        for launch_id in (1, 2, 3):
            flow.execute('INSERT INTO flow_tracking_targets VALUES(?,?,?,?,?,?,?,?,?)',
                         (launch_id, 'token', 'curve', 1, launch_id == 3, 100, 'curve', 10, 'active_curve'))
        main.executemany('INSERT INTO outcome_targets VALUES(?,?)',
                         [(2, 'random_initial'), (3, 'random_initial')])
        self.assertEqual([t['launch_id'] for t in new_targets(flow, main, 99)], [2])
        self.assertEqual(new_targets(flow, main, 101), [])


if __name__ == '__main__':
    unittest.main()
