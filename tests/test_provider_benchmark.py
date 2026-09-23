"""Checks for the isolated benchmark's comparisons and secret-safe output."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from provider_benchmark import fingerprint, identity, normalize_error, summarize, filters, verify_window, compare_http, classify_provider


class ProviderBenchmarkTests(unittest.IsolatedAsyncioTestCase):
    def test_fingerprint_does_not_return_key(self):
        key = 'VERY_SECRET_TEST_KEY'
        output = fingerprint('https://example.invalid/v2/' + key)
        self.assertEqual(len(output), 12)
        self.assertNotIn(key, output)

    def test_json_rpc_errors_and_chain_latency(self):
        self.assertEqual(normalize_error({'error': {'code': -32005, 'message': 'SECRET'}}),
                         {'kind': 'rpc_error', 'code': -32005})
        self.assertEqual(summarize([10, 20, 30])['median_ms'], 20)

    def test_log_identity_and_duplicate_set(self):
        a = {'blockHash': '0xAB', 'transactionHash': '0xCD', 'logIndex': '0x2'}
        b = {'blockHash': '0xab', 'transactionHash': '0xcd', 'logIndex': '0x2'}
        self.assertEqual(identity(a), identity(b))
        self.assertEqual(len({identity(a), identity(b)}), 1)

    def test_getlogs_comparison_ignores_order_but_detects_missing(self):
        a = {'methods': {'eth_call': {'ok': True, 'result': '0x12'},
                         'eth_getBlockByNumber': {'ok': True, 'block': {'hash': '0xabc'}}},
             'getlogs': {'curve_10': {'ok': True, 'identities': [('a', 'b', 1), ('c', 'd', 2)]}}}
        b = {'methods': {'eth_call': {'ok': True, 'result': '0x12'},
                         'eth_getBlockByNumber': {'ok': True, 'block': {'hash': '0xabc'}}},
             'getlogs': {'curve_10': {'ok': True, 'identities': [('c', 'd', 2), ('a', 'b', 1)]}}}
        compared = compare_http(a, b)
        self.assertEqual(compared['getlogs']['curve_10']['left_only'], 0)
        self.assertFalse(compared['getlogs']['curve_10']['same_order'])
        self.assertTrue(compared['pinned_state_equal'])
        b['getlogs']['curve_10']['identities'].pop()
        self.assertEqual(compare_http(a, b)['getlogs']['curve_10']['left_only'], 1)

    def test_missing_credentials_and_incomplete_live_data_are_not_passes(self):
        self.assertEqual(classify_provider({}, credential_missing=True), 'NOT_TESTED_CREDENTIAL_REQUIRED')
        self.assertEqual(classify_provider({'wss': {'connected': True, 'chain_id_ok': True,
            'subscriptions': {'curve': {'accepted': True}}, 'reconnect': {'success': True},
            'unexpected_disconnects': 0, 'recovery': {'filters': {'curve': {'complete_query': True,
            'expected_logs': 0, 'missing_live': 0, 'extra_live': 0}}}}}), 'INCONCLUSIVE')

    def test_filter_selection_reads_only_and_uses_real_pool_id(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'flow.db'
            with closing(sqlite3.connect(path)) as c, c:
                c.execute('CREATE TABLE flow_tracking_targets (launch_id integer, curve_address text, graduation_json text, tracking_start_at real)')
                c.execute('CREATE TABLE flow_events (launch_id integer, phase text, tx_hash text)')
                grad = {'pool_manager_address': '0x1234', 'hooks': '0x5678', 'pool_id': '0xabcd'}
                c.executemany('INSERT INTO flow_tracking_targets VALUES(?,?,?,?)',
                              [(1, '0xaaaa', None, 1), (2, '0xbbbb', json.dumps(grad), 2)])
                c.execute("INSERT INTO flow_events VALUES(1,'curve','0x1')")
            before = path.read_bytes()
            c = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
            try: selected = filters(c)
            finally: c.close()
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(selected['curve']['address'], ['0xaaaa'])
            self.assertEqual(selected['v4']['topics'][1], '0xabcd')
            self.assertEqual(selected['hook']['topics'][1], '0xabcd')

    async def test_bounded_recovery_counts_missing_without_secret(self):
        item = {'blockHash': '0xab', 'transactionHash': '0xcd', 'logIndex': '0x0'}
        async def fake_rpc(client, url, method, params):
            self.assertEqual(method, 'eth_getLogs')
            self.assertIn('address', params[0])
            return {'ok': True, 'result': [item]}
        with patch('provider_benchmark.rpc', fake_rpc), patch('provider_benchmark.asyncio.sleep'):
            result = await verify_window('https://example.invalid/SECRET',
                {'curve': {'address': '0x1234', 'topics': [['0xaaaa']]}}, {'events': {}}, 1, 10)
        self.assertEqual(result['filters']['curve']['missing_live'], 1)
        self.assertNotIn('SECRET', json.dumps(result))

    async def test_rate_limit_stops_recovery_without_retry_storm(self):
        calls = []
        async def limited(client, url, method, params):
            calls.append(params)
            return {'ok': False, 'status': 429, 'error': {'kind': 'rpc_error', 'code': -32005}}
        with patch('provider_benchmark.rpc', limited), patch('provider_benchmark.asyncio.sleep'):
            result = await verify_window('https://example.invalid',
                {'curve': {'address': '0x1234', 'topics': [['0xaaaa']]}}, {'events': {}}, 1, 100)
        self.assertEqual(len(calls), 1)
        self.assertFalse(result['filters']['curve']['complete_query'])


if __name__ == '__main__':
    unittest.main()
