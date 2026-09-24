"""Round 3 gates are deterministic and never call a live provider."""
import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import provider_benchmark_round3 as r3


def log(topic=r3.BUY, block='0x10', tx='0xAB', index='0x1', address='0xabc', second=None):
    return {'address': address, 'topics': [topic] + ([second] if second else []),
            'blockHash': block, 'blockNumber': '0x10', 'transactionHash': tx, 'logIndex': index}


class Round3Tests(unittest.IsolatedAsyncioTestCase):
    def test_shared_filter_freezes_real_active_curve_and_pool(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'flow.db'
            with closing(sqlite3.connect(path)) as db:
                db.execute('CREATE TABLE flow_tracking_targets (curve_address text, graduation_json text, status text, tracking_start_at real)')
                db.executemany('INSERT INTO flow_tracking_targets VALUES(?,?,?,?)', [
                    ('0xold', None, 'completed', 1), ('0xactive', None, 'tracking', 2),
                    ('0xgrad', json.dumps({'pool_manager_address': '0xpool', 'hooks': '0xhook',
                                            'pool_id': '0xid'}), 'tracking', 3)])
                db.commit()
            before = path.read_bytes()
            with closing(r3.db_ro(path)) as db:
                selected = r3.select_filters(db)
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(selected['curve']['address'], ['0xactive'])
            self.assertEqual(selected['v4']['topics'], [r3.SWAP, '0xid'])
            self.assertEqual(selected['hook']['topics'], [r3.HOOK, '0xid'])

    def test_canonical_identity_duplicate_removed_wrong_and_malformed(self):
        selected = {'curve': {'address': ['0xabc'], 'topics': [[r3.BUY, r3.SELL]]}}
        record = r3.new_record(); signal = asyncio.Event(); routes = {'sub': 'curve'}
        def send(item, subscription='sub'):
            r3.record_notification(record, {'params': {'subscription': subscription, 'result': item}},
                                   routes, selected, signal)
        send(log()); send(log(block='0x10', tx='0xab', index='0x01'))
        self.assertEqual(record['duplicates'], 1)
        self.assertEqual(len(record['events']['curve_buy']), 1)
        self.assertTrue(signal.is_set())
        send(log(address='0xwrong')); send(log(), 'bad')
        self.assertEqual(record['wrong_filter'], 2)
        send({'topics': [r3.BUY], 'address': '0xabc'})
        self.assertEqual(record['malformed'], 1)
        removed = log(); removed['removed'] = True; send(removed)
        self.assertEqual(record['removed'], 1)
        self.assertFalse(record['events']['curve_buy'])

    def test_same_window_ground_truth_missing_extra_and_zero_class(self):
        a = r3.event_identity(log(tx='0xA')); b = r3.event_identity(log(tx='0xB'))
        c = r3.event_identity(log(tx='0xC'))
        empty = lambda: {k: set() for k in r3.CLASSES}
        public, validation, truth = empty(), empty(), empty()
        public['curve_buy'] = {a, c}; validation['curve_buy'] = {a, b}; truth['curve_buy'] = {a, b}
        comparison, recovery = r3.compare_sets(public, validation, truth, True)
        self.assertEqual(comparison['curve_buy']['intersection'], 1)
        self.assertEqual(comparison['curve_buy']['publicnode_only'], 1)
        self.assertEqual(comparison['curve_buy']['validation_only'], 1)
        self.assertEqual(recovery['publicnode']['curve_buy']['missing'], 1)
        self.assertEqual(recovery['publicnode']['curve_buy']['extra'], 1)
        self.assertEqual(recovery['validation']['curve_buy']['missing'], 0)
        self.assertIsNone(recovery['validation']['curve_sell']['completeness'])
        self.assertEqual(recovery['validation']['curve_sell']['missing'], 0)

    def test_classification_curve_unproven_full_and_wss_only(self):
        record = r3.new_record(); record.update(connected=True, chain_id_ok=True, subscription_ids_count=3)
        clean = {k: {'expected': 0, 'received': 0, 'missing': 0, 'extra': 0} for k in r3.CLASSES}
        self.assertEqual(r3.classify('validation', record, clean, True, False),
                         'VALIDATION_WSS_PASS_CURVE_UNPROVEN')
        self.assertEqual(r3.classify('publicnode', record, clean, True, False),
                         'PUBLICNODE_WSS_PASS_CURVE_UNPROVEN')
        clean['curve_buy']['expected'] = 1
        self.assertEqual(r3.classify('validation', record, clean, True, True),
                         'VALIDATION_FULL_SECONDARY_PASS')
        self.assertEqual(r3.classify('publicnode', record, clean, True, True),
                         'PUBLICNODE_WSS_SECONDARY_PASS')
        clean['curve_buy']['missing'] = 1
        self.assertEqual(r3.classify('publicnode', record, clean, True, True), 'PUBLICNODE_FAIL')
        self.assertEqual(r3.classify('validation', record, clean, False, True), 'VALIDATION_INCONCLUSIVE')

    async def test_both_collectors_wait_for_shared_start(self):
        ready = {n: asyncio.Event() for n in ('validation', 'publicnode')}
        start, stop, curve = asyncio.Event(), asyncio.Event(), asyncio.Event()
        marks = {}
        async def fake(name, url, selected, ready_event, start_event, stop_event, curve_event, max_seconds):
            ready_event.set(); await start_event.wait(); marks[name] = asyncio.get_running_loop().time()
            await stop_event.wait(); return r3.new_record()
        with patch.object(r3, 'collect', fake):
            tasks = [asyncio.create_task(r3.collect(n, 'url', {}, ready[n], start, stop, curve, 60)) for n in ready]
            await asyncio.gather(*(x.wait() for x in ready.values()))
            self.assertEqual(marks, {})
            start.set(); await asyncio.sleep(0); stop.set(); await asyncio.gather(*tasks)
        self.assertEqual(set(marks), {'validation', 'publicnode'})
        self.assertLess(abs(marks['validation'] - marks['publicnode']), .1)

    async def test_http_retries_bounded_and_error_redacted(self):
        secret = 'HIDDEN_VALIDATION_TOKEN'
        class Client:
            calls = 0
            async def post(self, url, json):
                self.calls += 1
                raise httpx.ConnectError('connection to ' + url + ' failed')
        import httpx
        client = Client()
        with patch.object(r3.asyncio, 'sleep', return_value=None):
            result = await r3.http_rpc(client, 'https://example.invalid/' + secret, 'eth_getLogs', [], retries=2)
        self.assertEqual(client.calls, 3)
        self.assertNotIn(secret, json.dumps(result))

    async def test_http_429_backoff_is_bounded(self):
        import httpx
        class Client:
            calls = 0
            async def post(self, url, json):
                self.calls += 1
                return httpx.Response(429 if self.calls < 4 else 200,
                                      json={'error': {'code': -32005}} if self.calls < 4 else {'result': '0x1'},
                                      request=httpx.Request('POST', 'https://example.invalid'))
        client = Client()
        with patch.object(r3.asyncio, 'sleep', return_value=None) as sleep:
            result = await r3.http_rpc(client, 'https://example.invalid', 'eth_blockNumber', [])
        self.assertTrue(result['ok'])
        self.assertEqual(client.calls, 4)
        self.assertEqual(sleep.await_count, 3)

    async def test_ground_truth_shrinks_rejected_range(self):
        selected = {'curve': {'address': ['0xabc'], 'topics': [[r3.BUY, r3.SELL]]}}
        calls = []
        async def fake(client, url, method, params, **kwargs):
            first, last = int(params[0]['fromBlock'], 16), int(params[0]['toBlock'], 16)
            calls.append((first, last))
            return {'ok': False, 'status': 413} if last-first+1 > 50 else {'ok': True, 'result': [log()]}
        with patch.object(r3, 'http_rpc', fake), patch.object(r3.asyncio, 'sleep', return_value=None):
            truth, report = await r3.ground_truth(None, 'https://example.invalid', selected, 1, 100)
        self.assertTrue(report['complete'])
        self.assertEqual(report['ranges_reduced'], 1)
        self.assertEqual(calls, [(1, 100), (1, 50), (51, 100)])
        self.assertEqual(len(truth['curve_buy']), 1)

    def test_secret_safe_output_and_only_allowed_providers(self):
        secret = 'HIDDEN_VALIDATION_TOKEN'
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'result.json'
            payload = {'endpoint_fingerprints': {'validation': r3.fingerprint('https://x/' + secret)},
                       'providers_used': ['publicnode_wss', 'validation_wss', 'validation_http'],
                       'incremental_alchemy_requests': 0, 'production_routing_changed': False,
                       'error': r3.safe_error(RuntimeError('url=' + secret))}
            r3.safe_output(path, payload)
            content = path.read_text()
            self.assertNotIn(secret, content)
            self.assertNotIn('alchemy', content.lower().replace('incremental_alchemy_requests', ''))
            self.assertFalse(json.loads(content)['production_routing_changed'])
            if sys.platform != 'win32':
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_only_validation_endpoints_accepted_even_with_other_credentials(self):
        secret = 'HIDDEN_VALIDATION_TOKEN'
        env = {'BENCH_VALIDATION_HTTP': 'https://mainnet.robinhood.validationcloud.io/v1/' + secret,
               'BENCH_VALIDATION_WS': 'wss://mainnet.robinhood.validationcloud.io/v1/' + secret,
               'BENCH_CHAINSTACK_HTTP': 'https://other.invalid/unused'}
        self.assertEqual(r3.validation_urls(env),
                         (env['BENCH_VALIDATION_HTTP'], env['BENCH_VALIDATION_WS']))
        env['BENCH_VALIDATION_HTTP'] = 'https://robinhood-mainnet.g.alchemy.com/v2/' + secret
        with self.assertRaisesRegex(ValueError, 'Validation Cloud endpoint required'):
            r3.validation_urls(env)

    def test_production_snapshot_reads_only_and_detects_routing_change(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); (root / 'config').mkdir()
            (root / 'config/.env').write_text('ROBINHOOD_RPC_HTTP=one')
            (root / 'config/flow.env').write_text('FLOW_ENABLED=true')
            for name in ('main.db', 'flow.db'):
                with closing(sqlite3.connect(root / name)) as db:
                    db.execute('CREATE TABLE marker (id integer)'); db.commit()
            before_files = {name: (root / name).read_bytes() for name in ('main.db', 'flow.db')}
            class Output:
                stdout = 'ActiveState=active\nMainPID=100\nNRestarts=0\n'
            with patch.object(r3.subprocess, 'run', return_value=Output()):
                before = r3.snapshot(root, root / 'main.db', root / 'flow.db')
                self.assertEqual(before['integrity'], {'main': 'ok', 'flow': 'ok'})
                (root / 'config/.env').write_text('ROBINHOOD_RPC_HTTP=two')
                after = r3.snapshot(root, root / 'main.db', root / 'flow.db')
            self.assertNotEqual(before['routing_sha256'], after['routing_sha256'])
            self.assertEqual(before['services'], after['services'])
            self.assertEqual(before_files, {name: (root / name).read_bytes() for name in before_files})


if __name__ == '__main__':
    unittest.main()
