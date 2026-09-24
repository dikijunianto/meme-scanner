"""Deterministic checks for synchronized dynamic provider proof."""
import asyncio
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import provider_benchmark_round4 as r4

A = '0x' + 'a'*40
B = '0x' + 'b'*40
C = '0x' + 'c'*40


def log(topic, address=A, block=101, tx='0xAB', index='0x1', second=None):
    return {'address': address, 'topics': [topic] + ([second] if second else []),
            'blockHash': hex(block), 'blockNumber': hex(block),
            'transactionHash': tx, 'logIndex': index}


def empty():
    return {cls: set() for cls in r4.CLASSES}


class FakeClient:
    def __init__(self, ok=True):
        self.ok = ok
        self.added = []
        self.removed = []

    async def subscribe(self, key, kind, query, address):
        self.added.append((key, kind, query, address))
        return '2026-01-01T00:00:00+00:00' if self.ok else None

    async def unsubscribe(self, key):
        self.removed.append(key)
        return True


class Round4Tests(unittest.IsolatedAsyncioTestCase):
    def test_discovery_is_local_ranked_and_read_only(self):
        now = 1_800_000_000.0
        with tempfile.TemporaryDirectory() as temp:
            flow_path, main_path = Path(temp)/'flow.db', Path(temp)/'main.db'
            with closing(sqlite3.connect(flow_path)) as db:
                db.execute('CREATE TABLE flow_tracking_targets (launch_id integer,curve_address text,created_at real,tracking_start_at real,tracking_end_at real,graduation_json text,status text)')
                db.execute('CREATE TABLE flow_events (launch_id integer,phase text,observed_at real)')
                db.executemany('INSERT INTO flow_tracking_targets VALUES(?,?,?,?,?,?,?)', [
                    (1,A,now-1000,now-1000,now+600,None,'active_curve'),
                    (2,B,now-100,now-100,now+600,None,'active_curve'),
                    (3,C,now-100,now-100,now-1,None,'completed')])
                db.execute('INSERT INTO flow_events VALUES(1,?,?)', ('curve', now-30)); db.commit()
            with closing(sqlite3.connect(main_path)) as db:
                db.execute('CREATE TABLE launches (id integer,curve_address text,block_number integer,block_timestamp text,token_address text,launch_type text)')
                db.execute('CREATE TABLE graduations (token_address text)')
                db.executemany('INSERT INTO launches VALUES(?,?,?,?,?,?)', [
                    (1,A,10,r4.timestamp(now-100),'0x1','pons-v2'),
                    (2,B,11,r4.timestamp(now-100),'0x2','pons-v2'),
                    (3,C,12,r4.timestamp(now-10),'0x3','pons-v2')]); db.commit()
            before = (flow_path.read_bytes(), main_path.read_bytes())
            found = r4.discover(flow_path, main_path, now)
            self.assertEqual([(x['curve_address'], x['reason']) for x in found],
                             [(A, 'recent_activity'), (B, 'new_active_target'), (C, 'recent_launch')])
            self.assertEqual(before, (flow_path.read_bytes(), main_path.read_bytes()))

    async def test_synchronized_add_acks_then_head_plus_one(self):
        clients = {'validation': FakeClient(), 'publicnode': FakeClient()}
        active, entries, failures = {}, [], []
        candidate = {'curve_address': A, 'launch_id': 1, 'reason': 'recent_launch',
                     'discovered_utc': '2026-01-01T00:00:00+00:00'}
        async def fake_head(client, url):
            self.assertEqual(len(clients['validation'].added), 1)
            self.assertEqual(len(clients['publicnode'].added), 1)
            return 100
        with patch.object(r4.r3, 'head', fake_head):
            added = await r4.add_curves([candidate], clients, None, 'https://example.invalid',
                                        active, entries, failures)
        self.assertEqual(added, 1)
        self.assertFalse(failures)
        self.assertEqual(entries[0]['start_block'], 101)
        self.assertEqual(set(active), {A})
        self.assertEqual(clients['validation'].added[0][2], clients['publicnode'].added[0][2])
        self.assertEqual(clients['validation'].added[0][2]['topics'], [[r4.BUY, r4.SELL]])

    async def test_one_provider_failure_rolls_back_target(self):
        clients = {'validation': FakeClient(), 'publicnode': FakeClient(ok=False)}
        candidate = {'curve_address': A, 'launch_id': 1, 'reason': 'recent_launch',
                     'discovered_utc': '2026-01-01T00:00:00+00:00'}
        active, entries, failures = {}, [], []
        with patch.object(r4.r3, 'head', side_effect=AssertionError('head before both ACKs')):
            added = await r4.add_curves([candidate], clients, None, 'https://example.invalid',
                                        active, entries, failures)
        self.assertEqual(added, 0)
        self.assertFalse(entries)
        self.assertEqual(failures[0]['providers'], ['publicnode'])
        self.assertEqual(clients['validation'].removed, ['curve:' + A])

    def test_activation_excludes_early_events_and_segments_exactly(self):
        entries = [{'curve_address': A, 'start_block': 100, 'end_block': 199},
                   {'curve_address': B, 'start_block': 200, 'end_block': None},
                   {'curve_address': C, 'start_block': 300, 'end_block': None}]
        self.assertEqual(r4.curve_segments(entries, 350), [
            {'first': 100, 'last': 199, 'addresses': [A]},
            {'first': 200, 'last': 299, 'addresses': [B]},
            {'first': 300, 'last': 350, 'addresses': [B,C]}])
        record = r4.r3.new_record()
        key1, key2 = ('0x1','0x1',1), ('0x2','0x2',2)
        record['events']['curve_buy'][key1] = (99,A)
        record['events']['curve_sell'][key2] = (100,A)
        eligible = r4.eligible_sets(record, entries, None, 350)
        self.assertFalse(eligible['curve_buy'])
        self.assertEqual(eligible['curve_sell'], {key2})

    async def test_http_multi_address_buy_sell_and_bounded_ranges(self):
        class Counter:
            getlogs_calls = 0
        client = Counter(); calls = []
        entries = [{'curve_address': A, 'start_block': 100, 'end_block': None},
                   {'curve_address': B, 'start_block': 100, 'end_block': None}]
        async def fake(client, url, method, params):
            client.getlogs_calls += 1; calls.append(params[0])
            return {'ok': True, 'result': [log(r4.BUY,A,100), log(r4.SELL,B,100,tx='0xCD',index='0x2')]}
        with patch.object(r4.r3, 'http_rpc', fake), patch.object(r4.asyncio, 'sleep', return_value=None):
            truth, report = await r4.truth_query(client, 'https://example.invalid', entries, None, 4100)
        self.assertEqual(report['getlogs_calls'], 3)
        self.assertEqual(report['queried_dynamic_segments'], 1)
        self.assertEqual(calls[0]['address'], [A,B])
        self.assertEqual(calls[0]['topics'], [[r4.BUY,r4.SELL]])
        self.assertEqual((len(truth['curve_buy']),len(truth['curve_sell'])), (1,1))

    def test_canonical_duplicate_and_wrong_filter(self):
        client = r4.WSSClient('validation', 'wss://example.invalid/SECRET')
        route = ('curve', r4.curve_query(A), A)
        message = lambda item: {'params': {'result': item}}
        client.notification(message(log(r4.BUY)), route)
        client.notification(message(log(r4.BUY,tx='0xab',index='0x01')), route)
        client.notification(message(log(r4.SELL,address=B)), route)
        self.assertEqual(client.record['duplicates'], 1)
        self.assertEqual(client.record['wrong_filter'], 1)
        self.assertEqual(len(client.record['events']['curve_buy']), 1)

    def test_ground_truth_comparison_buy_only_sell_only_full_zero_and_failures(self):
        record = r4.r3.new_record()
        record.update(connected=True, chain_id_ok=True, reconnect_failures=0)
        truth = empty(); actual = empty()
        comp = r4.compare(truth, actual, True)
        self.assertEqual(comp['curve_buy']['status'], 'UNPROVEN_NO_EVENTS')
        self.assertEqual(r4.classify('validation',record,comp,True,truth,0),
                         'VALIDATION_WSS_CURVE_UNPROVEN')
        truth['curve_buy'] = {('b','t',i) for i in range(2)}; actual['curve_buy'] = truth['curve_buy'].copy()
        comp = r4.compare(truth,actual,True)
        self.assertEqual(r4.classify('validation',record,comp,True,truth,0),
                         'VALIDATION_WSS_CURVE_BUY_ONLY')
        truth, actual = empty(), empty()
        truth['curve_sell'] = {('b','t',1)}; actual['curve_sell'] = truth['curve_sell'].copy()
        comp = r4.compare(truth,actual,True)
        self.assertEqual(r4.classify('publicnode',record,comp,True,truth,0),
                         'PUBLICNODE_WSS_CURVE_SELL_ONLY')
        truth['curve_buy'] = {('b','t',i) for i in range(2,6)}
        actual['curve_buy'] = truth['curve_buy'].copy()
        comp = r4.compare(truth,actual,True)
        self.assertTrue(r4.curve_target_met(truth))
        self.assertEqual(r4.classify('validation',record,comp,True,truth,0),
                         'VALIDATION_FULL_SECONDARY_PASS')
        actual['curve_buy'].pop(); comp = r4.compare(truth,actual,True)
        self.assertEqual(comp['curve_buy']['missing'], 1)
        self.assertEqual(r4.classify('validation',record,comp,True,truth,0), 'VALIDATION_FAIL')
        actual['curve_buy'] = truth['curve_buy'].copy(); actual['curve_buy'].add(('x','y',9))
        self.assertEqual(r4.compare(truth,actual,True)['curve_buy']['extra'], 1)
        record['unexpected_disconnects'] = 1
        self.assertEqual(r4.classify('validation',record,r4.compare(truth,truth,True),True,truth,0),
                         'VALIDATION_FAIL')

    def test_secret_error_type_and_provider_allowlist(self):
        secret = 'HIDDEN_SECRET_TOKEN'
        error = r4.r3.safe_error(RuntimeError('wss://example.invalid/' + secret))
        self.assertNotIn(secret, json.dumps(error))
        urls = {'BENCH_VALIDATION_HTTP': 'https://mainnet.robinhood.validationcloud.io/v1/'+secret,
                'BENCH_VALIDATION_WS': 'wss://mainnet.robinhood.validationcloud.io/v1/'+secret,
                'BENCH_CHAINSTACK_WS': 'wss://other.invalid/unused'}
        self.assertEqual(r4.r3.validation_urls(urls)[:2],
                         (urls['BENCH_VALIDATION_HTTP'],urls['BENCH_VALIDATION_WS']))
        urls['BENCH_VALIDATION_WS'] = 'wss://robinhood-mainnet.g.alchemy.com/v2/'+secret
        with self.assertRaises(ValueError):
            r4.r3.validation_urls(urls)

    async def test_wss_and_http_exceptions_never_emit_credential_urls(self):
        secret = 'HIDDEN_SECRET_TOKEN'
        class Socket:
            async def recv(self):
                raise RuntimeError('wss://example.invalid/' + secret)
        wss = r4.WSSClient('validation', 'wss://example.invalid/' + secret)
        wss.ws = Socket()
        await wss.reader()
        self.assertNotIn(secret, json.dumps(wss.record['errors']))
        class HTTP:
            async def post(self, url, json):
                raise RuntimeError('https://example.invalid/' + secret)
        reply = await r4.r3.http_rpc(HTTP(), 'https://example.invalid/' + secret,
                                      'eth_getLogs', [], retries=0)
        self.assertNotIn(secret, json.dumps(reply))


if __name__ == '__main__':
    unittest.main()
