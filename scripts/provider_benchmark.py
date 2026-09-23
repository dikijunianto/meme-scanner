"""Isolated, bounded Robinhood RPC comparison. Reads production flow DB read-only."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import statistics
import time
from urllib.parse import urlsplit

import httpx
from dotenv import dotenv_values
from websockets.asyncio.client import connect

from app.flow_data import BUY, SELL, SWAP, HOOK

PUBLIC = {'publicnode': ('https://robinhood-rpc.publicnode.com', 'wss://robinhood-rpc.publicnode.com'),
          'robinhood': ('https://rpc.mainnet.chain.robinhood.com', None)}
KEYS = {'chainstack': ('BENCH_CHAINSTACK_HTTP', 'BENCH_CHAINSTACK_WS'),
        'validation': ('BENCH_VALIDATION_HTTP', 'BENCH_VALIDATION_WS'),
        'dwellir': ('BENCH_DWELLIR_HTTP', 'BENCH_DWELLIR_WS')}


def fingerprint(url):
    return hashlib.sha256(urlsplit(url).path.rsplit('/', 1)[-1].encode()).hexdigest()[:12]


def identity(log):
    return (log['blockHash'].lower(), log['transactionHash'].lower(), int(log['logIndex'], 16))


def summarize(values):
    if not values: return {'n': 0}
    sorted_values = sorted(values)
    return {'n': len(values), 'median_ms': round(statistics.median(values), 2),
            'p90_ms': round(sorted_values[min(len(values)-1, int(.9*(len(values)-1)))], 2),
            'p95_ms': round(sorted_values[min(len(values)-1, int(.95*(len(values)-1)))], 2)}


def normalize_error(response):
    if not isinstance(response, dict): return {'kind': 'invalid_json_rpc'}
    if 'error' in response:
        error = response['error']
        return {'kind': 'rpc_error', 'code': error.get('code') if isinstance(error, dict) else None}
    return None


def filters(db):
    # Reuse real targets/events; no chain discovery or unfiltered subscriptions.
    curves = [row[0] for row in db.execute('''SELECT t.curve_address FROM flow_tracking_targets t
        LEFT JOIN flow_events e ON e.launch_id=t.launch_id AND e.phase='curve'
        WHERE t.graduation_json IS NULL GROUP BY t.launch_id
        ORDER BY count(e.tx_hash) DESC,t.tracking_start_at DESC LIMIT 5''')]
    graduated = db.execute('''SELECT graduation_json FROM flow_tracking_targets
        WHERE graduation_json IS NOT NULL ORDER BY tracking_start_at DESC LIMIT 1''').fetchone()
    result = {'curve': {'address': curves, 'topics': [[BUY, SELL]]}}
    if graduated:
        g = json.loads(graduated[0]); result['v4'] = {'address': g['pool_manager_address'], 'topics': [SWAP, g['pool_id']]}
        result['hook'] = {'address': g['hooks'], 'topics': [HOOK, g['pool_id']]}
    return result


async def rpc(client, url, method, params):
    started = time.monotonic()
    try:
        r = await client.post(url, json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params})
        data = r.json(); error = normalize_error(data)
        return {'ok': r.status_code == 200 and error is None, 'status': r.status_code,
                'error': error, 'result': data.get('result'), 'latency_ms': round((time.monotonic()-started)*1000, 2),
                'bytes': len(r.content)}
    except Exception as exc:
        return {'ok': False, 'error': {'kind': type(exc).__name__}, 'latency_ms': round((time.monotonic()-started)*1000, 2)}


async def http_check(name, url, selected, block, known_blocks, token):
    out = {'provider': name, 'protocol': 'http', 'fingerprint': fingerprint(url) if name not in PUBLIC else None,
           'methods': {}, 'head_samples': [], 'getlogs': {}}
    async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
        calls = {'eth_chainId': [], 'eth_blockNumber': [],
                 'eth_getBlockByNumber': [hex(block), False],
                 'eth_call': [{'to': token, 'data': '0x313ce567'}, hex(block)]}
        for method, params in calls.items():
            answer = await rpc(client, url, method, params)
            if method == 'eth_chainId' and answer.get('result') != '0x1237': answer['ok'] = False
            if method == 'eth_getBlockByNumber' and answer.get('result'):
                header = answer.pop('result')
                answer['block'] = {'number': header.get('number'), 'hash': header.get('hash')}
            if method == 'eth_call' and answer.get('result'):
                answer['result_sha256'] = hashlib.sha256(answer.pop('result').encode()).hexdigest()
            out['methods'][method] = answer
            await asyncio.sleep(1)
        # Known filters and bounded fixed ranges. Never query all transactions/logs.
        for kind, query in selected.items():
            for span in (10, 100):
                target_block = known_blocks.get(kind) or block
                params = [dict(query, fromBlock=hex(target_block-span+1), toBlock=hex(target_block))]
                answer = await rpc(client, url, 'eth_getLogs', params)
                logs = answer.pop('result', []) if answer.get('ok') else []
                answer['count'] = len(logs) if isinstance(logs, list) else None
                answer['identities'] = [identity(item) for item in logs] if isinstance(logs, list) else []
                answer['range'] = [target_block-span+1, target_block]
                out['getlogs'][f'{kind}_{span}'] = answer
                await asyncio.sleep(1)
        batch = [{'jsonrpc': '2.0', 'id': n, 'method': 'eth_chainId', 'params': []} for n in (1, 2)]
        try:
            r = await client.post(url, json=batch)
            body = r.json(); out['batch'] = {'status': r.status_code, 'accepted': isinstance(body, list) and
                                             {x.get('id') for x in body} == {1, 2} and
                                             all(x.get('result') == '0x1237' for x in body)}
        except Exception as exc: out['batch'] = {'accepted': False, 'error_type': type(exc).__name__}
        for _ in range(30):
            head = await rpc(client, url, 'eth_blockNumber', [])
            if head.get('ok'):
                number = int(head['result'], 16)
                header = await rpc(client, url, 'eth_getBlockByNumber', [hex(number), False])
                out['head_samples'].append({'number': number, 'hash': (header.get('result') or {}).get('hash'),
                                            'latency_ms': head['latency_ms'], 'header_latency_ms': header['latency_ms'],
                                            'http_status': head.get('status')})
            await asyncio.sleep(2)
        eth_call_latencies = [out['methods']['eth_call']['latency_ms']]
        for _ in range(4):
            response = await rpc(client, url, 'eth_call', calls['eth_call'])
            eth_call_latencies.append(response['latency_ms'])
            await asyncio.sleep(1)
        out['method_latency'] = {'eth_blockNumber': summarize([x['latency_ms'] for x in out['head_samples']]),
                                 'eth_call': summarize(eth_call_latencies),
                                 'eth_getLogs': summarize([x['latency_ms'] for x in out['getlogs'].values()])}
    # Result payloads can contain arbitrary provider text. Keep only safe metadata.
    for item in out['methods'].values():
        if 'result' in item and isinstance(item['result'], str) and len(item['result']) > 80:
            item['result_sha256'] = hashlib.sha256(item.pop('result').encode()).hexdigest()
    return out


def compare_http(left, right):
    comparison = {'getlogs': {}, 'pinned_state_equal': None, 'pinned_block_hash_equal': None}
    for key, a in left['getlogs'].items():
        b = right['getlogs'].get(key)
        if not b or not a['ok'] or not b['ok']:
            comparison['getlogs'][key] = {'comparable': False}; continue
        aa, bb = set(map(tuple, a['identities'])), set(map(tuple, b['identities']))
        comparison['getlogs'][key] = {'comparable': True, 'left_only': len(aa-bb),
            'right_only': len(bb-aa), 'same_order': a['identities'] == b['identities'],
            'block_hashes_equal': {x[0] for x in aa} == {x[0] for x in bb}}
    state_a, state_b = left['methods']['eth_call'], right['methods']['eth_call']
    if state_a['ok'] and state_b['ok']:
        comparison['pinned_state_equal'] = state_a.get('result_sha256', state_a.get('result')) == state_b.get('result_sha256', state_b.get('result'))
    block_a, block_b = left['methods']['eth_getBlockByNumber'], right['methods']['eth_getBlockByNumber']
    if block_a['ok'] and block_b['ok']:
        comparison['pinned_block_hash_equal'] = block_a['block']['hash'] == block_b['block']['hash']
    return comparison


def classify_provider(record, *, control=False, credential_missing=False, free=True):
    if credential_missing: return 'NOT_TESTED_CREDENTIAL_REQUIRED'
    if not free: return 'NOT_FREE_CURRENTLY'
    http = record.get('http') or {}; ws = record.get('wss') or {}
    methods = http.get('methods') or {}
    http_ok = all(methods.get(name, {}).get('ok') for name in
                  ('eth_chainId', 'eth_blockNumber', 'eth_getBlockByNumber', 'eth_call'))
    logs_ok = bool(http.get('getlogs')) and all(x.get('ok') for x in http['getlogs'].values())
    if control: return 'HTTP_FALLBACK_PASS' if http_ok and logs_ok else 'INCONCLUSIVE'
    if ws and (not ws.get('chain_id_ok') or any(not x.get('accepted') for x in ws.get('subscriptions', {}).values())
               or ws.get('wrong_filter')): return 'FAIL'
    recovery = (ws.get('recovery') or {}).get('filters', {})
    proven = (ws.get('connected') and (ws.get('reconnect') or {}).get('success') and
              ws.get('unexpected_disconnects') == 0 and recovery and
              all(x.get('complete_query') and x.get('expected_logs', 0) > 0 and
                  x.get('missing_live') == 0 and x.get('extra_live') == 0 for x in recovery.values()))
    if proven: return 'FULL_SECONDARY_PASS' if http_ok and logs_ok else 'WSS_SECONDARY_PASS'
    if not ws and http_ok and logs_ok: return 'HTTP_FALLBACK_PASS'
    return 'INCONCLUSIVE'


async def paired_heads(left_url, right_url):
    samples = []
    async with httpx.AsyncClient(timeout=20) as client:
        for _ in range(30):
            a, b = await asyncio.gather(rpc(client, left_url, 'eth_blockNumber', []),
                                        rpc(client, right_url, 'eth_blockNumber', []))
            if a['ok'] and b['ok']:
                x, y = int(a['result'], 16), int(b['result'], 16)
                h1, h2 = await asyncio.gather(rpc(client, left_url, 'eth_getBlockByNumber', [hex(min(x, y)), False]),
                                              rpc(client, right_url, 'eth_getBlockByNumber', [hex(min(x, y)), False]))
                samples.append({'publicnode': x, 'robinhood': y, 'lag_from_highest':
                    {'publicnode': max(x, y)-x, 'robinhood': max(x, y)-y},
                    'common_height': min(x, y), 'common_hash_equal':
                    ((h1.get('result') or {}).get('hash') == (h2.get('result') or {}).get('hash'))
                    if h1['ok'] and h2['ok'] and h1.get('result') and h2.get('result') else None})
            await asyncio.sleep(2)
    return samples


async def live_check(url, selected, seconds):
    out = {'connected': False, 'chain_id_ok': False, 'subscriptions': {}, 'events': {},
           'duplicates': 0, 'wrong_filter': 0, 'bytes': 0, 'unexpected_disconnects': 0,
           'reconnect': None, 'errors': [], 'duration_seconds': seconds}
    seen = set(); received = defaultdict(list); start = time.monotonic(); reconnect_at = start + seconds/2
    for attempt in (0, 1):
        if attempt and time.monotonic() < reconnect_at: await asyncio.sleep(reconnect_at-time.monotonic())
        try:
            opened = time.monotonic()
            async with connect(url, open_timeout=15, ping_interval=20, ping_timeout=20,
                               max_size=262144, max_queue=8, compression=None) as socket:
                out['connected'] = True
                if attempt: out['reconnect'] = {'seconds': round(time.monotonic()-opened, 2), 'success': True}
                next_id = 1
                async def command(method, params):
                    nonlocal next_id
                    request_id = next_id; next_id += 1
                    await socket.send(json.dumps({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params}))
                    while True:
                        raw = await asyncio.wait_for(socket.recv(), 20)
                        if isinstance(raw, bytes): raw = raw.decode()
                        out['bytes'] += len(raw.encode())
                        msg = json.loads(raw)
                        if msg.get('id') == request_id: return msg
                        if msg.get('method') == 'eth_subscription': record(msg, time.monotonic())
                def record(message, at):
                    entry = message.get('params', {}); log = entry.get('result', {})
                    sub = entry.get('subscription'); kind = routes.get(sub)
                    if not kind or not isinstance(log, dict): out['wrong_filter'] += 1; return
                    try: key = identity(log)
                    except (KeyError, ValueError, TypeError): out['errors'].append('malformed_log'); return
                    query = selected[kind]; topics = log.get('topics', [])
                    addresses = query['address'] if isinstance(query['address'], list) else [query['address']]
                    expected = log.get('address', '').lower() in {x.lower() for x in addresses}
                    expected &= bool(topics) and topics[0].lower() in [x.lower() for x in (query['topics'][0] if isinstance(query['topics'][0], list) else [query['topics'][0]])]
                    if len(query['topics']) > 1: expected &= len(topics) > 1 and topics[1].lower() == query['topics'][1].lower()
                    if not expected: out['wrong_filter'] += 1
                    if (kind, key) in seen: out['duplicates'] += 1
                    seen.add((kind, key)); received[kind].append({'identity': key, 'at_monotonic': at,
                                                                  'block': int(log['blockNumber'], 16), 'removed': bool(log.get('removed'))})
                routes = {}
                chain = await command('eth_chainId', [])
                out['chain_id_ok'] = chain.get('result') == '0x1237'
                if not out['chain_id_ok']: break
                for kind, query in selected.items():
                    response = await command('eth_subscribe', ['logs', query])
                    sub = response.get('result')
                    out['subscriptions'][kind] = {'accepted': isinstance(sub, str), 'error_code':
                        response.get('error', {}).get('code') if isinstance(response.get('error'), dict) else None}
                    if isinstance(sub, str): routes[sub] = kind
                stop = min(reconnect_at, start+seconds) if not attempt else start+seconds
                while time.monotonic() < stop:
                    try: raw = await asyncio.wait_for(socket.recv(), min(10, stop-time.monotonic()))
                    except asyncio.TimeoutError: continue
                    if isinstance(raw, bytes): raw = raw.decode()
                    out['bytes'] += len(raw.encode())
                    try: message = json.loads(raw)
                    except ValueError: out['errors'].append('invalid_json'); continue
                    if message.get('method') == 'eth_subscription': record(message, time.monotonic())
                for sub in routes:
                    response = await command('eth_unsubscribe', [sub])
                    out['subscriptions'][routes[sub]]['unsubscribe_ok'] = response.get('result') is True
        except Exception as exc:
            out['unexpected_disconnects'] += 1; out['errors'].append(type(exc).__name__)
    out['events'] = dict(received)
    out['notification_count'] = sum(map(len, received.values()))
    out['bytes_per_event'] = round(out['bytes']/out['notification_count'], 2) if out['notification_count'] else None
    return out


async def verify_window(http_url, selected, live, first, last):
    """Recover the exact live block interval in bounded filtered pages."""
    result = {'first_block': first, 'last_block': last, 'filters': {}}
    async with httpx.AsyncClient(timeout=25) as client:
        for kind, query in selected.items():
            expected = set(); current = first; span = 100; calls = 0; errors = []
            while current <= last and calls < 500:
                end = min(last, current+span-1)
                answer = await rpc(client, http_url, 'eth_getLogs',
                                   [dict(query, fromBlock=hex(current), toBlock=hex(end))])
                calls += 1
                if answer.get('ok') and isinstance(answer.get('result'), list):
                    expected.update(identity(x) for x in answer['result']); current = end+1
                elif span > 1 and answer.get('status') != 429 and (answer.get('status') in (400, 413)
                        or (answer.get('error') or {}).get('code') in (-32000, -32602)):
                    span = max(1, span//2)
                else:
                    errors.append({'block': current, 'status': answer.get('status'),
                                   'error': answer.get('error')}); break
                await asyncio.sleep(1)
            received = [tuple(x['identity']) for x in live['events'].get(kind, []) if first <= x['block'] <= last]
            actual = set(received)
            complete = current > last and not errors
            result['filters'][kind] = {'complete_query': complete, 'http_calls': calls,
                'expected_logs': len(expected), 'received_unique_expected': len(actual & expected),
                'missing_live': len(expected-actual) if complete else None,
                'extra_live': len(actual-expected) if complete else None,
                'duplicate_live': len(received)-len(actual), 'errors': errors,
                'completeness': round(len(actual & expected)/len(expected), 5) if complete and expected else None}
    return result


async def main(args):
    output = args.output.resolve()
    allowed = (Path('/tmp').resolve(), (Path(__file__).resolve().parents[1] / 'data/provider-benchmark').resolve())
    if not any(output.is_relative_to(directory) for directory in allowed):
        raise ValueError('Benchmark output must stay in an isolated temporary directory')
    if args.env and (args.env.stat().st_mode & 0o077 or args.env.parent.stat().st_mode & 0o077):
        raise ValueError('Benchmark endpoint file and parent must be private')
    env = dotenv_values(args.env, interpolate=False) if args.env else {}
    providers = dict(PUBLIC)
    for name, (h, w) in KEYS.items():
        if env.get(h) or env.get(w): providers[name] = (env.get(h), env.get(w))
    db = sqlite3.connect(Path(args.flow_db).resolve().as_uri()+'?mode=ro', uri=True)
    selected = filters(db)
    block = db.execute('SELECT max(block_number) FROM flow_events').fetchone()[0]
    known_blocks = {}
    for kind in selected:
        phase = 'curve' if kind == 'curve' else kind
        clause = 'lower(t.curve_address)=lower(?)' if kind == 'curve' else 'lower(t.pool_id)=lower(?)'
        value = selected[kind]['address'][0] if kind == 'curve' else selected[kind]['topics'][1]
        row = db.execute('''SELECT max(e.block_number) FROM flow_events e JOIN flow_tracking_targets t
            ON t.launch_id=e.launch_id WHERE e.phase=? AND '''+clause, (phase, value)).fetchone()
        known_blocks[kind] = row[0]
    token = db.execute('SELECT token_address FROM flow_tracking_targets WHERE curve_address=? LIMIT 1',
                       (selected['curve']['address'][0],)).fetchone()[0]
    db.close()
    if not block or not selected['curve']['address']: raise ValueError('No known chain evidence')
    result = {'scope': 'isolated_read_only', 'block': block, 'known_blocks': known_blocks, 'filters': selected,
              'providers': {}, 'missing_credentials': sorted(set(KEYS)-set(providers)),
              'production_routing_changed': False, 'incremental_alchemy_requests': 0}
    for name, (http, ws) in providers.items():
        http_result = await http_check(name, http, selected, block, known_blocks, token) if http else None
        live_result = None
        if ws:
            async with httpx.AsyncClient(timeout=20) as client:
                start_head = await rpc(client, http, 'eth_blockNumber', [])
            live_result = await live_check(ws, selected, args.minutes*60)
            async with httpx.AsyncClient(timeout=20) as client:
                end_head = await rpc(client, http, 'eth_blockNumber', [])
            if start_head.get('ok') and end_head.get('ok'):
                live_result['recovery'] = await verify_window(http, selected, live_result,
                    int(start_head['result'], 16), int(end_head['result'], 16))
        result['providers'][name] = {'http': http_result, 'wss': live_result}
        args.output.write_text(json.dumps(result, indent=2)); args.output.chmod(0o600)
    if 'publicnode' in result['providers'] and 'robinhood' in result['providers']:
        result['consistency'] = compare_http(result['providers']['publicnode']['http'],
                                             result['providers']['robinhood']['http'])
        result['paired_heads'] = await paired_heads(PUBLIC['publicnode'][0], PUBLIC['robinhood'][0])
    result['classifications'] = {name: classify_provider(record, control=name == 'robinhood')
                                 for name, record in result['providers'].items()}
    result['classifications'].update({name: 'NOT_TESTED_CREDENTIAL_REQUIRED' for name in result['missing_credentials']})
    args.output.write_text(json.dumps(result, indent=2)); args.output.chmod(0o600)
    print(json.dumps({'result_path': str(args.output), 'providers': list(result['providers']),
                      'missing_credentials': result['missing_credentials']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--flow-db', type=Path, default=Path('/opt/meme-scanner/data/flow.db'))
    parser.add_argument('--env', type=Path)
    parser.add_argument('--output', type=Path, default=Path('/tmp/provider-benchmark-results.json'))
    parser.add_argument('--minutes', type=int, choices=range(20, 31), default=20)
    asyncio.run(main(parser.parse_args()))
