"""Concurrent, read-only WSS comparison against Validation HTTP logs."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import sqlite3
import subprocess
import time
from urllib.parse import urlsplit

import httpx
from dotenv import dotenv_values
from websockets.asyncio.client import connect

from app.flow_data import BUY, SELL, SWAP, HOOK
from provider_benchmark import PUBLIC

CLASSES = ('curve_buy', 'curve_sell', 'v4', 'hook')
TOPIC_CLASS = {BUY.lower(): 'curve_buy', SELL.lower(): 'curve_sell',
               SWAP.lower(): 'v4', HOOK.lower(): 'hook'}
CHAIN_ID = '0x1237'


def utc():
    return datetime.now(timezone.utc).isoformat()


def fingerprint(url):
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def validation_urls(env):
    http, ws = env.get('BENCH_VALIDATION_HTTP'), env.get('BENCH_VALIDATION_WS')
    if not http or not ws:
        raise ValueError('Validation HTTP and WSS credentials are required')
    for url, scheme in ((http, 'https'), (ws, 'wss')):
        parsed = urlsplit(url)
        if parsed.scheme != scheme or parsed.hostname != 'mainnet.robinhood.validationcloud.io':
            raise ValueError('Validation Cloud endpoint required')
    return http, ws


def db_ro(path):
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)


def select_filters(db):
    rows = db.execute('''SELECT curve_address FROM flow_tracking_targets
        WHERE graduation_json IS NULL AND status NOT IN ('completed','partial')
        ORDER BY tracking_start_at DESC LIMIT 5''').fetchall()
    if not rows:
        rows = db.execute('''SELECT curve_address FROM flow_tracking_targets
            WHERE graduation_json IS NULL ORDER BY tracking_start_at DESC LIMIT 5''').fetchall()
    curves = list(dict.fromkeys(row[0] for row in rows))
    if not curves:
        raise ValueError('No real curve targets in flow database')
    selected = {'curve': {'address': curves, 'topics': [[BUY, SELL]]}}
    graduated = db.execute('''SELECT graduation_json FROM flow_tracking_targets
        WHERE graduation_json IS NOT NULL ORDER BY tracking_start_at DESC LIMIT 1''').fetchone()
    if graduated:
        g = json.loads(graduated[0])
        selected['v4'] = {'address': g['pool_manager_address'], 'topics': [SWAP, g['pool_id']]}
        selected['hook'] = {'address': g['hooks'], 'topics': [HOOK, g['pool_id']]}
    return selected


def snapshot(root, main_db, flow_db):
    services = {}
    for name in ('meme-scanner', 'meme-scanner-flow'):
        p = subprocess.run(['systemctl', 'show', name, '--property=ActiveState,MainPID,NRestarts'],
                           capture_output=True, text=True, timeout=10, check=True)
        services[name] = dict(line.split('=', 1) for line in p.stdout.splitlines() if '=' in line)
    integrity = {}
    for name, path in (('main', main_db), ('flow', flow_db)):
        with closing(db_ro(path)) as db:
            integrity[name] = db.execute('PRAGMA quick_check').fetchone()[0]
    routing = hashlib.sha256()
    for name in ('config/.env', 'config/flow.env'):
        routing.update((root / name).read_bytes())
    return {'utc': utc(), 'services': services, 'integrity': integrity,
            'routing_sha256': routing.hexdigest()}


def event_identity(log):
    block = log.get('blockHash') or log.get('blockNumber')
    return (str(block).lower(), str(log['transactionHash']).lower(),
            int(log['logIndex'], 16) if isinstance(log['logIndex'], str) else int(log['logIndex']))


def log_class(log, query):
    topics = log.get('topics') or []
    if not topics:
        return None
    actual_class = TOPIC_CLASS.get(str(topics[0]).lower())
    addresses = query['address'] if isinstance(query['address'], list) else [query['address']]
    if str(log.get('address', '')).lower() not in {x.lower() for x in addresses}:
        return None
    permitted = query['topics'][0] if isinstance(query['topics'][0], list) else [query['topics'][0]]
    if str(topics[0]).lower() not in {x.lower() for x in permitted}:
        return None
    if len(query['topics']) > 1 and (len(topics) < 2 or str(topics[1]).lower() != query['topics'][1].lower()):
        return None
    return actual_class


def safe_error(exc):
    return {'type': type(exc).__name__}


async def command(ws, request_id, method, params, record):
    await ws.send(json.dumps({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params}))
    while True:
        raw = await asyncio.wait_for(ws.recv(), 20)
        record['bytes'] += len(raw if isinstance(raw, bytes) else raw.encode())
        message = json.loads(raw)
        if message.get('id') == request_id:
            return message
        # Notifications before the shared start block are deliberately ignored.


def new_record():
    return {'connected': False, 'chain_id_ok': False, 'subscription_ids_count': 0,
            'start_monotonic': None, 'end_monotonic': None, 'start_utc': None, 'end_utc': None,
            'first_block': None, 'last_block': None, 'notification_count': 0,
            'duplicates': 0, 'removed': 0, 'wrong_filter': 0, 'malformed': 0,
            'unexpected_disconnects': 0, 'reconnect_attempts': 0, 'reconnect_success': 0,
            'reconnect_seconds': [], 'bytes': 0, 'errors': [], 'events': {k: {} for k in CLASSES},
            '_seen_tx_log': set()}


def record_notification(record, message, routes, selected, curve_seen):
    record['notification_count'] += 1
    try:
        params = message['params']
        kind = routes.get(params['subscription'])
        if kind is None:
            record['wrong_filter'] += 1
            return
        log = params['result']
        if not isinstance(log, dict):
            raise TypeError('non-object log')
        cls = log_class(log, selected[kind])
        if cls is None:
            record['wrong_filter'] += 1
            return
        key = event_identity(log)
        block = int(log['blockNumber'], 16) if isinstance(log['blockNumber'], str) else int(log['blockNumber'])
        if log.get('removed'):
            record['removed'] += 1
            record['events'][cls].pop(key, None)
            return
        pair = (key[1], key[2])
        if pair in record['_seen_tx_log']:
            record['duplicates'] += 1
        record['_seen_tx_log'].add(pair)
        record['events'][cls][key] = block
        record['first_block'] = block if record['first_block'] is None else min(block, record['first_block'])
        record['last_block'] = block if record['last_block'] is None else max(block, record['last_block'])
        if cls.startswith('curve_'):
            curve_seen.set()
    except (KeyError, TypeError, ValueError, IndexError):
        record['malformed'] += 1


async def collect(name, url, selected, ready, start, stop, curve_seen, max_seconds):
    record = new_record()
    deadline = time.monotonic() + max_seconds + 90
    for attempt in range(4):
        if stop.is_set() or time.monotonic() >= deadline:
            break
        if attempt:
            record['reconnect_attempts'] += 1
            await asyncio.sleep(min(8, 2 ** attempt + random.random()))
        opened = time.monotonic()
        try:
            async with connect(url, open_timeout=20, ping_interval=20, ping_timeout=20,
                               max_size=262144, max_queue=8, compression=None) as ws:
                record['connected'] = True
                if attempt:
                    record['reconnect_success'] += 1
                    record['reconnect_seconds'].append(round(time.monotonic()-opened, 3))
                chain = await command(ws, 1, 'eth_chainId', [], record)
                record['chain_id_ok'] = chain.get('result') == CHAIN_ID
                if not record['chain_id_ok']:
                    record['errors'].append({'type': 'WrongChain', 'code': None})
                    break
                routes = {}
                for request_id, (kind, query) in enumerate(selected.items(), 2):
                    answer = await command(ws, request_id, 'eth_subscribe', ['logs', query], record)
                    sub = answer.get('result')
                    if not isinstance(sub, str):
                        error = answer.get('error')
                        record['errors'].append({'type': 'SubscriptionRejected',
                                                 'code': error.get('code') if isinstance(error, dict) else None})
                        break
                    routes[sub] = kind
                if len(routes) != len(selected):
                    break
                record['subscription_ids_count'] = len(routes)
                ready.set()
                await start.wait()
                if record['start_monotonic'] is None:
                    record['start_monotonic'], record['start_utc'] = time.monotonic(), utc()
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), 1)
                    except asyncio.TimeoutError:
                        continue
                    record['bytes'] += len(raw if isinstance(raw, bytes) else raw.encode())
                    try:
                        message = json.loads(raw)
                    except (ValueError, TypeError):
                        record['malformed'] += 1
                        continue
                    if message.get('method') == 'eth_subscription':
                        record_notification(record, message, routes, selected, curve_seen)
        except Exception as exc:
            record['unexpected_disconnects'] += 1
            record['errors'].append(safe_error(exc))
    ready.set()
    record['end_monotonic'], record['end_utc'] = time.monotonic(), utc()
    return record


async def http_rpc(client, url, method, params, *, retries=3):
    for attempt in range(retries + 1):
        try:
            response = await client.post(url, json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params})
            body = response.json()
            error = body.get('error') if isinstance(body, dict) else None
            code = error.get('code') if isinstance(error, dict) else None
            if response.status_code == 200 and not error and isinstance(body, dict):
                return {'ok': True, 'result': body.get('result')}
            result = {'ok': False, 'status': response.status_code, 'code': code}
            transient = response.status_code in (429, 500, 502, 503, 504) or code in (-32005,)
        except Exception as exc:
            result = {'ok': False, 'error': safe_error(exc)}
            transient = isinstance(exc, (httpx.TimeoutException, httpx.ConnectError))
        if not transient or attempt == retries:
            return result
        await asyncio.sleep(min(8, 0.5 * 2 ** attempt) + random.uniform(0, .25))


async def head(client, url):
    result = await http_rpc(client, url, 'eth_blockNumber', [])
    if not result['ok'] or not isinstance(result['result'], str):
        raise RuntimeError('Validation HTTP head unavailable')
    return int(result['result'], 16)


async def ground_truth(client, url, selected, first, last):
    truth = {k: set() for k in CLASSES}
    report = {'complete': True, 'calls': 0, 'ranges_reduced': 0, 'errors': [], 'filters': {}}
    for kind, query in selected.items():
        current, span, calls = first, 100, 0
        while current <= last and calls < 2000:
            end = min(last, current + span - 1)
            answer = await http_rpc(client, url, 'eth_getLogs', [dict(query, fromBlock=hex(current), toBlock=hex(end))])
            calls += 1; report['calls'] += 1
            if answer['ok'] and isinstance(answer['result'], list):
                try:
                    for log in answer['result']:
                        cls = log_class(log, query)
                        if cls is None:
                            raise ValueError('WrongFilter')
                        truth[cls].add(event_identity(log))
                except (KeyError, TypeError, ValueError) as exc:
                    report['errors'].append({'filter': kind, 'block': current, 'type': type(exc).__name__})
                    break
                current = end + 1
                await asyncio.sleep(.25)
            elif span > 1 and (answer.get('status') in (400, 413) or answer.get('code') in (-32000, -32602)):
                span = max(1, span // 2); report['ranges_reduced'] += 1
            else:
                report['errors'].append({'filter': kind, 'block': current, 'status': answer.get('status'),
                                         'code': answer.get('code'), 'type': (answer.get('error') or {}).get('type')})
                break
        complete = current > last
        report['filters'][kind] = {'complete': complete, 'calls': calls}
        report['complete'] &= complete
    return truth, report


def window_sets(record, first, last):
    return {cls: {key for key, block in record['events'][cls].items() if first <= block <= last}
            for cls in CLASSES}


def compare_sets(public, validation, truth, complete):
    comparisons, recovery = {}, {'publicnode': {}, 'validation': {}}
    for cls in CLASSES:
        p, v, expected = public[cls], validation[cls], truth[cls]
        union = p | v
        comparisons[cls] = {'publicnode_unique': len(p), 'validation_unique': len(v),
                            'intersection': len(p & v), 'publicnode_only': len(p-v),
                            'validation_only': len(v-p),
                            'jaccard': round(len(p & v)/len(union), 6) if union else None}
        for name, actual in (('publicnode', p), ('validation', v)):
            recovery[name][cls] = {'expected': len(expected) if complete else None,
                'received': len(actual), 'missing': len(expected-actual) if complete else None,
                'extra': len(actual-expected) if complete else None,
                'completeness': round(len(actual & expected)/len(expected), 6) if complete and expected else None}
    pa, va = set().union(*public.values()), set().union(*validation.values())
    comparisons['overall'] = {'publicnode_unique': len(pa), 'validation_unique': len(va),
                              'intersection': len(pa & va), 'publicnode_only': len(pa-va),
                              'validation_only': len(va-pa),
                              'jaccard': round(len(pa & va)/len(pa | va), 6) if pa | va else None}
    return comparisons, recovery


def classify(name, record, recovery, truth_complete, curve_observed):
    prefix = 'VALIDATION' if name == 'validation' else 'PUBLICNODE'
    if not truth_complete or not record['connected'] or not record['chain_id_ok'] or record['subscription_ids_count'] == 0:
        return prefix + '_INCONCLUSIVE'
    if record['subscription_ids_count'] != record.get('required_subscriptions', record['subscription_ids_count']):
        return prefix + '_FAIL'
    if record['wrong_filter'] or record['malformed'] or record['unexpected_disconnects'] > record['reconnect_success']:
        return prefix + '_FAIL'
    if any(v['missing'] or v['extra'] for v in recovery.values()):
        return prefix + '_FAIL'
    if not curve_observed:
        return prefix + '_WSS_PASS_CURVE_UNPROVEN'
    return 'VALIDATION_FULL_SECONDARY_PASS' if name == 'validation' else 'PUBLICNODE_WSS_SECONDARY_PASS'


def safe_output(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, 'w') as file:
        json.dump(data, file, indent=2)


async def run(args):
    root = Path(__file__).resolve().parents[1]
    env_path = args.env.resolve()
    if args.output.resolve() != Path('/tmp/provider-benchmark-round3.json'):
        raise ValueError('Round 3 output path is fixed under /tmp')
    if env_path.stat().st_mode & 0o077 or env_path.parent.stat().st_mode & 0o077:
        raise ValueError('Benchmark credentials and parent must be private')
    env = dotenv_values(env_path, interpolate=False)
    validation_http, validation_ws = validation_urls(env)
    with closing(db_ro(args.flow_db)) as db:
        selected = select_filters(db)
    before = snapshot(root, args.main_db, args.flow_db)
    ready = {name: asyncio.Event() for name in ('validation', 'publicnode')}
    start, stop, curve_seen = asyncio.Event(), asyncio.Event(), asyncio.Event()
    urls = {'validation': validation_ws, 'publicnode': PUBLIC['publicnode'][1]}
    tasks = {name: asyncio.create_task(collect(name, urls[name], selected, ready[name], start, stop,
                                                curve_seen, args.max_minutes*60)) for name in urls}
    started_mono = None
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready.values())), 90)
        if any(task.done() for task in tasks.values()):
            raise RuntimeError('WSS subscription setup failed')
        async with httpx.AsyncClient(timeout=25, follow_redirects=False) as client:
            first_head = await head(client, validation_http)
            started_mono, started_utc = time.monotonic(), utc()
            start.set()
            await asyncio.sleep(args.min_minutes*60)
            if not curve_seen.is_set():
                try:
                    await asyncio.wait_for(curve_seen.wait(), (args.max_minutes-args.min_minutes)*60)
                except asyncio.TimeoutError:
                    pass
            ended_mono, ended_utc = time.monotonic(), utc()
            last_head = await head(client, validation_http)
            stop.set()
            live = {name: await task for name, task in tasks.items()}
            first, last = first_head + 1, last_head - 1
            if first > last:
                raise RuntimeError('No stable interior block interval')
            truth, http_report = await ground_truth(client, validation_http, selected, first, last)
    finally:
        stop.set(); start.set()
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    sets = {name: window_sets(record, first, last) for name, record in live.items()}
    comparison, recovery = compare_sets(sets['publicnode'], sets['validation'], truth, http_report['complete'])
    curve_observed = bool(truth['curve_buy'] or truth['curve_sell']) if http_report['complete'] else False
    for name, record in live.items():
        record['required_subscriptions'] = len(selected)
        record['event_counts'] = {cls: len(sets[name][cls]) for cls in CLASSES}
        record['unique_event_count'] = sum(record['event_counts'].values())
        record['bytes_per_unique_event'] = round(record['bytes']/record['unique_event_count'], 2) if record['unique_event_count'] else None
        del record['events']
        del record['_seen_tx_log']
    classifications = {name: classify(name, live[name], recovery[name], http_report['complete'], curve_observed)
                       for name in live}
    overall = ('PROVIDER_SPLIT_READY' if classifications['publicnode'] == 'PUBLICNODE_WSS_SECONDARY_PASS' else
               'VALIDATION_FULL_SECONDARY_READY' if classifications['validation'] == 'VALIDATION_FULL_SECONDARY_PASS' else
               'FAIL' if all(value.endswith('_FAIL') for value in classifications.values()) else
               'MORE_BENCHMARK_REQUIRED')
    after = snapshot(root, args.main_db, args.flow_db)
    services_unchanged = all(before['services'][name] == after['services'][name] for name in before['services'])
    if (before['routing_sha256'] != after['routing_sha256'] or not services_unchanged or
            any(v != 'ok' for v in after['integrity'].values())):
        overall = 'FAIL'
    result = {'round': 3, 'strategy': 'filters frozen before both subscriptions',
              'providers_used': ['publicnode_wss', 'validation_wss', 'validation_http'],
              'endpoint_fingerprints': {'validation_http': fingerprint(validation_http),
                                        'validation_wss': fingerprint(validation_ws)},
              'selected_filters': selected, 'selected_filter_count': len(selected),
              'selected_curve_count': len(selected['curve']['address']),
              'window': {'start_utc': started_utc, 'end_utc': ended_utc,
                         'duration_seconds': round(ended_mono-started_mono, 3),
                         'first_block': first, 'last_block': last},
              'live': live, 'cross_provider': comparison,
              'http_ground_truth': {'report': http_report,
                                    'expected_counts': {cls: len(truth[cls]) for cls in CLASSES}},
              'recovery': recovery, 'curve_proof': 'OBSERVED' if curve_observed else 'NO_CURVE_EVENT_OBSERVED',
              'classifications': classifications, 'overall': overall,
              'incremental_alchemy_requests': 0, 'production_before': before, 'production_after': after,
              'production_routing_changed': before['routing_sha256'] != after['routing_sha256'],
              'production_services_unchanged': services_unchanged}
    safe_output(args.output, result)
    print('ROUND3')
    print('window_seconds=' + str(result['window']['duration_seconds']))
    print('curve_event_observed=' + str(curve_observed).lower())
    print('alchemy_requests=0')
    print('production_routing_changed=' + str(result['production_routing_changed']).lower())
    for name in ('validation', 'publicnode'):
        print(name + ':')
        print('  wss_connected=' + str(live[name]['connected']).lower())
        for cls in CLASSES:
            x = recovery[name][cls]
            print('  ' + cls + ' expected/received/missing/extra=' + '/'.join(str(x[k]) for k in ('expected','received','missing','extra')))
        print('  duplicates=' + str(live[name]['duplicates']))
        print('  reconnects=' + str(live[name]['reconnect_success']))
        print('  classification=' + classifications[name])
    print('validation_http_ground_truth=' + ('PASS' if http_report['complete'] else 'FAIL'))
    print('overall=' + overall)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type=Path, default=Path('/opt/meme-scanner/config/provider-benchmark.env'))
    parser.add_argument('--flow-db', type=Path, default=Path('/opt/meme-scanner/data/flow.db'))
    parser.add_argument('--main-db', type=Path, default=Path('/opt/meme-scanner/data/scanner.db'))
    parser.add_argument('--output', type=Path, default=Path('/tmp/provider-benchmark-round3.json'))
    parser.add_argument('--min-minutes', type=int, default=20)
    parser.add_argument('--max-minutes', type=int, default=60)
    args = parser.parse_args()
    if not 0 < args.min_minutes <= args.max_minutes <= 60:
        parser.error('Require 0 < min-minutes <= max-minutes <= 60')
    asyncio.run(run(args))
