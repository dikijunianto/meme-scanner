"""Dynamic, synchronized curve WSS proof; production databases are read-only."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import re
import time

import httpx
from dotenv import dotenv_values
from websockets.asyncio.client import connect

from app.flow_data import BUY, SELL, SWAP, HOOK
from provider_benchmark import PUBLIC
import provider_benchmark_round3 as r3

CURVE_QUERY = {'topics': [[BUY, SELL]]}
CLASSES = r3.CLASSES
REASONS = ('recent_activity', 'new_active_target', 'recent_launch', 'fallback')
ADDRESS = re.compile(r'^0x[0-9a-fA-F]{40}$')
HTTP_SPAN = 2000
POLL_SECONDS = 7


def timestamp(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def curve_query(address):
    return {'address': address, **CURVE_QUERY}


def discover(flow_db, main_db, now):
    """Rank real local curves only; never query an RPC during discovery."""
    candidates = {}
    recent = now - 300
    with closing(r3.db_ro(flow_db)) as flow:
        rows = flow.execute('''SELECT t.launch_id,t.curve_address,t.created_at,t.tracking_start_at,
                t.tracking_end_at,max(e.observed_at) AS recent_event
            FROM flow_tracking_targets t LEFT JOIN flow_events e
              ON e.launch_id=t.launch_id AND e.phase='curve' AND e.observed_at>=?
            WHERE t.graduation_json IS NULL AND t.tracking_end_at>?
              AND t.status NOT IN ('completed','partial')
            GROUP BY t.launch_id''', (recent, now)).fetchall()
    for launch_id, address, created, started, end, event_at in rows:
        if not address or not ADDRESS.fullmatch(address):
            continue
        reason = ('recent_activity' if event_at is not None else
                  'new_active_target' if created >= recent else 'fallback')
        score = (4-REASONS.index(reason), event_at or created or started)
        candidates[address.lower()] = {'curve_address': address.lower(), 'launch_id': launch_id,
            'reason': reason, 'score': score, 'tracking_end_at': end}
    with closing(r3.db_ro(main_db)) as main:
        rows = main.execute('''SELECT l.id,l.curve_address,l.block_number FROM launches l
            WHERE l.launch_type='pons-v2' AND l.curve_address IS NOT NULL
              AND l.block_timestamp>=?
              AND NOT EXISTS(SELECT 1 FROM graduations g WHERE g.token_address=l.token_address)
            ORDER BY l.block_number DESC LIMIT 500''', (timestamp(now-900),)).fetchall()
    for launch_id, address, block in rows:
        if not address or not ADDRESS.fullmatch(address):
            continue
        key = address.lower()
        if key not in candidates:
            candidates[key] = {'curve_address': key, 'launch_id': launch_id,
                'reason': 'recent_launch', 'score': (2, block), 'tracking_end_at': None}
    return sorted(candidates.values(), key=lambda row: row['score'], reverse=True)


def control_filters(flow_db, now):
    with closing(r3.db_ro(flow_db)) as flow:
        row = flow.execute('''SELECT graduation_json,tracking_end_at,status
            FROM flow_tracking_targets WHERE graduation_json IS NOT NULL
            ORDER BY (tracking_end_at>? AND status NOT IN ('completed','partial')) DESC,
                     tracking_start_at DESC LIMIT 1''', (now,)).fetchone()
    if not row:
        return None
    g = json.loads(row[0])
    return {'v4': {'address': g['pool_manager_address'], 'topics': [SWAP, g['pool_id']]},
            'hook': {'address': g['hooks'], 'topics': [HOOK, g['pool_id']]},
            'currently_tracked': bool(row[1] > now and row[2] not in ('completed', 'partial'))}


def curve_segments(entries, last):
    """Only blocks covered after both ACKs and before both unsubscriptions."""
    eligible = [e for e in entries if e.get('start_block') is not None and e['start_block'] <= last]
    if not eligible:
        return []
    boundaries = {e['start_block'] for e in eligible}
    boundaries.update(min(last+1, e.get('end_block') or last) + 1 for e in eligible)
    boundaries.add(last+1)
    points = sorted(x for x in boundaries if x <= last+1)
    segments = []
    for first, following in zip(points, points[1:]):
        end = following - 1
        if first > end:
            continue
        addresses = sorted({e['curve_address'] for e in eligible
                            if e['start_block'] <= first <= min(last, e.get('end_block') or last)})
        if not addresses:
            continue
        if segments and segments[-1]['last'] + 1 == first and segments[-1]['addresses'] == addresses:
            segments[-1]['last'] = end
        else:
            segments.append({'first': first, 'last': end, 'addresses': addresses})
    return segments


def eligible_sets(record, entries, control, last):
    ranges = {}
    for entry in entries:
        if entry.get('start_block') is not None:
            ranges.setdefault(entry['curve_address'], []).append(
                (entry['start_block'], min(last, entry.get('end_block') or last)))
    result = {cls: set() for cls in CLASSES}
    for cls, logs in record['events'].items():
        for key, (block, address) in logs.items():
            if cls.startswith('curve_'):
                if any(first <= block <= end for first, end in ranges.get(address, [])):
                    result[cls].add(key)
            elif control and control.get('start_block') is not None:
                if control['start_block'] <= block <= min(last, control.get('end_block') or last):
                    result[cls].add(key)
    return result


class CountedHTTP:
    def __init__(self, client):
        self.client = client
        self.getlogs_calls = 0
        self.head_calls = 0

    async def post(self, url, json):
        if json['method'] == 'eth_getLogs':
            self.getlogs_calls += 1
        elif json['method'] == 'eth_blockNumber':
            self.head_calls += 1
        return await self.client.post(url, json=json)


async def truth_query(client, url, entries, control, last):
    truth = {cls: set() for cls in CLASSES}
    report = {'complete': True, 'getlogs_calls': 0, 'successful_queries': 0,
              'failed_queries': 0, 'retries': 0, 'range_reductions': 0,
              'queried_blocks': 0, 'queried_dynamic_segments': 0, 'errors': []}
    segments = curve_segments(entries, last)
    report['queried_dynamic_segments'] = len(segments)
    plans = [(segment['first'], segment['last'],
              {'address': segment['addresses'], 'topics': [[BUY, SELL]]}, 'curve')
             for segment in segments]
    if control and control.get('start_block') is not None and control['start_block'] <= last:
        pool = control['filters']
        plans.append((control['start_block'], min(last, control.get('end_block') or last),
            {'address': [pool['v4']['address'], pool['hook']['address']],
             'topics': [[SWAP, HOOK], pool['v4']['topics'][1]]}, 'control'))
    before_calls = client.getlogs_calls
    span = HTTP_SPAN
    for first, final, query, kind in plans:
        current = first
        while current <= final:
            end = min(final, current+span-1)
            answer = await r3.http_rpc(client, url, 'eth_getLogs',
                [dict(query, fromBlock=hex(current), toBlock=hex(end))])
            if answer['ok'] and isinstance(answer.get('result'), list):
                report['successful_queries'] += 1
                report['queried_blocks'] += end-current+1
                try:
                    for log in answer['result']:
                        if kind == 'curve':
                            cls = r3.log_class(log, query)
                            if cls not in ('curve_buy', 'curve_sell'):
                                raise ValueError('Unexpected curve log')
                        else:
                            cls = next((name for name in ('v4', 'hook')
                                        if r3.log_class(log, control['filters'][name]) == name), None)
                            if cls is None:
                                continue  # OR-address/topic query may include unrelated combinations.
                        truth[cls].add(r3.event_identity(log))
                except (KeyError, TypeError, ValueError) as exc:
                    report['errors'].append({'kind': kind, 'block': current, 'type': type(exc).__name__})
                    report['complete'] = False
                    break
                current = end+1
                await asyncio.sleep(.25)
            elif span > 1 and (answer.get('status') in (400, 413) or answer.get('code') in (-32000, -32602)):
                span = max(1, span//2)
                report['range_reductions'] += 1
            else:
                report['failed_queries'] += 1
                report['errors'].append({'kind': kind, 'block': current, 'status': answer.get('status'),
                                         'code': answer.get('code'),
                                         'type': (answer.get('error') or {}).get('type')})
                report['complete'] = False
                break
        if not report['complete']:
            break
    report['getlogs_calls'] = client.getlogs_calls-before_calls
    report['retries'] = max(0, report['getlogs_calls'] - report['successful_queries'] -
                            report['failed_queries'] - report['range_reductions'])
    return truth, report


def compare(truth, live, complete):
    result = {}
    for cls in CLASSES:
        expected, actual = truth[cls], live[cls]
        missing, extra = expected-actual, actual-expected
        result[cls] = {'expected_http': len(expected) if complete else None,
            'received_wss': len(actual), 'intersection': len(expected & actual) if complete else None,
            'missing': len(missing) if complete else None, 'extra': len(extra) if complete else None,
            'completeness_ratio': round(len(expected & actual)/len(expected), 6) if complete and expected else None,
            'status': ('INCONCLUSIVE' if not complete else 'FAIL' if missing or extra else
                       'UNPROVEN_NO_EVENTS' if not expected else 'PASS')}
    for label, classes in (('curve_total', ('curve_buy', 'curve_sell')),
                           ('overall', CLASSES)):
        expected = set().union(*(truth[cls] for cls in classes))
        actual = set().union(*(live[cls] for cls in classes))
        result[label] = {'expected_http': len(expected) if complete else None,
            'received_wss': len(actual), 'intersection': len(expected & actual) if complete else None,
            'missing': len(expected-actual) if complete else None,
            'extra': len(actual-expected) if complete else None,
            'completeness_ratio': round(len(expected & actual)/len(expected), 6) if complete and expected else None}
    return result


def classify(name, record, comparison, truth_complete, truth, sync_failures):
    prefix = 'VALIDATION' if name == 'validation' else 'PUBLICNODE'
    if not truth_complete or not record['connected'] or not record['chain_id_ok']:
        return prefix + '_INCONCLUSIVE'
    if (sync_failures or record['wrong_filter'] or record['malformed'] or
            record['unexpected_disconnects'] or record['reconnect_failures']):
        return prefix + '_FAIL'
    if any(comparison[cls]['status'] == 'FAIL' for cls in CLASSES):
        return prefix + '_FAIL'
    buy, sell = len(truth['curve_buy']), len(truth['curve_sell'])
    if buy and sell and buy+sell >= 5:
        return 'VALIDATION_FULL_SECONDARY_PASS' if name == 'validation' else 'PUBLICNODE_WSS_SECONDARY_PASS'
    if buy and not sell:
        return prefix + '_WSS_CURVE_BUY_ONLY'
    if sell and not buy:
        return prefix + '_WSS_CURVE_SELL_ONLY'
    return prefix + '_WSS_CURVE_UNPROVEN'


class WSSClient:
    def __init__(self, name, url):
        self.name, self.url = name, url
        self.ws = None
        self.reader_task = None
        self.pending = {}
        self.sequence = 0
        self.routes = {}
        self.unknown = {}
        self.retired = set()
        self.subscriptions = {}  # stable local key -> (query, provider subscription ID)
        self.closing = False
        self.record = r3.new_record()
        self.record['reconnect_failures'] = 0

    def notification(self, message, route):
        record = self.record
        record['notification_count'] += 1
        try:
            kind, query, address = route
            log = message['params']['result']
            if not isinstance(log, dict):
                raise TypeError('Non-object log')
            cls = r3.log_class(log, query)
            if cls is None or (kind == 'curve' and cls not in ('curve_buy', 'curve_sell')) or (
                    kind != 'curve' and cls != kind):
                record['wrong_filter'] += 1
                return
            key = r3.event_identity(log)
            block = int(log['blockNumber'], 16) if isinstance(log['blockNumber'], str) else int(log['blockNumber'])
            if log.get('removed'):
                record['removed'] += 1
                record['events'][cls].pop(key, None)
                return
            pair = (key[1], key[2])
            if pair in record['_seen_tx_log']:
                record['duplicates'] += 1
            record['_seen_tx_log'].add(pair)
            record['events'][cls][key] = (block, address)
            record['first_block'] = block if record['first_block'] is None else min(block, record['first_block'])
            record['last_block'] = block if record['last_block'] is None else max(block, record['last_block'])
        except (KeyError, ValueError, TypeError, IndexError):
            record['malformed'] += 1

    async def reader(self):
        try:
            while True:
                raw = await self.ws.recv()
                self.record['bytes'] += len(raw if isinstance(raw, bytes) else raw.encode())
                try:
                    message = json.loads(raw)
                except (ValueError, TypeError):
                    self.record['malformed'] += 1
                    continue
                if not isinstance(message, dict):
                    self.record['malformed'] += 1
                    continue
                request_id = message.get('id')
                if request_id in self.pending:
                    future = self.pending[request_id]
                    if not future.done():
                        future.set_result(message)
                elif message.get('method') == 'eth_subscription':
                    params = message.get('params') or {}
                    sub = params.get('subscription') if isinstance(params, dict) else None
                    if sub in self.routes:
                        self.notification(message, self.routes[sub])
                    elif sub not in self.retired:
                        waiting = self.unknown.setdefault(sub, [])
                        if len(waiting) < 32:
                            waiting.append(message)
                        else:
                            self.record['wrong_filter'] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.closing:
                self.record['unexpected_disconnects'] += 1
                self.record['errors'].append(r3.safe_error(exc))
                for future in self.pending.values():
                    if not future.done():
                        future.set_exception(RuntimeError('WSS disconnected'))
                self.ws = None

    async def call(self, method, params):
        if self.ws is None:
            raise RuntimeError('WSS unavailable')
        self.sequence += 1
        request_id = self.sequence
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            await self.ws.send(json.dumps({'jsonrpc': '2.0', 'id': request_id,
                                           'method': method, 'params': params}))
            return await asyncio.wait_for(future, 20)
        finally:
            self.pending.pop(request_id, None)

    async def open(self):
        self.ws = await connect(self.url, open_timeout=20, ping_interval=20,
                                ping_timeout=20, max_size=262144, max_queue=8, compression=None)
        self.reader_task = asyncio.create_task(self.reader())
        answer = await self.call('eth_chainId', [])
        self.record['connected'] = True
        self.record['chain_id_ok'] = answer.get('result') == r3.CHAIN_ID
        if not self.record['chain_id_ok']:
            raise RuntimeError('Wrong chain ID')

    async def subscribe(self, key, kind, query, address):
        answer = await self.call('eth_subscribe', ['logs', query])
        sub = answer.get('result')
        if not isinstance(sub, str):
            error = answer.get('error')
            self.record['errors'].append({'type': 'SubscriptionRejected',
                'code': error.get('code') if isinstance(error, dict) else None})
            return None
        self.subscriptions[key] = (kind, query, address, sub)
        self.routes[sub] = (kind, query, address)
        self.record['subscription_ids_count'] = max(self.record['subscription_ids_count'],
                                                    len(self.subscriptions))
        for message in self.unknown.pop(sub, []):
            self.notification(message, self.routes[sub])
        return r3.utc()

    async def unsubscribe(self, key):
        item = self.subscriptions.get(key)
        if not item:
            return True
        sub = item[3]
        answer = await self.call('eth_unsubscribe', [sub])
        if answer.get('result') is not True:
            return False
        self.subscriptions.pop(key, None)
        self.routes.pop(sub, None)
        self.retired.add(sub)
        return True

    async def reconnect(self):
        if self.ws is not None or self.closing:
            return
        old = list(self.subscriptions.items())
        for attempt in range(3):
            self.record['reconnect_attempts'] += 1
            started = time.monotonic()
            await asyncio.sleep(min(8, 2 ** attempt + random.random()))
            try:
                self.routes.clear(); self.unknown.clear(); self.retired.clear()
                self.subscriptions.clear()
                await self.open()
                for key, (kind, query, address, _) in old:
                    if not await self.subscribe(key, kind, query, address):
                        raise RuntimeError('Reconnect subscription rejected')
                self.record['reconnect_success'] += 1
                self.record['reconnect_seconds'].append(round(time.monotonic()-started, 3))
                return
            except Exception as exc:
                self.record['errors'].append(r3.safe_error(exc))
                self.record['reconnect_failures'] += 1
                if self.ws is not None:
                    await self.ws.close()
                self.ws = None

    async def close(self):
        if self.closing:
            return
        self.closing = True
        if self.ws is not None:
            await self.ws.close()
        if self.reader_task:
            self.reader_task.cancel()
            await asyncio.gather(self.reader_task, return_exceptions=True)
        self.record['end_monotonic'], self.record['end_utc'] = time.monotonic(), r3.utc()
        self.record['unrouted_notifications'] = sum(map(len, self.unknown.values()))
        self.record['wrong_filter'] += self.record['unrouted_notifications']


async def add_curves(candidates, clients, http, http_url, active, entries, failures):
    provisional = []
    for candidate in candidates:
        address = candidate['curve_address']
        key = 'curve:' + address
        query = curve_query(address)
        responses = await asyncio.gather(*(client.subscribe(key, 'curve', query, address)
                                           for client in clients.values()), return_exceptions=True)
        acks = dict(zip(clients, responses))
        if all(isinstance(value, str) for value in responses):
            provisional.append((candidate, acks))
        else:
            failures.append({'curve_address': address, 'launch_id': candidate['launch_id'],
                             'providers': [name for name, value in acks.items() if not isinstance(value, str)]})
            await asyncio.gather(*(client.unsubscribe(key) for name, client in clients.items()
                                   if isinstance(acks[name], str)), return_exceptions=True)
    if provisional:
        try:
            start_block = await r3.head(http, http_url) + 1
        except Exception as exc:
            failures.extend({'curve_address': candidate['curve_address'],
                             'launch_id': candidate['launch_id'],
                             'providers': list(clients), 'action': 'activation_head',
                             'error_type': type(exc).__name__} for candidate, _ in provisional)
            await asyncio.gather(*(client.unsubscribe('curve:' + candidate['curve_address'])
                                   for candidate, _ in provisional for client in clients.values()),
                                 return_exceptions=True)
            return 0
        activated_at = r3.utc()
        for candidate, acks in provisional:
            entry = {k: candidate[k] for k in ('curve_address', 'launch_id', 'reason')}
            entry.update({'discovered_utc': candidate['discovered_utc'],
                          'validation_ack_utc': acks['validation'], 'publicnode_ack_utc': acks['publicnode'],
                          'start_block': start_block, 'activation_utc': activated_at,
                          'end_block': None, 'deactivation_utc': None})
            entries.append(entry)
            active[entry['curve_address']] = entry
    return len(provisional)


async def remove_curves(addresses, clients, http, http_url, active, failures):
    if not addresses:
        return True
    end_block = await r3.head(http, http_url) - 1
    removed_at = r3.utc()
    safe = True
    for address in addresses:
        entry = active[address]
        entry['end_block'], entry['deactivation_utc'] = end_block, removed_at
        key = 'curve:' + address
        responses = await asyncio.gather(*(client.unsubscribe(key) for client in clients.values()),
                                         return_exceptions=True)
        if not all(value is True for value in responses):
            failures.append({'curve_address': address, 'launch_id': entry['launch_id'],
                             'providers': [name for name, value in zip(clients, responses) if value is not True],
                             'action': 'unsubscribe'})
            safe = False
        active.pop(address)
    return safe


async def add_control(filters, clients, http, http_url, failures):
    if not filters:
        return None
    acks = {}
    for kind in ('v4', 'hook'):
        query = filters[kind]
        responses = await asyncio.gather(*(client.subscribe(kind, kind, query, query['address'].lower())
                                           for client in clients.values()), return_exceptions=True)
        acks[kind] = dict(zip(clients, responses))
        if not all(isinstance(value, str) for value in responses):
            failures.append({'control': kind, 'providers': [name for name, value in acks[kind].items()
                                                          if not isinstance(value, str)]})
            await asyncio.gather(*(client.unsubscribe(key) for key in ('v4', 'hook')
                                   for client in clients.values()), return_exceptions=True)
            return None
    try:
        first = await r3.head(http, http_url) + 1
    except Exception as exc:
        failures.append({'control': 'both', 'providers': list(clients),
                         'action': 'activation_head', 'error_type': type(exc).__name__})
        await asyncio.gather(*(client.unsubscribe(key) for key in ('v4', 'hook')
                               for client in clients.values()), return_exceptions=True)
        return None
    return {'filters': {kind: filters[kind] for kind in ('v4', 'hook')},
            'currently_tracked': filters['currently_tracked'],
            'start_block': first, 'end_block': None, 'activation_utc': r3.utc(), 'acks': acks}


def live_counts(clients):
    return {name: {cls: len(client.record['events'][cls]) for cls in CLASSES}
            for name, client in clients.items()}


def curve_target_met(truth):
    buy, sell = len(truth['curve_buy']), len(truth['curve_sell'])
    return buy >= 1 and sell >= 1 and buy + sell >= 5


def summarize_live(client, eligible):
    record = client.record
    record['event_counts'] = {cls: len(eligible[cls]) for cls in CLASSES}
    record['unique_event_count'] = sum(record['event_counts'].values())
    record['bytes_per_unique_event'] = round(record['bytes']/record['unique_event_count'], 2) if record['unique_event_count'] else None
    del record['events']
    del record['_seen_tx_log']
    return record


async def run(args):
    root = Path(__file__).resolve().parents[1]
    if args.output.resolve() != Path('/tmp/provider-benchmark-round4.json'):
        raise ValueError('Round 4 output path is fixed under /tmp')
    env_path = args.env.resolve()
    if env_path.stat().st_mode & 0o077 or env_path.parent.stat().st_mode & 0o077:
        raise ValueError('Benchmark credential file and parent must be private')
    validation_http, validation_ws = r3.validation_urls(dotenv_values(env_path, interpolate=False))
    before = r3.snapshot(root, args.main_db, args.flow_db)
    clients = {'validation': WSSClient('validation', validation_ws),
               'publicnode': WSSClient('publicnode', PUBLIC['publicnode'][1])}
    entries, active, seen, sync_failures = [], {}, {}, []
    removed_at = {}
    control = None
    max_simultaneous = 0
    preliminary_calls = 0
    precheck_after = 0
    next_rotation = 0
    fatal_subscriptions = False
    started_mono = ended_mono = None
    last = None
    try:
        await asyncio.gather(*(client.open() for client in clients.values()))
        started_mono, started_utc = time.monotonic(), r3.utc()
        for client in clients.values():
            client.record['start_monotonic'] = started_mono
            client.record['start_utc'] = started_utc
        deadline = started_mono + args.max_minutes*60
        async with httpx.AsyncClient(timeout=30, follow_redirects=False) as raw_http:
            http = CountedHTTP(raw_http)
            initial = discover(args.flow_db, args.main_db, time.time())
            for candidate in initial:
                seen.setdefault(candidate['curve_address'], r3.utc())
                candidate['discovered_utc'] = seen[candidate['curve_address']]
            await add_curves(initial[:min(8, args.max_curves)], clients, http, validation_http,
                             active, entries, sync_failures)
            control = await add_control(control_filters(args.flow_db, time.time()),
                                        clients, http, validation_http, sync_failures)
            max_simultaneous = len(active)
            next_poll = started_mono + POLL_SECONDS
            while time.monotonic() < deadline - 2:
                now_mono = time.monotonic()
                if now_mono < next_poll:
                    await asyncio.sleep(min(next_poll-now_mono, deadline-2-now_mono))
                    continue
                next_poll = now_mono + POLL_SECONDS
                for client in clients.values():
                    if client.ws is None:
                        await client.reconnect()
                candidates = discover(args.flow_db, args.main_db, time.time())
                for candidate in candidates:
                    seen.setdefault(candidate['curve_address'], r3.utc())
                    candidate['discovered_utc'] = seen[candidate['curve_address']]
                desired = candidates[:args.max_curves]
                desired_addresses = {c['curve_address'] for c in desired}
                if (not fatal_subscriptions and all(client.ws is not None for client in clients.values())
                        and (len(active) < args.max_curves or now_mono >= next_rotation)):
                    stale = [address for address in active if address not in desired_addresses][:4]
                    if stale:
                        safe = await remove_curves(stale, clients, http, validation_http, active, sync_failures)
                        removed_at.update({address: time.monotonic() for address in stale})
                        fatal_subscriptions |= not safe
                    slots = args.max_curves-len(active)
                    fresh = [c for c in desired if c['curve_address'] not in active and
                             time.monotonic()-removed_at.get(c['curve_address'], -1e9) >= 60][:min(4, slots)]
                    if fresh and not fatal_subscriptions:
                        await add_curves(fresh, clients, http, validation_http,
                                         active, entries, sync_failures)
                    max_simultaneous = max(max_simultaneous, len(active))
                    if len(active) >= args.max_curves:
                        next_rotation = time.monotonic() + 30
                if (now_mono-started_mono >= args.min_minutes*60 and
                        now_mono >= precheck_after and
                        all(x['curve_buy'] and x['curve_sell'] and
                            x['curve_buy']+x['curve_sell'] >= 5 for x in live_counts(clients).values())):
                    try:
                        probe_last = await r3.head(http, validation_http)-1
                        provisional, report = await asyncio.wait_for(
                            truth_query(http, validation_http, entries, control, probe_last),
                            timeout=max(1, deadline-time.monotonic()-3))
                        preliminary_calls += report['getlogs_calls']
                        if report['complete'] and curve_target_met(provisional):
                            last = probe_last
                            break
                    except (RuntimeError, asyncio.TimeoutError):
                        pass
                    precheck_after = time.monotonic()+300
            if last is None:
                # Fetch before the hard deadline; the final 1-2 seconds are excluded
                # rather than claiming coverage beyond either live subscription.
                last = await r3.head(http, validation_http)-1
                remaining = deadline-time.monotonic()
                if remaining > 0:
                    await asyncio.sleep(remaining)
            ended_mono, ended_utc = time.monotonic(), r3.utc()
            await asyncio.gather(*(client.close() for client in clients.values()))
            for entry in active.values():
                entry['end_block'] = last
                entry['deactivation_utc'] = ended_utc
            if control:
                control['end_block'] = last
            truth, http_report = await truth_query(http, validation_http, entries, control, last)
            http_report['preliminary_getlogs_calls'] = preliminary_calls
            http_report['total_getlogs_calls'] = http.getlogs_calls
            http_report['total_head_calls'] = http.head_calls
    finally:
        await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)
    eligible = {name: eligible_sets(client.record, entries, control, last)
                for name, client in clients.items()}
    comparison = {name: compare(truth, eligible[name], http_report['complete']) for name in clients}
    live = {name: summarize_live(client, eligible[name]) for name, client in clients.items()}
    failed_by_provider = {name: sum(name in failure.get('providers', []) for failure in sync_failures)
                          for name in clients}
    classes = {name: classify(name, live[name], comparison[name], http_report['complete'],
                              truth, failed_by_provider[name]) for name in clients}
    if classes['publicnode'] == 'PUBLICNODE_WSS_SECONDARY_PASS':
        overall = 'PROVIDER_SPLIT_READY'
    elif classes['validation'] == 'VALIDATION_FULL_SECONDARY_PASS':
        overall = 'VALIDATION_FULL_SECONDARY_READY'
    elif all(value.endswith('_FAIL') for value in classes.values()):
        overall = 'FAIL'
    elif truth['curve_buy'] or truth['curve_sell']:
        overall = 'PARTIAL_CURVE_PROOF'
    else:
        overall = 'MORE_BENCHMARK_REQUIRED'
    after = r3.snapshot(root, args.main_db, args.flow_db)
    services_unchanged = before['services'] == after['services']
    routing_changed = before['routing_sha256'] != after['routing_sha256']
    if routing_changed or not services_unchanged or any(x != 'ok' for x in after['integrity'].values()):
        overall = 'FAIL'
    reason_counts = Counter(entry['reason'] for entry in entries)
    result = {'round': 4, 'providers_used': ['validation_wss', 'publicnode_wss', 'validation_http'],
        'endpoint_fingerprints': {'validation_http': r3.fingerprint(validation_http),
                                  'validation_wss': r3.fingerprint(validation_ws),
                                  'publicnode_wss': r3.fingerprint(PUBLIC['publicnode'][1])},
        'window': {'start_utc': started_utc, 'end_utc': ended_utc,
                   'duration_seconds': round(ended_mono-started_mono, 3),
                   'verified_last_block': last},
        'activation_rule': 'Validation HTTP head + 1 after both WSS ACKs; end head - 1 before unsubscription',
        'dynamic_curves_discovered': len(seen),
        'dynamic_curves_subscribed': len({e['curve_address'] for e in entries}),
        'dynamic_subscription_intervals': len(entries), 'max_simultaneous_curves': max_simultaneous,
        'reason_counts': {reason: reason_counts[reason] for reason in REASONS},
        'targets': entries, 'sync_failures': sync_failures,
        'control': control, 'live': live,
        'validation_http': {'report': http_report,
                            'expected_counts': {cls: len(truth[cls]) for cls in CLASSES}},
        'comparison': comparison, 'classifications': classes, 'overall': overall,
        'incremental_alchemy_requests': 0, 'production_before': before,
        'production_after': after, 'production_routing_changed': routing_changed,
        'production_services_unchanged': services_unchanged}
    r3.safe_output(args.output, result)
    print('ROUND4')
    summary = {'window_seconds': result['window']['duration_seconds'],
        'dynamic_curves_discovered': result['dynamic_curves_discovered'],
        'dynamic_curves_subscribed': result['dynamic_curves_subscribed'],
        'curve_buy_expected': len(truth['curve_buy']), 'curve_sell_expected': len(truth['curve_sell']),
        'curve_total_expected': len(truth['curve_buy'])+len(truth['curve_sell']),
        'validation_curve_received': comparison['validation']['curve_total']['received_wss'],
        'publicnode_curve_received': comparison['publicnode']['curve_total']['received_wss'],
        'validation_curve_missing': comparison['validation']['curve_total']['missing'],
        'publicnode_curve_missing': comparison['publicnode']['curve_total']['missing'],
        'validation_http_getlogs_calls': http_report['total_getlogs_calls'],
        'alchemy_requests': 0, 'production_routing_changed': str(routing_changed).lower(),
        'validation_classification': classes['validation'],
        'publicnode_classification': classes['publicnode'], 'overall': overall}
    summary.update({reason: reason_counts[reason] for reason in REASONS})
    for key, value in summary.items():
        print(f'{key}={value}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type=Path, default=Path('/opt/meme-scanner/config/provider-benchmark.env'))
    parser.add_argument('--flow-db', type=Path, default=Path('/opt/meme-scanner/data/flow.db'))
    parser.add_argument('--main-db', type=Path, default=Path('/opt/meme-scanner/data/scanner.db'))
    parser.add_argument('--min-minutes', type=int, default=20)
    parser.add_argument('--max-minutes', type=int, default=60)
    parser.add_argument('--max-curves', type=int, default=32)
    parser.add_argument('--output', type=Path, default=Path('/tmp/provider-benchmark-round4.json'))
    arguments = parser.parse_args()
    if not 0 < arguments.min_minutes <= arguments.max_minutes <= 60 or not 1 <= arguments.max_curves <= 32:
        parser.error('Require 0 < min <= max <= 60 minutes and 1 <= max-curves <= 32')
    try:
        asyncio.run(run(arguments))
    except Exception as exc:
        print(json.dumps({'round': 'ROUND4', 'status': 'error', 'error_type': type(exc).__name__}))
        raise SystemExit(1) from None
