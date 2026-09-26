"""Read-only, event-triggered production validation for the running flow worker."""
import _bootstrap  # noqa: F401

import argparse
from datetime import datetime, timezone
import json
import re
import sqlite3
import subprocess
import time

from app.config import Config, ROOT
from app.flow_worker import FlowSettings


METRICS = ('flow_rpc_members', 'flow_eth_getLogs', 'flow_recovery_blocks',
           'flow_budget_rejections', 'flow_http_calls_alchemy',
           'flow_eth_getTransactionByHash', 'flow_eth_getTransactionReceipt')
WAIT = re.compile(r'budget_scope=(\w+) used=(\d+) limit=(\d+) retry_after_seconds=(\d+)')


def utc(at):
    return datetime.fromtimestamp(at, timezone.utc).isoformat()


def readonly(path):
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


def service(name):
    result = subprocess.run(['systemctl', 'show', name, '-p', 'MainPID', '-p',
                             'NRestarts', '-p', 'ActiveState'], capture_output=True,
                            text=True, check=True, timeout=10)
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def new_targets(flow, main, since):
    rows = flow.execute('SELECT launch_id,token_address,curve_address,cohort_initial,'
                        'cohort_long,created_at,current_phase,launch_block,status '
                        'FROM flow_tracking_targets WHERE created_at>=? ORDER BY created_at', (since,))
    result = []
    for row in rows:
        flags = {r[0] for r in main.execute(
            'SELECT sampling_group FROM outcome_targets WHERE launch_id=?', (row['launch_id'],))}
        if row['cohort_initial'] != 1 or 'random_initial' not in flags:
            continue
        if bool(row['cohort_long']) != ('random_long' in flags):
            continue
        item = dict(row)
        item['created_utc'] = utc(item.pop('created_at'))
        result.append(item)
    return result


def metric_totals(flow, now):
    totals = dict(flow.execute('SELECT metric,sum(count) FROM flow_usage GROUP BY metric'))
    day = int(now) // 86400 * 86400
    minute = int(now) // 60 * 60
    return {'total': {k: totals.get(k, 0) for k in METRICS},
            'day_rpc': flow.execute('SELECT coalesce(sum(count),0) FROM flow_usage '
                                    'WHERE metric=? AND minute>=?', ('flow_rpc_members', day)).fetchone()[0],
            'day_getlogs': flow.execute('SELECT coalesce(sum(count),0) FROM flow_usage '
                                        'WHERE metric=? AND minute>=?', ('flow_eth_getLogs', day)).fetchone()[0],
            'minute_rpc': flow.execute('SELECT coalesce(sum(count),0) FROM flow_usage '
                                      'WHERE metric=? AND minute=?', ('flow_rpc_members', minute)).fetchone()[0]}


def snapshot(flow, main, settings, tracked=()):
    now = time.time()
    state = dict(flow.execute('SELECT key,value FROM flow_state WHERE key IN '
                              "('service_status','current_wss_provider','recovery_rejection_scope','budget_pause_reason')"))
    counts = dict(flow.execute('SELECT reason,count(*) FROM flow_gaps WHERE resolved=0 GROUP BY reason'))
    targets = {}
    for launch_id in tracked:
        row = flow.execute('SELECT status,current_phase FROM flow_tracking_targets WHERE launch_id=?',
                           (launch_id,)).fetchone()
        if not row:
            continue
        cursors = {r['key'].rsplit(':', 1)[-1]: int(r['value']) for r in flow.execute(
            'SELECT key,value FROM flow_state WHERE key LIKE ?', (f'recovery:{launch_id}:%',))}
        gaps = [dict(r) for r in flow.execute(
            'SELECT id,reason,first_block,resolved FROM flow_gaps WHERE launch_id=? ORDER BY id',
            (launch_id,))]
        targets[launch_id] = {'status': row['status'], 'phase': row['current_phase'],
                              'events': flow.execute('SELECT count(*) FROM flow_events WHERE launch_id=?',
                                                     (launch_id,)).fetchone()[0],
                              'features': flow.execute('SELECT count(*) FROM flow_features WHERE launch_id=?',
                                                       (launch_id,)).fetchone()[0],
                              'cursors': cursors, 'gaps': gaps}
    return {'at_utc': utc(now), 'events': flow.execute('SELECT count(*) FROM flow_events').fetchone()[0],
            'features': flow.execute('SELECT count(*) FROM flow_features').fetchone()[0],
            'unresolved_gaps': sum(counts.values()), 'gap_reasons': counts,
            'active_targets': flow.execute("SELECT count(*) FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')").fetchone()[0],
            'targets': targets, 'budget': metric_totals(flow, now),
            'limits': {'minute_rpc': settings.minute_calls, 'day_rpc': settings.daily_calls,
                       'day_getlogs': settings.daily_getlogs},
            'state': state, 'main': service('meme-scanner.service'),
            'flow': service('meme-scanner-flow.service'),
            'main_launches': main.execute('SELECT count(*) FROM launches').fetchone()[0],
            'phase2a_snapshots': main.execute('SELECT count(*) FROM market_snapshots').fetchone()[0]}


def journal_evidence(since):
    stamp = datetime.fromtimestamp(since, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    result = subprocess.run(['journalctl', '-u', 'meme-scanner-flow.service', '--since', stamp,
                             '--no-pager', '-o', 'json'], capture_output=True, text=True,
                            check=True, timeout=30)
    counts = {'disconnects': 0, 'legacy_budget_loops': 0, 'credential_urls': 0,
              'tracebacks': 0}
    waits = []
    for line in result.stdout.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        message = str(record.get('MESSAGE', ''))
        at = int(record.get('__REALTIME_TIMESTAMP', 0)) / 1_000_000
        counts['disconnects'] += 'Flow disconnected' in message
        counts['legacy_budget_loops'] += 'connected_http_budget' in message
        counts['credential_urls'] += bool(re.search(r'https?://|wss?://', message))
        counts['tracebacks'] += 'Traceback' in message
        match = WAIT.search(message)
        if match:
            scope, used, limit, seconds = match.groups()
            waits.append({'at_utc': utc(at), 'scope': scope, 'used': int(used),
                          'limit': int(limit), 'retry_after_seconds': int(seconds),
                          'reset_at_estimate_utc': utc(at + int(seconds))})
    return {'counts': counts, 'temporary_waits': waits}


def emit(kind, **fields):
    print(json.dumps({'kind': kind, **fields}, separators=(',', ':')), flush=True)


def integrity(flow, scanner):
    return {'main': scanner.execute('PRAGMA integrity_check').fetchone()[0],
            'flow': flow.execute('PRAGMA integrity_check').fetchone()[0]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-wait-hours', type=float, default=6)
    parser.add_argument('--validation-minutes', type=float, default=30)
    parser.add_argument('--poll-seconds', type=float, default=30)
    args = parser.parse_args()
    if not 0 < args.max_wait_hours <= 6 or args.validation_minutes < 30 or not 1 <= args.poll_seconds <= 60:
        parser.error('Unsafe watcher duration or polling interval')
    settings = FlowSettings.load()
    if not settings.enabled or settings.split_enabled:
        parser.error('Expected active legacy flow route')
    started = time.time()
    flow, scanner = readonly(settings.database), readonly(Config.load().database)
    baseline = snapshot(flow, scanner, settings)
    if baseline['flow']['ActiveState'] != 'active' or baseline['state'].get('current_wss_provider') != 'alchemy':
        parser.error('Expected active Alchemy flow process')
    revision = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT,
                              capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    emit('baseline', watcher_start_utc=utc(started), source_revision=revision, snapshot=baseline)
    trigger = None
    tracked = set()
    while True:
        now = time.time()
        found = new_targets(flow, scanner, started)
        for target in found:
            if target['launch_id'] not in tracked:
                tracked.add(target['launch_id'])
                if trigger is None:
                    trigger = now
                emit('target', detected_utc=utc(now), target=target)
        current = snapshot(flow, scanner, settings, sorted(tracked))
        observed_journal = journal_evidence(started) if trigger is not None else None
        emit('sample', validation_start_utc=utc(trigger) if trigger else None, snapshot=current,
             journal_counts=observed_journal['counts'] if observed_journal else None,
             latest_budget_wait=observed_journal['temporary_waits'][-1]
             if observed_journal and observed_journal['temporary_waits'] else None)
        if trigger is None and now - started >= args.max_wait_hours * 3600:
            emit('final', gate='RECOVERY_FIX_BLOCKED_NO_LIVE_WORK', watcher_start_utc=utc(started),
                 watcher_end_utc=utc(now), baseline=baseline, final=current,
                 journal=journal_evidence(started), integrity=integrity(flow, scanner))
            break
        if trigger is not None and now - trigger >= args.validation_minutes * 60:
            emit('final', gate='OPERATOR_REVIEW', watcher_start_utc=utc(started),
                 validation_start_utc=utc(trigger), validation_end_utc=utc(now),
                 targets=found, baseline=baseline, final=current,
                 journal=journal_evidence(started), integrity=integrity(flow, scanner))
            break
        time.sleep(args.poll_seconds)


if __name__ == '__main__':
    main()
