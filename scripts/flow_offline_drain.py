"""Explicit offline maintenance. Never invokes a service start/stop/restart."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import subprocess
import time

from app.config import Config, ROOT
from app.flow_config import checked_file, readable_as_service, no_network_probe
from app.flow_data import FlowDB
from app.flow_lock import flow_writer_lock
from app.flow_offline_drain import OfflineAbort, OfflineDrain, OfflineRpc, GuardedConnection, dry_plan
from app.flow_providers import FlowProviders
from app.flow_shadow import verified_checkout
from app.flow_worker import FlowSettings, FlowWorker, FlowBudget


def services():
    result = {}
    for name in ('meme-scanner.service', 'meme-scanner-flow.service'):
        raw = subprocess.check_output(['systemctl', 'show', name, '-p', 'ActiveState',
                                       '-p', 'SubState', '-p', 'MainPID', '-p', 'NRestarts'], text=True)
        result[name] = dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
    return result


def offline_guard(source_pid, baseline=None):
    current = services()
    main, flow = current['meme-scanner.service'], current['meme-scanner-flow.service']
    if main['ActiveState'] != 'active' or int(main['MainPID']) <= 0:
        raise OfflineAbort('Main service is not active')
    if flow['ActiveState'] != 'inactive' or flow['SubState'] != 'dead' or int(flow['MainPID']) != 0:
        raise OfflineAbort('Flow service must be inactive/dead with no main process')
    if source_pid <= 0 or Path('/proc', str(source_pid)).exists():
        raise OfflineAbort('Externally stopped source flow process still exists')
    if baseline and current != baseline:
        raise OfflineAbort('Service identity changed during maintenance')
    return current


def database_preflight(db, main, settings):
    if any(conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' for conn in (db.conn, main)):
        raise OfflineAbort('Database integrity failed')
    if db.conn.execute("SELECT 1 FROM flow_provider_switches WHERE state!='HEALTHY' LIMIT 1").fetchone():
        raise OfflineAbort('Provider switch pending, failed or unresolved')
    if any(db.used(metric,0) for metric in ('flow_eth_getTransactionByHash', 'flow_eth_getTransactionReceipt')):
        raise OfflineAbort('Transaction/receipt counters must be zero')
    now = int(time.time()); day, minute = now//86400*86400, now//60*60
    if (db.used('flow_eth_getLogs',day) > settings.daily_getlogs-50 or
        db.used('flow_rpc_members',day)+3 > settings.daily_calls or
        db.used('flow_rpc_members',minute)+3 > settings.minute_calls):
        raise FlowBudget('offline_preflight_budget')


async def run(args):
    if not args.acknowledge_flow_offline:
        raise OfflineAbort('Explicit offline maintenance acknowledgement required')
    import pwd
    if os.geteuid() != pwd.getpwnam('ubuntu').pw_uid:
        raise OfflineAbort('Run maintenance as the ubuntu service user')
    revision = verified_checkout()
    if revision != args.expected_revision:
        raise OfflineAbort('Expected source revision does not match')
    before = offline_guard(args.source_flow_pid)
    for filename, env in (('.env','SCANNER_ENV'), ('flow.env','FLOW_ENV'), ('flow-rpc.env','FLOW_RPC_ENV')):
        path = ROOT/'config'/filename
        if env in os.environ and Path(os.environ[env]).resolve() != path.resolve():
            raise OfflineAbort('Protected config override forbidden')
        checked_file(path, path.parent)
        if not readable_as_service(path, 'ubuntu'):
            raise OfflineAbort('Protected config is not ubuntu-readable')
    await no_network_probe(require_split=True)
    config, settings, providers = Config.load(), FlowSettings.load(), FlowProviders.load()
    if not settings.enabled or not settings.split_enabled or config.chain_id != 4663:
        raise OfflineAbort('Production split flow configuration required')
    if settings.daily_getlogs>400 or settings.daily_calls>1000 or settings.minute_calls>12:
        raise OfflineAbort('Maintenance budget ceilings exceeded')
    db = FlowDB(settings.database, readonly=True)
    worker = FlowWorker(config, settings, db, providers)
    try:
        database_preflight(db, worker.main, settings)
        offline_guard(args.source_flow_pid, before)
        if args.dry_run:
            result = dry_plan(worker)
            offline_guard(args.source_flow_pid, before)
            return {'revision': revision, 'services': before, **result}
    finally:
        await worker.rpc.close(); worker.main.close(); db.conn.close()
    with flow_writer_lock(settings.database):
        guard = lambda: offline_guard(args.source_flow_pid, before)
        guard()
        db = FlowDB(settings.database)
        db.conn = GuardedConnection(db.conn, guard)
        worker = FlowWorker(config, settings, db, providers)
        old_rpc = worker.rpc
        # All local gates passed. Only accounting may write before chain verification;
        # no targets, cursors, gaps, proof or manifest are changed by this preflight RPC.
        preflight_rpc = OfflineRpc(config, settings, db, providers)
        preflight_rpc.guard = guard
        try:
            database_preflight(db, worker.main, settings)
            await preflight_rpc.check_chain()
            guard()
            runner = OfflineDrain(worker, guard, args.generation, revision, args.source_flow_pid)
            result = await runner.run()
            guard()
            return {'revision': revision, 'services': services(), **result}
        finally:
            await worker.rpc.close(); await preflight_rpc.close(); await old_rpc.close()
            worker.main.close(); db.conn.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--acknowledge-flow-offline', action='store_true')
    p.add_argument('--expected-revision', required=True)
    p.add_argument('--source-flow-pid', required=True, type=int)
    p.add_argument('--generation', required=True)
    p.add_argument('--dry-run', action='store_true')
    return p


def main():
    p = parser(); args = p.parse_args()
    if not re.fullmatch('[0-9a-f]{40}', args.expected_revision) or not re.fullmatch('[a-zA-Z0-9_-]{1,64}', args.generation):
        p.error('Full expected revision and safe generation ID required')
    try:
        print(json.dumps(asyncio.run(run(args)), indent=2))
    except FlowBudget:
        print(json.dumps({'gate': 'OFFLINE_DRAIN_BUDGET_PENDING', 'restart_safe_now': False}))
        raise SystemExit(2) from None
    except Exception as exc:
        # Never serialize provider error bodies, endpoint URLs or credential paths.
        print(json.dumps({'gate': 'OFFLINE_DRAIN_IMPLEMENTATION_BLOCKED',
                          'restart_safe_now': False, 'error_type': type(exc).__name__}))
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
