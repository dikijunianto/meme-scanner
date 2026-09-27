"""Online proof bootstrap; never stops a service or changes provider routing."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
import re
import subprocess

from app.config import Config
from app.flow_bootstrap import CursorBootstrap
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders
from app.flow_shadow import make_reconciler, verified_checkout
from app.flow_worker import FlowSettings
from app.rpc import RpcError


def service_state():
    result={}
    for name in ('meme-scanner.service','meme-scanner-flow.service'):
        raw=subprocess.check_output(('systemctl','show',name,'-p','ActiveState','-p','MainPID','-p','NRestarts'),text=True)
        state=dict(line.split('=',1) for line in raw.splitlines())
        if state['ActiveState']!='active' or int(state['MainPID'])<=0:
            raise RpcError('Production service not active')
        result[name]=state
    return result


async def run(mode, stage, include_expired):
    revision=verified_checkout()
    before=service_state()
    config=Config.load();settings=FlowSettings.load()
    if not settings.enabled or settings.split_enabled:raise RpcError('Legacy flow route required')
    db=FlowDB(settings.database);db.migrate()
    if db.state('current_wss_provider')!='alchemy':raise RpcError('Legacy Alchemy WSS is not active')
    runner,old_rpc=make_reconciler(config,settings,db,FlowProviders.load())
    try:
        await old_rpc.close()
        active=[r[0] for r in db.conn.execute(
            "SELECT launch_id FROM flow_tracking_targets WHERE status NOT IN ('completed','partial') ORDER BY launch_id")]
        ids=sorted(set(active+include_expired)) if mode=='bootstrap' else active
        bootstrap=CursorBootstrap(runner)
        if mode=='preflight':result={'gate':'RESTART_PREFLIGHT',**await bootstrap.restart_plan()}
        else:
            if not ids:raise RpcError('No targets for proof stage')
            result=await bootstrap.run(stage,ids)
        after=service_state()
        if before!=after:raise RpcError('Production service changed during bootstrap')
        return {'revision':revision,'services':after,'active_at_start':active,**result,
                'integrity':{'flow':db.conn.execute('PRAGMA integrity_check').fetchone()[0],
                             'main':runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]}}
    finally:
        await runner.worker.rpc.close()
        runner.worker.main.close()
        db.conn.close()


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=('bootstrap','tail','preflight'),required=True)
    p.add_argument('--stage',default='cursor_bootstrap')
    p.add_argument('--include-expired-ids',default='')
    a=p.parse_args()
    if (a.mode in ('bootstrap','preflight') and a.stage!='cursor_bootstrap') or (a.mode=='tail' and
        (not re.fullmatch(r'cursor_tail_[1-9][0-9]*',a.stage) or a.include_expired_ids)):
        p.error('Invalid stage or target selection')
    try:print(json.dumps(asyncio.run(run(a.mode,a.stage,[int(x) for x in a.include_expired_ids.split(',') if x])),indent=2))
    except Exception as exc:
        print(json.dumps({'gate':'MISSING_CURSOR_BOOTSTRAP_PENDING','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
