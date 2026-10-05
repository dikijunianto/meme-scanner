"""Resume only frozen failed-switch proof; no service restart or status override."""
if __package__:from scripts import _bootstrap  # noqa: F401
else:import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
import time

from app.config import Config
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders,provider
from app.flow_provider_switch import latest,pending,resume_proved_failure,reconcile_zero_filter_failure,value
from app.flow_shadow import make_reconciler,verified_checkout
from app.flow_worker import FlowSettings
from app.flow_identity import canonical_address


async def recover(identity,handoff=False,conservative_later_head=False):
    # Native advisory lock serializes operator invocations, not the live worker.
    import fcntl
    import tempfile
    from pathlib import Path
    with (Path(tempfile.gettempdir())/'meme-scanner-switch-recovery.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        return await _recover(identity,handoff,conservative_later_head)


async def _recover(identity,handoff=False,conservative_later_head=False):
    from scripts.phase2b2_shadow import service
    revision=verified_checkout()
    services={u:service(u) for u in ('meme-scanner','meme-scanner-flow')}
    if any(s['ActiveState']!='active' for s in services.values()):raise ValueError('Services must be active')
    config=Config.load();settings=FlowSettings.load();providers=FlowProviders.load()
    if not settings.split_enabled or provider(providers.http)!='validation':raise ValueError('Validation split required')
    db=FlowDB(settings.database)
    row=db.conn.execute('SELECT * FROM flow_provider_switches WHERE id=?',(identity,)).fetchone()
    if not row or row['state']!='FAILED':raise ValueError('Failed switch required')
    item=value(row);stage=f'provider_switch:{identity}'
    if (latest(db,row['session_id'])['id']!=identity or pending(db,row['session_id']) or
        db.state('connection_state')!='connected' or db.state('current_wss_provider')!='validation' or
        not 0<=time.time()-float(db.state('heartbeat',0))<=65):
        raise ValueError('Fresh matching failed-switch connection required')
    if not item['filters']:
        try:
            import sqlite3
            main=sqlite3.connect(f'file:{config.database}?mode=ro',uri=True)
            try:
                if any(x!='ok' for x in (db.conn.execute('PRAGMA integrity_check').fetchone()[0],
                                        main.execute('PRAGMA integrity_check').fetchone()[0])):
                    raise ValueError('Database integrity failed')
            finally:main.close()
            if not handoff:raise ValueError('Explicit handoff required for zero-filter reconciliation')
            if services!={u:service(u) for u in services}:raise ValueError('Service identity changed')
            result=reconcile_zero_filter_failure(db,identity,revision)
            return {'revision':revision,'services':services,'zero_filter_reconciliation':result,
                    'rpc_calls':0,'worker_runtime_untouched':True}
        finally:db.conn.close()
    runner,old=make_reconciler(config,settings,db,providers)
    try:
        await old.close()
        if any(x!='ok' for x in (db.conn.execute('PRAGMA integrity_check').fetchone()[0],
                                runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0])):
            raise ValueError('Database integrity failed')
        # Filters remain separate even when their block ranges overlap.
        for f in item['filters']:
            target=db.target(f['launch_id'])
            launch=runner.worker.main.execute('SELECT * FROM launches WHERE id=?',(f['launch_id'],)).fetchone()
            if (not target or not launch or f['kind']!='curve' or target['graduation_json'] or
                target['launch_block']!=f['base'] or canonical_address(target['curve_address'])!=canonical_address(launch['curve_address'])):
                raise ValueError('Immutable curve identity mismatch')
        before=runner.summary(stage)
        if conservative_later_head:
            from app.flow_switch_recovery import recover as conservative_recover
            return await conservative_recover(runner,identity,revision,handoff,
                unchanged=lambda: services=={u:service(u) for u in services})
        day=int(time.time())//86400*86400
        budget_before={k:db.used(k,day) for k in ('flow_eth_getLogs','flow_rpc_members')}
        complete=await runner.run_stage(stage)
        after=runner.summary(stage)
        if complete and handoff:
            if services!={u:service(u) for u in services}:raise ValueError('Service identity changed')
            resume_proved_failure(db,identity,revision)
        return {'revision':revision,'services':services,'before':before,'after':after,
                'budget_before':budget_before,'budget_after':{k:db.used(k,day) for k in budget_before},
                'worker_handoff_requested':complete and handoff}
    finally:
        await runner.worker.rpc.close();runner.worker.main.close();db.conn.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--switch-id',type=int,required=True)
    p.add_argument('--handoff',action='store_true')
    p.add_argument('--conservative-later-head',action='store_true');a=p.parse_args()
    try:print(json.dumps(asyncio.run(recover(a.switch_id,a.handoff,a.conservative_later_head)),indent=2))
    except Exception as exc:
        print(json.dumps({'gate':'PROVIDER_SWITCH_RECOVERY_BLOCKED','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
