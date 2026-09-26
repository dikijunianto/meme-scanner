"""Proof-backed cleanup of named gaps from an expired, failed-cutover target."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
import subprocess
import time

from app.config import Config
from app.flow_cutover import PENDING, session as cutover_session
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders
from app.flow_shadow import CUTOVER_RESERVE, SHADOW_SPAN, make_reconciler, verified_checkout
from app.flow_worker import FlowSettings
from app.rpc import RpcError


async def cleanup(ids, head):
    revision=verified_checkout()
    settings=FlowSettings.load()
    if settings.split_enabled:raise RpcError('Legacy flow route required')
    for unit in ('meme-scanner.service','meme-scanner-flow.service'):
        state=subprocess.check_output(('systemctl','is-active',unit),text=True).strip()
        if state!='active':raise RpcError('Production service is not active')
    db=FlowDB(settings.database)
    runner,old_rpc=make_reconciler(Config.load(),settings,db,FlowProviders.load())
    try:
        await old_rpc.close()
        session=cutover_session(db)
        if session and session['state'] in PENDING:raise RpcError('Active cutover session')
        if runner.meta('git_revision')!='40c715a5ad4f7ab0d0cf92124787fed45157e16f':
            raise RpcError('Failed-cutover ledger identity mismatch')
        if runner.meta('H_stop')!='73211568' or set(ids)!={238,239,240}:
            raise RpcError('Unexpected failed-cutover proof identity')
        gaps=[db.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(i,)).fetchone() for i in ids]
        if any(not g or g['launch_id']!=254657 for g in gaps):
            raise RpcError('Named gap state changed')
        if all(g['resolved'] for g in gaps):
            return {'gate':'GAPS_ALREADY_RECONCILED','revision':revision,'resolved':ids}
        first=int(runner.meta('H_stop'))+1
        if not first<=head<=first+SHADOW_SPAN-1:raise RpcError('Cleanup head exceeds one bounded range')
        day=int(time.time())//86400*86400
        remaining=settings.daily_getlogs-db.used('flow_eth_getLogs',day)
        if remaining<runner.worker.rpc.config.retry_attempts+CUTOVER_RESERVE:
            return {'gate':'HANDOFF_FIX_GAPS_PENDING','remaining_getlogs':remaining,'needed_attempts':3}
        header=await runner.worker.rpc.call('eth_getBlockByNumber',[hex(head),False])
        if int(header['number'],16)!=head:raise RpcError('Cleanup header identity mismatch')
        head_at=int(header['timestamp'],16)
        if any(g['end_at']>head_at for g in gaps):
            raise RpcError('Cleanup head predates a named gap end')
        runner.add_jobs('failed_cutover_cleanup_tail',first,head,{254657})
        complete=await runner.run_stage('failed_cutover_cleanup_tail')
        if not complete:
            return {'gate':'HANDOFF_FIX_GAPS_PENDING',**runner.summary('failed_cutover_cleanup_tail')}
        resolved=runner.resolve_failed_cutover_gaps(ids,head,head_at)
        return {'gate':'GAPS_PROOF_RECONCILED','revision':revision,'head':head,
                'head_at':head_at,'resolved':resolved,
                **runner.summary('failed_cutover_cleanup_tail'),
                'flow_integrity':db.conn.execute('PRAGMA integrity_check').fetchone()[0],
                'main_integrity':runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0]}
    finally:
        await runner.worker.rpc.close()
        runner.worker.main.close()
        db.conn.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--gap-ids',required=True)
    parser.add_argument('--head',type=int,required=True)
    args=parser.parse_args()
    try:print(json.dumps(asyncio.run(cleanup([int(x) for x in args.gap_ids.split(',')],args.head)),indent=2))
    except Exception as exc:
        print(json.dumps({'gate':'HANDOFF_FIX_GAPS_PENDING','error_type':type(exc).__name__}))
        raise SystemExit(1) from None
