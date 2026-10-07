"""Read-only inventory by default; explicit verified-boundary enrollment only."""
if __package__:from scripts import _bootstrap  # noqa: F401
else:import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
from pathlib import Path

from app.config import Config
from app.flow_data import FlowDB
from app.flow_expired_recovery import inventory,retain_verified_bounds,CLASSES
from app.flow_providers import FlowProviders,provider
from app.flow_shadow import make_reconciler,verified_checkout
from app.flow_worker import FlowSettings


async def enroll(gap_id,upper_block,expected_epoch):
    """Future authorized operation: verify ONLY two explicitly named headers."""
    import fcntl
    from pathlib import Path
    with Path('/tmp/meme-scanner-expired-boundary.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        verified_checkout();settings=FlowSettings.load();providers=FlowProviders.load()
        if not settings.split_enabled or provider(providers.http)!='validation':raise ValueError('Validation split required')
        db=FlowDB(settings.database)
        runner=None
        try:
            epoch=db.epoch()
            if not epoch or epoch['epoch_id']!=expected_epoch or epoch['status']!='ACTIVE':raise ValueError('Active epoch identity mismatch')
            gap=dict(db.conn.execute('SELECT * FROM flow_gaps WHERE id=?',(gap_id,)).fetchone() or {})
            target=db.target(gap.get('launch_id'))
            from app.flow_expired_recovery import expired
            if not target or not expired(target) or gap.get('resolved') or gap.get('reason') not in CLASSES:
                raise ValueError('Existing expired supported current gap required')
            if type(gap.get('first_block')) is not int or gap['first_block']<=0:raise ValueError('Exact retained lower block required before RPC')
            if type(upper_block) is not int or upper_block<gap['first_block']:raise ValueError('Explicit retained upper block required')
            if db.state(f'expired_gap_bounds:{gap_id}'):
                from app.flow_expired_recovery import bounds
                retained=bounds(db,gap)
                if int(retained['upper_header']['number'],16)!=upper_block:raise ValueError('Forensic bounds are immutable')
                return retained
            runner,old=make_reconciler(Config.load(),settings,db,providers);await old.close()
            first=await runner.worker.rpc.call('eth_getBlockByNumber',[hex(upper_block),False])
            second=await runner.worker.rpc.call('eth_getBlockByNumber',[hex(upper_block+1),False])
            if int(first['number'],16)!=upper_block or int(second['number'],16)!=upper_block+1:raise ValueError('Header number mismatch')
            if db.epoch()['epoch_id']!=expected_epoch:raise ValueError('Epoch changed during boundary validation')
            return retain_verified_bounds(db,gap_id,first,second)
        finally:
            if runner:await runner.worker.rpc.close();runner.worker.main.close()
            db.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--retain-verified-boundary',type=int,metavar='GAP_ID')
    parser.add_argument('--upper-block',type=int)
    parser.add_argument('--expected-epoch')
    parser.add_argument('--database',type=Path,help='Explicit read-only catalog for offline inventory')
    args=parser.parse_args()
    if args.retain_verified_boundary is not None:
        if args.database:parser.error('Database override is read-only inventory only')
        if args.upper_block is None or not args.expected_epoch:parser.error('Explicit upper-block and expected-epoch required')
        print(json.dumps(asyncio.run(enroll(args.retain_verified_boundary,args.upper_block,args.expected_epoch)),indent=2))
    else:
        if args.upper_block is not None or args.expected_epoch:parser.error('Enrollment flags require explicit retain-verified-boundary')
        db=FlowDB(args.database or FlowSettings.load().database,readonly=True)
        try:
            db.conn.execute('BEGIN')
            print(json.dumps(inventory(db),indent=2))
        finally:db.close()
