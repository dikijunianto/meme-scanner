"""Explicit future offline rollover. Never called during source deployment."""
if __package__:from scripts import _bootstrap  # noqa: F401
else:import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
from app.config import Config
from app.flow_data import FlowDB
from app.flow_epochs import quarantine,prepare
from app.flow_lock import flow_writer_lock
from app.flow_providers import FlowProviders
from app.flow_shadow import make_reconciler,verified_checkout
from app.flow_worker import FlowSettings


async def execute(args):
    from scripts.phase2b2_shadow import service
    from app.flow_cutover import source_process_gone
    revision=verified_checkout();settings=FlowSettings.load()
    main=service('meme-scanner');flow=service('meme-scanner-flow')
    if args.old_flow_pid<=0 or main['ActiveState']!='active' or flow['ActiveState']!='inactive' or not source_process_gone(args.old_flow_pid):
        raise ValueError('Main active and flow fully stopped required')
    with flow_writer_lock(settings.database):
        db=FlowDB(settings.database,follow_epoch=False)
        runner=old=None
        try:
            quarantine(db,epoch_id='GENERALIZED_BOOTSTRAP_EPOCH_1',start_block=78006186,
                start_at=1790921343,revision='e9616961209a9e8fe780dadd26145fd9d488a1ad',
                incident_at=1790955170.6994033,switch_id=17,estimated_calls=2514,
                reason='historical_collection_epoch_closed')
            runner,old=make_reconciler(Config.load(),settings,db,FlowProviders.load())
            await old.close()
            epoch=await prepare(runner,epoch_id=args.epoch_id,revision=revision,
                reason='fresh generalized bootstrap collection after historical proof interruption',
                predecessor='GENERALIZED_BOOTSTRAP_EPOCH_1')
            if service('meme-scanner')!=main or service('meme-scanner-flow')!=flow:
                raise ValueError('Service identity changed; keep flow stopped and inspect')
            return {'epoch':epoch,'architecture':'SEALED_POSTSTART_BOOTSTRAP','pit_eligible':False,'next':'start flow once; validate canonical discovery, ACKs, explicit bootstrap and tail before PIT'}
        finally:
            if runner:
                await runner.worker.rpc.close();runner.worker.main.close()
            if old:await old.close()
            db.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--old-flow-pid',required=True,type=int)
    p.add_argument('--epoch-id',required=True)
    args=p.parse_args()
    print(json.dumps(asyncio.run(execute(args)),indent=2))


if __name__=='__main__':main()
