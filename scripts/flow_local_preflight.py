"""Stopped-writer, read-only real local startup path; no provider operations."""
if __package__:from scripts import _bootstrap
else:import _bootstrap
import argparse,asyncio,json,socket
from contextlib import ExitStack
from unittest.mock import patch
from app.config import Config
from app.flow_worker import FlowSettings,local_preflight
from app.flow_providers import FlowProviders
from app.flow_data import FlowDB
from app.flow_lock import flow_writer_lock
from scripts.phase2b2_shadow import service


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--read-only-live',action='store_true',help='Validate a live read snapshot and private-copy writers; no stopped-writer lock claim')
    args=parser.parse_args()
    flow=service('meme-scanner-flow')
    if not args.read_only_live and (flow['ActiveState']!='inactive' or int(flow['MainPID'])!=0):raise ValueError('FLOW_STOP_REQUIRED_FOR_LOCAL_PREFLIGHT')
    settings=FlowSettings.load();config=Config.load();providers=FlowProviders.load()
    if not settings.enabled:raise ValueError('Flow disabled; not a startup proof')
    from contextlib import nullcontext
    with nullcontext() if args.read_only_live else flow_writer_lock(settings.database):
        db=FlowDB(settings.database,readonly=True)
        try:
            db.conn.execute('BEGIN');db.catalog_conn.execute('BEGIN')
            attempts=[]
            def deny(*args,**kwargs):
                attempts.append(True);raise RuntimeError('Network forbidden in local preflight')
            with ExitStack() as guard:
                for name in ('socket.socket.connect','socket.socket.connect_ex','socket.create_connection','socket.getaddrinfo'):
                    guard.enter_context(patch(name,side_effect=deny))
                result=asyncio.run(local_preflight(config,settings,db,providers))
            if attempts:raise RuntimeError('Local preflight attempted network')
            result['external_attempts']=len(attempts)
            print(json.dumps({**result,'gate':'PRODUCTION_LOCAL_STARTUP_PREFLIGHT_PASS',
                             'snapshot_only':args.read_only_live,'stopped_writer_lock_proven':not args.read_only_live},indent=2))
        finally:db.close()


if __name__=='__main__':main()
