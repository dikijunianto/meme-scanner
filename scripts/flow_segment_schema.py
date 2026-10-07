"""Read-only schema status; explicit additive repair requires a stopped flow writer."""
if __package__:from scripts import _bootstrap  # noqa: F401
else:import _bootstrap  # noqa: F401
import argparse
import json
import sqlite3
from pathlib import Path
from app.flow_data import FlowDB
from app.flow_lock import flow_writer_lock
from app.flow_partition_schema import VERSION,missing,repair
from app.flow_segments import record
from app.flow_shadow import verified_checkout
from app.flow_worker import FlowSettings
from scripts.phase2b2_shadow import service


def repair_existing(database,segment_id,epoch_id,start_block,start_at):
    flow=service('meme-scanner-flow')
    if flow['ActiveState']!='inactive' or int(flow['MainPID'])!=0:
        raise ValueError('FLOW_STOP_REQUIRED_FOR_SCHEMA_REPAIR')
    revision=verified_checkout()
    with flow_writer_lock(database):
        db=FlowDB(database,readonly=True)
        try:
            db.catalog_conn.execute('BEGIN')
            before=record(db)
            if (not before or before['segment_id']!=segment_id or before['epoch_id']!=epoch_id or
                before['start_block']!=start_block or before['start_at']!=start_at):
                raise ValueError('Exact existing segment identity and boundary required')
            path=Path(before['db_path']).resolve()
            conn=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=5)
            try:result=repair(conn)
            finally:conn.close()
            if record(db)!=before:raise ValueError('Segment identity changed')
            return {**result,'segment_id':segment_id,'epoch_id':epoch_id,'start_block':start_block,
                    'start_at':start_at,'source_revision':revision,'research_clean_start':None}
        finally:db.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repair-existing-segment')
    parser.add_argument('--expected-epoch')
    parser.add_argument('--expected-start-block',type=int)
    parser.add_argument('--expected-start-at',type=float)
    args=parser.parse_args();database=FlowSettings.load().database
    if args.repair_existing_segment:
        if any(x is None for x in (args.expected_epoch,args.expected_start_block,args.expected_start_at)):
            parser.error('Exact expected epoch, start block and timestamp required')
        result=repair_existing(database,args.repair_existing_segment,args.expected_epoch,
                               args.expected_start_block,args.expected_start_at)
    else:
        if any(x is not None for x in (args.expected_epoch,args.expected_start_block,args.expected_start_at)):
            parser.error('Expected identities require explicit repair')
        db=FlowDB(database,readonly=True)
        try:
            db.conn.execute('BEGIN')
            result={'segment':record(db),'contract_version':VERSION,'missing':missing(db.conn),
                    'repair_requirement':'FLOW_STOP_REQUIRED_FOR_SCHEMA_REPAIR'}
        finally:db.close()
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
