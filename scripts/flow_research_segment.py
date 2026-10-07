"""Read-only status by default; opt-in request handled by the existing live writer."""
if __package__:from scripts import _bootstrap  # noqa: F401
else:import _bootstrap  # noqa: F401
import argparse
import json
from app.flow_data import FlowDB
from app.flow_segments import record,historical_debt
from app.flow_shadow import verified_checkout
from app.flow_worker import FlowSettings


def request(db,segment_id,expected_pid,epoch_id,revision):
    import re
    from scripts.phase2b2_shadow import service
    flow=service('meme-scanner-flow');main=service('meme-scanner')
    if (flow['ActiveState']!='active' or main['ActiveState']!='active' or int(flow['MainPID'])!=expected_pid or
        expected_pid<=0 or db.epoch()['epoch_id']!=epoch_id or record(db) or
        not re.fullmatch('[A-Za-z0-9_-]+',segment_id)):raise ValueError('Exact active service/epoch and new segment required')
    value={'segment_id':segment_id,'epoch_id':epoch_id,'flow_pid':expected_pid,'source_revision':revision,
           'reason':'research-clean collection after retained pre-clean incident debt','status':'REQUESTED'}
    with db.catalog_conn:
        db.catalog_conn.execute('BEGIN IMMEDIATE')
        prior=db.catalog_conn.execute("SELECT value FROM flow_state WHERE key='research_segment_request'").fetchone()
        if prior and json.loads(prior[0])!=value:raise ValueError('Existing request must be inspected, never overwritten')
        db.catalog_conn.execute('INSERT OR IGNORE INTO flow_state VALUES(?,?)',('research_segment_request',json.dumps(value,sort_keys=True)))
    return value


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-segment');parser.add_argument('--expected-flow-pid',type=int)
    parser.add_argument('--expected-epoch')
    args=parser.parse_args();db=FlowDB(FlowSettings.load().database,readonly=not bool(args.request_segment))
    try:
        if args.request_segment:
            if not args.expected_flow_pid or not args.expected_epoch:parser.error('Exact expected flow PID and epoch required')
            result=request(db,args.request_segment,args.expected_flow_pid,args.expected_epoch,verified_checkout())
        else:
            if args.expected_flow_pid or args.expected_epoch:parser.error('Expected identities require an explicit segment request')
            db.conn.execute('BEGIN')
            result={'epoch':db.epoch(),'research_segment':record(db),'preclean_debt':historical_debt(db),
                    'research_clean_start':None if not record(db) or record(db)['status']!='VALIDATED' else
                      {'segment_id':record(db)['segment_id'],'start_at':record(db)['start_at'],'start_block':record(db)['start_block']}}
        print(json.dumps(result,indent=2))
    finally:db.close()
