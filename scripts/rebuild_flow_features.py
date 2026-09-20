"""Recompute derived rows locally; never opens RPC or alters raw events."""
import _bootstrap  # noqa: F401
import argparse
import json
import time
from app.flow_data import FlowDB
from app.flow_worker import FlowSettings

if __name__=='__main__':
    p=argparse.ArgumentParser();g=p.add_mutually_exclusive_group(required=True)
    g.add_argument('--token');g.add_argument('--days',type=float);a=p.parse_args()
    if a.days is not None and a.days<=0:p.error('Days must be positive')
    db=FlowDB(FlowSettings.load().database)
    rows=db.conn.execute('SELECT * FROM flow_tracking_targets WHERE '+('lower(token_address)=lower(?)' if a.token else 'tracking_start_at>=?'),
                         (a.token if a.token else time.time()-a.days*86400,)).fetchall()
    for row in rows:db.rebuild(dict(row))
    print(json.dumps({'rebuilt_targets':len(rows),'rpc_calls':0,'raw_events_modified':0}))
