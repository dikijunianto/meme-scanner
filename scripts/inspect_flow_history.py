import _bootstrap  # noqa: F401
import argparse
import json
from app.flow_data import FlowDB
from app.flow_worker import FlowSettings
from app.flow_reports import inspect

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('token');a=p.parse_args()
    print(json.dumps(inspect(FlowDB(FlowSettings.load().database,readonly=True),a.token),indent=2))
