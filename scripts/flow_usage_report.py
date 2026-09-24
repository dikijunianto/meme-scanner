import _bootstrap  # noqa: F401
import argparse
import json
from app.flow_data import FlowDB
from app.flow_worker import FlowSettings
from app.flow_providers import FlowProviders
from app.flow_reports import usage

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--hours',type=float,default=24);a=p.parse_args()
    settings=FlowSettings.load()
    print(json.dumps(usage(FlowDB(settings.database,readonly=True),settings,a.hours,providers=FlowProviders.load()),indent=2))
