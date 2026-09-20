import _bootstrap  # noqa: F401
import argparse
import json
from app.config import Config
from app.flow_data import FlowDB
from app.flow_worker import FlowSettings
from app.flow_reports import readonly, outcome

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--days',type=float,default=7)
    p.add_argument('--feature-window',type=int,default=300);p.add_argument('--outcome-horizon',type=int,default=3600)
    p.add_argument('--ticker');p.add_argument('--quote-address');a=p.parse_args()
    try:
        result=outcome(FlowDB(FlowSettings.load().database,readonly=True),readonly(Config.load().database),a.days,
                       a.feature_window,a.outcome_horizon,a.ticker,a.quote_address)
    except ValueError as exc:p.error(str(exc))
    print(json.dumps(result,indent=2))
