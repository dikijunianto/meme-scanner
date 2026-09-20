"""Bounded, local-only resource/ingestion benchmark. No RPC or service changes."""
import _bootstrap  # noqa: F401
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from app.config import Config
from app.flow_data import FlowDB, iso
from app.flow_worker import FlowSettings
from app.flow_reports import readonly, usage


def snapshot(main,flow):
    services={}
    for name in ('meme-scanner','meme-scanner-flow'):
        out=subprocess.check_output(['systemctl','show',name,'-p','MainPID','-p','NRestarts','-p','MemoryCurrent','-p','CPUUsageNSec','-p','ActiveState'],text=True)
        values=dict(line.split('=',1) for line in out.splitlines())
        for key in ('MainPID','NRestarts','MemoryCurrent','CPUUsageNSec'):values[key]=int(values[key]) if values[key].isdigit() else None
        if values['MainPID']:
            try:values['rss_bytes']=int(Path('/proc') .joinpath(str(values['MainPID']),'statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
            except (OSError,IndexError):values['rss_bytes']=None
        services[name]=values
    return {'at':time.time(),'services':services,
            'main_counts':{table:main.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in ('launches','graduations','market_snapshots','outcome_targets')},
            'flow_targets':flow.conn.execute('SELECT count(*) FROM flow_tracking_targets').fetchone()[0],
            'flow_raw_rows':flow.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],
            'flow_state':flow.state('service_status'),'flow_heartbeat':flow.state('heartbeat'),
            'flow_db_allocated_bytes':sum(p.stat().st_size for p in (flow.path,Path(str(flow.path)+'-wal')) if p.exists())}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--minutes',type=float,default=30);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if not 30<=a.minutes<=60:p.error('Live benchmark must last 30–60 minutes')
    os.umask(0o077)
    settings=FlowSettings.load();main=readonly(Config.load().database);flow=FlowDB(settings.database,readonly=True)
    started=time.time();deadline=time.monotonic()+a.minutes*60;samples=[]
    while True:
        samples.append(snapshot(main,flow))
        result={'started_at':iso(started),'duration_seconds':time.time()-started,'complete':time.monotonic()>=deadline,'samples':samples}
        if result['complete']:result['usage']=usage(flow,settings,(time.time()-started)/3600)
        a.output.write_text(json.dumps(result,indent=2));a.output.chmod(0o600)
        print(json.dumps({'elapsed_seconds':round(result['duration_seconds']),'targets':samples[-1]['flow_targets'],
                          'raw_rows':samples[-1]['flow_raw_rows'],'flow_state':samples[-1]['flow_state']}),flush=True)
        if result['complete']:break
        time.sleep(min(30,max(0,deadline-time.monotonic())))
