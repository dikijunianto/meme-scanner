"""Offline config validation and redacted journal counts. Never starts collection."""
import _bootstrap  # noqa: F401
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from datetime import datetime,timezone
from urllib.parse import urlsplit
from dotenv import dotenv_values
from app.config import Config,ROOT
from app.flow_data import FlowDB
from app.flow_reports import readonly
from app.flow_worker import FlowSettings


def fingerprint(url):
    parsed=urlsplit(url);parts=parsed.path.rstrip('/').split('/')
    if parsed.hostname!='robinhood-mainnet.g.alchemy.com' or len(parts)!=3 or parts[1]!='v2' or not parts[2]:
        raise ValueError('Unexpected provider endpoint shape')
    return hashlib.sha256(parts[2].encode()).hexdigest()[:12]


def journal_counts(since):
    pattern=re.compile(r'(?:https?|wss?)://[^\s"<>]+/(?:v1|v2)/[A-Za-z0-9_/-]{12,}|[?&](?:api_key|apikey|key|token)=[A-Za-z0-9_-]{12,}',re.I)
    fragments={}
    for label,path,names in (
        ('validation',ROOT/'config/flow-rpc.env',('FLOW_RPC_HTTP','FLOW_RPC_WS_FALLBACK')),
        ('benchmark',ROOT/'config/provider-benchmark.env',('BENCH_VALIDATION_HTTP','BENCH_VALIDATION_WS'))):
        if path.is_file():
            env=dotenv_values(path,interpolate=False)
            fragments[label]={part for name in names for part in urlsplit(env.get(name) or '').path.split('/') if len(part)>=12}
    result={}
    for unit in ('meme-scanner','meme-scanner-flow'):
        p=subprocess.run(['journalctl','-u',unit,'--since',since,'--no-pager','-o','json'],capture_output=True,text=True,check=True)
        rows=[json.loads(line) for line in p.stdout.splitlines() if line.strip()]
        matches=[];auth=0;client_info=0;traceback_urls=0;credential_fragments={name:0 for name in fragments}
        for row in rows:
            msg=row.get('MESSAGE','')
            if not isinstance(msg,str):continue
            for name,parts in fragments.items():credential_fragments[name]+=any(part in msg for part in parts)
            if pattern.search(msg):
                matches.append(int(row['__REALTIME_TIMESTAMP'])/1e6)
                client_info+=bool(re.search(r'INFO.*HTTP Request|httpcore.*INFO',msg))
                traceback_urls+='Traceback' in msg
            auth+=bool(re.search(r'\bHTTP (401|403)\b|unauthorized|invalid api key|authentication fail',msg,re.I))
        result[unit]={'credential_url_matches':len(matches),'auth_error_patterns':auth,
                      'client_info_url_records':client_info,'traceback_url_records':traceback_urls,
                      'credential_fragment_matches':credential_fragments,
                      'first_match_utc':datetime.fromtimestamp(min(matches),timezone.utc).isoformat() if matches else None,
                      'last_match_utc':datetime.fromtimestamp(max(matches),timezone.utc).isoformat() if matches else None}
    return result


def status(since):
    config=Config.load();settings=FlowSettings.load()
    if config.database.resolve()==settings.database.resolve():raise ValueError('Flow database must be separate')
    main=readonly(config.database);flow=FlowDB(settings.database,readonly=True)
    try:
        integrity={'main':main.execute('PRAGMA integrity_check').fetchone()[0],
                   'flow':flow.conn.execute('PRAGMA integrity_check').fetchone()[0]}
    finally:main.close();flow.conn.close()
    if any(value!='ok' for value in integrity.values()):raise ValueError('Database integrity failed')
    endpoints={name:{'provider':'alchemy','credential_fingerprint':fingerprint(url)} for name,url in
               [('ROBINHOOD_RPC_HTTP',config.rpc_http),('ROBINHOOD_RPC_WS',config.rpc_ws)]}
    services={}
    for unit in ('meme-scanner','meme-scanner-flow'):
        raw=subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','UnitFileState','-p','MainPID','-p','NRestarts'],text=True)
        services[unit]=dict(line.split('=',1) for line in raw.splitlines())
    secret_path=Path(os.environ.get('SCANNER_ENV',ROOT/'config/.env'))
    flow_path=Path(os.environ.get('FLOW_ENV',ROOT/'config/flow.env'))
    flow_rpc_path=Path(os.environ.get('FLOW_RPC_ENV',ROOT/'config/flow-rpc.env'))
    paths=list(dict.fromkeys([secret_path.parent,secret_path,flow_path.parent,flow_path]+([flow_rpc_path] if flow_rpc_path.exists() else [])))
    return {'as_of':datetime.now(timezone.utc).isoformat(),'offline_config_validation':'passed','rpc_calls':0,
            'endpoints':endpoints,'chain_id':config.chain_id,'flow_enabled':settings.enabled,'services':services,
            'flow_limits':{name:getattr(settings,name) for name in ('daily_calls','minute_calls','daily_getlogs','daily_ws_bytes','max_subscriptions','recovery_blocks')},
            'permissions':{str(p):oct(p.stat().st_mode&0o777) for p in paths},'integrity':integrity,
            'journal_since':since,'journal':journal_counts(since),
            'warning':'Fingerprints describe configuration, not proof of the key loaded in an already-running process. No provider authentication or revocation verified.'}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--since',default='1 hour ago');args=p.parse_args()
    try:print(json.dumps(status(args.since),indent=2))
    except Exception as exc:
        print(json.dumps({'validation':'failed','error_type':type(exc).__name__}));raise SystemExit(1) from None
