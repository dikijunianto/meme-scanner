"""SQLite-safe backup before additive independent flow schema creation."""
import _bootstrap  # noqa: F401
from datetime import datetime, timezone
from contextlib import closing
import json
import os
import sqlite3
from app.config import Config, ROOT
from app.flow_data import FlowDB
from app.flow_worker import FlowSettings


def initialize(main,flow,backup_dir):
    if main.resolve()==flow.resolve():raise ValueError('Flow database must be separate from the main database')
    backup_dir.mkdir(mode=0o700,parents=True,exist_ok=False)
    backup_dir.chmod(0o700)
    checks={}
    for name,path in [('main',main),('flow',flow)]:
        if not path.exists():continue
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as source:
            check=source.execute('PRAGMA integrity_check').fetchone()[0]
            if check!='ok':raise ValueError(name+' integrity failure')
            destination=backup_dir/(name+'.db')
            fd=os.open(destination,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
            with closing(sqlite3.connect(destination)) as out:
                source.backup(out,pages=256,sleep=.02)
                checks[name]=out.execute('PRAGMA integrity_check').fetchone()[0]
                if checks[name]!='ok':raise ValueError('Backup integrity failure')
            destination.chmod(0o600)
    flow.parent.mkdir(parents=True,exist_ok=True)
    db=FlowDB(flow);db.migrate();db.set_state('schema_version',1)
    checks['flow_after']=db.conn.execute('PRAGMA integrity_check').fetchone()[0]
    db.conn.close();flow.chmod(0o600)
    return {'backup_path':str(backup_dir),'integrity':checks,'main_database_modified':False}


if __name__=='__main__':
    os.umask(0o077)
    print(json.dumps(initialize(Config.load().database,FlowSettings.load().database,
          ROOT/'data'/('phase2b-backup-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))),indent=2))
