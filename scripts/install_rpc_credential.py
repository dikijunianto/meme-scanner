"""Operator-only hidden-prompt installation. No RPC, restart, revocation or resume."""
import _bootstrap  # noqa: F401
from getpass import getpass
import io
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit,urlunsplit
from dotenv import dotenv_values
from app.config import ROOT
from flow_security_status import fingerprint


class InstallationError(ValueError):
    """Only fixed, credential-free operator validation messages."""


def failure_reason(exc):
    return str(exc) if isinstance(exc,InstallationError) else 'Unexpected failure; run flow_security_status.py before retrying'


def install(path,key,temporary_directory):
    if not re.fullmatch(r'[A-Za-z0-9_-]{16,200}',key):raise InstallationError('Invalid key format: enter only the API key, without URL, quotes or spaces')
    info=path.stat()
    if path.is_symlink() or info.st_mode&0o077 or path.parent.stat().st_mode&0o077:raise InstallationError('Unsafe secret path permissions')
    if hasattr(os,'getuid') and info.st_uid!=os.getuid():raise InstallationError('Run as the secret file owner')
    text=path.read_text();env=dotenv_values(stream=io.StringIO(text),interpolate=False)
    if urlsplit(env['ROBINHOOD_RPC_HTTP']).path!=urlsplit(env['ROBINHOOD_RPC_WS']).path:
        raise InstallationError('Existing HTTP and WSS credentials differ')
    old={fingerprint(env[name]) for name in ('ROBINHOOD_RPC_HTTP','ROBINHOOD_RPC_WS')}
    if len(old)!=1:raise InstallationError('Existing HTTP and WSS credentials differ')
    result={}
    for name,scheme in [('ROBINHOOD_RPC_HTTP','https'),('ROBINHOOD_RPC_WS','wss')]:
        parsed=urlsplit(env[name])
        if parsed.scheme!=scheme or parsed.username or parsed.query or parsed.fragment:raise InstallationError('Unexpected endpoint structure')
        value=urlunsplit((scheme,parsed.netloc,'/v2/'+key,'',''))
        result[name]=fingerprint(value)
        if result[name] in old:raise InstallationError('Replacement must differ from the compromised key')
        text,n=re.subn(r'^\s*(?:export\s+)?'+name+r'\s*=.*$',name+'='+value,text,flags=re.M)
        if n!=1:raise InstallationError('Missing or duplicate endpoint field')
    temporary_directory.mkdir(parents=True,mode=0o700,exist_ok=True)
    if temporary_directory.is_symlink():raise InstallationError('Unsafe staging directory')
    temporary_directory.chmod(0o700)
    if temporary_directory.stat().st_dev!=path.parent.stat().st_dev:raise InstallationError('Atomic installation requires same filesystem')
    fd,name=tempfile.mkstemp(prefix='rpc-',dir=temporary_directory)
    try:
        os.fchmod(fd,0o600)
        with os.fdopen(fd,'w') as stream:stream.write(text);stream.flush();os.fsync(stream.fileno())
        os.replace(name,path)
        if os.name!='nt':
            directory=os.open(path.parent,os.O_RDONLY)
            try:os.fsync(directory)
            finally:os.close(directory)
    finally:
        if os.path.exists(name):os.unlink(name)
    return {'installed':True,'old_fingerprint':next(iter(old)),'new_fingerprints':result,
            'services_restarted':False,'old_key_revoked':False,'flow_enabled':False,
            'next_step':'Return for main-scanner switchover validation. Do not revoke the old key or enable flow yet.'}


if __name__=='__main__':
    try:
        from app.flow_worker import FlowSettings
        if FlowSettings.load().enabled:raise InstallationError('Flow must remain disabled')
        import subprocess
        unit=subprocess.check_output(['systemctl','show','meme-scanner-flow','-p','ActiveState','-p','UnitFileState'],text=True)
        state=dict(line.split('=',1) for line in unit.splitlines())
        if state.get('ActiveState')!='inactive' or state.get('UnitFileState')!='disabled':raise InstallationError('Stop and disable flow before installation')
        import sys
        if not sys.stdin.isatty():raise InstallationError('Use an interactive SSH terminal for the hidden prompt')
        key=getpass('Replacement Alchemy API key (hidden): ')
        if key!=getpass('Confirm replacement key (hidden): '):raise InstallationError('Keys do not match')
        print(json.dumps(install(ROOT/'config/.env',key,Path.home()/'.local/state/meme-scanner-secrets'),indent=2))
    except (Exception,KeyboardInterrupt) as exc:
        print(json.dumps({'installation':'not_confirmed','error_type':type(exc).__name__,'reason':failure_reason(exc),
                          'next_step':'Run flow_security_status.py before retrying; do not restart or revoke yet.'}));raise SystemExit(1) from None
