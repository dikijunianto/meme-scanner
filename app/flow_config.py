"""Protected flow configuration updates and network-free startup checks."""
from dataclasses import replace
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from uuid import uuid4

from app.config import ROOT, Config
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders, provider
from app.flow_worker import FlowSettings, FlowWorker

FLOW_ENV = ROOT / 'config/flow.env'
FLOW_RPC_ENV = ROOT / 'config/flow-rpc.env'
FLOW_UNIT = ROOT / 'deploy/meme-scanner-flow.service'


def service_identity():
    import grp
    import pwd
    keys = ('User', 'Group', 'Environment', 'EnvironmentFiles', 'ExecStart', 'WorkingDirectory')
    output = subprocess.check_output(['systemctl', 'show', 'meme-scanner-flow.service',
                                      *(arg for key in keys for arg in ('-p', key))], text=True)
    unit = dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
    user = unit.get('User', '')
    if not user or not unit.get('Group'):
        raise ValueError('Flow service identity is missing')
    identity = pwd.getpwnam(user)
    return unit, identity.pw_uid, grp.getgrnam(unit['Group']).gr_gid


def readable_as_service(path, user):
    return (subprocess.run(['sudo', '-n', '-u', user, 'test', '-x', str(path.parent)],
                           capture_output=True).returncode == 0 and
            subprocess.run(['sudo', '-n', '-u', user, 'test', '-r', str(path)],
                           capture_output=True).returncode == 0)


def checked_file(path, parent):
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError('Protected config is not a regular file')
    if parent.is_symlink() or not stat.S_ISDIR(parent.lstat().st_mode):
        raise ValueError('Protected config parent is not a directory')
    original = path.stat()
    if stat.S_IMODE(original.st_mode) != 0o600 or stat.S_IMODE(parent.stat().st_mode) != 0o700:
        raise ValueError('Protected config permissions changed')
    return original


def split_contents(before, enabled):
    lines = before.splitlines(keepends=True)
    matches = [i for i, line in enumerate(lines) if line.split(b'=', 1)[0].strip() == b'FLOW_PROVIDER_SPLIT_ENABLED']
    if len(matches) > 1:
        raise ValueError('Duplicate provider split flag')
    value = b'FLOW_PROVIDER_SPLIT_ENABLED=' + (b'true' if enabled else b'false') + b'\n'
    if matches:
        lines[matches[0]] = value
        return b''.join(lines)
    return before.rstrip(b'\n') + b'\n' + value


def atomic_split_update(enabled, *, root=ROOT, identity=None, readable=readable_as_service):
    """The CLI fixes root to ROOT; root injection is only for isolated tests."""
    path = Path(root) / 'config/flow.env'
    unit, uid, gid = identity or service_identity()
    user = unit['User']
    old = checked_file(path, path.parent)
    if not readable(path, user):
        raise ValueError('Flow service user cannot read current config')
    before = path.read_bytes()
    after = split_contents(before, enabled)
    if before == after:
        return metadata(path, unit, uid, gid, readable)
    fd, temp_name = tempfile.mkstemp(prefix='.flow.env.', dir=path.parent)
    temp = Path(temp_name)
    backup = path.parent / ('.flow.env.previous-' + uuid4().hex)
    linked = False
    replaced = False
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(after)
            os.fchown(output.fileno(), uid, gid)
            os.fchmod(output.fileno(), 0o600)
            output.flush()
            os.fsync(output.fileno())
        if not readable(temp, user):
            raise ValueError('Flow service user cannot read replacement config')
        os.link(path, backup)
        linked = True
        os.replace(temp, path)
        replaced = True
        fsync_dir(path.parent)
        result = metadata(path, unit, uid, gid, readable)
        if not result['readable_by_service_user'] or result['owner_uid'] != uid or result['owner_gid'] != gid or result['mode'] != '0600':
            raise ValueError('Replacement config metadata check failed')
        return result
    except BaseException:
        if replaced and linked:
            os.replace(backup, path)
            linked = False
            fsync_dir(path.parent)
        raise
    finally:
        temp.unlink(missing_ok=True)
        if linked:
            backup.unlink(missing_ok=True)
            fsync_dir(path.parent)


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def metadata(path, unit, uid, gid, readable=readable_as_service):
    item = checked_file(path, path.parent)
    return {'path': str(path), 'owner_uid': item.st_uid, 'owner_gid': item.st_gid,
            'mode': f'{stat.S_IMODE(item.st_mode):04o}', 'parent_owner_uid': path.parent.stat().st_uid,
            'parent_owner_gid': path.parent.stat().st_gid,
            'parent_mode': f'{stat.S_IMODE(path.parent.stat().st_mode):04o}',
            'service_user': unit['User'], 'service_group': unit['Group'],
            'service_uid': uid, 'service_gid': gid,
            'readable_by_service_user': readable(path, unit['User'])}


def check_unit(unit):
    env = unit.get('Environment', '').split()
    if (unit.get('WorkingDirectory') != str(ROOT) or
        f'argv[]={ROOT}/.venv/bin/python -m app.flow_worker' not in unit.get('ExecStart', '') or
        f'FLOW_ENV={FLOW_ENV}' not in env or unit.get('EnvironmentFiles', '') not in ('', None)):
        raise ValueError('Installed flow unit does not reference expected config')
    source = FLOW_UNIT.read_text()
    required = (f'User={unit["User"]}', f'Group={unit["Group"]}',
                f'WorkingDirectory={ROOT}', f'Environment=FLOW_ENV={FLOW_ENV}',
                f'Environment=FLOW_RPC_ENV={FLOW_RPC_ENV}',
                f'ExecStart={ROOT}/.venv/bin/python -m app.flow_worker')
    if any(line not in source.splitlines() for line in required):
        raise ValueError('Future flow unit does not reference expected config')


async def no_network_probe(*, require_split=False):
    """Construct the worker as the service user without running or sending RPC."""
    settings = FlowSettings.load()
    if require_split and not settings.split_enabled:
        raise ValueError('Split flag is not enabled')
    config, providers = Config.load(), FlowProviders.load()
    db = FlowDB(settings.database, readonly=True)
    worker = FlowWorker(config, replace(settings, split_enabled=True), db, providers)
    try:
        if (provider(worker.rpc.config.rpc_http) != 'validation' or
            provider(worker.ws_url()) != 'publicnode' or config.chain_id != 4663):
            raise ValueError('Flow provider roles changed')
        return {'no_network': True, 'config_parsed': True,
                'candidate_split': True, 'current_split': settings.split_enabled}
    finally:
        await worker.rpc.close()
        worker.main.close()
        db.conn.close()


async def prestart_check(*, require_split=False):
    unit, uid, gid = service_identity()
    check_unit(unit)
    for path in (FLOW_ENV, FLOW_RPC_ENV, ROOT / 'config/.env'):
        if not readable_as_service(path, unit['User']):
            raise ValueError('Service user cannot read required config')
    result = metadata(FLOW_ENV, unit, uid, gid)
    if not result['readable_by_service_user']:
        raise ValueError('Service user cannot read flow config')
    if os.geteuid() == uid:
        probe = await no_network_probe(require_split=require_split)
    else:
        command = ['sudo', '-n', '-u', unit['User'], sys.executable,
                   str(ROOT / 'scripts/flow_config_status.py'), '--self-check']
        if require_split:
            command.append('--require-split')
        done = subprocess.run(command, capture_output=True, text=True)
        if done.returncode:
            raise ValueError('Service-user no-network probe failed')
        probe = json.loads(done.stdout)
    return {**result, **probe, 'installed_unit_checked': True, 'future_unit_checked': True}
