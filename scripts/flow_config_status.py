"""Protected flow config status, atomic flag update, and offline pre-start gate."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
import subprocess

from app.flow_config import (FLOW_ENV, atomic_split_update, metadata, no_network_probe,
                             prestart_check, service_identity,candidate_split_preflight,
                             candidate_split_status,safe_split_fingerprint)
from app.flow_cutover import current as active_cutover,mark_split_configured,source_process_gone
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders
from app.flow_worker import FlowSettings


async def operate(args):
    if args.self_check:
        return await no_network_probe(require_split=args.require_split)
    if args.preflight:
        return await prestart_check(require_split=args.require_split)
    if getattr(args,'candidate_preflight',False):
        return await candidate_split_preflight()
    unit, uid, gid = service_identity()
    if args.enable_split or args.disable_split:
        before = FlowSettings.load().split_enabled
        db=None
        if args.enable_split:
            db=FlowDB(FlowSettings.load().database)
            row=active_cutover(db)
            value=json.loads(row['payload']) if row else None
            if not value or value['state'] not in ('STOP_TAIL_VERIFIED','SPLIT_CONFIGURED') or \
               not value.get('source_stopped_at') or not value.get('candidate_config_fingerprint'):
                raise ValueError('Verified stopped source and authorized candidate required')
            raw=subprocess.check_output(['systemctl','show','meme-scanner-flow.service',
                                         '-p','ActiveState','-p','MainPID'],text=True)
            state=dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
            if (state.get('ActiveState')!='inactive' or
                state.get('MainPID')==value['source_legacy_pid'] or
                not source_process_gone(value['source_legacy_pid'])):
                raise ValueError('Legacy source process is still active')
            if not before:
                candidate=candidate_split_status()
                if (candidate['candidate_fingerprint']!=value['candidate_config_fingerprint'] or
                    candidate['legacy_file_digest']!=value['authorization']['legacy_file_digest']):
                    raise ValueError('Candidate or legacy config changed')
            elif safe_split_fingerprint(FlowSettings.load(),FlowProviders.load())!=value['candidate_config_fingerprint']:
                raise ValueError('Configured split differs from authorized candidate')
        if args.disable_split and not before:
            raise ValueError('Split flag is already disabled')
        await prestart_check(require_split=before)
        if before!=args.enable_split:
            atomic_split_update(args.enable_split, identity=(unit, uid, gid))
        try:
            await prestart_check(require_split=args.enable_split)
            if args.enable_split:
                fingerprint=safe_split_fingerprint(FlowSettings.load(),FlowProviders.load())
                if fingerprint!=value['candidate_config_fingerprint']:
                    raise ValueError('Split config fingerprint mismatch')
                result=metadata(FLOW_ENV,unit,uid,gid)
                if (result['mode'],result['parent_mode'])!=('0600','0700') or \
                   not result['readable_by_service_user']:
                    raise ValueError('Protected split config metadata changed')
                if value['state']=='STOP_TAIL_VERIFIED':mark_split_configured(db,value,fingerprint)
        except BaseException:
            if before!=args.enable_split:
                atomic_split_update(before, identity=(unit, uid, gid))
            raise
        finally:
            if db:db.conn.close()
    result = metadata(FLOW_ENV, unit, uid, gid)
    providers = FlowProviders.load()
    result.update(split_enabled=FlowSettings.load().split_enabled,
                  provider_roles={'primary_wss': 'publicnode', 'fallback_wss': 'validation',
                                  'recovery_http': 'validation'},
                  endpoint_fingerprints=providers.fingerprints(),
                  environment_file=unit.get('EnvironmentFiles') or None,
                  working_directory=unit.get('WorkingDirectory'),
                  exec_start='python -m app.flow_worker')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--preflight', action='store_true')
    modes.add_argument('--candidate-preflight', action='store_true')
    modes.add_argument('--self-check', action='store_true')
    modes.add_argument('--enable-split', action='store_true')
    modes.add_argument('--disable-split', action='store_true')
    parser.add_argument('--require-split', action='store_true')
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(operate(args))))
    except Exception as exc:
        # An exception may contain an endpoint URL. Report only its class.
        print(json.dumps({'validation': 'failed', 'error_type': type(exc).__name__}))
        raise SystemExit(1) from None
