"""Protected flow config status, atomic flag update, and offline pre-start gate."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json

from app.flow_config import (FLOW_ENV, atomic_split_update, metadata, no_network_probe,
                             prestart_check, service_identity)
from app.flow_providers import FlowProviders
from app.flow_worker import FlowSettings


async def operate(args):
    if args.self_check:
        return await no_network_probe(require_split=args.require_split)
    if args.preflight:
        return await prestart_check(require_split=args.require_split)
    unit, uid, gid = service_identity()
    if args.enable_split or args.disable_split:
        before = FlowSettings.load().split_enabled
        if args.enable_split and before:
            raise ValueError('Split flag is already enabled')
        if args.disable_split and not before:
            raise ValueError('Split flag is already disabled')
        await prestart_check(require_split=False)
        atomic_split_update(args.enable_split, identity=(unit, uid, gid))
        try:
            await prestart_check(require_split=args.enable_split)
        except BaseException:
            atomic_split_update(before, identity=(unit, uid, gid))
            raise
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
