"""Copy only Validation endpoints into the protected Phase 2B configuration."""
import _bootstrap  # noqa: F401
import argparse
import json
import os
from pathlib import Path
import tempfile

from dotenv import dotenv_values

from app.config import ROOT
from app.flow_providers import FlowProviders


def install(source, destination):
    source, destination = Path(source), Path(destination)
    if (not source.is_file() or source.is_symlink() or destination.is_symlink()
            or source.parent != destination.parent or source.parent.is_symlink()
            or source.stat().st_mode & 0o077 or source.parent.stat().st_mode & 0o077
            or (destination.exists() and destination.stat().st_mode & 0o077)):
        raise ValueError('Unsafe flow provider configuration path')
    if hasattr(os, 'getuid') and source.stat().st_uid != os.getuid():
        raise ValueError('Run as protected configuration owner')
    env = dotenv_values(source, interpolate=False)
    providers = FlowProviders(env.get('BENCH_VALIDATION_HTTP'), env.get('BENCH_VALIDATION_WS'))
    content = 'FLOW_RPC_HTTP=' + providers.http + '\nFLOW_RPC_WS_FALLBACK=' + providers.ws_fallback + '\n'
    old_umask = os.umask(0o077)
    try:
        fd, temporary = tempfile.mkstemp(prefix='.flow-rpc-', dir=destination.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'w') as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            if os.name != 'nt':
                directory = os.open(destination.parent, os.O_RDONLY)
                try:os.fsync(directory)
                finally:os.close(directory)
        finally:
            if os.path.exists(temporary):os.unlink(temporary)
    finally:
        os.umask(old_umask)
    return {'installed': True, 'path': str(destination), 'mode': oct(destination.stat().st_mode & 0o777),
            'fingerprints': providers.fingerprints(), 'services_restarted': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-benchmark', choices=['validation'], required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(install(ROOT/'config/provider-benchmark.env', ROOT/'config/flow-rpc.env'), indent=2))
    except Exception as exc:
        print(json.dumps({'installed': False, 'error_type': type(exc).__name__}))
        raise SystemExit(1) from None
