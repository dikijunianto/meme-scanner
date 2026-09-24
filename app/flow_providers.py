"""Protected Phase 2B endpoint configuration, independent of scanner routing."""
from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import dotenv_values

from app.config import ROOT

PUBLICNODE_WS = 'wss://robinhood-rpc.publicnode.com'
VALIDATION_HOST = 'mainnet.robinhood.validationcloud.io'


def provider(url):
    host = urlsplit(url).hostname
    return {'robinhood-rpc.publicnode.com': 'publicnode', VALIDATION_HOST: 'validation',
            'robinhood-mainnet.g.alchemy.com': 'alchemy'}.get(host, 'unknown')


def fingerprint(url):
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def validate(http, fallback):
    for value, scheme in ((http, 'https'), (fallback, 'wss')):
        parsed = urlsplit(value or '')
        if (parsed.scheme != scheme or parsed.hostname != VALIDATION_HOST or not parsed.path
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError('Invalid Validation flow endpoint')


@dataclass(frozen=True)
class FlowProviders:
    http: str = field(repr=False)
    ws_fallback: str = field(repr=False)
    ws_primary: str = PUBLICNODE_WS

    def __post_init__(self):
        validate(self.http, self.ws_fallback)
        if self.ws_primary != PUBLICNODE_WS:
            raise ValueError('Invalid PublicNode flow endpoint')

    @classmethod
    def load(cls):
        path = Path(os.environ.get('FLOW_RPC_ENV', ROOT / 'config/flow-rpc.env'))
        if not path.is_file() or path.is_symlink():
            raise ValueError('Missing protected flow RPC configuration')
        if os.name != 'nt' and (path.stat().st_mode & 0o077 or path.parent.stat().st_mode & 0o077):
            raise ValueError('Unsafe flow RPC configuration permissions')
        env = dotenv_values(path, interpolate=False)
        return cls(env.get('FLOW_RPC_HTTP'), env.get('FLOW_RPC_WS_FALLBACK'))

    def ws(self, name):
        return {'publicnode': self.ws_primary, 'validation': self.ws_fallback}[name]

    def fingerprints(self):
        return {'http': fingerprint(self.http), 'ws_primary': fingerprint(self.ws_primary),
                'ws_fallback': fingerprint(self.ws_fallback)}
