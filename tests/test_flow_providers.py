import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from install_flow_rpc_provider import install
from app.flow_providers import FlowProviders, PUBLICNODE_WS, provider
from app.flow_worker import FlowSettings

HTTP='https://mainnet.robinhood.validationcloud.io/v1/TEST_SECRET_DO_NOT_LOG'
WS='wss://mainnet.robinhood.validationcloud.io/v1/TEST_SECRET_DO_NOT_LOG'


class FlowProviderTests(unittest.TestCase):
    def test_only_validation_http_and_wss_with_publicnode_primary(self):
        selected=FlowProviders(HTTP,WS)
        self.assertEqual(selected.ws('publicnode'),PUBLICNODE_WS)
        self.assertEqual(selected.ws('validation'),WS)
        self.assertEqual(provider(selected.http),'validation')
        self.assertNotIn('TEST_SECRET',repr(selected))
        self.assertNotIn('TEST_SECRET',json.dumps(selected.fingerprints()))
        for http,ws in ((HTTP.replace('https:','http:'),WS),(HTTP,WS.replace('wss:','ws:')),
                        ('https://robinhood-mainnet.g.alchemy.com/v2/key',WS),
                        (HTTP+'?key=hidden',WS)):
            with self.assertRaises(ValueError):FlowProviders(http,ws)

    def test_new_secondary_cap_ignores_legacy_setting(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'flow.env'
            path.write_text('FLOW_PROVIDER_SPLIT_ENABLED=true\nFLOW_MAX_WS_BYTES_PER_DAY=8000000\nFLOW_SECONDARY_WS_BYTES_PER_DAY=64000000\n')
            path.chmod(0o600)
            with patch.dict(os.environ,{'FLOW_ENV':str(path)}):
                self.assertEqual(FlowSettings.load().daily_ws_bytes,64_000_000)

    @unittest.skipIf(os.name=='nt','POSIX secret modes tested on VPS')
    def test_atomic_install_and_protected_load(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root);directory.chmod(0o700)
            source=directory/'provider-benchmark.env';target=directory/'flow-rpc.env'
            source.write_text('BENCH_VALIDATION_HTTP='+HTTP+'\nBENCH_VALIDATION_WS='+WS+'\nBENCH_DWELLIR_HTTP=unused\n')
            source.chmod(0o600)
            before=source.read_bytes()
            result=install(source,target)
            self.assertEqual(source.read_bytes(),before)
            self.assertEqual(target.stat().st_mode&0o777,0o600)
            self.assertNotIn('TEST_SECRET',json.dumps(result))
            self.assertNotIn('DWELLIR',target.read_text())
            with patch.dict(os.environ,{'FLOW_RPC_ENV':str(target)}):
                self.assertEqual(FlowProviders.load().http,HTTP)
            target.chmod(0o644)
            with patch.dict(os.environ,{'FLOW_RPC_ENV':str(target)}):
                with self.assertRaises(ValueError):FlowProviders.load()


if __name__=='__main__':unittest.main()
