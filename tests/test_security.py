import asyncio
from contextlib import redirect_stderr
import io
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch,AsyncMock,Mock
import httpx

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from flow_security_status import fingerprint,journal_counts,status
from install_rpc_credential import install,InstallationError,failure_reason
from app.config import Config
from app.flow_data import FlowDB
from app.flow_worker import FlowSettings,FlowWorker,main as flow_main,cli as flow_cli
from app.flow_providers import FlowProviders
from app.heads import HeadFeed
from app.main import setup_logging
from app.rpc import Rpc,RpcError

SECRET='TEST_SECRET_MUST_NOT_APPEAR_123456'
HTTP='https://robinhood-mainnet.g.alchemy.com/v2/'+SECRET
WS='wss://robinhood-mainnet.g.alchemy.com/v2/'+SECRET


class SecurityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.stream=io.StringIO();self.handler=logging.StreamHandler(self.stream)
        root=logging.getLogger();self.old_level=root.level;root.setLevel(logging.INFO);root.addHandler(self.handler)
        self.levels={name:logging.getLogger(name).level for name in ('httpx','httpcore','websockets')}
        self.config=Config(HTTP,4663,(),'',self.root/'main.db',self.root/'scanner.log',rpc_ws=WS,retry_attempts=2)

    def tearDown(self):
        root=logging.getLogger();root.removeHandler(self.handler);root.setLevel(self.old_level)
        for name,level in self.levels.items():logging.getLogger(name).setLevel(level)
        self.tmp.cleanup()

    def configure_flow(self):
        with patch('app.flow_worker.FlowSettings.load',return_value=FlowSettings()),patch('app.flow_worker.Config.load',side_effect=AssertionError('Disabled must not load credentials')):
            flow_main()

    def clean(self,*values):
        for value in (self.stream.getvalue(),*values):
            self.assertNotIn(SECRET,value);self.assertNotIn(HTTP,value);self.assertNotIn(WS,value)

    def test_both_entry_points_suppress_library_info(self):
        for configure in (lambda:setup_logging(self.config.log_path),self.configure_flow):
            with patch('app.main.RotatingFileHandler',return_value=logging.NullHandler()):configure()
            for name,url in [('httpx',HTTP),('httpcore',HTTP),('websockets',WS)]:logging.getLogger(name).info('request %s',url)
            self.clean()

    async def test_http_auth_and_transport_failures_do_not_expose_url(self):
        self.configure_flow()
        for kind in ('auth','transport'):
            rpc=Rpc(self.config);await rpc.client.aclose()
            def response(request):
                if kind=='transport':raise httpx.ConnectError('failed '+HTTP,request=request)
                return httpx.Response(401,json={'error':'invalid API key '+SECRET})
            rpc.client=httpx.AsyncClient(transport=httpx.MockTransport(response))
            try:
                with patch('app.rpc.asyncio.sleep',AsyncMock()):
                    with self.assertRaises(RpcError) as error:await rpc.call('eth_chainId',[])
                self.clean(str(error.exception))
            finally:await rpc.close()

    async def test_base_websocket_failure_does_not_expose_url(self):
        self.configure_flow();feed=HeadFeed(self.config)
        with patch('app.heads.connect',side_effect=OSError(WS)),patch('app.heads.asyncio.sleep',AsyncMock()):await feed.run()
        self.clean();self.assertIn('WebSocket',self.stream.getvalue())

    async def test_flow_websocket_failure_does_not_expose_url(self):
        self.configure_flow();sqlite3.connect(self.config.database).close()
        db=FlowDB(self.root/'flow.db');db.migrate();settings=FlowSettings(database=self.root/'flow.db')
        worker=FlowWorker(self.config,settings,db,FlowProviders(
            'https://mainnet.robinhood.validationcloud.io/v1/test',
            'wss://mainnet.robinhood.validationcloud.io/v1/test'))
        try:
            with patch('app.flow_worker.connect',side_effect=OSError(WS)),patch('app.flow_worker.asyncio.sleep',AsyncMock(side_effect=asyncio.CancelledError)):
                with self.assertRaises(asyncio.CancelledError):await worker.run()
            self.clean();self.assertIn('Flow disconnected provider=publicnode error=OSError',self.stream.getvalue())
        finally:db.conn.close()

    def test_flow_startup_exception_hides_environment_values(self):
        errors=io.StringIO()
        with patch('app.flow_worker.main',side_effect=ValueError(HTTP)),redirect_stderr(errors):
            with self.assertRaises(SystemExit) as exit:flow_cli()
        self.assertEqual(exit.exception.code,1);self.clean(errors.getvalue());self.assertIn('ValueError',errors.getvalue())

    def test_installer_only_exposes_its_fixed_validation_messages(self):
        self.assertEqual(failure_reason(InstallationError('Keys do not match')),'Keys do not match')
        for exc in (ValueError(HTTP),OSError(SECRET)):
            self.clean(failure_reason(exc))
        with self.assertRaises(InstallationError) as error:
            install(self.root/'unused',HTTP,self.root/'staging')
        self.assertIn('enter only the API key',failure_reason(error.exception))
        self.clean(failure_reason(error.exception))

    def test_fingerprints_do_not_emit_values_and_match_http_wss(self):
        self.assertEqual(fingerprint(HTTP),fingerprint(WS));self.assertEqual(len(fingerprint(HTTP)),12)
        self.clean(json.dumps({'fingerprint':fingerprint(HTTP)}),repr(self.config))
        with self.assertRaises(ValueError):fingerprint('https://example.invalid/v2/'+SECRET)

    def test_journal_reports_counts_never_matching_lines(self):
        entry=json.dumps({'MESSAGE':'INFO HTTP Request: POST '+HTTP,'__REALTIME_TIMESTAMP':'1789894537934678'})
        with patch('flow_security_status.subprocess.run',return_value=Mock(stdout=entry)):
            result=journal_counts('1 hour ago')
        self.assertEqual(result['meme-scanner-flow']['credential_url_matches'],1);self.clean(json.dumps(result))

    def test_disabled_status_is_offline_and_does_not_resolve_secrets_into_output(self):
        config_dir=self.root/'config';config_dir.mkdir()
        for name in ('.env','flow.env'):(config_dir/name).write_text('')
        sqlite3.connect(self.config.database).close();db=FlowDB(self.root/'flow.db');db.migrate();db.conn.close()
        with (patch('flow_security_status.Config.load',return_value=self.config),patch('flow_security_status.FlowSettings.load',return_value=FlowSettings(database=self.root/'flow.db')),
             patch('flow_security_status.ROOT',self.root),patch('flow_security_status.journal_counts',return_value={}),
             patch('flow_security_status.subprocess.check_output',return_value='ActiveState=inactive\n'),patch.object(Rpc,'call',side_effect=AssertionError('No RPC'))):
            result=status('1 hour ago')
        self.assertEqual(result['rpc_calls'],0);self.assertFalse(result['flow_enabled']);self.clean(json.dumps(result))


@unittest.skipIf(os.name=='nt','POSIX permissions and atomic installation verified on Ubuntu VPS')
class InstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.path=self.root/'.env';self.path.write_text('ROBINHOOD_RPC_HTTP='+HTTP+'\nROBINHOOD_RPC_WS='+WS+'\nFLOW_MAX_WS_BYTES_PER_DAY=8000000\n')
        self.path.chmod(0o600);self.new='TEST_REPLACEMENT_KEY_NOT_A_REAL_KEY'

    def tearDown(self):self.tmp.cleanup()

    def test_atomic_install_preserves_other_fields_and_removes_staging_file(self):
        out=install(self.path,self.new,self.root/'staging')
        contents=self.path.read_text();self.assertNotIn(SECRET,contents);self.assertEqual(contents.count(self.new),2)
        self.assertIn('FLOW_MAX_WS_BYTES_PER_DAY=8000000',contents);self.assertEqual(self.path.stat().st_mode&0o777,0o600)
        self.assertEqual(list((self.root/'staging').iterdir()),[])
        self.assertNotIn(self.new,json.dumps(out));self.assertNotIn(SECRET,json.dumps(out));self.assertFalse(out['services_restarted'])

    def test_reused_injected_or_duplicate_key_fields_rejected_without_write(self):
        before=self.path.read_bytes()
        for key in (SECRET,'bad\nNEW_VAR=value'):
            with self.assertRaises(ValueError):install(self.path,key,self.root/'staging')
            self.assertEqual(self.path.read_bytes(),before)
        self.path.write_bytes(before+b'ROBINHOOD_RPC_HTTP='+HTTP.encode()+b'\n')
        with self.assertRaises(ValueError):install(self.path,self.new,self.root/'staging')

    def test_unsafe_permissions_and_failed_replace_preserve_original(self):
        before=self.path.read_bytes();self.path.chmod(0o644)
        with self.assertRaises(ValueError):install(self.path,self.new,self.root/'staging')
        self.path.chmod(0o600)
        with patch('install_rpc_credential.os.replace',side_effect=OSError('blocked')):
            with self.assertRaises(OSError):install(self.path,self.new,self.root/'staging')
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(list((self.root/'staging').iterdir()),[])
