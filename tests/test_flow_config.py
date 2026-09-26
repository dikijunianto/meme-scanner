import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from app.flow_config import atomic_split_update, no_network_probe, prestart_check
from app.flow_providers import FlowProviders
from app.flow_worker import FlowSettings


@unittest.skipUnless(os.name == 'posix', 'Protected ownership checks require POSIX')
class FlowConfigTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.parent = self.root / 'config'
        self.parent.mkdir(mode=0o700)
        self.path = self.parent / 'flow.env'
        self.secret = b'PRIVATE_ENDPOINT=https://example.invalid/v1/secret-test-value\n'
        self.path.write_bytes(self.secret + b'FLOW_PROVIDER_SPLIT_ENABLED=false\n')
        self.path.chmod(0o600)
        self.unit = {'User': 'test-user', 'Group': 'test-group'}
        self.identity = (self.unit, os.getuid(), os.getgid())
        self.readable = lambda path, user: path.exists() and os.access(path, os.R_OK)

    def tearDown(self):
        self.tmp.cleanup()

    def test_atomic_root_style_temp_gets_service_owner_and_mode(self):
        original = os.fchown
        changed = []
        def chown(fd, uid, gid):
            changed.append((uid, gid, os.fstat(fd).st_uid))
            return original(fd, uid, gid)
        with patch('app.flow_config.os.fchown', side_effect=chown):
            result = atomic_split_update(True, root=self.root, identity=self.identity, readable=self.readable)
        self.assertEqual(changed[0][:2], (os.getuid(), os.getgid()))
        self.assertEqual((result['owner_uid'], result['owner_gid']), (os.getuid(), os.getgid()))
        self.assertEqual((result['mode'], result['parent_mode']), ('0600', '0700'))
        self.assertTrue(result['readable_by_service_user'])
        self.assertIn(b'FLOW_PROVIDER_SPLIT_ENABLED=true', self.path.read_bytes())
        self.assertNotIn(self.secret.decode().strip(), str(result))
        self.assertEqual(len(list(self.parent.iterdir())), 1)

    def test_unreadable_and_failed_replace_leave_previous_config(self):
        before = self.path.read_bytes()
        with patch('app.flow_config.tempfile.mkstemp') as create:
            with self.assertRaises(ValueError):
                atomic_split_update(True, root=self.root, identity=self.identity,
                                    readable=lambda path, user: False)
            create.assert_not_called()
        with patch('app.flow_config.os.replace', side_effect=OSError('blocked')):
            with self.assertRaises(OSError):
                atomic_split_update(True, root=self.root, identity=self.identity, readable=self.readable)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(len(list(self.parent.iterdir())), 1)

    def test_failed_postwrite_service_read_restores_original_inode(self):
        before = self.path.read_bytes()
        inode = self.path.stat().st_ino
        checks = 0
        def readable(path, user):
            nonlocal checks
            checks += 1
            return checks < 3
        with self.assertRaises(ValueError):
            atomic_split_update(True, root=self.root, identity=self.identity, readable=readable)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.path.stat().st_ino, inode)
        self.assertEqual(len(list(self.parent.iterdir())), 1)

    async def test_unreadable_prestart_fails_without_worker_or_stop(self):
        with patch('app.flow_config.service_identity', return_value=self.identity), \
             patch('app.flow_config.check_unit'), \
             patch('app.flow_config.readable_as_service', return_value=False), \
             patch('app.flow_config.no_network_probe') as worker, \
             patch('app.flow_config.subprocess.run') as stop:
            with self.assertRaises(ValueError):
                await prestart_check()
            worker.assert_not_called()
            stop.assert_not_called()

    async def test_no_network_probe_validates_candidate_worker(self):
        settings = FlowSettings(database=self.root / 'flow.db', split_enabled=False)
        config = type('ConfigStub', (), {'chain_id': 4663})()
        providers = FlowProviders('https://mainnet.robinhood.validationcloud.io/v1/test',
                                  'wss://mainnet.robinhood.validationcloud.io/v1/test')
        class Worker:
            def __init__(self, config, settings, db, providers):
                self.rpc = type('RpcStub', (), {'config': type('RpcConfig', (), {
                    'rpc_http': providers.http})(), 'close': AsyncMock()})()
                self.main = type('MainStub', (), {'close': lambda self: None})()
            def ws_url(self):
                return providers.ws_primary
        class DB:
            conn = type('ConnStub', (), {'close': lambda self: None})()
        with patch('app.flow_config.FlowSettings.load', return_value=settings), \
             patch('app.flow_config.Config.load', return_value=config), \
             patch('app.flow_config.FlowProviders.load', return_value=providers), \
             patch('app.flow_config.FlowDB', return_value=DB()), \
             patch('app.flow_config.FlowWorker', Worker), \
             patch('app.rpc.Rpc._send', side_effect=AssertionError('network called')):
            result = await no_network_probe()
        self.assertEqual(result, {'no_network': True, 'config_parsed': True,
                                  'candidate_split': True, 'current_split': False})
        with patch('app.flow_config.FlowSettings.load', return_value=settings):
            with self.assertRaises(ValueError):
                await no_network_probe(require_split=True)
