"""Fresh-interpreter proof of the real module CLI, with only external boundaries faked."""
import asyncio
from contextlib import ExitStack,redirect_stdout
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]

class EpochCommandTests(unittest.TestCase):
    def scenario(self,name):
        result=subprocess.run([sys.executable,str(Path(__file__).resolve()),name],cwd=ROOT,
                              capture_output=True,text=True,timeout=90)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('External attempts: 0',result.stdout)

    def test_fresh_import_and_help_have_no_db_network_or_service_effects(self):self.scenario('import-safety')

    def test_exact_module_cli_rollover_then_sealed_lifecycle(self):self.scenario('lifecycle')
    def test_first_import_interruption_preserves_db_then_same_command_resumes(self):self.scenario('interruption')
    def test_deployed_entrypoint_reaches_preflight_without_mutation(self):self.scenario('preflight')


def command_scenario(name):
    # No scripts directory on sys.path: the production -m import layout.
    sys.path[:]=[str(ROOT),*[p for p in sys.path if Path(p or '.').resolve() not in (ROOT/'scripts',ROOT/'tests')]]
    sys.modules.pop('_bootstrap',None)
    from tests import network_guard
    network_guard.install()
    if name=='import-safety':
        # Initialize only the native crypto dependency's platform probe before the service guard.
        from eth_utils import keccak
        keccak(text='offline fixture')
        with patch('sqlite3.connect',side_effect=AssertionError('import DB access')),patch('subprocess.run',side_effect=AssertionError('import service action')):
            from scripts import phase2b2_shadow
            assert phase2b2_shadow._bootstrap.__name__=='scripts._bootstrap'
            sys.argv=['scripts.flow_epoch_prepare','--help']
            with redirect_stdout(io.StringIO()):
                try:runpy.run_module('scripts.flow_epoch_prepare',run_name='__main__')
                except SystemExit as exc:assert exc.code==0
                else:raise AssertionError('Help parser must exit')
        assert not network_guard.attempts
        print('External attempts: 0; import/help without DB or service effects')
        return
    from tests import test_flow_epochs as fixtures
    from app import flow_epochs,flow_provider_switch
    from app.flow_data import FlowDB
    from app.flow_shadow import make_reconciler
    from app.config import Config
    from app.flow_worker import FlowSettings
    from app.flow_providers import FlowProviders
    case=fixtures.EpochTests();asyncio.run(case.asyncSetUp())
    case.failure()
    # The operator's retained switch identity/incident are constants; fixture owns the data.
    payload=json.loads(case.raw);payload['first_disconnect_at']=1790955170.6994033
    case.raw=json.dumps(payload,sort_keys=True)
    with case.db.conn:
        case.db.conn.execute('UPDATE flow_provider_switches SET id=17,payload=? WHERE id=?',(case.raw,case.sid))
    case.sid=17
    before=list(case.db.conn.iterdump())
    from dataclasses import replace
    case.settings=replace(case.settings,minute_calls=12)
    case.fake_rpc()
    from scripts import phase2b2_shadow
    assert phase2b2_shadow._bootstrap.__name__=='scripts._bootstrap'
    assert '_bootstrap' not in sys.modules
    actions=[]
    def service_read(args,**kwargs):
        assert args[:2]==('systemctl','show'),args
        actions.append(args[2])
        active=args[2]=='meme-scanner'
        return SimpleNamespace(stdout=f'ActiveState={"active" if active else "inactive"}\nMainPID={64326 if active else 0}\nNRestarts=0\nExecMainStartTimestampMonotonic=1\n',returncode=0)
    def invoke():
        sys.argv=['scripts.flow_epoch_prepare','--old-flow-pid','999999999','--epoch-id','epoch2']
        out=io.StringIO()
        with redirect_stdout(out):runpy.run_module('scripts.flow_epoch_prepare',run_name='__main__')
        return json.loads(out.getvalue())
    with ExitStack() as stack:
        stack.enter_context(patch('app.flow_shadow.verified_checkout',return_value='b'*40))
        stack.enter_context(patch.object(Config,'load',return_value=case.config))
        stack.enter_context(patch.object(FlowSettings,'load',return_value=case.settings))
        stack.enter_context(patch.object(FlowProviders,'load',return_value=case.providers))
        stack.enter_context(patch('subprocess.run',side_effect=service_read))
        stack.enter_context(patch('app.flow_epochs.time.time',return_value=1200))
        if name=='interruption':
            import builtins
            original=builtins.__import__
            def interrupted(module,*args,**kwargs):
                if module=='scripts.phase2b2_shadow':raise ModuleNotFoundError("No module named '_bootstrap'",name='_bootstrap')
                return original(module,*args,**kwargs)
            with patch('builtins.__import__',side_effect=interrupted):
                try:invoke()
                except ModuleNotFoundError as exc:assert exc.name=='_bootstrap'
                else:raise AssertionError('Import interruption must fail')
            assert list(case.db.conn.iterdump())==before and not actions
            assert not flow_epochs.exists(case.db.conn)
            assert not list(case.path.glob('*.epoch-*'))
        if name=='preflight':
            # Exact handler dispatch stops at clean-source preflight before services/DB/RPC.
            with patch('app.flow_shadow.verified_checkout',side_effect=ValueError('fixture preflight boundary')):
                try:invoke()
                except ValueError as exc:assert str(exc)=='fixture preflight boundary'
                else:raise AssertionError('Must stop before mutation')
            assert list(case.db.conn.iterdump())==before and not actions and not case.calls
        else:
            result=invoke()
            assert result['architecture']=='SEALED_POSTSTART_BOOTSTRAP'
            assert result['epoch']['status']=='ACTIVATING' and not result['epoch']['pit_eligible']
            assert case.calls==['eth_chainId','eth_blockNumber','eth_getBlockByNumber']
            assert len(actions)==4 and set(actions)=={'meme-scanner','meme-scanner-flow'}
            assert flow_provider_switch.latest(case.db)['state']=='FAILED'
            assert flow_provider_switch.latest(case.db)['payload']==case.raw
            assert case.db.conn.execute('SELECT count(*) FROM flow_gaps WHERE resolved=0').fetchone()[0]==2
            assert [dict(r) for r in case.db.conn.execute('SELECT * FROM flow_feature_versions')]==case.before
    if name=='lifecycle':
        case.fresh=FlowDB(case.db.path)
        runner,old=make_reconciler(case.config,case.settings,case.fresh,case.providers)
        asyncio.run(old.close())
        from dataclasses import replace
        runner.worker.rpc.config=replace(runner.worker.rpc.config,rpc_rps=100000)
        async def prepared_rollover():return runner
        case.rollover=prepared_rollover
        # Reuse the actual bootstrap -> ACK/tail -> ACTIVE -> immutable PIT assertions.
        asyncio.run(case.test_materializer_rollover_old_missing_stays_missing_actual_availability())
        assert case.fresh.epoch()['status']=='ACTIVE'
        asyncio.run(runner.worker.rpc.close());runner.worker.main.close();case.fresh.close()
    asyncio.run(case.asyncTearDown());case.doCleanups()
    assert not network_guard.attempts,network_guard.attempts
    print('External attempts: 0; real CLI scenario passed:',name)

if __name__=='__main__':command_scenario(sys.argv[1])
