"""Full local startup contract, real creator/entrypoint and zero-network maintenance."""
import asyncio,ast,json,sqlite3,unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from app import flow_partition_schema as contract,flow_worker,flow_segments
from app.flow_data import FlowDB
from tests import test_flow_research_segments as fixture

class StartupContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_original_entrypoint_failure_exactly_reproduced(self):
        await self.segment()
        self.fresh.conn.execute("DELETE FROM flow_state WHERE key='schema_version'");self.fresh.conn.commit()
        source="def main():\n    logging.Formatter.converter=time.gmtime\n    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')\n    # HTTP client INFO records contain credential-bearing provider URLs.\n    logging.getLogger('httpx').setLevel(logging.WARNING)\n    logging.getLogger('httpcore').setLevel(logging.WARNING)\n    logging.getLogger('websockets').setLevel(logging.CRITICAL)\n    settings=FlowSettings.load()\n    if not settings.enabled:\n        log.info('Phase 2B disabled; no database or network activity');return\n    config=Config.load()\n    providers=FlowProviders.load()\n    if settings.database.resolve()==config.database.resolve():raise ValueError('Flow database must be separate from the main database')\n    from app.flow_lock import flow_writer_lock\n    with flow_writer_lock(settings.database):\n        db=FlowDB(settings.database)\n        try:\n            if (epoch:=db.epoch()) and epoch['status'] not in ('ACTIVATING','ACTIVE'):\n                raise ValueError('Historical epoch is closed; prepare a fresh proved epoch before starting')\n            # Migration is explicit, never a side effect of starting service.\n            if db.state('schema_version')!='1':raise ValueError('Run SQLite-safe flow initialization first')\n            asyncio.run(FlowWorker(config,settings,db,providers).run())\n        finally:\n            db.close()"
        tree=ast.parse(source);node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
        ns=dict(vars(flow_worker));exec(compile(ast.Module(body=[node],type_ignores=[]),'old_flow_worker.py','exec'),ns)
        # Real old main checks the selected partition before constructing FlowWorker.
        with patch.object(flow_worker.FlowSettings,'load',return_value=replace(self.settings,enabled=True)),patch.object(flow_worker.Config,'load',return_value=self.config),patch.object(flow_worker.FlowProviders,'load',return_value=self.providers):
            with self.assertRaisesRegex(ValueError,'Run SQLite-safe flow initialization first'):ns['main']()
        self.assertEqual(contract.validate_startup_contract(self.fresh.conn)['failures'],['missing metadata:schema_version'])

    async def test_creation_and_repaired_production_shape_reach_real_boundary(self):
        await self.segment();before=flow_segments.record(self.fresh)
        self.assertTrue(contract.validate_startup_contract(self.fresh.conn,segment=before,epoch=self.fresh.epoch(),catalog_path=self.db.path)['complete'])
        self.assertEqual(self.fresh.state('schema_version'),'1')
        with patch('app.flow_worker.connect') as wss,patch('httpx.AsyncClient.send') as http:
            self.assertEqual((await flow_worker.local_preflight(self.config,self.settings,self.fresh,self.providers))['gate'],'LOCAL_STARTUP_PREFLIGHT_PASS')
        wss.assert_not_called();http.assert_not_called()
        self.fresh.conn.execute("DELETE FROM flow_state WHERE key='schema_version'");self.fresh.conn.commit()
        retained={t:[tuple(x) for x in self.fresh.conn.execute('SELECT * FROM '+t)] for t in contract.MANDATORY_TABLES if t!='flow_state'}
        current=dict(self.fresh.conn.execute('SELECT * FROM flow_state'))
        for repeat in range(2):
            result=contract.repair(self.fresh.conn,segment=before,epoch=self.fresh.epoch(),catalog_path=self.db.path)
            self.assertTrue(result['complete']);self.assertEqual(flow_segments.record(self.fresh),before)
            self.assertEqual(dict(self.fresh.conn.execute('SELECT * FROM flow_state')),{**current,'schema_version':'1'})
            self.assertEqual({t:[tuple(x) for x in self.fresh.conn.execute('SELECT * FROM '+t)] for t in retained},retained)
            with patch('app.flow_worker.connect') as wss,patch('httpx.AsyncClient.send') as http:
                result=await flow_worker.local_preflight(self.config,self.settings,self.fresh,self.providers)
            self.assertEqual(result['gate'],'LOCAL_STARTUP_PREFLIGHT_PASS');wss.assert_not_called();http.assert_not_called()
        self.assertEqual(flow_segments.historical_debt(self.fresh)['unresolved'],66)
        self.assertEqual(self.db.conn.execute('SELECT state FROM flow_provider_switches WHERE id=?',(self.sid,)).fetchone()[0],'FAILED')

    async def test_version_cases_and_full_structure(self):
        await self.segment()
        for value in (None,'2','garbage',sqlite3.Binary(b'1')):
            self.fresh.conn.execute("DELETE FROM flow_state WHERE key='schema_version'")
            if value is not None:self.fresh.conn.execute("INSERT INTO flow_state VALUES('schema_version',?)",(value,))
            self.fresh.conn.commit()
            self.assertFalse(contract.validate_startup_contract(self.fresh.conn)['complete'])
        self.fresh.conn.execute("DELETE FROM flow_state WHERE key='schema_version'");self.fresh.conn.commit();contract.repair(self.fresh.conn)
        self.assertTrue(contract.validate_startup_contract(self.fresh.conn)['complete'])
        with self.assertRaises(sqlite3.IntegrityError):self.fresh.conn.execute("INSERT INTO flow_state VALUES('schema_version','2')")
        self.fresh.conn.rollback()
        for name,kind in (('flow_shadow_jobs','TABLE'),('flow_feature_versions_eligible','INDEX'),('flow_feature_versions_no_update','TRIGGER')):
            self.fresh.conn.execute('DROP '+kind+' '+name);self.fresh.conn.commit()
            self.assertFalse(contract.validate_startup_contract(self.fresh.conn)['complete']);contract.repair(self.fresh.conn)
            self.assertTrue(contract.validate_startup_contract(self.fresh.conn)['complete'])

    async def test_context_errors_block_before_provider_effects(self):
        await self.segment();segment=flow_segments.record(self.fresh);epoch=self.fresh.epoch()
        for field,value in (('epoch_id','wrong'),('start_block',segment['start_block']+1),('start_at',segment['start_at']+1),('db_path','/wrong'),('boundary_json','bad')):
            altered={**segment,field:value}
            result=contract.validate_startup_contract(self.fresh.conn,segment=altered,epoch=epoch,catalog_path=self.db.path)
            self.assertFalse(result['complete'])
        before=[tuple(x) for x in self.fresh.budget_conn.execute('SELECT * FROM flow_usage')]
        for key,value in (('schema_version','2'),('phase2b_coverage_start_at','bad'),('epoch_catalog_path','/wrong')):
            old=self.fresh.state(key);self.fresh.set_state(key,value)
            with patch('app.flow_worker.connect') as connect,patch('app.flow_worker.FlowRpc') as rpc:
                with self.assertRaises(contract.SegmentSchemaIncomplete):flow_worker.initialize_worker(self.config,self.settings,self.fresh,self.providers)
            connect.assert_not_called();rpc.assert_not_called();self.fresh.set_state(key,old)
        self.assertEqual([tuple(x) for x in self.fresh.budget_conn.execute('SELECT * FROM flow_usage')],before)

    async def test_interruptions_never_publish_partial_ready(self):
        await self.segment();self.fresh.conn.execute("DELETE FROM flow_state WHERE key='schema_version'");self.fresh.conn.commit()
        original=contract.initialize_metadata
        def fail_before(c):raise sqlite3.OperationalError('before metadata')
        def fail_after(c):original(c);raise sqlite3.OperationalError('after metadata')
        for injected in (fail_before,fail_after):
            with patch.object(contract,'initialize_metadata',side_effect=injected):
                with self.assertRaises(sqlite3.OperationalError):contract.repair(self.fresh.conn)
            self.assertIsNone(self.fresh.state('schema_version'));self.assertFalse(contract.validate_startup_contract(self.fresh.conn)['complete'])
        with patch.object(contract,'validate_startup_contract',side_effect=sqlite3.OperationalError('before commit')):
            with self.assertRaises(sqlite3.OperationalError):contract.repair(self.fresh.conn)
        self.assertIsNone(self.fresh.state('schema_version'));self.assertTrue(contract.repair(self.fresh.conn)['complete'])

    async def test_real_worker_run_reaches_first_network_boundary_offline(self):
        await self.segment()
        worker=flow_worker.initialize_worker(self.config,self.settings,self.fresh,self.providers)
        class NetworkBoundary(BaseException):pass
        try:
            with patch('app.flow_worker.connect',side_effect=NetworkBoundary) as connect:
                with self.assertRaises(NetworkBoundary):await worker.run()
            connect.assert_called_once()
            self.assertEqual(worker.db.state('schema_version'),'1')
        finally:await worker.rpc.close();worker.main.close()

    async def test_statement_and_commit_interruptions(self):
        await self.segment()
        db=self.fresh
        class Interrupted:
            def __init__(proxy,point):proxy.point=point
            def __getattr__(proxy,name):return getattr(db.conn,name)
            def execute(proxy,sql,*args):
                if proxy.point=='indexes' and sql.lstrip().upper().startswith(('CREATE INDEX','CREATE UNIQUE INDEX','CREATE TRIGGER')):
                    raise sqlite3.OperationalError('before index/trigger completion')
                return db.conn.execute(sql,*args)
            def commit(proxy):
                if proxy.point=='before_commit':raise sqlite3.OperationalError('before commit')
                db.conn.commit()
                if proxy.point=='after_commit':raise sqlite3.OperationalError('lost confirmation')
        for point in ('indexes','before_commit','after_commit'):
            db.conn.execute("DELETE FROM flow_state WHERE key='schema_version'");db.conn.commit()
            with self.assertRaises(sqlite3.OperationalError):contract.repair(Interrupted(point))
            self.assertEqual(contract.validate_startup_contract(db.conn)['complete'],point=='after_commit')
            self.assertTrue(contract.repair(db.conn)['complete'])

    async def test_case_sensitive_sql_literals_are_not_normalized_away(self):
        await self.segment()
        self.fresh.conn.execute('DROP VIEW curve_trade_events')
        self.fresh.conn.execute("CREATE VIEW curve_trade_events AS SELECT * FROM flow_events WHERE phase='CURVE'")
        self.fresh.conn.commit()
        self.assertIn('incompatible definition:curve_trade_events',contract.validate_startup_contract(self.fresh.conn)['failures'])

for name in ('asyncSetUp','asyncTearDown','failure','quarantine','fake_rpc','rollover','bootstrap_epoch_target','reopened_worker','segment','active_segment'):
    setattr(StartupContractTests,name,getattr(fixture.SegmentTests,name))
