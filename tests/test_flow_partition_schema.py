"""Empty partitions must be complete before any shadow reconciler is constructed."""
import sqlite3,tempfile,unittest
from pathlib import Path
from app.flow_data import FlowDB


class PartitionSchemaTests(unittest.TestCase):
    def test_fresh_core_initializer_includes_all_shadow_tables(self):
        with tempfile.TemporaryDirectory() as folder:
            db=FlowDB(Path(folder)/'segment.db',follow_epoch=False)
            try:
                db.migrate()
                # The production empty-segment path failed on this exact first health query.
                self.assertIsNone(db.conn.execute("SELECT 1 FROM flow_shadow_jobs WHERE completion_status!='complete' LIMIT 1").fetchone())
                for name in ('flow_shadow_meta','flow_shadow_jobs','flow_shadow_ranges'):
                    self.assertEqual(db.conn.execute('SELECT count(*) FROM '+name).fetchone()[0],0)
            finally:db.close()

from unittest.mock import patch,AsyncMock,Mock
from app import flow_partition_schema as schema


class RepairTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.db=FlowDB(Path(self.folder.name)/'segment.db',follow_epoch=False)
        self.addCleanup(self.db.close);self.db.migrate(shared_budget=True)

    def remove_shadow(self):
        for t in schema.SHADOW_TABLES:self.db.conn.execute('DROP TABLE '+t)
        self.db.conn.commit()

    def test_complete_registry_and_idempotent_core(self):
        tables={r[0] for r in self.db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables-{'sqlite_sequence'},schema.MANDATORY_TABLES)
        self.assertFalse(tables & schema.GLOBAL_TABLES)
        self.assertEqual(schema.missing(self.db.conn),[])
        self.db.migrate(shared_budget=True)
        self.assertEqual(schema.missing(self.db.conn),[])

    def test_atomic_interruptions_and_postcommit_rerun(self):
        self.remove_shadow()
        for stop in range(4):
            def interrupted(conn):
                for i,sql in enumerate(schema.SHADOW_DDL):
                    if i==stop:raise sqlite3.OperationalError('injected interruption')
                    conn.execute(sql)
                raise sqlite3.OperationalError('before index/trigger validation')
            with patch.object(schema,'create_shadow',side_effect=interrupted):
                with self.assertRaises(sqlite3.OperationalError):schema.repair(self.db.conn)
            self.assertEqual(set(schema.missing(self.db.conn)),set(schema.SHADOW_TABLES))
        result=schema.repair(self.db.conn)
        self.assertEqual(result['added_tables'],list(schema.SHADOW_TABLES))
        self.assertEqual(result['proof_rows_seeded'],0)
        self.assertEqual(schema.repair(self.db.conn)['added_tables'],[])
        self.assertEqual(self.db.conn.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    def test_prior_partial_ddl_converges(self):
        for retained in (1,2):
            self.remove_shadow()
            for sql in schema.SHADOW_DDL[:retained]:self.db.conn.execute(sql)
            self.db.conn.commit()
            self.assertEqual(len(schema.repair(self.db.conn)['added_tables']),3-retained)

    def test_lost_confirmation_after_commit_reruns_without_seeding(self):
        self.remove_shadow()
        class LostConfirmation:
            def __getattr__(proxy,name):return getattr(self.db.conn,name)
            def commit(proxy):
                self.db.conn.commit()
                raise sqlite3.OperationalError('lost commit confirmation')
        with self.assertRaises(sqlite3.OperationalError):schema.repair(LostConfirmation())
        self.assertEqual(schema.repair(self.db.conn)['added_tables'],[])
        for table in schema.SHADOW_TABLES:self.assertEqual(self.db.conn.execute('SELECT count(*) FROM '+table).fetchone()[0],0)

    def test_incompatible_columns_and_missing_trigger_block(self):
        self.db.conn.execute('DROP TABLE flow_shadow_jobs')
        self.db.conn.execute('CREATE TABLE flow_shadow_jobs(wrong INTEGER)');self.db.conn.commit()
        with self.assertRaisesRegex(ValueError,'incompatible_columns'):schema.repair(self.db.conn)
        self.db.conn.execute('DROP TRIGGER flow_feature_versions_no_update');self.db.conn.commit()
        self.assertIn('flow_feature_versions_no_update',schema.missing(self.db.conn))


from tests import test_flow_research_segments as segment_fixture
from app import flow_segments,flow_epochs,flow_provider_switch
import asyncio
import json


class SegmentSchemaTests(unittest.IsolatedAsyncioTestCase):
    def drop_shadow(self):
        for table in schema.SHADOW_TABLES:self.fresh.conn.execute('DROP TABLE '+table)
        self.fresh.conn.commit()

    async def test_existing_segment_upgrade_preserves_identity_history_and_no_pit(self):
        await self.segment()
        before=flow_segments.record(self.fresh)
        partition={t:[tuple(r) for r in self.fresh.conn.execute('SELECT * FROM '+t)]
                   for t in schema.MANDATORY_TABLES-set(schema.SHADOW_TABLES)}
        old=[dict(r) for r in self.parent.conn.execute('SELECT * FROM flow_feature_versions')]
        self.drop_shadow()
        self.assertEqual(self.fresh.current_health(),'SEGMENT_SCHEMA_INCOMPLETE')
        result=schema.repair(self.fresh.conn)
        self.assertEqual(result['added_tables'],list(schema.SHADOW_TABLES))
        self.assertEqual(flow_segments.record(self.fresh),before)
        self.assertEqual({t:[tuple(r) for r in self.fresh.conn.execute('SELECT * FROM '+t)] for t in partition},partition)
        self.assertEqual(flow_segments.historical_debt(self.fresh)['unresolved'],66)
        self.assertTrue(flow_segments.historical_debt(self.fresh)['original_rows_unchanged'])
        self.assertEqual([dict(r) for r in self.parent.conn.execute('SELECT * FROM flow_feature_versions')],old)
        self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM flow_feature_versions').fetchone()[0],0)
        for table in schema.SHADOW_TABLES:self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM '+table).fetchone()[0],0)
        self.assertEqual(schema.repair(self.fresh.conn)['added_tables'],[])
        self.assertIsNone(flow_segments.record(self.fresh)['validated_at'])

    async def test_missing_schema_blocks_start_and_existing_connection_without_provider_effects(self):
        worker,t=await self.active_segment()
        before=[tuple(r) for r in self.fresh.budget_conn.execute('SELECT * FROM flow_usage ORDER BY minute,metric')]
        connections=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_provider_connections')]
        switches=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_provider_switches')]
        self.drop_shadow()
        with self.assertRaises(schema.SegmentSchemaIncomplete):await worker.reconcile()
        self.assertFalse(flow_epochs.seal_live(worker))
        async def cancel_wait(_event):raise asyncio.CancelledError
        with patch('app.flow_worker.connect') as connect,patch('asyncio.Event.wait',cancel_wait):
            with self.assertRaises(asyncio.CancelledError):await worker.run()
        connect.assert_not_called()
        self.assertEqual(self.fresh.state('service_status'),'SEGMENT_SCHEMA_INCOMPLETE')
        self.assertIn('flow_shadow_jobs',self.fresh.state('local_database_diagnostic'))
        self.assertEqual([tuple(r) for r in self.fresh.budget_conn.execute('SELECT * FROM flow_usage ORDER BY minute,metric')],before)
        self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_provider_connections')],connections)
        self.assertEqual([dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_provider_switches')],switches)

    async def test_schema_missing_cannot_append_eligible_or_validate_research(self):
        worker,t=await self.active_segment()
        with patch('app.flow_data.time.time',return_value=1440):self.fresh.rebuild(t,1440)
        rows=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')]
        self.drop_shadow()
        with patch('app.flow_data.time.time',return_value=1450):self.fresh.rebuild(t,1450)
        latest=[dict(r) for r in self.fresh.conn.execute('SELECT * FROM flow_feature_versions')]
        self.assertEqual(latest[:len(rows)],rows)
        self.assertFalse(any(r['model_eligible_at'] for r in latest[len(rows):]))
        self.assertFalse(flow_segments.validate_fresh(self.fresh,1450))
        from scripts.phase2c_dataset_audit import open_readonly
        from scripts.phase2c_point_in_time_audit import ledger_audit
        with open_readonly(self.config.database,self.db.path) as joined:
            report=ledger_audit(joined,1450)
        self.assertFalse(report['primary_eligible'])
        self.assertIn('flow_shadow_jobs',report['partition_schema']['missing'])
        # Even a retained VALIDATED record cannot override missing proof schema;
        # the read-only failure must not discard its historical fixed boundary.
        with self.fresh.catalog_conn:self.fresh.catalog_conn.execute("UPDATE flow_research_segments SET status='VALIDATED'")
        with open_readonly(self.config.database,self.db.path) as joined:
            blocked=ledger_audit(joined,1450)
        self.assertEqual(blocked['research_clean_start'],flow_segments.record(self.fresh)['start_at'])
        self.assertFalse(blocked['primary_eligible'])

    async def test_complete_fixture_proof_switch_tail_and_pit(self):
        worker,t=await self.active_segment()
        self.assertEqual(schema.missing(self.fresh.conn),[])
        self.assertTrue(self.fresh.conn.execute('SELECT * FROM flow_shadow_ranges').fetchone())
        self.assertEqual(self.fresh.current_health(),'healthy')
        self.assertTrue(json.loads(self.fresh.state('epoch_tail_proof:200:curve'))['acknowledged'])
        with patch('app.flow_data.time.time',return_value=1440):self.fresh.rebuild(t,1440)
        self.assertTrue(self.fresh.conn.execute('SELECT 1 FROM flow_feature_versions WHERE model_eligible_at IS NOT NULL').fetchone())
        sid,item=flow_provider_switch.start(self.fresh,'publicnode',worker.switch_filters(),[],now=1441)
        self.assertIsNotNone(sid);self.assertTrue(item['filters'])

    async def test_repair_cli_refuses_running_flow_without_writes(self):
        from scripts.flow_segment_schema import repair_existing
        await self.segment();self.drop_shadow()
        with patch('scripts.flow_segment_schema.service',return_value={'ActiveState':'active','MainPID':'42'}):
            with self.assertRaisesRegex(ValueError,'FLOW_STOP_REQUIRED'):repair_existing(self.db.path,'clean1','epoch2',1,1)
        self.assertEqual(set(schema.missing(self.fresh.conn)),set(schema.SHADOW_TABLES))

    async def test_stopped_repair_cli_exact_boundary_and_idempotency(self):
        from scripts.flow_segment_schema import repair_existing
        await self.segment();self.drop_shadow();before=flow_segments.record(self.fresh)
        with patch('scripts.flow_segment_schema.service',return_value={'ActiveState':'inactive','MainPID':'0'}),\
             patch('scripts.flow_segment_schema.verified_checkout',return_value='d'*40):
            with self.assertRaisesRegex(ValueError,'Exact existing'):
                repair_existing(self.db.path,'clean1','epoch2',before['start_block']+1,before['start_at'])
            self.assertEqual(set(schema.missing(self.fresh.conn)),set(schema.SHADOW_TABLES))
            result=repair_existing(self.db.path,'clean1','epoch2',before['start_block'],before['start_at'])
            self.assertEqual(result['added_tables'],list(schema.SHADOW_TABLES))
            self.assertEqual(repair_existing(self.db.path,'clean1','epoch2',before['start_block'],before['start_at'])['added_tables'],[])
        self.assertEqual(flow_segments.record(self.fresh),before)

    async def connected_local_failure(self,missing_table):
        worker,t=await self.active_segment()
        async def read():await asyncio.Future()
        async def reconcile():
            if missing_table:
                self.drop_shadow();schema.require_segment(self.fresh)
            raise sqlite3.OperationalError('no such table: local_fixture')
        async def cancel_wait(_event):raise asyncio.CancelledError
        connection=AsyncMock()
        worker.command=AsyncMock(return_value=hex(4663));worker.read_socket=read
        worker.reconcile=reconcile;worker.pressure=Mock(return_value=None)
        worker.secondary_ws_bytes=Mock(return_value=0)
        with patch('app.flow_worker.connect',return_value=connection) as connect,\
             patch('app.flow_worker.time.time',return_value=1445),patch('asyncio.Event.wait',cancel_wait):
            with self.assertRaises(asyncio.CancelledError):await worker.run()
        connect.assert_called_once()
        self.assertEqual(self.fresh.used('flow_provider_connection_errors_publicnode',0),0)
        self.assertEqual(self.fresh.used('flow_provider_connection_errors_validation',0),0)
        self.assertEqual(self.fresh.used('flow_provider_failovers',0),0)
        self.assertEqual(self.fresh.conn.execute('SELECT count(*) FROM flow_provider_switches').fetchone()[0],0)
        self.assertEqual(self.fresh.state('recovery_state'),'SEGMENT_SCHEMA_INCOMPLETE' if missing_table else 'LOCAL_DATABASE_ERROR')

    async def test_connected_missing_schema_never_reconnects_or_fails_over(self):
        await self.connected_local_failure(True)

    async def test_local_sqlite_exception_is_not_a_provider_failure(self):
        await self.connected_local_failure(False)


for _name in ('asyncSetUp','asyncTearDown','failure','quarantine','fake_rpc','rollover','bootstrap_epoch_target',
              'reopened_worker','segment','active_segment'):
    setattr(SegmentSchemaTests,_name,getattr(segment_fixture.SegmentTests,_name))
