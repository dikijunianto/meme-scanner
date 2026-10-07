"""The old cutover is audit evidence; a new attempt has its own empty ledger."""
import asyncio
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.flow_cutover import (ACCEPTANCE,abort_empty,abort_pre_stop,accept_validation,advance,
                              authorize_stop,begin_validation,complete_soak,create,current,
                              import_rolled_back_legacy,legacy_digest,mark_source_stopped,
                              mark_split_configured,mark_stop_issued,new,record_operational_outcome,
                              record_rollback,rollback_intent,require_phase_pid,
                              save,schema,status)
from app.flow_data import FlowDB


class SessionLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'flow.db'
        self.db=FlowDB(self.path)
        self.db.migrate()
        self.db.set_state('current_wss_provider','alchemy')
        with self.db.conn:
            self.db.conn.executescript('''
              INSERT INTO flow_shadow_meta VALUES('git_revision','old-revision');
              INSERT INTO flow_shadow_meta VALUES('old_flow_pid','1745059');
              INSERT INTO flow_shadow_meta VALUES('H_prefetch','10');
              INSERT INTO flow_shadow_meta VALUES('H_pre_stop','12');
              INSERT INTO flow_shadow_meta VALUES('H_stop','14');
              INSERT INTO flow_shadow_jobs(stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
                next_unverified_block,highest_contiguous_verified_block,completion_status)
                VALUES('historical',1,'curve',1,10,11,10,'complete');
              INSERT INTO flow_shadow_jobs(stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
                next_unverified_block,highest_contiguous_verified_block,completion_status)
                VALUES('stop_tail',1,'curve',11,14,15,14,'complete');
              INSERT INTO flow_shadow_ranges VALUES('historical',1,'curve',1,10,1);
              INSERT INTO flow_shadow_ranges VALUES('stop_tail',1,'curve',11,14,1);
            ''')

    def tearDown(self):
        self.db.conn.close();self.tmp.cleanup()

    def archive(self):
        return import_rolled_back_legacy(self.db,'1963150')

    def fresh(self):
        return create(self.db,revision='new-revision',source_pid='1963150',
                      source_start='100',main_pid='64326',roles_fingerprint='roles')

    def complete_job(self,stage):
        first,last=(11,14) if stage.endswith('stop_tail') else (1,10)
        with self.db.conn:self.db.conn.execute('''INSERT INTO flow_shadow_jobs(
          stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
          next_unverified_block,highest_contiguous_verified_block,completion_status)
          VALUES(?,1,'curve',?,?,?,?,'complete')''',(stage,first,last,last+1,last))

    def test_old_terminal_revision_is_visible_and_immutable(self):
        before=legacy_digest(self.db)
        old=self.archive()
        self.assertEqual(self.archive()['id'],old['id'])
        self.assertEqual(legacy_digest(self.db),before)
        self.assertEqual(self.db.conn.execute('''SELECT count(*) FROM flow_cutover_legacy_proof
          WHERE session_id=?''',(old['id'],)).fetchone()[0],before[1]+before[2])
        report=status(self.db)
        self.assertIsNone(report['current_active'])
        self.assertEqual(report['historical_latest_terminal']['deploy_git_revision'],'old-revision')
        self.assertEqual(report['historical_latest_terminal']['source_legacy_pid'],'1745059')
        self.assertEqual(report['historical_latest_terminal']['status'],'ROLLED_BACK')
        fresh=self.fresh()
        self.assertNotEqual(fresh['id'],old['id'])
        self.assertEqual(legacy_digest(self.db),before)
        self.assertEqual(status(self.db)['historical_latest_terminal']['legacy_proof_digest'],before[0])

    def test_nonterminal_old_session_blocks_fresh_creation(self):
        self.archive()
        first=self.fresh()
        with self.assertRaises(ValueError):self.fresh()
        self.assertEqual(current(self.db)['id'],first['id'])
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_cutover_sessions').fetchone()[0],2)

    def test_new_session_is_empty_abortable_and_status_is_read_only(self):
        old=self.archive();fresh=self.fresh()
        self.assertEqual([fresh[x] for x in ('H_prefetch','H_pre_stop','H_stop','H_live')],[None]*4)
        self.assertEqual(fresh['targets'],[])
        self.assertEqual(fresh['source_legacy_pid'],'1963150')
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_shadow_jobs WHERE stage LIKE 'cutover:%'").fetchone()[0],0)
        before=self.path.read_bytes()
        readonly=FlowDB(self.path,readonly=True)
        try:
            self.assertEqual(status(readonly)['current_active']['id'],fresh['id'])
        finally:readonly.conn.close()
        self.assertEqual(self.path.read_bytes(),before)
        abort_empty(self.db)
        self.assertIsNone(current(self.db))
        self.assertEqual(len(status(self.db)['historical_sessions']),2)
        self.assertEqual(status(self.db)['historical_sessions'][1]['id'],old['id'])

    def test_creation_failure_and_concurrent_duplicate_leave_one_session(self):
        self.archive()
        with patch('app.flow_cutover.uuid4',side_effect=OSError('crash')):
            with self.assertRaises(OSError):self.fresh()
        self.assertIsNone(current(self.db))
        created=self.fresh()
        other=FlowDB(self.path)
        try:
            with self.assertRaises(ValueError):create(other,revision='other',source_pid='2',
                source_start='2',main_pid='3',roles_fingerprint='other')
        finally:other.conn.close()
        self.assertEqual(current(self.db)['id'],created['id'])

    def test_revision_and_source_identity_are_immutable(self):
        self.archive();fresh=self.fresh()
        for key,value in (('revision','changed'),('source_legacy_pid','changed')):
            with self.assertRaises(ValueError):
                with self.db.conn:save(self.db,dict(fresh,**{key:value}))
        self.assertEqual(status(self.db)['current_active']['deploy_git_revision'],'new-revision')
        self.assertEqual(status(self.db)['current_active']['source_legacy_pid'],'1963150')

    def test_phase_pid_and_rollback_pid_are_independent(self):
        self.archive();fresh=self.fresh()
        require_phase_pid(fresh,'prefetch','1963150','100')
        with self.assertRaises(ValueError):require_phase_pid(fresh,'prefetch','unexpected','100')
        with self.assertRaises(ValueError):require_phase_pid(fresh,'prefetch','1963150','unexpected')
        require_phase_pid(fresh,'stop-tail','0')
        with self.assertRaises(ValueError):require_phase_pid(fresh,'stop-tail','1963150')
        split=advance(self.db,fresh,'SPLIT_WSS_CONNECTING',split_pid='2000000')
        require_phase_pid(split,'ready-tail','2000000')
        with self.assertRaises(ValueError):require_phase_pid(split,'ready-tail','1745059')
        from app.flow_cutover import record_rollback
        rolled=record_rollback(self.db,split,'3000000')
        self.assertEqual(rolled['source_legacy_pid'],'1963150')
        self.assertEqual(rolled['split_pid'],'2000000')
        self.assertEqual(rolled['rollback_pid'],'3000000')

    def test_old_proof_cannot_satisfy_new_session(self):
        self.archive();fresh=self.fresh()
        with self.assertRaises(ValueError):new(self.db,10,14,[])
        fresh=advance(self.db,fresh,'SHADOW_VERIFIED',H_prefetch=10,shadow_proof='verified')
        with self.assertRaises(ValueError):new(self.db,10,14,[])
        for stage in ('historical','stop_tail'):
            self.complete_job(f'cutover:{fresh["id"]}:{stage}')
        advance(self.db,fresh,'SOURCE_STOPPED',source_stopped_at='2026-09-27T00:00:00Z')
        self.assertEqual(new(self.db,10,14,[])['state'],'STOP_TAIL_VERIFIED')
        self.assertEqual(legacy_digest(self.db)[1],2)

    def test_operator_status_and_new_session_need_no_rpc(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        from scripts import phase2b2_shadow as script
        self.archive()
        main=Path(self.tmp.name)/'main.db'
        connection=sqlite3.connect(main);connection.close()
        self.db.set_state('service_status','connected')
        self.db.set_state('connection_state','connected')
        self.db.set_state('recovery_state','healthy')
        settings=SimpleNamespace(database=self.path,split_enabled=False)
        provider=SimpleNamespace(fingerprints=lambda:{'ws_primary':'publicnode',
            'ws_fallback':'validation','http':'validation'})
        def service(name):
            return {'ActiveState':'active','MainPID':'64326' if name=='meme-scanner.service' else '1963150',
                    'NRestarts':'0','ExecMainStartTimestampMonotonic':'100'}
        with (patch.object(script,'service',side_effect=service),
              patch.object(script.FlowSettings,'load',return_value=settings),
              patch.object(script.Config,'load',return_value=SimpleNamespace(database=main)),
              patch.object(script.FlowProviders,'load',return_value=provider),
              patch.object(script,'verified_checkout',return_value='new-revision'),
              patch.object(script,'prestart_check',AsyncMock(return_value={'no_network':True})),
              patch.object(script,'make_reconciler',side_effect=AssertionError('RPC path used')),
              patch.object(script,'chain_ids',side_effect=AssertionError('Provider call used')),
              patch.object(script.subprocess,'check_output',side_effect=['new-revision\n',''])):
            before=asyncio.run(script.operate('status'))
            self.assertTrue(before['historical_session_revision_mismatch'])
            self.assertIsNone(before['current_active'])
            created=asyncio.run(script.operate('new-session'))
        self.assertEqual(created['gate'],'CUTOVER_SESSION_CREATED')
        self.assertEqual(created['H_prefetch'],None)
        self.assertEqual(current(self.db)['source_legacy_pid'],'1963150')

    def test_shadowed_and_authorized_abort_keep_proof_and_allow_fresh_attempt(self):
        self.archive();fresh=self.fresh()
        stage=f'cutover:{fresh["id"]}:historical'
        self.complete_job(stage)
        with self.db.conn:
            self.db.conn.execute('INSERT INTO flow_shadow_ranges VALUES(?,?,?,?,?,?)',
                                 (stage,1,'curve',1,10,1))
        shadow=advance(self.db,fresh,'SHADOW_VERIFIED',H_prefetch=10,targets=[{'launch_id':1}],
                       shadow_proof='verified')
        aborted=abort_pre_stop(self.db,shadow,'target_expired',source_pid='1963150',
                               source_start='100',route='alchemy',split=False)
        self.assertEqual(aborted['state'],'ABORTED_PRE_STOP')
        self.assertEqual(aborted['H_prefetch'],10)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_shadow_ranges WHERE stage=?',
                                             (stage,)).fetchone()[0],1)
        with self.assertRaises(ValueError):advance(self.db,aborted,'SHADOW_VERIFIED')
        second=self.fresh()
        self.assertNotEqual(second['id'],fresh['id'])
        self.assertIsNone(second['H_prefetch'])
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_shadow_jobs WHERE stage LIKE ?',
                         (f'cutover:{second["id"]}:%',)).fetchone()[0],0)
        second=advance(self.db,second,'SHADOW_VERIFIED',H_prefetch=20,shadow_proof='verified')
        authorized=authorize_stop(self.db,second,{'H_pre_stop':22,
            'candidate_config_fingerprint':'candidate'})
        aborted2=abort_pre_stop(self.db,authorized,'operator_cancelled',source_pid='1963150',
                                source_start='100',route='alchemy',split=False)
        self.assertEqual(aborted2['state'],'ABORTED_PRE_STOP')
        self.assertIsNone(current(self.db))

    def test_stop_intent_and_split_match_are_durable(self):
        self.archive();fresh=self.fresh()
        fresh=advance(self.db,fresh,'SHADOW_VERIFIED',H_prefetch=10,shadow_proof='verified')
        authorized=authorize_stop(self.db,fresh,{'H_pre_stop':12,
            'candidate_config_fingerprint':'candidate'})
        self.assertIsNone(authorized.get('source_stopped_at'))
        with self.assertRaises(ValueError):mark_source_stopped(self.db,authorized)
        issued=mark_stop_issued(self.db,authorized)
        self.assertIsNotNone(status(self.db)['current_active']['stop_command_issued_at'])
        stopped=mark_source_stopped(self.db,issued)
        self.assertEqual(stopped['state'],'SOURCE_STOPPED')
        with self.assertRaises(ValueError):abort_pre_stop(self.db,stopped,'too_late',
            source_pid='1963150',source_start='100',route='alchemy',split=False)
        for stage in ('historical','stop_tail'):
            self.complete_job(f'cutover:{fresh["id"]}:{stage}')
        tail=new(self.db,10,14,[])
        with self.assertRaises(ValueError):mark_split_configured(self.db,tail,'different')
        configured=mark_split_configured(self.db,tail,'candidate')
        self.assertEqual(configured['state'],'SPLIT_CONFIGURED')

    def test_abort_requires_original_live_legacy_process(self):
        self.archive();fresh=self.fresh()
        for kwargs in ({'source_pid':'different','source_start':'100','route':'alchemy','split':False},
                       {'source_pid':'1963150','source_start':'changed','route':'alchemy','split':False},
                       {'source_pid':'1963150','source_start':'100','route':'publicnode','split':False},
                       {'source_pid':'1963150','source_start':'100','route':'alchemy','split':True}):
            with self.assertRaises(ValueError):abort_pre_stop(self.db,fresh,'invalid',**kwargs)
        self.assertEqual(current(self.db)['id'],fresh['id'])

    def test_validation_soak_and_completion_require_elapsed_reviewed_evidence(self):
        self.archive();fresh=self.fresh()
        ready=advance(self.db,fresh,'READY_TAIL_VERIFIED',ready_tail_proof='verified')
        validating=begin_validation(self.db,ready)
        self.assertEqual(status(self.db)['current_active']['session_phase'],'POST_CUTOVER_VALIDATING')
        evidence={key:True for key in ACCEPTANCE}
        with self.assertRaises(ValueError):accept_validation(self.db,validating,evidence,
            now=validating['validation_started_at']+1799)
        with self.assertRaises(ValueError):accept_validation(self.db,validating,dict(evidence,no_alchemy=False),
            now=validating['validation_started_at']+1800)
        soaking=accept_validation(self.db,validating,evidence,now=validating['validation_started_at']+1800)
        self.assertEqual(soaking['state'],'SOAKING')
        with self.assertRaises(ValueError):complete_soak(self.db,soaking,evidence,
            now=soaking['soak_started_at']+86399)
        complete=complete_soak(self.db,soaking,evidence,now=soaking['soak_started_at']+86400)
        self.assertEqual(complete['state'],'COMPLETE')
        self.assertIsNone(current(self.db))

    def test_post_handoff_rollback_from_validation_and_soak(self):
        self.archive()
        for soak in (False,True):
            fresh=self.fresh()
            ready=advance(self.db,fresh,'READY_TAIL_VERIFIED',ready_tail_proof='verified',
                          split_pid='2000000')
            validating=begin_validation(self.db,ready)
            state=accept_validation(self.db,validating,{key:True for key in ACCEPTANCE},
                now=validating['validation_started_at']+1800) if soak else validating
            intent=rollback_intent(self.db,state,reason='provider_switch_unproved',
                provider='validation',unresolved_ranges=[[1,'curve',10,20]])
            with self.assertRaises(ValueError):record_rollback(self.db,intent,'3000000')
            rolled=record_rollback(self.db,intent,'3000000',proof={
                'legacy_route_connected':True,'active_unresolved_gaps':0,
                'recovery_state':'healthy'})
            self.assertEqual(rolled['state'],'ROLLED_BACK')
            self.assertEqual(rolled['source_legacy_pid'],'1963150')
            self.assertEqual(rolled['split_pid'],'2000000')
            self.assertEqual(rolled['rollback_pid'],'3000000')
            self.assertEqual(rolled['provider_at_failure'],'validation')
            self.assertEqual(status(self.db)['historical_latest_terminal']['operational_outcome']
                             ['rollback_reason'],'provider_switch_unproved')
            self.assertTrue(status(self.db)['historical_latest_terminal']['operational_outcome']
                            ['later_operational_rollback'])

    def test_completed_legacy_handoff_accepts_append_only_later_outcome(self):
        self.archive();fresh=self.fresh()
        complete=advance(self.db,fresh,'COMPLETE',ready_tail_proof='verified',
                         H_prefetch=10,H_stop=14,H_live=16,split_pid='2000000')
        before=self.db.conn.execute('SELECT payload FROM flow_cutover_sessions WHERE id=?',
                                    (fresh['id'],)).fetchone()[0]
        proof={'zero_active_filters':True,'rollback_gap_seconds':13.2}
        first=record_operational_outcome(self.db,complete,reason='primary_wss_validation_policy/provider_disconnect',
            rollback_pid='2091349',provider='validation',reconciliation_proof=proof)
        self.assertEqual(record_operational_outcome(self.db,complete,
            reason='primary_wss_validation_policy/provider_disconnect',rollback_pid='2091349',
            provider='validation',reconciliation_proof=proof),first)
        self.assertEqual(self.db.conn.execute('SELECT payload FROM flow_cutover_sessions WHERE id=?',
                                             (fresh['id'],)).fetchone()[0],before)
        reported=status(self.db)['historical_latest_terminal']
        self.assertEqual(reported['status'],'COMPLETE')
        self.assertTrue(reported['operational_outcome']['later_operational_rollback'])
        self.assertEqual(reported['operational_outcome']['rollback_pid'],'2091349')


if __name__=='__main__':unittest.main()
