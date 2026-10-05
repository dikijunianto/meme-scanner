import copy
import hashlib
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

from app import flow_provider_switch as switch
from app.flow_identity import canonical_address, query_identity
from app.flow_switch_recovery import plan, recover, workload, semantics, finish
from app.flow_shadow import ShadowReconciler
from app.rpc import Rpc, LogRangeError
from app.flow_budget import BudgetWait
from tests.test_flow import target, insert_target, event
from tests import test_flow_switch_fix as fixtures


class SwitchPlanTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.SwitchFixTests.asyncSetUp
    asyncTearDown = fixtures.SwitchFixTests.asyncTearDown
    failed_fixture = fixtures.SwitchFixTests.failed_fixture

    def test_address_identity_validates_only_addresses_and_preserves_hashes(self):
        display='0x'+'aBcD'*10
        payload=json.dumps({'address':display,'topics':['0xAa'],'pool_id':'0xBc'})
        digest=hashlib.sha256(payload.encode()).hexdigest()
        for equivalent in (display,display.lower(),'0x'+'AbCd'*10):
            self.assertEqual(canonical_address(equivalent),canonical_address(display))
        self.assertNotEqual(canonical_address(display),canonical_address('0x'+'ab'*20))
        for bad in (None,1,'abc','0x'+'a'*39,'0x'+'a'*41,'0x'+'g'*40,'0x'+'a'*64):
            with self.subTest(bad=bad), self.assertRaises(ValueError):canonical_address(bad)
        original=json.loads(payload);derived=query_identity(original)
        self.assertEqual(original['address'],display)
        self.assertEqual(derived['topics'],['0xAa']);self.assertEqual(derived['pool_id'],'0xBc')
        self.assertEqual(hashlib.sha256(json.dumps(original).encode()).hexdigest(),digest)

    def add_proof(self,launch,first,last,stage=None,query=None,lifecycle=None,legacy=False):
        stage=stage or f'fixture_proof:{launch}:{first}'
        t=self.db.target(launch)
        from app.flow_data import BUY,SELL
        query=query or {'address':t['curve_address'],'topics':[[BUY,SELL]]}
        with self.db.conn:
            self.db.conn.execute('''INSERT INTO flow_shadow_jobs
              (stage,launch_id,kind,original_safe_start,reconciliation_upper_bound,
               next_unverified_block,highest_contiguous_verified_block,completion_status)
              VALUES(?,?,'curve',?,?,?,?, 'complete')''',(stage,launch,first,last,last+1,last))
            self.db.conn.execute('INSERT INTO flow_shadow_ranges VALUES(?,?,?,?,?,1)',(stage,launch,'curve',first,last))
            self.db.conn.execute('INSERT OR REPLACE INTO flow_bootstrap_identity VALUES(?,?,?,?,?)',
                (stage,launch,'curve',json.dumps(query),time.time()-100))
            if legacy:
                self.db.conn.execute("INSERT INTO flow_bootstrap VALUES(?, 'curve',?, 'complete',?,?,?)",
                                    (launch,first,last,time.time()-110,time.time()-100))
            else:
                self.db.conn.execute('INSERT INTO flow_shadow_meta VALUES(?,?)',
                    (stage+f':semantics:{launch}:curve',lifecycle or semantics(self.db,{'launch_id':launch})))
        return stage

    async def test_checksum_snapshot_and_legacy_proof_preserve_original_bytes(self):
        self.db.activate_pit_ledger('a'*40,self.base,now=900)
        self.db.require_bootstrap(self.t,'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.base+33)
        self.db.rebuild(self.db.target(1),1060)
        pit_before=[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')]
        self.assertTrue(pit_before)
        with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET curve_address=? WHERE launch_id=2",('0x'+'Cd'*20,))
        self.main.execute("UPDATE launches SET curve_address=? WHERE id=2",('0x'+'cD'*20,));self.main.commit()
        self.failed_fixture()
        stage=self.add_proof(2,self.base,self.base+33,stage='live_bootstrap:2',legacy=True)
        before=[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_bootstrap_identity')]
        _,item,filters=plan(self.db,self.runner.worker.main,self.sid)
        work=workload(self.db,item,filters,self.head)
        self.assertEqual(work[1]['completed_ranges'],[[self.base,self.base+33]])
        result=await recover(self.runner,self.sid,'a'*40,True)
        self.assertTrue(result['complete'])
        self.assertEqual(switch.value(switch.latest(self.db))['failure_history']['payload_raw'],self.raw)
        self.assertEqual(before,[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_bootstrap_identity WHERE stage NOT LIKE ?',
            (f'provider_switch:{self.sid}:%',))])
        self.assertEqual(self.db.conn.execute('SELECT curve_address FROM flow_tracking_targets WHERE launch_id=2').fetchone()[0],'0x'+'Cd'*20)
        self.assertEqual(work[1]['reusable_proof'][0]['stage'],stage)
        self.assertEqual(pit_before,[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')])

    def test_topics_kind_lifecycle_and_other_filter_never_reuse(self):
        self.failed_fixture();_,item,filters=plan(self.db,self.runner.worker.main,self.sid)
        q=dict(filters[0]['query'],topics=[['different']])
        self.add_proof(1,self.base,self.base+10,query=q)
        self.add_proof(1,self.base+11,self.base+20,lifecycle='changed lifecycle')
        stage=self.add_proof(1,self.base+21,self.base+30)
        with self.db.conn:
            for table in ('flow_shadow_jobs','flow_shadow_ranges','flow_bootstrap_identity'):
                self.db.conn.execute(f"UPDATE {table} SET kind='hook' WHERE stage=?",(stage,))
        self.add_proof(2,self.base,self.base+100)
        work=workload(self.db,item,filters,self.head)
        self.assertEqual(work[0]['completed_ranges'],[])
        self.assertEqual(work[1]['completed_ranges'],[[self.base,self.base+100]])

    async def test_proof_islands_and_adjacent_ranges_skip_requests(self):
        self.failed_fixture()
        for launch in (1,2):
            self.add_proof(launch,self.base,self.base+9)
            self.add_proof(launch,self.base+10,self.base+19)
            self.add_proof(launch,self.base+100,self.base+199)
        _,item,filters=plan(self.db,self.runner.worker.main,self.sid)
        self.assertEqual(workload(self.db,item,filters,self.head)[0]['completed_ranges'],
                         [[self.base,self.base+19],[self.base+100,self.base+199]])
        self.assertTrue((await recover(self.runner,self.sid,'a'*40,True))['complete'])
        self.assertEqual(self.calls[:2],[(self.base+20,self.base+99),(self.base+200,self.base+2199)])
        self.assertFalse(any(a<=self.base+199 and b>=self.base+100 for a,b in self.calls))

    async def test_multiple_actual_utc_budget_resets_fixed_head_and_interruption(self):
        self.failed_fixture();original_head=self.head
        # Real shared limiter allows one proof call/day; no mocked BudgetWait.
        self.runner.worker.rpc.settings.daily_getlogs=51
        self.logs=[event(self.t,at=1010,index=1)]
        heads=[];base_send=Rpc._send
        async def send(rpc,payload,method):
            if method=='eth_blockNumber':heads.append(method)
            return await base_send(rpc,payload,method)
        with patch.object(Rpc,'_send',send):
            for day in range(6):
                self.db.set_state('heartbeat',time.time());switch.connection_seen(self.db,self.connection)
                before=len(self.calls)
                result=await recover(self.runner,self.sid,'a'*40,True)
                manifest=switch.value(switch.latest(self.db))['conservative_recovery']
                self.assertEqual(manifest['reconciliation_head'],original_head)
                self.assertLessEqual(self.db.used('flow_eth_getLogs',int(time.time())//86400*86400),1)
                self.assertEqual(len(self.calls)-before,1)
                if day<5:
                    self.assertEqual(result['gate'],'RECOVERY_BUDGET_PAUSED')
                    self.assertEqual(switch.latest(self.db)['state'],'FAILED')
                    rows=[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_shadow_ranges')]
                    zero=await recover(self.runner,self.sid,'a'*40,True)
                    self.assertEqual(zero['gate'],'RECOVERY_BUDGET_PAUSED');self.assertEqual(len(self.calls),before+1)
                    self.assertEqual(rows,[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_shadow_ranges')])
                    # Reconstruct reconciler as after an interrupted operator process.
                    resumed=object.__new__(ShadowReconciler)
                    resumed.worker=self.runner.worker;resumed.db=self.db;resumed.session_id=None
                    self.runner=resumed
                    with self.db.conn:self.db.conn.execute('UPDATE flow_usage SET minute=minute-86400')
                    self.head+=9999  # moving live chain must not move the fixed obligation
                else:self.assertTrue(result['complete'])
        self.assertEqual(len(heads),1)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events WHERE launch_id=1').fetchone()[0],1)
        self.assertEqual(self.calls,[(self.base,self.base+1999),(self.base+2000,self.base+3999),(self.base+4000,original_head)]*2)
        self.assertEqual(switch.latest(self.db)['state'],'HEALTHY')
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_feature_versions').fetchone()[0],0)
        self.assertIsNone(switch.value(switch.latest(self.db))['frozen_head'])

    async def test_header_interruption_keeps_first_captured_head(self):
        self.failed_fixture();original=self.runner.worker.rpc.call;head=self.head;methods=[]
        async def call(method,params):
            methods.append(method)
            if method=='eth_getBlockByNumber':raise BudgetWait('minute_rpc',12,12,time.time()+60)
            return await original(method,params)
        with patch.object(self.runner.worker.rpc,'call',call):
            self.assertEqual((await recover(self.runner,self.sid,'a'*40))['gate'],'RECOVERY_BUDGET_PAUSED')
        self.head+=100
        async def resumed(method,params):
            methods.append(method)
            if method=='eth_getBlockByNumber':
                return {'number':hex(head),'timestamp':hex(int(time.time())+10)}
            return await original(method,params)
        with patch.object(self.runner.worker.rpc,'call',resumed):
            self.assertTrue((await recover(self.runner,self.sid,'a'*40,True))['complete'])
        self.assertEqual(methods.count('eth_blockNumber'),1)
        self.assertEqual(switch.value(switch.latest(self.db))['recovery']['reconciliation_head'],head)

    async def test_adaptive_reduction_preserved_without_relaxing_normal_guard(self):
        self.failed_fixture();base_send=Rpc._send;reject=[True]
        async def send(rpc,payload,method):
            if method=='eth_getLogs' and reject[0]:
                reject[0]=False;raise LogRangeError('fixture provider range limit')
            return await base_send(rpc,payload,method)
        with patch.object(Rpc,'_send',send):
            self.assertTrue((await recover(self.runner,self.sid,'a'*40,True))['complete'])
        job=self.db.conn.execute("SELECT * FROM flow_shadow_jobs WHERE stage=? AND launch_id=1",
                                 (f'provider_switch:{self.sid}:conservative',)).fetchone()
        self.assertEqual(job['span'],1000);self.assertEqual(job['range_reductions'],1)
        with self.assertRaises(Exception):self.runner.worker.recovery_plan(self.t,self.head)

    async def test_insufficient_budget_before_manifest_sends_zero_rpc(self):
        self.failed_fixture();self.db.count('flow_eth_getLogs',350)
        with patch.object(self.runner.worker.rpc,'call',side_effect=AssertionError('RPC forbidden')):
            result=await recover(self.runner,self.sid,'a'*40)
        self.assertEqual(result['gate'],'RECOVERY_BUDGET_PAUSED')
        self.assertEqual(switch.latest(self.db)['payload'],self.raw)

    async def test_handoff_requires_actual_ranges_and_identity_not_complete_flags(self):
        self.failed_fixture()
        self.assertTrue((await recover(self.runner,self.sid,'a'*40,False))['complete'])
        _,_,filters=plan(self.db,self.runner.worker.main,self.sid)
        stage=f'provider_switch:{self.sid}:conservative'
        with self.db.conn:self.db.conn.execute('DELETE FROM flow_shadow_ranges WHERE stage=? AND first_block=?',
            (stage,self.base))
        with self.assertRaisesRegex(ValueError,'range proof'):finish(self.runner,self.sid,filters)
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')

    async def test_final_handoff_refuses_query_change(self):
        self.failed_fixture()
        await recover(self.runner,self.sid,'a'*40,False)
        _,_,filters=plan(self.db,self.runner.worker.main,self.sid)
        with self.db.conn:self.db.conn.execute('UPDATE flow_bootstrap_identity SET query_json=? WHERE stage=?',
            (json.dumps(dict(filters[0]['query'],topics=[['wrong']])),f'provider_switch:{self.sid}:conservative'))
        with self.assertRaisesRegex(ValueError,'Final proof identity'):finish(self.runner,self.sid,filters)
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')

    async def test_live_worker_consumption_and_identity_change_guard(self):
        self.failed_fixture();base_send=Rpc._send
        async def send(rpc,payload,method):
            result=await base_send(rpc,payload,method)
            if method=='eth_getLogs':self.db.count('flow_eth_getLogs',350)
            return result
        with patch.object(Rpc,'_send',send):result=await recover(self.runner,self.sid,'a'*40)
        self.assertEqual(result['gate'],'RECOVERY_BUDGET_PAUSED');self.assertEqual(len(self.calls),1)
        with self.db.conn:self.db.conn.execute('UPDATE flow_usage SET minute=minute-86400')
        with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET curve_address=? WHERE launch_id=1",('0x'+'fe'*20,))
        with self.assertRaisesRegex(ValueError,'FILTER_IDENTITY'):await recover(self.runner,self.sid,'a'*40)
        self.assertEqual(len(self.calls),1)

    def test_retained_switch17_offline_workload(self):
        fixture=json.loads((Path(__file__).parent/'fixtures/phase2b/switch17_recovery_metadata.json').read_text(encoding='utf-8-sig'))
        with self.db.conn:
            self.db.conn.execute('DELETE FROM flow_tracking_targets')
            self.db.conn.execute('DELETE FROM flow_bootstrap_identity')
        self.main.execute('DELETE FROM launches')
        for f in fixture['filters']:
            saved=f['target'];t=target(launch=saved['launch_id'],start=saved['tracking_start_at'])
            for key in ('token_address','quote_asset_address','curve_address','launch_block','launch_log_index','tracking_end_at','status'):
                t[key]=saved[key]
            insert_target(self.db,t)
            l=f['launch']
            self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',
                (l['id'],l['token_address'],l['quote_asset_address'],l['curve_address'],t['creator_address'],
                 l['block_timestamp'],l['block_number'],l['log_index'],1))
            r=f['ranges'][0]
            self.add_proof(t['launch_id'],r['first_block'],r['last_block'],stage=r['stage'],legacy=True)
            with self.db.conn:
                self.db.conn.execute('UPDATE flow_bootstrap_identity SET query_json=?,upper_at=? WHERE stage=?',
                    (f['query_identity'][0]['query_json'],f['query_identity'][0]['upper_at'],r['stage']))
        self.main.commit()
        s=fixture['switch17']
        with self.db.conn:self.db.conn.execute('INSERT INTO flow_provider_switches VALUES(?,?,?,?)',
            (s['id'],s['session_id'],'FAILED',json.dumps(s['payload'])))
        raw=self.db.conn.execute('SELECT payload FROM flow_provider_switches WHERE id=17').fetchone()[0]
        _,item,filters=plan(self.db,self.runner.worker.main,17)
        work=workload(self.db,item,filters,int(fixture['comparison_endpoint']['value']))
        self.assertEqual(work,workload(self.db,item,filters,int(fixture['comparison_endpoint']['value'])))
        self.assertEqual([x['filter_block_positions'] for x in work],[2521714,2504315])
        self.assertEqual([x['estimated_getlogs'] for x in work],[1261,1253])
        self.assertEqual([x['completed_ranges'] for x in work],[[[78323288,78323321]],[[78340692,78340720]]])
        self.assertEqual(self.db.conn.execute('SELECT payload FROM flow_provider_switches WHERE id=17').fetchone()[0],raw)
