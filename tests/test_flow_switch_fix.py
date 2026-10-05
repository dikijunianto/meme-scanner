import copy
from dataclasses import replace
import hashlib
import importlib
import json
import runpy
import time
import socket
import unittest
from unittest.mock import AsyncMock, patch

from app import flow_provider_switch as switch
from app.flow_budget import BudgetWait
from app.flow_data import iso, BUY, SELL
from app.flow_switch_recovery import plan, recover
from app.rpc import Rpc, RpcError
from tests.test_flow import target, insert_target, event
from tests import test_flow_bootstrap
from tests import network_guard


class SwitchFixTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await test_flow_bootstrap.BootstrapTests.asyncSetUp(self)
        self.runner.worker.settings=replace(self.settings,split_enabled=True,minute_calls=12)
        self.runner.worker.rpc.settings=self.runner.worker.settings
        self.runner.worker.ws_provider='validation'
        self.db.set_state('current_wss_provider','validation')
        self.db.set_state('connection_state','connected')
        self.db.set_state('heartbeat',time.time())
        self.connection=switch.connection_open(self.db,'validation')
        self.runner.worker.connection_id=self.connection
        second=target(launch=2);second['token_address']='0x'+'ab'*20;second['curve_address']='0x'+'cd'*20
        for t in (self.t,insert_target(self.db,second)):
            self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',
                (t['launch_id'],t['token_address'],t['quote_asset_address'],t['curve_address'],
                 t['creator_address'],iso(t['tracking_start_at']),t['launch_block'],t['launch_log_index'],1))
        self.main.commit()
        with self.db.conn:
            for launch in (1,2):
                t=self.db.target(launch)
                self.db.conn.execute('INSERT INTO flow_bootstrap_identity VALUES(?,?,?,?,?)',
                    (f'live_bootstrap:{launch}',launch,'curve',
                     json.dumps({'address':t['curve_address'].lower(),'topics':[[BUY,SELL]]}),time.time()-100))
        self.head=self.base+4001
        self.calls=[]
        async def answer(rpc,payload,method):
            if method=='eth_chainId':value=hex(4663)
            elif method=='eth_blockNumber':value=hex(self.head)
            elif method=='eth_getBlockByNumber':value={'number':hex(self.head),'timestamp':hex(int(time.time())+10)}
            elif method=='eth_getLogs':
                q=payload['params'][0];self.calls.append((int(q['fromBlock'],16),int(q['toBlock'],16)))
                value=[copy.deepcopy(log) for log in getattr(self,'logs',[])
                       if int(q['fromBlock'],16)<=int(log['blockNumber'],16)<=int(q['toBlock'],16)
                       and log['address'].lower()==q['address'].lower()]
            else:raise AssertionError(method)
            return {'jsonrpc':'2.0','id':payload['id'],'result':value}
        p=patch.object(Rpc,'_send',answer);p.start();self.addCleanup(p.stop)
        self.runner.worker.rpc.config=replace(self.runner.worker.rpc.config,rpc_rps=100000)

    async def asyncTearDown(self):await test_flow_bootstrap.BootstrapTests.asyncTearDown(self)

    def failed_fixture(self):
        gaps=[]
        with self.db.conn:
            for launch in (1,2):
                result=self.db.conn.execute('INSERT INTO flow_gaps(launch_id,start_at,end_at,reason,first_block,resolved) VALUES(?,?,?,?,?,0)',
                    (launch,time.time()-50,time.time()-40,'ws_gap',self.base+2))
                gaps.append(result.lastrowid)
                self.db.conn.execute("UPDATE flow_tracking_targets SET status='partial' WHERE launch_id=?",(launch,))
        self.sid,item=switch.start(self.db,'validation',
            [{'launch_id':i,'kind':'curve','base':self.base,'cursor':str(self.base+3)} for i in (1,2)],
            gaps,last_block=str(self.base+3),now=time.time()-60)
        switch.connected(self.db,self.sid,'validation',self.connection,time.time()-55)
        switch.acknowledged(self.db,self.sid,time.time()-54)
        switch.failed(self.db,self.sid,'FlowBudget')
        self.raw=switch.latest(self.db)['payload']
        return self.sid

    async def test_shared_exception_and_prehead_repeated_waits(self):
        # Loading a second module identity cannot produce a second budget class.
        alternate=runpy.run_module('app.flow_worker',run_name='offline_alternate_worker')
        self.assertIs(alternate['BudgetWait'],BudgetWait)
        self.assertFalse(issubclass(BudgetWait,RpcError))
        sid,_=switch.start(self.db,'validation',[{'launch_id':1,'kind':'curve','base':self.base,'cursor':None}],[7])
        switch.connected(self.db,sid,'validation',self.connection)
        worker=self.runner.worker
        with patch.object(worker,'prove_provider_switch',side_effect=BudgetWait('minute_rpc',12,12,time.time()+60)), \
             patch('app.flow_worker.connect',side_effect=AssertionError('Accidental enclosing runtime/WSS fixture')) as wss:
            await worker.advance_provider_switch();await worker.advance_provider_switch()
        row=switch.latest(self.db)
        self.assertEqual(row['id'],sid);self.assertEqual(row['state'],'WAITING_FOR_BUDGET')
        item=switch.value(row)
        self.assertIsNone(item['frozen_head']);self.assertEqual(item['gap_ids'],[7])
        self.assertIsNone(switch.resume_budget_wait(self.db,row))
        self.assertEqual(switch.resume_budget_wait(self.db,row,time.time()+61)['last_budget_wait']['pending_state'],'VALIDATION_CONNECTED')
        wss.assert_not_called()

    async def test_real_connection_failure_stays_terminal(self):
        sid,_=switch.start(self.db,'validation',[{'launch_id':1,'kind':'curve','base':self.base,'cursor':None}],[])
        switch.connected(self.db,sid,'validation',self.connection)
        with patch.object(self.runner.worker,'prove_provider_switch',side_effect=RpcError('WS closed')):
            await self.runner.worker.advance_provider_switch()
        self.assertEqual(switch.latest(self.db)['state'],'FAILED')

    async def test_actual_shadow_limiter_before_frozen_head(self):
        sid,_=switch.start(self.db,'validation',[{'launch_id':1,'kind':'curve','base':self.base,'cursor':None}],[7])
        switch.connected(self.db,sid,'validation',self.connection)
        worker=self.runner.worker
        self.db.count('flow_rpc_members',12)
        with patch.object(worker,'subscriptions_acknowledged',return_value=True):
            await worker.advance_provider_switch()
        row=switch.latest(self.db);item=switch.value(row)
        self.assertEqual(row['state'],'WAITING_FOR_BUDGET')
        self.assertIsNone(item['frozen_head']);self.assertEqual(item['budget_wait']['scope'],'minute_rpc')
        self.assertEqual(self.calls,[])

    async def test_switch17_later_head_zero_logs_preserves_failure(self):
        self.failed_fixture()
        expiry=[tuple(r) for r in self.db.conn.execute('SELECT launch_id,status,tracking_end_at FROM flow_tracking_targets')]
        result=await recover(self.runner,self.sid,'a'*40,True)
        self.assertTrue(result['complete']);self.assertEqual(switch.latest(self.db)['state'],'HEALTHY')
        item=switch.value(switch.latest(self.db))
        self.assertIsNone(item['frozen_head'])
        self.assertEqual(item['failure_history']['payload_raw'],self.raw)
        self.assertEqual(item['failure_history']['payload_sha256'],hashlib.sha256(self.raw.encode()).hexdigest())
        self.assertEqual(item['conservative_recovery']['reconciliation_head'],self.head)
        self.assertEqual(item['failure'],'FlowBudget')
        self.assertEqual(switch.report(self.db)['provider_switch_unresolved_ranges'],0)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_gaps WHERE resolved=0').fetchone()[0],0)
        self.assertEqual(expiry,[tuple(r) for r in self.db.conn.execute('SELECT launch_id,status,tracking_end_at FROM flow_tracking_targets')])
        self.assertEqual(self.db.state('recovery_state'),None)  # no runtime status override

    async def test_completed_chunks_survive_multiple_budget_waits(self):
        self.failed_fixture()
        original=self.runner.worker.rpc.call
        count=0
        async def call(method,params):
            nonlocal count
            if method=='eth_getLogs':
                count+=1
                if count in (2,4):raise BudgetWait('minute_rpc',12,12,time.time()-1)
            return await original(method,params)
        self.runner.worker.rpc.call=call
        self.assertFalse((await recover(self.runner,self.sid,'a'*40,True))['complete'])
        first=tuple(self.db.conn.execute('SELECT first_block,last_block FROM flow_shadow_ranges').fetchone())
        self.assertFalse((await recover(self.runner,self.sid,'a'*40,True))['complete'])
        self.assertTrue((await recover(self.runner,self.sid,'a'*40,True))['complete'])
        self.assertEqual(self.calls.count(first),2) # exact same bounds, two different filter addresses
        self.assertEqual(sum(a==self.base for a,b in self.calls),2) # one per exact filter, not retries
        self.assertIsNone(switch.value(switch.latest(self.db))['frozen_head'])

    async def test_head_budget_wait_is_durable(self):
        self.failed_fixture()
        with patch.object(self.runner.worker.rpc,'call',side_effect=BudgetWait('daily_rpc',1000,1000,time.time()+10)):
            self.assertTrue((await recover(self.runner,self.sid,'a'*40))['waiting_for_budget'])
        item=switch.value(switch.latest(self.db));self.assertEqual(item['conservative_budget_wait']['scope'],'daily_rpc')
        self.assertEqual(item['failure_history']['payload_raw'],self.raw)
        self.assertEqual(switch.latest(self.db)['state'],'FAILED') # original historical failure retained

    async def test_missing_lower_bound_and_snapshot_refuse_without_rpc(self):
        self.failed_fixture()
        row=switch.latest(self.db);item=switch.value(row);item['filters'][0].pop('base')
        with self.db.conn:self.db.conn.execute('UPDATE flow_provider_switches SET payload=? WHERE id=?',(json.dumps(item),self.sid))
        with patch.object(self.runner.worker.rpc,'call',side_effect=AssertionError('RPC before evidence guard')):
            with self.assertRaisesRegex(ValueError,'MISSING_LOWER_BOUND'):await recover(self.runner,self.sid,'a'*40)
            item['filters']=[]
            with self.db.conn:self.db.conn.execute('UPDATE flow_provider_switches SET payload=? WHERE id=?',(json.dumps(item),self.sid))
            with self.assertRaisesRegex(ValueError,'MISSING_FILTER'):plan(self.db,self.runner.worker.main,self.sid)

    async def test_smaller_head_and_changed_identity_refuse(self):
        self.failed_fixture();self.head=self.base-1
        with self.assertRaisesRegex(ValueError,'INVALID_RECONCILIATION_HEAD'):await recover(self.runner,self.sid,'a'*40)
        with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET curve_address=? WHERE launch_id=1",('0x'+'ff'*20,))
        with self.assertRaisesRegex(ValueError,'FILTER_IDENTITY'):plan(self.db,self.runner.worker.main,self.sid)

    async def test_overlap_dedupes_before_closure(self):
        self.failed_fixture()
        self.logs=[event(self.t,at=1010,index=1)]
        # Same raw event is already canonical; both filter queries see overlap.
        self.runner.worker.ingest(self.t,copy.deepcopy(self.logs[0]))
        result=await recover(self.runner,self.sid,'a'*40,True)
        self.assertGreaterEqual(result['recovery']['duplicates'],1)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events WHERE launch_id=1').fetchone()[0],1)

    async def test_scheduler_resumes_current_versions_without_incident_pit(self):
        self.failed_fixture()
        self.db.activate_pit_ledger('a'*40,self.base,now=900)
        self.db.set_state('service_status','provider_switch_failed')
        self.db.set_state('recovery_state','provider_switch_failed')
        self.assertTrue(switch.blocked(self.db))
        await recover(self.runner,self.sid,'a'*40,True)
        with patch.object(self.runner.worker,'discover',new=AsyncMock()):
            await self.runner.worker.reconcile()
        self.assertEqual(self.db.state('recovery_state'),'healthy')
        incident=json.loads(self.db.state(f'pit_collection_incident:{self.sid}'))
        self.assertIsNone(incident['PIT_COLLECTION_RECOVERY_END'])
        self.assertIsNotNone(incident['runtime_healthy_at'])
        for i,start in ((3,incident['PIT_COLLECTION_REGRESSION_START']+1),(4,time.time()+1)):
            data=target(launch=i,start=start);data['token_address']='0x'+f'{i:040x}'
            t=insert_target(self.db,data)
            self.db.require_bootstrap(t,'curve',self.base);self.db.complete_bootstrap(i,'curve',self.head)
            with patch('app.flow_data.time.time',return_value=start+65):self.db.rebuild(self.db.target(i),start+60)
        old=dict(self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=3 AND window_seconds=60').fetchone())
        new=dict(self.db.conn.execute('SELECT * FROM flow_feature_versions WHERE launch_id=4 AND window_seconds=60').fetchone())
        self.assertIsNone(old['model_eligible_at']);self.assertIsNotNone(new['model_eligible_at'])
        self.assertEqual(new['model_eligible_at'],new['materialized_at'])
        incident=json.loads(self.db.state(f'pit_collection_incident:{self.sid}'))
        self.assertEqual(incident['PIT_COLLECTION_RECOVERY_END'],new['materialized_at'])
        self.assertEqual(incident['first_fresh_pit']['launch_id'],4)
        before=[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')]
        self.db.rebuild(self.db.target(3),time.time()+100)
        self.assertEqual(before[:len(before)],[tuple(r) for r in self.db.conn.execute('SELECT * FROM flow_feature_versions')][:len(before)])


class ImportSafetyTests(unittest.TestCase):
    def test_guard_blocks_fixture_wss_transport_before_dns_or_connect(self):
        # Deliberate denial test has its own counter; no network function is reached.
        with patch.object(network_guard,'attempts',[]) as rejected:
            with self.assertRaisesRegex(AssertionError,'External network forbidden'):
                socket.create_connection(('synthetic-wss.invalid',443))
            self.assertEqual(len(rejected),1)

    def test_imports_open_no_network_database_or_service(self):
        before=len(network_guard.attempts)
        with patch('socket.socket.connect',side_effect=AssertionError('socket')) as sock, \
             patch('httpx.Client.send',side_effect=AssertionError('HTTP')) as http, \
             patch('httpx.AsyncClient.send',side_effect=AssertionError('HTTP')) as async_http, \
             patch('sqlite3.connect',side_effect=AssertionError('DB')) as db, \
             patch('subprocess.run',side_effect=AssertionError('service')) as service:
            for module in ('app.flow_worker','app.flow_provider_switch','app.flow_bootstrap',
                           'app.flow_switch_recovery','scripts.provider_switch_recover'):
                runpy.run_module(module,run_name='offline_import_safety')
        for mock in (sock,http,async_http,db,service):mock.assert_not_called()
        self.assertEqual(len(network_guard.attempts),before)
