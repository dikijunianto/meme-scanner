import copy
from dataclasses import replace
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from app.config import Config
from app.flow_bootstrap import CursorBootstrap
from app.flow_data import FlowDB
from app.flow_providers import FlowProviders, provider
from app.flow_shadow import make_reconciler
from app.flow_worker import FlowSettings
from app.rpc import Rpc, RpcError
from tests.test_flow import event, fixture, insert_target, main_schema, target


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)
        self.main=main_schema(self.path/'main.db')
        self.db=FlowDB(self.path/'flow.db');self.db.migrate()
        self.t=insert_target(self.db,target())
        self.config=Config('https://alchemy.invalid',4663,(),'',self.path/'main.db',self.path/'log')
        self.settings=FlowSettings(database=self.path/'flow.db',minute_calls=100)
        self.providers=FlowProviders('https://mainnet.robinhood.validationcloud.io/v1/test',
                                     'wss://mainnet.robinhood.validationcloud.io/v1/test')
        self.runner,self.old_rpc=make_reconciler(self.config,self.settings,self.db,self.providers)
        self.runner.worker.rpc.config=replace(self.runner.worker.rpc.config,rpc_rps=1000)
        self.bootstrap=CursorBootstrap(self.runner)
        self.base=self.t['launch_block']

    async def asyncTearDown(self):
        await self.old_rpc.close();await self.runner.worker.rpc.close()
        self.runner.worker.main.close();self.main.close();self.db.conn.close();self.tmp.cleanup()

    def rpc(self,head,logs=(),fail_from=None,head_at=1100):
        calls=[]
        async def answer(rpc,payload,method):
            self.assertEqual(provider(rpc.config.rpc_http),'validation')
            current=head[0] if isinstance(head,list) else head
            if method=='eth_blockNumber':value=hex(current)
            elif method=='eth_getBlockByNumber':value={'number':hex(current),'timestamp':hex(head_at),'hash':'0x'+'11'*32}
            else:
                self.assertEqual(method,'eth_getLogs')
                q=payload['params'][0];first,last=int(q['fromBlock'],16),int(q['toBlock'],16)
                calls.append((first,last))
                if fail_from is not None and first>=fail_from:raise RpcError('provider failure')
                value=[copy.deepcopy(x) for x in logs if first<=int(x['blockNumber'],16)<=last]
            return {'jsonrpc':'2.0','id':payload['id'],'result':value}
        p=patch.object(Rpc,'_send',answer);p.start();self.addCleanup(p.stop)
        return calls

    async def test_missing_cursor_proof_is_contiguous_deduped_and_idempotent(self):
        row=event(self.t);self.rpc(self.base+354,[row,row])
        result=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(result['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        self.assertEqual(result['blocks_verified'],355)
        self.assertEqual(result['recovered_events'],1)
        self.assertEqual(result['duplicates'],1)
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+354))
        self.assertEqual(self.db.conn.execute("SELECT status FROM flow_bootstrap").fetchone()[0],'complete')
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_gaps WHERE resolved=0").fetchone()[0],0)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_events").fetchone()[0],1)
        again=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(again['actual_getlogs_calls'],1)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_events").fetchone()[0],1)

    async def test_failed_chunk_keeps_cursor_null_and_resume_uses_first_unverified(self):
        calls=self.rpc(self.base+3000,fail_from=self.base+2000)
        result=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(result['gate'],'MISSING_CURSOR_BOOTSTRAP_PENDING')
        self.assertIsNone(self.db.state('recovery:1:curve'))
        self.assertEqual(result['jobs'][0]['highest_contiguous_verified_block'],self.base+1999)
        self.assertEqual(result['jobs'][0]['next_unverified_block'],self.base+2000)
        self.assertEqual(calls[:2],[(self.base,self.base+1999),(self.base+2000,self.base+3000)])
        self.assertEqual(self.db.conn.execute("SELECT status FROM flow_bootstrap").fetchone()[0],'in_progress')

    async def test_reserve_stops_and_next_day_resumes(self):
        self.settings.daily_getlogs=51
        self.runner.worker.rpc.settings.daily_getlogs=51
        calls=self.rpc(self.base+3000)
        first=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(first['gate'],'MISSING_CURSOR_BOOTSTRAP_PENDING')
        self.assertEqual(len(calls),1)
        self.assertIsNone(self.db.state('recovery:1:curve'))
        with self.db.conn:self.db.conn.execute('UPDATE flow_usage SET minute=minute-86400')
        second=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(second['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        self.assertEqual(calls[-1],(self.base+2000,self.base+3000))
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+3000))

    async def test_existing_stale_cursor_gets_proven_tail_without_weakening_runtime(self):
        self.db.set_state('recovery:1:curve',self.base+100)
        self.rpc(self.base+354)
        with self.assertRaises(Exception):self.runner.worker.recovery_plan(self.t,self.base+354)
        result=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(result['jobs'][0]['original_safe_start'],self.base+98)
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+354))
        self.assertEqual(self.runner.worker.recovery_plan(self.t,self.base+354)[0][4:],(self.base+352,self.base+354))

    async def test_expired_target_stops_at_first_proven_post_expiry_block(self):
        with self.main:self.main.execute('''INSERT INTO launches
          (id,token_address,quote_asset_address,curve_address,creator_address,block_timestamp,block_number,log_index,is_stock_quote)
          VALUES(?,?,?,?,?,?,?,?,?)''',(2,'token','quote','curve','creator','1970-01-01T01:16:41+00:00',self.base+5000,0,1))
        self.rpc(self.base+10000,head_at=10000)
        result=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual(result['jobs'][0]['reconciliation_upper_bound'],self.base+5000)

    async def test_filter_change_prevents_promotion(self):
        self.rpc(self.base+20)
        await self.bootstrap.freeze('cursor_bootstrap',[1])
        self.assertTrue(await self.runner.run_stage('cursor_bootstrap'))
        with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET curve_address=? WHERE launch_id=1",('0x'+'11'*20,))
        with self.assertRaises(RpcError):self.bootstrap.promote('cursor_bootstrap')
        self.assertIsNone(self.db.state('recovery:1:curve'))

    async def test_five_targets_have_distinct_activation_starts(self):
        offsets=(0,5,30,100,200)
        for launch,offset in enumerate(offsets[1:],2):
            t=target(launch=launch);t['launch_block']=self.base+offset
            t['token_address']='0x'+format(launch,'040x')
            t['curve_address']='0x'+format(launch+100,'040x')
            insert_target(self.db,t)
        self.rpc(self.base+250)
        result=await self.bootstrap.run('cursor_bootstrap',[1,2,3,4,5])
        self.assertEqual([j['original_safe_start'] for j in result['jobs']],
                         [self.base+x for x in offsets])
        self.assertEqual(result['actual_getlogs_calls'],5)
        self.assertEqual(self.db.used('flow_eth_getLogs_validation',0),5)
        self.assertEqual(self.db.used('flow_eth_getTransactionReceipt',0),0)
        self.assertEqual(self.runner.worker.main.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    async def test_moving_head_tail_makes_restart_gate_pass(self):
        head=[self.base+50];self.rpc(head)
        await self.bootstrap.run('cursor_bootstrap',[1])
        head[0]=self.base+200
        blocked=await self.bootstrap.restart_plan()
        self.assertFalse(blocked['safe_now'])
        self.assertEqual(blocked['filters'][0]['delta'],150)
        tail=await self.bootstrap.run('cursor_tail_1',[1])
        self.assertEqual(tail['gate'],'BOOTSTRAP_PROOF_COMPLETE')
        ready=await self.bootstrap.restart_plan()
        self.assertTrue(ready['safe_now'])
        self.assertEqual(ready['filters'][0]['delta'],0)

    async def test_graduated_filters_start_at_proven_activation(self):
        import json
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+20
        with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=? WHERE launch_id=1',(json.dumps(g),))
        self.rpc(self.base+30)
        result=await self.bootstrap.run('cursor_bootstrap',[1])
        self.assertEqual({j['kind']:(j['original_safe_start'],j['reconciliation_upper_bound']) for j in result['jobs']},
                         {'curve':(self.base,self.base+20),'v4':(self.base+20,self.base+30),
                          'hook':(self.base+20,self.base+30)})
        self.assertEqual({r[0] for r in self.db.conn.execute('SELECT kind FROM flow_bootstrap WHERE status="complete"')},
                         {'curve','v4','hook'})

    async def test_live_graduation_over_100_blocks_proves_only_new_filters(self):
        import json
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+20
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=?,tracking_end_at=? WHERE launch_id=1',
                                 (json.dumps(g),time.time()+3600))
        self.db.require_bootstrap(self.db.target(1),'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.base+20)
        for kind in ('v4','hook'):
            self.db.require_bootstrap(self.db.target(1),kind,self.base+20)
        with self.assertRaisesRegex(RpcError,'explicit bootstrap'):
            self.runner.worker.recovery_plan(self.db.target(1),self.base+423)
        calls=self.rpc(self.base+423)
        complete=await self.runner.worker.bootstrap_graduated(self.db.target(1))
        self.assertTrue(complete)
        self.assertEqual(calls,[(self.base+20,self.base+423)]*2)
        self.assertEqual(self.db.state('recovery:1:v4'),str(self.base+423))
        self.assertEqual(self.db.state('recovery:1:hook'),str(self.base+423))
        self.assertEqual(self.db.state('recovery:1:curve'),str(self.base+20))
        self.assertEqual({r[0] for r in self.db.conn.execute("SELECT kind FROM flow_bootstrap WHERE status='complete'")},
                         {'curve','v4','hook'})
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_gaps WHERE resolved=0").fetchone()[0],0)
        self.assertEqual(self.db.used('flow_eth_getTransactionReceipt',0),0)
        self.assertEqual(self.db.used('flow_eth_getTransactionByHash',0),0)
        self.assertEqual(self.db.used('flow_http_calls_alchemy',0),0)

    async def test_live_graduation_budget_wait_holds_null_cursors_and_resumes(self):
        import json
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+20
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=?,tracking_end_at=? WHERE launch_id=1',
                                 (json.dumps(g),time.time()+3600))
        for kind in ('v4','hook'):
            self.db.require_bootstrap(self.db.target(1),kind,self.base+20)
        self.settings.daily_getlogs=0
        calls=self.rpc(self.base+423)
        self.assertFalse(await self.runner.worker.bootstrap_graduated(self.db.target(1)))
        self.assertEqual(calls,[])
        self.assertIsNone(self.db.state('recovery:1:v4'))
        self.assertIsNone(self.db.state('recovery:1:hook'))
        self.assertEqual(self.db.used('flow_graduated_bootstrap_budget_waits',0),1)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_gaps WHERE reason='reconnect_recovery_incomplete'").fetchone()[0],0)
        self.settings.daily_getlogs=400
        self.assertTrue(await self.runner.worker.bootstrap_graduated(self.db.target(1)))
        self.assertEqual(calls,[(self.base+20,self.base+423)]*2)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_shadow_ranges WHERE stage='live_graduation:1'").fetchone()[0],2)

    async def test_live_graduation_failed_second_chunk_resumes_from_proved_range(self):
        import json
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+20
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=?,tracking_end_at=? WHERE launch_id=1',
                                 (json.dumps(g),time.time()+3600))
        for kind in ('v4','hook'):
            self.db.require_bootstrap(self.db.target(1),kind,self.base+20)
        first_calls=self.rpc(self.base+3020,fail_from=self.base+2020)
        self.assertFalse(await self.runner.worker.bootstrap_graduated(self.db.target(1)))
        self.assertEqual(first_calls,[(self.base+20,self.base+2019),(self.base+2020,self.base+3020)])
        self.assertIsNone(self.db.state('recovery:1:hook'))
        self.assertEqual(self.db.conn.execute("SELECT next_unverified_block FROM flow_shadow_jobs WHERE stage='live_graduation:1' AND kind='hook'").fetchone()[0],self.base+2020)
        resumed_calls=self.rpc(self.base+3020)
        self.assertTrue(await self.runner.worker.bootstrap_graduated(self.db.target(1)))
        self.assertEqual(resumed_calls,[(self.base+2020,self.base+3020),
                                        (self.base+20,self.base+2019),(self.base+2020,self.base+3020)])
        self.assertEqual(self.db.state('recovery:1:v4'),str(self.base+3020))
        self.assertEqual(self.db.state('recovery:1:hook'),str(self.base+3020))

    async def test_expired_live_graduation_keeps_proof_pending_without_cursor(self):
        import json
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+20
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET graduation_json=? WHERE launch_id=1',(json.dumps(g),))
        for kind in ('v4','hook'):
            self.db.require_bootstrap(self.db.target(1),kind,self.base+20)
        self.rpc(self.base+30)
        result=await self.bootstrap.run('live_graduation:1',[1],{'v4','hook'},active_only=True)
        self.assertEqual(result['gate'],'MISSING_CURSOR_BOOTSTRAP_PENDING')
        self.assertTrue(result['expired_incomplete'])
        self.assertIsNone(self.db.state('recovery:1:v4'))
        self.assertIsNone(self.db.state('recovery:1:hook'))
        self.runner.worker.discover=AsyncMock()
        await self.runner.worker.reconcile()
        self.assertEqual(self.db.target(1)['status'],'partial')
        self.assertEqual(self.db.used('flow_graduated_bootstrap_expired_incomplete',0),1)

    async def test_reconcile_routes_new_graduated_filters_around_normal_guard(self):
        g=fixture('v4_buy')['launch'];g['block_number']=self.base+20
        g['token_address']=self.t['token_address']
        existing={'token_address','block_number','log_index'}
        for key in g.keys()-existing:
            self.main.execute(f'ALTER TABLE graduations ADD COLUMN {key} TEXT')
        self.main.execute('INSERT INTO graduations('+','.join(g)+') VALUES('+','.join('?' for _ in g)+')',tuple(g.values()))
        self.main.commit()
        with self.db.conn:
            self.db.conn.execute('UPDATE flow_tracking_targets SET tracking_end_at=? WHERE launch_id=1',
                                 (time.time()+3600,))
        self.db.require_bootstrap(self.db.target(1),'curve',self.base)
        self.db.complete_bootstrap(1,'curve',self.base+20)
        worker=self.runner.worker
        worker.subscriptions[(1,'curve')]='old'
        worker.command=AsyncMock(side_effect=['swap','hook',True])
        worker.discover=AsyncMock()
        calls=self.rpc(self.base+423)
        await worker.reconcile()
        self.assertEqual(calls,[(self.base+20,self.base+423)]*2)
        self.assertEqual(self.db.state('recovery:1:v4'),str(self.base+423))
        self.assertEqual(self.db.state('recovery:1:hook'),str(self.base+423))
        self.assertNotEqual(self.db.state('service_status'),'unrecoverable_gap')
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_gaps WHERE resolved=0").fetchone()[0],0)
