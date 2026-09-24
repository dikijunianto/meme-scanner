import asyncio
import copy
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
from eth_abi import encode, decode
from eth_utils import keccak

from app.config import Config
from app.flow_data import FlowDB, decode_event, BUY, SELL, SWAP, HOOK, iso
from app.flow_worker import FlowWorker, FlowSettings, FlowRpc, FlowBudget, HeaderCache, eligible_launches
from app.flow_providers import FlowProviders
from app.flow_reports import outcome, usage, inspect
from app.rpc import Rpc, RpcError, LogRangeError, RetryableRpcError, RetriesExhausted

FIXTURES=Path(__file__).parent/'fixtures/phase2b'


def fixture(name):return json.loads((FIXTURES/(name+'.json')).read_text())


def target(f=None,launch=1,start=1000,long=True):
    f=f or fixture('curve_buy_1');g=f['launch']
    return dict(launch_id=launch,token_address=g['token_address'],quote_asset_address=g['quote_asset_address'],
                curve_address=g['curve_address'],creator_address=g['creator_address'],quote_decimals=18,
                cohort_initial=1,cohort_long=int(long),tracking_start_at=start,tracking_end_at=start+(3600 if long else 900),
                launch_block=int(f['log']['blockNumber'],16),launch_log_index=0,coverage_start_at=start,
                coverage_end_at=start+(3600 if long else 900),created_at=start,updated_at=start)


def insert_target(db,t):
    with db.conn:db.conn.execute('INSERT INTO flow_tracking_targets('+','.join(t)+') VALUES('+','.join('?' for _ in t)+')',list(t.values()))
    return db.target(t['launch_id'])


def event(t,at=1010,index=1,buy=True,quote=100,tokens=200,caller='11',recipient='22'):
    f=fixture('curve_buy_1')['log'];f['address']=t['curve_address'];f['topics']=[BUY if buy else SELL,'0x'+'00'*12+caller*20,'0x'+'00'*12+recipient*20]
    f['data']='0x'+encode(['uint256']*4,[quote,tokens,2,3] if buy else [tokens,quote,2,3]).hex()
    f.update(blockTimestamp=hex(at),logIndex=hex(index),transactionHash='0x'+format(index,'064x'),blockNumber=hex(t['launch_block']))
    return f


def main_schema(path):
    c=sqlite3.connect(path);c.row_factory=sqlite3.Row
    c.executescript('''CREATE TABLE launches(id INTEGER PRIMARY KEY,token_address TEXT,quote_asset_address TEXT,curve_address TEXT,creator_address TEXT,
      block_timestamp TEXT,block_number INTEGER,log_index INTEGER,is_stock_quote INTEGER);
      CREATE TABLE outcome_targets(launch_id INTEGER,target_age_seconds INTEGER,sampling_group TEXT,due_at TEXT);
      CREATE TABLE stock_assets(address TEXT,stock_ticker TEXT,verified INTEGER);
      CREATE TABLE market_snapshots(launch_id INTEGER,target_age_seconds INTEGER,quote_asset_address TEXT,observed_at TEXT,data_quality TEXT,price_quote TEXT);
      CREATE TABLE market_static(key TEXT,value TEXT);
      CREATE TABLE graduations(token_address TEXT,block_number INTEGER,log_index INTEGER);''')
    return c


class FlowDataTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)
        self.db=FlowDB(self.path/'flow.db');self.db.migrate();self.t=insert_target(self.db,target())

    def tearDown(self):self.db.conn.close();self.tmp.cleanup()

    def add(self,**kwargs):
        e=event(self.t,**kwargs);self.db.store(self.t,e,decode_event(e,self.t));return e

    def metrics(self,window=300):
        self.db.rebuild(self.db.target(1),5000)
        r=self.db.conn.execute('SELECT * FROM flow_features WHERE launch_id=1 AND window_seconds=?',(window,)).fetchone()
        return dict(r),json.loads(r['metrics'])

    def test_six_real_fixtures_preserve_fee_legs_and_identity(self):
        for side in ('buy','sell'):
            for n in range(1,4):
                f=fixture(f'curve_{side}_{n}');t=target(f);e=decode_event(f['log'],t,f['launch'])
                a,b,fee,tax=decode(['uint256']*4,bytes.fromhex(f['log']['data'][2:]))
                quote=a if side=='buy' else b
                self.assertEqual(e['direction'],side);self.assertEqual(int(e['quote_event_amount_raw']),quote)
                self.assertEqual(int(e['pricing_quote_amount_raw']),quote-fee-tax if side=='buy' else quote+fee+tax)
                self.assertEqual(Decimal(e['base_fee_quote_normalized']),Decimal(fee)/10**18)
                self.assertIsNone(e['economic_actor']);self.assertIsNone(e['transaction_from']);self.assertIsNone(e['refund_quote_raw'])
        f=fixture('curve_buy_2');e=decode_event(f['log'],target(f));self.assertNotEqual(e['caller_address'],e['recipient_address'])

    def test_real_v4_fixtures_and_hook_fees_remain_separate(self):
        for side in ('buy','sell'):
            f=fixture('v4_'+side);t=target(f);g=f['launch'];e=decode_event(f['log'],t,g)
            self.assertEqual(e['direction'],side);self.assertEqual(e['amount_semantics'],'core_pool_delta_before_afterSwap')
            self.assertIsNone(e['economic_actor']);self.assertIsNone(e['recipient_address'])
            hooks=[x for x in f['receipt']['logs'] if x['topics'] and x['topics'][0]==HOOK]
            self.assertTrue(hooks)
            for log in hooks:
                h=decode_event(log,t,g);self.assertEqual(h['phase'],'hook');self.assertIsNone(h['swap_log_index'])
                self.assertEqual(h['correlation_status'],'unknown')

    def test_v4_currency_direction_truth_table_and_invalid_signs(self):
        f=fixture('v4_buy');g=f['launch'];t=target(f)
        for currency0 in (True,False):
            t['token_address']=g['currency0'] if currency0 else g['currency1'];t['quote_asset_address']=g['currency1'] if currency0 else g['currency0']
            for td,qd,direction in [(7,-3,'buy'),(-7,3,'sell'),(0,3,None),(7,3,None),(-7,-3,None)]:
                a,b=(td,qd) if currency0 else (qd,td)
                f['log']['data']='0x'+encode(['int128','int128','uint160','uint128','int24','uint24'],[a,b,2**96,1,0,0]).hex()
                if direction:self.assertEqual(decode_event(f['log'],t,g)['direction'],direction)
                else:
                    with self.assertRaises(ValueError):decode_event(f['log'],t,g)

    def test_wrong_pool_currency_and_emitter_rejected(self):
        f=fixture('v4_buy');t=target(f);t['token_address']='0x'+'99'*20
        with self.assertRaises(ValueError):decode_event(f['log'],t,f['launch'])
        f['log']['address']='0x'+'99'*20
        with self.assertRaises(ValueError):decode_event(f['log'],target(f),f['launch'])

    def test_boundary_before_after_and_no_double_count(self):
        f=fixture('curve_buy_1');g=f['launch'];f['log']['blockNumber']=hex(g['block_number']);f['log']['logIndex']=hex(g['log_index'])
        with self.assertRaises(ValueError):decode_event(f['log'],target(f),g)
        f['log']['logIndex']=hex(g['log_index']-1);self.assertEqual(decode_event(f['log'],target(f),g)['phase'],'curve')
        f=fixture('v4_buy');f['log']['logIndex']=hex(f['launch']['log_index'])
        with self.assertRaises(ValueError):decode_event(f['log'],target(f),f['launch'])

    def test_duplicate_removed_reincluded_and_replay_idempotent(self):
        e=self.add();self.assertFalse(self.db.store(self.t,e,decode_event(e,self.t)))
        e['removed']=True;self.db.store(self.t,e,decode_event(e,self.t));self.assertEqual(self.metrics()[1]['curve_buy_count'],0)
        e['removed']=False;self.db.store(self.t,e,decode_event(e,self.t));self.assertEqual(self.metrics()[1]['curve_buy_count'],1)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)

    def test_zero_complete_gap_unavailable_and_partial_nonzero(self):
        r,m=self.metrics();self.assertEqual(r['coverage_quality'],'complete');self.assertEqual(m['curve_buy_count'],0)
        self.db.gap(1,1000,1030,'ws_gap');r,m=self.metrics();self.assertEqual(r['coverage_quality'],'unavailable');self.assertIsNone(m['curve_buy_count'])
        self.add();r,m=self.metrics();self.assertEqual(r['coverage_quality'],'partial');self.assertEqual(m['curve_buy_count'],1)

    def test_startup_late_is_never_complete(self):
        with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET coverage_start_at=1010')
        self.add();r,_=self.metrics();self.assertEqual(r['coverage_quality'],'partial');self.assertIn('service_started_late',r['coverage_reason'])

    def test_cutoff_inclusive_and_late_event_rebuild(self):
        self.add(at=1300);self.add(at=1301,index=2);self.add(at=1901,index=3)
        self.assertEqual(self.metrics(300)[1]['curve_buy_count'],1);self.assertEqual(self.metrics(900)[1]['curve_buy_count'],2)
        self.add(at=1299,index=4);self.assertEqual(self.metrics(300)[1]['curve_buy_count'],2)
        self.assertEqual(self.metrics(300)[1]['curve_buy_count'],2)

    def test_counts_quantiles_concentration_creator_and_timing(self):
        self.t['creator_address']='0x'+'11'*20
        with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET creator_address=?',(self.t['creator_address'],))
        self.add(quote=100,tokens=300);self.add(index=2,quote=200,tokens=100,recipient='33');self.add(index=3,buy=False,quote=40,tokens=10)
        _,m=self.metrics();self.assertEqual((m['curve_buy_count'],m['curve_sell_count'],m['unique_buy_recipients']),(2,1,2))
        self.assertEqual(Decimal(m['curve_net_pricing_quote_flow']),Decimal(245)/10**18)
        self.assertEqual(Decimal(m['curve_net_event_quote_flow']),Decimal(260)/10**18)
        self.assertEqual(Decimal(m['curve_buy_quote_event_amount_median']),Decimal(150)/10**18)
        self.assertEqual(Decimal(m['curve_buy_quote_event_amount_p90']),Decimal(190)/10**18)
        self.assertEqual(Decimal(m['top1_buy_recipient_token_share']),Decimal('.75'))
        self.assertEqual(m['same_launch_block_curve_buy_count'],2);self.assertEqual(m['first_3_blocks_curve_buy_count'],2)
        self.assertTrue(m['creator_seen_as_buy_caller']);self.assertFalse(m['creator_seen_as_buy_recipient'])

    def test_prior_history_strictly_before_cutoff_and_typed(self):
        self.add();other=target(launch=2);other['token_address']='0x'+'99'*20;other=insert_target(self.db,other)
        for i,at in enumerate([999,1299,1300,1400],10):
            e=event(other,at=at,index=i);self.db.store(other,e,decode_event(e,other))
        _,m=self.metrics();self.assertEqual(Decimal(m['buy_recipient_prior_events_mean']),2)
        self.assertEqual(Decimal(m['buy_recipient_prior_tracked_tokens_mean']),1)
        e=event(other,at=900,index=20,buy=False);self.db.store(other,e,decode_event(e,other))
        _,m=self.metrics();self.assertEqual(Decimal(m['buy_recipient_prior_events_mean']),2);self.assertEqual(Decimal(m['curve_caller_prior_events_mean']),3)

    def test_initial_has_no_hour_feature(self):
        with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET cohort_long=0,tracking_end_at=1900')
        self.metrics();self.assertEqual([r[0] for r in self.db.conn.execute('SELECT window_seconds FROM flow_features ORDER BY window_seconds')],[30,60,300,900])

    def test_reports_readonly_and_explicit_identity(self):
        self.add();self.metrics();ro=FlowDB(self.path/'flow.db',readonly=True)
        self.assertIsNone(inspect(ro,self.t['token_address'])['events'][0]['payload']['economic_actor'])
        self.assertEqual(usage(ro,FlowSettings(),now=5000)['raw_rows_all_time'],1)
        with self.assertRaises(sqlite3.OperationalError):ro.conn.execute('DELETE FROM flow_events')
        ro.conn.close()


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)
        self.main=main_schema(self.path/'main.db');self.db=FlowDB(self.path/'flow.db');self.db.migrate()
        self.config=Config('https://example.invalid',4663,(),'',self.path/'main.db',self.path/'log',retry_base=.001,retry_max=.001)
        self.settings=FlowSettings(database=self.path/'flow.db')
        self.providers=FlowProviders('https://mainnet.robinhood.validationcloud.io/v1/test',
                                     'wss://mainnet.robinhood.validationcloud.io/v1/test')
        self.worker=FlowWorker(self.config,self.settings,self.db,self.providers)
        self.t=insert_target(self.db,target());self.db.set_state('phase2b_coverage_start_at',900)

    async def asyncTearDown(self):
        await self.worker.rpc.close();self.worker.main.close();self.main.close();self.db.conn.close();self.tmp.cleanup()

    async def test_live_event_path_no_http_and_late_history_dirty(self):
        self.worker.rpc.call=AsyncMock(side_effect=AssertionError('unexpected HTTP'))
        self.worker.ingest(self.t,event(self.t));self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)
        self.worker.rpc.call.assert_not_called();self.assertIn(1,self.worker.dirty)

    async def test_recovery_header_fallback_unique_cached_and_filtered(self):
        e=event(self.t);del e['blockTimestamp']
        async def rpc(method,args):
            if method=='eth_getBlockByNumber':return {'number':e['blockNumber'],'hash':e['blockHash'],'timestamp':hex(1010)}
            self.assertEqual(args[0]['address'],self.t['curve_address']);self.assertEqual(args[0]['topics'],[[BUY,SELL]])
            return [copy.deepcopy(e)]
        self.worker.rpc.call=AsyncMock(side_effect=rpc)
        await self.worker.recover(self.t,self.t['launch_block'],self.t['launch_block'])
        await self.worker.recover(self.t,self.t['launch_block'],self.t['launch_block'])
        self.assertEqual(sum(c.args[0]=='eth_getBlockByNumber' for c in self.worker.rpc.call.call_args_list),1)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events').fetchone()[0],1)

    async def test_recovery_range_reduces_and_bounded_partial(self):
        queries=[]
        async def rpc(method,args):
            q=args[0];span=int(q['toBlock'],16)-int(q['fromBlock'],16)+1;queries.append(span)
            if span>2:raise LogRangeError('range')
            return []
        self.worker.rpc.call=AsyncMock(side_effect=rpc)
        self.settings.recovery_blocks=10
        self.assertFalse(await self.worker.recover(self.t,1,20));self.assertEqual(queries[:3],[10,5,2])

    async def test_headers_cache_reorg(self):
        h=HeaderCache();self.assertFalse(h.head({'number':'0x1','hash':'a','timestamp':'0x64'}))
        self.assertEqual(h.blocks[1],('a',100));self.assertTrue(h.head({'number':'0x1','hash':'b','timestamp':'0x65'}))

    async def test_persisted_sampling_and_fixed_duration(self):
        now=1100
        for i,group in [(2,'not_sampled'),(3,'random_initial'),(4,'random_initial')]:
            t=target(launch=i);t['token_address']='0x'+format(i,'040x')
            self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',(i,t['token_address'],t['quote_asset_address'],t['curve_address'],t['creator_address'],iso(1000),t['launch_block'],0,1))
            self.main.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',(i,0,group,iso(1000)))
        self.main.execute('INSERT INTO outcome_targets VALUES(4,3600,?,?)',('random_long',iso(4600)));self.main.commit()
        self.assertEqual([r['id'] for r in eligible_launches(self.main,now)],[3,4])
        self.assertEqual([r['id'] for r in eligible_launches(self.main,2000)],[4])
        self.worker.header=AsyncMock(return_value=1000);self.worker.rpc.call=AsyncMock(return_value=hex(18))
        with patch('app.flow_worker.time.time',return_value=1100):await self.worker.discover()
        self.assertEqual(self.db.target(3)['tracking_end_at'],1900);self.assertEqual(self.db.target(4)['tracking_end_at'],4600)
        self.assertIsNone(self.db.target(2))

    async def test_restart_restores_target_and_subscriptions(self):
        self.worker.command=AsyncMock(return_value='sub1')
        self.worker.discover=AsyncMock();self.worker.rpc.call=AsyncMock(return_value=hex(self.t['launch_block']))
        self.worker.recover=AsyncMock(return_value=True)
        with patch('app.flow_worker.time.time',return_value=1100):await self.worker.reconcile()
        self.assertIn((1,'curve'),self.worker.subscriptions);self.assertEqual(self.db.target(1)['status'],'active_curve')

    async def test_graduation_subscribes_v4_and_hook_before_curve_unsubscribe(self):
        f=fixture('v4_buy');t=target(f);t['graduation_json']=json.dumps(f['launch']);t['launch_id']=2;t=insert_target(self.db,t)
        self.worker.subscriptions[(2,'curve')]='old';self.worker.command=AsyncMock(side_effect=['swap','hook',True])
        await self.worker.subscribe(t)
        calls=self.worker.command.call_args_list
        self.assertEqual([c.args[0] for c in calls],['eth_subscribe','eth_subscribe','eth_unsubscribe'])
        self.assertEqual(calls[0].args[1][1]['topics'],[SWAP,f['launch']['pool_id']]);self.assertNotIn((2,'curve'),self.worker.subscriptions)

    async def test_budget_persists_attempts_and_cannot_write_main(self):
        self.settings.daily_calls=1
        with patch.object(Rpc,'_send',AsyncMock(side_effect=RpcError('offline'))) as send:
            for _ in range(2):
                with self.assertRaises(RpcError):await self.worker.rpc._send({'method':'eth_call'},'eth_call')
            self.assertEqual(send.await_count,1)
        self.assertEqual(self.db.used('flow_rpc_members',0),1)
        with self.assertRaises(sqlite3.OperationalError):self.worker.main.execute('DELETE FROM launches')
        self.assertEqual(self.main.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    async def test_rate_limit_retries_bounded_and_all_counted(self):
        with patch.object(Rpc,'_send',AsyncMock(side_effect=RetryableRpcError('HTTP 429'))),patch('app.rpc.asyncio.sleep',AsyncMock()):
            with self.assertRaises(RetriesExhausted):await self.worker.rpc.call('eth_blockNumber',[])
        self.assertEqual(self.db.used('flow_rpc_members',0),3);self.assertEqual(self.db.used('flow_rpc_retries',0),2)

    async def test_budget_cannot_extend_expired_tracking(self):
        self.worker.discover=AsyncMock(side_effect=FlowBudget('paused'));self.worker.command=AsyncMock(return_value=True)
        self.worker.subscriptions[(1,'curve')]='old'
        with patch('app.flow_worker.time.time',return_value=4700):await self.worker.reconcile()
        self.assertFalse(self.worker.subscriptions);self.assertEqual(self.db.target(1)['status'],'completed')

    async def test_missing_timestamp_partial_and_no_network(self):
        e=event(self.t);del e['blockTimestamp'];self.worker.rpc.call=AsyncMock()
        with patch('app.flow_data.time.time',return_value=1010):self.worker.ingest(self.t,e)
        self.db.rebuild(self.t,1500)
        self.assertEqual(self.db.conn.execute('SELECT coverage_quality FROM flow_features LIMIT 1').fetchone()[0],'partial')
        self.worker.rpc.call.assert_not_called()

    async def test_reorg_invalidates_other_targets_and_prior_features(self):
        self.worker.ingest(self.t,event(self.t))
        other=target(launch=2);other['token_address']='0x'+'99'*20;other=insert_target(self.db,other)
        self.worker.ingest(other,event(other,index=2))
        self.db.rebuild(self.t,5000);self.db.rebuild(other,5000)
        replacement=event(self.t,at=1400,index=3);replacement['blockHash']='0x'+'99'*32
        self.worker.ingest(self.t,replacement)
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_events WHERE removed=1').fetchone()[0],2)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM flow_features WHERE coverage_quality='complete'").fetchone()[0],0)
        self.assertEqual(float(self.db.state('features_dirty_from')),1010)

    async def test_subscription_cap_marks_partial_without_stopping_main(self):
        self.settings.max_subscriptions=1;self.worker.subscriptions[(99,'curve')]='other'
        self.worker.discover=AsyncMock()
        with patch('app.flow_worker.time.time',return_value=1100):await self.worker.reconcile()
        self.assertEqual(self.db.conn.execute('SELECT reason FROM flow_gaps LIMIT 1').fetchone()[0],'provider_budget')
        self.assertEqual(self.main.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    async def test_ws_budget_counts_packet_then_stops_only_flow(self):
        class Socket:
            def __aiter__(self):return self
            async def __anext__(self):return '{"jsonrpc":"2.0","id":1,"result":"0x1237"}'
        self.worker.socket=Socket();self.settings.daily_ws_bytes=20
        with self.assertRaises(FlowBudget):await self.worker.read_socket()
        self.assertGreater(self.db.used('flow_ws_bytes',0),20)
        self.assertEqual(self.main.execute('SELECT count(*) FROM launches').fetchone()[0],0)

    async def test_provider_split_is_explicit_and_main_config_untouched(self):
        self.assertEqual(self.config.rpc_http,'https://example.invalid')
        self.assertEqual(self.worker.rpc.config.rpc_http,self.providers.http)
        self.assertEqual(self.worker.rpc.config.fallback_http,'')
        self.assertEqual(self.worker.providers.ws('publicnode'),'wss://robinhood-rpc.publicnode.com')
        self.assertEqual(self.worker.providers.ws('validation'),self.providers.ws_fallback)
        self.assertEqual(self.worker.ws_provider,'publicnode')

    async def test_http_provider_counters_at_send_boundary(self):
        with patch.object(Rpc,'_send',AsyncMock(return_value={'result':[]})):
            await self.worker.rpc._send({'method':'eth_getLogs'},'eth_getLogs')
        self.assertEqual(self.db.used('flow_http_calls_validation',0),1)
        self.assertEqual(self.db.used('flow_eth_getLogs_validation',0),1)
        self.assertEqual(self.db.used('flow_http_calls_alchemy',0),0)

    async def test_secondary_budget_ignores_legacy_eight_mb_counter(self):
        self.db.count('flow_ws_bytes',8_000_001)
        self.assertEqual(self.worker.secondary_ws_bytes(0),0)
        self.db.count('flow_ws_bytes_publicnode',63_999_999)
        self.assertLess(self.worker.secondary_ws_bytes(0),self.settings.daily_ws_bytes)
        self.db.count('flow_ws_bytes_validation',1)
        self.assertEqual(self.worker.secondary_ws_bytes(0),self.settings.daily_ws_bytes)

    async def test_primary_failure_fails_over_without_alchemy(self):
        seen=[]
        class FailedConnect:
            async def __aenter__(self):raise OSError('TEST_SECRET_MUST_NOT_APPEAR')
            async def __aexit__(self,*args):pass
        def fail(url,**kwargs):
            seen.append(url)
            if len(seen)==3:raise asyncio.CancelledError
            return FailedConnect()
        self.worker.pressure=lambda:None
        with patch('app.flow_worker.connect',side_effect=fail),patch('app.flow_worker.asyncio.sleep',AsyncMock()):
            with self.assertRaises(asyncio.CancelledError):await self.worker.run()
        self.assertEqual(seen,[self.providers.ws_primary,self.providers.ws_primary,self.providers.ws_fallback])
        self.assertEqual(self.db.used('flow_provider_failovers',0),1)
        self.assertEqual(self.db.used('flow_wss_connections_alchemy',0),0)

    async def test_fallback_restores_subscription_and_reconciles_gap(self):
        self.worker.ws_provider='validation'
        self.db.set_state('connected_once',1)
        self.db.set_state('last_connected_block',self.t['launch_block'])
        self.db.gap(1,1000,1100,'ws_gap',self.t['launch_block'])
        self.worker.command=AsyncMock(return_value='validation-sub')
        self.worker.discover=AsyncMock()
        self.worker.rpc.call=AsyncMock(return_value=hex(self.t['launch_block']+2))
        self.worker.recover=AsyncMock(return_value=True)
        with patch('app.flow_worker.time.time',return_value=1100):await self.worker.reconcile()
        self.assertEqual(self.worker.subscriptions[(1,'curve')],'validation-sub')
        self.assertEqual(self.worker.command.call_args.args[1][1],self.worker.filters(self.t)['curve'])
        self.assertEqual(self.worker.recover.call_args.args[1],self.t['launch_block'])
        self.assertEqual(self.db.conn.execute("SELECT resolved FROM flow_gaps WHERE reason='ws_gap'").fetchone()[0],1)

    async def test_usage_preserves_legacy_fields_and_exposes_routing(self):
        self.db.set_state('current_wss_provider','publicnode')
        self.db.count('flow_ws_bytes_publicnode',123)
        report=usage(self.db,self.settings,now=time.time(),providers=self.providers)
        self.assertIn('flow_ws_bytes',report['metrics'])
        self.assertEqual(report['routing']['wss_bytes']['publicnode'],123)
        self.assertEqual(report['routing']['flow_alchemy_http_requests'],0)
        self.assertEqual(report['routing']['secondary_ws_daily_cap'],64_000_000)

    async def test_bounded_reconnect_does_not_clear_older_gap(self):
        self.db.gap(1,1000,1050,'ws_gap',1)
        self.db.set_state('last_connected_block',self.t['launch_block'])
        self.worker.command=AsyncMock(return_value='sub');self.worker.discover=AsyncMock()
        self.worker.rpc.call=AsyncMock(return_value=hex(self.t['launch_block']));self.worker.recover=AsyncMock(return_value=True)
        with patch('app.flow_worker.time.time',return_value=1100):await self.worker.reconcile()
        self.assertEqual(self.db.conn.execute('SELECT resolved FROM flow_gaps').fetchone()[0],0)

    async def test_pressure_paused_target_still_expires_and_feature_not_zero(self):
        with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET coverage_end_at=1050')
        self.db.gap(1,1050,4600,'provider_budget')
        with patch('app.flow_worker.time.time',return_value=4700):self.worker.finalize(False)
        self.assertEqual(self.db.target(1)['status'],'partial')
        f=self.db.conn.execute('SELECT * FROM flow_features WHERE window_seconds=300').fetchone()
        self.assertEqual(f['coverage_quality'],'unavailable');self.assertIsNone(json.loads(f['metrics'])['curve_buy_count'])

    async def test_dirty_invalidation_survives_database_reopen(self):
        self.worker.ingest(self.t,event(self.t));self.db.conn.close()
        self.db=FlowDB(self.path/'flow.db');self.worker.db=self.db;self.worker.rpc.db=self.db
        self.assertEqual(float(self.db.state('features_dirty_from')),1010)
        with patch('app.flow_worker.time.time',return_value=1500):self.worker.finalize(False)
        self.assertEqual(json.loads(self.db.conn.execute('SELECT metrics FROM flow_features WHERE window_seconds=300').fetchone()[0])['curve_buy_count'],1)

    async def test_client_info_logging_never_exposes_rpc_credentials(self):
        import io,logging,httpx
        from app.flow_worker import main
        stream=io.StringIO();handler=logging.StreamHandler(stream);root=logging.getLogger();root.addHandler(handler)
        names=('httpx','httpcore','websockets');levels={n:logging.getLogger(n).level for n in names}
        try:
            with patch('app.flow_worker.FlowSettings.load',return_value=FlowSettings(enabled=False)),patch('app.flow_worker.Config.load',side_effect=AssertionError('disabled config opened')):main()
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,json={'ok':True}))) as client:
                await client.get('https://example.invalid/v2/TEST_SECRET_MUST_NOT_APPEAR')
            self.assertNotIn('TEST_SECRET_MUST_NOT_APPEAR',stream.getvalue())
            self.assertGreaterEqual(logging.getLogger('httpx').level,logging.WARNING)
        finally:
            root.removeHandler(handler)
            for name,level in levels.items():logging.getLogger(name).setLevel(level)


class OutcomeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();p=Path(self.tmp.name)
        self.db=FlowDB(p/'flow.db');self.db.migrate();self.main=main_schema(p/'main.db');self.t=insert_target(self.db,target())
        self.db.set_state('phase2b_coverage_start_at',900);self.db.rebuild(self.t,5000)
        t=self.t
        self.main.execute('INSERT INTO launches VALUES(?,?,?,?,?,?,?,?,?)',(1,t['token_address'],t['quote_asset_address'],t['curve_address'],t['creator_address'],iso(1000),t['launch_block'],0,1))
        self.main.execute('INSERT INTO stock_assets VALUES(?,?,1)',(t['quote_asset_address'],'NVDA'))
        for h,group,price in [(0,'random_initial','1'),(3600,'random_long','2')]:
            self.main.execute('INSERT INTO outcome_targets VALUES(?,?,?,?)',(1,h,group,iso(1000+h)))
            self.main.execute('INSERT INTO market_snapshots VALUES(?,?,?,?,?,?)',(1,h,t['quote_asset_address'],iso(1000+h),'verified',price))
        self.main.commit()

    def tearDown(self):self.main.close();self.db.conn.close();self.tmp.cleanup()

    def test_invalid_windows_rejected(self):
        for w,h in [(3600,900),(300,300),(31,3600)]:
            with self.assertRaises(ValueError):outcome(self.db,self.main,window=w,horizon=h)

    def test_marginal_n_and_registry_filter(self):
        r=outcome(self.db,self.main,now=5000,ticker='NVDA');self.assertEqual(r['outcome_metric'],'marginal_price_multiple')
        self.assertEqual(r['groups_by_quote_address'][self.t['quote_asset_address']]['ge_2x']['N'],1)
        self.assertEqual(outcome(self.db,self.main,now=5000,ticker='AMD')['denominator_all_stock_launches'],0)

    def test_partial_default_excluded_and_future_outcome_not_used(self):
        self.assertEqual(outcome(self.db,self.main,now=2000)['missingness_exclusive_first_reason']['outcome_not_due'],1)
        self.db.gap(1,1010,1100,'ws_gap');self.db.rebuild(self.t,5000)
        self.assertEqual(outcome(self.db,self.main,now=5000)['missingness_exclusive_first_reason']['feature_partial'],1)

    def test_nonfinite_zero_negative_and_future_snapshots_rejected(self):
        for value in ('NaN','Infinity','0','-1'):
            self.main.execute('UPDATE market_snapshots SET price_quote=? WHERE target_age_seconds=3600',(value,))
            self.assertEqual(outcome(self.db,self.main,now=5000)['missingness_exclusive_first_reason']['invalid_market_price'],1)
        self.main.execute('UPDATE market_snapshots SET price_quote=2,observed_at=? WHERE target_age_seconds=3600',(iso(6000),))
        self.assertEqual(outcome(self.db,self.main,now=5000)['missingness_exclusive_first_reason']['invalid_market_price'],1)

    def test_config_disabled_without_credentials_and_enrichment_rejected(self):
        p=Path(self.tmp.name)/'flow.env';p.write_text('FLOW_TX_ENRICHMENT_ENABLED=true');p.chmod(0o600)
        with patch.dict('os.environ',{'FLOW_ENV':str(p)}):
            with self.assertRaises(ValueError):FlowSettings.load()
        with patch.dict('os.environ',{'FLOW_ENV':str(p)+'missing'}):self.assertFalse(FlowSettings.load().enabled)

    def test_quote_amounts_are_never_pooled_across_assets(self):
        r=outcome(self.db,self.main,now=5000,quote_address='0x'+'00'*20)
        self.assertEqual(r['groups_by_quote_address'],{});self.assertEqual(r['denominator_all_stock_launches'],0)

    def test_sqlite_backup_includes_live_wal_and_migration_is_additive(self):
        import sys
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        from init_flow import initialize
        self.main.execute('PRAGMA journal_mode=WAL');self.main.execute("INSERT INTO stock_assets VALUES('extra','AMD',1)");self.main.commit()
        folder=Path(self.tmp.name)/'backup'
        result=initialize(Path(self.tmp.name)/'main.db',self.db.path,folder)
        self.assertEqual(result['integrity'],{'main':'ok','flow':'ok','flow_after':'ok'})
        backup=sqlite3.connect(folder/'main.db')
        try:self.assertEqual(backup.execute('SELECT count(*) FROM stock_assets').fetchone()[0],2)
        finally:backup.close()
        self.assertEqual(self.db.conn.execute('SELECT count(*) FROM flow_tracking_targets').fetchone()[0],1)
        with self.assertRaises(ValueError):initialize(self.db.path,self.db.path,Path(self.tmp.name)/'unsafe-backup')


if __name__=='__main__':unittest.main()
