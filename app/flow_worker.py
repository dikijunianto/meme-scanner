"""Independent sampled-flow service. Main scanner database is read-only."""
import asyncio
from dataclasses import dataclass, replace
import json
import logging
import os
from pathlib import Path
import sqlite3
import time

from dotenv import dotenv_values
from websockets.asyncio.client import connect
from eth_utils import keccak
from eth_abi.exceptions import DecodingError

from app.config import Config, ROOT
from app.flow_data import FlowDB, BUY, SELL, SWAP, HOOK, decode_event, stamp, iso, WINDOWS
from app.rpc import Rpc, RpcError, LogRangeError, retry_delay

log=logging.getLogger(__name__)


@dataclass
class FlowSettings:
    enabled: bool=False
    database: Path=ROOT/'data/flow.db'
    max_subscriptions: int=64
    daily_calls: int=1000
    minute_calls: int=12
    daily_getlogs: int=400
    recovery_blocks: int=100
    daily_ws_bytes: int=8_000_000

    @classmethod
    def load(cls):
        path=Path(os.environ.get('FLOW_ENV',ROOT/'config/flow.env'))
        if not path.exists():return cls()
        if os.name!='nt' and path.stat().st_mode&0o077:raise ValueError('Flow configuration permissions must be 600')
        env=dotenv_values(path,interpolate=False)
        enabled=env.get('FLOW_TRACKING_ENABLED','false').lower()
        if enabled not in ('true','false'):raise ValueError('Invalid flow enabled flag')
        if env.get('FLOW_TX_ENRICHMENT_ENABLED','false').lower()!='false':raise ValueError('Transaction enrichment is not enabled in this phase')
        if tuple(map(int,env.get('FLOW_FEATURE_WINDOWS','30,60,300,900,3600').split(',')))!=WINDOWS:raise ValueError('Required windows must be preserved')
        result=cls(enabled=='true',Path(env.get('FLOW_DATABASE') or ROOT/'data/flow.db'))
        for attr,key,maximum in [('max_subscriptions','FLOW_MAX_ACTIVE_SUBSCRIPTIONS',128),('daily_calls','FLOW_MAX_HTTP_CALLS_PER_DAY',5000),
            ('minute_calls','FLOW_MAX_HTTP_CALLS_PER_MINUTE',30),('daily_getlogs','FLOW_MAX_RECOVERY_GETLOGS_PER_DAY',2000),
            ('recovery_blocks','FLOW_RECOVERY_MAX_BLOCKS',1000),('daily_ws_bytes','FLOW_MAX_WS_BYTES_PER_DAY',8_000_000)]:
            value=int(env.get(key) or getattr(result,attr))
            if not 1<=value<=maximum:raise ValueError('Unsafe '+key)
            setattr(result,attr,value)
        return result


class FlowBudget(RpcError):
    pass


class FlowRpc(Rpc):
    def __init__(self,config,settings,db):
        super().__init__(replace(config,rpc_rps=.5,retry_attempts=3))
        self.settings,self.db=settings,db
        self.telemetry=self

    def add(self,metric,n=1):
        self.db.count('flow_rpc_'+metric,n)

    async def _send(self,payload,method):
        members=payload if isinstance(payload,list) else [payload]
        now=int(time.time());day=now//86400*86400;minute=now//60*60
        n=len(members);getlogs=sum(m['method']=='eth_getLogs' for m in members)
        if (self.db.used('flow_rpc_members',day)+n>self.settings.daily_calls or
            self.db.used('flow_rpc_members',minute)+n>self.settings.minute_calls or
            self.db.used('flow_eth_getLogs',day)+getlogs>self.settings.daily_getlogs):
            self.db.count('flow_budget_pauses');raise FlowBudget('Phase 2B HTTP budget exhausted')
        # Count attempts before I/O, including failed attempts and retries.
        self.db.count('flow_rpc_members',n);self.db.count('flow_http_calls')
        for member in members:self.db.count('flow_'+member['method'])
        return await super()._send(payload,method)


class HeaderCache:
    def __init__(self):self.blocks={}
    def add(self,number,block_hash,timestamp):
        previous=self.blocks.get(number)
        self.blocks[number]=(block_hash,timestamp)
        return previous is not None and previous[0]!=block_hash
    def head(self,header):
        return self.add(int(header['number'],16),header['hash'],int(header['timestamp'],16))


def eligible_launches(main,now):
    # Authoritative persisted cohort flags, never an independent hash/sample.
    return [dict(r) for r in main.execute('''SELECT l.*,
      EXISTS(SELECT 1 FROM outcome_targets o WHERE o.launch_id=l.id AND o.sampling_group='random_long') cohort_long
      FROM launches l WHERE l.is_stock_quote=1 AND l.block_timestamp>=?
      AND EXISTS(SELECT 1 FROM outcome_targets o WHERE o.launch_id=l.id AND o.sampling_group='random_initial')
      ORDER BY l.id''',(iso(now-3700),))
      if stamp(r['block_timestamp'])+(3600 if r['cohort_long'] else 900)>now]


class FlowWorker:
    def __init__(self,config,settings,db):
        self.config,self.settings,self.db=config,settings,db
        self.main=sqlite3.connect(config.database.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
        self.main.row_factory=sqlite3.Row
        self.rpc=FlowRpc(config,settings,db)
        self.headers=HeaderCache();self.socket=None;self.reader=None;self.pending={};self.sequence=0
        self.subscriptions={};self.routes={};self.queue=asyncio.Queue(maxsize=2048)
        self.connected=False;self.latest_block=0;self.last_tick=0;self.dirty=set()
        self.next_command=0

    def pressure(self):
        # Degrade flow first, leaving the base service's settings untouched.
        import shutil
        paths=[Path('/sys/fs/cgroup/system.slice')/(name+'.service')/'memory.current' for name in ('meme-scanner','meme-scanner-flow')]
        if all(p.exists() for p in paths) and sum(int(p.read_text()) for p in paths)>=350*1024**2:return 'combined_memory'
        if shutil.disk_usage(self.settings.database.parent).free<2_000_000_000:return 'disk_reserve'
        return None

    async def header(self,number):
        if number not in self.headers.blocks:
            h=await self.rpc.call('eth_getBlockByNumber',[hex(number),False])
            self.headers.head(h);self.db.count('flow_recovery_header_calls')
        return self.headers.blocks[number][1]

    async def read_socket(self):
        async for raw in self.socket:
            size=len(raw.encode() if isinstance(raw,str) else raw)
            self.db.count('flow_ws_bytes',size)
            if self.db.used('flow_ws_bytes',int(time.time())//86400*86400)>=self.settings.daily_ws_bytes:
                raise FlowBudget('Phase 2B WS byte budget exhausted')
            message=json.loads(raw)
            if 'id' in message:
                future=self.pending.get(message['id'])
                if future and not future.done():future.set_result(message)
            elif message.get('method')=='eth_subscription':
                # Unknown subscription responses can precede local registration;
                # route at queue-drain time after the ack has been processed.
                self.queue.put_nowait(message['params'])
            else:raise RpcError('Unexpected WS message')

    async def command(self,method,params):
        loop=asyncio.get_running_loop()
        await asyncio.sleep(max(0,self.next_command-loop.time()))
        self.next_command=loop.time()+.1
        self.sequence+=1;number=self.sequence
        future=asyncio.get_running_loop().create_future();self.pending[number]=future
        try:
            await self.socket.send(json.dumps({'jsonrpc':'2.0','id':number,'method':method,'params':params}))
            message=await asyncio.wait_for(future,20)
            return Rpc.result(message,number,method)
        finally:self.pending.pop(number,None)

    def filters(self,t):
        if t['graduation_json']:
            g=json.loads(t['graduation_json'])
            return {'v4':{'address':g['pool_manager_address'],'topics':[SWAP,g['pool_id']]},
                    'hook':{'address':g['hooks'],'topics':[HOOK,g['pool_id']]}}
        return {'curve':{'address':t['curve_address'],'topics':[[BUY,SELL]]}}

    async def subscribe(self,t):
        new=[]
        desired=self.filters(t)
        for kind,query in desired.items():
            key=(t['launch_id'],kind)
            if key in self.subscriptions:continue
            if len(self.subscriptions)>=self.settings.max_subscriptions:
                raise FlowBudget('Phase 2B subscription cap')
            sub=await self.command('eth_subscribe',['logs',query])
            self.subscriptions[key]=sub;self.routes[sub]=(t['launch_id'],kind);new.append(kind)
        for key,sub in list(self.subscriptions.items()):
            if key[0]==t['launch_id'] and key[1] not in desired:
                await self.command('eth_unsubscribe',[sub]);del self.subscriptions[key]
                # Keep route until queued notifications are drained.
        return new

    async def recover(self,t,first,last,filters=None):
        if last<first:return True
        complete=last-first+1<=self.settings.recovery_blocks
        start=max(first,last-self.settings.recovery_blocks+1)
        for query in (filters or self.filters(t)).values():
            current=start;span=10
            while current<=last:
                end=min(last,current+span-1)
                try:
                    rows=await self.rpc.call('eth_getLogs',[dict(query,fromBlock=hex(current),toBlock=hex(end))])
                except LogRangeError:
                    if span==1:raise
                    span=max(1,span//2);continue
                self.db.count('flow_recovery_blocks',end-current+1)
                for item in rows:
                    if not item.get('blockTimestamp'):
                        item['blockTimestamp']=hex(await self.header(int(item['blockNumber'],16)))
                    self.ingest(t,item)
                current=end+1
        return complete

    def ingest(self,t,item):
        try:
            g=json.loads(t['graduation_json']) if t['graduation_json'] else None
            event=decode_event(item,t,g)
            block=int(item['blockNumber'],16)
            self.latest_block=max(self.latest_block,block)
            if item.get('blockTimestamp'):
                at=int(item['blockTimestamp'],16)
                if self.headers.add(block,item['blockHash'],at):
                    affected=self.db.conn.execute('SELECT launch_id,min(event_time) at FROM flow_events WHERE block_number=? AND block_hash!=? GROUP BY launch_id',(block,item['blockHash'])).fetchall()
                    earliest=min([at]+[r['at'] for r in affected])
                    with self.db.conn:
                        self.db.conn.execute('UPDATE flow_events SET removed=1 WHERE block_number=? AND block_hash!=?',(block,item['blockHash']))
                        self.db.conn.execute("INSERT INTO flow_state VALUES('features_dirty_from',?) ON CONFLICT(key) DO UPDATE SET value=min(cast(value AS REAL),excluded.value)",(earliest,))
                    for r in affected:self.db.gap(r['launch_id'],r['at'],max(at,r['at']),'reorg_unresolved')
                    self.db.gap(t['launch_id'],at,at,'reorg_unresolved')
                    self.dirty.update(r[0] for r in self.db.conn.execute('SELECT DISTINCT launch_id FROM flow_events WHERE block_number=?',(block,)))
                if not t['tracking_start_at']<=at<=t['tracking_end_at'] and not item.get('removed'):return
            changed=self.db.store(t,item,event)
            if changed:
                self.dirty.add(t['launch_id'])
                # The store atomically persists the earliest changed event time,
                # including the old time when re-inclusion moves an event.
            if item.get('removed'):
                self.db.gap(t['launch_id'],int(item.get('blockTimestamp','0x0'),16),time.time(),'reorg_unresolved')
        except (ValueError,KeyError,OverflowError,IndexError,DecodingError) as exc:
            self.db.gap(t['launch_id'],t['tracking_start_at'],min(time.time(),t['tracking_end_at']),'unsupported_semantics')
            self.db.count('flow_rejected_events');self.dirty.add(t['launch_id'])
            log.warning('Flow event rejected launch=%s error=%s',t['launch_id'],type(exc).__name__)

    async def discover(self):
        now=time.time()
        for row in eligible_launches(self.main,now):
            if self.db.target(row['id']):continue
            start=await self.header(row['block_number'])
            end=start+(3600 if row['cohort_long'] else 900)
            if end<=now:continue
            key='quote_decimals:'+row['quote_asset_address']
            dec=self.main.execute('SELECT value FROM market_static WHERE key=?',(key,)).fetchone()
            cached=self.db.state(key)
            if dec:decimals=int(dec[0])
            elif cached is not None:decimals=int(cached)
            else:
                raw=await self.rpc.call('eth_call',[{'to':row['quote_asset_address'],'data':'0x'+keccak(text='decimals()').hex()[:8]},'latest'])
                decimals=int(raw,16);self.db.set_state(key,decimals)
            if not 0<=decimals<=36:raise ValueError('Invalid quote decimals')
            with self.db.conn:self.db.conn.execute('''INSERT OR IGNORE INTO flow_tracking_targets
              (launch_id,token_address,quote_asset_address,curve_address,creator_address,quote_decimals,cohort_initial,cohort_long,
               tracking_start_at,tracking_end_at,launch_block,launch_log_index,created_at,updated_at)
              VALUES(?,?,?,?,?,?,1,?,?,?,?,?,?,?)''',
              (row['id'],row['token_address'],row['quote_asset_address'],row['curve_address'],row['creator_address'],decimals,
               row['cohort_long'],start,end,row['block_number'],row['log_index'],now,now))
            if start<float(self.db.state('phase2b_coverage_start_at')):
                self.db.gap(row['id'],start,float(self.db.state('phase2b_coverage_start_at')),'service_started_late')
            self.db.count('flow_targets_created')

    async def reconcile(self):
        now=time.time()
        # Expiry is independent of HTTP availability or discovery success.
        for t in self.db.conn.execute("SELECT * FROM flow_tracking_targets WHERE tracking_end_at+10<? AND status NOT IN ('completed','partial')",(now,)).fetchall():
            for key,sub in list(self.subscriptions.items()):
                if key[0]==t['launch_id']:
                    await self.command('eth_unsubscribe',[sub]);del self.subscriptions[key]
            gaps=self.db.conn.execute('SELECT count(*) FROM flow_gaps WHERE launch_id=? AND resolved=0',(t['launch_id'],)).fetchone()[0]
            status='partial' if gaps or (t['coverage_end_at'] or 0)<t['tracking_end_at'] else 'completed'
            with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET status=?,completed_at=?,updated_at=? WHERE launch_id=?',(status,now,now,t['launch_id']))
            self.dirty.add(t['launch_id']);self.db.count('flow_targets_'+status)
        try:await self.discover()
        except FlowBudget:self.db.count('flow_budget_pauses')
        targets=[dict(r) for r in self.db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')")]
        for t in targets:
            grad=self.main.execute('SELECT * FROM graduations WHERE token_address=? ORDER BY block_number,log_index LIMIT 1',(t['token_address'],)).fetchone()
            transition=bool(grad and not t['graduation_json'])
            if transition:
                g=dict(grad)
                with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET graduation_json=?,pool_id=?,current_phase='v4' WHERE launch_id=?",(json.dumps(g),g['pool_id'],t['launch_id']))
                t=self.db.target(t['launch_id'])
            try:new=await self.subscribe(t)
            except FlowBudget:
                self.db.gap(t['launch_id'],t['coverage_end_at'] or t['tracking_start_at'],now,'provider_budget')
                self.db.count('flow_budget_pauses');continue
            if new:
                transition=bool(t['graduation_json'] and ('v4' in new or 'hook' in new))
                # On reconnect use the last durable known block; on first activation recover launch.
                first=(json.loads(t['graduation_json'])['block_number'] if transition else
                       max(t['launch_block'],int(self.db.state('last_connected_block',t['launch_block']))-2) if t['coverage_start_at'] else t['launch_block'])
                try:
                    last=int(await self.rpc.call('eth_blockNumber',[]),16);self.latest_block=max(self.latest_block,last)
                    complete=await self.recover(t,first,last)
                except RpcError as exc:
                    complete=False;log.warning('Flow recovery incomplete error=%s',type(exc).__name__)
                if not complete:self.db.gap(t['launch_id'],t['last_event_at'] or t['tracking_start_at'],time.time(),'reconnect_recovery_incomplete')
                # A graduation switch also closes the final curve segment.
                if transition:
                    g=json.loads(t['graduation_json'])
                    try:
                        curve_first=max(t['launch_block'],int(self.db.state('last_connected_block',t['launch_block']))-2) if t['coverage_start_at'] else t['launch_block']
                        curve_ok=await self.recover(t,curve_first,g['block_number'],
                            {'curve':{'address':t['curve_address'],'topics':[[BUY,SELL]]}})
                        if not curve_ok:raise RpcError('Incomplete curve boundary')
                    except RpcError:self.db.gap(t['launch_id'],t['tracking_start_at'],now,'graduation_boundary_ambiguous')
                with self.db.conn:
                    self.db.conn.execute('UPDATE flow_tracking_targets SET coverage_start_at=coalesce(coverage_start_at,?),status=?,updated_at=? WHERE launch_id=?',
                        (t['tracking_start_at'] if complete else now,'active_v4' if t['graduation_json'] else 'active_curve',now,t['launch_id']))
                    if complete:self.db.conn.execute("UPDATE flow_gaps SET resolved=1 WHERE launch_id=? AND reason='ws_gap' AND first_block>=? AND first_block<=? AND first_block>0",(t['launch_id'],first,last))
                self.dirty.add(t['launch_id'])

    def drain(self):
        while not self.queue.empty():
            params=self.queue.get_nowait();route=self.routes.get(params['subscription'])
            if not route:raise RpcError('Notification without registered route')
            target=self.db.target(route[0])
            if target:self.ingest(target,params['result'])

    def finalize(self,connected):
        now=time.time()
        if not connected:
            with self.db.conn:self.db.conn.execute("UPDATE flow_tracking_targets SET status='partial',completed_at=?,updated_at=? WHERE tracking_end_at+10<? AND status NOT IN ('completed','partial')",(now,now,now))
        if connected:
            active_ids={key[0] for key in self.subscriptions}
            with self.db.conn:
                for launch in active_ids:
                    self.db.conn.execute('UPDATE flow_tracking_targets SET coverage_end_at=min(tracking_end_at,?),updated_at=? WHERE launch_id=?',(now-3,now,launch))
            self.db.set_state('last_connected_block',self.latest_block)
        if self.db.state('features_dirty_from') is not None:
            self.dirty.update(r[0] for r in self.db.conn.execute('SELECT DISTINCT launch_id FROM flow_features WHERE feature_cutoff_at>=?',(float(self.db.state('features_dirty_from')),)))
        for row in self.db.conn.execute('SELECT * FROM flow_tracking_targets').fetchall():
            t=dict(row)
            due=sum(t['tracking_start_at']+w<=now-3 for w in WINDOWS if w!=3600 or t['cohort_long'])
            existing=self.db.conn.execute('SELECT count(*) FROM flow_features WHERE launch_id=?',(t['launch_id'],)).fetchone()[0]
            if t['launch_id'] in self.dirty or existing<due:
                self.db.rebuild(t,now-3)
                qualities=[r[0] for r in self.db.conn.execute('SELECT coverage_quality FROM flow_features WHERE launch_id=?',(t['launch_id'],))]
                quality='unavailable' if not qualities or all(q=='unavailable' for q in qualities) else 'partial' if any(q!='complete' for q in qualities) else 'complete'
                with self.db.conn:self.db.conn.execute('UPDATE flow_tracking_targets SET coverage_quality=? WHERE launch_id=?',(quality,t['launch_id']))
        self.dirty.clear()
        with self.db.conn:self.db.conn.execute("DELETE FROM flow_state WHERE key='features_dirty_from'")
        counts={k:sum(key[1]==k for key in self.subscriptions) for k in ('curve','v4','hook')}
        size=sum(p.stat().st_size for p in [self.settings.database,Path(str(self.settings.database)+'-wal')] if p.exists())
        with self.db.conn:self.db.conn.execute('INSERT OR REPLACE INTO flow_samples VALUES(?,?,?,?,?,?,?)',
            (int(now)//30*30,len({key[0] for key in self.subscriptions}) if connected else 0,len(self.subscriptions) if connected else 0,counts['curve'] if connected else 0,counts['v4'] if connected else 0,counts['hook'] if connected else 0,size))
        self.db.set_state('heartbeat',now)

    async def run(self):
        if self.db.state('phase2b_coverage_start_at') is None:self.db.set_state('phase2b_coverage_start_at',time.time())
        self.db.set_state('service_status','starting')
        self.dirty.update(r[0] for r in self.db.conn.execute("SELECT launch_id FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"))
        if self.db.state('connected_once'):
            for t in self.db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')").fetchall():
                self.db.gap(t['launch_id'],t['coverage_end_at'] or t['tracking_start_at'],min(time.time(),t['tracking_end_at']),
                            'ws_gap',int(self.db.state('last_connected_block',0)))
        failures=0
        try:
            while True:
                day=int(time.time())//86400*86400
                pressure=self.pressure()
                if pressure:
                    self.db.set_state('service_status','paused_'+pressure);self.finalize(False);await asyncio.sleep(60);continue
                if self.db.used('flow_ws_bytes',day)>=self.settings.daily_ws_bytes:
                    self.db.set_state('service_status','paused_ws_budget');self.finalize(False);await asyncio.sleep(60);continue
                try:
                    async with connect(self.config.rpc_ws,open_timeout=20,ping_interval=20,ping_timeout=20,
                                       max_size=65536,max_queue=4,compression=None) as self.socket:
                        self.reader=asyncio.create_task(self.read_socket());self.subscriptions={};self.routes={}
                        if await self.command('eth_chainId',[])!=hex(self.config.chain_id):raise RpcError('Wrong WS chain')
                        self.connected=True;self.db.set_state('service_status','connected');started=time.time()
                        if self.db.state('connected_once'):self.db.count('flow_subscription_reconnects')
                        self.db.set_state('connected_once',1)
                        while True:
                            if self.pressure():raise FlowBudget('Phase 2B resource reserve reached')
                            if self.reader.done():await self.reader;raise RpcError('WS closed')
                            self.drain()
                            try:await self.reconcile()
                            except FlowBudget:
                                self.db.set_state('service_status','connected_http_budget');self.db.count('flow_budget_pauses')
                            self.drain()
                            if self.reader.done():await self.reader;raise RpcError('WS closed during recovery')
                            self.finalize(True)
                            if time.time()-started>60:failures=0
                            await asyncio.sleep(2)
                except asyncio.CancelledError:raise
                except Exception as exc:
                    failures+=1;log.warning('Flow disconnected error=%s attempts=%d',type(exc).__name__,failures)
                    now=time.time()
                    for t in self.db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"):
                        self.db.gap(t['launch_id'],t['coverage_end_at'] or t['tracking_start_at'],min(now,t['tracking_end_at']),
                                    'provider_budget' if isinstance(exc,FlowBudget) else 'ws_gap',int(self.db.state('last_connected_block',0)))
                        self.dirty.add(t['launch_id'])
                    self.db.set_state('service_status','disconnected');self.finalize(False)
                    await asyncio.sleep(min(60,retry_delay(self.config,min(failures,6))))
                finally:
                    self.connected=False
                    if self.reader:
                        self.reader.cancel();await asyncio.gather(self.reader,return_exceptions=True)
                    # Buffered events are persisted before losing subscription routes.
                    self.drain()
                    self.subscriptions={};self.routes={}
        finally:
            await self.rpc.close();self.main.close();self.db.set_state('service_status','stopped')


def main():
    logging.Formatter.converter=time.gmtime
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    # HTTP client INFO records contain credential-bearing provider URLs.
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.CRITICAL)
    settings=FlowSettings.load()
    if not settings.enabled:
        log.info('Phase 2B disabled; no database or network activity');return
    config=Config.load()
    if settings.database.resolve()==config.database.resolve():raise ValueError('Flow database must be separate from the main database')
    db=FlowDB(settings.database)
    # Migration is an explicit deployment step, never a side effect of starting service.
    if db.state('schema_version')!='1':raise ValueError('Run SQLite-safe flow initialization first')
    asyncio.run(FlowWorker(config,settings,db).run())


def cli():
    try:main()
    except (KeyboardInterrupt,asyncio.CancelledError):pass
    except Exception as exc:
        # Startup/configuration exceptions can embed environment values.
        import sys
        print(f'Flow startup failed ({type(exc).__name__}); check protected configuration',file=sys.stderr)
        raise SystemExit(1) from None


if __name__=='__main__':cli()
