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
from app.flow_providers import FlowProviders, provider
from app.flow_data import FlowDB, BUY, SELL, SWAP, HOOK, decode_event, stamp, iso, WINDOWS
from app.rpc import Rpc, RpcError, LogRangeError, retry_delay

log=logging.getLogger(__name__)
RECOVERY_CHUNK_BLOCKS=10
RECOVERY_MAX_BLOCKS=100


@dataclass
class FlowSettings:
    enabled: bool=False
    database: Path=ROOT/'data/flow.db'
    split_enabled: bool=True
    max_subscriptions: int=64
    daily_calls: int=1000
    minute_calls: int=12
    daily_getlogs: int=400
    daily_ws_bytes: int=64_000_000

    @classmethod
    def load(cls):
        path=Path(os.environ.get('FLOW_ENV',ROOT/'config/flow.env'))
        if not path.exists():return cls()
        if os.name!='nt' and path.stat().st_mode&0o077:raise ValueError('Flow configuration permissions must be 600')
        env=dotenv_values(path,interpolate=False)
        enabled=env.get('FLOW_TRACKING_ENABLED','false').lower()
        if enabled not in ('true','false'):raise ValueError('Invalid flow enabled flag')
        split=env.get('FLOW_PROVIDER_SPLIT_ENABLED','false').lower()
        if split not in ('true','false'):raise ValueError('Invalid flow provider split flag')
        if env.get('FLOW_TX_ENRICHMENT_ENABLED','false').lower()!='false':raise ValueError('Transaction enrichment is not enabled in this phase')
        if tuple(map(int,env.get('FLOW_FEATURE_WINDOWS','30,60,300,900,3600').split(',')))!=WINDOWS:raise ValueError('Required windows must be preserved')
        result=cls(enabled=='true',Path(env.get('FLOW_DATABASE') or ROOT/'data/flow.db'),split=='true')
        for attr,key,maximum in [('max_subscriptions','FLOW_MAX_ACTIVE_SUBSCRIPTIONS',128),('daily_calls','FLOW_MAX_HTTP_CALLS_PER_DAY',5000),
            ('minute_calls','FLOW_MAX_HTTP_CALLS_PER_MINUTE',30),('daily_getlogs','FLOW_MAX_RECOVERY_GETLOGS_PER_DAY',2000),
            ('daily_ws_bytes','FLOW_SECONDARY_WS_BYTES_PER_DAY',64_000_000)]:
            value=int(env.get(key) or getattr(result,attr))
            if not 1<=value<=maximum:raise ValueError('Unsafe '+key)
            setattr(result,attr,value)
        if not result.split_enabled:
            result.daily_ws_bytes=int(env.get('FLOW_MAX_WS_BYTES_PER_DAY') or 8_000_000)
            if not 1<=result.daily_ws_bytes<=8_000_000:raise ValueError('Unsafe legacy WS byte budget')
        return result


class FlowBudget(RpcError):
    def __init__(self, scope, used=None, limit=None, reset_at=None, first_block=None):
        self.scope,self.used,self.limit,self.reset_at,self.first_block=scope,used,limit,reset_at,first_block
        self.category=('UNRECOVERABLE_GAP' if scope=='recovery_range' else
                       'TEMPORARY_BUDGET_WAIT' if scope=='minute_rpc' else
                       'DAILY_BUDGET_EXHAUSTED' if scope in ('daily_rpc','daily_getlogs') else
                       'RESOURCE_LIMIT')
        super().__init__(scope)


class FlowRpc(Rpc):
    def __init__(self,config,settings,db,providers):
        super().__init__(replace(config,rpc_http=providers.http if settings.split_enabled else config.rpc_http,
                                 rpc_ws='',fallback_http='',rpc_rps=.5,retry_attempts=3))
        self.settings,self.db=settings,db
        self.telemetry=self

    def add(self,metric,n=1):
        self.db.count('flow_rpc_'+metric,n)

    async def _send(self,payload,method):
        members=payload if isinstance(payload,list) else [payload]
        now=int(time.time());day=now//86400*86400;minute=now//60*60
        n=len(members);getlogs=sum(m['method']=='eth_getLogs' for m in members)
        routed=provider(self.config.rpc_http)
        # Serialize with the shadow process. Rejected-before-send calls count as zero.
        self.db.conn.execute('BEGIN IMMEDIATE')
        try:
            for scope,used,needed,limit,reset in (
                ('daily_rpc',self.db.used('flow_rpc_members',day),n,self.settings.daily_calls,day+86400),
                ('daily_getlogs',self.db.used('flow_eth_getLogs',day),getlogs,self.settings.daily_getlogs,day+86400),
                ('minute_rpc',self.db.used('flow_rpc_members',minute),n,self.settings.minute_calls,minute+60)):
                if used+needed>limit:raise FlowBudget(scope,used,limit,reset)
            metrics=[('flow_http_calls_'+routed,1),('flow_rpc_members_'+routed,n),
                     ('flow_rpc_members',n),('flow_http_calls',1)]
            if getlogs:metrics += [('flow_eth_getLogs_'+routed,getlogs)]
            metrics += [('flow_'+member['method'],1) for member in members]
            for name,count in metrics:
                self.db.conn.execute('''INSERT INTO flow_usage VALUES(?,?,?) ON CONFLICT(minute,metric)
                  DO UPDATE SET count=count+excluded.count''',(minute,name,count))
            self.db.conn.commit()
        except BaseException as exc:
            self.db.conn.rollback()
            if isinstance(exc,FlowBudget):
                self.db.set_state('budget_pause_provider',routed)
                self.db.set_state('budget_pause_reason',exc.scope)
                self.db.count('flow_budget_rejections')
            raise
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
    def __init__(self,config,settings,db,providers):
        self.config,self.settings,self.db,self.providers=config,settings,db,providers
        self.main=sqlite3.connect(config.database.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
        self.main.row_factory=sqlite3.Row
        self.rpc=FlowRpc(config,settings,db,providers)
        self.headers=HeaderCache();self.socket=None;self.reader=None;self.pending={};self.sequence=0
        self.subscriptions={};self.routes={};self.queue=asyncio.Queue(maxsize=2048)
        self.connected=False;self.latest_block=0;self.last_tick=0;self.dirty=set()
        self.next_command=0
        self.ws_provider='publicnode' if settings.split_enabled else 'alchemy'
        self.pending_recovery=set()
        self.recovery_heads={};self.blocked_recovery={};self.recovery_wait_until=0;self.connection_started_at=0

    # A global observed block is not a safe cursor for individual log filters.
    # These cursors advance only after a complete, committed HTTP range.
    def recovery_plan(self,t,last):
        filters=self.filters(t)
        bases={kind:t['launch_block'] for kind in filters}
        g=json.loads(t['graduation_json']) if t['graduation_json'] else None
        if last<t['launch_block'] or (g and last<g['block_number']):
            raise RpcError('Recovery head precedes target boundary')
        if g:
            filters['curve']={'address':t['curve_address'],'topics':[[BUY,SELL]]}
            bases.update(v4=g['block_number'],hook=g['block_number'],curve=t['launch_block'])
        plans=[]
        for kind,query in filters.items():
            end=min(last,g['block_number']) if kind=='curve' and g else last
            key=f'recovery:{t["launch_id"]}:{kind}'
            persisted=self.db.state(key)
            if persisted is not None and int(persisted)>last+2:raise RpcError('Recovery cursor is ahead of validated head')
            if kind=='curve' and g and persisted is not None and int(persisted)>=end:continue
            start=max(bases[kind],int(persisted)-2) if persisted is not None else bases[kind]
            if end>=start:
                if end-start+1>RECOVERY_MAX_BLOCKS:raise FlowBudget('recovery_range',end-start+1,RECOVERY_MAX_BLOCKS,first_block=start)
                plans.append((t,kind,query,key,start,end))
        return plans

    async def recover_plans(self,plans):
        for t,kind,query,key,first,last in plans:
            g=json.loads(t['graduation_json']) if t['graduation_json'] else None
            boundary=(g['block_number'],g['log_index']) if g else None
            current=first;span=RECOVERY_CHUNK_BLOCKS
            while current<=last:
                minute=int(time.time())//60*60
                if self.db.used('flow_rpc_members',minute)>=self.settings.minute_calls:
                    raise FlowBudget('minute_rpc',self.settings.minute_calls,self.settings.minute_calls,minute+60)
                end=min(last,current+span-1)
                try:rows=await self.rpc.call('eth_getLogs',[dict(query,fromBlock=hex(current),toBlock=hex(end))])
                except LogRangeError:
                    if span==1:raise
                    span=max(1,(end-current+1)//2)
                    continue
                if not isinstance(rows,list):raise RpcError('Invalid recovery logs')
                for item in rows:
                    if not isinstance(item,dict):raise RpcError('Invalid recovery log')
                    try:block=int(item['blockNumber'],16);position=(block,int(item['logIndex'],16))
                    except (KeyError,TypeError,ValueError):raise RpcError('Invalid recovery log position') from None
                    if not current<=block<=end:
                        raise RpcError('Recovery log outside requested range')
                    if item.get('removed'):raise RpcError('Removed log in historical recovery')
                    if kind=='curve' and position<(t['launch_block'],t['launch_log_index']):continue
                    if boundary:
                        if (kind=='curve' and position>=boundary) or (kind!='curve' and position<=boundary):continue
                    if not item.get('blockTimestamp'):
                        item['blockTimestamp']=hex(await self.header(int(item['blockNumber'],16)))
                    if self.ingest(t,item) is False:raise RpcError('Recovery event rejected')
                # Store the cursor after every event in this inclusive chunk committed.
                self.db.set_state(key,end)
                self.db.count('flow_recovery_blocks',end-current+1)
                current=end+1
                self.drain()

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
            self.db.count('flow_ws_bytes_'+self.ws_provider,size)
            if self.secondary_ws_bytes(int(time.time())//86400*86400)>=self.settings.daily_ws_bytes:
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

    def secondary_ws_bytes(self,day):
        if not self.settings.split_enabled:return self.db.used('flow_ws_bytes',day)
        return sum(self.db.used('flow_ws_bytes_'+name,day) for name in ('publicnode','validation'))

    def ws_url(self):
        return self.config.rpc_ws if self.ws_provider=='alchemy' else self.providers.ws(self.ws_provider)

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

    def recovery_fingerprint(self,t):
        return tuple((kind,self.db.state(f'recovery:{t["launch_id"]}:{kind}')) for kind in sorted(self.filters(t)))

    def accept_shadow_handoff(self,t):
        marker=self.db.state(f'flow_shadow_handoff:{t["launch_id"]}')
        if not marker:return False
        head,at=map(int,marker.split(':'))
        if not self.connection_started_at<=at<=time.time():return False
        if any((t['launch_id'],kind) not in self.subscriptions for kind in self.filters(t)):return False
        if any(int(self.db.state(f'recovery:{t["launch_id"]}:{kind}',-1))<head for kind in self.filters(t)):
            return False
        self.pending_recovery.discard(t['launch_id']);self.recovery_heads.pop(t['launch_id'],None)
        self.blocked_recovery.pop(t['launch_id'],None);self.db.count('flow_shadow_handoffs_accepted')
        return True

    def recovery_gap(self,t,first=None):
        exists=self.db.conn.execute("SELECT 1 FROM flow_gaps WHERE launch_id=? AND reason='reconnect_recovery_incomplete' AND resolved=0 LIMIT 1",(t['launch_id'],)).fetchone()
        if not exists:self.db.gap(t['launch_id'],t['last_event_at'] or t['tracking_start_at'],time.time(),
                                  'reconnect_recovery_incomplete',first if first is not None else t['launch_block'])

    def defer_recovery(self,exc):
        if exc.reset_at:self.recovery_wait_until=max(self.recovery_wait_until,exc.reset_at)
        self.db.set_state('service_status',exc.category.lower())
        self.db.set_state('recovery_rejection_scope',exc.scope)
        self.db.count('flow_budget_pauses')
        log.warning('Flow recovery deferred budget_scope=%s used=%s limit=%s '
                    'retry_after_seconds=%s category=%s',exc.scope,exc.used,exc.limit,
                    max(0,int(exc.reset_at-time.time())) if exc.reset_at else None,exc.category)

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

    def ingest(self,t,item,shadow=False):
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
            changed=self.db.store(t,item,event,shadow=shadow)
            if changed:
                self.dirty.add(t['launch_id'])
                # The store atomically persists the earliest changed event time,
                # including the old time when re-inclusion moves an event.
            if item.get('removed'):
                self.db.gap(t['launch_id'],int(item.get('blockTimestamp','0x0'),16),time.time(),'reorg_unresolved')
            return True
        except (ValueError,KeyError,OverflowError,IndexError,DecodingError) as exc:
            self.db.gap(t['launch_id'],t['tracking_start_at'],min(time.time(),t['tracking_end_at']),'unsupported_semantics')
            self.db.count('flow_rejected_events');self.dirty.add(t['launch_id'])
            log.warning('Flow event rejected launch=%s error=%s',t['launch_id'],type(exc).__name__)
            return False

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
            self.pending_recovery.discard(t['launch_id']);self.recovery_heads.pop(t['launch_id'],None)
            self.blocked_recovery.pop(t['launch_id'],None)
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
        recovery=[]
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
            if new or t['launch_id'] in self.pending_recovery:
                self.pending_recovery.add(t['launch_id'])
                if new:
                    self.recovery_heads.pop(t['launch_id'],None)
                    self.blocked_recovery.pop(t['launch_id'],None)
                recovery.append(t)
        if recovery:
            recovery=[t for t in recovery if not self.accept_shadow_handoff(t)]
        if recovery and time.time()<self.recovery_wait_until:return
        if recovery:
            try:
                if any(t['launch_id'] not in self.recovery_heads for t in recovery):
                    # Pin a head once; retries cannot chase a moving chain.
                    last=int(await self.rpc.call('eth_blockNumber',[]),16)
                    self.latest_block=max(self.latest_block,last)
                    for t in recovery:self.recovery_heads.setdefault(t['launch_id'],last)
            except FlowBudget:
                raise
            except RpcError as exc:
                log.warning('Flow recovery head failed error=%s',type(exc).__name__)
                for t in recovery:self.recovery_gap(t)
                self.recovery_wait_until=max(self.recovery_wait_until,time.time()+15)
                self.db.set_state('service_status','provider_error')
                self.db.set_state('recovery_rejection_scope','provider_error')
                return
            for t in recovery:
                launch=t['launch_id']
                fingerprint=self.recovery_fingerprint(t)
                if self.blocked_recovery.get(launch)==fingerprint:continue
                try:
                    last=self.recovery_heads[launch]
                    plans=self.recovery_plan(t,last)
                    await self.recover_plans(plans)
                except FlowBudget as exc:
                    self.recovery_gap(t,exc.first_block)
                    if exc.category=='UNRECOVERABLE_GAP':
                        self.blocked_recovery[launch]=fingerprint
                        self.db.set_state('recovery_rejection_scope',exc.scope)
                        self.db.set_state('service_status','unrecoverable_gap')
                        log.warning('Flow recovery blocked budget_scope=%s used=%s limit=%s category=%s',
                                    exc.scope,exc.used,exc.limit,exc.category)
                        continue
                    raise
                except RpcError as exc:
                    self.recovery_gap(t)
                    log.warning('Flow recovery provider error=%s',type(exc).__name__)
                    self.recovery_wait_until=max(self.recovery_wait_until,time.time()+15)
                    self.db.set_state('service_status','provider_error')
                    self.db.set_state('recovery_rejection_scope','provider_error')
                    continue
                with self.db.conn:
                    self.db.conn.execute('UPDATE flow_tracking_targets SET coverage_start_at=coalesce(coverage_start_at,?),status=?,updated_at=? WHERE launch_id=?',
                        (t['tracking_start_at'],'active_v4' if t['graduation_json'] else 'active_curve',time.time(),launch))
                    # Only the exact queried suffix is proved; older gaps remain visible.
                    if plans:
                        first=min(p[4] for p in plans)
                        self.db.conn.execute("""UPDATE flow_gaps SET resolved=1 WHERE launch_id=?
                          AND reason IN ('ws_gap','reconnect_recovery_incomplete')
                          AND first_block BETWEEN ? AND ? AND end_at<=?""",
                          (launch,first,last,time.time()))
                self.pending_recovery.discard(launch);self.recovery_heads.pop(launch,None)
                self.blocked_recovery.pop(launch,None);self.dirty.add(launch)
        if not self.pending_recovery and self.db.state('service_status') in (
                'temporary_budget_wait','daily_budget_exhausted','unrecoverable_gap','provider_error'):
            self.db.set_state('service_status','connected')

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
                if self.secondary_ws_bytes(day)>=self.settings.daily_ws_bytes:
                    self.db.set_state('service_status','paused_ws_budget')
                    self.db.set_state('budget_pause_provider',self.ws_provider)
                    self.db.set_state('budget_pause_reason','secondary_ws_bytes')
                    self.finalize(False);await asyncio.sleep(60);continue
                try:
                    routed=provider(self.ws_url())
                    self.db.count('flow_wss_connections_'+routed)
                    async with connect(self.ws_url(),open_timeout=20,ping_interval=20,ping_timeout=20,
                                       max_size=65536,max_queue=4,compression=None) as self.socket:
                        self.reader=asyncio.create_task(self.read_socket());self.subscriptions={};self.routes={}
                        if await self.command('eth_chainId',[])!=hex(self.config.chain_id):raise RpcError('Wrong WS chain')
                        self.connection_started_at=time.time()
                        self.connected=True;self.db.set_state('service_status','connected')
                        self.db.set_state('current_wss_provider',routed);started=time.time()
                        if self.db.state('connected_once'):self.db.count('flow_subscription_reconnects')
                        if self.db.state('connected_once'):self.db.count('flow_provider_reconnects_'+routed)
                        self.db.set_state('connected_once',1)
                        while True:
                            if self.pressure():raise FlowBudget('Phase 2B resource reserve reached')
                            if self.reader.done():await self.reader;raise RpcError('WS closed')
                            self.drain()
                            try:await self.reconcile()
                            except FlowBudget as exc:
                                self.defer_recovery(exc)
                            self.drain()
                            if self.reader.done():await self.reader;raise RpcError('WS closed during recovery')
                            self.finalize(True)
                            if time.time()-started>60:failures=0
                            await asyncio.sleep(2)
                except asyncio.CancelledError:raise
                except Exception as exc:
                    failures+=1
                    self.db.count('flow_provider_connection_errors_'+self.ws_provider)
                    log.warning('Flow disconnected provider=%s error=%s attempts=%d',self.ws_provider,type(exc).__name__,failures)
                    now=time.time()
                    for t in self.db.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"):
                        self.db.gap(t['launch_id'],t['coverage_end_at'] or t['tracking_start_at'],min(now,t['tracking_end_at']),
                                    'provider_budget' if isinstance(exc,FlowBudget) else 'ws_gap',int(self.db.state('last_connected_block',0)))
                        self.dirty.add(t['launch_id'])
                    self.db.set_state('service_status','disconnected');self.finalize(False)
                    if self.ws_provider=='publicnode' and failures>=2:
                        self.ws_provider='validation';self.db.count('flow_provider_failovers')
                        failures=1
                        log.warning('Flow failover to Validation WSS; HTTP gap recovery required')
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
    providers=FlowProviders.load()
    if settings.database.resolve()==config.database.resolve():raise ValueError('Flow database must be separate from the main database')
    db=FlowDB(settings.database)
    # Migration is an explicit deployment step, never a side effect of starting service.
    if db.state('schema_version')!='1':raise ValueError('Run SQLite-safe flow initialization first')
    asyncio.run(FlowWorker(config,settings,db,providers).run())


def cli():
    try:main()
    except (KeyboardInterrupt,asyncio.CancelledError):pass
    except Exception as exc:
        # Startup/configuration exceptions can embed environment values.
        import sys
        print(f'Flow startup failed ({type(exc).__name__}); check protected configuration',file=sys.stderr)
        raise SystemExit(1) from None


if __name__=='__main__':cli()
