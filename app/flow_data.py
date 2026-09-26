"""Isolated flow storage, event semantics and deterministic local features."""
import json
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

from eth_abi import decode, encode
from eth_utils import keccak
from app.models import hash32

WINDOWS = (30, 60, 300, 900, 3600)
BUY = '0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455'
SELL = '0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df'
SWAP = '0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f'
HOOK = '0x' + keccak(text='HookFeeCollected(bytes32,address,uint256,uint256)').hex()


def stamp(value):
    return datetime.fromisoformat(value).timestamp() if isinstance(value, str) else value


def iso(value):
    return (datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(seconds=value)).isoformat()


def normalized(value, decimals):
    with localcontext() as ctx:
        ctx.prec = 90
        return str(Decimal(value) / Decimal(10)**decimals)


def quantile(values, p):
    if not values:
        return None
    values = sorted(Decimal(v) for v in values)
    with localcontext() as ctx:
        ctx.prec = 90
        index = Decimal(len(values)-1)*Decimal(str(p))
        low = int(index)
        return str(values[low]+(values[min(low+1,len(values)-1)]-values[low])*(index-low))


def decode_event(log, target, graduation=None):
    """Preflight B semantics; identities never promoted to economic actors."""
    hash32(log['transactionHash']);hash32(log['blockHash'])
    if int(log['blockNumber'],16)<0 or int(log['logIndex'],16)<0:raise ValueError('Invalid log position')
    topics, raw = log['topics'], bytes.fromhex(log['data'][2:])
    emitter = log['address'].lower()
    base = dict(transaction_from=None, economic_actor=None, economic_actor_basis=None,
                token_address=target['token_address'],quote_asset_address=target['quote_asset_address'],
                curve_address=target['curve_address'],
                caller_address=None, recipient_address=None, swap_sender=None,
                direction=None, origin_class='unknown')
    qd = target['quote_decimals']
    if not 0 <= qd <= 36:
        raise ValueError('Unsupported quote decimals')
    def addr(word):
        return decode(['address'],bytes.fromhex(word[2:]))[0]
    if topics[0] in (BUY, SELL):
        if emitter != target['curve_address'].lower() or len(topics)!=3 or len(raw)!=128:
            raise ValueError('Unexpected curve log')
        if graduation and (int(log['blockNumber'],16),int(log['logIndex'],16)) >= (graduation['block_number'],graduation['log_index']):
            raise ValueError('Curve event at/after graduation boundary')
        a,b,fee,tax = decode(['uint256']*4,raw)
        buying=topics[0]==BUY
        tokens,quote=(b,a) if buying else (a,b)
        pricing=quote-fee-tax if buying else quote+fee+tax
        if min(tokens,quote,pricing)<=0:
            raise ValueError('Invalid trade amounts')
        base.update(phase='curve',direction='buy' if buying else 'sell',
            caller_address=addr(topics[1]),recipient_address=addr(topics[2]),
            source_event_name='CurveBuy' if buying else 'CurveSell',refund_quote_raw=None,
            refund_quote_normalized=None)
        for key,value,decimals in [('tokens_amount',tokens,18),('quote_event_amount',quote,qd),
                ('pricing_quote_amount',pricing,qd),('base_fee_quote',fee,qd),('creator_tax_quote',tax,qd)]:
            base[key+'_raw']=str(value);base[key+'_normalized']=normalized(value,decimals)
    else:
        if not graduation:
            raise ValueError('Pool not verified')
        g=graduation
        if {g['currency0'].lower(),g['currency1'].lower()} != {target['token_address'].lower(),target['quote_asset_address'].lower()}:
            raise ValueError('Pool currencies mismatch')
        key=[g['currency0'],g['currency1'],g['fee'],g['tick_spacing'],g['hooks']]
        pool='0x'+keccak(encode(['address','address','uint24','int24','address'],key)).hex()
        if pool!=g['pool_id'].lower() or len(topics)<2 or topics[1].lower()!=pool:
            raise ValueError('PoolId mismatch')
        if (int(log['blockNumber'],16),int(log['logIndex'],16)) <= (g['block_number'],g['log_index']):
            raise ValueError('Pool event before verified graduation boundary')
        base.update(pool_id=pool,currency0=g['currency0'],currency1=g['currency1'],pool_manager_address=g['pool_manager_address'])
        if topics[0]==SWAP:
            if emitter!=g['pool_manager_address'].lower() or len(topics)!=3 or len(raw)!=192:
                raise ValueError('Unexpected swap')
            a,b,sqrt,liquidity,tick,fee=decode(['int128','int128','uint160','uint128','int24','uint24'],raw)
            td,qd_raw=(a,b) if target['token_address'].lower()==g['currency0'].lower() else (b,a)
            if td*qd_raw>=0:
                raise ValueError('Ambiguous swap direction')
            sender=addr(topics[2])
            base.update(phase='v4',swap_sender=sender,direction='buy' if td>0 else 'sell',
                amount0_raw=str(a),amount1_raw=str(b),token_core_delta_raw=str(td),quote_core_delta_raw=str(qd_raw),
                token_core_amount_normalized=normalized(abs(td),18),quote_core_amount_normalized=normalized(abs(qd_raw),qd),
                sqrt_price_x96=str(sqrt),liquidity=str(liquidity),tick=tick,swap_event_fee=fee,
                source_event_name='Swap',amount_semantics='core_pool_delta_before_afterSwap',
                origin_class='hook_self_call' if sender==g['hooks'].lower() else 'unknown')
        elif topics[0]==HOOK:
            if emitter!=g['hooks'].lower() or len(topics)!=2 or len(raw)!=96:
                raise ValueError('Unexpected hook event')
            currency,fee,tax=decode(['address','uint256','uint256'],raw)
            if currency not in (target['token_address'].lower(),target['quote_asset_address'].lower()):
                raise ValueError('Unknown hook currency')
            decimals=18 if currency==target['token_address'].lower() else qd
            base.update(phase='hook',currency=currency,base_fee_raw=str(fee),creator_tax_raw=str(tax),
                base_fee_normalized=normalized(fee,decimals),creator_tax_normalized=normalized(tax,decimals),
                source_event_name='HookFeeCollected',correlation_status='unknown',swap_log_index=None)
        else:
            raise ValueError('Unknown event topic')
    return base


class FlowDB:
    def __init__(self,path,readonly=False):
        self.path=Path(path)
        self.conn=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro' if readonly else str(path),uri=readonly,timeout=2)
        self.conn.row_factory=sqlite3.Row
        self.conn.execute('PRAGMA busy_timeout=2000')

    def migrate(self):
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.executescript('''
        CREATE TABLE IF NOT EXISTS flow_state(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS flow_tracking_targets(
          launch_id INTEGER PRIMARY KEY,token_address TEXT NOT NULL UNIQUE,quote_asset_address TEXT NOT NULL,
          curve_address TEXT NOT NULL,creator_address TEXT NOT NULL,quote_decimals INTEGER NOT NULL,
          cohort_initial INTEGER NOT NULL,cohort_long INTEGER NOT NULL,tracking_start_at REAL NOT NULL,
          tracking_end_at REAL NOT NULL,launch_block INTEGER NOT NULL,launch_log_index INTEGER NOT NULL,
          current_phase TEXT NOT NULL DEFAULT 'curve',pool_id TEXT,graduation_json TEXT,
          status TEXT NOT NULL DEFAULT 'scheduled',coverage_start_at REAL,coverage_end_at REAL,coverage_quality TEXT NOT NULL DEFAULT 'unavailable',
          last_event_block INTEGER,last_event_at REAL,completed_at REAL,error_code TEXT,
          created_at REAL NOT NULL,updated_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS flow_events(
          chain_id INTEGER NOT NULL,tx_hash TEXT NOT NULL,log_index INTEGER NOT NULL,launch_id INTEGER NOT NULL,
          phase TEXT NOT NULL,block_number INTEGER NOT NULL,block_hash TEXT NOT NULL,event_time REAL NOT NULL,
          event_time_source TEXT NOT NULL,observed_at REAL NOT NULL,removed INTEGER NOT NULL,
          direction TEXT,caller_address TEXT,recipient_address TEXT,swap_sender TEXT,economic_actor TEXT,
          source_contract TEXT NOT NULL,data_quality TEXT NOT NULL,payload TEXT NOT NULL,
          PRIMARY KEY(chain_id,tx_hash,log_index));
        CREATE INDEX IF NOT EXISTS flow_events_launch_time ON flow_events(launch_id,event_time);
        CREATE INDEX IF NOT EXISTS flow_events_block ON flow_events(block_number);
        CREATE INDEX IF NOT EXISTS flow_events_caller ON flow_events(caller_address,event_time);
        CREATE INDEX IF NOT EXISTS flow_events_recipient ON flow_events(recipient_address,event_time);
        CREATE INDEX IF NOT EXISTS flow_events_sender ON flow_events(swap_sender,event_time);
        CREATE VIEW IF NOT EXISTS curve_trade_events AS SELECT * FROM flow_events WHERE phase='curve';
        CREATE VIEW IF NOT EXISTS v4_swap_events AS SELECT * FROM flow_events WHERE phase='v4';
        CREATE VIEW IF NOT EXISTS v4_hook_fee_events AS SELECT * FROM flow_events WHERE phase='hook';
        CREATE TABLE IF NOT EXISTS flow_gaps(id INTEGER PRIMARY KEY,launch_id INTEGER NOT NULL,
          start_at REAL NOT NULL,end_at REAL NOT NULL,reason TEXT NOT NULL,resolved INTEGER NOT NULL DEFAULT 0,first_block INTEGER);
        CREATE TABLE IF NOT EXISTS flow_features(launch_id INTEGER NOT NULL,window_seconds INTEGER NOT NULL,
          token_address TEXT NOT NULL,quote_asset_address TEXT NOT NULL,feature_cutoff_at REAL NOT NULL,
          finalized_at REAL NOT NULL,coverage_start_at REAL,coverage_end_at REAL,coverage_quality TEXT NOT NULL,
          coverage_reason TEXT NOT NULL,metrics TEXT NOT NULL,
          expected_window_start REAL GENERATED ALWAYS AS (feature_cutoff_at-window_seconds) VIRTUAL,
          expected_window_end REAL GENERATED ALWAYS AS (feature_cutoff_at) VIRTUAL,
          PRIMARY KEY(launch_id,window_seconds));
        CREATE TABLE IF NOT EXISTS flow_usage(minute INTEGER NOT NULL,metric TEXT NOT NULL,count INTEGER NOT NULL,
          PRIMARY KEY(minute,metric));
        CREATE TABLE IF NOT EXISTS flow_samples(at REAL PRIMARY KEY,active INTEGER,subscriptions INTEGER,
          curve_subscriptions INTEGER,v4_subscriptions INTEGER,hook_subscriptions INTEGER,db_bytes INTEGER);
        ''')
        self.conn.commit()

    def state(self,key,default=None):
        r=self.conn.execute('SELECT value FROM flow_state WHERE key=?',(key,)).fetchone()
        return r[0] if r else default

    def set_state(self,key,value):
        with self.conn:self.conn.execute('INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,str(value)))

    def count(self,metric,n=1,now=None):
        minute=int(now if now is not None else time.time())//60*60
        with self.conn:self.conn.execute('INSERT INTO flow_usage VALUES(?,?,?) ON CONFLICT(minute,metric) DO UPDATE SET count=count+excluded.count',(minute,metric,n))

    def used(self,metric,since):
        return self.conn.execute('SELECT coalesce(sum(count),0) FROM flow_usage WHERE metric=? AND minute>=?',(metric,since)).fetchone()[0]

    def gap(self,launch_id,start,end,reason,first_block=None):
        with self.conn:
            self.conn.execute('INSERT INTO flow_gaps(launch_id,start_at,end_at,reason,first_block) VALUES(?,?,?,?,?)',(launch_id,start,max(start,end),reason,first_block))
            # Persist invalidation atomically; a crash must not leave stale complete rows.
            self.conn.execute("UPDATE flow_features SET coverage_quality='partial',coverage_reason=? WHERE launch_id=? AND feature_cutoff_at>=?",(reason,launch_id,start))

    def target(self,launch_id):
        r=self.conn.execute('SELECT * FROM flow_tracking_targets WHERE launch_id=?',(launch_id,)).fetchone()
        return dict(r) if r else None

    def store(self,target,log,event,observed=None,shadow=False):
        if shadow:
            if not log.get('blockTimestamp'):raise ValueError('Shadow log requires a verified block timestamp')
            # Serialize the duplicate read with the live writer's commit.
            self.conn.execute('BEGIN IMMEDIATE')
            try:return self._store(target,log,event,observed,shadow)
            except BaseException:
                self.conn.rollback()
                raise
        return self._store(target,log,event,observed,shadow)

    def _store(self,target,log,event,observed,shadow):
        observed=observed or time.time()
        event_time=int(log['blockTimestamp'],16) if log.get('blockTimestamp') else observed
        source='log_block_timestamp' if log.get('blockTimestamp') else 'observed_at'
        if source=='observed_at':self.gap(target['launch_id'],target['tracking_start_at'],target['tracking_end_at'],'unknown_timestamp')
        identity=(4663,log['transactionHash'].lower(),int(log['logIndex'],16))
        removed=int(log.get('removed',False))
        old=self.conn.execute('SELECT removed,block_hash,event_time FROM flow_events WHERE chain_id=? AND tx_hash=? AND log_index=?',identity).fetchone()
        if old and old['removed']==removed and old['block_hash']==log['blockHash']:
            self.count('flow_duplicate_events')
            if shadow:self.count(f'flow_shadow_duplicates:{shadow}:{target["launch_id"]}:{event["phase"]}')
            return False
        with self.conn:
            dirty_from=min(event_time,old['event_time'] if old else event_time,float(self.state('features_dirty_from',event_time)))
            self.conn.execute("INSERT INTO flow_state VALUES('features_dirty_from',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(dirty_from),))
            self.conn.execute('''INSERT INTO flow_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(chain_id,tx_hash,log_index) DO UPDATE SET removed=excluded.removed,
              block_number=excluded.block_number,block_hash=excluded.block_hash,event_time=excluded.event_time,
              event_time_source=excluded.event_time_source,data_quality=excluded.data_quality,payload=excluded.payload,
              launch_id=excluded.launch_id,phase=excluded.phase,direction=excluded.direction,caller_address=excluded.caller_address,
              recipient_address=excluded.recipient_address,swap_sender=excluded.swap_sender,source_contract=excluded.source_contract''',
              (*identity,target['launch_id'],event['phase'],int(log['blockNumber'],16),log['blockHash'],event_time,
               source,observed,removed,event.get('direction'),event.get('caller_address'),event.get('recipient_address'),
               event.get('swap_sender'),None,log['address'].lower(),'verified' if source!='observed_at' else 'partial',json.dumps(event)))
            self.conn.execute('''UPDATE flow_tracking_targets SET
              last_event_block=max(coalesce(last_event_block,0),?),
              last_event_at=max(coalesce(last_event_at,0),?) WHERE launch_id=?''',
                              (int(log['blockNumber'],16),event_time,target['launch_id']))
            if shadow:
                self.conn.execute('INSERT INTO flow_usage VALUES(?,?,1) ON CONFLICT(minute,metric) DO UPDATE SET count=count+1',
                                  (int(time.time())//60*60,f'flow_shadow_events_stored:{shadow}:{target["launch_id"]}:{event["phase"]}'))
        self.count('flow_removed_events' if removed else 'flow_events_stored')
        if not removed:self.count('flow_'+event['phase']+'_'+(event.get('direction') or 'fee')+'_events')
        if not removed and event['phase']=='v4':self.count('flow_v4_swap_events')
        return True

    def rebuild(self,target,now=None):
        now=time.time() if now is None else now
        for window in WINDOWS:
            if window==3600 and not target['cohort_long']:continue
            end=target['tracking_start_at']+window
            if end>now:continue
            rows=[dict(r) for r in self.conn.execute('SELECT * FROM flow_events WHERE launch_id=? AND event_time>=? AND event_time<=? AND removed=0 ORDER BY event_time,log_index',
                    (target['launch_id'],target['tracking_start_at'],end))]
            gaps=self.conn.execute('SELECT reason FROM flow_gaps WHERE launch_id=? AND start_at<=? AND end_at>=? AND resolved=0',
                    (target['launch_id'],end,target['tracking_start_at'])).fetchall()
            reasons=sorted({r[0] for r in gaps})
            if target['coverage_start_at'] is None or target['coverage_start_at']>target['tracking_start_at']:reasons.append('service_started_late')
            if target['coverage_end_at'] is None or target['coverage_end_at']<end:reasons.append('coverage_not_confirmed')
            quality='partial' if reasons else 'complete'
            if reasons and not rows:quality='unavailable'
            metrics=features(self,target,rows,end)
            if quality=='unavailable':metrics={k:None for k in metrics}
            with self.conn:self.conn.execute('''INSERT INTO flow_features VALUES(?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(launch_id,window_seconds) DO UPDATE SET finalized_at=excluded.finalized_at,
              coverage_start_at=excluded.coverage_start_at,coverage_end_at=excluded.coverage_end_at,
              coverage_quality=excluded.coverage_quality,coverage_reason=excluded.coverage_reason,metrics=excluded.metrics''',
              (target['launch_id'],window,target['token_address'],target['quote_asset_address'],end,now,
               target['coverage_start_at'],min(end,target['coverage_end_at']) if target['coverage_end_at'] is not None else None,
               quality,','.join(sorted(set(reasons))) or 'complete',json.dumps(metrics)))


def features(db,target,rows,cutoff):
    with localcontext() as ctx:
        ctx.prec=90
        return _features(db,target,rows,cutoff)


def _features(db,target,rows,cutoff):
    groups=defaultdict(list)
    for row in rows:groups[row['phase']].append({**row,**json.loads(row['payload'])})
    curve,v4,hooks=groups['curve'],groups['v4'],groups['hook']
    buy=[r for r in curve if r['direction']=='buy'];sell=[r for r in curve if r['direction']=='sell']
    m={'curve_event_count':len(curve),'v4_event_count':len(v4),'v4_core_swap_count':len(v4),
       'unique_swap_senders':len({r['swap_sender'] for r in v4}), 'unique_known_economic_actors':0,
       'known_actor_coverage_pct':'0' if curve or v4 else None}
    def total(rows,key):return sum((Decimal(r[key]) for r in rows),Decimal(0))
    for side,events in [('buy',buy),('sell',sell)]:
        m['curve_'+side+'_count']=len(events)
        for role in ['caller','recipient']:m['unique_'+side+'_'+role+'s']=len({r[role+'_address'] for r in events})
        for leg in ['quote_event_amount','pricing_quote_amount']:
            vals=[r[leg+'_normalized'] for r in events]
            for label,p in [('median',.5),('p75',.75),('p90',.9),('max',1)]:m[f'curve_{side}_{leg}_{label}']=quantile(vals,p)
    for name,events,key in [('curve_tokens_bought',buy,'tokens_amount'),('curve_tokens_sold',sell,'tokens_amount'),
        ('curve_buy_gross_quote_spent',buy,'quote_event_amount'),('curve_sell_net_quote_received',sell,'quote_event_amount'),
        ('curve_buy_pricing_quote_in',buy,'pricing_quote_amount'),('curve_sell_gross_priced_quote_out',sell,'pricing_quote_amount'),
        ('curve_base_fee_quote',curve,'base_fee_quote'),('curve_creator_tax_quote',curve,'creator_tax_quote')]:
        m[name]=str(total(events,key+'_normalized'))
    m['curve_net_pricing_quote_flow']=str(Decimal(m['curve_buy_pricing_quote_in'])-Decimal(m['curve_sell_gross_priced_quote_out']))
    m['curve_net_event_quote_flow']=str(Decimal(m['curve_buy_gross_quote_spent'])-Decimal(m['curve_sell_net_quote_received']))
    recipients=defaultdict(Decimal)
    for r in buy:recipients[r['recipient_address']]+=Decimal(r['tokens_amount_normalized'])
    ordered=sorted(recipients.values(),reverse=True);den=sum(ordered,Decimal(0))
    for n in (1,3,5,10):m[f'top{n}_buy_recipient_token_share']=str(sum(ordered[:n])/den) if den else None
    for label,pred in [('same_launch_block',lambda r:r['block_number']==target['launch_block']),
                       ('first_3_blocks',lambda r:target['launch_block']<=r['block_number']<target['launch_block']+3)]:
        early=[r for r in buy if pred(r)]
        m[label+'_curve_buy_count']=len(early);m[label+'_unique_buy_recipients']=len({r['recipient_address'] for r in early})
    for seconds in (30,60):m[f'first_{seconds}s_curve_buy_count']=sum(r['event_time']<=target['tracking_start_at']+seconds for r in buy)
    creator=target['creator_address'].lower();ages=[]
    for side,events in [('buy',buy),('sell',sell)]:
        for role in ['caller','recipient']:
            matches=[r for r in events if r[role+'_address']==creator]
            m[f'creator_seen_as_{side}_{role}']=bool(matches);ages.extend(r['event_time']-target['tracking_start_at'] for r in matches)
    matches=[r for r in v4 if r['swap_sender']==creator]
    m['creator_seen_as_v4_swap_sender']=bool(matches);ages.extend(r['event_time']-target['tracking_start_at'] for r in matches)
    m['creator_first_seen_age_seconds']=min(ages) if ages else None
    for side in ['buy','sell']:
        events=[r for r in v4 if r['direction']==side];m['v4_core_'+side+'_count']=len(events)
        m['v4_core_'+side+'_quote_'+('input' if side=='buy' else 'output')]=str(total(events,'quote_core_amount_normalized'))
        m['v4_core_token_'+('bought' if side=='buy' else 'sold')]=str(total(events,'token_core_amount_normalized'))
    for unit,asset in [('quote',target['quote_asset_address']),('token',target['token_address'])]:
        matching=[r for r in hooks if r['currency']==asset.lower()]
        m['v4_hook_fee_'+unit]=str(total(matching,'base_fee_normalized')+total(matching,'creator_tax_normalized'))
    m['total_directional_event_count']=len(curve)+len(v4)
    for side in ['buy','sell']:m['total_'+side+'_direction_event_count']=sum(r['direction']==side for r in curve+v4)
    for label,column,events in [('buy_recipient','recipient_address',buy),('curve_caller','caller_address',curve),('swap_sender','swap_sender',v4)]:
        counts=[];event_counts=[]
        for addr in {r[column] for r in events}:
            sql=f'SELECT count(DISTINCT launch_id),count(*) FROM flow_events WHERE {column}=? AND event_time<? AND launch_id!=? AND removed=0'
            if label=='buy_recipient':sql+=" AND phase='curve' AND direction='buy'"
            if label=='curve_caller':sql+=" AND phase='curve'"
            if label=='swap_sender':sql+=" AND phase='v4'"
            count,num=db.conn.execute(sql,(addr,cutoff,target['launch_id'])).fetchone();counts.append(count);event_counts.append(num)
        m[label+'_prior_tracked_tokens_mean']=str(sum(map(Decimal,counts))/len(counts)) if counts else None
        m[label+'_prior_tracked_tokens_median']=quantile(counts,.5)
        m[label+'_prior_events_mean']=str(sum(map(Decimal,event_counts))/len(event_counts)) if event_counts else None
        m[label+'_with_prior_activity_count']=sum(n>0 for n in counts)
    return m
