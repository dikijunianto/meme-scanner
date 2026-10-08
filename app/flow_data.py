"""Isolated flow storage, event semantics and deterministic local features."""
import json
import hashlib
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
FEATURE_SCHEMA_VERSION = 'v1'
PIT_METRICS = ('curve_buy_count', 'curve_sell_count', 'total_directional_event_count',
               'v4_core_swap_count', 'unique_buy_recipients', 'top1_buy_recipient_token_share',
               'curve_tokens_bought')
BUY = '0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455'
SELL = '0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df'
SWAP = '0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f'
HOOK = '0x' + keccak(text='HookFeeCollected(bytes32,address,uint256,uint256)').hex()


def stamp(value):
    return datetime.fromisoformat(value).timestamp() if isinstance(value, str) else value


def iso(value):
    return (datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(seconds=value)).isoformat()


def required_filter_intervals(target, cutoff):
    """The curve ends at the graduation log; pool filters begin after it."""
    start=target['tracking_start_at']
    graduation=json.loads(target['graduation_json']) if target['graduation_json'] else None
    if not graduation or stamp(graduation['block_timestamp'])>cutoff:
        return [{'kind':'curve','start_at':start,'end_at':cutoff,
                 'safe_start_block':target['launch_block'],'address':target['curve_address'].lower()}]
    at=stamp(graduation['block_timestamp'])
    boundary=[graduation['block_number'],graduation['log_index']]
    return [
        {'kind':'curve','start_at':start,'end_at':at,
         'safe_start_block':target['launch_block'],'address':target['curve_address'].lower(),
         'end_before_position':boundary},
        *({'kind':kind,'start_at':at,'end_at':cutoff,
           'safe_start_block':graduation['block_number'],'address':graduation[address].lower(),
           'pool_id':graduation['pool_id'].lower(),'start_after_position':boundary}
          for kind,address in (('v4','pool_manager_address'),('hook','hooks')))]


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
    def __init__(self,path,readonly=False,follow_epoch=True):
        self.path=Path(path)
        self.conn=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro' if readonly else str(path),uri=readonly,timeout=2)
        self.conn.row_factory=sqlite3.Row
        self.conn.execute('PRAGMA busy_timeout=2000')
        from app.flow_epochs import active_path
        self.catalog_conn=self.conn
        if follow_epoch:
            selected=active_path(self.conn,self.path)
            if selected.resolve()!=self.path.resolve():
                if not selected.is_file():raise ValueError('Active epoch database is missing; never recreate it implicitly')
                self.path=selected
                self.conn=sqlite3.connect(selected.resolve().as_uri()+'?mode=ro' if readonly else str(selected),uri=readonly,timeout=2)
                self.conn.row_factory=sqlite3.Row
        # An explicitly opened epoch partition still shares the original budget
        # ledger. Read-only opening never initializes or migrates a catalog.
        if self.catalog_conn is self.conn and self.conn.execute("SELECT 1 FROM sqlite_master WHERE name='flow_state'").fetchone():
            catalog=self.state('epoch_catalog_path')
            if catalog:
                self.catalog_conn=sqlite3.connect(Path(catalog).as_uri()+'?mode=ro' if readonly else catalog,uri=readonly,timeout=2)
                self.catalog_conn.row_factory=sqlite3.Row
        self.budget_conn=self.catalog_conn

    def epoch(self):
        from app.flow_epochs import row
        return row(self.catalog_conn,self.path)

    def collection_context(self):
        from app.flow_segments import context
        return context(self)

    def close(self):
        self.conn.close()
        if self.catalog_conn is not self.conn:self.catalog_conn.close()

    def migrate(self, *, shared_budget=False):
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
        CREATE TABLE IF NOT EXISTS flow_samples(at REAL PRIMARY KEY,active INTEGER,subscriptions INTEGER,
          curve_subscriptions INTEGER,v4_subscriptions INTEGER,hook_subscriptions INTEGER,db_bytes INTEGER);
        CREATE TABLE IF NOT EXISTS flow_bootstrap(
          launch_id INTEGER NOT NULL,kind TEXT NOT NULL,safe_start INTEGER NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('required','in_progress','complete')),
          completed_head INTEGER,created_at REAL NOT NULL,completed_at REAL,
          PRIMARY KEY(launch_id,kind));
        CREATE TABLE IF NOT EXISTS flow_bootstrap_identity(
          stage TEXT NOT NULL,launch_id INTEGER NOT NULL,kind TEXT NOT NULL,
          query_json TEXT NOT NULL,upper_at REAL NOT NULL,
          PRIMARY KEY(stage,launch_id,kind));
        CREATE TABLE IF NOT EXISTS flow_feature_ledger_start(
          id INTEGER PRIMARY KEY CHECK(id=1),start_at REAL NOT NULL,start_block INTEGER,
          deploy_revision TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS flow_feature_versions(
          launch_id INTEGER NOT NULL,window_seconds INTEGER NOT NULL,version_number INTEGER NOT NULL,
          feature_schema_version TEXT NOT NULL,feature_cutoff_at REAL NOT NULL,
          materialized_at REAL NOT NULL,completeness_proved_at REAL,model_eligible_at REAL,
          coverage_quality TEXT NOT NULL,coverage_reason TEXT NOT NULL,
          payload TEXT NOT NULL,payload_sha256 TEXT NOT NULL,semantic_sha256 TEXT NOT NULL,
          write_reason TEXT NOT NULL,source_revision TEXT NOT NULL,proof_json TEXT NOT NULL,
          created_at REAL NOT NULL,
          PRIMARY KEY(launch_id,window_seconds,version_number));
        CREATE INDEX IF NOT EXISTS flow_feature_versions_eligible
          ON flow_feature_versions(model_eligible_at,feature_schema_version)
          WHERE model_eligible_at IS NOT NULL;
        CREATE TRIGGER IF NOT EXISTS flow_feature_versions_no_update BEFORE UPDATE ON flow_feature_versions
          BEGIN SELECT RAISE(ABORT,'feature versions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS flow_feature_versions_no_delete BEFORE DELETE ON flow_feature_versions
          BEGIN SELECT RAISE(ABORT,'feature versions are immutable'); END;
        ''')
        self.conn.commit()
        from app.flow_cutover import schema as cutover_schema
        cutover_schema(self)
        from app.flow_partition_schema import create_shadow,initialize_metadata
        with self.conn:
            create_shadow(self.conn)
            initialize_metadata(self.conn)
            if not shared_budget:
                self.conn.execute('''CREATE TABLE IF NOT EXISTS flow_usage(
                  minute INTEGER NOT NULL,metric TEXT NOT NULL,count INTEGER NOT NULL,PRIMARY KEY(minute,metric))''')

    def activate_pit_ledger(self,revision,block=None,now=None):
        """Start prospective collection once, after the new worker is connected."""
        if len(revision)!=40 or any(c not in '0123456789abcdef' for c in revision):
            raise ValueError('Full source revision required')
        at=time.time() if now is None else now
        with self.conn:
            self.conn.execute('INSERT OR IGNORE INTO flow_feature_ledger_start VALUES(1,?,?,?)',
                              (at,block,revision))

    def _append_feature_version(self,target,row,reason):
        epoch=self.collection_context()
        if epoch and (epoch['status'] not in ('ACTIVATING','ACTIVE') or target['tracking_start_at']<epoch['start_block_timestamp'] or
                      target['launch_block']<epoch['start_block']):
            return
        start=self.conn.execute('SELECT * FROM flow_feature_ledger_start WHERE id=1').fetchone()
        if not start or target['tracking_start_at']<start['start_at']:
            return
        launch,window=target['launch_id'],row['window_seconds']
        metrics=json.loads(row['metrics'])
        payload=json.dumps({k:metrics.get(k) for k in PIT_METRICS},sort_keys=True,separators=(',',':'))
        payload_hash=hashlib.sha256(payload.encode()).hexdigest()
        intervals=required_filter_intervals(target,row['feature_cutoff_at'])
        filter_hash=hashlib.sha256(json.dumps(intervals,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        proof=[]
        for interval in intervals:
            kind=interval['kind']
            bootstrap=self.conn.execute('SELECT status,completed_head,completed_at FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                        (launch,kind)).fetchone()
            cursor=self.state(f'recovery:{launch}:{kind}')
            proof.append({**interval,'bootstrap_status':bootstrap['status'] if bootstrap else None,
                          'completed_head':bootstrap['completed_head'] if bootstrap else None,
                          'completed_at':bootstrap['completed_at'] if bootstrap else None,
                          'cursor':int(cursor) if cursor is not None else None})
        ready=(row['coverage_quality']=='complete' and
               all(p['bootstrap_status']=='complete' and p['cursor'] is not None for p in proof))
        activation=None
        if epoch and epoch.get('research_segment_id'):
            from app.flow_activation import evidence as activation_evidence
            activation=activation_evidence(self)
            first=self.conn.execute('SELECT materialized_at FROM flow_feature_versions WHERE launch_id=? AND window_seconds=? ORDER BY version_number LIMIT 1',(launch,window)).fetchone()
            ready=(ready and activation['complete'] and row['feature_cutoff_at']>=activation.get('activated_at',float('inf'))
                   and (not first or first[0]>=activation['activated_at']))
        if epoch and epoch.get('research_segment_id'):ready=ready and self.current_health()=='healthy'
        if epoch:
            if self.state(f'expired_forensic_target:{launch}'):
                ready=False  # Forensic completeness is never retrospective PIT.
            from app.flow_provider_switch import pending,blocked
            seal=json.loads(epoch['boundary_json']).get('live_seal')
            if activation and activation['complete']:seal={'proved_at':activation['activated_at']}
            ready=(ready and epoch['status']=='ACTIVE' and bool(epoch['pit_eligible']) and bool(seal) and not pending(self) and not blocked(self)
                   and not self.conn.execute('SELECT 1 FROM flow_gaps WHERE resolved=0 LIMIT 1').fetchone())
            # Descriptive repair after a missed recovery cutoff is not fresh PIT.
            for gap in self.conn.execute("SELECT id,start_at FROM flow_gaps WHERE launch_id=? AND reason IN ('ws_gap','reconnect_recovery_incomplete')",
                                         (launch,)):
                recovery=json.loads(self.state(f'gap_recovery:{gap["id"]}','{}'))
                if gap['start_at']<=row['feature_cutoff_at']<recovery.get('completed_at',gap['start_at']):
                    ready=False
            missed=self.conn.execute('SELECT coverage_reason,model_eligible_at FROM flow_feature_versions '
                'WHERE launch_id=? AND window_seconds=? ORDER BY version_number LIMIT 1',(launch,window)).fetchone()
            if missed and missed['model_eligible_at'] is None and missed['coverage_reason'] in ('ws_gap','reconnect_recovery_incomplete'):
                ready=False
        incidents=[];fresh_incidents=[]
        for incident_row in self.conn.execute("SELECT key,value FROM flow_state WHERE key LIKE 'pit_collection_incident:%'"):
            incident=json.loads(incident_row['value'])
            boundary=incident.get('runtime_healthy_at') or incident['PIT_COLLECTION_RECOVERY_END']
            if (row['feature_cutoff_at']>=incident['PIT_COLLECTION_REGRESSION_START'] and
                (boundary is None or row['coverage_start_at'] is None or row['coverage_start_at']<boundary)):
                incidents.append(incident['switch_id'])
            elif boundary is not None and incident['PIT_COLLECTION_RECOVERY_END'] is None and row['feature_cutoff_at']>=boundary:
                fresh_incidents.append((incident_row['key'],incident))
        if incidents:ready=False
        semantic=json.dumps((FEATURE_SCHEMA_VERSION,payload_hash,row['coverage_quality'],
                             row['coverage_reason'],ready,filter_hash),separators=(',',':'))
        semantic_hash=hashlib.sha256(semantic.encode()).hexdigest()
        prior=self.conn.execute('''SELECT version_number,semantic_sha256,payload_sha256,coverage_quality,
                                        coverage_reason,model_eligible_at
                                 FROM flow_feature_versions
                                 WHERE launch_id=? AND window_seconds=? ORDER BY version_number DESC LIMIT 1''',
                                (launch,window)).fetchone()
        if prior and prior['semantic_sha256']==semantic_hash:
            return
        if reason=='rebuild':
            reason=('first_materialization' if not prior else
                    'payload_changed_rebuild' if prior['payload_sha256']!=payload_hash else
                    'coverage_changed_rebuild' if prior['coverage_quality']!=row['coverage_quality'] else
                    'coverage_reason_changed_rebuild' if prior['coverage_reason']!=row['coverage_reason'] else
                    'proof_state_changed_rebuild' if (prior['model_eligible_at'] is None)!=ready else
                    'rebuild')
        at=time.time()
        proved=at if ready else None
        if ready and epoch:
            proved=max(at,seal['proved_at'],*(p['completed_at'] for p in proof))
            if activation:proved=max(proved,activation['activated_at'])
        evidence={'window_end_at':row['feature_cutoff_at'],'coverage_start_at':row['coverage_start_at'],
                  'coverage_end_at':row['coverage_end_at'],'filters':proof,
                  'lifecycle_state_at_cutoff':'graduated' if len(intervals)>1 else 'curve',
                  'required_filter_set_hash':filter_hash,
                  'proof_source':'existing_bootstrap_and_recovery_cursors' if ready else 'incomplete_or_unknown'}
        if epoch:evidence['collection_epoch_id']=epoch['epoch_id']
        if epoch and epoch.get('research_segment_id'):evidence['research_segment_id']=epoch['research_segment_id']
        if activation and activation['complete']:evidence['activation_evidence_id']=activation['id']
        if incidents:evidence['incident_reconstruction_not_pit_safe']=incidents
        self.conn.execute('''INSERT INTO flow_feature_versions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                          (launch,window,1+(prior['version_number'] if prior else 0),FEATURE_SCHEMA_VERSION,
                           row['feature_cutoff_at'],at,proved,max(row['feature_cutoff_at'],at,proved) if ready else None,
                           row['coverage_quality'],row['coverage_reason'],payload,payload_hash,semantic_hash,
                           reason,start['deploy_revision'],json.dumps(evidence,sort_keys=True,separators=(',',':')),at))
        if ready:
            for key,incident in fresh_incidents:
                incident['PIT_COLLECTION_RECOVERY_END']=at
                incident['first_fresh_pit']={'launch_id':launch,'window_seconds':window,
                    'feature_cutoff_at':row['feature_cutoff_at'],'materialized_at':at,
                    'model_eligible_at':max(row['feature_cutoff_at'],at),'payload_sha256':payload_hash}
                self.conn.execute('UPDATE flow_state SET value=? WHERE key=?',(json.dumps(incident),key))

    def needs_bootstrap(self,launch,kind):
        """A required filter without an established cursor needs explicit proof."""
        return self.state(f'recovery:{launch}:{kind}') is None

    def require_bootstrap(self,target,kind,safe_start):
        """Make unknown completeness durable before a target can claim coverage."""
        launch=target['launch_id'];key=f'recovery:{launch}:{kind}'
        if not self.needs_bootstrap(launch,kind):return False
        row=self.conn.execute('SELECT safe_start,status FROM flow_bootstrap WHERE launch_id=? AND kind=?',(launch,kind)).fetchone()
        if row:
            if row['safe_start']!=safe_start or row['status']=='complete':
                raise ValueError('Bootstrap identity conflicts with recovery cursor')
            return True
        at=stamp(json.loads(target['graduation_json'])['block_timestamp']) if kind!='curve' and target['graduation_json'] else target['tracking_start_at']
        with self.conn:
            self.conn.execute('INSERT INTO flow_bootstrap VALUES(?,?,?,?,?,?,?)',
                              (launch,kind,safe_start,'required',None,time.time(),None))
            if not (self.collection_context() or {}).get('research_segment_id'):
                self.conn.execute('INSERT INTO flow_gaps(launch_id,start_at,end_at,reason,first_block) VALUES(?,?,?,?,?)',
                                  (launch,at,at,f'bootstrap_required:{kind}',safe_start))
            self.conn.execute("UPDATE flow_features SET coverage_quality='partial',coverage_reason=? WHERE launch_id=? AND feature_cutoff_at>=?",
                              (f'bootstrap_required:{kind}',launch,at))
            for feature in self.conn.execute('SELECT * FROM flow_features WHERE launch_id=? AND feature_cutoff_at>=?',(launch,at)):
                self._append_feature_version(target,feature,f'bootstrap_required:{kind}')
        return True

    def complete_bootstrap(self,launch,kind,head):
        """Called only after full contiguous HTTP proof has committed."""
        with self.conn:
            row=self.conn.execute('SELECT safe_start,status,completed_head FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                  (launch,kind)).fetchone()
            if not row:raise ValueError('Bootstrap state missing')
            if row['status']=='complete':
                if row['completed_head']>head:return
            else:
                self.conn.execute("UPDATE flow_bootstrap SET status='complete',completed_head=?,completed_at=? WHERE launch_id=? AND kind=?",
                                  (head,time.time(),launch,kind))
            self.conn.execute('''INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key)
              DO UPDATE SET value=max(cast(value AS INTEGER),cast(excluded.value AS INTEGER))''',
                              (f'recovery:{launch}:{kind}',str(head)))
            self.conn.execute("UPDATE flow_gaps SET resolved=1 WHERE launch_id=? AND reason=? AND first_block=?",
                              (launch,f'bootstrap_required:{kind}',row['safe_start']))

    def state(self,key,default=None):
        r=self.conn.execute('SELECT value FROM flow_state WHERE key=?',(key,)).fetchone()
        value=r[0] if r else default
        return self.current_health() if key=='recovery_state' and value=='healthy' else value

    def current_health(self):
        """Quarantine is a partition boundary, never a target-expiry exemption."""
        epoch=self.collection_context()
        if not epoch:return 'healthy'  # Preserve legacy reporting semantics.
        if epoch.get('research_segment_id'):
            from app.flow_partition_schema import missing
            if missing(self.conn):return 'SEGMENT_SCHEMA_INCOMPLETE'
            local=self.conn.execute("SELECT value FROM flow_state WHERE key='recovery_state'").fetchone()
            if local and local[0]=='LOCAL_DATABASE_ERROR':return 'LOCAL_DATABASE_ERROR'
            if self.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'unbounded_current_gap:%' LIMIT 1").fetchone():return 'UNBOUNDED_CURRENT_GAP'
            from app.flow_activation import evidence
            if not evidence(self)['complete']:return 'activation_proof_pending'
        if self.state('research_segment_request_failure'):return 'research_segment_blocked'
        if epoch.get('research_segment_id') and self.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'unbounded_current_gap:%' LIMIT 1").fetchone():return 'UNBOUNDED_CURRENT_GAP'
        from app.flow_epochs import sealed_proof_intact
        if (epoch['status']!='ACTIVE' or not epoch['pit_eligible'] or
            (not epoch.get('research_segment_id') and not sealed_proof_intact(self,json.loads(epoch['boundary_json']).get('live_seal')))):return 'bootstrap_required'
        if self.state('connection_state')!='connected':return 'disconnected'
        from app.flow_provider_switch import pending,blocked
        if blocked(self):return 'provider_switch_failed'
        if pending(self):return 'provider_switch_pending'
        if epoch.get('research_segment_id'):
            if self.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'unbounded_current_gap:%' LIMIT 1").fetchone():return 'UNBOUNDED_CURRENT_GAP'
            if self.conn.execute("SELECT 1 FROM flow_state WHERE key LIKE 'pending_uncertainty:%' LIMIT 1").fetchone():return 'recovering'
        if self.conn.execute('SELECT 1 FROM flow_gaps WHERE resolved=0 LIMIT 1').fetchone():
            from app.flow_gap_recovery import obligations
            debts=obligations(self)
            if any('UNBOUNDED_CURRENT_GAP' in (d.get('reason_detail') or '') for d in debts):return 'UNBOUNDED_CURRENT_GAP'
            if any(d['state']=='operator_blocked' for d in debts):return 'unrecoverable_gap'
            if any(d['state']=='budget_wait' for d in debts):return 'temporary_budget_wait'
            return 'recovering'
        if self.conn.execute("SELECT 1 FROM flow_bootstrap WHERE status!='complete' LIMIT 1").fetchone():
            return 'bootstrap_required'
        for target in self.conn.execute("SELECT * FROM flow_tracking_targets WHERE status NOT IN ('completed','partial')"):
            for interval in required_filter_intervals(target,target['tracking_end_at']):
                state=self.conn.execute('SELECT status FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                    (target['launch_id'],interval['kind'])).fetchone()
                if not state or state['status']!='complete' or self.needs_bootstrap(target['launch_id'],interval['kind']):
                    return 'bootstrap_required'
        if self.conn.execute("SELECT 1 FROM flow_shadow_jobs WHERE completion_status!='complete' LIMIT 1").fetchone():
            return 'recovering'
        return 'healthy'

    def set_state(self,key,value):
        if key=='recovery_state' and value=='healthy':
            value=self.current_health()
        with self.conn:self.conn.execute('INSERT INTO flow_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,str(value)))

    def count(self,metric,n=1,now=None):
        minute=int(now if now is not None else time.time())//60*60
        with self.budget_conn:self.budget_conn.execute('INSERT INTO flow_usage VALUES(?,?,?) ON CONFLICT(minute,metric) DO UPDATE SET count=count+excluded.count',(minute,metric,n))

    def used(self,metric,since):
        return self.budget_conn.execute('SELECT coalesce(sum(count),0) FROM flow_usage WHERE metric=? AND minute>=?',(metric,since)).fetchone()[0]

    def gap(self,launch_id,start,end,reason,first_block=None,*,through_block=None,provenance=None):
        with self.conn:
            context=self.collection_context();contract=None
            if context and context.get('research_segment_id'):
                from app.flow_gap_contracts import build,failure
                try:contract=build(self,launch_id,reason,first_block,through_block,provenance)
                except (ValueError,TypeError,KeyError) as exc:
                    failure(self,launch_id,start,end,reason,str(exc));return None
            gap=self.conn.execute('INSERT INTO flow_gaps(launch_id,start_at,end_at,reason,first_block) VALUES(?,?,?,?,?)',(launch_id,start,max(start,end),reason,first_block))
            if contract:self.conn.execute('INSERT INTO flow_state VALUES(?,?)',(f'gap_contract:{gap.lastrowid}',json.dumps(contract,sort_keys=True)))
            if self.epoch():
                self.conn.execute('INSERT INTO flow_state VALUES(?,?)',
                    (f'gap_recovery:{gap.lastrowid}',json.dumps({'state':'queued','attempts':0,'created_at':time.time()})))
            # Persist invalidation atomically; a crash must not leave stale complete rows.
            self.conn.execute("UPDATE flow_features SET coverage_quality='partial',coverage_reason=? WHERE launch_id=? AND feature_cutoff_at>=?",(reason,launch_id,start))
            target=self.target(launch_id)
            if target:
                for feature in self.conn.execute('SELECT * FROM flow_features WHERE launch_id=? AND feature_cutoff_at>=?',(launch_id,start)):
                    self._append_feature_version(target,feature,reason)
            return gap.lastrowid

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
            intervals=required_filter_intervals(target,end)
            states=(self.conn.execute('SELECT status FROM flow_bootstrap WHERE launch_id=? AND kind=?',
                                      (target['launch_id'],interval['kind'])).fetchone() for interval in intervals)
            if any((state is None and len(intervals)>1) or
                   (state is not None and state['status']!='complete') for state in states):
                reasons.append('bootstrap_required')
            if target['coverage_start_at'] is None or target['coverage_start_at']>target['tracking_start_at']:reasons.append('service_started_late')
            if target['coverage_end_at'] is None or target['coverage_end_at']<end:reasons.append('coverage_not_confirmed')
            quality='partial' if reasons else 'complete'
            if reasons and not rows:quality='unavailable'
            metrics=features(self,target,rows,end)
            if quality=='unavailable':metrics={k:None for k in metrics}
            with self.conn:
                self.conn.execute('''INSERT INTO flow_features VALUES(?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(launch_id,window_seconds) DO UPDATE SET finalized_at=excluded.finalized_at,
              coverage_start_at=excluded.coverage_start_at,coverage_end_at=excluded.coverage_end_at,
              coverage_quality=excluded.coverage_quality,coverage_reason=excluded.coverage_reason,metrics=excluded.metrics''',
              (target['launch_id'],window,target['token_address'],target['quote_asset_address'],end,now,
               target['coverage_start_at'],min(end,target['coverage_end_at']) if target['coverage_end_at'] is not None else None,
               quality,','.join(sorted(set(reasons))) or 'complete',json.dumps(metrics)))
                feature=self.conn.execute('SELECT * FROM flow_features WHERE launch_id=? AND window_seconds=?',
                                          (target['launch_id'],window)).fetchone()
                self._append_feature_version(target,feature,'rebuild')


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
