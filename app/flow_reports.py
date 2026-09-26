"""Local-only flow inspection, usage and descriptive outcome joins."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation, localcontext
import json
import sqlite3
import time

from app.flow_data import WINDOWS, iso, stamp, quantile


def readonly(path):
    conn=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
    conn.row_factory=sqlite3.Row
    return conn


def unpack(row,field):
    item=dict(row);item[field]=json.loads(item[field]);return item


def inspect(db,token):
    t=db.conn.execute('SELECT * FROM flow_tracking_targets WHERE lower(token_address)=lower(?)',(token,)).fetchone()
    if not t:return {'token':token,'status':'not_tracked'}
    t=dict(t);launch=t['launch_id']
    t['graduation']=json.loads(t.pop('graduation_json') or 'null')
    events=[]
    for row in db.conn.execute('SELECT * FROM flow_events WHERE launch_id=? ORDER BY block_number,log_index',(launch,)):
        item=unpack(row,'payload');item['age_seconds']=item['event_time']-t['tracking_start_at'];events.append(item)
    features=[]
    for row in db.conn.execute('SELECT * FROM flow_features WHERE launch_id=? ORDER BY window_seconds',(launch,)):
        item=unpack(row,'metrics');item['expected_window_start']=t['tracking_start_at'];item['expected_window_end']=item['feature_cutoff_at'];features.append(item)
    return {'launch':t,'gaps':[dict(r) for r in db.conn.execute('SELECT * FROM flow_gaps WHERE launch_id=?',(launch,))],
            'events':events,'features':features,'identity_policy':'explicit roles; economic actor unknown',
            'concentration_semantics':'early acquisition flow concentration; not holder concentration'}


def usage(db,settings,hours=24,now=None,providers=None):
    if hours<=0:raise ValueError('Hours must be positive')
    now=time.time() if now is None else now;since=now-hours*3600
    c=db.conn
    counters=dict(c.execute('SELECT metric,sum(count) FROM flow_usage WHERE minute>=? GROUP BY metric',(int(since)//60*60,)))
    for metric in ('flow_http_calls','flow_eth_getLogs','flow_eth_getBlockByNumber','flow_eth_call','flow_eth_getTransactionByHash',
                   'flow_eth_getTransactionReceipt','flow_ws_bytes','flow_duplicate_events','flow_removed_events','flow_subscription_reconnects',
                   'flow_curve_buy_events','flow_curve_sell_events','flow_v4_swap_events','flow_hook_fee_events'):
        counters.setdefault(metric,0)
    for name in ('publicnode','validation','alchemy'):
        for stem in ('flow_http_calls_','flow_eth_getLogs_','flow_ws_bytes_','flow_wss_connections_',
                     'flow_provider_connection_errors_','flow_provider_reconnects_'):
            counters.setdefault(stem+name,0)
    for stem in ('flow_provider_failovers','flow_provider_failbacks'):
        counters.setdefault(stem,0)
    targets=c.execute('SELECT count(*),coalesce(sum(cohort_initial),0),coalesce(sum(cohort_long),0) FROM flow_tracking_targets WHERE created_at>=?',(since,)).fetchone()
    status=dict(c.execute('SELECT status,count(*) FROM flow_tracking_targets GROUP BY status'))
    events=dict(c.execute('SELECT phase,count(*) FROM flow_events WHERE observed_at>=? AND removed=0 GROUP BY phase',(since,)))
    coverage=dict(c.execute('SELECT coverage_quality,count(*) FROM flow_features WHERE finalized_at>=? GROUP BY coverage_quality',(since,)))
    windows=[dict(r) for r in c.execute('SELECT window_seconds,coverage_quality,count(*) n FROM flow_features GROUP BY window_seconds,coverage_quality')]
    samples=c.execute('SELECT avg(active),max(active),avg(subscriptions),max(subscriptions),max(curve_subscriptions),max(v4_subscriptions),max(hook_subscriptions) FROM flow_samples WHERE at>=?',(since,)).fetchone()
    first=c.execute('SELECT at,db_bytes FROM flow_samples WHERE at>=? ORDER BY at LIMIT 1',(since,)).fetchone()
    last=c.execute('SELECT at,db_bytes FROM flow_samples WHERE at>=? ORDER BY at DESC LIMIT 1',(since,)).fetchone()
    growth={'sample_seconds':last[0]-first[0],'bytes':max(0,last[1]-first[1])} if first and last else None
    if growth:
        rate=growth['bytes']/growth['sample_seconds'] if growth['sample_seconds'] else None
        growth['inferred_bytes_per_day']=rate*86400 if rate is not None else None
        growth['inferred_projection_bytes']={str(d):rate*86400*d if rate is not None else None for d in (7,30,90)}
    day=int(now)//86400*86400
    budgets={k:{'used':db.used(m,day),'limit':v,'pct':round(100*db.used(m,day)/v,2)} for k,m,v in
             [('rpc_members','flow_rpc_members',settings.daily_calls),('getLogs','flow_eth_getLogs',settings.daily_getlogs)]}
    secondary_bytes=sum(db.used('flow_ws_bytes_'+name,day) for name in ('publicnode','validation'))
    budgets['ws_bytes']={'used':secondary_bytes,'limit':settings.daily_ws_bytes,
                         'pct':round(100*secondary_bytes/settings.daily_ws_bytes,2)}
    oldest=c.execute("SELECT min(tracking_start_at) FROM flow_tracking_targets WHERE status IN ('scheduled','active_curve','active_v4')").fetchone()[0]
    counters.update(flow_targets_active=sum(status.get(k,0) for k in ('active_curve','active_v4')),
                    flow_targets_partial=status.get('partial',0),flow_complete_windows=coverage.get('complete',0),
                    flow_partial_windows=coverage.get('partial',0),flow_unavailable_windows=coverage.get('unavailable',0),
                    flow_recovery_gaps=c.execute('SELECT count(*) FROM flow_gaps WHERE resolved=0').fetchone()[0],
                    flow_raw_rows=c.execute('SELECT count(*) FROM flow_events').fetchone()[0],
                    flow_db_bytes=sum(p.stat().st_size for p in (db.path,db.path.with_name(db.path.name+'-wal')) if p.exists()))
    from app.flow_cutover import gap_counts, session as cutover_session
    active_gaps,historical_gaps=gap_counts(db)
    cutover=cutover_session(db)
    return {'hours':hours,'as_of':iso(now),'tracked_launches':targets[0],'initial_cohort_tracked':targets[1],'long_cohort_tracked':targets[2],
            'target_status_all_time':status,'events_in_period':events,'raw_rows_all_time':c.execute('SELECT count(*) FROM flow_events').fetchone()[0],
            'coverage':coverage,'windows':windows,'active_average':samples[0],'active_peak':samples[1],'subscriptions_average':samples[2],
            'subscriptions_peak':samples[3],'curve_subscriptions_peak':samples[4],'v4_subscriptions_peak':samples[5],'hook_subscriptions_peak':samples[6],
            'metrics':counters,'budget_utilization_utc_day':budgets,'db_growth':growth,'oldest_active_tracking_start':oldest,
            'phase2b_coverage_start_at':db.state('phase2b_coverage_start_at'),'service_status':db.state('service_status'),
            'connection_state':db.state('connection_state'),'recovery_state':db.state('recovery_state'),
            'cutover_state':cutover['state'].lower() if cutover else None,
            'active_unresolved_gap_count':active_gaps,'historical_unresolved_gap_count':historical_gaps,
            'routing':{'current_wss_provider':db.state('current_wss_provider'),
                       'wss_primary_provider':'publicnode' if providers else None,
                       'wss_fallback_provider':'validation' if providers else None,
                       'http_provider':'validation' if providers else None,
                       'fingerprints':providers.fingerprints() if providers else None,
                       'failovers':counters['flow_provider_failovers'],'failbacks':counters['flow_provider_failbacks'],
                       'connection_errors':{name:counters['flow_provider_connection_errors_'+name] for name in ('publicnode','validation','alchemy')},
                       'reconnects':{name:counters['flow_provider_reconnects_'+name] for name in ('publicnode','validation','alchemy')},
                       'http_calls':{name:counters['flow_http_calls_'+name] for name in ('publicnode','validation','alchemy')},
                       'getLogs_calls':{name:counters['flow_eth_getLogs_'+name] for name in ('publicnode','validation','alchemy')},
                       'wss_connections':{name:counters['flow_wss_connections_'+name] for name in ('publicnode','validation','alchemy')},
                       'wss_bytes':{name:counters['flow_ws_bytes_'+name] for name in ('publicnode','validation','alchemy')},
                       'secondary_ws_daily_cap':settings.daily_ws_bytes,
                       'secondary_ws_daily_utilization':budgets['ws_bytes'],
                       'flow_alchemy_http_requests':counters['flow_http_calls_alchemy'],
                       'flow_alchemy_wss_connections':counters['flow_wss_connections_alchemy'],
                       'flow_alchemy_wss_bytes':counters['flow_ws_bytes_alchemy'],
                       'budget_pause_reason':db.state('budget_pause_reason'),
                       'budget_pause_provider':db.state('budget_pause_provider')},
            'billing':'local counters only; no provider billing or remaining-quota data',
            'sample_note':'30-second gauges; projections include allocated DB/WAL overhead, not raw bytes/event'}


def price(row,now,quote):
    if not row or row['data_quality']!='verified' or row['quote_asset_address'].lower()!=quote.lower() or stamp(row['observed_at'])>now:return None
    try:
        value=Decimal(row['price_quote'])
        return value if value.is_finite() and value>0 else None
    except (InvalidOperation,TypeError):return None


def outcome(db,main,days=7,window=300,horizon=3600,ticker=None,quote_address=None,now=None):
    if window not in WINDOWS or horizon not in (300,900,3600,21600,86400) or window>=horizon:
        raise ValueError('Feature window must be supported and strictly less than outcome horizon')
    if days<=0:raise ValueError('Days must be positive')
    now=time.time() if now is None else now
    where=['l.is_stock_quote=1','l.block_timestamp>=?','l.block_timestamp<=?'];args=[iso(now-days*86400),iso(now)]
    if ticker:where+=['sa.verified=1','upper(sa.stock_ticker)=upper(?)'];args.append(ticker)
    if quote_address:where+=['lower(l.quote_asset_address)=lower(?)'];args.append(quote_address)
    launches=main.execute('SELECT l.*,sa.stock_ticker FROM launches l LEFT JOIN stock_assets sa ON sa.address=l.quote_asset_address WHERE '+' AND '.join(where),args).fetchall()
    categories=('unsampled','feature_not_deployed_yet','feature_window_not_due','feature_partial','feature_missing',
                'outcome_unsampled','outcome_not_due','outcome_missing','invalid_market_price','valid_pair')
    missing=Counter({k:0 for k in categories});pairs=defaultdict(list)
    deployment=float(db.state('phase2b_coverage_start_at',now))
    for l in launches:
        launch=l['id'];quote=l['quote_asset_address']
        targets={r['target_age_seconds']:r for r in main.execute('SELECT * FROM outcome_targets WHERE launch_id=?',(launch,))}
        if not any(r['sampling_group']=='random_initial' for r in targets.values()):missing['unsampled']+=1;continue
        target=db.target(launch)
        start=target['tracking_start_at'] if target else stamp(l['block_timestamp'])
        if start<deployment:missing['feature_not_deployed_yet']+=1;continue
        if start+window>now:missing['feature_window_not_due']+=1;continue
        f=db.conn.execute('SELECT * FROM flow_features WHERE launch_id=? AND window_seconds=?',(launch,window)).fetchone()
        if not f:missing['feature_missing']+=1;continue
        if f['coverage_quality']!='complete':missing['feature_partial']+=1;continue
        o=targets.get(horizon)
        if not o or o['sampling_group']=='not_sampled':missing['outcome_unsampled']+=1;continue
        if stamp(o['due_at'])>now:missing['outcome_not_due']+=1;continue
        snaps={r['target_age_seconds']:r for r in main.execute('SELECT * FROM market_snapshots WHERE launch_id=? AND target_age_seconds IN (0,?)',(launch,horizon))}
        if 0 not in snaps or horizon not in snaps:missing['outcome_missing']+=1;continue
        a,b=price(snaps[0],now,quote),price(snaps[horizon],now,quote)
        if a is None or b is None:missing['invalid_market_price']+=1;continue
        # Scheduled horizon and actual measurement must both be after the feature.
        if stamp(snaps[horizon]['observed_at'])<=f['feature_cutoff_at']:
            missing['invalid_market_price']+=1;continue
        with localcontext() as ctx:
            ctx.prec=90;multiple=b/a
        pairs[quote].append((multiple,json.loads(f['metrics'])))
        missing['valid_pair']+=1
    groups={}
    predicates={'all_valid_pairs':lambda x:True,'ge_1_25x':lambda x:x>=Decimal('1.25'),
                'ge_1_5x':lambda x:x>=Decimal('1.5'),'ge_2x':lambda x:x>=2,'lt_2x':lambda x:x<2,
                'le_0_75x':lambda x:x<=Decimal('.75'),'le_0_5x':lambda x:x<=Decimal('.5')}
    for quote,rows in pairs.items():
        groups[quote]={}
        for label,pred in predicates.items():
            subset=[r for r in rows if pred(r[0])];distribution={}
            for key in sorted({k for _,m in subset for k in m}):
                values=[str(int(m[key])) if isinstance(m[key],bool) else m[key] for _,m in subset if m.get(key) is not None]
                distribution[key]={'N':len(values),**{name:quantile(values,p) for name,p in [('p25',.25),('median',.5),('p75',.75),('p90',.9)]}}
            groups[quote][label]={'N':len(subset),'features':distribution}
    return {'days':days,'as_of':iso(now),'feature_window_seconds':window,'outcome_horizon_seconds':horizon,
            'outcome_metric':'marginal_price_multiple','price_semantics':'pre-fee marginal market price; not trader ROI',
            'feature_coverage_filter':'complete','denominator_all_stock_launches':len(launches),
            'missingness_exclusive_first_reason':dict(missing),'groups_by_quote_address':groups,
            'warnings':['descriptive only','sampled local participant history only','quote-native amounts are never pooled across assets']+
                       (['small_sample'] if missing['valid_pair']<30 else [])}
