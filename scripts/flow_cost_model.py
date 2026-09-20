"""Read-only production cohorts and conservative locally enforced cost ceilings."""
import _bootstrap  # noqa: F401
from collections import Counter
import json
import shutil
import time
from app.config import Config
from app.flow_data import iso, stamp
from app.flow_worker import FlowSettings
from app.flow_reports import readonly


def model(conn,settings,now=None):
    now=time.time() if now is None else now;since=iso(now-86400)
    rows=conn.execute('''SELECT l.*,max(o.sampling_group='random_initial') initial,max(o.sampling_group='random_long') long
      FROM launches l JOIN outcome_targets o ON o.launch_id=l.id WHERE l.is_stock_quote=1 AND l.block_timestamp>=?
      GROUP BY l.id''',(since,)).fetchall()
    stock=conn.execute('SELECT count(*) FROM launches WHERE is_stock_quote=1 AND block_timestamp>=?',(since,)).fetchone()[0]
    sampled=[dict(r) for r in rows if r['initial']];points=[];v4_seconds=0
    for r in sampled:
        start=stamp(r['block_timestamp']);end=start+(3600 if r['long'] else 900);points.extend([(start,1),(end,-1)])
        g=conn.execute('SELECT block_timestamp FROM graduations WHERE token_address=? ORDER BY block_number LIMIT 1',(r['token_address'],)).fetchone()
        if g:v4_seconds+=max(0,end-max(start,stamp(g[0])))
    active=peak=0
    for _,change in sorted(points):active+=change;peak=max(peak,active)
    total_seconds=sum(3600 if r['long'] else 900 for r in sampled)
    metrics=dict(conn.execute('SELECT metric,sum(count) FROM rpc_usage WHERE minute>=? GROUP BY metric',(int(now-86400)//60*60,)))
    # Alchemy published method CU weights; treat unknown methods as 100 CU.
    weights={'eth_call':26,'eth_getLogs':60,'eth_getBlockByNumber':20,'eth_blockNumber':10,'eth_chainId':0}
    http_cu=sum(n*weights.get(k[7:],100) for k,n in metrics.items() if k.startswith('method:'))
    ws_bytes=metrics.get('ws_bytes',0)
    if not ws_bytes:
        ws_bytes=sum(n for k,n in metrics.items() if k.endswith('ws_bytes'))
    base_month=(http_cu+ws_bytes*.04)*30
    # HTTP max charges every allowed member at most expensive allowed method.
    flow_month=(settings.daily_ws_bytes*.04+settings.daily_calls*60)*30
    free=shutil.disk_usage(settings.database.parent).free
    # Capacity bound before measurement: <=8MB WS/day, >=500-byte envelopes,
    # conservative 4KB indexed stored rows, plus 5MB/day targets/features/telemetry.
    rows_day=settings.daily_ws_bytes/500
    disk_day=rows_day*4096+5_000_000
    return {'as_of':iso(now),'production_window_hours':24,'stock_launches':stock,'initial_sampled':len(sampled),
      'long_sampled':sum(r['long'] for r in sampled),'average_active_inferred':total_seconds/86400,
      'average_curve_active_inferred':(total_seconds-v4_seconds)/86400,'average_v4_active_inferred':v4_seconds/86400,
      'historical_peak_active':peak,'expected_average_subscriptions_inferred':(total_seconds+v4_seconds)/86400,
      'peak_subscription_capacity_inferred':peak*2,'configured_subscription_cap':settings.max_subscriptions,
      'http_scenarios':{'new_target_typical_members':5,'new_target_max_attempts_bounded_by_daily_budget':settings.daily_calls,
                        'reconnect_max_blocks_per_filter':settings.recovery_blocks,'reconnect_getlogs_per_filter_without_rejection':(settings.recovery_blocks+9)//10,
                        'recovery_headers':'unique missing-timestamp blocks only; included in HTTP cap','tx_enrichment':0},
      'event_rate_before_benchmark':'NOT YET MEASURED; curve/V4/hook activity unknown',
      'ws_capacity_bytes_day':settings.daily_ws_bytes,'raw_rows_capacity_scenario_day':rows_day,
      'inferred_db_bytes_day':disk_day,'inferred_db_bytes_30days':disk_day*30,'free_disk_bytes':free,
      'base_ws_bytes_24h':ws_bytes,'base_estimated_cu_month':base_month,'flow_max_estimated_cu_month':flow_month,
      'combined_estimated_cu_month':base_month+flow_month,'free_plan_cu_month':30_000_000,
      'safe_to_enable':bool(ws_bytes and base_month+flow_month<30_000_000 and free>disk_day*90+2_000_000_000 and peak*2<=settings.max_subscriptions),
      'classification':'INFERRED, local telemetry projections; not provider billing or quota remaining',
      'cost_source':'https://www.alchemy.com/docs/reference/compute-unit-costs',
      'uncertainty':'Short-term production volume can change; WS message in flight can slightly overshoot cap; flow stops first at its persisted budget.'}


if __name__=='__main__':
    print(json.dumps(model(readonly(Config.load().database),FlowSettings.load()),indent=2))
