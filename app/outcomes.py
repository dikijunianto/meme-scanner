"""Read-only horizon outcome report."""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

def _d(row, qualities):
    try: return Decimal(row["price_quote"]) if row and row["data_quality"] in qualities and Decimal(row["price_quote"]) > 0 else None
    except (InvalidOperation, TypeError): return None
def _pct(n, d): return {"count":n,"denominator":d,"pct":round(100*n/d,2) if d else None}
def _q(v,p): return str(sorted(v)[round((len(v)-1)*p)]) if v else None
def _label(h): return {300:"5m",900:"15m",3600:"1h",21600:"6h",86400:"24h"}.get(h,f"{h}s")

def report(conn, days=7, hours=None, ticker=None, quote_address=None, min_completeness=0, quality="verified", now=None):
    now=now or datetime.now(timezone.utc); start=now-timedelta(hours=hours if hours is not None else days*24); qs=set(quality.split(","))
    where=["l.is_stock_quote=1","l.detected_at>=?","l.detected_at<=?"]; args=[start.isoformat(),now.isoformat()]
    if ticker: where += ["sa.verified=1","sa.stock_ticker=?"]; args += [ticker]
    if quote_address: where += ["l.quote_asset_address=?"]; args += [quote_address]
    launches=[dict(x) for x in conn.execute("SELECT l.* FROM launches l LEFT JOIN stock_assets sa ON sa.address=l.quote_asset_address WHERE "+" AND ".join(where),args)]
    ids=[x["id"] for x in launches]
    if not ids: return {"window":{"from":start.isoformat(),"to":now.isoformat(),"timezone":"UTC"},"cohort":{},"horizons":{},"warnings":["no_launches"]}
    marks=",".join("?"*len(ids)); targets=[dict(x) for x in conn.execute(f"SELECT * FROM outcome_targets WHERE launch_id IN ({marks})",ids)]; snaps=[dict(x) for x in conn.execute(f"SELECT * FROM market_snapshots WHERE launch_id IN ({marks})",ids)]
    ts={(x["launch_id"],x["target_age_seconds"]):x for x in targets}; ss={(x["launch_id"],x["target_age_seconds"]):x for x in snaps}
    initial={x["launch_id"] for x in targets if x["sampling_group"]=="random_initial"}; long={x["launch_id"] for x in targets if x["sampling_group"]=="random_long"}
    due=defaultdict(list)
    for x in targets:
      if x["sampling_group"]!="not_sampled" and datetime.fromisoformat(x["due_at"])<=now: due[x["launch_id"]].append(x)
    passing={i for i in initial if due[i] and sum((i,x["target_age_seconds"]) in ss for x in due[i])/len(due[i])>=min_completeness}
    horizons={}
    for h in sorted({x["target_age_seconds"] for x in targets if x["target_age_seconds"]}):
      scheduled=[x for x in targets if x["target_age_seconds"]==h]; mature=[x for x in scheduled if datetime.fromisoformat(x["due_at"])<=now]; observed=[x for x in mature if (x["launch_id"],h) in ss]; multiples=[]; phases=Counter(); excluded=Counter()
      for x in mature:
       a,b=_d(ss.get((x["launch_id"],0)),qs),_d(ss.get((x["launch_id"],h)),qs)
       if x["launch_id"] not in passing: excluded["min_completeness"]+=1
       elif not a: excluded["missing_or_invalid_baseline"]+=1
       elif not b: excluded["missing_or_invalid_horizon"]+=1
       else: multiples.append(b/a); phases[ss[(x["launch_id"],h)]["market_phase"]]+=1
      n=len(multiples); thresholds={name:_pct(sum(f(v) for v in multiples),n) for name,f in {"ge_1_25x":lambda v:v>=Decimal("1.25"),"ge_1_5x":lambda v:v>=Decimal("1.5"),"ge_2x":lambda v:v>=2,"ge_3x":lambda v:v>=3,"ge_5x":lambda v:v>=5,"ge_10x":lambda v:v>=10,"le_0_75x":lambda v:v<=Decimal(".75"),"le_0_5x":lambda v:v<=Decimal(".5"),"le_0_25x":lambda v:v<=Decimal(".25"),"le_0_1x":lambda v:v<=Decimal(".1")}.items()}
      horizons[_label(h)]={"target_age_seconds":h,"scheduled":len(scheduled),"eligible_sampled":len(scheduled),"target_due":len(mature),"snapshot_available":len(observed),"missing_due":len(mature)-len(observed),"pending_future":len(scheduled)-len(mature),"completion_rate_pct":round(100*len(observed)/len(mature),2) if mature else None,"valid_price_pairs":n,"phase_counts":dict(phases),"excluded":dict(excluded),"multiple_distribution":{k:_q(multiples,p) for k,p in (("min",0),("p10",.1),("p25",.25),("median",.5),("p75",.75),("p90",.9),("p95",.95),("max",1))},"thresholds":thresholds,"warning":"small_sample" if n<30 else "limited_sample" if n<100 else None}
    miss=Counter()
    for x in targets:
      if x["sampling_group"]=="not_sampled": miss["unsampled"]+=1
      elif datetime.fromisoformat(x["due_at"])>now: miss["not_due"]+=1
      elif (x["launch_id"],x["target_age_seconds"]) not in ss: miss[x["error_code"] or ("budget_skipped" if x["status"]=="pending" else x["status"] or "unknown")]+=1
    return {"window":{"from":start.isoformat(),"to":now.isoformat(),"timezone":"UTC"},"quality_filter":sorted(qs),"min_completeness":min_completeness,"cohort":{"all_stock_launches":len(launches),"initial_sampled":len(initial),"long_sampled":len(long),"baseline_available":sum(_d(ss.get((i,0)),qs) is not None for i in initial),"launches_passing_min_completeness":len(passing),"long_without_initial":len(long-initial)},"horizons":horizons,"missingness":dict(miss),"warnings":[x for x in (["small_sample" if len(passing)<30 else "limited_sample" if len(passing)<100 else None,"long_without_initial" if long-initial else None]) if x]}
