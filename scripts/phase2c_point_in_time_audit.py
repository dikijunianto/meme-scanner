"""Offline Phase 2C point-in-time and era-separated descriptive audit.

Run as ``python -m scripts.phase2c_point_in_time_audit``. No RPC imports.
"""
import argparse
import hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys
import time

from scripts import phase2c_dataset_audit as base


PAIR_SQL = '''
SELECT t.launch_id,t.tracking_start_at,t.cohort_long,t.token_address target_token,
       t.quote_asset_address target_quote,l.token_address launch_token,
       l.quote_asset_address launch_quote,ot.due_at,
       f.feature_cutoff_at,f.finalized_at,f.coverage_quality,f.metrics,
       b.observed_at baseline_at,b.data_quality baseline_quality,b.price_quote baseline_price,
       b.quote_asset_address baseline_quote,b.market_phase baseline_phase,
       b.quote_reserve baseline_quote_reserve,b.token_reserve baseline_token_reserve,
       y.observed_at label_at,y.data_quality label_quality,y.price_quote label_price,
       y.quote_asset_address label_quote,y.market_phase label_phase,
       y.quote_reserve label_quote_reserve,y.token_reserve label_token_reserve,
       (SELECT g.block_timestamp FROM graduations g WHERE lower(g.token_address)=lower(t.token_address)
        ORDER BY g.block_number,g.log_index LIMIT 1) graduation_at
FROM flow.flow_tracking_targets t JOIN launches l ON l.id=t.launch_id
JOIN outcome_targets ot ON ot.launch_id=t.launch_id AND ot.target_age_seconds=?
LEFT JOIN flow.flow_features f ON f.launch_id=t.launch_id AND f.window_seconds=?
LEFT JOIN market_snapshots b ON b.launch_id=t.launch_id AND b.target_age_seconds=0
LEFT JOIN market_snapshots y ON y.launch_id=t.launch_id AND y.target_age_seconds=?
WHERE l.is_stock_quote=1 AND ot.sampling_group!='not_sampled'
ORDER BY t.launch_id'''


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


def eligible_at(feature_available_at, proof_at, prediction_at):
    return (feature_available_at is not None and proof_at is not None and
            feature_available_at <= prediction_at and proof_at <= prediction_at)


def availability(cutoff, latest_observed, late_events, latest_bootstrap,
                 unknown_event_times, last_finalized, era):
    """Historical first materialization/proof time is not in the current schema."""
    lower = max(x for x in (cutoff, latest_observed, latest_bootstrap) if x is not None)
    reasons = []
    if late_events:
        reasons.append('retained_event_first_seen_after_cutoff')
    if latest_bootstrap is not None and latest_bootstrap > cutoff:
        reasons.append('bootstrap_proof_after_cutoff')
    if unknown_event_times:
        reasons.append('event_time_unverified')
    reasons.extend(('first_feature_version_not_retained', 'first_completeness_proof_not_timestamped'))
    # The deployed split worker calls rebuild(now-3), so even its stored
    # finalized_at is three seconds earlier than actual materialization.
    if era == 'SPLIT_FLOW_ERA':
        reasons.append('split_worker_three_second_confirmation_buffer')
    definitely_late = late_events > 0 or (latest_bootstrap is not None and latest_bootstrap > cutoff)
    return {'raw_first_all_present_at': None, 'raw_latest_retained_first_seen_at': utc(latest_observed),
            'first_completeness_proof_at': None, 'feature_available_at': None,
            'last_stored_finalized_at': utc(last_finalized),
            'usable_at_exact_cutoff': 'NO' if definitely_late or era == 'SPLIT_FLOW_ERA' else 'UNKNOWN',
            'minimum_delay_seconds': round(lower-cutoff, 3) if lower > cutoff else None,
            'reasons': reasons}


def window_rows(db, as_of, legacy_end, split_start):
    bootstrap = {(r['launch_id'],r['kind']):r['completed_at'] for r in db.execute('''
        SELECT launch_id,kind,completed_at FROM flow.flow_bootstrap WHERE status='complete' ''')}
    sql = '''SELECT f.launch_id,f.window_seconds,f.feature_cutoff_at,f.finalized_at,
      f.coverage_quality,f.metrics,t.tracking_start_at,t.cohort_long,t.graduation_json,
      max(CASE WHEN e.removed=0 THEN e.observed_at END) latest_observed,
      sum(CASE WHEN e.removed=0 AND e.observed_at>f.feature_cutoff_at THEN 1 ELSE 0 END) late_events,
      sum(CASE WHEN e.removed=0 AND e.event_time_source!='log_block_timestamp' THEN 1 ELSE 0 END) unknown_times,
      sum(CASE WHEN e.removed=0 THEN 1 ELSE 0 END) current_events,
      sum(CASE WHEN e.removed=1 THEN 1 ELSE 0 END) removed_events
      FROM flow.flow_features f JOIN flow.flow_tracking_targets t USING(launch_id)
      LEFT JOIN flow.flow_events e ON e.launch_id=f.launch_id
        AND e.event_time>=t.tracking_start_at AND e.event_time<=f.feature_cutoff_at
      WHERE f.feature_cutoff_at<=? AND t.tracking_start_at<=?
      GROUP BY f.launch_id,f.window_seconds ORDER BY f.launch_id,f.window_seconds'''
    rows = {}
    counts = defaultdict(Counter)
    delays = defaultdict(list)
    finalization_offsets = defaultdict(list)
    for r in db.execute(sql, (as_of, as_of)):
        row = dict(r)
        row_era = base.era(row['tracking_start_at'], row['feature_cutoff_at'], legacy_end, split_start)
        row['era'] = row_era
        graduation = json.loads(row['graduation_json']) if row['graduation_json'] else None
        kinds = ('curve','v4','hook') if graduation and base.stamp(graduation['block_timestamp'])<=row['feature_cutoff_at'] else ('curve',)
        latest_bootstrap = max((bootstrap[(row['launch_id'],kind)] for kind in kinds
                                if (row['launch_id'],kind) in bootstrap),default=None)
        row['pit'] = availability(row['feature_cutoff_at'], row['latest_observed'],
                                  row['late_events'] or 0, latest_bootstrap,
                                  row['unknown_times'] or 0, row['finalized_at'], row_era)
        rows[(row['launch_id'], row['window_seconds'])] = row
        c = counts[(row_era, row['window_seconds'])]
        finalization_offsets[(row_era, row['window_seconds'])].append(
            row['finalized_at']-row['feature_cutoff_at'])
        c['windows'] += 1
        c['quality_'+row['coverage_quality']] += 1
        c['exact_cutoff_'+row['pit']['usable_at_exact_cutoff']] += 1
        c['first_complete_proof_unknown'] += 1
        c['first_feature_available_unknown'] += 1
        c['first_all_raw_present_unknown'] += 1
        if row['late_events']:
            c['with_late_retained_events'] += 1
        if row['pit']['minimum_delay_seconds'] is not None:
            c['known_positive_delay_lower_bound'] += 1
            delays[(row_era, row['window_seconds'])].append(row['pit']['minimum_delay_seconds'])
        if row['unknown_times']:
            c['with_unverified_event_time'] += 1
        if row['removed_events']:
            c['with_current_removed_events'] += 1
    return rows, [{'era': era, 'window_seconds': window, **dict(c),
                   'positive_delay_lower_bound_seconds':base.distribution(delays[(era,window)]),
                   'last_stored_finalized_minus_cutoff_seconds':base.distribution(
                       finalization_offsets[(era,window)])}
                  for (era, window), c in sorted(counts.items())]


def label_distribution(rows):
    values = [r['multiple'] for r in rows]
    n = len(values)
    d = base.distribution(values)
    d.update({'min': min(values) if n else None, 'max': max(values) if n else None,
              'fraction_exactly_one': sum(r['label_relation']==0 for r in rows)/n if n else None,
              'fraction_below_one': sum(r['label_relation']<0 for r in rows)/n if n else None,
              'fraction_above_one': sum(r['label_relation']>0 for r in rows)/n if n else None})
    exact = [r for r in rows if r['label_relation']==0]
    d['exact_one_curve_same_reserves'] = sum(r['reserve_state']=='same_curve_reserves' for r in exact)
    d['exact_one_curve_different_reserves'] = sum(r['reserve_state']=='different_curve_reserves' for r in exact)
    d['exact_one_other_or_unknown_state'] = len(exact)-d['exact_one_curve_same_reserves']-d['exact_one_curve_different_reserves']
    return d


def reserve_state(r):
    if r['baseline_phase'] != 'curve' or r['label_phase'] != 'curve':
        return 'other_or_unknown_state'
    values = (r['baseline_quote_reserve'],r['baseline_token_reserve'],
              r['label_quote_reserve'],r['label_token_reserve'])
    if any(v is None for v in values):
        return 'other_or_unknown_state'
    return ('same_curve_reserves' if values[:2] == values[2:]
            else 'different_curve_reserves')


def descriptive_row(r, window, horizon, as_of, windows):
    if window >= horizon:
        raise ValueError('Feature cutoff must precede label horizon')
    if base.stamp(r['due_at']) > as_of or r['tracking_start_at']+window > as_of:
        return None
    f = windows.get((r['launch_id'],window))
    if not f or f['coverage_quality'] != 'complete':
        return None
    if r['feature_cutoff_at'] is None or abs(r['feature_cutoff_at']-r['tracking_start_at']-window)>1:
        return None
    if (r['target_token'].lower()!=r['launch_token'].lower() or
        r['target_quote'].lower()!=r['launch_quote'].lower() or
        not r['baseline_at'] or not r['label_at']):
        return None
    baseline_at,label_at = base.stamp(r['baseline_at']),base.stamp(r['label_at'])
    if (label_at>as_of or label_at<=r['feature_cutoff_at'] or baseline_at>label_at or
        r['finalized_at'] is None or r['finalized_at']>=label_at):
        return None
    if (r['baseline_quality']!='verified' or r['label_quality']!='verified' or
        r['baseline_quote'].lower()!=r['target_quote'].lower() or
        r['label_quote'].lower()!=r['target_quote'].lower()):
        return None
    p0,p1 = base.positive(r['baseline_price']),base.positive(r['label_price'])
    if p0 is None or p1 is None:
        return None
    multiple = float(p1/p0)
    if not math.isfinite(multiple):
        return None
    metrics = json.loads(r['metrics'])
    features = {}
    for key in base.PREDICTORS:
        try:
            x = float(metrics[key]) if metrics.get(key) is not None else None
            features[key] = x if x is not None and math.isfinite(x) else None
        except (TypeError, ValueError, OverflowError):
            features[key] = None
    return {'multiple':multiple,'label_relation':(p1>p0)-(p1<p0),
            'features':features,'cohort':'long' if r['cohort_long'] else 'initial_only',
            'state':'v4_at_cutoff' if r['graduation_at'] and base.stamp(r['graduation_at'])<=r['feature_cutoff_at'] else 'curve_at_cutoff',
            'reserve_state':reserve_state(r),'current_event_count':f['current_events'] or 0,
            'tracking_start_at':r['tracking_start_at'],'pit':f['pit']}


def summarize_rows(rows, seed):
    return {'descriptive_n':len(rows),
            'point_in_time_safe_n':sum(r['pit']['usable_at_exact_cutoff']=='YES' for r in rows),
            'launch_span_days':((max(r['tracking_start_at'] for r in rows)-
                                 min(r['tracking_start_at'] for r in rows))/86400 if rows else None),
            'label':label_distribution(rows),
            'curve_at_cutoff_n':sum(r['state']=='curve_at_cutoff' for r in rows),
            'v4_at_cutoff_n':sum(r['state']=='v4_at_cutoff' for r in rows),
            'candidate_features':{key:base.screen([(r['features'][key],r['multiple']) for r in rows],seed+i)
                                  for i,key in enumerate(base.PREDICTORS)}}


def chronological_readiness(rows, ledger=False):
    policy = {'minimum_point_in_time_safe_split_era_n':600,
              'minimum_span_days':60,'chronological_train_validation_holdout':'60/20/20',
              'minimum_holdout_n':120,'minimum_below_one_and_above_one_per_slice':20,
              'required_feature_proof':'immutable first materialization and completeness time <= prediction time'}
    eligible = sorted((r for r in rows if ledger or r['pit']['usable_at_exact_cutoff']=='YES'),
                      key=lambda r:r['tracking_start_at'])
    n = len(eligible)
    span = ((eligible[-1]['tracking_start_at']-eligible[0]['tracking_start_at'])/86400
            if n>1 else 0)
    parts = (eligible[:int(n*.6)],eligible[int(n*.6):int(n*.8)],eligible[int(n*.8):])
    slices = {name:{'n':len(part),'below_one':sum(r['label_relation']<0 for r in part),
                    'exact_one':sum(r['label_relation']==0 for r in part),
                    'above_one':sum(r['label_relation']>0 for r in part)}
              for name,part in zip(('train','validation','holdout'),parts)}
    failures = []
    if n < 600: failures.append('point_in_time_safe_n_below_600')
    if span < 60: failures.append('launch_span_below_60_days')
    if slices['holdout']['n'] < 120: failures.append('holdout_below_120')
    if any(min(s['below_one'],s['above_one'])<20 for s in slices.values()):
        failures.append('insufficient_outcome_variation_in_chronological_slice')
    return {'predefined_policy':policy,'current_point_in_time_safe_n':n,
            'current_descriptive_n':len(rows),
            'current_descriptive_launch_span_days':((max(r['tracking_start_at'] for r in rows)-
                min(r['tracking_start_at'] for r in rows))/86400 if rows else None),
            'point_in_time_safe_launch_span_days':span,'chronological_slices':slices,
            'unmet_conditions':failures,
            'status':'MODEL_DATA_NOT_MATURE' if failures else 'MODEL_DATA_MINIMUM_READY'}


def pair_audit(db, windows, as_of, legacy_end, split_start):
    output, sensitivity, model_gate = [], {}, None
    for window in base.WINDOWS:
        for horizon in base.HORIZONS:
            if window >= horizon:
                continue
            by_era = defaultdict(list)
            counts = defaultdict(Counter)
            for r in db.execute(PAIR_SQL,(horizon,window,horizon)):
                cutoff = r['tracking_start_at']+window
                row_era = base.era(r['tracking_start_at'],cutoff,legacy_end,split_start)
                c = counts[row_era]
                if cutoff>as_of or base.stamp(r['due_at'])>as_of:
                    continue
                c['mature_scheduled_labels'] += 1
                if r['label_at'] and base.stamp(r['label_at'])<=as_of and r['label_quality']=='verified':
                    c['verified_label_snapshots'] += 1
                f = windows.get((r['launch_id'],window))
                if f and f['coverage_quality']=='complete':
                    c['complete_features_among_mature'] += 1
                    if f['pit']['usable_at_exact_cutoff']=='YES':
                        c['point_in_time_safe_features_among_mature'] += 1
                row = descriptive_row(r,window,horizon,as_of,windows)
                if row:
                    by_era[row_era].append(row)
            item = {'feature_cutoff_seconds':window,'label_horizon_seconds':horizon,'eras':{}}
            for row_era in ('LEGACY_FLOW_ERA','TRANSITION_ERA','SPLIT_FLOW_ERA'):
                c = counts[row_era]
                for key in ('mature_scheduled_labels','verified_label_snapshots',
                            'complete_features_among_mature','point_in_time_safe_features_among_mature'):
                    c.setdefault(key,0)
                rows = by_era[row_era]
                summary = summarize_rows(rows,window*100000+horizon)
                item['eras'][row_era] = {**dict(c),**summary,
                    'final_model_usable_n':summary['point_in_time_safe_n']}
            output.append(item)
            if window==60 and horizon==21600:
                legacy = by_era['LEGACY_FLOW_ERA']
                nonzero = [r for r in legacy if r['features']['total_directional_event_count'] and
                           r['features']['total_directional_event_count']>0]
                varied = [r for r in legacy if r['label_relation']!=0]
                low = [r for r in legacy if r['current_event_count']<=1]
                high = [r for r in legacy if r['current_event_count']>1]
                sensitivity = {name:{'n':len(subset),'label_exact_one_fraction':label_distribution(subset)['fraction_exactly_one'],
                    'curve_buy':base.screen([(r['features']['curve_buy_count'],r['multiple']) for r in subset],101),
                    'directional_events':base.screen([(r['features']['total_directional_event_count'],r['multiple']) for r in subset],102)}
                    for name,subset in [('all_eligible',legacy),('nonzero_directional',nonzero),
                                        ('label_not_exact_one',varied),('low_raw_activity_0_or_1',low),
                                        ('higher_raw_activity_2_plus',high),
                                        ('long_cohort',[r for r in legacy if r['cohort']=='long'])]}
            if window==60 and horizon==86400:
                model_gate = chronological_readiness(by_era['SPLIT_FLOW_ERA'])
    return output,sensitivity,model_gate


def ledger_audit(db,as_of):
    """Only prospective, immutable first-eligible versions can enter model counts."""
    if not db.execute("SELECT 1 FROM flow.sqlite_master WHERE name='flow_feature_ledger_start'").fetchone():
        return {'boundary':None,'launches':0,'complete_windows':0,'first_eligible_versions':0,
                'usable_by_pair':{},'days_of_history':0,'model_readiness':chronological_readiness([],True)}
    start=db.execute('SELECT * FROM flow.flow_feature_ledger_start WHERE id=1').fetchone()
    if not start or start['start_at']>as_of:
        return {'boundary':None,'launches':0,'complete_windows':0,'first_eligible_versions':0,
                'usable_by_pair':{},'days_of_history':0,'model_readiness':chronological_readiness([],True)}
    versions={}
    for r in db.execute('''SELECT v.*,t.tracking_start_at,t.graduation_json FROM flow.flow_feature_versions v
        JOIN flow.flow_tracking_targets t USING(launch_id)
        WHERE t.tracking_start_at>=? AND v.materialized_at<=? AND v.feature_schema_version='v1'
        ORDER BY v.launch_id,v.window_seconds,v.version_number''',(start['start_at'],as_of)):
        key=(r['launch_id'],r['window_seconds'])
        if key not in versions and r['coverage_quality']=='complete' and r['model_eligible_at'] is not None and r['model_eligible_at']<=as_of:
            graduation=json.loads(r['graduation_json']) if r['graduation_json'] else None
            if graduation and base.stamp(graduation['block_timestamp'])<=r['feature_cutoff_at']:
                proof=json.loads(r['proof_json'])
                if not {'curve','v4','hook'}.issubset(
                        {f['kind'] for f in proof.get('filters',[]) if f.get('bootstrap_status')=='complete'
                         and f.get('cursor') is not None}):
                    continue
            if hashlib.sha256(r['payload'].encode()).hexdigest()!=r['payload_sha256']:
                raise ValueError('Immutable feature payload hash mismatch')
            versions[key]=dict(r)
    invalidated={(r['launch_id'],r['window_seconds']) for r in db.execute('''
        SELECT launch_id,window_seconds,version_number FROM flow.flow_feature_versions
        WHERE materialized_at<=? AND (write_reason LIKE 'reorg%' OR coverage_reason LIKE '%reorg%')''',(as_of,))
        if (r['launch_id'],r['window_seconds']) in versions and
        r['version_number']>versions[(r['launch_id'],r['window_seconds'])]['version_number']}
    features={(r['launch_id'],r['window_seconds']):r['coverage_quality'] for r in db.execute('''
        SELECT launch_id,window_seconds,coverage_quality FROM flow.flow_features''')}
    bias={'total_launches':0,'graduated_during_tracking':0,'spanning_windows':0,
          'complete_spanning_windows':0,'partial_spanning_windows':0,
          'unavailable_spanning_windows':0,'pit_eligible_spanning_windows':0}
    rates={name:{'due_windows':0,'pit_eligible_windows':0} for name in
           ('never_graduated_during_window','graduated_during_window')}
    for t in db.execute('''SELECT launch_id,tracking_start_at,tracking_end_at,cohort_long,
        graduation_json FROM flow.flow_tracking_targets WHERE tracking_start_at>=?
        AND tracking_start_at<=?''',(start['start_at'],as_of)):
        bias['total_launches']+=1
        graduation=json.loads(t['graduation_json']) if t['graduation_json'] else None
        graduated_at=base.stamp(graduation['block_timestamp']) if graduation else None
        if graduated_at is not None and t['tracking_start_at']<graduated_at<=t['tracking_end_at']:
            bias['graduated_during_tracking']+=1
        for window in base.WINDOWS:
            if window==3600 and not t['cohort_long']:continue
            cutoff=t['tracking_start_at']+window
            if cutoff>as_of:continue
            spanning=graduated_at is not None and t['tracking_start_at']<graduated_at<=cutoff
            key=(t['launch_id'],window)
            group=rates['graduated_during_window' if spanning else 'never_graduated_during_window']
            group['due_windows']+=1
            if key in versions and key not in invalidated:group['pit_eligible_windows']+=1
            if spanning:
                bias['spanning_windows']+=1
                quality=features.get(key,'unavailable')
                bias[f'{quality}_spanning_windows']+=1
                if key in versions and key not in invalidated:bias['pit_eligible_spanning_windows']+=1
    bias['eligibility_rates']={name:{**group,'rate':(group['pit_eligible_windows']/group['due_windows']
                                                    if group['due_windows'] else None)}
                               for name,group in rates.items()}
    pair_counts={};model_rows=[]
    for window in base.WINDOWS:
        for horizon in base.HORIZONS:
            if window>=horizon:continue
            n=0
            for r in db.execute('''SELECT t.launch_id,t.tracking_start_at,t.token_address target_token,
                t.quote_asset_address target_quote,l.token_address launch_token,
                l.quote_asset_address launch_quote,ot.due_at,
                y.observed_at label_at,y.data_quality label_quality,y.price_quote label_price,
                y.quote_asset_address label_quote,b.price_quote baseline_price,
                b.data_quality baseline_quality,b.quote_asset_address baseline_quote
                FROM flow.flow_tracking_targets t JOIN launches l ON l.id=t.launch_id
                JOIN outcome_targets ot
                  ON ot.launch_id=t.launch_id AND ot.target_age_seconds=?
                LEFT JOIN market_snapshots y ON y.launch_id=t.launch_id AND y.target_age_seconds=?
                LEFT JOIN market_snapshots b ON b.launch_id=t.launch_id AND b.target_age_seconds=0
                WHERE t.tracking_start_at>=? AND l.is_stock_quote=1
                  AND ot.sampling_group!='not_sampled' ''',
                (horizon,horizon,start['start_at'])):
                key=(r['launch_id'],window);v=versions.get(key)
                if not v or key in invalidated or not r['label_at'] or r['label_quality']!='verified' or r['baseline_quality']!='verified':
                    continue
                if (r['target_token'].lower()!=r['launch_token'].lower() or
                    r['target_quote'].lower()!=r['launch_quote'].lower() or
                    r['target_quote'].lower()!=r['label_quote'].lower() or
                    r['target_quote'].lower()!=r['baseline_quote'].lower()):
                    continue
                label_at=base.stamp(r['label_at'])
                if not (v['model_eligible_at']<label_at<=as_of and base.stamp(r['due_at'])<=as_of):
                    continue
                p0,p1=base.positive(r['baseline_price']),base.positive(r['label_price'])
                if p0 is None or p1 is None:continue
                n+=1
                if window==60 and horizon==86400:
                    model_rows.append({'tracking_start_at':r['tracking_start_at'],
                                       'label_relation':(p1>p0)-(p1<p0)})
            pair_counts[f'{window}s_to_{horizon}s']=n
    launches=db.execute('SELECT count(*) FROM flow.flow_tracking_targets WHERE tracking_start_at>=? AND tracking_start_at<=?',
                        (start['start_at'],as_of)).fetchone()[0]
    complete=db.execute('''SELECT count(DISTINCT v.launch_id || ':' || v.window_seconds) FROM flow.flow_feature_versions v
        JOIN flow.flow_tracking_targets t USING(launch_id)
        WHERE t.tracking_start_at>=? AND v.coverage_quality='complete' AND v.materialized_at<=?''',
        (start['start_at'],as_of)).fetchone()[0]
    return {'boundary':{'start_utc':utc(start['start_at']),'start_block':start['start_block'],
                        'deploy_revision':start['deploy_revision']},'launches':launches,
            'complete_windows':complete,'first_eligible_versions':len(versions),
            'graduation_selection_bias':bias,
            'usable_by_pair':pair_counts,'days_of_history':(as_of-start['start_at'])/86400,
            'model_readiness':chronological_readiness(model_rows,True)}


def audit(db, as_of, session_id=base.SESSION):
    row = db.execute('SELECT payload,status FROM flow.flow_cutover_sessions WHERE id=?',(session_id,)).fetchone()
    if not row or row['status']!='COMPLETE':
        raise ValueError('Completed split session proof required')
    session = json.loads(row['payload'])
    legacy_end,split_start = base.stamp(session['source_stopped_at']),session['validation_started_at']
    if not legacy_end<split_start<=as_of:
        raise ValueError('Invalid quality-era boundary')
    windows,coverage = window_rows(db,as_of,legacy_end,split_start)
    pairs,sensitivity,model_gate = pair_audit(db,windows,as_of,legacy_end,split_start)
    ledger=ledger_audit(db,as_of)
    split_targets = db.execute('''SELECT count(*) n,sum(cohort_initial) initial_n,sum(cohort_long) long_n
        FROM flow.flow_tracking_targets WHERE tracking_start_at>=? AND tracking_start_at<=?''',
        (split_start,as_of)).fetchone()
    recipient = db.execute('''SELECT count(*) curve_buy_events,
        sum(CASE WHEN recipient_address IS NULL THEN 1 ELSE 0 END) null_recipients,
        sum(CASE WHEN economic_actor IS NOT NULL THEN 1 ELSE 0 END) claimed_economic_actors
        FROM flow.flow_events WHERE removed=0 AND phase='curve' AND direction='buy' ''').fetchone()
    return {'as_of_utc':utc(as_of),'session_id':session_id,
            'era_boundary':{'legacy_end_utc':utc(legacy_end),'split_start_utc':utc(split_start),
                            'split_start_block':session['H_live']},
            'point_in_time_rule':'first eligible immutable version with model_eligible_at < label_observed_at',
            'historical_first_availability':'UNKNOWN: feature versions and coverage proof times are not retained',
            'candidate_classification':{key:'RECONSTRUCTABLE_ONLY' for key in base.PREDICTORS},
            'split_tracked':{'n':split_targets['n'],'initial':split_targets['initial_n'] or 0,
                             'long':split_targets['long_n'] or 0},
            'window_availability':coverage,'pairs_by_era':pairs,
            'point_in_time_ledger_era':ledger,
            'legacy_60s_to_6h_sensitivity':sensitivity,
            'recipient_semantics':dict(recipient),
            'model_readiness':ledger['model_readiness'],
            'pre_ledger_reconstructed_readiness':model_gate,
            'pooled_inference_permitted':False,
            'limitations':['First raw-event arrival is retained, but later corrections/removals have no version timestamp.',
                           'flow_features.finalized_at is overwritten and in normal split worker is stored as wall clock minus three seconds.',
                           'coverage_end_at and resolved gaps have no immutable first-proof timestamp.',
                           'Current COMPLETE status is not historical cutoff-time proof.']}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('main-db','flow-db','as-of','output'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--session-id',default=base.SESSION)
    a = p.parse_args()
    at = datetime.fromisoformat(a.as_of)
    if at.tzinfo is None or at.utcoffset().total_seconds()!=0:
        p.error('--as-of must include a UTC offset')
    if Path(a.output).resolve() in (Path(a.main_db).resolve(),Path(a.flow_db).resolve()):
        p.error('Output cannot replace an input database')
    started = time.perf_counter()
    with base.open_readonly(a.main_db,a.flow_db) as db:
        result = audit(db,at.timestamp(),a.session_id)
    Path(a.output).write_text(json.dumps(result,sort_keys=True,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except ImportError:
        rss = None
    print(json.dumps({'runtime_seconds':round(time.perf_counter()-started,3),
                      'peak_rss_kib':rss}),file=sys.stderr)


if __name__=='__main__':
    main()
