"""Deterministic, read-only Phase 2C research preflight. No RPC imports."""
import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import random
import sqlite3
import statistics
import sys
import time

WINDOWS = (30, 60, 300, 900, 3600)
HORIZONS = (300, 900, 3600, 21600, 86400)
# Only event-time aggregates. Amounts remain quote-native and are not pooled here.
PREDICTORS = ('curve_buy_count', 'curve_sell_count', 'total_directional_event_count',
              'v4_core_swap_count', 'unique_buy_recipients', 'top1_buy_recipient_token_share')
SESSION = '1eb525a3fe2a41b2bab1b5bf8dca4a9a'


def stamp(value):
    return datetime.fromisoformat(value).timestamp()


def quantile(values, p):
    if not values:
        return None
    values = sorted(values)
    at = (len(values) - 1) * p
    lo = int(at)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (at - lo)


def distribution(values):
    return {'n': len(values), **{k: quantile(values, p) for k, p in
            (('p10', .1), ('p25', .25), ('median', .5), ('p75', .75), ('p90', .9))}}


def ranks(values):
    result = [0.0] * len(values)
    order = sorted(range(len(values)), key=values.__getitem__)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and values[order[j]] == values[order[i]]:
            j += 1
        for k in order[i:j]:
            result[k] = (i + j + 1) / 2
        i = j
    return result


def spearman(xs, ys):
    if len(xs) < 3 or len(set(xs)) < 2 or len(set(ys)) < 2:
        return None
    return statistics.correlation(ranks(xs), ranks(ys))


def screen(values, seed):
    pairs = [(x, y) for x, y in values if x is not None and math.isfinite(x)]
    out = {'n': len(pairs), 'feature': distribution([x for x, _ in pairs])}
    if len(pairs) < 30:
        out['note'] = 'tiny_sample_no_correlation'
        return out
    xs, ys = zip(*pairs)
    if len(set(xs)) < 2:
        out['note'] = 'constant_feature_no_correlation'
        return out
    zero_count = sum(x == 0 for x in xs)
    if len(set(xs)) == 2 and zero_count and min(zero_count, len(xs) - zero_count) < 10:
        out['note'] = 'sparse_feature_no_correlation'
        return out
    out['spearman'] = spearman(xs, ys)
    if out['spearman'] is not None:
        rng = random.Random(seed)
        draws = []
        for _ in range(100):
            ix = [rng.randrange(len(xs)) for _ in xs]
            value = spearman([xs[i] for i in ix], [ys[i] for i in ix])
            if value is not None:
                draws.append(value)
        out['bootstrap_95pct'] = [quantile(draws, .025), quantile(draws, .975)]
    ordered = sorted(pairs)
    out['feature_terciles'] = [{'n': len(part), 'median_label_multiple': quantile([y for _, y in part], .5)}
                               for part in (ordered[:len(ordered)//3],
                                            ordered[len(ordered)//3:2*len(ordered)//3],
                                            ordered[2*len(ordered)//3:])]
    return out


@contextmanager
def open_readonly(main_path, flow_path):
    main_uri = Path(main_path).resolve().as_uri() + '?mode=ro'
    flow_uri = Path(flow_path).resolve().as_uri() + '?mode=ro'
    db = sqlite3.connect(main_uri, uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    db.execute('ATTACH DATABASE ? AS flow', (flow_uri,))
    db.execute('PRAGMA query_only=ON')
    db.execute('BEGIN')
    try:
        yield db
    finally:
        db.rollback()
        db.execute('DETACH DATABASE flow')
        db.close()


def era(start, cutoff, legacy_end, split_start):
    if start >= split_start:
        return 'SPLIT_FLOW_ERA'
    if cutoff <= legacy_end:
        return 'LEGACY_FLOW_ERA'
    return 'TRANSITION_ERA'


def positive(value):
    try:
        number = Decimal(value)
        return number if number.is_finite() and number > 0 else None
    except (InvalidOperation, TypeError):
        return None


PAIR_SQL = '''
SELECT t.launch_id,t.tracking_start_at,t.cohort_long,t.token_address target_token,
       t.quote_asset_address,l.token_address launch_token,l.quote_asset_address launch_quote,
       l.block_timestamp,ot.due_at,ot.sampling_group,
       f.feature_cutoff_at,f.finalized_at,f.coverage_quality,f.coverage_reason,f.metrics,
       b.observed_at baseline_at,b.data_quality baseline_quality,b.price_quote baseline_price,
       b.fdv_quote baseline_fdv,b.quote_asset_address baseline_quote,
       y.observed_at label_at,y.data_quality label_quality,y.price_quote label_price,
       y.fdv_quote label_fdv,y.quote_asset_address label_quote,y.market_phase label_phase,
       (SELECT g.block_timestamp FROM graduations g WHERE lower(g.token_address)=lower(t.token_address)
        ORDER BY g.block_number,g.log_index LIMIT 1) graduation_at
FROM flow.flow_tracking_targets t JOIN launches l ON l.id=t.launch_id
JOIN outcome_targets ot ON ot.launch_id=t.launch_id AND ot.target_age_seconds=?
LEFT JOIN flow.flow_features f ON f.launch_id=t.launch_id AND f.window_seconds=?
LEFT JOIN market_snapshots b ON b.launch_id=t.launch_id AND b.target_age_seconds=0
LEFT JOIN market_snapshots y ON y.launch_id=t.launch_id AND y.target_age_seconds=?
WHERE l.is_stock_quote=1 AND ot.sampling_group!='not_sampled'
ORDER BY t.launch_id'''


def audit_pair(db, window, horizon, as_of, legacy_end, split_start):
    if window not in WINDOWS or horizon not in HORIZONS or window >= horizon:
        raise ValueError('Feature cutoff must precede a supported label horizon')
    reasons, quality, eras, phases, cohorts = Counter(), Counter(), Counter(), Counter(), Counter()
    rows, grain, separations = [], set(), []
    for r in db.execute(PAIR_SQL, (horizon, window, horizon)):
        cutoff = r['tracking_start_at'] + window
        if cutoff > as_of or stamp(r['due_at']) > as_of:
            reasons['not_mature'] += 1
            continue
        if r['feature_cutoff_at'] is None:
            reasons['feature_missing'] += 1
            continue
        if (r['target_token'].lower() != r['launch_token'].lower() or
            r['quote_asset_address'].lower() != r['launch_quote'].lower()):
            reasons['launch_identity_mismatch'] += 1
            continue
        quality[r['coverage_quality']] += 1
        if r['coverage_quality'] != 'complete':
            reasons['flow_' + r['coverage_quality']] += 1
            continue
        if abs(r['feature_cutoff_at'] - cutoff) > 1:
            reasons['cutoff_mismatch'] += 1
            continue
        if not r['baseline_at'] or not r['label_at']:
            reasons['market_missing'] += 1
            continue
        label_at, baseline_at = stamp(r['label_at']), stamp(r['baseline_at'])
        if label_at > as_of or label_at <= cutoff or baseline_at > label_at:
            reasons['market_timing_invalid'] += 1
            continue
        if r['finalized_at'] is None or r['finalized_at'] >= label_at:
            reasons['feature_finalized_after_label'] += 1
            continue
        if (r['baseline_quality'] != 'verified' or r['label_quality'] != 'verified' or
            r['baseline_quote'].lower() != r['quote_asset_address'].lower() or
            r['label_quote'].lower() != r['quote_asset_address'].lower()):
            reasons['market_quality_or_quote'] += 1
            continue
        baseline, label = positive(r['baseline_price']), positive(r['label_price'])
        if baseline is None or label is None:
            reasons['price_invalid'] += 1
            continue
        key = (r['launch_id'], window, horizon)
        if key in grain:
            raise ValueError('Duplicate analytical grain')
        grain.add(key)
        metrics = json.loads(r['metrics'])
        values = {}
        for name in PREDICTORS:
            try:
                number = float(metrics[name]) if metrics.get(name) is not None else None
                values[name] = number if number is not None and math.isfinite(number) else None
            except (TypeError, ValueError, OverflowError):
                values[name] = None
        row_era = era(r['tracking_start_at'], cutoff, legacy_end, split_start)
        state = 'v4_at_cutoff' if r['graduation_at'] and stamp(r['graduation_at']) <= cutoff else 'curve_at_cutoff'
        ratio = float(label / baseline)
        if not math.isfinite(ratio):
            reasons['ratio_invalid'] += 1
            grain.remove(key)
            continue
        fdv0, fdv1 = positive(r['baseline_fdv']), positive(r['label_fdv'])
        fdv_ratio = float(fdv1/fdv0) if fdv0 and fdv1 else None
        rows.append({'launch_id': r['launch_id'], 'launch_at': stamp(r['block_timestamp']),
                     'era': row_era, 'cohort': 'long' if r['cohort_long'] else 'initial_only',
                     'state': state, 'label_phase': r['label_phase'], 'multiple': ratio,
                     'fdv_multiple': fdv_ratio if fdv_ratio is None or math.isfinite(fdv_ratio) else None,
                     'features': values})
        separations.append(label_at - cutoff)
        reasons['eligible'] += 1
        eras[row_era] += 1
        phases[state] += 1
        cohorts['long' if r['cohort_long'] else 'initial_only'] += 1
    labels = [r['multiple'] for r in rows]
    feature_screen = {name: screen([(r['features'][name], r['multiple']) for r in rows],
                                   window * 100000 + horizon + i)
                      for i, name in enumerate(PREDICTORS)}
    def subgroups(field):
        return {name: {'n': len(subset), 'label_multiple': distribution([r['multiple'] for r in subset]),
                       'median_curve_buy_count': quantile([r['features']['curve_buy_count'] for r in subset
                                                           if r['features']['curve_buy_count'] is not None], .5),
                       'median_directional_event_count': quantile([r['features']['total_directional_event_count'] for r in subset
                                                                    if r['features']['total_directional_event_count'] is not None], .5)}
                for name in sorted({r[field] for r in rows}) for subset in [[r for r in rows if r[field] == name]]}
    return {'feature_cutoff_seconds': window, 'label_horizon_seconds': horizon,
            'minimum_scheduled_separation_seconds': horizon-window,
            'minimum_observed_separation_seconds': min(separations) if separations else None,
            'mature_denominator': sum(reasons.values()) - reasons['not_mature'], 'coverage': dict(quality),
            'exclusive_exclusions': dict(reasons), 'usable_n': len(rows),
            'usable_by_era': dict(eras), 'usable_by_state_at_cutoff': dict(phases),
            'usable_by_cohort': dict(cohorts), 'usable_by_label_market_phase': dict(Counter(r['label_phase'] for r in rows)),
            'descriptives_by_era': subgroups('era'),
            'descriptives_by_state_at_cutoff': subgroups('state'),
            'descriptives_by_cohort': subgroups('cohort'),
            'label_price_multiple_t0_to_h': distribution(labels),
            'label_fdv_multiple_t0_to_h': distribution([r['fdv_multiple'] for r in rows if r['fdv_multiple'] is not None]),
            'feature_outcome_screen': feature_screen,
            'split_era_launch_span_days': ((max(r['launch_at'] for r in rows if r['era']=='SPLIT_FLOW_ERA') -
                                            min(r['launch_at'] for r in rows if r['era']=='SPLIT_FLOW_ERA'))/86400
                                           if eras['SPLIT_FLOW_ERA'] else None)}


def selection_bias(db, as_of, legacy_end, split_start):
    groups = defaultdict(lambda: {'n': 0, 'launch_hours': [], 'event_counts': []})
    sql = '''SELECT t.tracking_start_at,t.cohort_long,f.coverage_quality,
      (SELECT g.block_timestamp FROM graduations g WHERE lower(g.token_address)=lower(t.token_address)
       ORDER BY g.block_number,g.log_index LIMIT 1) graduation_at,
      (SELECT count(*) FROM flow.flow_events e WHERE e.launch_id=t.launch_id AND e.removed=0
       AND e.event_time>=t.tracking_start_at AND e.event_time<=t.tracking_start_at+60) event_count
      FROM flow.flow_tracking_targets t JOIN flow.flow_features f ON f.launch_id=t.launch_id
      WHERE f.window_seconds=60 AND f.feature_cutoff_at<=?'''
    for r in db.execute(sql, (as_of,)):
        cutoff = r['tracking_start_at'] + 60
        key = (r['coverage_quality'], era(r['tracking_start_at'], cutoff, legacy_end, split_start),
               'long' if r['cohort_long'] else 'initial_only',
               'v4_at_cutoff' if r['graduation_at'] and stamp(r['graduation_at']) <= cutoff else 'curve_at_cutoff')
        g = groups[key]
        g['n'] += 1
        g['launch_hours'].append((r['tracking_start_at'] % 86400) / 3600)
        g['event_counts'].append(r['event_count'])
    return [{'quality': k[0], 'era': k[1], 'cohort': k[2], 'state_at_cutoff': k[3],
             'n': v['n'], 'median_utc_launch_hour': quantile(v['launch_hours'], .5),
             'median_raw_event_count_60s': quantile(v['event_counts'], .5)}
            for k, v in sorted(groups.items())]


def audit(db, as_of, session_id=SESSION):
    row = db.execute('SELECT payload,status FROM flow.flow_cutover_sessions WHERE id=?', (session_id,)).fetchone()
    if not row or row['status'] != 'COMPLETE':
        raise ValueError('Completed cutover session proof required')
    session = json.loads(row['payload'])
    legacy_end = stamp(session['source_stopped_at'])
    split_start = session['validation_started_at']
    if not legacy_end < split_start <= as_of:
        raise ValueError('Invalid quality-era boundary')
    as_of_iso = datetime.fromtimestamp(as_of, timezone.utc).isoformat()
    scalar = lambda sql, arg: db.execute(sql, (arg,)).fetchone()[0]
    counts = {'stock_paired_launches': scalar('SELECT count(*) FROM launches WHERE is_stock_quote=1 AND block_timestamp<=?', as_of_iso),
              'phase2a_sampled_launches': scalar("SELECT count(DISTINCT o.launch_id) FROM outcome_targets o JOIN launches l ON l.id=o.launch_id WHERE o.sampling_group!='not_sampled' AND l.block_timestamp<=?", as_of_iso),
              'phase2b_tracked_launches': scalar('SELECT count(*) FROM flow.flow_tracking_targets WHERE tracking_start_at<=?', as_of),
              'phase2a_phase2b_overlap': scalar("SELECT count(DISTINCT t.launch_id) FROM flow.flow_tracking_targets t JOIN outcome_targets o ON o.launch_id=t.launch_id WHERE o.sampling_group!='not_sampled' AND t.tracking_start_at<=?", as_of),
              'initial_cohort': scalar('SELECT count(*) FROM flow.flow_tracking_targets WHERE cohort_initial=1 AND tracking_start_at<=?', as_of),
              'long_cohort': scalar('SELECT count(*) FROM flow.flow_tracking_targets WHERE cohort_long=1 AND tracking_start_at<=?', as_of),
              'graduated_by_as_of_diagnostic': scalar('SELECT count(DISTINCT t.launch_id) FROM flow.flow_tracking_targets t JOIN graduations g ON lower(g.token_address)=lower(t.token_address) WHERE g.block_timestamp<=?', as_of_iso)}
    quality = [dict(r) for r in db.execute('SELECT window_seconds,coverage_quality,count(*) n FROM flow.flow_features WHERE feature_cutoff_at<=? GROUP BY 1,2 ORDER BY 1,2', (as_of,))]
    era_quality = Counter()
    for r in db.execute('''SELECT t.tracking_start_at,f.feature_cutoff_at,f.window_seconds,f.coverage_quality
                         FROM flow.flow_features f JOIN flow.flow_tracking_targets t ON t.launch_id=f.launch_id
                         WHERE f.feature_cutoff_at<=?''', (as_of,)):
        era_quality[(era(r['tracking_start_at'],r['feature_cutoff_at'],legacy_end,split_start),
                     r['window_seconds'],r['coverage_quality'])] += 1
    pairs = [audit_pair(db, w, h, as_of, legacy_end, split_start)
             for w in WINDOWS for h in HORIZONS if w < h]
    primary = next(p for p in pairs if p['feature_cutoff_seconds']==60 and p['label_horizon_seconds']==86400)
    n = primary['usable_by_era'].get('SPLIT_FLOW_ERA', 0)
    span = primary['split_era_launch_span_days'] or 0
    return {'as_of_utc': datetime.fromtimestamp(as_of, timezone.utc).isoformat(),
            'session_id': session_id, 'quality_era_boundary': {
                'legacy_end_utc': datetime.fromtimestamp(legacy_end, timezone.utc).isoformat(),
                'split_start_utc': datetime.fromtimestamp(split_start, timezone.utc).isoformat(),
                'split_start_block': session['H_live']},
            'analytical_grain': 'launch_id x feature_window_seconds x label_horizon_seconds',
            'predictor_allowlist': list(PREDICTORS), 'counts': counts, 'feature_coverage': quality,
            'era_feature_coverage': [{'era': k[0], 'window_seconds': k[1], 'quality': k[2], 'n': n}
                                     for k,n in sorted(era_quality.items())],
            'pairs': pairs, 'selection_bias_60s': selection_bias(db, as_of, legacy_end, split_start),
            'chronological_split': {'primary_pair': '60s_to_24h', 'split_era_n': n,
                                    'split_era_launch_span_days': span,
                                    'ready_for_train_validation_holdout': n >= 300 and span >= 30,
                                    'rule': 'Require at least 300 split-era rows over 30 days; no random shuffle.'},
            'interpretation': ['retrospective event-time features, not proven available live at exact cutoff',
                               'T0-to-H marginal price and FDV multiples are not executable returns',
                               'future market phase and final target status are diagnostics, never predictors']}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--main-db', required=True)
    p.add_argument('--flow-db', required=True)
    p.add_argument('--as-of', required=True, help='Fixed UTC ISO timestamp for deterministic maturity rules')
    p.add_argument('--session-id', default=SESSION)
    p.add_argument('--output', help='Write counts/statistics JSON outside the production databases')
    a = p.parse_args()
    started = time.perf_counter()
    at = datetime.fromisoformat(a.as_of)
    if at.tzinfo is None or at.utcoffset().total_seconds() != 0:
        p.error('--as-of must include a UTC offset')
    as_of = at.timestamp()
    if a.output and Path(a.output).resolve() in (Path(a.main_db).resolve(), Path(a.flow_db).resolve()):
        p.error('Output cannot replace an input database')
    with open_readonly(a.main_db, a.flow_db) as db:
        result = audit(db, as_of, a.session_id)
    runtime = round(time.perf_counter() - started, 3)
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except ImportError:
        rss = None
    output = json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + '\n'
    if a.output:
        Path(a.output).write_text(output, encoding='utf-8')
    else:
        print(output, end='')
    print(json.dumps({'runtime_seconds': runtime, 'peak_rss_kib': rss}), file=sys.stderr)


if __name__ == '__main__':
    main()
