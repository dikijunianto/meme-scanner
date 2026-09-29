# Phase 2C.1 point-in-time validity and split-era replication

This is an offline research audit, not a trading or execution specification. Run `python -m scripts.phase2c_point_in_time_audit` from an isolated source copy against the two production SQLite files opened with `mode=ro` and `query_only`. It makes no provider call and exports aggregate statistics only. The exact quality-era boundary comes from completed cutover session `1eb525a3fe2a41b2bab1b5bf8dca4a9a`: guarded legacy stop **2026-09-27 17:38:23.866288 UTC**, split validation start **2026-09-27 17:39:04.824785 UTC**, H_live **74123713**. Crossing windows remain `TRANSITION_ERA` and are not pooled into either clean era.

## Three different times

- **FEATURE_CUTOFF_TIME** is `tracking_start_at + window_seconds`, an on-chain event-time boundary. Events at the exact boundary are included by the current feature builder. It is not the time the feature was known locally.
- **FEATURE_AVAILABLE_TIME** is the first real wall-clock instant when all required canonical events had arrived, an immutable feature version had been materialized, and its required contiguous coverage/recovery proof existed. A hypothetical prediction at time T may use the feature only when `feature_available_time <= T` **and** the proof existed by T. Recovery after T cannot make a feature available retroactively.
- **FEATURE_FINALIZED_TIME** in the current `flow_features.finalized_at` column is the *latest stored rebuild time*, not the first version's availability. Normal `FlowWorker.finalize` passes `now-3` to `FlowDB.rebuild`; the resulting `finalized_at` is therefore approximately three seconds earlier than actual materialization. Rebuilds overwrite it. Neither it nor today's `coverage_quality='complete'` proves exact-cutoff availability.

`flow_events.event_time` comes from a verified block timestamp when available; `observed_at` records first local ingestion. The upsert can revise phase, direction, payload, block and removed state without updating `observed_at` or retaining an old version. Thus `max(observed_at)` for currently retained events is only a **lower bound** on the first time today's event set could have been present. A removed/corrected event may have changed the historical feature without a timestamped revision. `flow_bootstrap.completed_at` timestamps one bootstrap proof; it does not timestamp the first complete proof for every window. Recovery cursors, gap resolution, target coverage and feature quality are mutable without an immutable first-proof timestamp. The audit reports historical first raw-set arrival, first complete proof, and first feature availability as **UNKNOWN** where they cannot be established. A known late event or bootstrap completion yields a positive minimum delay and a definite exact-cutoff failure, but absence of such evidence never becomes a pass.

Under the deployed split worker, normal first materialization cannot happen at the exact cutoff because of the three-second confirmation buffer. A practical future prediction time would be after cutoff, event ingestion, and bounded completeness proof; the precise time must be recorded prospectively. No Phase 2B collection behavior was changed for this audit.

## Candidate classification

All six historical candidates are **`RECONSTRUCTABLE_ONLY`** with the current schema. They are event-time bounded and could become `POINT_IN_TIME_SAFE_AFTER_PROOF` in a prospective immutable availability ledger, but today's persisted rows do not prove their first version/proof time. None is a verified exact-cutoff model feature. No candidate is labeled `POINT_IN_TIME_SAFE` merely because its present row is complete.

| Candidate | Source and observation semantics | Recovery and proof requirement |
|---|---|---|
| `curve_buy_count`, `curve_sell_count` | Non-removed decoded curve buy/sell events; block `event_time <= cutoff`; first local ingestion `observed_at`. | Late getLogs or correction can revise counts. Require a canonical event-version snapshot and curve range proof no later than proposed prediction time. |
| `total_directional_event_count` | Curve buys/sells plus V4 core swaps in the window; hook fee events excluded. Same event/ingestion clocks. | Require curve proof, and V4 proof if graduation occurred by cutoff. A later graduation cannot enter the predictor. |
| `v4_core_swap_count` | Non-removed V4 core swap events, not hook fees or independent economic trades. Same clocks. | Require verified graduation state at cutoff, V4 filter proof, and no unresolved range. A zero count without proof is not evidence of zero swaps. |
| `unique_buy_recipients` | Distinct recipient addresses on curve buy events observed within the window. | Late/corrected recipient identity changes the set; require the same event-version and curve completeness proof. Recipient is not an economic actor. |
| `top1_buy_recipient_token_share` | Largest curve-buy recipient's bought-token amount divided by total bought-token amount; NULL if denominator is zero. | Any late/corrected buy or amount changes numerator/denominator; require complete curve proof and immutable input version. It is not holder concentration. |

The actual source decoder reads curve caller and recipient from different indexed topics. V4 swap sender is a separate role. `economic_actor` is written as NULL; contracts or intermediaries may be recipients, and this audit does not classify address code or promote recipient to trader. No raw addresses are exported.

## Audit snapshot and era-separated findings

At fixed maturity cutoff **2026-09-29 03:43:00 UTC**, the split era has **78** tracked launches (78 initial, 29 long). Its feature quality is:

| Window | Complete | Partial | Unavailable |
|---|---:|---:|---:|
| 30s | 76 | 1 | 1 |
| 60s | 76 | 1 | 1 |
| 5m | 76 | 1 | 1 |
| 15m | 75 | 1 | 1 |
| 1h | 26 | 1 | 1 |

For every listed cutoff→6h pair, 25 split-era labels were due, 23 windows were complete, and 23 rows passed the conservative descriptive filter. For every cutoff→24h pair, **7** labels were due and all 7 yielded descriptive rows. The exact-cutoff point-in-time-safe and final model-usable counts are **zero for every pair**, because first feature/proof availability is not durably recorded and the split worker buffers past cutoff. The [machine summary](../data/phase2c-point-in-time-summary-20260929.json) reports every valid pair's mature, verified-label, complete-feature, descriptive, and point-in-time-safe counts separately for legacy, transition, and split eras. The 60s→24h split descriptive rows span only **0.355 days** of launch time.

The legacy 60s→6h descriptive sample has **217** rows (216 curve, 1 V4 at cutoff); split has **23** (all curve). Legacy 60s→24h has **221** (220 curve, 1 V4); split has **7** (all curve). Legacy curve-buy Spearman rho is **−0.676** at 6h and **−0.736** at 24h; total directional-event rho is **−0.689** and **−0.744**. Top-one recipient share rho is **+0.481** on 121 legacy 6h rows and **+0.532** on 123 legacy 24h rows. These are *legacy-era descriptive associations*, affected by activity-dependent missingness. Split N is below 30, so the audit reports feature/label distributions and `tiny_sample_no_correlation`, not a rho or bootstrap interval. No V4 signal claim is possible with N=0–1. Pooled associations are **NOT VALID FOR INFERENCE** and are not emitted by default.

The legacy 60s→6h rho changes materially under prespecified descriptive subsets: curve-buy rho **−0.676** on all 217 rows, **−0.479** on 121 nonzero-directional rows, **−0.513** on 122 rows whose stored Decimal prices differ, **−0.106** in 113 rows with at most one current raw event, and **−0.402** in 104 rows with two or more. Long-cohort restriction changes nothing because all 6h rows are long-cohort. The old route had higher missingness in active launches, so even these subsets are selected. Their differences are diagnostic, not causal evidence or threshold choices.

For the 6h label, **exact equality of stored Decimal prices** occurs in **95/217** legacy rows and **12/23** split rows; every such pair also has identical sampled curve quote/token reserves. For the 24h label, exact equality occurs in **94/221** legacy rows and **5/7** split rows, again with identical sampled reserves. Float conversion rounds a further 7 legacy and 1 split 6h ratios, and 8 legacy 24h ratios, to `1.0`; these are **not** counted as exactly unchanged. The implementation obtains fresh `getReserves`/V4 state and stores a verified marginal price; it has no carry-forward `1.0` fallback convention. Equal sampled reserves show unchanged sampled state, **not proof of no trades between snapshots**. The flow collector expires earlier than a 6h/24h label, so its absence of later events cannot settle that question.

## Fixed modeling-readiness policy

This policy is defined now, before future clean outcomes. Require at least **600** split-era rows with durably proved feature availability by the *proposed prediction time*, a launch span of **60 days**, and a chronological 60/20/20 train/validation/holdout allocation (at least **120** holdout rows). Each slice must contain at least **20** below-one and **20** above-one outcomes; report exact-one separately. This is only a minimum data gate, not permission to train or trade. Current point-in-time-safe N is **0** and clean descriptive 60s→24h N is **7**, so the modeling data are **`MODEL_DATA_NOT_MATURE`**. A credible date-to-threshold cannot be estimated from roughly 34 hours of split-era observation, right-censored 24h labels, and no availability ledger. The earliest possible 60-day span alone places the threshold more than eight weeks from split start if collection continues; this is not a forecast.

The next evidence improvement is an immutable, timestamped feature-version and complete-range proof ledger. It must record event-version identity, actual materialization wall clock, proof completion wall clock, cutoff, required filter kinds and prediction time, without rewriting prior versions. Only then can historical or prospective rows be marked `POINT_IN_TIME_SAFE_AFTER_PROOF` at a specified later T. This design note does not authorize altering the running collector.

Example audit (isolated Ubuntu source copy, aggregate output outside production DB paths):

```sh
python -m scripts.phase2c_point_in_time_audit \
  --main-db /opt/meme-scanner/data/scanner.db \
  --flow-db /opt/meme-scanner/data/flow.db \
  --as-of 2026-09-29T03:43:00+00:00 \
  --output /tmp/phase2c-point-in-time-summary-20260929.json
```
