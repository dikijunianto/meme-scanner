# Phase 2B operation

Separate `meme-scanner-flow.service`, user `ubuntu`, no inbound port. The existing
scanner and Phase 2A are unchanged. Flow opens the authoritative scanner SQLite
database with `mode=ro`; its own WAL database is `data/flow.db`. Short transactions
and a 2-second busy timeout avoid holding the main writer. No additional dependency.

Current release status is in [the implementation report](phase2b-implementation.md).
Do not enable while its security incident remains unresolved.

## Configuration and deployment

Copy `config/flow.env.example` to `config/flow.env`, permission 0600. Credentials
remain exclusively in protected `config/.env`; never copy them into service units,
command lines, reports or Git. Private production HTTP/WSS validation reuses Config.

| Setting | Default | Meaning |
|---|---:|---|
| FLOW_TRACKING_ENABLED | false | Disabled entry point does not open DB or RPC |
| FLOW_DATABASE | /opt/meme-scanner/data/flow.db | Separate persistent SQLite state |
| FLOW_FEATURE_WINDOWS | 30,60,300,900,3600 | Fixed launch-anchored windows; 3600 only long cohort |
| FLOW_MAX_ACTIVE_SUBSCRIPTIONS | 64 | All curve, manager and hook subscriptions combined |
| FLOW_MAX_HTTP_CALLS_PER_DAY | 1000 | RPC **members/attempts**, including retries and batch members |
| FLOW_MAX_HTTP_CALLS_PER_MINUTE | 12 | Persisted UTC-minute member limit |
| FLOW_MAX_RECOVERY_GETLOGS_PER_DAY | 400 | Persisted UTC-day targeted recovery attempts |
| Normal recovery replay | 10 blocks/query, 100 blocks/filter | Per-filter committed cursors; larger historical catch-up uses the separate [shadow-first migration](phase2b2-provider-migration.md) while the old flow service runs |
| FLOW_SECONDARY_WS_BYTES_PER_DAY | 64000000 | Persisted PublicNode/Validation received-message bytes, resets at UTC midnight |
| FLOW_TX_ENRICHMENT_ENABLED | false | Enabling is rejected in this release |

HTTP uses the existing bounded exponential jitter/backoff implementation: three
attempts, 0.5 requests/second; the shared configured backoff ceiling remains 60s.
Subscriptions are paced at at most ten commands/second. Limits apply independently
of the base scanner. WS bytes may exceed the threshold by the first message that
crosses it; collection closes, records partial coverage and pauses until the next
UTC day. An HTTP pause does not disable established log subscriptions. No paid upgrade.

Resource protection: flow MemoryMax=128MiB, CPUQuota=20%, Nice=10. The flow loop
pauses at 350MiB combined measured cgroup memory or less than 2GB free disk.
These are proactive guards, not a shared 400MiB kernel cgroup cap. Existing main
service settings remain untouched. Any observed combined 400MB breach is a stop gate.

Fresh installation, after reviewing the current release gate:

```sh
cd /opt/meme-scanner
install -m 600 config/flow.env.example config/flow.env
.venv/bin/python scripts/init_flow.py
.venv/bin/python -m unittest discover -s tests -q
.venv/bin/python scripts/phase2b_preflight.py
.venv/bin/python -m app.flow_worker  # disabled; no RPC
.venv/bin/python scripts/flow_cost_model.py
sudo install -m 644 deploy/meme-scanner-flow.service /etc/systemd/system/
sudo systemctl daemon-reload
```

`init_flow.py` uses SQLite's online backup API, never copies a live WAL DB. It backs
up main and any existing flow DB to a new directory under `data/`, checks both
backups, then creates additive flow schema. Backup directory 0700; files 0600.
It never calls main migrations. Running it again makes another consistent backup;
no existing data is rewritten. Schema version must be 1 before service startup.

After the current gate is cleared, set only `FLOW_TRACKING_ENABLED=true` in the
protected flow file and enable/start only `meme-scanner-flow`. Rollback is stop and
disable that service, set flag false, retain the DB and backup. Do not restore an
old main backup over a healthy live scanner; that would discard newer observations.

## Event time, state and recovery

Authoritative persisted `random_initial` and `random_long` decisions determine
eligibility. No new hash sampling or outcome-based extension. Initial-only target
ends at 900 seconds; long target at 3600. Events outside these times are excluded.
Expiry/unsubscribe has a short processing grace; that does not extend event windows.
Targets survive restart; only still-active launches are bootstrapped.

The main WS launch timestamp is an observation timestamp. Flow fetches the unique
launch block once to anchor T0 to chain time. Three live Alchemy log timestamps
were verified against headers. Therefore normal trades use `log.blockTimestamp`
without HTTP. Full newHeads was measured at ~10 headers/s and rejected on cost.
Unique recovered headers missing from the process cache use bounded HTTP. A live
missing timestamp is explicitly `observed_at`, invalidating coverage for that target.
There is no healthy periodic getLogs loop or transaction/receipt lookup.

Curve filters are address + CurveBuy/CurveSell topics. Graduation derives the pool
key from main's verified graduation record and validates its PoolId. V4 manager
and hook subscriptions each filter the exact PoolId. The worker subscribes to the
new phase before unsubscribing the old phase and recovers the bounded boundary.
Curve positions before graduation are accepted; V4 positions after it are accepted.
Unresolved ordering/recovery is partial. A longer disconnection remains partial
even when the latest bounded suffix was successfully recovered.

## Storage and interpretation

`flow_tracking_targets`, `flow_events`, `flow_gaps`, `flow_features`, `flow_usage`,
`flow_samples`, and `flow_state` live in the flow DB. Shared indexed raw columns
carry event identity, phase, block/time, removal state and distinct address roles.
Typed JSON payloads preserve raw integer strings, Decimal-normalized amounts,
source names and semantics. SQL views `curve_trade_events`, `v4_swap_events`,
`v4_hook_fee_events` expose those phases. This avoids three duplicated persistence
paths while retaining explicit economic meanings. JSON is queryable with SQLite
`json_extract(payload,'$.pricing_quote_amount_raw')`, for example.

Raw key: `(chain_id, tx_hash, log_index)`. Duplicate recovery does not add rows.
Removed logs/reorgs invalidate coverage and recompute derived features; raw evidence
is retained. A durable earliest-changed-event marker survives interrupted feature
recomputation. Indexed role history uses only other tracked launches with event time
strictly before each feature cutoff, never present-day lifetime totals or outcomes.

Curve buy quote is gross **spent**, fee/tax included, refund excluded. Pricing input
subtracts fee and tax. Curve sell event quote is net; gross priced output adds them.
Base fee is not labeled exclusively protocol revenue. Refund is unknown without
additional evidence. V4 signed core deltas precede afterSwap; hook charges remain
separate by currency. No beneficiary-net reconstruction or forced swap/fee pairing.
`caller_address`, `recipient_address`, `swap_sender` never become an inferred user;
transaction_from and economic_actor remain NULL. Hook self-call sender is explicitly
classified; other swap origins remain unknown.

Features retain separate phase quote legs, explicit roles, quantiles, timing,
creator roles, and recipient acquisition concentration (not holder concentration).
Only direction counts are combined across curve and V4. All closed windows include
events at T0 and at the exact cutoff. Missing coverage with no events produces NULL
metrics; observed partial windows retain observed values with partial quality.
Complete zero-activity windows remain true zeros.

## Reports and retention

```sh
.venv/bin/python scripts/flow_usage_report.py --hours 24
.venv/bin/python scripts/inspect_flow_history.py TOKEN_ADDRESS
.venv/bin/python scripts/rebuild_flow_features.py --token TOKEN_ADDRESS
.venv/bin/python scripts/rebuild_flow_features.py --days 7
.venv/bin/python scripts/flow_outcome_report.py --days 7 --feature-window 300 --outcome-horizon 3600 --ticker NVDA
.venv/bin/python scripts/flow_outcome_report.py --days 7 --feature-window 300 --outcome-horizon 3600 --quote-address ADDRESS
.venv/bin/python scripts/benchmark_flow.py --minutes 30 --output /tmp/flow-benchmark.json
```

Inspection, usage and outcome analysis are read-only and perform no RPC. Rebuild
only updates derived rows. Outcome reports require feature window < outcome horizon,
complete coverage, a sampled due horizon and finite positive verified prices in
the same quote asset. Post-as-of snapshots are excluded. Groups expose N and
feature-specific nonmissing N; quote-native amounts are never pooled across assets.
Missingness is an explicitly ordered, mutually exclusive first-failure denominator,
with an additional `outcome_unsampled` category for initial-only targets at long
horizons. Ratios are `marginal_price_multiple`, not trader ROI.

No automatic raw deletion. Flow usage measures allocated DB+WAL growth; early WAL
allocation and short bursts can strongly inflate linear projections. Report those
alongside logical page bytes and budget-constrained scenarios. None is provider
billing. Review disk/cost/coverage before continued operation; propose export or
retention separately if needed, never silently delete research data.
