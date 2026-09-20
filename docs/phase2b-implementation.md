# Phase 2B Implementation V2 — evidence report

Final recommendation: **NOT_READY**. Collection, migration, reports and the live
benchmark are implemented. A security gate remains: the first deployed flow entry
point left HTTPX INFO logging enabled, which wrote credential-bearing RPC URLs to
the VPS journal. Flow was stopped and disabled when this was discovered. HTTPX and
HTTPcore logging are now restricted; WebSocket library logging follows the base
scanner's CRITICAL setting. A real HTTPX MockTransport regression test proves the
credential-shaped URL is absent from captured logs. **The shared Alchemy key must
be rotated before re-enabling flow.** No credential or journal extract is included
in this report or Git. Existing journal records were not destructively removed.

Coordinate replacement of the shared credential with the base scanner before
revoking the old key, so rotation does not interrupt ingestion. Flow is currently
`FLOW_TRACKING_ENABLED=false`, systemd inactive and disabled. Main scanner remains
active. No paid plan, widened sampling or increased RPC concurrency was introduced.

Evidence as of **2026-09-20 12:51 UTC**. Machine-readable curated evidence:
[phase2b-implementation-evidence.json](phase2b-implementation-evidence.json).
Configuration, storage, commands, rollback and restart procedure:
[phase2b-flow-operations.md](phase2b-flow-operations.md).

## Revision and changes

Preflight baseline: `68ce59b215ecc8c99b9da16153f1d1d81aa0d1e4`, main branch,
initially clean. The implementation revision is the Git commit containing this
report; the exact resulting hash is supplied in the delivery message.

New production modules: `app/flow_data.py`, `app/flow_worker.py`,
`app/flow_reports.py`. Added seven operator scripts: initialization, cost model,
bounded benchmark, usage, event inspection, derived rebuild and outcome analysis
(no separate daemon wrapper). Added `config/flow.env.example`,
`deploy/meme-scanner-flow.service`, `tests/test_flow.py`, operation documentation
and this curated evidence report. Updated README and ignored the protected flow
environment file. Existing scanner, database, outcome, RPC and market modules
were not edited. No dependency was added.

## Safety, migration and tests

**LIVE VERIFIED:** Before production changes, main PID 842555, NRestarts 0,
SQLite integrity `ok`, WAL mode, approximately 106MB cgroup memory and 33GB free
disk. Existing tests passed before implementation: **53/53**. The committed
offline Preflight B continued to return **READY**.

**LIVE VERIFIED:** SQLite online backup created before flow schema initialization:

`/opt/meme-scanner/data/phase2b-backup-20260920T082841Z/main.db`

Backup directory 0700, file 0600. Backup integrity `ok`. Migration created only
the independent `data/flow.db` schema; main remained read-only. Both databases
returned `integrity_check=ok`, `journal_mode=wal` afterward. Schema initialization
is idempotent and refuses a flow path equal to the main DB path.

**TEST VERIFIED:** Final suite **91 passed**: all 53 original tests and 38 new
tests. Coverage includes real curve/V4/hook fixtures, both currency orderings,
invalid signs/emitter/pool, fee legs, distinct identities, persisted cohorts,
fixed expiry, restart subscriptions, graduation subscription ordering, exact
cutoffs, late events, causal role history, duplicate/removal/reorg handling,
partial versus zero, adaptive bounded recovery, unique-header fallback, separate
budgets including retries, protected logs, read-only reports, finite prices,
registry filtering, SQLite WAL backup and additive preservation.

Stage A initially found a missing already-committed preflight helper on the VPS;
it was deployed and the complete suite rerun before migration/enabling. No test
failure was bypassed. Stage A disabled execution produced zero flow usage rows and
no network calls; main PID and restart count remained unchanged.

**LIVE VERIFIED:** Final comparison against the consistent backup:

| Table | Backup rows | Final rows | Missing original IDs | Changed original rows |
|---|---:|---:|---:|---:|
| launches | 193,867 | 195,454 | 0 | 0 |
| graduations | 2,497 | 2,507 | 0 | 0 |
| market_snapshots | 12,593 | 12,644 | 0 | 0 |
| outcome_targets | 40,613 | 40,837 | 0 | Not compared: normal queue updates continue |

Main never restarted during this work; final PID 842555, NRestarts 0. Additional
launches, graduations and snapshots demonstrate continued Phase 1/2A progress.

## Architecture, semantics and coverage

Separate lightweight asyncio/systemd service isolates Phase 2B failures and budget
pauses. Main DB is opened read-only. Flow events, targets, gaps, features and usage
are in the separate WAL DB. This avoids adding another writer to the main DB.

Persisted `random_initial` and `random_long` flags are authoritative. Initial-only
tracking is T0–900s; long tracking T0–3600s. No price/activity/graduation-based
extension. Feature windows: 30/60/300/900s and 3600s only for long targets. All
inclusion is launch time <= event time <= cutoff. Phase 2B coverage began
**2026-09-20 08:30:04.005432 UTC**. No days of historical flow were backfilled.

**LIVE VERIFIED:** All 8,560 stored event timestamps used Alchemy's
`blockTimestamp`. Three pre-enable log timestamps exactly matched unique fetched
headers and hashes. A newHeads probe measured 120 headers / 200,290 bytes in
12.041s: roughly 1.44GB/day and 57.5M CU/day at the published byte rate. Therefore
the preferred full-head feed was not enabled. Launch headers are fetched once per
new sampled launch; recovered missing timestamps use cached unique headers.
Normal event processing performs no HTTP transaction/receipt/header lookup.

**BEHAVIORALLY VERIFIED / TEST VERIFIED:** Committed Preflight B semantics were
retained without new broad protocol research. Curve buy event quote is gross
actually spent, including base fee/tax and excluding refund; pricing input is
event quote minus fee and tax. Curve sell quote is net output; gross priced quote
adds fee and tax. Fee pot is not labeled exclusively protocol revenue.

V4 launched-token delta positive/opposite quote negative means buy direction;
opposite signs mean sell direction. Zero/same signs are rejected. Core deltas are
explicitly pre-afterSwap; fee/tax hook evidence remains separate by currency.
No invented final user amount, economic actor or swap/hook correlation.
Curve caller/recipient and V4 swap sender are stored independently. Transaction
enrichment is disabled, transaction_from/economic_actor NULL throughout the live
dataset. Token normalization uses the verified Pons 18-decimal launch-token model;
quote decimals are cached from authoritative metadata or a bounded metadata call.

**LIVE VERIFIED:** Stored-event arithmetic, V4 signs, NULL identities, fixed
tracking duration, persisted cohort membership and event-window inclusion all
passed local checks. Rebuilding all stored features reproduced every existing
coverage reason/quality/metric exactly; raw event count remained unchanged.

## Cost model and enforced limits

**LIVE VERIFIED input, INFERRED projection:** Pre-enable 24h production cohort:
1,503 stock launches, 147 initial samples, 79 long samples. Long is counted once,
not added again to initial. Average active targets 4.0; estimated curve 3.990 and
V4 0.010. Historical peak active targets 16; worst two subscriptions each gives
32, below configured cap 64. Expected average subscriptions approximately 4.01.

Daily flow limits: 1,000 RPC members/attempts, 400 getLogs attempts, 8,000,000 WS
bytes; minute limit 12 members; HTTP pace 0.5/s; at most 100 recovery blocks per
filter in 10-block ranges reduced on rejection. No transaction enrichment.

Using [Alchemy's published CU costs](https://www.alchemy.com/docs/reference/compute-unit-costs),
base usage extrapolated to 15.58M CU/month and capped flow allowance to 11.40M,
combined **26.98M CU/month** against the free 30M allowance. These are local
estimates, not billing or remaining quota. Failed attempts are counted before IO.
WS subscribe/unsubscribe/control overhead and changing base activity add uncertainty.

**LIVE VERIFIED:** Later V4 activity exhausted the separate byte budget at
12:21:58 UTC. Received bytes were 8,000,636: the boundary-crossing message caused
a 636-byte overshoot. Flow disconnected and entered `paused_ws_budget`, while
base ingestion and Phase 2A continued. No retry/concurrency escalation occurred.
Continuous full-window coverage at this observed activity cannot be promised on
the selected free-tier allowance. Partial coverage is retained and excluded from
the default descriptive join; cohort decisions themselves are never changed.

## Required 30-minute benchmark

**LIVE VERIFIED:** 08:31:13.893821–09:01:13.908678 UTC, **1,800.015 seconds**, 61
resource samples. This was the original deployed collector, before the logging
fix. The security defect prevents labeling the overall release ready.

| Measurement | Result |
|---|---:|
| Sampled targets encountered | 1 initial, also long |
| Average / peak active targets | 0.20 / 1 |
| Average / peak subscriptions | 0.20 / 1 |
| Curve / V4 / hook subscription peaks | 1 / 0 / 0 |
| Curve buy / sell events | 32 / 36 |
| V4 swaps / hook events | 0 / 0 |
| Raw rows / duplicates / removed logs | 68 / 4 / 0 |
| Complete 30s / 60s / 300s features | 1 / 1 / 1 |
| 900s / 3600s features | Not due during benchmark |
| Partial windows | 0 |
| HTTP envelopes / members | 9 / 9 |
| getLogs / header / metadata eth_call / blockNumber | 6 / 1 / 1 / 1 |
| Transaction / receipt lookups | 0 / 0 |
| WS bytes during benchmark | 59,462 |
| Reconnects / service restarts | 0 / 0 |
| Peak combined RSS | 110,510,080 bytes |
| Peak combined cgroup memory | 147,808,256 bytes |
| Mean main / flow CPU, one-core percentage | 0.047% / 2.260% |
| Allocated flow DB+WAL, first / last sample | 531,008 / 4,341,240 bytes |

HTTP calls were target activation/metadata recovery, not per-event polling.
Main added 146 launches, one graduation, three snapshots and 28 outcome targets
during the benchmark. Both services stayed active with unchanged PIDs.

The quiet benchmark extrapolates to ~3,264 raw events/day and 2.85MB WS/day, but
the subsequent V4 burst invalidated any assumption that this short rate was stable.
Both estimates are **INFERRED**, not day-long measurements.

## Subsequent live evidence and final stored state

Collection continued until the byte budget paused it; later inspection found the
logging problem and disabled the service. This additional evidence is separate
from the bounded benchmark above.

**LIVE VERIFIED:** 9 initial targets, 6 also long; 8,560 raw rows: **415 curve buys,
325 curve sells, 3,925 V4 swaps, 3,895 hook-fee events**. Thirteen duplicates, zero
removed logs. Average sampled active targets 1.353; peak 4. Average subscriptions
1.408; peak 4, with curve peak 4, V4 peak 1 and hook peak 1.

Final feature rows: complete 30s=9, 60s=9, 300s=9, 900s=7, 3600s=4;
partial 900s=2, 3600s=1. Total **38 complete / 3 partial**. One target's scheduled
hour had not closed at the final evidence snapshot. Persisted target state can
still say active after an operator stop; systemd inactive/disabled is authoritative
for whether collection is running.

**LIVE VERIFIED:** A sampled curve-to-V4 transition was captured for
`0x094dFC1425EeD5e58D459122967098B8139D44c9`, launch 195021. Graduation block
67,926,200 / log 62, PoolId
`0xc1630e2386a682bb56eb7493f13a65bd8418f7c82198ec09bf38b4d678019085`.
Currency0 is the launched token, currency1 its verified quote. Swaps and hook
evidence were received under the exact manager/hook + PoolId subscriptions.
Its later window became partial when the WS budget paused collection.

Total HTTP: 78 envelopes/members = 54 getLogs, 9 unique launch headers, 5 quote
metadata calls, 10 blockNumber calls. Transaction and receipt lookups stayed zero.
The latest pre-stop combined cgroup memory observation was ~197.7MB, still below
300MB. Peak memory outside the 30-minute benchmark was **NOT YET MEASURED** continuously.

## Disk and insertion performance

**LIVE VERIFIED:** Final logical flow DB size 16,818,176 bytes for 8,560 raw rows
plus targets/features/indexes/telemetry. Approximately 1,965 logical bytes per raw
row inclusive of that overhead; not a pure event payload size.

**TEST VERIFIED on VPS:** 1,000 inserts into a temporary WAL database using a real
stored curve payload took 3.168s, **315.6 rows/s**, approximately 2,306 logical
bytes/inserted row including indexes. Integrity `ok`. Production data was untouched.

**INFERRED:** Final allocated DB+WAL linear-growth report: 115.10MB/day,
0.806GB/7 days, **3.453GB/30 days**, 10.359GB/90 days. Early WAL allocation and the
short concentrated trading burst inflate this projection; it is not measured
sustained growth. With one 8MB WS allowance/day at the observed event mix, logical
growth is roughly 16.8MB/day, 0.50GB/30 days, before additional recovery and
metadata overhead. This second scenario is also inferred. Pre-enable conservative
capacity scenario was 70.536MB/day / 2.116GB per 30 days. Free disk was 32.8GB after
backup, and the service has a 2GB stop reserve. No raw retention/deletion was added.

## Report examples

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/inspect_flow_history.py 0x04Cd9cF16A2A1cBBce83279009F73EA2005E756E
.venv/bin/python scripts/flow_usage_report.py --hours 24
.venv/bin/python scripts/rebuild_flow_features.py --token 0x04Cd9cF16A2A1cBBce83279009F73EA2005E756E
.venv/bin/python scripts/flow_outcome_report.py --days 7 --feature-window 300 --outcome-horizon 3600
```

Inspector excerpt, first curve event: age 0, buy direction, gross spent
4.959435542589092759 quote units, pricing input 4.909841187163201832,
base fee 0.049594355425890927, creator tax 0, tokens
145305246.034374850749368196. Caller and recipient are explicit address fields;
the next event in the same transaction has a different recipient. Economic actor
and transaction_from remain NULL. Complete raw/payload examples and the 24h usage
command output are in the curated JSON evidence.

**LIVE VERIFIED descriptive outcome join:** five mature valid 300s-feature /
3600s-price pairs, split by quote asset: N=4 and N=1. For the N=4 quote group,
the >=2x group has N=1, <2x N=3; no claim of prediction. The N=1 second quote
group has zero >=2x observations. Supported feature distributions use per-feature
nonmissing N. Output name is `marginal_price_multiple`, pre-fee marginal market
price, never trader ROI. Registry ticker/address filters work; prices/volumes are
not pooled across stock assets.

Final mutually exclusive first-failure counts: unsampled 19,977;
feature_not_deployed_yet 2,131; feature_window_not_due 0; feature_partial 0;
feature_missing 3; outcome_unsampled 3; outcome_not_due 1; outcome_missing 0;
invalid_market_price 0; valid_pair 5. These describe the seven-day launch cohort
as of the evidence timestamp, not all stored windows. In particular partial 15m/
1h features do not disqualify already-complete 5m features.

## Limitations and release gate

**UNRESOLVED:** Rotate the shared Alchemy credential. Patched logging is
**TEST VERIFIED** but the patched service was deliberately left disabled; there
is no new live benchmark after rotation. Do not claim the release complete/ready
while this gate remains open.

**LIVE VERIFIED limitation:** a busy sampled V4 pool exhausted the daily flow WS
allowance. Continue only within the chosen free-plan limits, retaining explicit
partial/missing coverage. No full-coverage promise or paid upgrade is implied.

**NOT YET LIVE OBSERVED:** removed/reorg logs and a network reconnect requiring
recovery; these are covered by tests and Preflight fixtures. Hook-to-individual-swap
correlation and economic-user identity remain intentionally unknown. Participant
history covers only this project's sampled local events. Five valid outcome pairs
are insufficient for broad descriptive conclusions. No scoring, signals, wallet
profitability, trading functionality or trading recommendation was added.
