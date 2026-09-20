# Phase 2B preflight — NOT READY

Observed 2026-09-20 UTC. Phase 2B is **not implemented or deployed**. This commit
adds an audit command, its tests, and bounded evidence only. Production collection,
sampling, schema, and historical observations were not changed.

## Safety baseline — LIVE VERIFIED

Local starting commit: `e05be54823203ab2d1cbd30b165461c5937411dd`, branch `main`,
clean working tree. Production is a deployed file tree, not a Git checkout;
its commit cannot be verified with `git rev-parse`.

| Check | Observation |
|---|---:|
| Existing tests on VPS | 37 passed |
| SQLite integrity_check | ok |
| Journal mode | wal |
| Launches | 192114 |
| Graduations | 2488 |
| Market snapshots | 12531 |
| Outcome targets | 40433 |
| Main DB bytes | 174051328 |
| WAL bytes | 4243632 |
| SHM bytes | 32768 |
| systemd | active |
| Service memory bytes | 104034304 |
| Filesystem available | 31 GiB |

Counts are a baseline while ingestion continues, not a frozen database snapshot.
No schema migration or service restart was performed; no backup was needed for
these read-only checks. A SQLite backup remains mandatory before any migration.
The preceding 24-hour RPC report recorded 2574 HTTP envelopes, 5094 eth_call
members, 16 eth_blockNumber calls, 11383 WS events and 10767894 notification
bytes. Market telemetry recorded 1479 logical calls and 662 completed targets.
These are local telemetry, not provider billing or a new Phase 2B benchmark.

## Price classification

**Source-model interpretation: VALID as pre-fee marginal reserve price.**
**Deployed-behavior acceptance: NOT VERIFIED.** It would be misnamed if presented
as an executable purchase/sale price or net investment return. No evidence here
establishes that historical marginal-price labels are materially invalid, and
none were rewritten. This is not a completed VALID/MISNAMED/INVALID deployment
sign-off.

Official source examined:
[PonsV2BondingCurve.sol, commit 162310f](https://github.com/ponsdotdev/pons-labs/blob/162310fbd1217717e2f5e4cde794d6a11322b469/contractsV2/src/v2/PonsV2BondingCurve.sol).
The source's `getReserves`, `buy` and `sell` use a virtual-quote constant-product
model. Fee/tax balances are excluded from tradeable quote reserves; phantom quote
remains included. With reserves Q and T, the model's pre-fee marginal price is
Q/T, adjusted for quote/token decimals. It is neither physical liquidity nor a
finite-size average execution price.

Let f be the combined fee fraction, x gross buy input, y sell token input.
Ignoring integer rounding and allocation limits:

```
buy tokens = T*x*(1-f)/(Q+x*(1-f))
sell quote net = Q*y/(T+y)*(1-f)
tiny buy implied price -> (Q/T)/(1-f)
tiny sell implied price -> (Q/T)*(1-f)
```

The command follows integer fee rounding separately for protocol fee and creator
tax. Its model amounts are one millionth of the respective reserve. This is not
an execution simulation: it does not test balance/allowance, remaining sellable
allocation, physical quote availability, paused/permissioned assets, or transfers.
Token decimals=18 is the existing collector/source assumption, not independently
read by this command.

## Bounded live reads and model estimates

See [machine-readable evidence](phase2b-price-evidence.json), block **67579535**.
All state reads use that block number. The command used 8 HTTP envelopes,
15 eth_call members, 3 eth_getCode calls, one chain check and one head read.
No retries, transaction lookups, getLogs, wallet access or transactions occurred.
This command's counters are separate from production telemetry.

| Token prefix | Live base fee + tax | Model buy difference | Model sell difference |
|---|---:|---:|---:|
| 0x39062DAF | 1% + 1% | +2.040916% | -2.000098% |
| 0x8230EbEE | 1% + 0% | +1.010201% | -1.000099% |
| 0x3781F0E0 | 1% + 0% | +1.010201% | -1.000099% |

**INFERRED:** Fee-adjusted relative differences are approximately +0.000098%
to +0.000099% for buys and -0.000100% for sells. This demonstrates source-model
convergence and rounding only. It does not independently validate that model
against executed deployed buy/sell behavior.

The four common quote selectors checked were absent from the sampled runtime
byte strings; the inspected curve source has no matching quote entry points.
A selector-byte search is not a complete ABI analysis and does not rule out
another quoter contract. A preliminary read of `currentSnipeTaxBps()` also
reverted on three curves; no such fee is assumed from unrelated documentation.
Sourcify full/partial metadata lookups for curve
`0x2F31646B85148c1AA4Ace1d4086459028DF0c2c2` both returned HTTP 404.
Distinct runtime hashes are recorded; immutable constructor values can cause
different hashes, so these differences do not establish a source mismatch.

## Remaining gate and scope

Independent tiny buy/sell read-only execution quotes and deployed-source
equivalence remain unresolved. A self-consistent formula is not that evidence.
Next work must obtain verified deployment artifacts or compile/match the correct
source with immutable substitutions, and identify a read-only quoter or bounded
execution simulation. Do not enable flow collection based on this report alone.

Pons source declares indexed caller/recipient for CurveBuy and CurveSell; source
buy amounts include fees while sell output is net of fees. This is **source
research only**, not a completed on-chain event-semantic verification. Recipient
is a beneficiary, not necessarily the payer or seller. V4 swap direction, hook
effects, real trade fixtures and actor identity are still **NOT VERIFIED**.

Phase 2B migrations, cohort lifecycle, subscriptions, raw trade storage,
aggregation, reporting, cost model and 30–60 minute deployment benchmark remain
unimplemented. Therefore live flow counts, finalized/partial windows, average/
peak subscriptions, flow bandwidth, disk growth and descriptive outcomes are
**NOT YET MEASURED**, not zero. No claim of Phase 2B completion is made.

## Reproduce

From `/opt/meme-scanner`, after placing this audit script in `scripts/`:

```
.venv/bin/python scripts/curve_price_preflight.py
.venv/bin/python -m unittest discover -s tests
```

The audit samples at most three recent stock launches, reads SQLite in read-only
mode and uses the protected existing config. Requests are sequential, batches
contain at most five members, pacing is one envelope/second and attempts are
bounded to two using the existing backoff/jitter handler. It always reports
`deployment_gate=NOT_READY`; no automatic enablement exists.

**TEST VERIFIED:** 40 local tests passed (37 existing plus 3 new arithmetic/input
checks). These tests do not substitute for live quotes or event fixtures.

Recommendation: **NOT READY for Phase 2B enablement.**
