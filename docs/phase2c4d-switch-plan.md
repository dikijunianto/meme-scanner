# Canonical identity and fixed-head switch recovery planning

This phase changes source, offline tests and documentation only. It does not run
production recovery, create a production reconciliation head, or restart either
service. Source deployment does not reload the existing worker.

## Identity and historical evidence

The old planner constructed `query.address` with `.lower()` and compared the
decoded `flow_bootstrap_identity.query_json` dictionary literally. Switch17's
retained curve addresses have checksum casing. Its topics and address bytes match,
but the dictionary comparison rejected them as `FILTER_QUERY_PROVENANCE_BLOCKED`.
SQL proof lookup in offline maintenance also required literal query JSON equality.

`flow_identity.canonical_address` accepts only `0x` plus exactly40 hexadecimal
digits and derives a lowercase20-byte comparison identity. `query_identity`
copies the query, canonicalizes only its contract address, and preserves every
other query field exactly. Topics, pool IDs, transaction hashes and arbitrary
payload values are not normalized. Launch/kind/stage keys stay distinct.

Bootstrap validation, failed-switch planning and maintenance proof matching use
this shared comparison. Original switch JSON, checksum addresses, bootstrap
query JSON, existing ranges and PIT payloads/hashes are never rewritten. Newly
created recovery records may store derived identity alongside original display
addresses. Failed-switch raw payload and its SHA256 remain in failure_history.

## Compatible proof and offline estimate

Reuse requires exact launch/kind, canonical address plus unchanged other query
fields, lifecycle semantics and successful committed ranges no farther than the
source job's contiguous verified frontier. Recovery/offline proof without query
identity or lifecycle metadata is rejected. Legacy `live_bootstrap:<launch>`
curve proof additionally requires matching persisted complete activation state,
safe_start, original job start and a retained query dated before the switch
snapshot. The planner independently verifies the main/target launch address,
token, quote, block/log index and ungraduated lifecycle. WSS/cursors are not proof.

The retained metadata fixture uses **80845035** as an offline comparison endpoint
(main checkpoint2026-10-05T14:10:14Z). It is not a production H_reconcile. Results:

| Curve filter | Original full positions | Retained proof reused | Missing interval | Missing positions | Calls at max2000 |
|---|---:|---|---|---:|---:|
| 293330 | 2521748 | 78323288–78323321 | 78323322–80845035 | 2521714 | 1261 |
| 293506 | 2504344 | 78340692–78340720 | 78340721–80845035 | 2504315 | 1253 |
| Total | 5026092 | 63 positions | Two separate exact queries | 5026029 | **2514** |

Before the fix the operational planner refused both checksum-case query records;
4c's2514 estimate was diagnostic and already optimistically subtracted these63
positions. The repaired offline planner actually recognizes them. The call total
does not decrease because neither subtraction crosses a2000-block chunk boundary.
The cost comes from two independent activation-to-later-head obligations spanning
roughly2.5million blocks each. It is not evidence of a response-size failure or a
previously failed subrange: this fixture has neither.2000 is the existing explicit
shadow/bootstrap maximum; normal100-block recovery is unchanged. Provider range
rejection retains the existing halving/retry behavior and may increase actual cost.

Adjacent/overlapping compatible intervals merge only within the same filter. New
requests stop before a proved island and resume after it; committed work is not
requested again. Original proof rows stay intact; a new obligation retains source
provenance and copies only clipped compatible intervals.

At0/400 daily getLogs with reserve50, theoretical capacity is350/day.2514 calls
therefore classify **MULTI_DAY_REQUIRED**, requiring at least**8 UTC budget days**.
The current counter is consulted again on every invocation and atomically before
every send; worker usage, minute limits, header RPC, retries, range reductions and
future first-head growth can extend that duration. The estimate is a lower bound,
not a promise of completion in8 days or a provider head captured today.

## Durable fixed-head manifest and budget slices

Use the existing switch JSON `conservative_recovery` plus existing shadow
job/range/identity/meta tables; no new schema. On a future authorized invocation,
validate inputs/budget before RPC. Chain identity is checked, one head is sampled
and pinned as `conservative_head_capture` immediately, then its matching header is
validated. A header interruption resumes that same head, without another head
sample. Original `frozen_head` remainsNULL throughout.

The manifest retains switch ID/method, created/captured times, head/block UTC,
original display addresses, derived canonical queries, trusted lower bounds,
filter/lifecycle obligations and reusable source proof. Progress stores completed
and pending ranges, recovered event and duplicate totals, daily attempts with
current before/after counters, budget-stop reasons, and completed_at. Job/range
tables are authoritative after a crash; a resume reconstructs manifest progress
from them. No new stage/generation or newer head is created on another UTC day.

Insufficient current capacity returns **RECOVERY_BUDGET_PAUSED**, with zero RPC
if there is not capacity for the initial head/proof work or the next proof call.
Every successful range remains durable. A shared limiter rejection is nonterminal
for the recovery attempt; the switch's original historical FAILED state remains
until all obligations are verified. The next invocation uses current actual
counters rather than assuming a reset or assigning the full350 calls to recovery.
Minute waits may require another invocation within the same day.

Closure revalidates exact jobs, pinned query/lifecycle identities, contiguous full
proof, canonical ingestion/dedupe ordering, gap association, matching fresh live
transport and unchanged service identity. Only then can the switch enter HEALTHY.
No runtime state is manually set. It cannot close from COMPLETE flags alone.

## Concurrency and PIT boundaries

**LIVE_RECOVERY_SAFE** for this narrowly supported latest FAILED, ungraduated
historical curve snapshot, subject to the existing guards. The failed-switch gate
suppresses normal worker proof/finalization of the blocked obligation; operator
work has its own stage and never reactivates expired targets or promotes their
cursors. Both worker and operator reserve budget under SQLite BEGIN IMMEDIATE.
Before each operator HTTP send, the tool rechecks matching transport, unchanged
service/filter identity and the same pinned manifest/head. The CLI uses native
Linux flock to refuse a second operator invocation. Disconnect, service change,
new switch or changed lifecycle refuses continuation; this is not a generic
concurrent handoff system. No stop is required or performed in this source task.

Recovery cannot restore lost contemporaneous PIT history. The source does not
rebuild features during historical switch proof. Existing immutable rows and
eligibility clocks remain unchanged. The prior incident exclusion remains active;
original clean boundary78006186/2026-10-02T06:09:03Z is retained. Incident start is
2026-10-02T15:32:50.699403Z, literal start blockunknown, anchor78340786. Recovery
end remainsNULL until fresh normal PIT production is observed; full proof or a
HEALTHY switch alone is not that observation. Existing process predates95037d1,
so source deployment alone does not activate new runtime budget-wait semantics.

The worker now records `runtime_healthy_at` separately. Only the durable append
of an eligible window whose coverage begins after that healthy boundary records
`PIT_COLLECTION_RECOVERY_END` and its `first_fresh_pit` identity/hash/actual clock.
Windows with earlier incident coverage stay ineligible even after that append;
existing retained incident-end records are read compatibly, never rewritten.

## Offline verification

Run `python scripts/run_offline_tests.py` in an isolated Ubuntu checkout. Network
denial remains installed before discovery. Tests cover checksum equality/malformed
inputs, original query/payload hashes, mismatch refusal, exact retained switch17
recalculation, proof islands, adaptive reduction, daily reserve pauses, zero-RPC
days, multiple resets, simulated operator reconstruction, fixed head despite chain
growth/header interruption, worker budget consumption, complete proof before
closure, canonical event dedupe and no incident-period PIT fabrication. Existing
switch13 exact-head and switch14 zero-filter regressions remain in the full suite.
