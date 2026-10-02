# Offline flow maintenance drain

Phase 2C.3g delivers source, deterministic tests and documentation only. It does
not authorize an operational stop, restart, drain, migration or provider change.

## Deliberate operator command

A later authorized maintenance task must record the running flow PID externally,
stop **only** `meme-scanner-flow.service`, and confirm that PID is gone. Main must
stay active. Run the tool as the `ubuntu` service user, from the clean deployed
checkout, supplying its full Git revision and one stable generation ID:

```sh
.venv/bin/python scripts/flow_offline_drain.py \
  --acknowledge-flow-offline \
  --expected-revision FULL_40_CHARACTER_DEPLOYED_HEAD \
  --source-flow-pid EXTERNALLY_RECORDED_OLD_PID \
  --generation operator-chosen-maintenance-id \
  --dry-run
```

Remove `--dry-run` only in that later authorized operational task. The tool never
starts, stops or restarts a service. The legacy `flow_bootstrap_missing_cursors.py`
commands keep their Alchemy/live-worker restrictions; they never switch modes.

## Offline contract and concurrency

The command requires main active with a positive PID, flow inactive/dead with
MainPID zero, and the externally recorded old flow PID absent from `/proc`.
Transient/failed service states are rejected. It pins both units' PID/restart/state
identities and rechecks before HTTP attempts (including retries after pacing),
after responses, before DB statements and before commits. An unexpected start
aborts without further requests or commits; any uncommitted transaction rolls
back. Already durable ranges remain valid. It does not stop a newly started unit.

The operator and new worker share an exclusive, nonblocking Linux `flock` on
`data/flow.db.offline-drain.lock`. The new worker takes it **before opening its DB**;
it cannot mutate during maintenance, including a start between service checks.
The old running worker predates this lock and must be externally stopped and
verified absent. The lock file lives in the service's existing writable data
folder, not its private `/tmp`. Maintenance must run as ubuntu so lock ownership
matches the worker. Two maintenance commands cannot own the writer simultaneously.

Before reconciliation: exact clean expected source; main/flow DB integrity; no
non-HEALTHY switch record; zero flow tx/receipt counters; config0600/parent0700,
no alternate protected config paths, ubuntu readability and offline split parse;
unchanged configured ceilings; current RPC/minute/getLogs budgets. Chain ID4663
is checked using Validation HTTP before any generation, target, cursor, gap or
proof mutation. Preflight HTTP attempt accounting is charged durably even if the
chain check fails; it is not completeness proof. No implicit DB migration occurs.

## Routing, budgets and proof

Split routing is required: PublicNode primary WSS, Validation fallback WSS,
Validation recovery HTTP. The command never opens a WSS connection. Its RPC class
allows only chain ID, head/header and getLogs, routes exclusively to Validation
and has no HTTP fallback. Transaction/receipt lookup, Alchemy, PublicNode HTTP
and provider/config changes are forbidden.

The existing shadow reconciler supplies canonical parsing, event identity/dedupe,
zero-log proof, adaptive ranges initially2000 blocks, retries and durable completed
range/job progress. HTTP pacing remains0.5 envelopes/sec; RPC members1000/day and
12/minute; getLogs400/day. The shared attempt counter rejects sends beyond350
getLogs/day, preserving50 calls. `OFFLINE_DRAIN_BUDGET_PENDING` means continue the
same generation after the UTC reset. Interrupted ranges resume at the first
unverified block. Retry attempts and adaptive reductions consume the same budget.

A generation snapshots active nonterminal, unexpired flow targets, lifecycle,
expiry, query identities, safe starts, bootstrap states, cursors and unresolved
gaps in one read transaction. New main launches do not become flow targets while
the flow worker is offline. A changed target identity or new active flow target
aborts. Expired targets are skipped, not reactivated, rebuilt or made eligible.
Expiry during a request/promotion aborts conservatively; rerun excludes the expired
obligation and retains any previously committed proof.

Curve safe start is its launch/activation block and log position. V4/hook begin
at the proven graduation block; canonical parsing excludes positions at/before
the graduation log. Curve proof ends at the graduation boundary, excluding its
at/after positions. No query begins before activation.

Reuse requires exact query identity, launch/log-position and graduation digest,
durable completed ranges and compatible contiguous interval coverage. Compatible
maintenance ranges from earlier/interrupted generations are reused. The tool
searches the existing bootstrap/shadow/recovery/switch ledger; legacy evidence
without durable semantic identity is rejected rather than inferred from current
state. WSS observations alone never prove completeness. Reused intervals retain
source-stage provenance. An unresolved gap causes proof to begin at its exact
first block, bounded by the relevant filter's activation.

For a null cursor or incomplete bootstrap, contiguous activation..H_drain proof
is mandatory. Partial chunks never create a runtime cursor. Promotion verifies
all complete jobs and exact filter identity, records bootstrap completion, then
advances cursors, then reconciles satisfied gaps. Existing complete cursors use
cursor+1..head tails unless an earlier unresolved gap requires more proof.
Promotion and gap provenance share a guarded transaction. There is no live-worker
handoff, fabricated ACK/head, or use of ordinary >100 recovery for this work.

Gap rows are never deleted or have reasons/timestamps rewritten. Known bootstrap,
rejection, ws-gap and reconnect obligations resolve only when every relevant
filter has complete contiguous proof through the frozen head (or its proven
curve transition boundary), and the gap's end time is covered. Unknown, reorg,
unbounded and future obligations remain blockers. Original rows and reconciliation
stage/time are recorded in the manifest.

## PIT and restart assessment

Only still-active targets are rebuilt using existing mutable feature/PIT append
semantics. Immutable rows and the original first eligible version are never
updated. New versions use actual materialization/proof time; late maintenance
cannot backdate eligibility. Expired historical targets receive no rebuild or
new eligibility. Incomplete graduation-spanning windows remain ineligible until
curve/V4/hook proof is complete. Protected272959 remains unchanged.

After each common frozen drain head, the command reads a new H_restart. If any
active filter is unsafe, another explicit frozen maintenance round catches up.
At most four rounds are attempted per invocation; otherwise return
`OFFLINE_DRAIN_HEAD_PENDING` and rerun the same generation. A completed generation
is still assessed against a fresh head on rerun, so old reports are not promises
of future startup safety.

`restart_safe_now=true` requires no pending/failed/unresolved switch, active gap
or incomplete bootstrap, and every required filter having a proof-backed cursor
with nonnegative lag<=100 **and** its actual normal overlap query<=100 blocks.
The existing two-block overlap makes the effective usual head lag limit97;
the normal100 guard is unchanged. A retired curve is assessed only through its
graduation boundary. Zero active filters need no getLogs/head/proof work; optional
chain preflight remains. The tool computes readiness but never executes restart.
Operators must reassess immediately before a separately authorized start.

## Manifest and dry-run

No new tables or production schema migration: records use `flow_shadow_meta`,
`flow_shadow_jobs`, `flow_shadow_ranges` and `flow_bootstrap_identity`. Missing
existing proof schema is a refusal. The manifest key is
`offline_drain:GENERATION:manifest`; semantic identities also occupy namespaced
metadata keys. Fields include generation ID, start time, exact revision, externally
stopped source PID, transactional snapshot/digest, filter identities/safe starts,
initial cursors, frozen head rounds, ranges/reuse provenance, recovered events,
duplicates, after-cursors, resolved gaps with original records, final readiness
and completion time. No endpoint or credential is stored or printed. Arbitrary
error strings are excluded; URL-bearing gap reasons are represented by a redacted
label/digest in snapshots while their DB rows remain untouched.

Dry-run opens flow/main DBs read-only and never constructs the mutating reconciler,
migrates schema, creates a writer lock, calls RPC or acts on a service. It reports
active filters, starts/cursors/gap IDs, compatible reusable proof, filter-block
positions, estimated2000-block getLogs work, current budgets and whether a
three-attempt estimate likely retains50 calls. It uses the stored last-connected
head and labels that estimate as stale/unknown, not an invented live head.

## Deterministic acceptance

`tests/test_flow_offline_drain.py` covers several10k–35k curve debts, paired gaps,
split/Validation-only routing, untouched normal guard, complete/partial/zero-log
proof, dedupe, identity-bound reuse, proof-hole refusal, interrupted proof and
post-cursor reruns, shared writer exclusion, unexpected worker starts, target
expiry, PIT immutability/timestamps, V4/hook activation, adaptive ranges, moving
heads, zero filters, reserve exhaustion/resume, preflight refusal and redaction.
All tests use isolated fixture DBs and mocked provider responses. Full Ubuntu
suite execution does not read or reconcile the production databases.
