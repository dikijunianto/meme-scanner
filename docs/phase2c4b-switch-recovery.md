# Budget waits and conservative failed-switch proof

## Reproduced old path

Systemd executes `python -m app.flow_worker`, defining a local
`__main__.FlowBudget(RpcError)`. `FlowWorker.prove_provider_switch` imports
`ShadowReconciler`, whose `ShadowRpc` imports a different
`app.flow_worker.FlowBudget`. After the subscription ACK, `ShadowRpc._send`
can reject the Validation head request before sending because its minute
allowance is exhausted. The raised shadow FlowBudget misses the entrypoint's
`except FlowBudget`, matches shared `RpcError`, and the run-loop handler calls
`flow_provider_switch.failed(..., 'FlowBudget')`. The durable transition is
ACK-ready -> FAILED with no frozen head/jobs. `pending` then excludes it,
`blocked` includes it, and both reconcile and finalize suppress collection.
No normal budget retry path remains. Phase2C.4a reproduced this with the exact
handler and an in-memory DB.

The accidental fixture WSS call was caused by executing the enclosing runtime
loop AST, not by importing a module. Runtime startup is already behind the
explicit `main`/`cli` entrypoint. Tests must deny network before discovery and
call a narrow handler directly rather than executing a copied runtime loop.

This task changes source, tests and documentation only. Operational recovery
and service actions require a later task. Deploying files does not reload the
already-running worker.

## Retry semantics

`app.flow_budget.BudgetWait` is the single limiter rejection type imported by
both module namespaces. It inherits `FlowBudget(Exception)`, never `RpcError`.
Both shared budget guards retain their accounting transaction and reject before
send. `advance_provider_switch` catches this type before genuine RPC/proof errors
and records `WAITING_FOR_BUDGET` with the prior pending state, scope, usage,
limit and reset deadline. Snapshot, gaps, pinned head, jobs and successful ranges
remain intact. `resume_budget_wait` restores the same obligation after its deadline;
it never creates another switch. Parser/header-stage waits likewise retain the
first unverified chunk; already ingested events deduplicate on retry.

## Conservative missing-head capability

The recovery CLI adds explicit `--conservative-later-head`; it is not invoked
by startup or normal collection. A further explicit `--handoff` is necessary
for closure after proof. No command in this source-only task performs recovery.
The existing exact-original-head and zero-filter paths remain separate.

The supported historical plan is an exact, ungraduated curve snapshot. Each
positive persisted switch `base` must match the launch block and log index in
the retained main launch and flow target; token, quote and curve addresses must
match. This deliberately uses the full durable activation-to-head interval,
not an unproved cursor suffix or current target start. Legacy snapshots lacking
a query require the earlier `live_bootstrap:<launch>` durable query identity,
dated no later than the original snapshot. New snapshots store query, token,
quote, launch log index and lifecycle directly. Missing provenance, absent or
duplicate snapshots, V4/hook/graduated lifecycle, changed identity or invalid
bounds refuse before proof. Unsupported lifecycle requires a separately proved
extension; it never broadens to address scanning.

Only the existing Validation HTTP shadow client is used, with limits capped at
400 getLogs/day, 1000 RPC members/day and 12/minute and reserve at least50;
the operational factory retains0.5 envelopes/sec. A later head and matching
header must exceed all retained lower/cursor/last-old-block anchors, and its
timestamp must be after the failure. It is pinned once as `reconciliation_head`
with capture timestamp, query plan and revision in the FAILED payload. The
historical `frozen_head`, uncertain upper bound, failure reason and failed time
are never overwritten. The later proof supersets the unknown historical upper
bound; it does not reconstruct that bound.

Successful shadow chunks and canonical ingestion/dedupe reuse the existing
crash-safe ordering: event commits precede their successful range commit,
so a crash replays/deduplicates instead of claiming proof before ingestion.
Closure checks the exact job/filter set, complete contiguous activation-to-head
ranges (zero-log responses count), gap identity and fresh matching transport.
Only after canonical events and all range evidence are durable does one atomic
transaction resolve the named switch gaps and reconcile FAILED to HEALTHY.
It does not clear unrelated gaps or reactivate expired targets. The original
FAILED raw payload and exact SHA256 are retained, along with method, separate
head, proof, reconciliation time, events and duplicate totals. No runtime status
is set by the operator; the updated worker derives status on its normal tick.

## PIT and incident diagnostics

No schema migration is required: wait/proof history uses switch JSON and
incident annotation uses existing flow_state. The incident start is the retained
first disconnect time; its exact start block remains unknown (last-old-block is
an anchor only). `PIT_COLLECTION_RECOVERY_END` staysNULL until the normal worker
observes connected status and zero active gaps after proof. A later rebuild
whose coverage overlaps this interval is explicitly diagnostic/ineligible.
New coverage starting after the observed recovery end can become eligible with
its actual materialization/proof clock. Existing immutable versions, first
eligible rows, times and the original clean-era boundary remain untouched.
Source deployment alone cannot activate the updated worker semantics in the
existing process; activation/recovery and boundary validation need a later
authorized operational task.

## Offline validation

Run from the repository root:

```text
python scripts/run_offline_tests.py
```

The runner installs socket/DNS denial before discovery and rejects a swallowed
unexpected attempt at suite completion. Test-package initialization installs
the same guard for ordinary discovery. HTTP/WSS fakes and isolated temporary
databases are used. Imports under denied HTTP/socket/DB/service factories cause
no work. The deliberate accidental-WSS regression fails before any transport
and the narrow proof tick asserts the runtime WSS entrypoint was never called.
