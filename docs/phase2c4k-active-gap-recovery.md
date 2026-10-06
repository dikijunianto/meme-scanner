# Active-epoch gap recovery and truthful health

Source-only release. It does not authorize a restart, live repair, epoch rollover,
historical switch17 recovery, provider request or PIT mutation.

## Target 309266 diagnosis

Launch/tracking start: 2026-10-06T06:26:33Z, block81419247/log92, epoch2.
Curve bootstrap: 81419247..81419281, completed06:26:40.457933Z.
The retained runtime cursor is81419346. Gap4 is unresolved
`reconnect_recovery_incomplete`, time interval1791268000..1791268015.026573,
saved first block81419247. There was no retained explicit upper block, normal
recovery job, last-attempt UTC or normal retry count. Those fields are UNKNOWN;
a cursor is not identity-bound range proof. The exact bootstrap prefix is proved;
its head time predates the uncertain end, so it cannot prove the whole gap.

The worker created this gap on a normal post-bootstrap recovery budget rejection.
The legacy default gap anchor used the launch block. Retry planning instead used
the advanced cursor minus two. The resolution predicate required the saved anchor
inside that queried suffix, leaving the gap unresolved after successful retry.
The worker then unconditionally removed the target from its in-memory pending set.
Later reconciliation only scheduled new subscriptions or in-memory pending IDs.
There was no durable gap scan. Expiry further removed the target from scheduling.

Health was written from `service_status == connected`; gap counts were diagnostics,
not prerequisites. Its ACTIVE-epoch guard only checked status/PIT-enabled flags.
Target expiry moved the gap to the existing “historical target” count, which is
not the same as quarantined historical epoch debt. The original epoch partition
selection was correct; health and retry ownership were incomplete.

## Invariants and bounded repair

Current health is derived at the shared database state read/write boundary from
the selected epoch partition: connected transport, retained verified startup
seal/tail, no current switch debt, no unresolved gap, complete required bootstrap
and no unfinished proof jobs. Expired current-partition gaps still block health.
Quarantined epoch1 switch17 is retained in its separate partition and excluded.
Existing active/historical target diagnostics keep their original definitions.

The durable gap row itself is the recovery obligation. New gaps immediately have
`gap_recovery:<id>` queued state in the existing flow_state table. Older gaps derive
a queue reference from their row. Diagnostics expose queued/recovering/budget_wait
or operator_blocked with a concrete reason; no unresolved row silently vanishes.
Reconcile reconstructs its work from these obligations on each tick/restart.

Current gap proof reuses CursorBootstrap/ShadowReconciler jobs and canonical query
and lifecycle matching from compatible_ranges. Exact committed ranges can cover
part of a required interval; overlap alone never resolves the obligation. An
already complete compatible proof after the uncertain end resolves without RPC.
Otherwise the Validation head is durably captured before its separately budgeted
header; jobs/ranges survive retry and restart. All required lifecycle filters
must independently verify before resolution. One proof chunk per tick, shared
budgets and reserve50 remain. More than100 unproved filter-block positions is
explicit operator-blocked; the normal initialized-cursor >100 guard is unchanged.
Unsupported reasons, missing boundaries, changed identities and expired targets
are operator-blocked. Budget waits are nonterminal and retain head/job/progress.

Expiry retains the gap and proof; it never satisfies debt. This automatic scheduler
does not resurrect expired targets. A later separately authorized forensic repair
may improve descriptive completeness. Existing missed 30/60/300s immutable
versions stay unchanged; their first missed recovery cutoff cannot gain eligibility
through rebuild. A later timely proved window can append with actual availability.

## Research clock

EPOCH2_OPERATIONAL_START remains2026-10-06T05:37:45Z/block81390867.
EPOCH2_RESEARCH_CLEAN_START is UNKNOWN, not that operational boundary. No primary
60-day clock is currently valid. Only a separate authorized operational task,
successful post-fix recovery and fresh healthy production PIT evidence may establish
the later clean boundary. Epoch2 identity stays fixed; no epoch3 for bookkeeping.
Daily readiness remains blocked until that boundary is documented and applied to
clean counters. Keep600 safe60s/24h rows,60elapsed clean days AND60-day eligible
chronological span,60/20/20 split,120holdout and20below/20above per slice unchanged.

## Verification

`python scripts/run_offline_tests.py` denies all external sockets and checks swallowed
attempts. The production-like fixture fails on the original source for false health
and stranded suffix recovery. Tests cover health A–H, expiry, restart reconstruction,
budget before/after job creation, pinned-head resume, proof reuse/rejection, dedupe,
immutable missed windows and a later eligible window. Existing switch/epoch/sealed
activation/NULL-curve/V4/hook/graduation/normal-range regressions remain required.

No new tables/columns or migration. Added flow_state keys and existing job/range
rows are runtime state changes only when separately activated; source deployment
does not migrate or mutate production. Disk HEAD is not a running revision claim.
