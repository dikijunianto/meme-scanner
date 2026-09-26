# Phase 2B.2.1 recovery-budget diagnosis

The 2026-09-26 cutover failed on recovery scheduling, not on provider
capability. At 01:23 and 01:24 UTC the shared HTTP bucket reached 12/12
members per minute; at 01:24 the worker had committed 102 recovered blocks.
The same 400/day getLogs ledger had only 131 calls by 01:26, and RPC members
were 179/1,000. The old worker logged only `FlowBudget`, so an individual
historic warning cannot be assigned more narrowly. Source control shows a
second rejection path: once a pinned cursor falls over 100 blocks behind the
moving head, `recovery_plan` rejects it before any getLogs call. The chain
advanced 432 blocks between H_stop and H_live. Retries every two seconds
could therefore keep failing even after the minute bucket reset.

The migration ledger had fully verified H_prefetch, H_stop, and H_live and
advanced per-filter cursors. It had not resolved corresponding `flow_gaps`
rows or signaled the already-connected worker to retire its in-memory pending
recovery. This caused redundant replay against later heads. The restored
legacy route also showed incomplete recovery; the failure is not evidence
of a PublicNode or Validation capability defect.

The runtime now names each rejection scope and records used/limit and the
next eligible time without endpoint values. Minute and UTC-day limits remain
12 and 1,000/400; HTTP remains paced at 0.5 envelopes/second. Every actual
send, including retries, is accounted under one SQLite write lock; a rejected
call is not counted as sent. Recovery pins a head, retains each committed
10-block chunk, waits for the correct bucket reset, and processes targets
independently. A gap beyond the unchanged 100-block normal safety bound is
explicitly `UNRECOVERABLE_GAP`, held until its cursor/proof changes; it does
not spin or prevent younger targets from progressing.

The shadow handoff marks a recovery gap resolved only when the ledger proves
its exact per-filter interval through H_live. An atomic marker lets only the
connection that was active before H_live retire that pending recovery.
Unproved and interrupted gaps remain unresolved. Provider routing is
unchanged by this fix; production validation must pass on the legacy route
before another cutover is considered.
The shadow start also honors an earlier unresolved gap block even when the
normal cursor has moved beyond it; this prevents a newer cursor from hiding
an older unproved interval.
