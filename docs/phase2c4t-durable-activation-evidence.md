# Durable research activation evidence

Phase 2C.4t is source/test/read-only validation. It performs no production service,
schema, state or provider operation. Its source readiness is not collection recovery.

The 4s subscription path awaited a validated `eth_subscribe` response but kept its
subscription ID and readiness time only in worker memory. Exact ACK time was not
retained. The existing segment's old ACTIVE/empty-seal record also bypassed the
ACTIVATING-only `activation_tail` path. Normal recovery/head capture is not proof
that that complete startup-tail path executed. The old catalog VALIDATED flag and
runtime-eligible feature rows cannot replace the missing activation evidence.

## One authoritative contract

`app.flow_activation` stores append-only `activation_v1:` JSON records in existing
canonical `flow_state`. Mutable current-session/readiness/filter pointers select
records; they never overwrite the records themselves. Existing `flow_shadow_meta`,
`flow_shadow_jobs`, `flow_shadow_ranges` and `flow_bootstrap_identity` store proof.
No new table, column, trigger or metadata migration is required:
**NO_PRODUCTION_SCHEMA_MIGRATION_REQUIRED**.

Each WSS connection has a distinct session ID, provider/connection ID, PID, clean
checkout revision/source path and exact segment/boundary hash. This records startup
checkout provenance; it is not loaded-module revision instrumentation. A successful
correlated protocol ACK is committed before in-memory routes/subscriptions change.
The record retains target/filter/query, subscription/request ID, response time and
provenance. Remote rejection/timeout produces no ACK; SQLite/malformed local state
blocks locally without a provider failure metric. Reconnect retains old records and
requires new ACKs applicable to the new connection. Duplicate responses do not
create another semantic ACK; conflicting evidence raises a local error.

Readiness requires every required curve/V4/hook filter, including curve history
after graduation. Its canonical filter-set identity contains lifecycle/query/base,
applicable lifecycle end and ACK IDs. Validation HTTP captures the head after ACKs;
the head is persisted before the separately budgeted header request. The matching
header and ready time are committed with the filter set. A budget interruption
cannot move this head. Empty-filter old seals never authorize primary PIT.

Each readiness generation has a fixed `research_tail:<readiness-id>` stage.
Lower bounds are the maximum of the unchanged segment boundary and the filter's
launch/graduation block. Upper bounds are the fixed ready head, clipped for curve
history at a retained graduation. Old cursors do not substitute for this proof.
Validation HTTP uses existing 2,000-block adaptive jobs, accounting, retries,
event-before-range commits, zero-log ranges and deduplication. Range contiguity,
query identity, exact bounds and completed jobs are independently rechecked before
completion. Bootstrap proof is committed before normal cursor promotion. Partial
generations survive reopen; a later session does not move their head or delete ACKs.

Pending reconnect uncertainty binds to the new matching Validation readiness
header with retained lower anchors. Existing bounded gap recovery then checks exact
contracts, reusing compatible proof where permitted. No unbounded marker is cleared.
Pending/failed switches or unresolved current gaps prevent completion. Completion
records retain provider, stage, filter-set hash, head and actual completion time.
ACTIVE/pit_eligible derives from the verifier, not the old catalog status. Health
additionally requires the actual connection, no current debt and all required
bootstrap proof. Historical pre-clean debt stays in its original partition.

## PIT and read-only audits

Primary eligibility needs complete applicable activation proof, healthy current
segment, filter completeness and actual availability. Its timestamp is the maximum
of first materialization, filter proof availability and activation availability.
Cutoffs and first materializations predating activation cannot be retroactively
promoted. Existing immutable versions remain unchanged. New versions do not make
4s incident windows primary. New clean validation retains the earlier catalog
validation as append-only diagnostic history and requires a NEW qualifying v1 row.

The joined read-only ledger audit uses the same proof verifier and each version's
activation ID. The old VALIDATED flag cannot start the clean clock. Current health
and historical feature-proof validity are separate: a disconnected current session
does not rewrite already immutable historical proof. Maturity remains 600 safe
60s/24h rows, 60 elapsed clean days AND a 60-day eligible span, chronological
60/20/20, 120 holdout and 20 below/20 above one per slice. No modeling/trading.

## Local preflight and tests

The real local startup preflight checks canonical schema and activation readers.
It backs the read snapshot into a private in-memory DB and exercises existing
unique keys, filter/bound metadata, job writers, transaction initialization and
rollback. No production write occurs. `scripts.flow_local_preflight --read-only-live`
permits this safe snapshot check with a running writer; it explicitly does not
claim the stopped-writer lock. The existing stopped mode still requires that lock.
Both modes deny network access and count attempts.

`tests.test_flow_activation` executes real subscription commands/readers, ACK
writers, fixed HTTP jobs, completion/health derivation and fresh feature eligibility
against fake providers. It covers multi-filter/out-of-order/missing/duplicate ACK,
local writer failure, timeout/rejection, ACK/proof ordering, crash/reopen, pinned
budget pauses and reconnect. Existing bounded-gap, startup/schema, immutable ledger
and partition tests remain required. Test-only synthetic windows never reach production.

The next plan is `phase2c4t-next-activation.md`; it remains unexecuted. Current
production unbounded-gap diagnostics are operational blockers, not permission for
this source task to repair or clear them.
