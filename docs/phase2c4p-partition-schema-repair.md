# Phase 2C.4p — complete partition schema, source-only repair

## Root cause and canonical ownership

Segment preparation called `FlowDB.migrate()` on the new physical partition. The
shadow reconciler was attached to the preceding partition; its lazy `_schema()`
initializer owned the three shadow tables. An empty segment could be sealed and
adopted before any reconciler was constructed against that new partition. The
first connected health lookup then raised `sqlite3.OperationalError: no such
table: flow_shadow_jobs` for:

```sql
SELECT 1 FROM flow_shadow_jobs WHERE completion_status!='complete' LIMIT 1
```

The old `FlowWorker.run` catch-all classified this local SQLite exception as a
provider failure, closed its recorded connection, retried WSS and eventually
selected Validation. The offline old-source fixture reproduces two established
connections, two false PublicNode errors and one false failover, with no external
network attempts. The pre-fix empty-partition test independently fails at this
exact health query before any shadow constructor.

Canonical DDL now lives once in `app/flow_partition_schema.py`. `FlowDB.migrate`
creates it for every partition; the existing `ShadowReconciler._schema` delegates
to the same definition for compatibility. `flow_segments.prepare` validates the
entire contract before publication. No simplified copy or imported proof exists.

| Table | Canonical purpose and direct callers | Constraints/defaults | Mutability |
|---|---|---|---|
| flow_shadow_meta | ShadowReconciler.meta/set_meta/freeze; CursorBootstrap._freeze; fixed stage head metadata | key TEXT PRIMARY KEY, value TEXT NOT NULL | Mutable fixed-stage metadata according to canonical freeze rules |
| flow_shadow_jobs | ShadowReconciler.prepare_stage/reconcile_stage/report; CursorBootstrap._prepare/_verify_job; FlowDB.current_health; epoch bootstrap/tail and provider switch verifier; gap recovery | PRIMARY KEY(stage,launch_id,kind); stage/kind TEXT and launch_id/range/cursors INTEGER NOT NULL; span DEFAULT 2000; five counters DEFAULT 0; completion_status DEFAULT pending; failed fields nullable | Mutable progress and completion; never seeded by schema repair |
| flow_shadow_ranges | ShadowReconciler.reconcile_stage; CursorBootstrap._verify_job; sealed_proof_intact/seal_live; provider-switch verifier and gap recovery | PRIMARY KEY(stage,launch_id,kind,first_block,last_block); all six columns NOT NULL | Durable verified ranges; canonical insert/ignore semantics |

These tables have no foreign keys, user-defined indexes or triggers. Their primary
keys create the canonical SQLite uniqueness indexes. Mandatory core indexes,
views and immutable feature-version triggers are also checked by the contract.
Range rows are not magically complete because their table exists. Existing
canonical proof verifiers still require exact identity, contiguous ranges,
terminal heads, bootstrap completion before cursors, and acknowledged startup tail.

## Authoritative partition contract

`MANDATORY_TABLES` in `app/flow_partition_schema.py` is authoritative. All eighteen
are mandatory at creation:

- flow_state
- flow_tracking_targets
- flow_events
- flow_gaps
- flow_features
- flow_samples
- flow_bootstrap
- flow_bootstrap_identity
- flow_feature_ledger_start
- flow_feature_versions
- flow_cutover_sessions
- flow_cutover_legacy_proof
- flow_cutover_outcomes
- flow_provider_connections
- flow_provider_switches
- flow_shadow_meta
- flow_shadow_jobs
- flow_shadow_ranges

No optional/lazy partition table remains. Audit includes core flow, shadow,
bootstrap, cutover/provider-switch, epoch/segment, bounded and expired recovery,
PIT writer and read-only ledger/status paths. Main database launch/graduation/
outcome tables remain read-only inputs, never segment copies.

`GLOBAL_TABLES` identifies flow_usage, flow_collection_epochs,
flow_epoch_global_ids and flow_research_segments. New segment migration explicitly
uses `shared_budget=True`, so none is copied into a segment. Usage is written only
through `budget_conn` into the existing catalog. Old physical usage tables remain
for compatibility; this repair neither drops nor copies them.

The three event views, six event/feature indexes, two feature immutability triggers,
three cutover uniqueness indexes and pending-switch uniqueness index are required
objects. The schema check rejects missing tables/objects, table names occupied by
views, and incompatible shadow columns/defaults/primary keys. Contract version 1
is a code-derived requirement, not persisted completeness metadata. Existing core
schema_version=1 remains unchanged; actual object inspection detects the old
incomplete partition. No unrelated schema-version framework is introduced.

## Readiness and local error boundary

Before subscriptions or provider counters, worker startup validates the segment
contract. Reconcile checks again before shadow/recovery work and immediately after
adopting a partition, before recording its connection or subscribing. The connected
loop checks before reading/reconciling buffered work. A missing contract produces
SEGMENT_SCHEMA_INCOMPLETE and the exact missing-name diagnostic. Other SQLite
errors produce LOCAL_DATABASE_ERROR. Both park the worker pending operator
maintenance. They do not increment transport error/failover counters, create
switch records, manufacture gaps, or enter a reconnect loop. Cancellation closes
resources without attempting a failing buffered DB flush.

Read-only health and ledger status fail closed on missing schema. Sealing cannot
publish ACTIVE from incomplete schema; fresh validation and PIT eligibility still
require original real proof. A retained operational ACTIVE catalog row is history,
not evidence of research-clean readiness. Repair cannot publish VALIDATED, change
any boundary, or establish a clean clock. Existing immutability/no-backdating,
proof ordering and natural feature/label availability rules remain unchanged.

## Existing-segment additive operation

`scripts/flow_segment_schema.py` defaults to a read-only status. Explicit repair
requires systemd flow inactive/PID 0, a clean checkout, the existing writer lock,
and exact segment/epoch/block/timestamp inputs. It opens only the already existing
partition in SQLite mode=rw. No catalog mutation or general migration is invoked.
Within BEGIN IMMEDIATE it creates the canonical missing tables with IF NOT EXISTS,
validates the whole contract and integrity, and commits. Failure rolls back.
Existing incompatible columns or missing unrelated core schema block instead of
being silently rewritten. The command inserts zero proof/state/ledger rows.

Interruption fixtures cover before first DDL, after table 1/2, before required
index/trigger contract verification, and a committed transaction whose confirmation
was lost. Reruns converge; pre-commit rollback leaves all three absent, committed
repair reruns add nothing, and independently retained partial DDL converges.
The same segment row (including boundary_json, predecessor, reason, status and
validation fields), every preexisting partition row and immutable history remains
unchanged. Historical 66-gap and quarantined switch proof remain separate.

## Operational repair plan — NOT EXECUTED

Migration requirement: **FLOW_STOP_REQUIRED_FOR_SCHEMA_REPAIR**.

SQLite supports transactional DDL, but this production process repeatedly executes
failed health queries and can invoke the old lazy executescript initializer, whose
implicit commit does not participate in the new repair transaction. The supported
mutation-owner contract is `flow_writer_lock`, held by the live worker. Repair
therefore refuses a live service/lock holder. An online repair plus an old loaded
worker is not the tested procedure. Main is never stopped or restarted.

The following is an exact next procedure for a separately authorized operational
window, not authorization to execute it during Phase 2C.4p:

1. Retain before/request evidence, clean deployed tested HEAD, service PID/start/
   restart identities, existing segment catalog row and exact boundary JSON,
   current schema and integrity, all immutable keyed rows/payload hashes, historical
   66 gap rows with NULL upper bounds, switch17 FAILED/two unresolved ranges,
   switch13/14 HEALTHY, protected272959 seven rows/hash/eligibility, catalog budget
   and offline configuration/security/routing. Confirm Validation reserve >=50
   getLogs with unchanged 400/1000/12 limits and 0.5 envelope/s; no repair RPC is
   needed. Refuse identity mismatch or newly unresolved operational ambiguity.
2. Inspect durable stop/start evidence before action to prevent duplicate service
   actions. Consume at most one flow-only stop/start pair. Save stop-request,
   stop-return, systemd inactive/PID0, prior PID absence and writer-lock availability.
   Main64326/NRestarts0/start identity must remain unchanged. Do not restart main.

   ```sh
   sudo systemctl stop meme-scanner-flow.service
   systemctl show meme-scanner-flow.service -p ActiveState -p MainPID -p NRestarts
   ```
3. From the tested clean source in /opt/meme-scanner, as the existing flow owner,
   run this additive command **only while stopped**:

   ```sh
   sudo -u ubuntu .venv/bin/python scripts/flow_segment_schema.py \
     --repair-existing-segment EPOCH2_RESEARCH_CLEAN_1 \
     --expected-epoch GENERALIZED_BOOTSTRAP_EPOCH_2 \
     --expected-start-block 82474128 \
     --expected-start-at 1791376255
   ```

   It must report exactly the absent canonical tables added, complete=true and
   proof_rows_seeded=0. A second execution is safe/no-op but not necessary. No
   general migrate, segment request/queue, new boundary RPC, replacement partition,
   classification change, historical recovery or switch recovery is permitted.
4. Use default read-only `scripts/flow_segment_schema.py` plus exact catalog/row
   comparisons to validate all mandatory schema, integrity, empty new tables and
   preserved existing data. Preserve operational ACTIVE rather than rewriting it
   to SEALED. SEALED/pending proof semantics apply only where actually retained;
   existing empty-target live_seal is history and cannot substitute for future
   target bootstrap/ACK-tail proof. Stop on any contract failure; no automatic fix.
5. Retain start-request evidence, then start flow exactly once from the tested clean
   checkout/ExecStart/working directory. Capture exact PID/systemd timestamps and
   unchanged main. Do not claim instrumented runtime revision. No additional
   restart or forced failback on any subsequent failure.

   ```sh
   sudo systemctl start meme-scanner-flow.service
   systemctl show meme-scanner-flow.service -p ActiveState -p MainPID -p NRestarts \
     -p ExecMainStartTimestamp -p ExecMainStartTimestampMonotonic
   ```
6. Resume the same segment EPOCH2_RESEARCH_CLEAN_1 within the same Epoch2 and fixed
   block82474128/2026-10-07T12:30:55Z/Validation provenance/hash. Discovery and all
   naturally needed curve/V4/hook bootstrap and startup-tail work must use real
   fixed ranges, identity and ACK proof before cursors. Every new gap must have its
   bounded contract. Any expired/unbounded/unproved obligation blocks readiness;
   no invented bound or legacy66-gap replay.
7. Passively validate connected health, actual proof ranges/identities/order,
   bounded resolved current obligations, no false provider-switch failures,
   isolation/routing/security/shared budgets. Observe fresh naturally arriving
   immutable eligible PIT, exact segment/epoch/launch/window identity, payload hash,
   materialization/completeness/eligibility times and real label availability. No
   synthesis, eligibility backfill or old-window promotion.
8. Only the canonical fresh-PIT verifier may transition the same record to VALIDATED.
   Independently retain that first actual eligible append and first_pit_json; only
   then document EPOCH2_RESEARCH_CLEAN_START using the already fixed segment boundary
   and validation evidence, and enable the 60-day research clock under unchanged
   semantics. Until then UNKNOWN/null, irrespective of source deployment, restart,
   successful schema repair or operational ACTIVE.
9. Final read-only audits compare all historical/segment rows and hashes, gap/switch
   records, exact service identities, integrity, budgets and zero new Phase2B
   Alchemy/transaction/receipt use. Readiness still requires 600 safe60s-to-24h rows,
   60 elapsed clean days AND a 60-day eligible chronological span, 60/20/20 split,
   120 holdout and 20 below/20 above per slice. No modeling/trading is authorized.
   A failure ends the window blocked with retained evidence, never a second restart,
   new segment, Epoch3 or silent boundary/clock change.
