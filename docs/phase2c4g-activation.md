# Epoch-2 activation protocol

Selected architecture: **SEALED_POSTSTART_BOOTSTRAP**.
This is the sole replacement operational protocol for the conflicting 4e/4f
ordering. Source deployment does not authorize executing it. Production currently
has no epoch catalog. A later authorized operation may stop flow once and start
it once; main must remain continuously active. Never recover switch17 here.

## States and proof ownership

The catalog has one current owner: ACTIVATING or ACTIVE. A fresh partition is
ACTIVATING with pit_eligible=0 and recovery_state=bootstrap_required. Ownership
allows discovery, subscriptions and Validation bootstrap but does not mean
research eligibility. Historical CLOSED/QUARANTINED partitions cannot run.
Only the atomic live seal changes ACTIVATING -> ACTIVE and pit_eligible -> 1.
No materialized version gets model_eligible_at while ACTIVATING, even if a
bootstrap cursor exists or someone sets pit_eligible without the ACTIVE seal.
Diagnostic immutable versions may be appended with NULL eligibility. Existing
historical versions cannot be updated or moved to another epoch.

The new catalog CHECK and unique partial index include ACTIVATING. This is an
additive first installation on production's absent epoch schema. If a catalog
was previously created with the old ACTIVE-only constraint/index, do not claim
that CREATE IF NOT EXISTS upgrades it: abort and obtain an explicitly tested
schema migration. Do not rebuild an existing catalog opportunistically.

## Exact supported code path

- `scripts.flow_epoch_prepare.execute`: verifies clean checkout/main active/flow
  inactive/old PID gone; holds configured writer lock. Calls `quarantine`, then
  `prepare`; does not discover or bootstrap and makes no service action.
- `app.flow_epochs.quarantine`: additive/idempotent transaction registers epoch1
  QUARANTINED, retains switch17 FAILED/FlowBudget/timestamps/NULL frozen_head/two
  unresolved ranges, and writes separate historical incident metadata only.
- `prepare`: three budgeted Validation calls (chainId, blockNumber, matching
  block header), fresh number/hash/time validation, exclusive sibling file,
  initialized collector schema/ledger, then fixed ACTIVATING catalog boundary.
- `FlowDB.active_path` selects ACTIVATING or ACTIVE; worker entry point accepts
  either initialized current partition, never creates a new epoch at startup.
- `FlowWorker.run`: establishes WSS chain identity/connection, then `reconcile`.
  `discover` reads persistent main `launches` and sampling `outcome_targets`,
  verifies launch block time and decimals, inserts canonical targets and calls
  `ensure_bootstrap_state`; NULL filters get required bootstrap and durable gaps.
- `subscribe` awaits each eth_subscribe response; the acknowledged dictionary
  covers all current filters. The worker records readiness before recovery head
  capture. WSS logs may be buffered/drained, but cannot promote completeness.
- `bootstrap_missing` uses Validation `ShadowReconciler` with >=50 reserve and
  `CursorBootstrap` stages live_bootstrap:<launch> / live_graduation:<launch>.
  A graduation during ACTIVATING independently proves the terminal curve prefix
  as well as new NULL V4/hook filters, and starts a new applicable tail round.
  Each stage pins its own head once; NULL safe_start-to-head contiguous query
  proof is committed before cursor promotion. H_epoch2 never moves. A stage
  whose proof does not include the epoch/required filter bound cannot seal.
- While ACTIVATING, `activation_tail` uses the same explicit verifier and
  budgeted HTTP with durable epoch_tail:<launch>:<round> jobs/ranges, after ACK.
  A durable activation-tail-required gap remains until proof completes, even if
  the target expires. Expiry cannot erase an unfinished activation obligation.
  It can exceed 100 blocks as explicit activation proof. Partial jobs retain
  next_unverified_block without promoting cursors or eligibility. A complete
  stage records exact canonical query/first/last/ready_head/ACK/proof time/stages
  before the eligibility transition. Verified HTTP heads also feed the retained
  connection checkpoint, so a silent WSS stream cannot leave a zero restart head.
  Current normal recovery is not used here.
- On interruption/reconnection, finish the old pinned tail stage, then create a
  new tail round for the new ACK. This advances the tail proof, not H_epoch2.
  Verified preceding ranges and overlapping next ranges are checked contiguously;
  the new connection cannot seal with its predecessor's ACK timestamp alone.
- `seal_live`: successful canonical discovery pass, connected transport,
  acknowledged filters, no pending recovery/current failed/pending switch and
  no unresolved current gaps, including expired targets; independently verifies
  bootstrap jobs/query/ranges/cursors and every tail stage through ready_head.
  Atomic catalog transaction persists live_seal and changes status/eligibility.
- `recovery_state`: healthy writes are blocked centrally by FlowDB until ACTIVE
  and eligible. Connected describes transport only. Status tool reports current
  recovery health separately from historical FAILED debt.
- `finalize`/`rebuild` compute due event-time features; `_append_feature_version`
  requires both epoch bounds, ACTIVE, pit flag, live_seal, current switch/gap
  safety and window filter proof for eligibility. Actual materialization,
  bootstrap completion, live seal and cutoff bound model_eligible_at. The
  read-only maturity audit selects one current partition and counts only ACTIVE
  eligible proof; never pools epoch1 history.

Offline primitives can create a pending epoch and query explicit proof. However,
there is no supported complete offline split-epoch discovery/bootstrap operator:
`flow_bootstrap_missing_cursors` requires both services active, legacy route and
Alchemy WSS, and consumes existing targets. It is not a pre-start split epoch
command. Discovery/subscription ACK/current live handoff require the runtime
worker orchestration. Implementing another offline orchestrator is unnecessary
when sealed post-start orchestration can reuse the tested proof engine.

## One future operational ordering

1. Capture fresh read-only identities/clean source/tests, original switch17
   payload and gaps, all immutable version keys/hashes/eligibility, protected
   target272959, integrity/security/routing and isolation. Require expected main
   PID64326/NRestarts0 and retained flow identity. Never use a disk revision as
   runtime revision instrumentation. Confirm catalog absent or compatible.
2. Before any stop, estimate only new-boundary/discovery/current-filter explicit
   bootstrap/ACK-tail/head/header/validation costs at retained real chain
   evidence. No timestamp-derived head. Include possible zero-target case and
   growing activation tail. Require capacity within 400 getLogs/day,1000 RPC/day,
   12/min,0.5 HTTP envelopes/sec with >=50 getLogs reserve. Historical2514 debt is
   excluded. If capacity is insufficient or discovery safety cannot be proved,
   defer stop. Verify main persisted launch/sampling discovery and canonical
   identity; check still-relevant pre-stop/downtime launches independently of WSS.
3. Retain service intent/before evidence; stop only flow exactly once. Verify old
   PID gone/inactive and unchanged active main. Retain exact stop UTC and an
   actual head only if durably recorded, never estimated. Do not use restart.
4. Under service account/protected environment in expected clean checkout, run:

   ```text
   /opt/meme-scanner/.venv/bin/python -m scripts.flow_epoch_prepare --old-flow-pid <retained-old-PID> --epoch-id GENERALIZED_BOOTSTRAP_EPOCH_2
   ```

   This applies additive migration, quarantines epoch1 metadata, captures fresh
   Validation boundary and creates one empty ACTIVATING epoch2. Retain exact
   epoch_id, block/header UTC/hash/capture/provider/chain/revision/predecessor/
   reason. No getLogs are executed by this command. Validate unchanged main,
   integrity, historical PIT and switch17/ranges, exact boundary, ACTIVATING,
   pit_eligible=0, reserve and routing. **Pre-start bootstrap completion is not
   required or claimed.** No vacuous empty-partition activation is accepted.
5. Start flow exactly once from expected checkout. Capture PID, NRestarts,
   execution UTC/ExecStart/WorkingDirectory and loaded epoch identity. Start is
   transport activation, not successful research activation; epoch remains
   ACTIVATING and recovery cannot report healthy yet. Main unchanged.
6. Worker performs canonical discovery (including persisted downtime launches),
   creates required NULL filters, subscribes while sealed and records ACKs.
   Retain WSS observations. Worker explicitly proves historical safe_start through
   pinned bootstrap heads including fixed H_epoch2, then each ACKed startup tail
   via durable explicit HTTP jobs. Reconcile any uncertainty contiguously. No
   normal NULL-cursor/>100 recovery and no WSS-only completeness assumption.
7. Budget wait is nonterminal: retain the same epoch/head/stage/ranges, no FAILED
   switch due solely to budget, no ACTIVE transition/PIT eligibility. Resume
   proof on later ticks with the same pinned stage. Current gaps or true proof/
   provider/identity failures remain blockers. Do not perform a second service
   action as a workaround. Crash semantics permit a separately authorized later
   start loading the same partition; never create epoch3 or reset H_epoch2.
8. Validate actual complete jobs/ranges/query identities, proof before cursor,
   ACKed full ready-head tails, discovery pass, zero required/in-progress/current
   unresolved obligations and switches, healthy transport. Only then allow the
   atomic ACTIVATING -> ACTIVE live seal and derive healthy current recovery.
   Historical switch17 stays FAILED with two unresolved obligations, not a
   current-epoch blocker. Verify protected immutable preservation again.
9. Wait for a naturally due new post-boundary 30s/60s window and first fresh
   eligible immutable PIT append. Retain launch/window/cutoff/materialization/
   proof/eligibility/hash/filter/epoch evidence; actual availability bounds
   eligibility. This is mandatory for operational success; ACTIVE alone is not
   success. Incident reconstructed rows remain unsafe. Only then document exact
   PIT_COLLECTION_RECOVERY_END using real version/time/head evidence.
10. Update aggregate monitoring to exact epoch2 boundary and counts only. Require
    600 safe60s-to24h rows,60 elapsed clean days AND60-day eligible chronological
    span,60/20/20 split,120holdout,20 below/20 above each slice. Epoch1 remains
    descriptive/diagnostic/secondary. Natural graduation is secondary/pending
    unless real V4/hook and spanning proof occurs. No model/trade authorization.

Abort before stop on preflight mismatch. After stopped bad/stale header/orphan/
failed migration, remain stopped and preserve records for operator review; never
resume old failed epoch as clean. After starting true proof/integrity/security/
PIT/isolation regression blocks without automatic restart or debt clearing.
Rollback requires explicit authorization and preserves all partitions/catalog/
proofs; do not recreate/reset epoch2. A process crash before atomic seal leaves
ACTIVATING; a crash after commit retains ACTIVE and the original seal. Startup
re-proves uncertain connection gaps before subsequent live claims.
