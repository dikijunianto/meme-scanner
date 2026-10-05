# Collection epochs and prospective rollover

This revision is source only. No production epoch is registered, no database
migration runs at deployment, no service is restarted and switch 17 is not
recovered or quarantined operationally.

The configured flow database becomes the durable catalog and shared budget
ledger only during a separately authorized offline activation. A fresh epoch
uses a sibling SQLite file, `flow.epoch-<epoch_id>.db` (the actual configured
stem/suffix are retained). It receives the existing collector schema, empty
targets/events/gaps/bootstrap/cursors/jobs/features and a new prospective ledger
boundary. Historical tables remain exactly where they are. This avoids changing
their primary keys or copying old uncertainty into new filter state.

`flow_collection_epochs` is an additive catalog table with epoch ID, creation
UTC, start block/timestamp, source revision, reason, predecessor, status,
eligibility flag, partition path, termination timestamp, incident metadata and
boundary evidence. `one_active_collection_epoch` enforces one active owner.
`flow_epoch_global_ids(kind,next_id)` reserves globally unique switch/connection
IDs across partitions under a transaction. Schema creation is transactional and
idempotent. Creation of a new partition is exclusive: an interrupted orphan is
never truncated or silently reused. An existing active boundary is never reset.

ACTIVE means current ownership; `pit_eligible=0` means activation proof is still
pending. The fixed Validation header is fresh, chain 4663, number/hash matched,
and retained before the pointer is published. No current cursor is copied.
Canonical discovery and required-filter bootstrap use the existing worker paths.
Every missing curve/V4/hook cursor is explicit bootstrap, never normal >100
recovery. All required historical curve proof for a graduated target is checked.
ACKs, contiguous identity-matched bootstrap jobs/ranges and durable acknowledged
recovery-tail evidence precede eligibility. New-epoch normal recovery records
the successful HTTP range/query/ACK/time before advancing its cursor. A pending
recovery, active gap or current failed/pending switch keeps eligibility blocked.
The normal 100-block recovery ceiling is unchanged.

Unresolved gaps in the current epoch block eligibility and readiness even when
their targets expire. Only debt in a separately closed/quarantined partition is
excluded from current blockers. Pre-boundary launches still require explicit
launch-to-head bootstrap; its durable bootstrap-required gap replaces the
redundant service-start timestamp gap, and those launches never enter primary PIT.

Historical registration retains the generalized boundary at
2026-10-02T06:09:03Z / 78006186 and terminates epoch 1 at the exact incident start
2026-10-02T15:32:50.699403Z. This timestamp is the interruption boundary, not a
fabricated incident-end or process-start block. The incident remains open.
Quarantine metadata lives in the catalog; switch 17's FAILED payload/state,
NULL original frozen head, timestamps/reason, two unresolved obligations and
gaps remain unchanged. The retained estimated workload is 2,514 calls, based on
the prior offline checkpoint, not a future live reconciliation head.

Current blockers read only the active partition, including all its sessions.
Historical diagnostics read prior partitions read only and keep failed debt
visible separately. Quarantining alone cannot resume a worker: a closed
partition cannot start or create new provider switches/connections. New switches
use ordinary proof/failure semantics and shared global identity allocation.
Legacy behavior remains compatible when no epoch registry is present.

New immutable PIT evidence carries `collection_epoch_id`. A target must satisfy
both start timestamp and launch block bounds. No historical PIT version is moved,
rewritten or reconstructed into primary eligibility. Closed partitions cannot
append PIT versions. The read-only ledger audit attaches only the active
partition for primary counts, verifies epoch identity, actual availability before
verified identity-matched labels, and requires both 60 elapsed clean days and a
60-day eligible chronological span. The original 600 / 60-20-20 / 120 holdout /
20 below and 20 above in each slice policy is unchanged. Old valid rows remain
descriptive, diagnostic and secondary robustness data only.

`scripts.flow_epoch_status` reports current health, current gap/switch obligations,
PIT/maturity telemetry and shared daily budgets separately from quarantined debt.
Use it in the daily procedure after operational activation; do not pool partitions
or use predictive screening. Historical debt alone must not page as a current
incident once a new epoch is healthy. Current failed recovery, persistent gaps,
integrity/hash failures or missing bootstrap proof still require investigation.

The 2df62d5 forensic recovery remains available. It opens the historical catalog
explicitly; a quarantined epoch no longer needs a current WSS handoff. Validation
chain identity, pinned head, unchanged service checks, canonical filter identity,
budget limits/reserve and every contiguous-range completion check remain. Only
actual full proof may subsequently mark that historical switch healthy. It never
changes the active partition, its PIT versions or its maturity clock. Budget
usage is shared, so optional forensic recovery must not starve current collection.

## Future operational activation — do not execute in this phase

1. Obtain authorization for this operational stage. Retain a fresh read-only
   snapshot of main PID/start/NRestarts, flow PID/start/NRestarts, exact clean
   deployed HEAD, original switch 17 raw payload/hash/FAILED/NULL head/two
   obligations, all immutable versions, protected target 272959, database
   integrity, routing fingerprints, security modes and new-era isolation.
   Verify protected configuration 0600, parent 0700, ubuntu readability and
   offline split validation. Preserve original Phase 2C.3j evidence and all four
   era boundaries. No historical recovery is necessary.
2. Budget preflight: Validation only; shared UTC getLogs <=400, RPC <=1000,
   minute <=12, reserve >=50, pacing unchanged. Require room for three boundary
   RPC members plus bootstrap/head/header/tail work. Read canonical relevant
   launches and sampling membership before stopping; estimate per-filter proof
   cost at a fresh operational head. Do not count recovered legacy ranges or
   old cursors as new-epoch proof. If safe capacity is insufficient, defer the
   stop rather than spending the reserve. Main stays untouched throughout.
3. With durable intent/before evidence, stop flow exactly once and verify the
   retained old PID is gone and the unit inactive. Do not use `restart` plus an
   additional stop/start. The single controlled stop/start activates source.
4. In the expected clean deployed checkout, as the same service account and
   under its protected environment, run the following future command once:

   ```text
   /opt/meme-scanner/.venv/bin/python -m scripts.flow_epoch_prepare --old-flow-pid <retained-old-PID> --epoch-id GENERALIZED_BOOTSTRAP_EPOCH_2
   ```

   It checks main active/flow inactive/old PID gone, holds the configured flow
   writer lock, transactionally registers/quarantines epoch 1 without changing
   switch 17, validates fresh Validation chain/head/header, creates the fresh
   partition and publishes a pending ACTIVE epoch. It runs no getLogs. Do not
   rerun after an interruption without inspecting the catalog/orphan evidence;
   inspect an existing pending epoch and resume its original boundary.
5. Retain the exact new header/capture/creation UTC and source revision. Epoch
   2's block/time are intentionally unknown in this source task. Start flow once
   from the expected checkout. Confirm PID/start/NRestarts, unchanged main,
   split/PublicNode primary/Validation fallback+HTTP, and shared budgets.
   Natural fallback is permitted; no forced reconnect/failback. The worker
   discovers every canonically relevant sampled launch, creates empty filter
   state and proves safe_start through a fixed bootstrap head which includes
   H_epoch_start, with subscription ACKs before an explicitly proved live tail.
   New launches discovered during downtime use that same canonical path.
   Earlier launches may need raw proof but cannot enter primary maturity.
6. Observe durable `boundary_json.live_seal`, exact curve and applicable V4/hook
   jobs/ranges/query identities, bootstrap completion before cursor, ACKed
   normal recovery-tail records, zero current gaps/pending/failed switches,
   healthy transport and then a natural fresh eligible PIT append. Compare
   availability to label observation, protected earlier hashes and all retained
   version keys. Do not claim success from a cursor/current COMPLETE alone.
   Missing natural graduation remains pending. Keep epoch-1 debt separately
   visible, FAILED/unresolved until optional proof actually completes.
7. After validation, update aggregate/status documentation and daily monitoring
   to the exact new partition and epoch boundary. Restart the primary 60-day
   clock there, require 60 elapsed days plus chronological span, and accumulate
   600 safe rows solely in epoch 2. No modeling or trading is authorized.

Abort before stopping on changed service identity, dirty/unexpected source,
security/routing mismatch, failed integrity, mutated PIT evidence or insufficient
budget. After stopping, a bad/stale boundary or orphan file means remain stopped
and retain evidence for operator review; never invent a boundary or reactivate
the failed historical epoch. After starting, budget pauses retain the same fixed
proof and no eligibility; an unproved >100 tail, failed recovery, new active gap,
missing identity/proof, unexpected Alchemy/txreceipt or PIT mutation is an
activation blocker. Do not auto-restart or automatically clear/quarantine fresh
debt. Rollback requires explicit operator authorization, preserves all catalog,
partition and proof records, and never resumes the old incident as clean data.
