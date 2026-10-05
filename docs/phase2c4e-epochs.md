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

ACTIVATING means current ownership without eligibility. ACTIVE follows the
atomic proof-backed live seal with `pit_eligible=1`. The fixed Validation header is fresh, chain 4663, number/hash matched,
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

## Replacement operational activation protocol

The Phase 2C.4e operational ordering is superseded by
[Phase 2C.4g activation protocol](phase2c4g-activation.md). That is the sole
operational plan: prepare an ACTIVATING/PIT-ineligible epoch while stopped,
start flow once while sealed, then canonical discovery, ACKed explicit bootstrap
and startup-tail proof, atomic ACTIVE eligibility seal and natural fresh PIT.
The earlier Phase 2C.4f pre-start completion invariant is not part of this
replacement protocol. No production activation occurs during source deployment.
