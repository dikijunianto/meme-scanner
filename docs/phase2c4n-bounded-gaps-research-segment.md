# Bounded gaps and an Epoch2 research segment

This revision is source-only. Production remains on its existing loaded worker;
no research segment, diagnostic classification, restart, recovery RPC or schema
operation is performed by publishing the source. Research clean start is UNKNOWN.

## Read-only root cause

The retained production inventory contains 66 unresolved gaps on 14 expired
targets: 52 `ws_gap`, 13 `reconnect_recovery_incomplete`, and one
`bootstrap_required:curve` (gap34, target311273). All 66 lack an upper proof
bound. The gap schema retains time endpoints and `first_block`, but no required
through-block, capture header, filter identity or provenance. A cursor, compatible
prefix, or a separate unfinished job is not the gap's proof endpoint.

| Class | Creation path and lower source | Missing upper root cause |
| --- | --- | --- |
| ws_gap (52) | startup/disconnect in FlowWorker.run; last-connected block, target launch or retained recovery anchor | Disconnect has no reconnect head. Writer immediately made a recoverable-looking time gap without deferring capture. No upper existed at the disconnect write. |
| reconnect_recovery_incomplete (13) | FlowWorker.recovery_gap; explicit failed-plan first block or minimum retained cursor minus overlap, clamped to launch | The planner may have a pinned in-memory recovery head, but gap() accepted only a lower block. No durable head-to-gap binding was written. Individual historical transient values cannot be established. |
| bootstrap_required:curve (1) | FlowDB.require_bootstrap; filter safe_start/launch block | The required row was created before bootstrap froze a head. Later job/meta evidence was independent, without an immutable gap association. An incomplete job cannot retroactively supply the missing obligation. |

All rows were inspected, including canonical filter queries and compatible
retained prefixes. The original representation was time-based uncertainty, not
an exact fixed block obligation. No missing upper is inferred from latest block,
expiry, subsequent jobs, cursors, time interpolation or current wall clock.

## Contract and capture rules

Every required gap in the opted-in research segment has a `gap_contract:<id>`
record containing operational epoch ID, research segment ID, launch identity,
canonical filter queries and their required periods, gap class, ordered positive
from/through blocks, both retained provenance references, exact upper header,
creation time and initial recovery state. `gap_recovery:<id>` contains mutable
progress; the contract remains immutable. Reads revalidate the contract against
its source, target identity, lifecycle and physical gap. No migration of old gap
columns or old proof rows is required.

| Required class | Lower rule | Upper rule |
| --- | --- | --- |
| curve/V4/hook bootstrap | immutable job original_safe_start, matching safe filter activation | immutable bootstrap stage head/header; curve endpoint may be clipped exactly to retained graduation block |
| startup subscription tail | immutable tail job original_safe_start following contiguous bootstrap/previous ACK tail | immutable tail stage readiness head/header; promotion verifies every range before gap resolution |
| ws/reconnect/normal recovery | retained launch/cursor-overlap/disconnect anchor saved with capture | fixed Validation recovery head and its exact matching header, named by unique durable generation |
| provider switch | frozen switch uncertain_from and identity-bound filter periods | frozen switch proof head and matching retained Validation header |
| expired target with existing bounded gap | original contract unchanged | original through-block unchanged; forensic recovery is allowed after expiry |
| expiry without a previously proved required endpoint | no guessing | UNBOUNDED_CURRENT_GAP; a header after expiry does not prove the last required expiry block |
| provider-budget failure | existing retained recovery capture if available | same capture, otherwise hard unbounded failure |
| reorg/unknown timestamp/unsupported semantics/service-start uncertainty without trustworthy capture | insufficient evidence | hard unbounded failure, never a normal gap |

Disconnect uncertainty is a durable AWAITING_RECONNECT_HEAD diagnostic. It is
nonhealthy and blocks PIT, but is not advertised as a bounded recovery obligation.
Binding requires the captured reconnect header and retained lower anchor. If the
target has already expired without an appropriate endpoint, it fails closed.
Attempts to create an actual gap without valid bounds persist
`UNBOUNDED_CURRENT_GAP`, return no normal gap ID, and require investigation.
Contractless segment rows also fail closed. Headers, gap endpoints, query
identities and lifecycle records cannot be changed to move a proof forward.
Later uncertainty receives a new capture generation and obligation. Interrupted
or budget-paused work resumes existing fixed jobs rather than capturing a later
head. The legacy initialized-cursor 100-block guard is retained; explicit fixed
obligations use the existing adaptive, budgeted proof runner.

## Segment ownership and preserved history

`flow_research_segments` is an opt-in catalog table, created only by the future
authorized writer operation. Fields: segment_id, epoch_id, exact block UTC and
number, source revision, reason/predecessor, status, partition path, boundary
provenance, preclean debt snapshot/hash, validation time and first PIT evidence.
One current segment is enforced by a unique partial index. States are SEALED,
ACTIVE, VALIDATED and CLOSED. The operational Epoch2 row is not rewritten.
There is no Epoch3.

The existing collector partition mechanism is reused. A new segment has a fresh
collector file with existing schema, a shared catalog/global ID allocator and
unchanged shared provider budgets. Original Epoch2 remains the pre-clean physical
partition. Its 66 gaps retain all original row values and remain unresolved;
upper bounds remain unknown. PRE_CLEAN_INCIDENT_DEBT is an additive exact-row
snapshot plus SHA256 and original partition identity. Diagnostics independently
read that old file and check preservation. Historical switch17 remains in its
separate quarantined epoch with FAILED state and two unresolved ranges.

The CLI queues an exact request only. The live writer captures and publishes the
SEALED segment and adopts it itself; it cannot race an external process rewriting
the writer's old partition. Routes are retired before awaiting unsubscribe ACKs;
logical connection ownership is registered in the new file without a reconnect.
Publication never overwrites an existing file. An interrupted orphan requires
inspection, not overwrite. A capture older than 60 seconds is rejected, not reset.
Failed preparation is durable and nonhealthy. Publication/status commands default
to read-only and do not create tables or partition files.

Current research health reads the new partition only after explicit catalog
ownership. It blocks on required gaps, pending uncertainty, bootstrap, tail,
switch proof or unbounded failures. Old debt is not globally ignored or resolved;
it remains visible by its retained partition and catalog snapshot. Generic old
partition behavior is retained for compatibility until opt-in; no clean research
claim is permitted for that failed pre-clean partition.

## PIT and maturity

Prospective PIT proof includes research_segment_id. Rows must have target time
and launch block at/after the exact boundary, and applicable feature cutoff after
it. They require healthy current collection, bootstrap, contiguous proof and
subscription tail. Existing immutable rows are never updated. Missed-cutoff or
expired forensic repair remains descriptive and cannot backdate eligibility.
Target309266 30s/60s/300s and other incident-era rows stay in the old partition.

SEALED_POSTSTART_BOOTSTRAP supplies independent discovery/bootstrap/ACK-tail proof
before ACTIVE. ACTIVE alone does not establish clean research time. VALIDATED
requires an actual fresh immutable first-version eligible append after the seal,
matching segment identity, payload hash, complete filter proof, retained contiguous
jobs and real availability before label observation. Only then does the retained
Validation block UTC become EPOCH2_RESEARCH_CLEAN_START. Both operational Epoch2
start and arbitrary process restart time remain separate.

The read-only ledger audit follows the active segment partition and requires its
VALIDATED identity for primary counts. Epoch1, Epoch2 pre-clean and reconstructed
incident rows are not pooled. Existing requirements remain: 600 safe 60s-to-24h
rows, 60 elapsed clean days AND a 60-day eligible chronological span, chronological
60/20/20 split, 120 holdout rows, and 20 below-one/20 above-one in each slice.
No readiness state authorizes modeling or trading in this task.

## Future activation plan — NOT EXECUTED

1. Obtain separate operational authorization. Save exact clean deployed HEAD,
   service PIDs/start identities, Epoch2 row, all 66 gap rows, immutable311 current
   versions and1004 historical versions, protected272959, switch17/13/14,
   configuration permissions/fingerprints, integrity and current budgets. Verify
   routing unchanged, Validation getLogs reserve at least50, limits400/1000/12,
   and the offline config. Boundary requires three RPC members minimum; bootstrap,
   retries/log timestamp headers and ACK tails consume the existing shared budget.
2. Restart **meme-scanner-flow.service exactly once**, never main. Retain durable
   before/request/after evidence and exact new PID/start times. Assert main64326/0
   unchanged. State only that the new process started from the expected clean
   checkout/ExecStart/working directory, absent runtime revision instrumentation.
3. Queue one fixed segment ID with the captured NEW_FLOW_PID from step2:

   ```sh
   cd /opt/meme-scanner
   sudo -u ubuntu .venv/bin/python scripts/flow_research_segment.py \
     --request-segment EPOCH2_RESEARCH_CLEAN_1 \
     --expected-flow-pid NEW_FLOW_PID \
     --expected-epoch GENERALIZED_BOOTSTRAP_EPOCH_2
   ```

   NEW_FLOW_PID is a required captured value, never a guessed/static old PID.
   The writer verifies PID, clean source and parent epoch. Do not issue duplicate
   requests or restart again. Its budget pause resumes the same request/capture;
   stale capture or permanent failure blocks and requires inspection.
4. The writer captures fixed Validation chain4663 head and exact matching fresh
   header UTC/hash. It snapshots the original66 rows as PRE_CLEAN_INCIDENT_DEBT,
   preserves them unresolved, creates the new partition and publishes SEALED.
   The capture occurs before atomic publication so SEALED never has an invented
   boundary. Verify parent Epoch2 is unchanged and original snapshot hash matches.
5. It discovers current relevant targets, proves curve/V4/hook fixed bootstrap
   intervals from real filter activation, acknowledges subscriptions, and proves
   bounded contiguous readiness tails. Inspect actual jobs, identities, range
   continuity and proof completion before cursors, not flags alone. Verify every
   new normal required gap has a contract and no UNBOUNDED_CURRENT_GAP exists.
6. Only the proof verifier can publish ACTIVE. Observe natural new post-boundary
   targets and fresh immutable PIT without synthesis or backfill. Independently
   verify actual first eligible append, cutoff/materialization/eligibility order,
   exact target/window/filter/segment identities, payload hash and retained jobs.
7. Only VALIDATED establishes EPOCH2_RESEARCH_CLEAN_START from the retained exact
   block UTC/number/provenance/revision and first-PIT evidence. Record that boundary
   in documentation/maturity aggregates and begin the60-day research clock. Until
   then keep it UNKNOWN. Recheck final health, all old row hashes, switch history,
   routing, budgets/security/Alchemy/txreceipt/integrity and service identities.
8. On concrete failure, retain evidence and report blocked. No automatic second
   restart, Epoch3, historical recovery, deletion, bound fabrication, rollback to
   pooling old rows, provider failback, budget increase, or eligibility repair.
   Older source does not understand segment ownership; reverting after publication
   requires a separately authorized compatibility review. Before publication this
   additive source remains compatible with the unchanged production database.
