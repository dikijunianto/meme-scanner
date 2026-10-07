# Expired current-epoch forensic recovery

Source/test/documentation release. No service actions, production recovery,
boundary enrollment, RPC or research-clock establishment are executed here.

The old guard in `flow_gap_recovery.obligations` marked every expired target
`operator_blocked/target_expired_required_proof_retained`; `recover` returned
before planning. `FlowWorker.reconcile` also excludes completed/partial targets.
Those guards prevented resurrection, new subscriptions and retrospective feature
availability. Keep the normal live path and its initialized-cursor100-block guard.

`EXPIRED_GAP_FORENSIC_RECOVERY` is separate existing-gap work. It selects only the
CURRENT ACTIVE epoch partition. Its fixed bounds, epoch, original gap row,
expiry/status, lifecycle and canonical query/filter set are retained in the
existing `flow_state` table. The lower bound is the durable gap first_block,
clipped only to the filter's exact lifecycle activation block. The upper bound
must come from two independently Validation-verified adjacent headers bracketing
the fixed uncertain end (capped at target expiry). Equal-timestamp blocks require
the last block at/before that end, followed by a strictly later timestamp.
Neither wall-clock estimation nor a new moving head is accepted. Missing/changed
bounds, class, epoch, query or lifecycle explicitly block the gap without RPC.

Only retained ordinary ws/reconnect and required curve/V4/hook bootstrap gaps
are supported. Unrelated reorg/unsupported semantics still require review.
The old bootstrap point obligation can mark its exact expired bootstrap metadata
complete only after full proof from its safe_start, without promoting a live
cursor, other gaps, target lifecycle or subscriptions.

Reuse `compatible_ranges` across bootstrap/normal/switch/tail/forensic stages in
the same partition and lifecycle. Address/topics/kind/target must match; overlap
alone is insufficient. Inventory coalesces only identical proof identities.
Execution reuses committed overlapping proof before each chunk, preserves its
own immutable bounds and contiguous progress, and queries only missing pieces.
WSS observations are never coverage proof.

Existing ShadowReconciler supplies Validation-only canonical parsing, chain/log
dedupe, event commits, range proof, retry/reduction bookkeeping and durable jobs.
Forensic stages never capture eth_blockNumber. Each tick runs at most one chunk,
after live bootstrap/recovery and current provider-switch work. Budget waits are
nonterminal with reset_at; progress survives process reopen and UTC rollover.
Oldest ready debt progresses deterministically when discretionary budget exists;
blocked/sleeping debt cannot orphan later ready work. Diagnostics are
expired_forensic_pending/in_progress/budget_wait/blocked. Budgets stay400
getLogs/day,1000 RPC members/day,12/minute,0.5 envelopes/sec, reserve50 getLogs.
Retries/reductions/missing log timestamps can raise actual costs above minimum.

Gap resolution follows canonical event commits, persisted contiguous proof and
identity revalidation. No expired target is subscribed, reactivated or extended;
no live cursor is advanced. Historical target metadata/event completeness may
improve. Forensic parser rejection fails the fixed job rather than generating
new live gap obligations. Previously eligible immutable versions are untouched.
An expired_forensic_target marker prevents *new* forensic/descriptive versions
from becoming retrospective first eligible, including previously absent windows.
Original30926630/60/300s remainNULL. Live new targets still bootstrap/materialize
while backlog exists; global clean/PIT eligibility remains gated by current debt.
Quarantined epoch1 switch17 is never selected or repaired.

## Read-only inventory

From `/opt/meme-scanner`:

```sh
.venv/bin/python -m scripts.expired_gap_recover
```

An isolated source copy can use `--database /opt/meme-scanner/data/flow.db` only
for read-only inventory. There is no database override for boundary enrollment.
The inventory reports targets/classes/filter identities, exact or missing bounds,
compatible retained prefixes, affected windows, coalesced unique positions/calls,
and minimum budget days. UNKNOWN is never encoded as a zero total cost.
Current audited inventory:66 gaps/14 expired targets,52 ws_gap,13 reconnect,
1 bootstrap_required:curve. All66 lack the exact upper boundary evidence;
total unique positions/getLogs/RPC/day estimates are UNKNOWN. Retained compatible
prefixes do not prove the missing upper interval. No production bounds enrolled.

## Future operational procedure — NOT EXECUTED

1. Read the current production inventory and immutable/service/epoch/routing/
   security baselines. Preserve main64326/0, epoch2 operational boundary/seal,
   old switch17FAILED/two gaps and all immutable rows. No epoch rollover/epoch3.
2. For each missing bound, obtain the exact candidate upper block from retained
   archival boundary evidence. Do not estimate a number from time/cursor/WSS.
   If unavailable, STOP with explicit missing-bound debt; do not restart merely
   to make the refusal disappear. A separately authorized boundary investigation
   is required. The following explicit opt-in command independently validates
   ONLY that block and its successor and persists fixed binding on success:

   ```sh
   .venv/bin/python -m scripts.expired_gap_recover \
     --retain-verified-boundary GAP_ID --upper-block EXACT_RETAINED_BLOCK \
     --expected-epoch GENERALIZED_BOOTSTRAP_EPOCH_2
   ```

   Placeholders must be replaced by retained values. This is an authorized future
   production write/two-header RPC operation, not part of this source release.
   Re-enrollment is idempotent without new RPC; changed bounds are refused.
3. Re-inventory once exact inputs exist. Budget current/startup/tail/fresh work,
   boundary-validation overhead (at most132 initial headers for66 gaps), then
   coalesced missing forensic work, retries/reductions/header overhead. Historical
   switch17's2514 calls are excluded. Keep reserve50. UNKNOWN cost means STOP;
   >350 minimum getLogs requires multi-day recovery, not higher limits.
4. Only under separate activation authorization, record durable restart request,
   confirm no prior4m/new-task restart and exact clean newest checkout, and use
   exactly one `systemctl restart meme-scanner-flow.service`. Never restart main;
   no second restart for pending/budget waits. Verify sole new worker PID/start/
   ExecStart/WorkingDirectory, main unchanged. Claim started from expected clean
   checkout; no stronger runtime revision assertion without instrumentation.
5. Immediately require nonhealthy while ANY current active or expired required
   debt exists. Active targets use the normal scheduler; exact-bound expired
   targets use forensic jobs. Monitor durable state/ranges, not healthy telemetry
   or COMPLETE flags alone. On unexpected health or identity failure STOP.
6. Preserve fresh live collection priority. Let one discretionary forensic chunk
   progress per tick; inspect diagnostics and retry_at. On budget pause wait for
   its UTC/minute reset without service actions, moving bounds or failed status.
   Inspect exact job next_unverified_block/ranges and reused intervals on resume.
7. Verify all current gap intervals proved, events committed/deduped, then gaps
   resolved. Verify all required bootstrap/job/tail work complete, correct ACK/
   startup/downtime discovery continuity and naturally derived healthy. Original
   missed windows and old first-eligible hashes/timestamps must remain unchanged.
   A current incomplete job outside the supported forensic obligations is still
   debt; never declare it complete just to satisfy health.
8. Only after all current obligations are handled, choose candidate UTC as the
   latest durable proof/tail/health time and an exact durable chain/proof block
   at/after it. No estimated block; no operational/startup time substitution.
9. Observe natural fresh forward PIT for at most2 hours after that candidate.
   Verify exact launch/filter/range identity, cutoff>=candidate, immutable append,
   actual materialization/completeness/model availability and no backdating.
   No309266 missed-window success substitution. If absent, remain pending with
   research clean startUNKNOWN, no second restart.
10. After real fresh PIT validates the candidate, document the conservative
    EPOCH2_RESEARCH_CLEAN_START with exact provenance and use it for primary
    counters. Keep600safe60s24h/60elapsed clean days AND60-dayeligible span,
    60/20/20,120holdout,20below+20above per slice. No modeling/trading.
    Natural graduation is optional here; any claim needs explicit V4/hook proof
    before cursor and spanning-window immutable evidence.

This procedure is executable only with its required exact boundary inputs. The
present production inventory cannot pass its boundary/cost preflight; report
that limitation rather than invent inputs or promise a one-day recovery.
