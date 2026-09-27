# Phase 2B.2 provider split: shadow-first cutover

## Cutover-session lifecycle (Phase 2B.2.4)

The original `flow_shadow_meta` revision/PID pin and unprefixed
`flow_shadow_jobs`/`flow_shadow_ranges` belong to the rolled-back attempt.
They remain unchanged. `archive-legacy` explicitly imports their identity and
SHA-256 proof digest as a terminal `ROLLED_BACK` row in the additive
`flow_cutover_sessions` table. `flow_cutover_legacy_proof` maps each old job and
range to that session without editing the original rows. The import requires
the old PID to be gone, Alchemy to be active, and the historical and stop-tail
jobs to be complete. It is idempotent.

`status` opens the flow DB read-only and makes no RPC calls. It reports the
historical terminal sessions, any active session, current Git revision, service
PIDs, split flag, provider, and fresh proof counts. A revision mismatch on a
terminal session is informational; an active-session mismatch blocks further
operator work. `new-session` requires a clean deploy, both healthy services and
databases, legacy Alchemy route, split disabled, completed bootstrap, zero active
gaps, service-user-readable protected config, and a terminal prior session.
Its transaction and partial unique index allow at most one active session.
`migrate-session-schema` installs the additive active-state index before a new
attempt; it preserves the rolled-back historical row and its proof digest.
The new `CREATED` record pins Git revision, source PID/start time, main PID,
and provider-role fingerprint; all H values, target snapshot, and proof are empty.
Any pre-stop attempt can be explicitly `abort-session --reason '...'`ed while
the bound Alchemy source is still running and split is false. Its shadow proof
remains immutable in the terminal `ABORTED_PRE_STOP` session. These commands
do not stop services, switch providers, or query chain RPCs.

Future shadow, stop-tail, ready-tail, and rollback proof uses stage and metadata
keys prefixed `cutover:<session-id>:`. The legacy unprefixed ranges can never
satisfy a fresh session. The target bootstrap ledger retains its separate
proof semantics. Before flow stop, the bound source PID and start time must
match. After stop, absence of that PID is expected; split and rollback PIDs
are recorded independently. A fresh shadow captures only filters active at
that time, with completed bootstrap and recovery cursors. It does not invent
a filter when none are active.

For inspection and explicit lifecycle management only:

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/phase2b2_shadow.py status
.venv/bin/python scripts/phase2b2_shadow.py migrate-session-schema
.venv/bin/python scripts/phase2b2_shadow.py archive-legacy
.venv/bin/python scripts/phase2b2_shadow.py new-session
.venv/bin/python scripts/phase2b2_shadow.py abort-session --reason 'target expired'
```

`prefetch`, `preflight`, and the cutover commands remain separate, gated
operations; creating a session is not authorization to run them.

## Durable startup handoff (Phase 2B.2.3)

The failed 2026-09-26 cutover exposed a gate cycle: split WSS connected and
subscribed, but ordinary 100-block startup recovery ran before the operator's
Validation HTTP ready-tail. Its pinned range was 73,211,566..73,211,741
(176 blocks), so it correctly reported `unrecoverable_gap`; the old ready-tail
command then refused that status. The 100-block normal limit remains unchanged.

Successful stop-tail now advances the explicit active session in the flow
database, pinned to fresh migration proof, `H_prefetch`, `H_stop`, provider roles,
and target/filter snapshot. Its states are `STOP_TAIL_VERIFIED` →
`SPLIT_CONFIGURED` →
`SPLIT_WSS_CONNECTING` → `SUBSCRIPTIONS_READY` → `READY_TAIL_PENDING` →
`READY_TAIL_VERIFIED` → `NORMAL_CONNECTED`. The split worker subscribes and
stores canonical WSS events while pending, but defers ordinary startup recovery,
cursor advancement, and feature finalization. `ready-tail` accepts only the
exact pending session after a post-ACK subscription sample, validates the
service-user config and provider, pins one `H_live`, and resumes durable
Validation HTTP chunks. It commits cursor/gap proof and the verified state
together. The worker then enters normal connected operation. A crash before
proof resumes pending; a crash after proof observes the committed state. A
disconnect, provider change, filter change, or new active gap fails the session
and requires flow-only rollback. With no active cutover session, ordinary
startup recovery is unchanged.

Gap IDs 238–240 from the failed retry remain durable evidence until separately
reconciled. Once the shared getLogs budget permits one bounded range plus three
attempts and the 50-call reserve, run the pinned cleanup command on the clean
deployed revision while legacy Alchemy flow remains active:

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/phase2b2_cleanup_gaps.py --gap-ids 238,239,240 --head 73212783
```

The command checks the fixed failed-cutover ledger identity, verifies the
candidate block timestamp covers all three gap ends, queries only the missing
Validation HTTP interval after `H_stop`, and resolves named expired-target gaps
only when all three contiguous stages prove coverage. If the budget gate fails,
leave services and gaps untouched. This cleanup is not a provider cutover.

Production cutover remains gated. The deployable source is the clean Git checkout at
`/opt/meme-scanner`; `/tmp/meme-scanner-phase2b2-test` is **not** deployment
authority. `scripts/phase2b2_shadow.py` refuses a non-Git or dirty source and
pins its exact revision in the migration ledger. Run the full Ubuntu suite from
that checkout before any reconciliation. Required ancestor commits are checked by
the script. Do not restart `meme-scanner.service`.

The current flow process remains on Alchemy while Validation HTTP reconciles
historical per-target/filter ranges. The protected `config/flow-rpc.env` holds
Validation endpoints; `config/flow.env` retains
`FLOW_PROVIDER_SPLIT_ENABLED=false` until cutover. Shadow reconciliation uses
targeted `eth_getLogs` queries of at most 2,000 blocks and halves rejected
ranges. It records each actual call in the shared 400/day ledger before sending,
and stops at 350/day to preserve 50 calls. The ordinary flow process still has
the independent 100-block startup safety limit and 10-block request chunks.
No transaction or receipt lookups are part of reconciliation.

Run `prefetch` while the old flow service is active:

```sh
cd /opt/meme-scanner
.venv/bin/python -m unittest discover -s tests -q
.venv/bin/python scripts/phase2b2_shadow.py prefetch
```

The script writes only the canonical flow DB and a durable, migration-only ledger.
It keeps successful and failed range boundaries, call/retry/reduction counts,
and deduplicates events by the existing chain/transaction/log key. An interrupted
range is replayed from the last contiguous verified block. If the result is
`MIGRATION_BLOCKED`, leave both services and routes unchanged. Resume `prefetch`
on a later UTC day. The historical gate needs every active filter complete to one
common `H_prefetch`, with zero unresolved historical ranges.

Immediately before cutover, run:

```sh
.venv/bin/python scripts/phase2b2_shadow.py preflight
```

This checks the pinned Git revision, old flow PID continuity, both DB integrity
checks, all four provider chain identities (4663), current Validation head, and
a network-free startup probe run as the installed flow unit's service user.
It estimates only `H_prefetch+1..H_pre_stop` with the smallest observed successful
non-terminal chunk and three attempts per query. At least 50 calls must remain
after this estimate. If the result is `MIGRATION_BLOCKED`, keep the old flow
running; `prefetch` can extend the common head before another preflight.

Only after `CUTOVER_PREFLIGHT_PASS`: capture service PIDs/restart counts, event
and feature totals, budget, and recovery ledger. Keep the old Alchemy flow
running with split disabled while validating the candidate offline and recording
an exact, durable stop authorization:

```sh
.venv/bin/python scripts/flow_config_status.py --candidate-preflight
.venv/bin/python scripts/phase2b2_shadow.py authorize-stop
.venv/bin/python scripts/phase2b2_shadow.py stop-flow
.venv/bin/python scripts/phase2b2_shadow.py stop-tail
.venv/bin/python scripts/flow_config_status.py --enable-split
.venv/bin/python scripts/flow_config_status.py --preflight --require-split
```

`authorize-stop` binds the source PID/start, revision, active-filter snapshot,
head, budgets, provider roles, and safe candidate fingerprint. `stop-flow`
rechecks them immediately before stopping. A changed target, bootstrap, gap,
budget, config, or source refuses the stop. Its durable stop-command marker lets
an operator reconcile a crash after the original process exits without issuing
a second stop. Until `SOURCE_STOPPED`, the protected config must remain split
disabled. `stop-tail` then proves the bounded gap on Validation HTTP.

Only `STOP_TAIL_VERIFIED` permits `--enable-split`. It checks that the bound
source is gone and the legacy config still matches the authorized candidate,
then creates the replacement inside protected `config/`, explicitly
sets the installed service user's uid/gid and mode 0600, fsyncs it, atomically
replaces the file, fsyncs the directory, and tests readability as that user.
The resulting safe fingerprint must match the authorized one before
`SPLIT_CONFIGURED` is persisted. A failed mutation restores the legacy flag.
Do not use a direct `systemctl stop` for cutover. A stop-tail failure yields
`ROLLBACK_REQUIRED`; use the flow-only rollback below. Do not start the new
route with an unresolved stop tail or unconfigured split.

Install the verified tree's `deploy/meme-scanner-flow.service` as the flow-only
systemd unit, reload systemd, and start **only** `meme-scanner-flow.service`.
Its primary WSS is PublicNode,
fallback WSS is Validation, and HTTP recovery is Validation. Once PublicNode is
connected and active subscriptions have been acknowledged, run `ready-tail`.
It captures `H_live`, reconciles `H_stop+1..H_live`, and deduplicates against WSS.
Any unresolved range yields `ROLLBACK_REQUIRED`. The script does not stop or
start services, edit configuration, or deliberately induce provider failover.
The ready-tail promotion resolves only recovery gaps whose complete per-filter
block intervals are proved by all three durable stages. It then publishes a
connection-bound handoff marker; the subscribed worker accepts this marker
instead of replaying those same ranges against a later moving head. Older or
unproved gaps remain unresolved. A failed or interrupted promotion publishes
no handoff marker.

After ready-tail proof, new sessions enter `POST_CUTOVER_VALIDATING`; they do
not become `COMPLETE`. Observe at least 30 minutes. A connected PublicNode
primary or a fully proved Validation fallback is acceptable. Require zero
Phase 2B Alchemy HTTP requests, WSS connections, and WSS bytes; no unresolved
active or provider-switch gap; healthy bootstrap/recovery; raw event persistence
when events occur and feature progress when due; zero per-event transaction or
receipt lookups; both DB integrity checks `ok`; a clean security review; and
unchanged main PID/restart count, Phase 1.6, and Phase 2A. The 64,000,000
bytes/day emergency WSS ceiling remains in force; the old Alchemy 8 MB pause
does not apply to split routing. A reviewed acceptance record advances to
`SOAKING` and yields `READY_24H_SOAK`. Only a reviewed successful 24-hour soak
advances to `COMPLETE`. No Phase 2C or trading change.

PublicNode disconnects are recorded with connection and subscription markers.
The existing two-failure policy selects Validation WSS; there is no automatic
failback to PublicNode and no Alchemy fallback. After the new subscription ACK,
the flow worker freezes a Validation HTTP head, proves the uncertain interval
with bounded, adaptive, durable getLogs jobs, and canonically deduplicates
HTTP/WSS overlap. Only zero unresolved ranges and resolved gap IDs mark the
switch healthy. A switch with zero active filters records that fact and uses no
getLogs. Until proof finishes, feature completeness stays pending. A failed
switch, both providers unavailable, repeated provider flapping, or an active
unresolved gap blocks acceptance. A future failback must use this same proof.

The 24-hour review records PublicNode disconnects/reconnects, fallback
activations, time on each provider, switch recovery calls/events, unresolved
ranges, failbacks/flapping, WSS bytes by provider, Validation HTTP usage, and
zero Phase 2B Alchemy traffic. The provider switch report and usage report
expose these fields without making RPC calls. The historical `a330fee593134e51b65511ce6f07d2e7`
handoff remains `COMPLETE` under its old semantics; its later operational
rollback is stored in a separate append-only outcome row. No H_* or proof row
is changed. New sessions can record rollback intent during validation or soak;
after flow-only legacy restoration, a connected Alchemy route with healthy
recovery and zero active gaps records the rollback PID and reconciliation proof.
The source-only schema step is `scripts/phase2b2_shadow.py migrate-session-schema`;
it requires both legacy services active and split disabled. On the known
historical rollback, `record-historical-rollback --session-id
a330fee593134e51b65511ce6f07d2e7 --reason
primary_wss_validation_policy/provider_disconnect` appends its outcome only
after read-only DB/service checks. Future reviewed 30-minute and 24-hour
decisions use `accept-validation --evidence-file <reviewed-json>` and
`complete-soak --evidence-file <reviewed-json>` respectively. These commands
perform no operator RPC calls and reject unhealthy routing, unresolved switch
proofs, or Alchemy traffic. Never infer a provider close cause from a generic
`RpcError`; the September 27 journals did not include a close code or reason.

## Missing-cursor proof bootstrap (legacy route)

A target with no `recovery:<launch>:<filter>` cursor has unknown HTTP completeness.
New curve targets record `flow_bootstrap=required` at activation; a graduation
adds separate V4 and hook requirements. The initial/long cohort flag is fixed
from the canonical outcome target at discovery and does not reset a cursor.
The worker may persist WSS events while required proof is pending, but a
bootstrap gap and feature check prevent a complete feature claim. A short
normal recovery commits a previously missing cursor only after its entire
activation-to-head range succeeds. Existing cursors retain the 100-block
startup bound. Expired targets retain their bootstrap state and gap evidence.

`scripts/flow_bootstrap_missing_cursors.py` runs with both services active,
the split disabled, and Validation HTTP as its only bootstrap RPC. It reuses
the canonical shadow parser, 2,000-block adaptive ranges, per-attempt shared
accounting, 12/minute and 1,000/day limits, 0.5 HTTP envelopes/second, and
the 50-getLogs-call reserve. Its stage ledger commits each successful range,
including empty ranges, before any normal cursor. Failure leaves that cursor
unchanged. Re-running an incomplete stage starts at the first unverified block.

For the five targets discovered while the old daily getLogs allowance was
exhausted, retain their IDs explicitly even if they have since expired:

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/flow_bootstrap_missing_cursors.py --mode bootstrap \
  --include-expired-ids 256799,256888,256894,256895,256918
```

The tool freezes one Validation head. A curve starts at its launch block; V4
and hook filters start at the proven graduation block. An expired target ends
at the first canonical main-scanner launch block timestamped after its tracking
window, avoiding hours of irrelevant history. The ledger and gap rows remain
after completion. No service restarts are part of this command.

For moving-head catch-up, run successive numbered stages while flow remains
online: `--mode tail --stage cursor_tail_1`, then
`cursor_tail_2`, and so on. A pending stage must be resumed with the **same**
name. Each new stage captures a fresh common head and includes current active
filters, including any newly required bootstrap. Run `--mode preflight`
afterward. Its `safe_now` flag requires all active filters to have cursors,
the actual normal recovery plan to fit 100 blocks, and no cursor more than
60 blocks behind the observed head. A later restart still needs a fresh,
immediate preflight, unchanged main PID, clean tested source, healthy DBs,
readable protected config, and remaining recovery budget. If any check fails,
leave the old Alchemy flow process running.

Rollback before a flow restart is simply to stop the operator tool: proof
rows, raw events, and unresolved bootstrap gaps remain durable and resumable.
Do not delete rows or set a cursor to an unproved head. The provider cutover
and Phase 2C remain separate decisions.

## Flow-only emergency rollback

If cutover fails, stop only `meme-scanner-flow.service`; restore the protected
pre-cutover flow source/config/unit copies under
`/opt/meme-scanner/rollback/phase2b2`, then reload systemd and start only the
flow service on its prior Alchemy route. Reconcile or explicitly mark the
stop/start gap. Keep the exact failed ledger and report `ROLLBACK_REQUIRED`.
Never restart the main service. The preserved copies must be checked before
cutover; their presence does not make `/tmp` an acceptable source.

The 2026-09-24 20-minute provider benchmark established complete PublicNode
and Validation WSS capture against Validation HTTP for the tested curves.
That benchmark did not authorize the production migration.
