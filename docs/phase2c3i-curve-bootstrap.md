# Required-filter bootstrap, Phase 2C.3i

This revision changes source only. No production stop/start/restart or drain is
authorized by this implementation task.

## Root cause and classification

Previously `eligible_launches()` read persisted main `launches` and
`outcome_targets` cohort membership; `discover()` inserted a deduplicated flow
target, then `ensure_bootstrap_state()` persisted curve `required` and its gap.
`reconcile()` subscribed and selected `bootstrap_graduated()` only for missing
V4/hook cursors. A missing curve cursor therefore reached `recovery_plan()` and
its unchanged100-block range guard. Even a101-block first proof was misclassified
as ordinary recovery.

`FlowDB.needs_bootstrap(launch, kind)` is now the shared cursor-absence predicate.
The caller supplies required filters: current curve or V4/hook, plus the retired
curve needed for graduation-spanning feature windows. Worker, explicit bootstrap
and offline drain use the same predicate. `recovery_plan()` and
`recover_plans()` refuse any missing cursor, including short curve intervals.
The ordinary100-block guard applies only after a cursor exists and is unchanged.

## Proof lifecycle and scheduling

`discover()` → required filter/gap → `bootstrap_missing()` → existing
`CursorBootstrap`/Validation-only `ShadowReconciler` → frozen head/header →
durable contiguous range proof → bootstrap complete → monotonic cursor →
satisfied bootstrap/reconnect gap reconciliation → ordinary recovery.

Curve safe start is launch block; canonical parsing excludes earlier log
positions within that block. Curve ends at its graduation boundary; V4/hook
begin at the proven graduation block and exclude positions at/before the
graduation log. Required feature-filter identities and PIT rules are unchanged.

Ranges start at the existing2000-block bound and adapt to provider range errors.
Canonical events deduplicate on the existing identity; successful empty ranges
are durable proof. There are no transaction/receipt requests or HTTP fallback.
RPC pacing and minute/day/getLogs ceilings remain unchanged.

Live reconciliation allows one completed proof chunk per target per turn and
rotates after targets that consumed getLogs attempts. This bounds work per tick
and shares scarce budget. Budget/provider waits retain required/in-progress state
and conservative cursors, with the existing30-second bootstrap retry delay;
they do not create normal-range rejection gaps. Other targets can progress.

Live curve stages use `live_bootstrap:LAUNCH_ID`; graduation retains existing
`live_graduation:LAUNCH_ID` keys for interrupted-proof compatibility. Head and
progress persist in existing shadow/bootstrap tables and survive worker restart.
Retries begin at the first unverified block. Cursor establishment requires full
contiguous frozen-interval proof, including zero-log chunks. Expired targets
become historical partial/unavailable; their proof remains and they are not
reactivated or assigned an optimistic cursor.

Provider-switch recovery remains a separate gate/snapshot/stage. Reconciliation
waits behind pending/failed switches; bootstrap does not certify switch health.
Existing connection-generation, guarded failed-switch and zero-filter semantics
remain covered by the regression suite.

## Discovery and offline integration

Live, startup-active and downtime-discovered targets share this path. Main launch
rows and persisted random_initial/random_long flags remain authoritative; no
independent sampling or synthetic targets. Discovery re-queries a3700-second
lookback; active cohort windows remain900/3600seconds. Expired/unselected launches
remain excluded. Nonexpired downtime curves can bootstrap over100blocks.

Offline drain uses the same classification and retains its exclusive writer
lock, inactive-flow guard, Validation-only routing, compatible durable proof
reuse, resumable manifest, catch-up/readiness checks and50-getLogs reserve. No
new schema or migration is required. Original historical rejection/gap evidence
is preserved; no PIT version/first eligibility is rewritten or backdated.

Deterministic fixtures cover exact101/500/5000/35000-block curves, an initialized
101-block ordinary violation, durable discovery, bounded multi-target turns,
partial/restart reuse, log-position dedupe, expiry, budget waits, PIT append times,
switch isolation and offline10k–35k debt followed by a downtime launch discovered
by a new worker. All provider responses are mocked.
