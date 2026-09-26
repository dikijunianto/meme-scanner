# Phase 2B.2 provider split: shadow-first cutover

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
checks, all four provider chain identities (4663), and current Validation head.
It estimates only `H_prefetch+1..H_pre_stop` with the smallest observed successful
non-terminal chunk and three attempts per query. At least 50 calls must remain
after this estimate. If the result is `MIGRATION_BLOCKED`, keep the old flow
running; `prefetch` can extend the common head before another preflight.

Only after `CUTOVER_PREFLIGHT_PASS`: capture service PIDs/restart counts, event
and feature totals, budget, and recovery ledger; stop **only**
`meme-scanner-flow.service`. Run `stop-tail` immediately. It rechecks the budget
against the actual `H_stop`, reconciles through that head, and promotes only
verified filter cursors. A failure yields `ROLLBACK_REQUIRED`; use the flow-only
rollback below. Do not start the new route with an unresolved stop tail.

Set `FLOW_PROVIDER_SPLIT_ENABLED=true` in protected `config/flow.env` (mode 0600),
install the verified tree's `deploy/meme-scanner-flow.service` as the flow-only
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

After both tails are verified, observe at least 30 minutes. Require the new
flow process to report zero Alchemy HTTP requests, WSS connections, and WSS
bytes; PublicNode WSS bytes and Validation HTTP/getLogs calls must be recorded.
Confirm new raw events and feature targets progress, zero per-event
transaction/receipt lookups, no legacy 8 MB pause, no journal credential leaks,
both DB integrity checks `ok`, main PID/restart count unchanged, and Phase 1.6
and Phase 2A healthy. Fallback WSS remains test-verified unless natural failover
occurs. Only then report `READY_24H_SOAK`. No Phase 2C or trading change.

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
