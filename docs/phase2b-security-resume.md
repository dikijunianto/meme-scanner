# Phase 2B security rotation and safe resume — 2026-09-21

**READY_KEEP_COLLECTING** — security/operations gate passed. This does not claim full event coverage or Phase 2C readiness.

| # | Evidence | Result |
|---|---|---|
| 1 | Final state | READY_KEEP_COLLECTING |
| 2 | Implementation | `57912c9`; this report is a documentation-only follow-up |
| 3 | Tests | TEST VERIFIED: original baseline 91; post-rotation full Ubuntu suite 103 passed; security subset 12 passed |
| 4 | Main | LIVE VERIFIED: active/enabled |
| 5 | Flow | LIVE VERIFIED: active/enabled, connected |
| 6 | Main PID/restarts | 842555 before deliberate credential reload; 64326 after; NRestarts 0 throughout |
| 7 | Flow PID/restarts | 66027 after deliberate isolation stop/start; NRestarts 0 |
| 8 | Old fingerprint | `831e6b644134` |
| 9 | Replacement fingerprint | `5ce290407fb3`, shared HTTP/WSS |
| 10 | Revocation | Operator confirmed provider revocation. LIVE VERIFIED: one old-key chainId request returned HTTP 401 / RPC -32600; replacement returned HTTP 200 / chain 4663. Provider administrative metadata unavailable |
| 11 | Secret paths | `/opt/meme-scanner/config/.env` ubuntu 0600; parent 0700. `config/flow.env` 0600, control fields only. Archived old credential `data/phase15-backup/config-before.env` retained 0600, parent 0700; never use it to restore credentials |
| 12 | Git | Preparation history scan: 176 reachable objects / 116 blobs, zero candidate credential findings. Secret files ignored/untracked; no history rewrite. Report additions contain no credentials |
| 13 | Historical journal | Old secret-bearing records remain: 78 flow records on September 20, 08:55:37–12:14:08 UTC. No journal deletion |
| 14 | Live journal | LIVE VERIFIED: September 21 14:48:07–15:23:45 UTC, both units: zero credential URL matches, authentication patterns, client INFO URL records, traceback URL matches. Additional minute monitor checked exact replacement key in memory; zero matches |
| 15 | Logging regression | TEST VERIFIED: HTTP/WSS success/failure protections and safe installer errors pass |
| 16 | Replacement main validation | LIVE VERIFIED: HTTP and WebSocket chainId checks passed without retries; main reloaded at 14:48:07 UTC, connected and ingested data before old-key revocation. Resolved config fingerprint plus restart and continued activity after old-key rejection prove switchover operationally; process memory was not dumped |
| 17 | Main/Phase 2A | LIVE VERIFIED benchmark deltas: +356 launches, +3 graduations, +11 market snapshots, +61 outcome targets |
| 18 | Flow dry validation | LIVE VERIFIED before enablement: flag false, shared replacement configured, separate DBs, offline validation passed, zero validation RPC calls |
| 19 | Flow enable time | 2026-09-21 14:53:20 UTC, after replacement/old-key/tests/log/integrity gates |
| 20 | Benchmark | LIVE VERIFIED: 14:53:28.485235–15:23:28.500553 UTC, 1800.015 seconds, 61 resource snapshots |
| 21 | Targets/subscriptions | Four targets created within exact benchmark window (all initial and long cohort); six created since enablement, including two before benchmark start. Average active targets/subscriptions 3.65, peak 5; curve peak 5, V4/hook 0 |
| 22 | Raw events | Three new: 2 curve buys, 1 curve sell; V4 swaps/hook events 0; duplicates 0, removed 0. Total persisted rows 8563; prior 8560 preserved |
| 23 | Feature finalizations | Benchmark report: 11 complete, 2 partial, 13 unavailable. Includes finalization of existing targets, not just newly sampled targets |
| 24 | HTTP | 35 envelopes/members; 21 getLogs, 7 headers, 3 metadata eth_call, 4 blockNumber; transaction/receipt lookups 0. Three budget pauses / failed-request counters, zero observed authentication errors |
| 25 | WebSocket | 3371 notification bytes; 0.04% of unchanged 8,000,000/day limit. Two connection counters include deliberate isolation start/stop |
| 26 | Budget pause | No WS-budget pause. Startup recovery encountered existing HTTP minute limits; coverage gaps retained. Daily members 35/1000, getLogs 21/400 |
| 27 | Resources | Main RSS/peak 56,188,928 bytes; flow RSS/peak 54,689,792. End combined cgroup memory 84,672,512 bytes. CPU time during benchmark: main 1.850s (~0.103% one core); flow 43.286s (~2.405% one core) |
| 28 | DB integrity | LIVE VERIFIED after benchmark: main ok, flow ok. No schema changes/migrations or new DB backups |
| 29 | DB growth | Flow allocated DB+WAL +4,024,736 bytes between benchmark endpoints. Internal flow gauges span 1770s and show +3,361,416 bytes; different sampling boundaries. Main DB byte growth not captured for this exact window; logical growth above |
| 30 | Next review | Exact commands below |
| 31 | Limits | No provider billing/quota metadata or administrative rotation timestamp. Short, sparse sample; no fresh flow V4/hook proof. Partial/unavailable windows and nine unresolved recovery gaps remain. Minute-bucket RPC counters include startup seconds preceding benchmark. Do not extrapolate allocated WAL growth as stable raw-event storage cost |

## Failure isolation and preserved controls

LIVE VERIFIED: stopping flow left main active with PID 64326 and NRestarts 0; starting flow did not restart main. Main retained those values through the benchmark.

No sampling, event semantics, recovery logic, Phase 2A logic or budgets changed. Limits remain 1000 RPC members/day, 12/minute, 400 getLogs/day, 0.5 HTTP envelopes/second, 8 MB WS/day, 64 subscriptions and 100 blocks per recovery range. Reported 196 recovered blocks are cumulative across bounded queries, not one enlarged range. No paid services, signing or trading added.

The temporary benchmark and minute security monitor are bounded operational checks, not permanent monitoring. Historical records remain sensitive even though the old key now fails authentication. The earlier manual handoff remains a dated preparation record, superseded by this successful rotation evidence.

## 24-hour review

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/flow_usage_report.py --hours 24
.venv/bin/python scripts/rpc_usage_report.py --hours 24
.venv/bin/python scripts/market_usage_report.py --hours 24
.venv/bin/python scripts/flow_security_status.py --since '24 hours ago'
```

Use `--since '2026-09-21 14:48:07 UTC'` to audit only the replacement-key period. These local reports do not represent Alchemy billing or remaining monthly quota.
