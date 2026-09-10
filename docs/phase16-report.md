# Phase 1.6 deployment evidence

Deployed on 2026-09-10 UTC using the configured private provider endpoints. The official
Robinhood RPC remains only an explicit connectivity fallback and is never selected by the
service.

The database was backed up before migration at
`/opt/meme-scanner/data/phase16-backup/scanner-before.db`; the backup directory is mode
`0700` and its contents are mode `0600`. The additive migration records
`phase16_migrated_at` and `phase16_telemetry_started_at`; it makes no destructive schema
change. Rollback is: stop the service, restore the SQLite backup with SQLite's backup API,
restore `data/phase16-backup/code-before.tar.gz`, then start the prior verified service.

The WS-first service subscribes only to the verified factory and the two Pons event topics.
It persists subscription events directly. Contract calls use the event block number;
steady events use their UTC observation timestamp and do not fetch headers. Startup,
reconnect, removed-log, and queue-overflow recovery are bounded filtered `eth_getLogs`
queries. An oversized recovery creates an explicit coverage gap and recovers only the
overlap tail. The initial rollout recorded `startup-gap` blocks `59602631..59607227`;
it was not hidden or automatically backfilled.

The final 20-minute uninterrupted measurement began at `2026-09-10T18:20:22Z` after the
last service restart. It processed 314 relevant WebSocket events (308 launches and six
graduations), with no reconnect recovery and no new intentional gaps. Local telemetry
projected the following daily rates; these are observations, not Alchemy billing data.

| Metric | Observed 20 min | Daily projection | Target |
|---|---:|---:|---:|
| HTTP envelopes | 107 | 7,704 | under 10,000 |
| `eth_getLogs` | 0 | 0 | under 2,000 |
| `eth_getBlockByNumber` | 0 | 0 | under 5,000 |
| `eth_call` | 220 | 15,840 | under 20,000 |

VPS verification passed: 32 tests, `systemctl` active with zero restart failures, SQLite
`integrity_check=ok`, zero duplicate launch identities, and zero pre-existing launches
missing compared with the backup. `scripts/show_coverage.py` reports the explicit gap
separately from WS-continuity segments.

Phase 2 is not implemented. This release does not add price data, liquidity, scoring,
signals, wallet operations, swaps, or trading.
