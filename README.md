# meme-scanner — Phase 1.5

Read-only Pons V2 launch collection on Robinhood Chain mainnet (4663), Python 3.12,
SQLite WAL, private provider HTTP and filtered WebSocket logs. No wallet, private keys,
signing, swaps, or trading.

## Verified integration

Research checked 2026-09-09; source snapshots can become stale.

| Item | Value / source |
|---|---|
| Mainnet chain ID | `4663` ([official network](https://docs.robinhood.com/chain/add-network-to-wallet/)) |
| Public HTTP RPC | `https://rpc.mainnet.chain.robinhood.com` |
| Explorer | `https://robinhoodchain.blockscout.com` |
| Provider alternative | Alchemy HTTP and keyed WebSocket documented in [Connecting](https://docs.robinhood.com/chain/connecting/) |
| Current V2 factory | `0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e` ([Pons docs](https://docs.ponsfamily.com/v2)) |
| Launch event | `TokenLaunched(address,address,address,address,uint256,uint256)`; first three addresses indexed |
| Asset identity | Chain-filtered deployments from the [official Stock Token API](https://docs.robinhood.com/chain/stock-token-apis/) |

The event emits token, curve, deployer, pairToken, launchConfigId and graduationThreshold.
Zero pairToken means native ETH. The bonding curve has its own column; no Uniswap
pool exists yet at launch. `graduations` records verified `PoolGraduated` events,
factory launch snapshots, and the canonical V4 PoolKey/PoolId (not a pool address).
See [graduation evidence](docs/phase15-graduation.md).
The minimal ABI and source links are in `app/pons.py`. Older V2 and V1 deployments
are excluded deliberately: their addresses/versions must be independently verified
before extending coverage. Historical fixtures are actual RPC logs, not synthetic examples.

Production requires `ROBINHOOD_RPC_HTTP` from a provider. Configure its private WSS
endpoint as `ROBINHOOD_RPC_WS`; credentials live only in the protected `.env`.
Factory/topic-filtered `logs` subscriptions are the default, avoiding the bandwidth
cost of every chain header. Production live mode rejects `newHeads` subscriptions.
Notifications coalesce into one wakeup. All launches and graduations in the live range
are recovered with filtered, bounded HTTP log ranges, including after disconnects.
Live startup begins at current head minus `LIVE_START_OVERLAP_BLOCKS=5`, then processes
through head minus three confirmations. Existing historical coverage remains paused.
Backfill does not pay for unused live notifications.
After bounded WS reconnect attempts, the same private HTTP endpoint supplies heads.
The official public RPC is optional basic connectivity only (`test_rpc.py --fallback`),
never an automatic production failover. Its sequencer feed is not `eth_subscribe`.

Requests are paced at five HTTP requests/second, with at most four in flight.
Metadata uses two-call batches; missing headers use batches of at most five. No batch
can exceed five members, keeping the logical-call rate within the user's stated
25 requests/second plan limit. Batching reduces HTTP overhead, not billed CUs.
HTTP 429/5xx and transport failures retry up to five total attempts, with exponential
backoff, jitter, Retry-After support, and a 60-second maximum. Exhaustion stops with
exit 3; systemd does not silently reset the retry budget. Review the provider/quota
and explicitly restart after resolving the cause. Logs omit URLs and provider bodies.

Alchemy Free supports only ten blocks per `eth_getLogs` query. `LOG_BLOCK_RANGE=10`
applies to historical and live scans; rejected ranges shrink, without skipping blocks.
The 30M monthly CU allowance is separate from request throughput. At approximately
10 blocks/second and 60 CU/query, complete ten-block HTTP log coverage alone projects
to 155.5M CU per 30 days. WebSocket notifications and enrichment add usage. This
implementation preserves full coverage of its selected live range; it cannot promise continuous operation within
30M CU. See [Alchemy's chain limits](https://www.alchemy.com/docs/chains/robinhood-chain/robinhood-chain-api-endpoints/eth-get-logs)
and [CU pricing](https://www.alchemy.com/docs/reference/compute-unit-costs).

## Install on the existing VPS

Run application commands as `ubuntu` in `/opt/meme-scanner`; preserve an existing venv.
Copy the repository files there, or clone the repository when the destination is empty.

```sh
cd /opt/meme-scanner
python3 -m venv .venv  # only if the existing venv is absent
.venv/bin/python -m pip install -r requirements.txt
cp -n config/.env.example config/.env
chmod 600 config/.env
# Edit config/.env to supply the private provider HTTP/WSS endpoints.
mkdir -p data logs
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/test_rpc.py
.venv/bin/python scripts/test_ws.py --seconds 60
.venv/bin/python scripts/init_db.py
.venv/bin/python scripts/inspect_recent_blocks.py --count 5
.venv/bin/python scripts/sync_stock_assets.py
.venv/bin/python -m app.main --max-batches 3
.venv/bin/python -m app.main --max-batches 3  # demonstrate restart/overlap
```

For interactive continuous monitoring, use `.venv/bin/python -m app.main`.
Configuration is read directly from `/opt/meme-scanner/config/.env`, without shell
evaluation. `SCANNER_ENV` can select another file for isolated tests. Relative data
paths resolve from the project root. The application rejects a wrong chain, unknown
factory, empty registry, and insufficient `.env` permissions on Linux.
No `.env`, database, logs, or virtualenv files are committed.

## Run as a service, only after the tests above pass

```sh
sudo install -o root -g root -m 644 deploy/meme-scanner.service /etc/systemd/system/meme-scanner.service
sudo systemd-analyze verify /etc/systemd/system/meme-scanner.service
sudo systemctl daemon-reload
sudo systemctl enable meme-scanner
sudo systemctl start meme-scanner
sudo systemctl status meme-scanner --no-pager
journalctl -u meme-scanner -f
```

Verify a service restart with `sudo systemctl restart meme-scanner`, then check the
journal and checkpoint. The service runs as ubuntu, limits memory to 400 MB, and
can write only `data/` and `logs/`. It needs outbound HTTPS; no inbound port or
firewall/SSH changes. Root ownership applies only to the installed unit.

## Data and recovery

`chain_state` preserves the original `scan_start_block` and `last_processed_block`.
An atomic, additive migration copies the old checkpoint into `historical_checkpoint`
once. `live_start_block` and `live_checkpoint` are independent. The `coverage` table
merges committed live/manual ranges; historical launch coverage does not imply historical
graduation coverage. Every block in each selected range is scanned for filtered logs.
`blocks` stores compact
headers/counts only for launch blocks and range boundaries, avoiding full header indexing.
`launches` stores normalized
events and optional metadata with `UNIQUE(tx_hash,log_index)` plus five query indexes.
`stock_assets` stores checksummed addresses, identity, source, verification and UTC time.
Stock Tokens include tokenized ETFs; this flag means a verified RHJ asset, not direct
ownership of shares. A ticker/name match never establishes identity.

```sh
sqlite3 /opt/meme-scanner/data/scanner.db \
  "SELECT block_number,token_symbol,quote_asset_symbol,is_stock_quote,tx_hash FROM launches ORDER BY block_number DESC LIMIT 20;"
sqlite3 /opt/meme-scanner/data/scanner.db \
  "SELECT key,value FROM chain_state; PRAGMA journal_mode; PRAGMA integrity_check;"
```

Only committed blocks advance the cursor; events and cursor share one transaction.
Restart resumes from the live checkpoint minus five overlap blocks. Confirmations default to three; this is
best-effort L2 reorg protection, not Ethereum settlement finality. Parent hashes and
the checkpoint are checked against RPC. A detected fork rolls back orphan headers
and launch/graduation events to a common ancestor within live coverage. A deeper fork stops with exit 2, without automatic
systemd restart: review the chain/provider, restore a consistent backup or choose a
new database after investigating. Do not edit only a checkpoint or erase gap evidence.

`SCANNER_MODE=live` is the default and is also set explicitly in systemd. It ignores
legacy `START_BLOCK`. `BACKFILL_ENABLED=false` is mandatory: no automatic historical
worker or hybrid mode is enabled. Manual backfill requires explicit inclusive bounds:

```sh
cd /opt/meme-scanner
sudo systemctl stop meme-scanner
.venv/bin/python -m app.backfill --from-block 58013387 --to-block 58013387
sudo systemctl start meme-scanner
.venv/bin/python scripts/show_coverage.py
.venv/bin/python scripts/rpc_usage_report.py --hours 24
.venv/bin/python scripts/rpc_usage_report.py --hours 72
```

The manual command uses the same writer lock and refuses to run alongside the service.
It records exact coverage and never moves either historical or live checkpoint. A
contradiction with stored history stops it; it cannot roll back a newer live range.
Choose small ranges so this exclusive writer does not cause a long live outage.
HTTP recovery is limited to `LIVE_RECOVERY_MAX_BLOCKS=10000` pending blocks and
ten-block provider queries. A larger outage stops for operator review without skipping.
It must be resolved explicitly; raising that bound consumes additional free quota.
The public RPC is never used to evade an exhausted private quota.

`rpc_usage` stores minute counters for up to 90 days, including HTTP envelopes,
individual batch methods, failures, retries, 429s, WebSocket notification bytes,
reconnects, new launch records, stock pairs, metadata calls, and recovery blocks.
Reports use no RPC calls and expose no endpoint URLs. They show actual elapsed time;
a requested 72-hour window is not a completed 72-hour observation until time has passed.
These are local counts, not account billing or CU estimates.

Malformed events hold the block cursor and retry, so they cannot silently disappear.
Broken/bytes32/reverting metadata produces null fields without losing the event.
Rate limits and outages during enrichment retry without committing missing metadata.
Persisted headers and enriched launches are reused on overlap; only canonical
checkpoint and range-end checks require fresh reads of already seen headers.
Metadata is read at the event block; an archive-capable provider may be needed for older
history. Registry sync replaces only a validated complete official snapshot and updates
existing launch classifications. A failed sync can reuse a verified snapshot for up to
48 hours; without one, collection waits and retries.

Stored headers are pruned outside the latest 10,000-block window by default; launches remain.
No transactions or raw blocks are retained. Log rotation is 10 MiB × six files total.
SQLite reuses freed pages; monitor disk usage as the launch history grows and back up
using SQLite's `.backup` command rather than copying a live database file alone.

## Checks and scope

`python -m unittest discover -s tests -v` exercises decoding, malformed metadata,
address-based identity, WAL, duplicate prevention, transaction rollback, restart
overlap, orphan cleanup, deep-fork stop, read-only RPC validation, response ID validation,
bounded jitter/backoff, shrinking log ranges, and disconnect recovery. Live deployment evidence and remaining limitations
are recorded in `docs/phase1-report.md`.

Phase 2 is not implemented. Phase 1.5 adds verified graduation identity and live-first
coverage, not FDV, liquidity, prices, volume, scoring, signals, or trading.
Deployment/backup evidence and rollback instructions: [Phase 1.5 report](docs/phase15-report.md).
