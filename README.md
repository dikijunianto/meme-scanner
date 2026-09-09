# meme-scanner — Phase 1

Read-only Pons V2 launch collection on Robinhood Chain mainnet (4663), Python 3.12,
SQLite WAL, private provider HTTP and WebSocket heads. No wallet, private keys,
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
pool exists yet at launch. Graduation/pool IDs belong to a later phase.
The minimal ABI and source links are in `app/pons.py`. Older V2 and V1 deployments
are excluded deliberately: their addresses/versions must be independently verified
before extending coverage. Historical fixtures are actual RPC logs, not synthetic examples.

Production requires `ROBINHOOD_RPC_HTTP` from a provider. Configure its private WSS
endpoint as `ROBINHOOD_RPC_WS`; credentials live only in the protected `.env`.
Factory/topic-filtered `logs` subscriptions are the default, avoiding the bandwidth
cost of every chain header. Optional `ROBINHOOD_WS_SUBSCRIPTION=newHeads` was also
tested. Notifications coalesce into one wakeup. All launches
are recovered with filtered, bounded HTTP log ranges, including after disconnects.
Subscriptions start only near the head; backfill does not pay for unused live notifications.
After bounded WS reconnect attempts, the same private HTTP endpoint supplies heads.
The official public RPC is optional basic connectivity only (`test_rpc.py --fallback`),
never an automatic production failover. Its sequencer feed is not `eth_subscribe`.

Requests are paced at five HTTP requests/second, with at most four in flight.
Metadata uses two-call batches, so the maximum logical-call rate remains below the
user's stated 25 requests/second plan limit. Batching does not reduce billed CUs.
HTTP 429/5xx and transport failures retry up to five total attempts, with exponential
backoff, jitter, Retry-After support, and a 60-second maximum. Exhaustion stops with
exit 3; systemd does not silently reset the retry budget. Review the provider/quota
and explicitly restart after resolving the cause. Logs omit URLs and provider bodies.

Alchemy Free supports only ten blocks per `eth_getLogs` query. `LOG_BLOCK_RANGE=10`
applies to historical and live scans; rejected ranges shrink, without skipping blocks.
The 30M monthly CU allowance is separate from request throughput. At approximately
10 blocks/second and 60 CU/query, complete ten-block HTTP log coverage alone projects
to 155.5M CU per 30 days. WebSocket notifications and enrichment add usage. This
implementation preserves full coverage; it cannot promise continuous operation within
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
.venv/bin/python scripts/inspect_launches.py --max-blocks 10000 --find-stock --output data/historical-evidence.json
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

`chain_state` stores chain identity, scan start, last committed block and registry
sync time. Every block in each range is scanned for logs. `blocks` stores compact
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
Restart replays five blocks by default. Confirmations default to three; this is
best-effort L2 reorg protection, not Ethereum settlement finality. Parent hashes and
the checkpoint are checked against RPC. A detected fork rolls back orphan headers
and launches to a common ancestor. A deeper fork stops with exit 2, without automatic
systemd restart: review the chain/provider, restore a consistent backup or choose a
new database with an explicit bounded START_BLOCK. Do not edit only the checkpoint.

First start begins near the head unless START_BLOCK is set. START_BLOCK applies only
to a fresh database; changing it does not rewind an existing checkpoint. For a separate
backfill, stop the service and use a separate `.env`/database, or raise OVERLAP_BLOCKS
within the retained history. Never run two watchers against the same database.
The historical inspection command prints evidence without moving the watcher cursor;
expand `--max-blocks` responsibly (maximum 1,000,000 per invocation).

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

Phase 2 is not implemented. Its next step is to verify graduation events and the
curve-to-V4 pool mapping before collecting liquidity/FDV/volume measurements.
