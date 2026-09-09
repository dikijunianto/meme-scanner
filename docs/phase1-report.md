# Phase 1 deployment report — 2026-09-09

Repository: https://github.com/dikijunianto/meme-scanner

Implemented on the existing Ubuntu VPS at `/opt/meme-scanner`, using its existing
Python 3.12 venv. This report distinguishes live evidence from local fault simulations.

## Deliverables

1. **Files:** `app/` contains configuration, RPC transport, WebSocket subscriptions,
   block/log watching, Pons decoding, metadata enrichment, stock registry, models,
   database and entry point. `scripts/` provides connectivity, WebSocket, database,
   block inspection, bounded historical inspection and stock sync commands.
   `deploy/meme-scanner.service`, `config/.env.example`, `requirements.txt`, `README.md`,
   twelve automated checks, two real RPC fixtures and `docs/chain-evidence.json` complete delivery.
2. **Direct packages:** httpx 0.28.1, python-dotenv 1.2.3, eth-utils 6.0.0,
   eth-abi 6.0.0, eth-hash[pycryptodome] 0.7.1, websockets 17.1. No Docker,
   PostgreSQL, web framework, wallet or trading library was installed.
3. **Chain ID:** `4663`; verified against official documentation and both public
   and private live `eth_chainId` responses.
4. **Production RPC:** private Alchemy Robinhood mainnet HTTPS/WSS endpoints,
   loaded only from the VPS `.env` (600, ubuntu:ubuntu). Credentials are deliberately
   absent from this report and repository. The public endpoint is basic connectivity
   only; production never silently falls back to it.
5. **Current Pons V2 factory:** `0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e`.
   The current official docs and source agree. Its 24,177-byte deployed runtime was
   read on-chain and its hash is pinned. Older V1/V2 factories are excluded.
6. **Launch event:** `TokenLaunched(address,address,address,address,uint256,uint256)`.
   Topic0: `0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607`.
   The minimal ABI was checked against official docs/source and actual emitted logs.
7. **Database:** `chain_state`, `blocks`, `launches`, `stock_assets`; SQLite WAL,
   UTC timestamps, five launch query indexes, unique transaction/log identity,
   atomic event/checkpoint commits, orphan rollback and bounded header retention.
8. **Historical launch example:** CATWIFI at block `57940522`, log index `59`,
   transaction `0xbc810f79d5a7a105f8c4b9ccd012a174210dd3dafa7064d227263f037cee6e9b`.
   Token: `0xd9435e01354Ffb1012e73c94145aB786C1eF9beA`.
   Curve: `0xA832f350E17B62041E4752f7a47Cfe53EFEF1f0E`.
   A curve is stored separately; no Uniswap pool address is fabricated at launch.
9. **Stock-paired example:** that CATWIFI launch quotes NVDA at
   `0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC`, verified by the official asset
   registry. An initial 1,000-block range (`57940521..57941520`) yielded 54 launches,
   25 stock-paired. The registry contained 194 mainnet assets. A private-Alchemy
   replay of `57940513..57940532` decoded three launches, including the same NVDA pair.
10. **Service:** see the final service snapshot below. It runs as ubuntu with no
    inbound port, a 400 MB memory ceiling, rotating logs, and the existing firewall
    and SSH settings preserved. Retry exhaustion exits 3 and requires an explicit
    restart, so systemd cannot circumvent the retry limit.
11. **RAM/CPU:** the private-endpoint interactive test peaked at 54,680 KiB RSS
    (53.4 MiB), with 5% CPU over a 15.21-second bounded catch-up test. These are
    measured short-test values, not a long-term resource guarantee.
12. **Disk:** the initial deployment used approximately 68 MB including its venv;
    see the final snapshot for current data/log sizes. Headers are retained only
    for launches and scan boundaries within a 10,000-block window. Launch history
    is retained; log rotation caps scanner logs to about 60 MiB.
13. **Limitations:** Blockscout returned Cloudflare HTTP 403, so no independent
    explorer/compiler bytecode match is claimed. Official ABI/source and real
    chain events are verified. WebSocket stability was tested over bounded windows,
    not weeks. The free monthly quota cannot sustain the observed full workload;
    the user explicitly chose operation until free-quota exhaustion, without upgrade
    or reducing launch coverage. The persisted checkpoint is retained across the
    testing pause, so the service must catch up rather than skip the gap.
14. **Phase 2 next step:** verify graduation events and curve-to-V4 pool mapping,
    then design read-only liquidity and FDV collection. No Phase 2 work is included.

## Verification

- Twelve automated checks pass: real-fixture decoding; malformed/bytes32/reverting
  metadata; 429 retry limits and jitter; dynamically shrinking log ranges without
  gaps; RPC response validation/read-only restriction; address-based stock identity;
  WAL, duplicate prevention and transaction rollback; restart reuse; orphan removal;
  deep-reorg stop; complete log coverage with sparse headers.
- Live subscriptions are deferred during backfill and enabled near the head, avoiding
  bandwidth charges for notifications the scanner cannot yet use.
- Private HTTP chain ID and latest block passed. Block `57953545` was read during
  connectivity verification, with eight transactions and a valid block hash.
- Private `newHeads`: 592 notifications over 60 seconds on one connection. A
  separate 10-second sample received 98 notifications / 163,633 notification bytes.
- Production filtered `logs`: 16 relevant notifications / 15,167 notification bytes
  over 60 seconds, on one connection. Both subscription types passed live tests.
- Three historical launches decoded through private Alchemy, using ten-block
  queries. ERC-20 metadata and canonical stock identity were resolved successfully.
- Public endpoint failures were observed live during the initial tests. Private
  retry exhaustion, backoff jitter and range rejection are tested with injected
  failures; no provider outage was fabricated as a successful live fault test.
- Six metadata rows left incomplete by the initial public-RPC tests were repaired
  through the private endpoint; the repair finished with zero missing metadata rows.
- No wallet/private-key handling, signing, approval, swap or broadcast operation
  exists. A prohibited-method string appears only in the rejection test.

## Free-plan operating limits

The configuration uses at most five HTTP requests/second and four concurrent
requests, with two-element metadata batches (at most ten logical calls/second).
This remains below the user's 25-request/second allowance. No concurrency increase
or aggressive retry loop is used to bypass throttling.

The plan's ten-block `eth_getLogs` limit is independent of request throughput.
At the measured approximately ten blocks/second, complete log coverage requires
about one such query/second: roughly **155.5M CU per 30 days** at the documented
60 CU/query, before metadata, headers or subscriptions. This is an extrapolation
from observed traffic and published method prices, not an account billing reading.

The all-chain head sample alone projects to roughly 1.7B CU/month at the documented
0.04 CU/byte. Therefore production defaults to factory/topic-filtered **log**
subscriptions, not the all-chain head stream. Full historical and reconnect
coverage remains intact through HTTP. This reduces unnecessary subscription
bandwidth but does not make a 30M monthly allowance sufficient for the full workload.

No paid plan was enabled. On quota/rate failures, bounded retries retain the
checkpoint and ultimately stop the service for review.

## Final service snapshot

At 2026-09-09 16:13:39 UTC, the private-Alchemy service was active/running with
filtered log subscriptions, 51.8 MiB RSS (38.1 MiB systemd memory accounting),
518 stored launches including 332 stock-paired launches, WAL mode, SQLite integrity
`ok`, zero duplicate groups and zero missing-metadata rows. The venv used 57.4 MiB,
data 4.2 MiB and logs 0.14 MiB. `.env` permissions remained 600.

The checkpoint was `57949224` while the RPC head was `58680664`: approximately
731,440 blocks remained from the testing pause. Catch-up preserves that gap rather
than silently resetting to the head. Service restart verification followed this
snapshot; the PID changed and checkpoint processing continued.

## Sources

- [Robinhood network parameters](https://docs.robinhood.com/chain/add-network-to-wallet/)
- [Robinhood provider endpoints](https://docs.robinhood.com/chain/connecting/)
- [Pons V2 official docs](https://docs.ponsfamily.com/v2)
- [Pons source at checked commit](https://github.com/ponsdotdev/ponsfamily/blob/33c2281bfcf91f18ddc3e8497894ae764118ce37/contractsV2/src/v2/PonsV2LaunchFactory.sol)
- [Official stock asset schema](https://docs.robinhood.com/chain/stock-token-apis/)
- [Alchemy Robinhood log-range limits](https://www.alchemy.com/docs/chains/robinhood-chain/robinhood-chain-api-endpoints/eth-get-logs)
- [Alchemy compute-unit pricing](https://www.alchemy.com/docs/reference/compute-unit-costs)
