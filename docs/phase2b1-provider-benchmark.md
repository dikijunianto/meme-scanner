# Phase 2B.1 — free RPC/WSS benchmark

Production routing remained unchanged. Benchmark process read the existing flow DB
through SQLite `mode=ro`, used public endpoints, and wrote only to `/tmp`. No
Alchemy benchmark calls, paid account, production subscription or systemd changes.

## Baseline, 2026-09-23 16:00 UTC

Main active, PID 64326, zero unexpected restarts. Flow active, PID 66027, zero
unexpected restarts, but `service_status=paused_ws_budget`; its UTC-day WebSocket
counter was 8,000,262 of 8,000,000 bytes. Last hour flow produced no new targets or
events. Main received 680 launches and eight graduations; Phase 2A completed 50
market targets. Both databases passed integrity checks; available disk space was
32,727,515,136 bytes. Main and flow config fingerprint remained `5ce290407fb3`.
Observed main/flow RSS during the bounded benchmark: 56,504 / 68,268 KiB;
the standalone harness used about 52,172 KiB, below the 150 MB target.
The last-hour journal security scan found zero credential URL or authentication
matches. The counter is local payload telemetry, not provider billing.

## Current provider terms, official sources checked 2026-09-23

| Provider | Robinhood mainnet | Free terms | HTTPS/WSS | Relevant limitation |
|---|---|---|---|---|
| [PublicNode](https://robinhood.publicnode.com/) | Listed | Public endpoint without account; numeric fair-use, WSS byte and subscription caps not published there | Both advertised; WSS chain ID independently verified | Archive access advertised separately; no free archive guarantee |
| [Chainstack](https://chainstack.com/pricing/) | [Listed](https://chainstack.com/build-better-with-robinhood-chain/) | Permanent Developer plan, 3M request units/month, 25 RPS, one node, no credit card advertised | Both, private endpoint required | Pricing page also lists $20/1M extra Developer units; free hard cap/overage control must be confirmed in account |
| [Validation Cloud](https://www.validationcloud.io/robinhood) | Listed | 50M CU/month free; [billing docs](https://docs.validationcloud.io/v1/about/billing) say no card for Free | HTTP advertised; Robinhood WSS for an actual free account unverified | Scale plan can automatically bill; stay on Free |
| [Dwellir](https://www.dwellir.com/) | [Listed](https://www.dwellir.com/networks/robinhood) | Free 100K responses/day, 20 RPS; current pricing page excludes `eth_getLogs` | Both advertised | Cannot handle free recovery HTTP unless another provider supplies it; free WSS account untested |
| [Robinhood official](https://docs.robinhood.com/chain/connecting/) | Native | Public HTTP | HTTP only for JSON-RPC in documented public endpoints | [Rate limited; not for production throughput](https://docs.robinhood.com/chain/terms-of-service/). Sequencer feed WSS is a different protocol |

No Chainstack, Validation Cloud or Dwellir account endpoint was available for
this run. Their actual WSS event delivery, limits and card/overage controls were
not inferred from generic marketing. Ethereum subscription semantics allow
`removed: true` on reorganizations per [go-ethereum documentation](https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub); a live removed log was not forced.

## Live measurements

**Final recommendation: MORE_BENCHMARK_REQUIRED.** PublicNode is the best
credentialless WSS-only candidate, but curve traffic was absent during the
window and its public HTTP rejects `eth_getLogs`. Independent HTTP verification
covered the eight blocks containing received V4/hook events, not every block of
the 20-minute window. No secondary provider was routed into production.
Git baseline: clean `main` at `f4df3e2776f28f97d640844381632f16e16a8481`.
The 103 existing VPS tests passed before this work; 111 passed afterward.
Windows also ran 111 tests, skipping three POSIX-specific checks.

The isolated PublicNode WSS test ran **1,200 seconds** (20 minutes) over blocks
**70,644,748–70,656,771**. It used the actual Phase 2B filter shapes:

- CurveBuy OR CurveSell topic0, five known curves:
  `0x3744047ec3F8dF5a9147fe5AbBEa8a0c0a770d99`,
  `0x35fb218e4559136a7b49F556c6e3B4ba8e83aD84`,
  `0xC9a626132FAAFE12C7cB99eDA7C4EBc071226c11`,
  `0x92675DBeBCf2DA6f3e29e5f1A5b82c5b7D4b40bA`,
  `0x0371cBe6125d97e989fb6aF7a010423755BE0A9E`.
- V4 Swap topic0 plus indexed PoolId
  `0x62d4a179de652f1e1f35d372fb4c5a97cb692cd111b29b269e5be84361727cb7`
  at PoolManager `0x8366a39CC670B4001A1121B8F6A443A643e40951`.
- HookFeeCollected topic0 plus the same PoolId at
  `0xE5e702641Ea86F4ae6cC3cDaeD2B886f976Be044`.

All three `eth_subscribe logs` filters were accepted; `eth_chainId=4663`,
`eth_unsubscribe` succeeded for each. A separate one-address curve
subscribe/unsubscribe also succeeded; the five-address curve filter was accepted.
Twelve simultaneous subscriptions and a ten-address array were not probed,
because the public provider does not document a safe free limit and the three
real filter shapes were the relevant bounded load.

The stream delivered **16 notifications**: eight V4 swaps and eight hook
events, in eight distinct blocks. No curve event occurred. Duplicate count 0,
wrong-address/topic/PoolId count 0, unexpected disconnects 0, malformed
messages 0. One controlled client reconnect succeeded in **1.03 seconds**
and all filters resubscribed. No `removed: true` notification occurred:
**NOT_LIVE_OBSERVED**. Inbound payloads including subscription/control messages
were **15,806 bytes** (987.88 bytes per observed event including that overhead).
This is not comparable to Alchemy's billing model without provider accounting.

PublicNode HTTP returned **403 / JSON-RPC -32602** for all six targeted
`eth_getLogs` queries, covering 10- and 100-block windows. Historical
`eth_call` at the stored flow block also returned 403. A current `latest` and
one near-head pinned `eth_call` succeeded, but five later near-head pinned
`eth_call` probes returned 403, so current-state access is inconsistent.
`eth_chainId`, `eth_blockNumber`, a pinned header and a two-member JSON-RPC
batch succeeded. PublicNode cannot be the only Phase 2B HTTP recovery source.

The Robinhood public HTTP control succeeded for chain ID, head, pinned header
and batch. It returned two HTTP 429s in six historical `eth_getLogs` samples,
and a historical `eth_call` failed; recent pinned state calls and later
single-block filtered log checks succeeded at conservative cadence. This
supports its role as a bounded control, consistent with Robinhood's published
rate-limit warning, not a production primary.

| Provider | Free / credentials | HTTP `getLogs` | Filtered WSS / PoolId | Live completeness | Disconnect / reconnect | Relative event latency | Inbound bytes/event | Rate behavior | Classification / role |
|---|---|---|---|---|---|---|---|---|---|
| PublicNode | Free public / none | 0/6; HTTP 403 | Three filters accepted; V4 and hook notifications correct; curve filter accepted without events | 16/16 against independent HTTP in eight event-bearing blocks; full 20-minute window **unverified** | 0 / 1.03s success | Not measurable with one live WSS provider | 987.88 including controls | No 429 observed; HTTP method restriction | **INCONCLUSIVE**; provisional WSS-only candidate |
| Chainstack | Permanent free Developer / private endpoint required | Not tested | Robinhood WSS advertised; account endpoint not tested | Not tested | Not tested | Not tested | Not tested | 3M RU/month, 25 RPS; hard overage control unverified | **NOT_TESTED_CREDENTIAL_REQUIRED** |
| Validation Cloud | Free / private endpoint required | Not tested | Robinhood WSS availability for Free account unverified | Not tested | Not tested | Not tested | Not tested | 50M CU/month; Scale auto billing must be avoided | **NOT_TESTED_CREDENTIAL_REQUIRED** |
| Dwellir | Free account/key required | Excluded by current free pricing | WSS advertised; free account not tested | Not tested | Not tested | Not tested | Not tested | 100K responses/day, 20 RPS; byte/subscription cap unknown | **NOT_TESTED_CREDENTIAL_REQUIRED**; possible WSS-only candidate |
| Robinhood official | Public / none | Mixed: 4/6 historical samples passed; 2 HTTP 429 | No documented JSON-RPC WSS control | Not applicable | Not applicable | Not applicable | Not applicable | Rate limited | **INCONCLUSIVE** as general HTTP fallback; bounded control only |

The independent Robinhood read used one block per observed V4/hook event and
the same PoolId topics. It returned **16 expected identities across eight
blocks; PublicNode WSS delivered all 16**, with zero extra identities in those
blocks. Five other 10-block curve spot checks across the window returned zero
logs. This is scoped evidence: unsampled blocks could still contain missed
events. PublicNode's own post-window recovery failed at the first block for
each filter because `eth_getLogs` returned 403; shrinking the range did not
fix the method restriction.

Thirty conservative paired-head samples yielded 29 valid pairs. At their
common heights, block hashes matched **29/29**; no canonical conflict was
seen. PublicNode had zero observed head lag; the public Robinhood endpoint
trailed by at most **14 fast blocks**. A recent pinned `decimals()` call returned
the same raw value (`18`) on both providers. The older pinned call failed on
both, so archive availability was not established.

Additional one-request-per-second probes: PublicNode `eth_blockNumber` 5/5
(median 153.30 ms, p95 186.76 ms), pinned `eth_call` 0/5 (403), `eth_getLogs`
0/3 (403). Robinhood `eth_blockNumber` 5/5 (median 251.93 ms, p95 252.24 ms),
pinned `eth_call` 5/5 (median 253.38 ms, p95 253.69 ms), `eth_getLogs` 3/3
(median 255.11 ms, p95 255.11 ms). These are tiny samples; failed-call
latencies measure rejection speed, not useful service latency. No 3–5 RPS
probe was needed. Cross-provider *relative event arrival latency* is
unavailable because only one WSS provider could be used without credentials.

## Safety and next configuration design

The harness did not import or overwrite the production Config, used the flow
SQLite file with `mode=ro`, used only public endpoints, and wrote machine
results under `/tmp`. It caused **zero Alchemy requests**. Production
continued on fingerprint `5ce290407fb3`; at the final check main PID 64326
and flow PID 66027 were active, both with NRestarts 0. Phase 2A completed
44 targets in the final hour. Both DB integrity checks were `ok`. The
standalone process stayed near 53 MB RSS; no production degradation was
observed. During the benchmark window, credential URL and auth-error journal
matches remained zero. The result, source, docs and test output were screened without
printing credentials: zero exact new-key matches and zero credential URL
patterns. The benchmark added no paid account or provider overage setting.

**If a later benchmark proves curve completeness and free sustainability**, the
smallest provider split is: keep `ROBINHOOD_RPC_HTTP` and `ROBINHOOD_RPC_WS`
for main/Phase 2A; add protected `FLOW_RPC_WS` for only the flow worker;
leave flow recovery HTTP on its existing bounded Alchemy configuration. A
flow-only config load would replace only that worker's WebSocket URL, preserving
main process, shared DB permissions, recovery limits and idempotency. No such
config or routing code was deployed here. PublicNode's blocked `eth_getLogs`
rules out using it for Phase 2B recovery HTTP on this tested endpoint.

For a credentialed free-plan comparison, create a private Robinhood endpoint
through the provider account, verify no paid overage can trigger, then place
only the endpoint URL(s) in `config/provider-benchmark.env` on the VPS with
file mode 0600 and parent directory 0700. The tracked
`config/provider-benchmark.env.example` has field names only. Run the bounded
`scripts/provider_benchmark.py --env config/provider-benchmark.env --minutes 20`.
No credential is needed in chat, Git, arguments or output.

Machine-readable credential-free results remain under `/tmp` on the VPS:
`provider-benchmark-results.json`, `provider-benchmark-independent-check.json`
and `provider-benchmark-latency.json`. This report is the reviewed summary;
no secret-bearing benchmark artifact is committed. Production routing remains
unchanged. No Phase 2C work was started.

## Round 3 concurrent completeness check

`scripts/provider_benchmark_round3.py` runs PublicNode and Validation Cloud
WebSocket subscriptions at the same time. It freezes up to five currently
active curve addresses and one real graduated V4/hook pool from the read-only
flow database before either subscription starts. Both providers receive the
same address/topic filters. The comparison uses the shared interior block
range after both subscriptions are ready and before either is stopped, so
boundary notifications do not create false misses. Validation Cloud HTTP
queries those exact filters over every block in that range after the live
window. Each `eth_getLogs` request covers at most 100 blocks, shrinks on
provider range rejection, and has bounded retries for transient failures.

Run from `/opt/meme-scanner` on the VPS with the private
`config/provider-benchmark.env` already installed:

```sh
.venv/bin/python scripts/provider_benchmark_round3.py --min-minutes 20 --max-minutes 60
```

The only network targets are PublicNode WSS and Validation Cloud WSS/HTTP.
The script does not load production RPC credentials or change systemd,
production configuration, or either database. It writes a mode-0600 result
to `/tmp/provider-benchmark-round3.json` with endpoint fingerprints, never
credential URLs. A curve pass requires at least one canonical curve event in
the complete Validation HTTP block range, 100% live match, no extra logs,
and no invalidating subscription evidence. If the range has no curve event,
the result is `NO_CURVE_EVENT_OBSERVED` and the gate remains
`MORE_BENCHMARK_REQUIRED` even when all zero-event classes match.

### Round 3 result, 2026-09-24 UTC

The shared live window ran 03:18:32.935–04:18:32.938 UTC (3600.003 seconds)
over interior blocks 71043527–71079042. It froze four active curve addresses
and one graduated V4/hook pool into three filters. Validation HTTP completed
all 1,068 bounded `eth_getLogs` requests without error or range reduction.
Its expected counts were zero CurveBuy, zero CurveSell, eight V4 swaps, and
seven hook events. Each WSS provider delivered the same 15 canonical events:
8/8 V4 and 7/7 hook, zero missing, zero extra, zero duplicates, zero malformed
or wrong-filter notifications, and zero disconnects/reconnects. Validation WSS
received 14,483 bytes; PublicNode WSS received 14,502 bytes.

No curve trade occurred in the entire verified block interval, so both
providers are classified `*_WSS_PASS_CURVE_UNPROVEN`; the overall gate is
`MORE_BENCHMARK_REQUIRED`. This demonstrates observed V4/hook completeness
for this frozen cohort, not curve delivery. No production routing changed,
no service restarted, both DB integrity checks passed, and the benchmark
made zero Alchemy requests. The private result file on the VPS is
`/tmp/provider-benchmark-round3.json` (0600); it contains no endpoint URL or
credential. Keep production routing unchanged until a later run captures and
verifies a live curve trade.
