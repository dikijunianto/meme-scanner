# Phase 1.5 deployment report

Evidence snapshot: **2026-09-10 09:06:33 UTC** (2026-09-10 16:06 WIB).
Production: `/opt/meme-scanner`, Ubuntu, existing Python 3.12 venv and SQLite WAL.
Release commit message: `feat: add free-tier live-first mode`; the full pushed hash is
supplied in the delivery message.

## LIVE VERIFIED

| Check | Result |
|---|---|
| Baseline commit / branch | `635e96a0e7e3fbe6120bbc216a308a5df0007c9e` / `main`; clean, deployed files matched |
| Safe backup | SQLite backup API, `/opt/meme-scanner/data/phase15-backup/scanner-before.db`; integrity `ok` |
| Migration | Additive `coverage`, `rpc_usage`, `graduations` tables/indexes and dual cursor keys; run twice safely |
| Launch rows | **3,968 → 15,153**; all 3,968 original complete rows compare equal to backup |
| Stock-paired launches | **2,242 → 6,988** |
| Stock assets | **194 → 194**; all original verified identities remain |
| Historical checkpoint | **58,013,614 → 58,013,614**; original legacy key also unchanged |
| Historical launch coverage | 57,941,820–58,013,614 inclusive |
| Live start | **58,732,202**, chosen from head 58,732,207 minus five |
| Live checkpoint | **59,283,085** |
| RPC head / live lag | **59,283,141 / 56 blocks** at snapshot |
| Explicit unprocessed gap | **58,013,615–58,732,201**, 718,587 blocks |
| Production mode | `SCANNER_MODE=live` in protected config and systemd |
| Historical worker | Disabled; `BACKFILL_ENABLED=false`; no hybrid worker |
| Manual backfill | Block 58,013,387 replayed twice; one graduation; neither cursor moved |
| Writer isolation | Manual command refused while production held the writer lock |
| Graduation records | **160**, including one historical fixture replay and 159 live observations |
| WebSocket | Connected, factory + launch/graduation topic filters; **476** notifications observed in the most recent hour |
| Restart | Checkpoint 58,736,116 → 58,736,182 in 7.13 seconds; historical cursor/live start unchanged |
| Resource usage | RSS **61,431,808 bytes (58.6 MiB)**; systemd memory 58,437,632 bytes; 400 MB cap unchanged |
| DB size | **11,907,072 bytes (11.4 MiB)**; WAL 4,264,232 bytes at snapshot |
| SQLite | Integrity **ok**, WAL, zero duplicate launch/graduation groups, zero missing metadata |
| Service | **active/running**, automatic restart count **0**; manual restarts verified |
| Secrets / network | `.env` 600, ubuntu:ubuntu; UFW remains SSH-only; no added listeners |
| Scope | Read-only RPC allowlist; no wallet/signing/trading, paid service, or Phase 2 metrics |

`docs/phase15-evidence.json` contains the sanitized audit and restart evidence. Counts
are a timestamped snapshot, not a claim that the continuously running service has stopped.
Compact headers are deliberately pruned to the live retention window; launch records and
historical coverage are preserved, including the historical anchor.

## Local usage — LIVE VERIFIED

Telemetry began **2026-09-09T17:40:00.549109+00:00**. About 15.44 hours had elapsed
at the snapshot, and the following is a completed one-hour window. These are local scanner/manual-backfill observations, **not account
billing data**. Separate research and diagnostic probes are outside these counters.

| Metric | Count |
|---|---:|
| HTTP envelopes, including retries | 6,997 |
| `eth_getLogs` | 3,815 |
| `eth_getBlockByNumber` | 2,460 |
| `eth_call` | 1,367 |
| `eth_blockNumber` | 500 |
| `eth_chainId` / `eth_getCode` | 0 / 0 |
| HTTP 429 / provider rate-limit errors | **0 / 0** |
| Retries / failed envelopes | **0 / 0** |
| WebSocket notification bytes | 450,341 |
| Recorded WebSocket reconnects | 0 |
| New committed launches / stock pairs | 470 / 162 |
| Metadata calls scheduled | 1,352 |
| Recovery blocks, including overlap replay | 0 |

Method counts include individual batch members and therefore exceed envelope counts.
Metadata scheduling can precede dispatch/commit; in-flight work can make snapshots
differ between application and transport counters. WebSocket bytes exclude handshake,
framing, and ping traffic. Counters are minute buckets retained for 90 days. The
persistent reconnect marker was added during deployment; reconnect counts are windowed.
Initial telemetry includes the two bounded manual replays only when they overlap the requested window.

## Graduation and pool identity — LIVE VERIFIED / TEST VERIFIED

Factory `0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e` emits
`PoolGraduated(address,uint256,uint256,uint256)` with indexed token. Topic:
`0x0a44ef75df69c534f43cd6c1aa3ef8983065fe5fe79ef9e79f6494e6f258c259`.
`LaunchSwept` is an earlier stage and does not prove pool creation.

The stock-preferred example observed live is **GS / GLD**:

- Token: `0x4DC46Fd943b9A46306a9278c27F7047Dd676A47A`
- Quote: `0xC9a981FEE1F9DEc688bb123ccDeCc63D0deBFC4e` (GLD)
- Curve: `0x10ECCcCFd0EdF316981937ed61D0261483aE4f03`
- Launch: block **58,732,651**, transaction `0xb10c44b9f4e669229daa7d1e4d0fa4bdb9ed6cc5ab1e2941e85bfedd837b47ec`
- Graduation: block **58,732,690**, transaction `0x4f8f6cafc857018beb6c06e5d4c1ea8bb3436ab15047792b9f820ec3a55a8f49`
- PoolId: `0x48a77bcb3f66c6653fc32d9d6012dd29f9a4851862a210422d5700f65bf94df6`

The factory's event-block snapshot provides curve, quote, original deployer, fee, and
tick spacing. Numerically sorted currencies plus fee, spacing, and immutable hook form
the PoolKey. Its ABI-encoded keccak hash matches the singleton PoolManager's `Initialize`
event in the same transaction. This is a V4 PoolId, not a pool contract address.
Full official source links, exact searches, native-ETH example, manager/hook addresses,
and fixture provenance are in [graduation evidence](phase15-graduation.md).

## Tests — TEST VERIFIED

**28 tests pass on Windows and the production Ubuntu venv:** all 12 original Phase 1
tests plus 16 Phase 1.5 tests. Coverage includes transactional/idempotent migration,
actual SQLite backup restore, complete-range SQL rollback, dual cursor isolation,
near-head startup, bounded restart/reconnect, long-outage stop, explicit coverage gaps,
manual range replay/fork rejection, orphan cleanup/deep-reorg stop, duplicate prevention,
bounded header batches, transport telemetry/credential redaction, filtered WS reconnects
across restarts, and real ETH/GLD graduation fixtures.

Synthetic 429/disconnect/reorg tests are not claimed as production outages. Production
restarts and resumed subscriptions were observed; no spontaneous provider disconnect
was required to establish the automated reconnect proof.

## Files changed / created

- Runtime: `app/config.py`, `app/database.py`, `app/block_watcher.py`, `app/main.py`,
  `app/rpc.py`, `app/heads.py`, `app/pons.py`; new `app/backfill.py`, `app/telemetry.py`.
- Operations: `config/.env.example`, `deploy/meme-scanner.service`,
  new `scripts/show_coverage.py`, `scripts/rpc_usage_report.py`.
- Proof/docs: `tests/test_live.py`, two `tests/fixtures/graduations/*.json`, `README.md`,
  `docs/phase15-graduation.md`, `docs/phase15-report.md`, `docs/phase15-evidence.json`.

No dependency was added. Secrets, databases, logs, backups, and research scratch files
remain untracked. Historical launch inspection retains its existing Phase 1 API;
production explicitly selects the independent live cursor.

## Backup and rollback

The pre-migration directory also contains `code-before.tar.gz`, `config-before.env`
(600), `service-before.unit`, and `audit-before.json`. Do not publish this directory.
Migration failures roll back DDL/state in one transaction; this was fault-injected in
tests. A SQLite backup was also restored over a migrated test database and verified.

For an operator-requested deployment rollback: stop the service, make another SQLite
backup of the **current** database so post-migration observations remain recoverable,
restore the archived Phase 1 code/unit, and use the SQLite backup API to restore
`scanner-before.db` into a **new** database filename. Point `DATABASE_PATH` at that
restored file using the backed-up protected config; preserve both databases and keep
permissions 600. Reload systemd. Leave the old service stopped pending review: Phase 1
resumes expensive historical catch-up. Never replace an active WAL database with a
plain file copy, run mixed-version writers, or silently discard newer observations.
This production downgrade procedure was not executed; production remains on Phase 1.5.

## Limits and next decision

**NOT YET MEASURED:** true 24/72-hour usage, monthly quota endurance, and long-term signal
utility. No CU/account-billing estimate is claimed. Filtered WebSocket notifications
wake the scanner; bounded HTTP ranges remain the completeness/recovery path. This
still consumes free quota and may exhaust 30M monthly CU. No paid upgrade or automatic
public-provider escape is enabled; exhausted retry budgets stop for operator review.

**INFERRED:** batching missing headers reduces HTTP overhead during launch bursts.
Observed lag settled to 60 blocks at the snapshot; no fixed latency guarantee is made.
The gap is intentional and never represented as processed. A live graduation can refer
to a launch inside that gap, so its launch transaction may be unavailable. Outages
requiring more than 10,000 recovery blocks stop without skipping; any larger recovery
budget requires an explicit operator decision. Manual backfill shares an exclusive
writer lock, so keep manual runs short.

Run after the respective elapsed production time:

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/rpc_usage_report.py --hours 24
.venv/bin/python scripts/rpc_usage_report.py --hours 72
.venv/bin/python scripts/show_coverage.py
```

Recommendation: the Phase 1.5 collection and verified pool identity foundation is
ready. Review 24/72-hour usage, lag, and data usefulness before authorizing Phase 2.
Phase 2 was not started. The service remains running within the existing free plan,
as authorized, until the provider rejects requests or another stop condition occurs.
