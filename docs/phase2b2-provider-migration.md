# Phase 2B.2 provider split

## Status and evidence

Original implementation commit: `6fa1d28`. Production cutover is **pending**.
The 2026-09-24 budget block reset, but the next preflight found the original
100-block startup replay could not cover the actual gap. A per-target recovery
patch is staged for a fresh preflight; neither production service or route has
been changed by that patch.

Round 3 compared Validation Cloud and PublicNode WSS concurrently against
Validation HTTP: V4 8/8 and hook 7/7 on each WSS, with no missing, extra,
duplicate, malformed, wrong-filter, or disconnected events. Round 4 ran
2026-09-24 15:32:47.581–15:53:27.190 UTC (1,239.609 seconds), found 243
curves, synchronized 154, and tracked at most 32. Each WSS returned all
770 CurveBuy and 933 CurveSell events reported by Validation HTTP; neither
had missing or extra events. Validation HTTP used 158 bounded `eth_getLogs`
queries with zero errors/retries/range reductions. Benchmark Alchemy calls
were zero. The Round 4 gate was `PROVIDER_SPLIT_READY`.

On the Ubuntu host, the implementation passed all 140 tests in an isolated
checkout. The separate endpoint file was installed with `config/` mode 0700
and `config/flow-rpc.env` mode 0600. The benchmark file and main `.env` remain
0600 and unchanged. All three endpoints returned chain ID 4663:

| Role | Provider | Safe fingerprint |
| --- | --- | --- |
| Main HTTP/WSS and Phase 2A | Alchemy | `5ce290407fb3` |
| Flow HTTP recovery | Validation Cloud | `2335586b89b7` |
| Flow primary WSS | PublicNode | `ec03b46401e3` |
| Flow fallback WSS | Validation Cloud | `2b83e9b3fbb4` |

The installed private endpoints came only from
`/opt/meme-scanner/config/provider-benchmark.env`. The installer creates
`/opt/meme-scanner/config/flow-rpc.env` atomically, with fsync and mode 0600.
The main service never reads it. The systemd flow unit contains only its path.

## Routing and limits

The target routing is main and Phase 2A on Alchemy; Phase 2B HTTP on
Validation; Phase 2B live WSS on PublicNode. After two unsuccessful primary
connection attempts, flow switches to Validation WSS and stays there until
a controlled restart. The same active filters are resubscribed, then bounded
Validation HTTP recovery uses the last persisted block anchor. Canonical
event identity deduplicates replay. There is no automatic Alchemy fallback.

HTTP limits remain 1,000 members/day, 12/minute, 400 `eth_getLogs`/day,
and 0.5 envelopes/second. The pending recovery patch uses per-target/filter
committed cursors, 2,000-block filtered queries, and a 100,000-block hard
limit per filter. It preflights the aggregate plan with all three allowed
attempts reserved for each query; provider range rejection causes a fresh
budget check before any smaller query. A failed or incomplete replay keeps
its last fully committed cursor and leaves the gap unresolved. The old 8 MB Alchemy WSS
pause is replaced by `FLOW_SECONDARY_WS_BYTES_PER_DAY=64000000`. This is an
**internal emergency circuit breaker**, not a PublicNode or Validation quota.
The historical aggregate byte counter remains; new counters identify the
provider actually used by each HTTP send and WSS connection/frame. Transaction
and receipt methods remain outside the read-only RPC allowlist.

## Controlled cutover after HTTP budget reset

At the cutover, first re-run the full suite in staging, the endpoint chain
preflight, both DB integrity checks, and service/row/usage snapshots. Require
remaining Phase 2B `eth_getLogs` budget for every active target. Record the
main and flow PIDs/restart counts, current flow `last_connected_block`,
maximum gap ID, raw-event/feature counts, Phase 2A snapshots, and endpoint
fingerprints. Protected pre-cutover copies are already stored in
`/opt/meme-scanner/rollback/phase2b2` (directory 0700, files 0600). Stop
**only** `meme-scanner-flow.service`,
replace the flow-only files and `config/flow.env` setting, install/reload the
flow unit, then start **only** that service. Do not touch `meme-scanner.service`
or `config/.env`.

The staged source is `/tmp/meme-scanner-phase2b2-test`. Once the preflight
budget gate passes, use this cutover sequence from `/opt/meme-scanner`:

```sh
sudo systemctl stop meme-scanner-flow.service
install -m 644 /tmp/meme-scanner-phase2b2-test/app/flow_worker.py app/flow_worker.py
install -m 644 /tmp/meme-scanner-phase2b2-test/app/flow_reports.py app/flow_reports.py
install -m 644 /tmp/meme-scanner-phase2b2-test/scripts/flow_usage_report.py scripts/flow_usage_report.py
install -m 644 /tmp/meme-scanner-phase2b2-test/scripts/flow_security_status.py scripts/flow_security_status.py
sed -i 's/^FLOW_MAX_WS_BYTES_PER_DAY=8000000$/FLOW_SECONDARY_WS_BYTES_PER_DAY=64000000/' config/flow.env
sed -i '/^FLOW_RECOVERY_MAX_BLOCKS=/d' config/flow.env
chmod 600 config/flow.env
install -m 644 /tmp/meme-scanner-phase2b2-test/deploy/meme-scanner-flow.service deploy/meme-scanner-flow.service
sudo install -m 644 deploy/meme-scanner-flow.service /etc/systemd/system/meme-scanner-flow.service
sudo systemctl daemon-reload
sudo systemctl start meme-scanner-flow.service
```

Immediately confirm the main PID and NRestarts are unchanged. Require flow
WSS provider `publicnode`, HTTP `validation`, new subscriptions, Validation
HTTP recovery calls, and all newly created cutover `ws_gap` rows resolved.
If there were no new blocks in the stop/start interval, record the head proof
instead. Any unresolved cutover gap or exhausted recovery budget fails the
cutover. Old unresolved historical gaps are reported separately, never erased.

Run a full 30-minute live observation. Record main launch and Phase 2A snapshot
progress; flow raw rows, feature-window progress, provider calls/bytes,
budget pauses, resource use, both DB integrity checks, journal scan counts,
and client-boundary Alchemy flow counters. Verify zero flow Alchemy HTTP
requests, WSS connections/bytes, and transaction/receipt lookups. Fallback
is test verified; do not disrupt production endpoints to force it.

## Emergency rollback

If the cutover fails, stop only `meme-scanner-flow.service`. Restore the
protected pre-cutover copies of `app/flow_worker.py`, `app/flow_reports.py`,
`scripts/flow_usage_report.py`, `config/flow.env`, and the old flow unit;
reload systemd and start only the flow service. This explicitly restores the
previous Alchemy flow route as an emergency action. Confirm main PID and
NRestarts are unchanged, record the reason and rollback time, and recover or
mark the stop/start gap before declaring service healthy. Never let runtime
code silently switch Phase 2B to Alchemy.

```sh
cd /opt/meme-scanner
sudo systemctl stop meme-scanner-flow.service
sudo install -o ubuntu -g ubuntu -m 644 rollback/phase2b2/flow_worker.py app/flow_worker.py
sudo install -o ubuntu -g ubuntu -m 644 rollback/phase2b2/flow_reports.py app/flow_reports.py
sudo install -o ubuntu -g ubuntu -m 644 rollback/phase2b2/flow_usage_report.py scripts/flow_usage_report.py
sudo install -o ubuntu -g ubuntu -m 600 rollback/phase2b2/flow.env config/flow.env
sudo install -m 644 rollback/phase2b2/meme-scanner-flow.service /etc/systemd/system/meme-scanner-flow.service
sudo systemctl daemon-reload
sudo systemctl start meme-scanner-flow.service
```

## 24-hour operator review

Run these from the VPS after 24 hours (no credential values are printed):

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/flow_usage_report.py --hours 24
.venv/bin/python scripts/rpc_usage_report.py --hours 24
.venv/bin/python scripts/flow_security_status.py --since '24 hours ago'
systemctl show meme-scanner.service meme-scanner-flow.service -p Id -p ActiveState -p MainPID -p NRestarts -p MemoryCurrent -p CPUUsageNSec
sqlite3 data/flow.db 'PRAGMA integrity_check; SELECT count(*) FROM flow_events; SELECT coverage_quality,count(*) FROM flow_features GROUP BY coverage_quality; SELECT reason,count(*) FROM flow_gaps WHERE resolved=0 GROUP BY reason;'
sqlite3 data/scanner.db 'PRAGMA integrity_check;'
```

Use the actual main DB filename from `Config.load().database` if it differs
from `data/scanner.db`. Review event volume, complete/partial/unavailable
features, budget pauses, recovery gaps, Validation HTTP/getLogs calls,
PublicNode and standby WSS bytes, failovers, Phase 2A progress, main Alchemy
usage, DB growth and journal leak counts. Acceptance expects zero Phase 2B
Alchemy traffic, no unexplained coverage regression, no legacy 8 MB pause,
zero credential leaks, healthy main/Phase 2A, and flow DB integrity `ok`.

## Cutover record

Cutover UTC time: pending. Startup recovery range/result: pending. Thirty-minute
live validation, Alchemy isolation, main isolation, resources, DB growth,
security scan and final gate: pending. Do not report `READY_24H_SOAK` until
all checks above pass. A fresh budget, head, and per-target recovery preflight
is required before any cutover. No Phase 2C or trading change is included.
