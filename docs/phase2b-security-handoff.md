# Phase 2B security rotation — manual provider handoff

Primary final state: **MANUAL_PROVIDER_ACTION_REQUIRED**.

Local preparation is complete. No authorized Alchemy management access was found:
no management connector, signed-in browser session available to this task,
management access-token configuration, Alchemy CLI or Alchemy account configuration.
The existing JSON-RPC key is not an Admin API access key. No replacement was
invented, no old key was revoked, and flow was not resumed.

## Evidence, 2026-09-20

| Check | Result |
|---|---|
| Baseline Git | Clean main at `26d5c270be73711db53b71cced6e12e0c5873456` |
| Tests before | **TEST VERIFIED:** 91 passed locally and on VPS |
| Tests after | **TEST VERIFIED:** 102 passed on Ubuntu VPS; Windows skips 3 POSIX installation tests |
| Main service | **LIVE VERIFIED:** active/enabled, PID **842555**, NRestarts **0**; no restart performed |
| Flow service | **LIVE VERIFIED:** inactive/disabled, PID **0**, NRestarts **0**, enabled flag false |
| Old configured credential fingerprint | `831e6b644134` — SHA-256 of the key, first 12 hex characters |
| HTTP/WSS identity | **LIVE VERIFIED:** actual key strings match; main and flow share the same protected Config source |
| New credential fingerprint | **UNRESOLVED:** no replacement obtained |
| Old credential revoked | **No**; intentionally retained until main works on a replacement |
| Main on replacement credential | **UNRESOLVED:** no switchover attempted |
| Provider-side verification | **UNRESOLVED:** no management access; no provider mutation attempted |
| Database integrity / journal | **LIVE VERIFIED:** main and flow `ok` / WAL |
| Flow offline validation | **LIVE VERIFIED:** passed, chain 4663, separate read-only main path, zero RPC calls |
| Historical journal exposure | **LIVE VERIFIED:** 78 flow-service records, 08:55:37–12:14:08 UTC; main records matched zero in the inspected September 20 window |
| Current journal review | **LIVE VERIFIED:** 12:59:00–13:24:34 UTC, zero credential URL, auth-error, client INFO URL or traceback-URL matches for both services |
| New-key log-match count | **UNRESOLVED / not measured**; current clean logs must not be presented as replacement-key validation |
| Main / Phase 2A health | **LIVE VERIFIED:** recent launches; 438 WS events and 23 market targets completed in the baseline hour; 615 market targets in the later 24h report |
| Memory | **LIVE VERIFIED:** main cgroup 108,261,376 bytes at final service check; flow stopped |
| Free disk | **LIVE VERIFIED:** 32,799,846,400 bytes at baseline |
| DB sizes | Main 177,381,376 bytes plus WAL at baseline; flow 16,818,176 bytes, unchanged logical data during this task |
| CPU / post-rotation DB-growth benchmark | **UNRESOLVED / not run**, because no rotation or resume occurred |
| Flow re-enable time / new benchmark duration | **Not performed**; no historical benchmark is reused as post-rotation evidence |

Only logging/security entry-point changes were deployed: main also restricts
HTTPcore logging, and the flow CLI suppresses credential-bearing startup exception
values while reporting the exception type and a failed exit status. Main's new
HTTPcore setting will take effect at its eventual credential-reload restart;
the currently running main process was not restarted during preparation.

The security regression suite covers credential-shaped HTTP/WSS URLs, HTTP 401
and transport failures, base/flow WebSocket failures, sanitized startup errors,
hidden fingerprints, offline status, journal count redaction, atomic installation,
same-key rejection, duplicate fields, permissions and failure rollback. Tests use
dummy credentials only; no live credential was submitted by the preparation helpers.

## Secret inventory and permissions

| File | Fields / role | Owner / mode | Parent mode |
|---|---|---|---|
| `/opt/meme-scanner/config/.env` | `ROBINHOOD_RPC_HTTP`, `ROBINHOOD_RPC_WS`; active shared credential | ubuntu / 0600 | **0700**, corrected from 0777 |
| `/opt/meme-scanner/config/flow.env` | Flow control values only; no duplicated endpoint credential | ubuntu / 0600 | 0700 |
| `/opt/meme-scanner/data/phase15-backup/config-before.env` | Archived pre-Phase-1.5 configuration containing the old credential; not an active consumer | ubuntu / 0600 | 0700 |

The archived config was retained, not rewritten or deleted. It must not be used to
restore the compromised key after rotation. No new DB or secret backup was created.

Both systemd units are root-owned 0644. Main selects its protected configuration
through `SCANNER_ENV`; flow selects its controls through `FLOW_ENV` and reads the
shared default Config. The endpoint keys are not in the main process OS environment;
only its configuration-file selector is present. Configuration fingerprints do not
claim to inspect an already-running Python process's memory.

Consumer/source references were inspected in Config, flow worker, RPC wrapper,
service/deployment files, scripts, shell profiles/history, cron and timers. Ubuntu
has no crontab; root's crontab has no project references or old-key match; no project
timer was found. An additional text scan of relevant `/tmp`, data/config backups and
research files found only the archived config above. No secret contents or journal
matching lines were printed. Compressed/binary archives were outside that text scan.

Application/script/deployment and related project directories had inherited unsafe
group/world write access. Removed that access from **110 owned paths** without
changing file contents or owner execution rights. Top-level app, scripts, deploy,
tests, docs and data directories are now 0755. Project root was already 0755,
`/opt` root-owned 0755, and the secret configuration directory is 0700.

**Git scan:** all 176 reachable baseline objects / 116 blobs were scanned using
provider URL/key patterns, with no candidate credential findings. The current secret
files and archived backup path are ignored and untracked. No history rewrite.
Before pushing, staged additions are scanned again. This is a scoped pattern scan,
not a claim that arbitrary unknown credential formats can be recognized perfectly.

## Existing collection state — preserved, not new security-benchmark data

The stopped flow dataset still has 9 targets and **8,560 raw rows**: 415 curve buys,
325 curve sells, 3,925 V4 swaps and 3,895 hook events. Thirteen duplicate attempts,
zero removed logs. Stored windows remain **38 complete / 3 partial**. Persisted
active-target labels are historical tracking state; systemd inactive/disabled is
authoritative for whether collection is running.

The earlier collection run used 78 HTTP members/envelopes: 54 getLogs, 9 headers,
5 metadata calls, 10 blockNumber calls, zero transaction/receipt lookups. Its
8,000,636 received WS bytes crossed the 8,000,000-byte cap and paused flow. No
additional flow collection was performed in this security task. This was legitimate
budget behavior, not a reason to increase the limit.

Actual preserved limits: 1,000 daily RPC members, 12/minute, 400 getLogs/day,
8,000,000 WS bytes/day, 64 subscriptions, 100 recovery blocks, 0.5 HTTP envelopes/s.
Sampling, feature/tracking windows, event semantics, schema, recovery, reports and
Phase 2A collection logic were not changed. No paid service or dependency added.

## Minimal operator handoff

1. In your Alchemy account, create a **separate replacement app/key** with Robinhood
   Chain mainnet enabled, within the existing free plan. Keep the current app/key
   active. Do not rotate the currently used app key in place yet: Alchemy says that
   action invalidates the previous key within two minutes, which would violate the
   required replacement-first switchover order. See [create an app/key](https://www.alchemy.com/docs/create-an-api-key)
   and [rotation behavior](https://www.alchemy.com/docs/how-to-rotate-api-keys).
2. In an interactive WSL SSH terminal, run:

   ```sh
   ssh meme-scanner
   cd /opt/meme-scanner
   .venv/bin/python scripts/install_rpc_credential.py
   .venv/bin/python scripts/flow_security_status.py
   ```

   Enter the replacement key only at the helper's two **hidden prompts**. Do not
   paste it into chat, Git, command arguments or shell history. The helper updates
   only `ROBINHOOD_RPC_HTTP` and `ROBINHOOD_RPC_WS` atomically in the existing protected
   file. It stages a 0600 file in a 0700 directory outside the project, verifies the
   filesystem supports atomic replacement, fsyncs and removes the staging name.
   Other settings are preserved. It prints only fingerprints and status.
3. Return with **“replacement installed”** and the safe fingerprint if desired.
   Do not restart services, revoke the old key or enable flow yet. The installation
   helper deliberately performs none of those actions. I will validate replacement
   authentication, refresh only main as necessary, and prove main/Phase 2A health
   before the old-key revocation step. Revocation still requires your provider
   action unless authorized management access becomes available.

After replacement works, revoke the old key through Alchemy, confirm its failure
and the replacement's success, validate clean post-switch logs, then resume only
flow. A new 30–60 minute benchmark and the stop/start flow-isolation check remain
required. The old credential must not be revoked ahead of that main verification.

## Tomorrow's 24h coverage review

```sh
cd /opt/meme-scanner
.venv/bin/python scripts/flow_usage_report.py --hours 24
.venv/bin/python scripts/rpc_usage_report.py --hours 24
.venv/bin/python scripts/market_usage_report.py --hours 24
.venv/bin/python scripts/flow_security_status.py --since '24 hours ago'
```

For the post-rotation security gate, use the actual switchover UTC timestamp as
`--since`; otherwise the known historical leak will correctly appear in a 24h scan.
These reports never create provider access, authenticate/revoke keys, resume flow
or claim a running process has reloaded configuration. Historical leaked journal
records remain present. **Old key revoked = false** until provider evidence proves
otherwise. No journal deletion was performed.
