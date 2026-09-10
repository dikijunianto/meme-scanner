"""Local counters, not Alchemy billing; no RPC calls or credentials in output."""
import _bootstrap  # noqa: F401
import argparse
from datetime import datetime, timezone
import json
import sqlite3
from app.config import Config

BASELINE = {"http_total": 181138, "method:eth_getLogs": 91305,
            "method:eth_getBlockByNumber": 67848, "method:eth_call": 53134,
            "method:eth_blockNumber": 12641}


def report(conn, hours, now=None, compare_baseline=False):
    now = now or datetime.now(timezone.utc)
    state = dict(conn.execute("SELECT key,value FROM chain_state"))
    started = state.get("telemetry_started_at")
    elapsed = max(0, (now - datetime.fromisoformat(started)).total_seconds()) if started else 0
    counts = dict(conn.execute("SELECT metric,sum(count) FROM rpc_usage WHERE minute>=? AND minute<=? GROUP BY metric",
                               (int(now.timestamp() - hours * 3600) // 60 * 60, int(now.timestamp()))))
    result = {"label": "LOCAL OBSERVATIONS — NOT ACCOUNT BILLING DATA", "requested_hours": hours,
            "telemetry_started_at": started, "elapsed_hours": round(elapsed / 3600, 4),
            "full_window_elapsed": elapsed >= hours * 3600, "bucket_seconds": 60,
            "websocket_last_recorded_status": state.get("websocket_status"),
            "counters": counts, "note": "Missing counters mean zero. Counts include retries; method calls count batch members separately from HTTP envelopes. WebSocket bytes are notification payloads, excluding handshake/framing. Failed requests count failed envelopes. No billing or CU projection is claimed."}
    if compare_baseline:
        phase16 = state.get("phase16_telemetry_started_at")
        if phase16:
            seconds = max(1, (now - datetime.fromisoformat(phase16)).total_seconds())
            since = int(datetime.fromisoformat(phase16).timestamp()) // 60 * 60
            measured = dict(conn.execute("SELECT metric,sum(count) FROM rpc_usage WHERE minute>=? AND minute<=? GROUP BY metric",
                                         (since, int(now.timestamp()))))
            daily = {metric: round(measured.get(metric, 0) * 86400 / seconds, 2) for metric in BASELINE}
            result["phase16_comparison"] = {"started_at": phase16, "elapsed_hours": round(seconds / 3600, 4),
                "daily_equivalent": daily, "phase15_baseline_daily": BASELINE,
                "reduction_percent": {metric: round(100 * (1 - daily[metric] / baseline), 2)
                                      for metric, baseline in BASELINE.items()}}
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, choices=(1, 24, 72), default=1)
    parser.add_argument("--compare-baseline", action="store_true")
    args = parser.parse_args()
    with sqlite3.connect(Config.load().database.as_uri() + "?mode=ro", uri=True) as conn:
        print(json.dumps(report(conn, args.hours, compare_baseline=args.compare_baseline), indent=2))
