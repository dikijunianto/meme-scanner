"""Local Phase 2A queue and RPC usage; not provider billing."""
import _bootstrap  # noqa: F401
import argparse
import json
import sqlite3
import time
from app.config import Config

parser = argparse.ArgumentParser()
parser.add_argument("--hours", type=int, choices=(1, 24), default=1)
args = parser.parse_args()
with sqlite3.connect(Config.load().database.as_uri() + "?mode=ro", uri=True) as c:
    since = int(time.time() - args.hours * 3600) // 60 * 60
    metrics = dict(c.execute("SELECT metric,sum(count) FROM rpc_usage WHERE minute>=? AND metric LIKE 'market_%' GROUP BY metric", (since,)))
    queue = c.execute("SELECT count(*),min(due_at) FROM outcome_targets WHERE status='pending'").fetchone()
    done = metrics.get("market_targets_completed", 0)
    print(json.dumps({"hours": args.hours, "metrics": metrics, "queue_depth": queue[0], "oldest_due_at": queue[1],
                      "rpc_calls_per_completed_snapshot": round(metrics.get("market_rpc_calls", 0) / done, 2) if done else None}, indent=2))
