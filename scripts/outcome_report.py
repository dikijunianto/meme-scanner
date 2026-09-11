"""Outcome statistics from stored snapshots only; no RPC calls."""
import _bootstrap  # noqa: F401
import argparse
from decimal import Decimal
import json
import sqlite3
import time
from app.config import Config

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
group.add_argument("--hours", type=int)
group.add_argument("--days", type=int, default=7)
parser.add_argument("--ticker")
parser.add_argument("--min-completeness", type=float, default=0)
args = parser.parse_args()
seconds = args.hours * 3600 if args.hours else args.days * 86400
with sqlite3.connect(Config.load().database.as_uri() + "?mode=ro", uri=True) as c:
    c.row_factory = sqlite3.Row
    rows = c.execute("SELECT l.id,l.token_address,l.quote_asset_symbol FROM launches l WHERE l.is_stock_quote=1 AND l.detected_at>=?" +
                     (" AND l.quote_asset_symbol=?" if args.ticker else ""),
                     (time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime(time.time()-seconds)), *( [args.ticker] if args.ticker else []))).fetchall()
    multiples=[]; complete=0
    for row in rows:
        prices=[Decimal(x[0]) for x in c.execute("SELECT price_quote FROM market_snapshots WHERE launch_id=? AND price_quote IS NOT NULL ORDER BY target_age_seconds", (row["id"],))]
        if len(prices) >= 2 and prices[0] > 0:
            multiples.append(prices[-1]/prices[0]); complete += len(prices) >= 6
    def pct(p): return round(100*sum(x >= p for x in multiples)/len(multiples),2) if multiples else None
    print(json.dumps({"launches_analyzed":len(rows),"sampled_launches":len(multiples),"complete_outcomes":complete,
                      "incomplete_outcomes":len(rows)-complete,"pct_ge_2x":pct(Decimal(2)),"pct_ge_5x":pct(Decimal(5)),
                      "pct_ge_10x":pct(Decimal(10)),"pct_le_0_5x":round(100*sum(x<=Decimal('.5') for x in multiples)/len(multiples),2) if multiples else None,
                      "pct_le_0_1x":round(100*sum(x<=Decimal('.1') for x in multiples)/len(multiples),2) if multiples else None},indent=2))
