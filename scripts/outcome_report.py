"""Read-only, denominator-explicit Phase 2A outcome report."""
import _bootstrap  # noqa:F401
import argparse,json,sqlite3
from app.config import Config
from app.outcomes import report
p=argparse.ArgumentParser(); g=p.add_mutually_exclusive_group(); g.add_argument("--hours",type=int); g.add_argument("--days",type=int,default=7)
p.add_argument("--ticker"); p.add_argument("--quote-address"); p.add_argument("--min-completeness",type=float,default=0); p.add_argument("--quality",default="verified")
a=p.parse_args()
if not 0<=a.min_completeness<=1: p.error("--min-completeness must be 0..1")
with sqlite3.connect(Config.load().database.as_uri()+"?mode=ro",uri=True) as c:
 c.row_factory=sqlite3.Row; print(json.dumps(report(c,days=a.days,hours=a.hours,ticker=a.ticker,quote_address=a.quote_address,min_completeness=a.min_completeness,quality=a.quality),indent=2))
