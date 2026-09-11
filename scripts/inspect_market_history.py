"""Print stored Phase 2A snapshots; never calls RPC."""
import _bootstrap  # noqa: F401
import argparse
import json
import sqlite3
from eth_utils import to_checksum_address
from app.config import Config

parser = argparse.ArgumentParser()
parser.add_argument("token_address")
args = parser.parse_args()
with sqlite3.connect(Config.load().database.as_uri() + "?mode=ro", uri=True) as c:
    c.row_factory = sqlite3.Row
    token = to_checksum_address(args.token_address)
    launch = c.execute("SELECT * FROM launches WHERE token_address=? ORDER BY block_number DESC LIMIT 1", (token,)).fetchone()
    snapshots = [dict(x) for x in c.execute("SELECT * FROM market_snapshots WHERE token_address=? ORDER BY target_age_seconds", (token,))]
    print(json.dumps({"launch": dict(launch) if launch else None, "snapshots": snapshots}, indent=2))
