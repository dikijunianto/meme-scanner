"""Read-only coverage report; historical coverage is launches only."""
import _bootstrap  # noqa: F401
import asyncio
import json
import sqlite3
from app.config import Config
from app.rpc import Rpc


def coverage(conn, head):
    state = dict(conn.execute("SELECT key,value FROM chain_state"))
    historical = ([int(state["scan_start_block"]), int(state["historical_checkpoint"])]
                  if "scan_start_block" in state and "historical_checkpoint" in state else None)
    ranges = [list(row) for row in conn.execute("SELECT kind,first,last FROM coverage ORDER BY first")]
    intervals = ([historical] if historical else []) + [[r[1], r[2]] for r in ranges]
    merged = []
    for first, last in sorted(intervals):
        if merged and first <= merged[-1][1] + 1:
            merged[-1][1] = max(last, merged[-1][1])
        else:
            merged.append([first, last])
    gaps = [[a[1] + 1, b[0] - 1] for a, b in zip(merged, merged[1:])]
    checkpoint = int(state["live_checkpoint"]) if "live_checkpoint" in state else None
    return {"historical_launch_coverage": historical, "phase15_event_coverage": ranges,
            "unprocessed_gaps": gaps, "live_start_block": state.get("live_start_block"),
            "live_checkpoint": checkpoint, "current_head": head,
            "live_lag_blocks": head - checkpoint if checkpoint is not None else None,
            "unprocessed_tail": [merged[-1][1] + 1, head] if merged and merged[-1][1] < head else None,
            "note": "Historical range covers launches only; phase15 ranges cover launches and graduations. Gaps exclude the unprocessed head tail."}


async def main():
    config = Config.load()
    rpc = Rpc(config)
    try:
        head = await rpc.head()
        with sqlite3.connect(config.database.as_uri() + "?mode=ro", uri=True) as conn:
            print(json.dumps(coverage(conn, head), indent=2))
    finally:
        await rpc.close()


if __name__ == "__main__":
    asyncio.run(main())
