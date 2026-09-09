"""Bounded historical log search. Prints evidence; does not move watcher state."""
import _bootstrap  # noqa: F401
import argparse
import asyncio
import json
from pathlib import Path

from app.config import Config
from app.database import Database
from app.models import quantity
from app.pons import Pons
from app.rpc import Rpc
from app.stock_assets import sync_assets


async def inspect(args):
    config = Config.load()
    db, rpc = Database(config.database, config.chain_id), Rpc(config)
    evidence = {"chain_id": config.chain_id, "factories": list(config.factories), "ranges": [], "launches": []}
    try:
        await rpc.check_chain()
        pons = Pons(rpc, config, db)
        await pons.verify()
        await sync_assets(config, db)
        end = args.to_block if args.to_block is not None else max(0, await rpc.head() - config.confirmations)
        floor = max(0, end - args.max_blocks + 1)
        chunk = args.chunk
        while end >= floor:
            start = max(floor, end - chunk + 1)
            events = await pons.logs(start, end)
            evidence["ranges"].append([start, end])
            print(f"Searched {start}..{end}: {len(events)} launches", flush=True)
            for event in events:
                block = await rpc.block(quantity(event["blockNumber"]))
                launch = (await pons.launches([event], block))[0]
                if (await rpc.block(block.number)).hash != block.hash:
                    raise ValueError("Historical block changed during read")
                evidence["launches"].append({"raw_event": event, "decoded": launch})
                print(json.dumps(launch), flush=True)
            if evidence["launches"] and (not args.find_stock or any(
                    item["decoded"]["is_stock_quote"] for item in evidence["launches"])):
                break
            end = start - 1
            await asyncio.sleep(1)
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        if not evidence["launches"]:
            print("No matching launches in the recorded bounded range; no event fabricated.")
    finally:
        await rpc.close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-blocks", type=int, default=10000)
    parser.add_argument("--chunk", type=int, default=1000)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--find-stock", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    if not 1 <= args.max_blocks <= 1000000 or not 1 <= args.chunk <= 10000:
        parser.error("max-blocks must be 1..1000000 and chunk 1..10000")
    if args.to_block is not None and args.to_block < 0:
        parser.error("to-block must be nonnegative")
    asyncio.run(inspect(args))
