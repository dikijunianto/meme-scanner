import _bootstrap  # noqa: F401
import asyncio
import argparse
from dataclasses import replace
from datetime import datetime
import json
import sys
from zoneinfo import ZoneInfo

from app.config import Config
from app.rpc import Rpc


async def test(fallback=False):
    config = Config.load()
    if fallback:
        if not config.fallback_http:
            raise ValueError("No optional fallback configured")
        config = replace(config, rpc_http=config.fallback_http)
    rpc = Rpc(config)
    try:
        chain = await rpc.check_chain()
        block = await rpc.block(await rpc.head())
        print(json.dumps({"chain_id": chain, "block_number": block.number, "block_hash": block.hash,
                          "timestamp_jakarta": datetime.fromisoformat(block.timestamp).astimezone(
                              ZoneInfo("Asia/Jakarta")).isoformat(), "tx_count": block.tx_count}, indent=2))
    finally:
        await rpc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fallback", action="store_true", help="Basic connectivity only; never switches production")
    args = parser.parse_args()
    try:
        asyncio.run(test(args.fallback))
    except Exception as exc:
        print(f"RPC test FAILED ({type(exc).__name__})", file=sys.stderr)
        sys.exit(1)
