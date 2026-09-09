import _bootstrap  # noqa: F401
import argparse
import asyncio
import json

from app.config import Config
from app.heads import HeadFeed


async def test(seconds):
    config = Config.load()
    if not config.rpc_ws:
        raise ValueError("No private WebSocket configured")
    feed = HeadFeed(config)
    task = asyncio.create_task(feed.run())
    try:
        for _ in range(seconds):
            await asyncio.sleep(1)
            if task.done():
                raise RuntimeError("WebSocket failed its bounded connection attempts")
        minimum = 10 if config.ws_subscription == "newHeads" else 1
        if feed.count < minimum or not feed.connected:
            raise RuntimeError("Insufficient live subscription notifications received")
        print(json.dumps({"seconds": seconds, "subscription": config.ws_subscription,
                          "notifications": feed.count, "connections": feed.connections,
                          "latest_block": feed.number, "chain_id": config.chain_id,
                          "notification_bytes": feed.bytes_received}))
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=60)
    args = parser.parse_args()
    if not 10 <= args.seconds <= 300:
        parser.error("seconds must be 10..300")
    asyncio.run(test(args.seconds))
