import _bootstrap  # noqa: F401
import argparse
import asyncio
from dataclasses import asdict
import json

from app.config import Config
from app.rpc import Rpc


async def inspect(count):
    rpc = Rpc(Config.load())
    try:
        await rpc.check_chain()
        head = await rpc.head()
        for number in range(max(0, head - count + 1), head + 1):
            print(json.dumps(asdict(await rpc.block(number))))
    finally:
        await rpc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.count <= 100:
        parser.error("--count must be 1..100")
    asyncio.run(inspect(args.count))
