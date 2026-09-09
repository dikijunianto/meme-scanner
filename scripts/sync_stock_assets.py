import _bootstrap  # noqa: F401
import asyncio
from app.config import Config
from app.database import Database
from app.stock_assets import sync_assets


async def main():
    config = Config.load()
    db = Database(config.database, config.chain_id)
    try:
        print(f"Imported {await sync_assets(config, db)} verified mainnet stock assets")
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
