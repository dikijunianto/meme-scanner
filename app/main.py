import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import signal
import sys
import time

from app.block_watcher import DeepReorg, Watcher
from app.config import Config
from app.database import Database
from app.heads import HeadFeed
from app.rpc import RetriesExhausted, Rpc
from app.telemetry import Telemetry
from app.stock_assets import sync_assets
from datetime import datetime, timezone


def setup_logging(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), RotatingFileHandler(
                            path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.CRITICAL)


async def main(max_batches=None, backfill=None):
    config = Config.load()
    if config.mode == "backfill" and backfill is None:
        raise ValueError("Backfill requires python -m app.backfill with an explicit range")
    setup_logging(config.log_path)
    config.database.parent.mkdir(parents=True, exist_ok=True)
    lock = config.database.with_suffix(".lock").open("a")
    if sys.platform != "win32":
        import fcntl
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise ValueError("Another watcher holds this database lock") from None
    db = Database(config.database, config.chain_id)
    db.migrate()
    telemetry = Telemetry(db)
    rpc = Rpc(config, telemetry)
    feed = HeadFeed(config, telemetry) if config.rpc_ws and backfill is None else None
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
    try:
        watcher = Watcher(config, rpc, db, feed, cursor="live_checkpoint" if backfill is None else None,
                          telemetry=telemetry)
        if backfill is None:
            await watcher.run(max_batches)
        else:
            await rpc.check_chain()
            await watcher.pons.verify()
            stamp = db.state("stock_assets_synced_at")
            if stamp is None or (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds() > 172800:
                await sync_assets(config, db)
            first, last = backfill
            head = await rpc.head()
            if last > head - config.confirmations:
                raise ValueError("Backfill end must be confirmed and not in the future")
            for start in range(first, last + 1, config.batch):
                await watcher.process_range(start, min(last, start + config.batch - 1), head)
    finally:
        if feed and feed.task:
            feed.task.cancel()
            await asyncio.gather(feed.task, return_exceptions=True)
        await rpc.close()
        if backfill is None:
            telemetry.status("stopped")
        db.close()
        lock.close()
        logging.info("Scanner stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read-only Pons V2 scanner")
    parser.add_argument("--max-batches", type=int, help="Stop after N successful polling iterations")
    args = parser.parse_args()
    if args.max_batches is not None and args.max_batches < 1:
        parser.error("--max-batches must be positive")
    try:
        asyncio.run(main(args.max_batches))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except DeepReorg:
        sys.exit(2)
    except RetriesExhausted:
        logging.error("Retry budget exhausted; service stopped for operator review")
        sys.exit(3)
    except Exception as exc:
        print(f"Scanner startup failed ({type(exc).__name__}); check configuration", file=sys.stderr)
        sys.exit(1)
