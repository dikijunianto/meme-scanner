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


def setup_logging(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), RotatingFileHandler(
                            path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.CRITICAL)


async def main(max_batches=None):
    config = Config.load()
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
    db, rpc = Database(config.database, config.chain_id), Rpc(config)
    feed = HeadFeed(config) if config.rpc_ws else None
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
    try:
        await Watcher(config, rpc, db, feed).run(max_batches)
    finally:
        if feed and feed.task:
            feed.task.cancel()
            await asyncio.gather(feed.task, return_exceptions=True)
        await rpc.close()
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
