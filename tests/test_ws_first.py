import asyncio
from dataclasses import replace
import tempfile
from pathlib import Path
import unittest
from unittest.mock import AsyncMock

from app.block_watcher import Watcher
from app.config import Config
from app.database import Database
from app.models import utc_now
from app.telemetry import Telemetry
from test_scanner import FACTORY, QUOTE, block, event


class WsFirstTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.config = Config("https://example.invalid/provider", 4663, (FACTORY,),
                             "https://api.robinhood.com/rhj/assets", root / "scanner.db", root / "scanner.log",
                             confirmations=0, start_block=10, overlap=2, retention=20, reorg_depth=5,
                             transport_mode="ws_first", live_overlap=2, startup_recovery_max=10,
                             recovery_max=10, head_healthcheck=60)
        self.db = Database(self.config.database, 4663)
        self.db.migrate()
        with self.db.conn:
            self.db.set_state("scan_start_block", 10)
            self.db.set_state("historical_checkpoint", 10)
            self.db.set_state("stock_assets_synced_at", utc_now())

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def watcher(self, head=100):
        rpc = AsyncMock()
        rpc.head.return_value = head
        rpc.block.side_effect = lambda n: block(n)
        rpc.blocks.side_effect = lambda ns: [block(n) for n in ns]
        telemetry = Telemetry(self.db)
        watcher = Watcher(self.config, rpc, self.db, cursor="live_checkpoint", telemetry=telemetry)
        watcher.pons.logs = AsyncMock(return_value=[])
        watcher.pons.verify = AsyncMock()
        return watcher

    async def test_nonstock_event_skips_metadata_and_header(self):
        watcher = self.watcher()
        await watcher.process_ws_event(event(block(99)))
        self.assertEqual(watcher.rpc.batch.await_count, 0)
        row = self.db.launch("0x" + "aa" * 32, 0)
        self.assertEqual((row["is_stock_quote"], row["token_name"], row["quote_asset_name"]), (0, None, None))
        counters = dict(self.db.conn.execute("SELECT metric,sum(count) FROM rpc_usage GROUP BY metric"))
        self.assertNotIn("block_header_cache_misses", counters)
        self.assertEqual(counters["skipped_nonstock_metadata_calls"], 4)
        await watcher.process_ws_event(event(block(99)))
        self.assertEqual(watcher.rpc.block.await_count, 0)

    async def test_stock_event_uses_registry_and_only_token_metadata(self):
        watcher = self.watcher()
        with self.db.conn:
            self.db.conn.execute("INSERT INTO stock_assets VALUES(?,?,?,?,?,?,?)",
                                 (QUOTE, "NVDA", "Nvidia", "NVDA", "test", 1, utc_now()))
        watcher.rpc.batch.return_value = ["0x" + b"TOKEN".ljust(32, b"\0").hex(),
                                          "0x" + b"Token".ljust(32, b"\0").hex()]
        await watcher.process_ws_event(event(block(99)))
        row = self.db.launch("0x" + "aa" * 32, 0)
        self.assertEqual((row["is_stock_quote"], row["quote_asset_symbol"], row["token_symbol"]), (1, "NVDA", "TOKEN"))
        watcher.rpc.batch.assert_awaited_once()
        self.assertEqual(len(watcher.rpc.batch.await_args.args[0]), 2)
        self.assertEqual(watcher.rpc.block.await_count, 0)
        counters = dict(self.db.conn.execute("SELECT metric,sum(count) FROM rpc_usage GROUP BY metric"))
        self.assertEqual((counters["quote_registry_hits"], counters["metadata_calls_stock"]), (1, 2))

    async def test_oversized_startup_recovery_records_gap_and_scans_overlap_only(self):
        watcher = self.watcher(100)
        with self.db.conn:
            self.db.set_state("live_start_block", 10)
            self.db.set_state("live_checkpoint", 10)
        watcher.pons.logs.return_value = []
        await watcher.ws_recover(10, "startup_recovery", "startup-gap")
        self.assertEqual(watcher.pons.logs.await_args.args[:2], (98, 100))
        gaps = [tuple(row) for row in self.db.conn.execute("SELECT kind,first,last FROM coverage WHERE kind='startup-gap'")]
        self.assertEqual(gaps, [("startup-gap", 10, 98)])
        self.assertEqual(self.db.last("live_checkpoint"), 100)

    async def test_healthy_ws_first_has_one_startup_scan_then_event_ingestion(self):
        watcher = self.watcher(100)
        queue = asyncio.Queue()
        await queue.put(event(block(101)))
        never = asyncio.Event()
        async def run():
            await never.wait()
        watcher.feed = type("Feed", (), {"connected": True, "connections": 1, "events": queue,
                                           "overflowed": False, "task": None, "run": staticmethod(run)})()
        await watcher.run_ws_first(max_batches=1)
        watcher.pons.logs.assert_awaited_once_with(98, 100, "startup_recovery")
        self.assertEqual(watcher.rpc.head.await_count, 1)
        self.assertEqual(self.db.last("live_checkpoint"), 101)
        watcher.feed.task.cancel()
        await asyncio.gather(watcher.feed.task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
