import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import httpx
from eth_abi import encode, decode

from app.block_watcher import DeepReorg, Watcher
from app.database import Database, PHASE15_SCHEMA
from app.heads import HeadFeed
from app.models import Block
from app.pons import decode_graduation, EVENT_TOPICS, LAUNCH_STATE_TYPES
from app.rpc import Rpc, RpcError
from app.telemetry import Telemetry
from test_scanner import block, event, FACTORY
import test_scanner as phase1
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from show_coverage import coverage
from rpc_usage_report import report


class LiveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Reuse the Phase 1 fixture setup without inheriting/rerunning its tests.
        phase1.ScannerTests.setUp(self)
        self.config = replace(self.config, live_overlap=2, recovery_max=100)
        self.db.save_block(block(10), [], 20)
        with self.db.conn:
            self.db.set_state("scan_start_block", 10)

    tearDown = phase1.ScannerTests.tearDown

    def watcher(self, head=100, feed=None):
        self.db.migrate()
        rpc = AsyncMock()
        rpc.head.return_value = head
        rpc.block.side_effect = lambda n: block(n)
        rpc.blocks.side_effect = lambda numbers: [block(n) for n in numbers]
        rpc.batch.return_value = ["0x" + encode(["string"], ["TEST"]).hex()] * 2
        w = Watcher(self.config, rpc, self.db, feed, cursor="live_checkpoint", telemetry=Telemetry(self.db))
        w.pons.logs = AsyncMock(return_value=[])
        return w

    def test_migration_preserves_data_and_is_idempotent(self):
        launch = phase1.ScannerTests.launch(self, block(10))
        self.db.save_block(block(10), [launch], 20)
        before = [tuple(r) for r in self.db.conn.execute("SELECT * FROM launches")]
        self.db.migrate()
        state = dict(self.db.conn.execute("SELECT key,value FROM chain_state"))
        self.db.migrate()
        self.assertEqual(state, dict(self.db.conn.execute("SELECT key,value FROM chain_state")))
        self.assertEqual(before, [tuple(r) for r in self.db.conn.execute("SELECT * FROM launches")])
        self.assertEqual(self.db.last("historical_checkpoint"), 10)
        self.assertIsNone(self.db.last("live_checkpoint"))

    def test_failed_migration_rolls_back_ddl_and_state(self):
        with patch("app.database.PHASE15_SCHEMA", PHASE15_SCHEMA + ";INVALID SQL;"):
            with self.assertRaises(sqlite3.Error):
                self.db.migrate()
        self.assertIsNone(self.db.state("phase15_migrated_at"))
        self.assertIsNone(self.db.conn.execute("SELECT name FROM sqlite_master WHERE name='graduations'").fetchone())
        self.assertEqual(self.db.last(), 10)
        self.assertEqual(self.db.conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.db.migrate()  # Retry is safe after the failed transaction.

    def test_sqlite_backup_restore_preserves_original(self):
        dest = sqlite3.connect(":memory:")
        self.db.conn.backup(dest)
        self.db.migrate()
        self.assertEqual(dict(dest.execute("SELECT key,value FROM chain_state"))["last_processed_block"], "10")
        self.assertIsNone(dest.execute("SELECT name FROM sqlite_master WHERE name='graduations'").fetchone())
        self.assertEqual(dest.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        dest.backup(self.db.conn)
        self.assertIsNone(self.db.state("phase15_migrated_at"))
        self.assertEqual(self.db.last(), 10)
        self.assertIsNone(self.db.conn.execute("SELECT name FROM sqlite_master WHERE name='graduations'").fetchone())
        self.assertEqual(self.db.conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        dest.close()

    async def test_live_ignores_old_backlog_and_preserves_independent_cursors(self):
        w = self.watcher()
        await w.step()
        w.pons.logs.assert_awaited_once_with(98, 100)
        self.assertEqual(self.db.last(), 10)
        self.assertEqual(self.db.last("historical_checkpoint"), 10)
        self.assertEqual(self.db.last("live_start_block"), 98)
        self.assertEqual(self.db.last("live_checkpoint"), 100)
        self.assertEqual(coverage(self.db.conn, 103)["unprocessed_gaps"], [[11, 97]])
        self.assertEqual(coverage(self.db.conn, 103)["live_lag_blocks"], 3)

    async def test_restart_and_reconnect_overlap_are_bounded_idempotent(self):
        feed = SimpleNamespace(connections=1, task=object(), latest=lambda: None)
        w = self.watcher(feed=feed)
        w.pons.logs.return_value = [event(block(99))]
        await w.step()
        metadata_calls = w.rpc.batch.await_count
        feed.connections = 2
        w.rpc.head.return_value = 104
        await w.step()
        w.pons.logs.assert_awaited_with(98, 104)
        self.assertEqual(w.rpc.batch.await_count, metadata_calls)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM launches").fetchone()[0], 1)
        self.assertEqual(report(self.db.conn, 1)["counters"]["blocks_recovered"], 7)
        restarted = self.watcher(107)
        await restarted.step()
        restarted.pons.logs.assert_awaited_with(102, 107)
        self.assertEqual(self.db.last("historical_checkpoint"), 10)

    async def test_oversized_disconnect_stops_without_skipping(self):
        await self.watcher().step()
        w = self.watcher(1000)
        with self.assertRaises(DeepReorg):
            await w.step()
        w.pons.logs.assert_not_awaited()
        self.assertEqual(self.db.last("live_checkpoint"), 100)

    async def test_backfill_isolated_and_coverage_splits_gap(self):
        await self.watcher().step()
        w = self.watcher()
        w.cursor = None
        await w.process_range(20, 22, 100)
        await w.process_range(20, 22, 100)
        self.assertEqual(self.db.last("live_checkpoint"), 100)
        self.assertEqual(self.db.last("historical_checkpoint"), 10)
        self.assertEqual(coverage(self.db.conn, 103)["unprocessed_gaps"], [[11, 19], [23, 97]])
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM coverage WHERE kind='backfill'").fetchone()[0], 1)

    async def test_live_reorg_removes_orphans_preserves_history(self):
        historical = phase1.ScannerTests.launch(self, block(10))
        historical["tx_hash"] = "0x" + "bb" * 32
        self.db.save_block(block(10), [historical], 20)
        w = self.watcher()
        w.pons.logs.return_value = [event(block(99))]
        await w.step()
        w.rpc.block.side_effect = lambda n: block(n, 1) if n > 98 else block(n)
        await w.reconcile(100)
        self.assertEqual(self.db.last("live_checkpoint"), 98)
        self.assertEqual(self.db.last("historical_checkpoint"), 10)
        self.assertEqual(self.db.conn.execute("SELECT block_number FROM launches").fetchone()[0], 10)
        self.assertEqual(self.db.conn.execute("SELECT last FROM coverage WHERE kind='live'").fetchone()[0], 98)
        w.rpc.block.side_effect = lambda n: block(n, 2)
        with self.assertRaises(DeepReorg):
            await w.reconcile(100)
        self.assertEqual(self.db.last("live_checkpoint"), 98)

    async def test_range_failure_cannot_commit_partial_progress(self):
        w = self.watcher()
        w.pons.logs.return_value = [event(block(99))]
        w.pons.graduations = AsyncMock(side_effect=ValueError("invalid graduation"))
        with self.assertRaises(ValueError):
            await w.step()
        self.assertIsNone(self.db.last("live_checkpoint"))
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM launches").fetchone()[0], 0)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM coverage").fetchone()[0], 0)

    def test_sql_failure_rolls_back_entire_range(self):
        self.db.migrate()
        first = phase1.ScannerTests.launch(self, block(98))
        second = dict(first, tx_hash="0x" + "cc" * 32, block_number=99, is_stock_quote=2)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.save_range([block(98), block(99)], [[first], [second]], [[], []], 98, 99,
                               "live_checkpoint", 20)
        self.assertIsNone(self.db.last("live_checkpoint"))
        self.assertIsNone(self.db.block(98))
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM launches").fetchone()[0], 0)

    async def test_backfill_rejects_fork_without_touching_live_data(self):
        await self.watcher().step()
        w = self.watcher()
        w.cursor = None
        w.rpc.block.side_effect = lambda n: block(n, 1)
        with self.assertRaises(DeepReorg):
            await w.process_range(98, 100, 100)
        self.assertEqual(self.db.last("live_checkpoint"), 100)
        self.assertEqual(self.db.block_hash(100), block(100).hash)

    async def test_rpc_telemetry_counts_envelopes_methods_retries_without_secrets(self):
        self.db.migrate()
        telemetry = Telemetry(self.db)
        rpc = Rpc(self.config, telemetry)
        await rpc.client.aclose()
        attempts = 0
        def response(request):
            nonlocal attempts
            attempts += 1
            payload = json.loads(request.content)
            if attempts == 1:
                return httpx.Response(429)
            if attempts == 4:
                return httpx.Response(403, text=self.config.rpc_http)
            def item(p):
                return {"jsonrpc": "2.0", "id": p["id"], "result": "0x1237"}
            return httpx.Response(200, json=[item(p) for p in payload] if isinstance(payload, list) else item(payload))
        rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        try:
            with patch("app.rpc.asyncio.sleep", new_callable=AsyncMock), self.assertLogs("app.rpc") as logs:
                await rpc.check_chain()
                await rpc.batch([("eth_call", []), ("eth_call", [])])
                with self.assertRaises(RpcError):
                    await rpc.head()
            result = report(self.db.conn, 72)
            counters = result["counters"]
            self.assertEqual({k: counters[k] for k in ["http_total", "http_429", "retries", "failed_requests"]},
                             {"http_total": 4, "http_429": 1, "retries": 1, "failed_requests": 2})
            self.assertEqual(counters["method:eth_call"], 2)
            self.assertEqual(counters["method:eth_chainId"], 2)
            self.assertFalse(result["full_window_elapsed"])
            self.assertNotIn("secret", json.dumps(result) + str(logs.output))
            with self.assertRaises(ValueError):
                telemetry.add(self.config.rpc_http)
        finally:
            await rpc.close()

    async def test_headers_batch_at_most_five_and_validate_numbers(self):
        rpc = Rpc(self.config)
        await rpc.client.aclose()
        sizes = []
        def response(request):
            payload = json.loads(request.content)
            sizes.append(len(payload))
            return httpx.Response(200, json=[{"jsonrpc": "2.0", "id": p["id"], "result": {
                "number": p["params"][0], "hash": block(int(p["params"][0], 16)).hash,
                "parentHash": block(int(p["params"][0], 16)).parent,
                "timestamp": "0x1", "transactions": []}} for p in reversed(payload)])
        rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        try:
            with patch("app.rpc.asyncio.sleep", new_callable=AsyncMock):
                blocks = await rpc.blocks(list(range(10, 22)))
            self.assertEqual([b.number for b in blocks], list(range(10, 22)))
            self.assertEqual(sizes, [5, 5, 2])
            with self.assertRaises(ValueError):
                await rpc.batch([("eth_call", [])] * 6)
        finally:
            await rpc.close()

    def test_real_graduation_matches_pool_manager_initialize(self):
        path = Path(__file__).parent / "fixtures/graduations/ponsx.json"
        row = json.loads(path.read_text())["events"][0]
        b = Block(int(row["blockNumber"], 16), row["blockHash"], "0x" + "00" * 32, row["block_timestamp"], 1)
        result = decode_graduation(row, b, (FACTORY,), *row["factory_state"])
        init = row["initialize_event"]
        self.assertEqual(result["pool_id"], init["topics"][1])
        self.assertEqual(result["tx_hash"], init["transactionHash"])
        self.assertEqual(result["pool_manager_address"].lower(), init["address"].lower())
        fee, spacing, hook, _, _ = decode(["uint24", "int24", "address", "uint160", "int24"], bytes.fromhex(init["data"][2:]))
        self.assertEqual((result["fee"], result["tick_spacing"], result["hooks"].lower()), (fee, spacing, hook))
        self.assertEqual(result["curve_address"], row["launch"]["curve_address"])
        self.db.migrate()
        for _ in range(2):
            self.db.save_range([b], [[]], [[result, result]], b.number, b.number, None, 20)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM graduations").fetchone()[0], 1)
        bad = dict(row, removed=True)
        with self.assertRaises(ValueError):
            decode_graduation(bad, b, (FACTORY,), *row["factory_state"])
        with self.db.conn:
            self.db.set_state("live_start_block", b.number - 1)
            self.db.set_state("live_checkpoint", b.number)
        self.db.rollback_to(b.number - 1, "live_checkpoint")
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM graduations").fetchone()[0], 0)
        self.assertEqual(self.db.last("historical_checkpoint"), 10)

    async def test_filtered_ws_reconnect_telemetry(self):
        self.db.migrate()
        telemetry = Telemetry(self.db)
        feed = HeadFeed(self.config, telemetry)
        connections = 0
        sent = []
        class Socket:
            def __init__(self, number): self.number, self.received = number, 0
            async def send(self, raw): sent.append(json.loads(raw))
            async def recv(self):
                self.received += 1
                if self.received <= 2:
                    return json.dumps({"jsonrpc": "2.0", "id": self.received, "result": "0x1237" if self.received == 1 else "sub"})
                if self.number >= 2: raise asyncio.CancelledError()
                if self.received == 3:
                    return json.dumps({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": "sub", "result": event(block(99))}})
                raise OSError("test disconnect")
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
        def connect(*args, **kwargs):
            nonlocal connections
            connections += 1
            return Socket(connections)
        with patch("app.heads.connect", side_effect=connect), patch("app.heads.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(asyncio.CancelledError): await feed.run()
        subscriptions = [s["params"] for s in sent if s["method"] == "eth_subscribe"]
        self.assertEqual(subscriptions, [["logs", {"address": [FACTORY], "topics": [EVENT_TOPICS]}]] * 2)
        counts = report(self.db.conn, 1)["counters"]
        self.assertEqual(counts["ws_reconnects"], 1)
        self.assertEqual(counts["ws_log_notifications"], 1)
        self.assertGreater(counts["ws_bytes"], 0)
        restarted = HeadFeed(self.config, telemetry)
        with patch("app.heads.connect", side_effect=connect):
            with self.assertRaises(asyncio.CancelledError): await restarted.run()
        self.assertEqual(report(self.db.conn, 1)["counters"]["ws_reconnects"], 2)

    def test_real_stock_paired_graduation(self):
        row = json.loads((Path(__file__).parent / "fixtures/graduations/gld.json").read_text())["events"][0]
        b = Block(int(row["blockNumber"], 16), row["blockHash"], "0x" + "00" * 32, row["block_timestamp"], 1)
        result = decode_graduation(row, b, (FACTORY,), *row["factory_state"])
        self.assertEqual(result["pool_id"], row["initialize_event"]["topics"][1])
        self.assertEqual(result["quote_asset_address"], row["launch"]["quote_asset_address"])
        self.assertEqual(result["curve_address"], row["launch"]["curve_address"])
        self.assertEqual(row["launch"]["is_stock_quote"], 1)
        self.assertEqual(row["launch"]["quote_asset_symbol"], "GLD")


if __name__ == "__main__":
    unittest.main()
