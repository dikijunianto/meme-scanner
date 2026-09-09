"""Run: python -m unittest discover -s tests -v (no test framework dependency)."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

import httpx
from eth_abi import encode

from app.block_watcher import DeepReorg, Watcher
from app.config import Config
from app.database import Database
from app.models import Block, utc_now
from app.pons import Pons, TOPIC, VERIFIED_FACTORIES, ZERO, decode_launch, metadata, metadata_text
from app.rpc import ContractCallError, LogRangeError, RetriesExhausted, Rpc, RpcError
from app.stock_assets import parse_assets

FACTORY = next(iter(VERIFIED_FACTORIES))
TOKEN, CURVE, CREATOR, QUOTE = ["0x" + f"{n:040x}" for n in range(1, 5)]


def block(number, fork=0):
    return Block(number, "0x" + f"{number + 1000 * fork:064x}",
                 "0x" + f"{number - 1 + 1000 * fork:064x}", "2026-09-09T00:00:00+00:00", 1)


def event(b):
    return {"address": FACTORY, "topics": [TOPIC] + ["0x" + encode(["address"], [a]).hex()
            for a in (TOKEN, CURVE, CREATOR)], "data": "0x" + encode(
                ["address", "uint256", "uint256"], [QUOTE, 1, 10**18]).hex(),
            "blockHash": b.hash, "blockNumber": hex(b.number),
            "transactionHash": "0x" + "aa" * 32, "logIndex": "0x0", "removed": False}


class ScannerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.config = Config("https://example.invalid/secret", 4663, (FACTORY,),
                             "https://api.robinhood.com/rhj/assets", root / "scanner.db", root / "scanner.log",
                             confirmations=0, start_block=10, overlap=2, retention=20, reorg_depth=5)
        self.db = Database(self.config.database, 4663)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def launch(self, b):
        launch = decode_launch(event(b), b, (FACTORY,))
        launch.update(token_symbol="NVDA", token_name=None, quote_asset_symbol="NVDA",
                      quote_asset_name=None, is_stock_quote=0)
        return launch

    def test_decode_and_metadata(self):
        launch = self.launch(block(10))
        self.assertEqual((launch["token_address"], launch["quote_asset_address"], launch["curve_address"]),
                         (TOKEN, QUOTE, CURVE))
        self.assertIsNone(launch["pair_or_pool_address"])
        self.assertEqual(metadata_text("0x" + encode(["string"], ["ABC"]).hex()), "ABC")
        self.assertEqual(metadata_text("0x" + b"ABC".ljust(32, b"\0").hex()), "ABC")
        self.assertRaises(Exception, metadata_text, "0x1234")
        bad = event(block(10)); bad["blockHash"] = block(11).hash
        self.assertRaises(ValueError, decode_launch, bad, block(10), (FACTORY,))

    def test_historical_rpc_fixtures(self):
        for path in (Path(__file__).parent / "fixtures").glob("*.json"):
            item = json.loads(path.read_text(encoding="utf-8"))
            expected, raw = item["decoded"], item["raw_event"]
            b = Block(expected["block_number"], raw["blockHash"], "0x" + "00" * 32,
                      expected["block_timestamp"], 1)
            actual = decode_launch(raw, b, (FACTORY,))
            for key in ("token_address", "quote_asset_address", "creator_address", "curve_address", "tx_hash", "log_index"):
                self.assertEqual(actual[key], expected[key])
        bad = event(block(10)); bad["removed"] = True
        self.assertRaises(ValueError, decode_launch, bad, block(10), (FACTORY,))

    async def test_broken_metadata_does_not_crash(self):
        rpc = AsyncMock()
        rpc.batch.return_value = [ContractCallError("revert"), "0x1234"]
        self.assertEqual(await metadata(rpc, TOKEN, 10), {"name": None, "symbol": None})
        self.assertEqual((await metadata(rpc, ZERO, 10))["symbol"], "ETH")
        rpc.batch.side_effect = RpcError("HTTP 429")
        with self.assertRaises(RpcError):
            await metadata(rpc, TOKEN, 10)

    def test_atomic_idempotence_wal_restart_and_registry(self):
        launch = self.launch(block(10))
        self.db.save_block(block(10), [launch, launch], 20)
        self.db.save_block(block(10), [launch], 20)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM launches").fetchone()[0], 1)
        self.assertEqual(self.db.conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertFalse(self.db.is_stock(QUOTE))
        rows = parse_assets({"assets": [{"tokenSymbol": "NVDA", "tokenName": "Nvidia",
                   "deployments": [{"chainId": 4663, "contractAddress": QUOTE},
                                   {"chainId": 46630, "contractAddress": TOKEN}]}]}, 4663, self.config.stock_url)
        self.db.replace_assets(rows, self.config.stock_url)
        self.assertTrue(self.db.is_stock(QUOTE))
        self.assertFalse(self.db.is_stock(TOKEN))  # Same ticker does not qualify.
        self.assertEqual(self.db.conn.execute("SELECT is_stock_quote FROM launches").fetchone()[0], 1)
        malformed = self.launch(block(11)); malformed["is_stock_quote"] = 2
        self.assertRaises(sqlite3.IntegrityError, self.db.save_block, block(11), [malformed], 20)
        self.assertEqual(self.db.last(), 10)
        self.assertIsNone(self.db.block_hash(11))
        self.db.close()
        self.db = Database(self.config.database, 4663)
        self.assertEqual(self.db.last(), 10)
        self.assertRaises(ValueError, Database, self.config.database, 46630)
        self.assertRaises(ValueError, parse_assets, {"assets": []}, 4663, self.config.stock_url)

    async def test_watcher_overlap_reorg_orphan_cleanup_and_deep_stop(self):
        rpc = AsyncMock()
        rpc.head.return_value = 12
        rpc.block.side_effect = lambda n: block(n)
        watcher = Watcher(self.config, rpc, self.db)
        watcher.pons.logs = AsyncMock(return_value=[event(block(11))])
        rpc.batch.return_value = ["0x" + encode(["string"], ["TEST"]).hex()] * 2
        await watcher.step()
        self.assertEqual(self.db.last(), 12)
        metadata_calls = rpc.batch.await_count
        restarted = Watcher(self.config, rpc, self.db)
        restarted.pons.logs = AsyncMock(return_value=[event(block(11))])
        await restarted.step()
        self.assertEqual(rpc.batch.await_count, metadata_calls)
        restarted.pons.logs.assert_awaited_with(11, 12)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM launches").fetchone()[0], 1)
        rpc.block.side_effect = lambda n: block(n, 1) if n > 10 else block(n)
        await restarted.reconcile(12)
        self.assertEqual(self.db.last(), 10)
        self.assertEqual(self.db.conn.execute("SELECT count(*) FROM launches").fetchone()[0], 0)
        rpc.block.side_effect = lambda n: block(n, 2)
        with self.assertRaises(DeepReorg):
            await restarted.reconcile(12)
        self.assertEqual(self.db.last(), 10)

    async def test_rpc_validation_secrets_and_read_only(self):
        rpc = Rpc(self.config)
        await rpc.client.aclose()
        def response(request):
            payload = json.loads(request.content)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": "0x1237"})
        rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        try:
            self.assertEqual(await rpc.check_chain(), 4663)
            with self.assertRaises(ValueError):
                await rpc.call("eth_sendRawTransaction", [])
            rpc.config = replace(self.config, chain_id=1)
            with self.assertRaises(ValueError):
                await rpc.check_chain()
        finally:
            await rpc.close()
        rpc = Rpc(self.config)
        await rpc.client.aclose()
        rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(403, text="secret")))
        try:
            with self.assertRaises(RpcError) as raised:
                await rpc.head()
            self.assertNotIn("secret", str(raised.exception))
        finally:
            await rpc.close()

    async def test_all_logs_scanned_with_sparse_headers(self):
        rpc = AsyncMock()
        rpc.head.return_value = 20
        rpc.block.side_effect = lambda n: block(n)
        rpc.batch.return_value = ["0x" + encode(["string"], ["TEST"]).hex()] * 2
        watcher = Watcher(self.config, rpc, self.db)
        watcher.pons.logs = AsyncMock(return_value=[event(block(15))])
        self.assertEqual(await watcher.step(), 11)
        watcher.pons.logs.assert_awaited_once_with(10, 20)
        self.assertEqual(self.db.last(), 20)
        self.assertEqual({row[0] for row in self.db.conn.execute("SELECT block_number FROM blocks")}, {10, 15, 20})
        self.assertEqual(self.db.conn.execute("SELECT block_number FROM launches").fetchone()[0], 15)

    async def test_retry_recovers_with_bounded_backoff(self):
        rpc = AsyncMock()
        watcher = Watcher(self.config, rpc, self.db)
        watcher.pons.verify = AsyncMock()
        watcher.step = AsyncMock(side_effect=[RpcError("offline"), RpcError("offline"), 1])
        watcher.last_sync = __import__("time").monotonic()
        with self.db.conn:
            self.db.set_state("stock_assets_synced_at", utc_now())
        with patch("app.block_watcher.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await watcher.run(max_batches=1)
            delays = [call.args[0] for call in sleep.await_args_list]
            self.assertTrue(1 <= delays[0] <= 2 and 2 <= delays[1] <= 4)
        self.assertEqual(rpc.check_chain.await_count, 3)

    async def test_invalid_rpc_response_ids(self):
        rpc = Rpc(self.config)
        await rpc.client.aclose()
        rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(
            200, json={"jsonrpc": "2.0", "id": 999, "result": "0x1237"})))
        try:
            with self.assertRaises(RpcError):
                await rpc.check_chain()
        finally:
            await rpc.close()

    async def test_429_has_bounded_jitter_retries(self):
        rpc = Rpc(replace(self.config, retry_attempts=3, retry_max=4))
        await rpc.client.aclose()
        count = 0
        def response(request):
            nonlocal count
            count += 1
            return httpx.Response(429, headers={"Retry-After": "2"})
        rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        try:
            with patch("app.rpc.asyncio.sleep", new_callable=AsyncMock) as sleep:
                with self.assertRaises(RetriesExhausted):
                    await rpc.head()
                self.assertEqual(count, 3)
                backoffs = [x.args[0] for x in sleep.await_args_list if x.args[0] >= 2]
                self.assertEqual(len(backoffs), 2)
                self.assertTrue(all(2 <= d <= 4 for d in backoffs))
        finally:
            await rpc.close()

    async def test_log_ranges_shrink_without_gaps(self):
        rpc = AsyncMock()
        accepted = []
        async def call(method, params):
            first, last = int(params[0]["fromBlock"], 16), int(params[0]["toBlock"], 16)
            if last - first + 1 > 3:
                raise LogRangeError("range rejected")
            accepted.extend(range(first, last + 1))
            return []
        rpc.call.side_effect = call
        with patch("app.pons.asyncio.sleep", new_callable=AsyncMock):
            self.assertEqual(await Pons(rpc, self.config, self.db).logs(10, 20), [])
        self.assertEqual(accepted, list(range(10, 21)))

    async def test_live_subscription_waits_for_catchup(self):
        rpc = AsyncMock()
        rpc.head.return_value = 100
        rpc.block.side_effect = lambda n: block(n)
        feed = SimpleNamespace(task=None, latest=lambda: None, run=AsyncMock())
        watcher = Watcher(self.config, rpc, self.db, feed)
        watcher.pons.logs = AsyncMock(return_value=[])
        await watcher.step()
        self.assertIsNone(feed.task)
        await watcher.step()
        self.assertIsNotNone(feed.task)
        await feed.task


if __name__ == "__main__":
    unittest.main()
