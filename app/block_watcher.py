import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import logging
import sqlite3
import time

from app.models import quantity
from app.pons import Pons
from app.rpc import RetriesExhausted, RpcError, retry_delay
from app.stock_assets import sync_assets

log = logging.getLogger(__name__)


class DeepReorg(RuntimeError):
    pass


class Watcher:
    def __init__(self, config, rpc, db, feed=None):
        self.config, self.rpc, self.db = config, rpc, db
        self.pons = Pons(rpc, config, db)
        self.next_block = None
        self.last_sync = float("-inf")
        self.feed = feed

    async def reconcile(self, target):
        last = self.db.last()
        if last is None:
            return
        # A lagging RPC must not erase canonical state.
        if target < last:
            raise RpcError("RPC head is behind the stored checkpoint")
        actual = await self.rpc.block(last)
        if self.db.block_hash(last) == actual.hash:
            return
        log.warning("Reorg at checkpoint=%d; searching retained ancestors", last)
        ancestors = self.db.conn.execute(
            "SELECT block_number,block_hash FROM blocks WHERE block_number<? AND block_number>=? "
            "ORDER BY block_number DESC", (last, max(0, last - self.config.reorg_depth))).fetchall()
        for number, stored in ancestors:
            if (await self.rpc.block(number)).hash == stored:
                self.db.rollback_to(number)
                self.next_block = number + 1
                log.warning("Reorg rolled back to ancestor=%d", number)
                return
        raise DeepReorg("No common ancestor within retained reorg window; state untouched; operator review required")

    async def step(self):
        head = self.feed.latest() if self.feed else None
        if head is None:
            head = await self.rpc.head()
        target = max(0, head - self.config.confirmations)
        await self.reconcile(head)
        if self.next_block is None:
            last = self.db.last()
            if last is None:
                start = self.config.start_block if self.config.start_block is not None else max(0, target - self.config.overlap + 1)
                with self.db.conn:
                    self.db.set_state("scan_start_block", start)
            else:
                start = max(int(self.db.state("scan_start_block") or 0), last - self.config.overlap + 1)
            self.next_block = start
        first = self.next_block
        if self.feed and self.feed.task is None and target - first <= self.config.batch:
            self.feed.task = asyncio.create_task(self.feed.run())
        if first > target:
            log.debug("Latest block=%d checkpoint=%s", head, self.db.last())
            return 0
        last = min(target, first + self.config.batch - 1)
        anchor = await self.rpc.block(last)
        events = await self.pons.logs(first, last)
        grouped = defaultdict(list)
        for event in events:
            number = quantity(event["blockNumber"])
            if not first <= number <= last:
                raise ValueError("RPC returned a log outside requested range")
            grouped[number].append(event)
        # Scan every block's logs, but fetch headers only for launches and range
        # boundaries. This avoids full header indexing on the rate-limited RPC.
        needed = sorted({first, last, *grouped})
        blocks = []
        for number in needed:
            # Stored headers are reusable after reconcile verified their canonical
            # checkpoint. Only checkpoint and range-end hash checks need fresh reads.
            stored = self.db.block(number)
            blocks.append(anchor if number == last else stored or await self.rpc.block(number))
        if blocks[-1].hash != anchor.hash:
            raise RpcError("Batch changed during log read")
        # Validate the complete batch before committing any part of it.
        previous_number, parent = first - 1, self.db.block_hash(first - 1)
        for block in blocks:
            if block.number == previous_number + 1 and parent is not None and block.parent != parent:
                log.warning("Reorg/parent mismatch at block=%d; batch held", block.number)
                raise RpcError("Parent hash mismatch; retrying canonical batch")
            parent = block.hash
            previous_number = block.number
        enriched = await asyncio.gather(*(self.pons.launches(grouped[b.number], b) for b in blocks),
                                        return_exceptions=True)
        for result in enriched:
            if isinstance(result, BaseException):
                raise result
        if (await self.rpc.block(last)).hash != anchor.hash:
            raise RpcError("Batch changed during enrichment")
        for block, launches in zip(blocks, enriched):
            self.db.save_block(block, launches, self.config.retention)
            self.next_block = block.number + 1
        log.info("Processed blocks=%d..%d latest=%d launches=%d", first, last, head, len(events))
        return last - first + 1

    async def run(self, max_batches=None):
        failures, connected, batches = 0, False, 0
        while True:
            try:
                if not connected:
                    await self.rpc.check_chain()
                    await self.pons.verify()
                    log.info("RPC connected chain=%d; Pons deployment present", self.config.chain_id)
                    connected = True
                stamp = self.db.state("stock_assets_synced_at")
                registry_expired = stamp is None or (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds() > 172800
                if time.monotonic() - self.last_sync > 86400 or registry_expired:
                    try:
                        count = await sync_assets(self.config, self.db)
                        log.info("Verified stock registry synced assets=%d", count)
                    except ValueError:
                        stamp = self.db.state("stock_assets_synced_at")
                        if stamp is None or (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds() > 172800:
                            raise
                        log.warning("Registry unavailable; using verified snapshot under 48 hours old")
                    self.last_sync = time.monotonic()
                processed = await self.step()
                failures = 0
                batches += 1
                if max_batches is not None and batches >= max_batches:
                    return
                # A full batch may mean backlog; drain it before the idle poll delay.
                if self.feed and processed < self.config.batch:
                    self.feed.changed.clear()
                    try:
                        await asyncio.wait_for(self.feed.changed.wait(), self.config.poll)
                    except TimeoutError:
                        pass
                    await asyncio.sleep(min(1, self.config.poll))
                else:
                    await asyncio.sleep(1 if processed == self.config.batch else self.config.poll)
            except DeepReorg:
                log.critical("Deep reorg: stopped for operator review")
                raise
            except RetriesExhausted:
                raise
            except (RpcError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                connected = False
                failures += 1
                if failures >= self.config.retry_attempts:
                    raise RetriesExhausted("Processing retry budget exhausted; checkpoint retained") from None
                delay = retry_delay(self.config, failures)
                log.warning("RPC disconnected or processing failed error=%s; retry in %.1fs",
                            type(exc).__name__, delay)
                if isinstance(exc, RpcError):
                    log.warning("RPC detail: %s", exc)
                if isinstance(exc, sqlite3.Error):
                    log.error("Database error; block checkpoint not advanced")
                await asyncio.sleep(delay)
