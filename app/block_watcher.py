import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import logging
import sqlite3
import time

from app.models import Block, quantity, utc_now
from app.pons import Pons
from app.rpc import RetriesExhausted, RpcError, retry_delay
from app.stock_assets import sync_assets

log = logging.getLogger(__name__)


class DeepReorg(RuntimeError):
    pass


class Watcher:
    def __init__(self, config, rpc, db, feed=None, *, cursor="last_processed_block", telemetry=None):
        self.config, self.rpc, self.db = config, rpc, db
        self.cursor, self.telemetry = cursor, telemetry
        self.pons = Pons(rpc, config, db, telemetry)
        self.pons.include_graduations = cursor != "last_processed_block"
        self.next_block = None
        self.last_sync = float("-inf")
        self.feed = feed
        self.feed_connections = 0
        self.recovery_end = None

    async def reconcile(self, target):
        last = self.db.last(self.cursor)
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
                if self.cursor == "live_checkpoint" and number < int(self.db.state("live_start_block")):
                    raise DeepReorg("Reorg predates live coverage; operator review required")
                self.db.rollback_to(number, self.cursor)
                self.next_block = number + 1
                log.warning("Reorg rolled back to ancestor=%d", number)
                return
        raise DeepReorg("No common ancestor within retained reorg window; state untouched; operator review required")

    async def step(self):
        head = self.feed.latest() if self.feed else None
        if head is None:
            head = await self.rpc.head()
        target = max(0, head - self.config.confirmations)
        if self.cursor == "live_checkpoint":
            with self.db.conn:
                self.db.set_state("observed_head", head)
            last_live = self.db.last(self.cursor)
            start_live = self.db.last("live_start_block")
            if start_live is None:
                start_live = max(0, head - self.config.live_overlap)
                with self.db.conn:
                    self.db.set_state("live_start_block", start_live)
                log.info("Live mode begins at %d; historical checkpoint=%s remains paused",
                         start_live, self.db.state("historical_checkpoint"))
            if self.next_block is None:
                self.next_block = max(start_live, last_live - self.config.live_overlap if last_live is not None else start_live)
                self.recovery_end = target if last_live is not None else None
            connections = self.feed.connections if self.feed else 0
            if connections > self.feed_connections:
                if self.feed_connections and last_live is not None:
                    self.next_block = max(start_live, last_live - self.config.live_overlap)
                    self.recovery_end = target
                self.feed_connections = connections
            if target - self.next_block + 1 > self.config.recovery_max:
                raise DeepReorg("Live recovery exceeds configured bound; checkpoint held; explicit backfill required")
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
        return await self.process_range(first, last, head)

    async def process_range(self, first, last, head, source="manual_backfill"):
        anchor = await self.rpc.block(last)
        events = (await self.pons.logs(first, last) if source == "manual_backfill"
                  else await self.pons.logs(first, last, source))
        grouped = defaultdict(list)
        for event in events:
            number = quantity(event["blockNumber"])
            if not first <= number <= last:
                raise ValueError("RPC returned a log outside requested range")
            grouped[number].append(event)
        # Scan every block's logs, but fetch headers only for launches and range
        # boundaries. This avoids full header indexing on the rate-limited RPC.
        needed = sorted({first, last, *grouped})
        cached = {number: self.db.block(number) for number in needed}
        fetched = {}
        if self.cursor != "last_processed_block":
            missing = [n for n in needed if n != last and cached[n] is None]
            fetched = {b.number: b for b in await self.rpc.blocks(missing)} if missing else {}
        blocks = []
        for number in needed:
            # Stored headers are reusable after reconcile verified their canonical
            # checkpoint. Only checkpoint and range-end hash checks need fresh reads.
            stored = cached[number]
            if self.cursor is None and stored and (await self.rpc.block(number)).hash != stored.hash:
                # Manual work must never roll back a newer live range.
                raise DeepReorg("Backfill contradicts persisted history; operator review required")
            blocks.append(anchor if number == last else stored or fetched.get(number) or await self.rpc.block(number))
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
        graduations = ([await self.pons.graduations(grouped[b.number], b) for b in blocks]
                       if self.pons.include_graduations else [[] for _ in blocks])
        if (await self.rpc.block(last)).hash != anchor.hash:
            raise RpcError("Batch changed during enrichment")
        if self.cursor == "last_processed_block":
            for block, launches in zip(blocks, enriched):
                self.db.save_block(block, launches, self.config.retention)
        else:
            launches, stock = self.db.save_range(blocks, enriched, graduations, first, last,
                                                  self.cursor, self.config.retention)
            if self.telemetry:
                self.telemetry.add("launches_received", launches)
                self.telemetry.add("stock_paired_launches", stock)
                if self.recovery_end is not None:
                    self.telemetry.add("blocks_recovered", max(0, min(last, self.recovery_end) - first + 1))
            if self.recovery_end is not None and last >= self.recovery_end:
                self.recovery_end = None
        self.next_block = last + 1
        log.info("Processed blocks=%d..%d latest=%d events=%d", first, last, head, len(events))
        return last - first + 1

    async def process_ws_event(self, event):
        number = quantity(event["blockNumber"])
        # The subscription carries the immutable event block hash. Contract calls
        # are pinned to its number; headers remain reserved for bounded recovery.
        block = Block(number, event["blockHash"].lower(), "0x" + "00" * 32, utc_now(), 0)
        launches = await self.pons.launches([event], block)
        graduations = await self.pons.graduations([event], block)
        previous = self.db.last("live_checkpoint")
        segment = self.db.state("live_segment_start")
        coverage_first = int(segment) if segment else block.number
        if previous is not None and block.number <= previous:
            coverage_first = block.number
        new_launches, stock = self.db.save_range([None], [launches], [graduations], block.number,
                                                 block.number, "live_checkpoint", self.config.retention,
                                                 coverage_first=coverage_first)
        with self.db.conn:
            self.db.set_state("live_segment_start", block.number + 1)
            self.db.set_state("live_degraded", 0)
        if self.telemetry:
            self.telemetry.add("launches_received", new_launches)
            self.telemetry.add("stock_paired_launches", stock)
        log.info("WebSocket event block=%d launches=%d graduations=%d", block.number,
                 len(launches), len(graduations))

    def record_gap(self, kind, first, last):
        if first > last:
            return
        self.db.record_gap(kind, first, last)
        with self.db.conn:
            self.db.set_state("live_degraded", 1)
            self.db.set_state("live_segment_start", "")
        if self.telemetry:
            self.telemetry.add("intentional_gap_count")
            self.telemetry.add("intentional_gap_blocks", last - first + 1)
        log.warning("Recorded %s blocks=%d..%d; no automatic backfill", kind, first, last)

    async def ws_recover(self, limit, source, gap_kind):
        head = await self.rpc.head()
        target = max(0, head - self.config.confirmations)
        with self.db.conn:
            self.db.set_state("observed_head", head)
        checkpoint = self.db.last("live_checkpoint")
        start = self.db.last("live_start_block")
        if start is None:
            start = max(0, target - self.config.live_overlap)
            with self.db.conn:
                self.db.set_state("live_start_block", start)
        first = max(start, checkpoint - self.config.live_overlap if checkpoint is not None else start)
        if target < first:
            return
        if target - first + 1 > limit:
            # Preserve the tail overlap, but make the old outage visible as a gap.
            skipped_last = max(first - 1, target - self.config.live_overlap)
            self.record_gap(gap_kind, first, skipped_last)
            first = max(start, target - self.config.live_overlap)
        await self.process_range(first, target, head, source)
        with self.db.conn:
            self.db.set_state("live_segment_start", target + 1)

    async def run_ws_first(self, max_batches=None):
        if not self.feed:
            raise ValueError("WS-first mode requires a filtered WebSocket endpoint")
        await self.rpc.check_chain()
        await self.pons.verify()
        stamp = self.db.state("stock_assets_synced_at")
        if stamp is None or (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds() > 172800:
            await sync_assets(self.config, self.db)
        self.feed.task = asyncio.create_task(self.feed.run())
        # Subscription starts before recovery; queued overlap logs are idempotent.
        for _ in range(int(self.config.timeout)):
            if self.feed.connected:
                break
            await asyncio.sleep(1)
        if not self.feed.connected:
            raise RpcError("Filtered WebSocket did not connect")
        self.feed_connections = self.feed.connections
        await self.ws_recover(self.config.startup_recovery_max, "startup_recovery", "startup-gap")
        batches = 0
        while max_batches is None or batches < max_batches:
            try:
                if self.feed.task.done():
                    # No HTTP completeness loop while the provider is unavailable.
                    self.feed.task = asyncio.create_task(self.feed.run())
                if self.feed.connections > self.feed_connections:
                    self.feed_connections = self.feed.connections
                    await self.ws_recover(self.config.recovery_max, "reconnect_recovery", "ws-gap")
                if self.feed.overflowed:
                    self.feed.overflowed = False
                    await self.ws_recover(self.config.recovery_max, "reconnect_recovery", "ws-queue-gap")
                try:
                    event = await asyncio.wait_for(self.feed.events.get(), self.config.head_healthcheck)
                except TimeoutError:
                    head = await self.rpc.head()
                    with self.db.conn:
                        self.db.set_state("observed_head", head)
                    continue
                if event.get("removed"):
                    # Removed logs are a reorg signal; bounded recovery verifies the recent event window.
                    await self.ws_recover(self.config.recovery_max, "reconnect_recovery", "ws-reorg-gap")
                    continue
                await self.process_ws_event(event)
                batches += 1
            except (RpcError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                log.warning("WS-first processing failed error=%s; event held", type(exc).__name__)
                await asyncio.sleep(retry_delay(self.config, 1))

    async def run(self, max_batches=None):
        if self.config.transport_mode == "ws_first" and self.cursor == "live_checkpoint":
            return await self.run_ws_first(max_batches)
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
