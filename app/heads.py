"""Filtered logs or heads wake the watcher; HTTP ranges provide gap-free recovery."""
import asyncio
import json
import logging
import time

from websockets.asyncio.client import connect
from eth_utils import to_checksum_address

from app.models import hash32, quantity
from app.rpc import Rpc, RpcError, retry_delay
from app.pons import EVENT_TOPICS

log = logging.getLogger(__name__)


class HeadFeed:
    def __init__(self, config, telemetry=None):
        self.config = config
        self.telemetry = telemetry
        self.number = None
        self.received_at = 0.0
        self.count = 0
        self.bytes_received = 0
        self.connections = 0
        self.connected = False
        self.task = None
        self.changed = asyncio.Event()

    def latest(self):
        return self.number if (self.config.ws_subscription == "newHeads" and self.connected
                               and time.monotonic() - self.received_at < 60) else None

    async def run(self):
        failures = 0
        while failures < self.config.retry_attempts:
            try:
                async with connect(self.config.rpc_ws, open_timeout=self.config.timeout,
                                   ping_interval=20, ping_timeout=20, max_size=65536,
                                   max_queue=4, compression=None) as socket:
                    await socket.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}))
                    chain = Rpc.result(json.loads(await asyncio.wait_for(socket.recv(), self.config.timeout)), 1, "eth_chainId")
                    if quantity(chain) != self.config.chain_id:
                        raise RpcError("WebSocket returned the wrong chain")
                    params = (["logs", {"address": list(self.config.factories), "topics": [EVENT_TOPICS]}]
                              if self.config.ws_subscription == "logs" else ["newHeads"])
                    await socket.send(json.dumps({"jsonrpc": "2.0", "id": 2,
                                                  "method": "eth_subscribe", "params": params}))
                    subscription = Rpc.result(json.loads(await asyncio.wait_for(socket.recv(), self.config.timeout)), 2, "eth_subscribe")
                    if not isinstance(subscription, str) or not subscription:
                        raise RpcError("Invalid subscription ID")
                    self.connections += 1
                    if self.telemetry:
                        if self.telemetry.db.state("websocket_connected_once") == "1":
                            self.telemetry.add("ws_reconnects")
                        with self.telemetry.db.conn:
                            self.telemetry.db.set_state("websocket_connected_once", 1)
                        self.telemetry.status("connected")
                    started = time.monotonic()
                    self.connected = True
                    self.changed.set()
                    log.info("WebSocket %s subscribed chain=%d", self.config.ws_subscription, self.config.chain_id)
                    while True:
                        raw = (await asyncio.wait_for(socket.recv(), 60) if self.config.ws_subscription == "newHeads"
                               else await socket.recv())
                        self.bytes_received += len(raw.encode() if isinstance(raw, str) else raw)
                        if self.telemetry:
                            self.telemetry.add("ws_bytes", len(raw.encode() if isinstance(raw, str) else raw))
                        message = json.loads(raw)
                        params = message.get("params", {})
                        if (message.get("jsonrpc") != "2.0" or message.get("method") != "eth_subscription"
                                or params.get("subscription") != subscription):
                            raise RpcError("Invalid head notification")
                        head = params["result"]
                        if self.config.ws_subscription == "logs":
                            if (to_checksum_address(head["address"]) not in self.config.factories
                                    or head["topics"][0].lower() not in EVENT_TOPICS):
                                raise RpcError("Unexpected log subscription result")
                            hash32(head["blockHash"])
                            self.number = quantity(head["blockNumber"])
                            if self.telemetry:
                                self.telemetry.add("ws_log_notifications")
                        else:
                            hash32(head["hash"])
                            hash32(head["parentHash"])
                            self.number = quantity(head["number"])
                        self.received_at = time.monotonic()
                        self.count += 1
                        self.connected = True
                        self.changed.set()  # Coalesces heads; never grows a block queue.
                        if self.received_at - started >= 60:
                            failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.changed.set()
                failures += 1
                if failures >= self.config.retry_attempts:
                    log.error("WebSocket retries exhausted (%d); primary HTTP polling remains active", failures)
                    return
                delay = retry_delay(self.config, failures)
                log.warning("WebSocket disconnected error=%s attempt=%d/%d backoff=%.2fs",
                            type(exc).__name__, failures, self.config.retry_attempts, delay)
                await asyncio.sleep(delay)
            finally:
                self.connected = False
                if self.telemetry:
                    self.telemetry.status("disconnected")
