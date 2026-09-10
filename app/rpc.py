import asyncio
import itertools
import json
import logging
import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from app.models import Block, quantity


class RpcError(RuntimeError):
    pass


class ContractCallError(ValueError):
    pass


class RetryableRpcError(RpcError):
    def __init__(self, message, retry_after=0):
        super().__init__(message)
        self.retry_after = retry_after


class RetriesExhausted(RpcError):
    pass


class LogRangeError(RpcError):
    pass


def retry_delay(config, attempt, retry_after=0):
    ceiling = min(config.retry_max, config.retry_base * 2 ** (attempt - 1))
    return min(config.retry_max, max(retry_after, random.uniform(ceiling / 2, ceiling)))


def retry_after_seconds(value):
    try:
        return max(0, float(value))
    except (ValueError, TypeError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0


log = logging.getLogger(__name__)


class Rpc:
    # Deliberately no generic transaction, account, signing or wallet methods.
    ALLOWED = {"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber",
               "eth_getLogs", "eth_call", "eth_getCode"}

    def __init__(self, config, telemetry=None):
        self.config = config
        self.telemetry = telemetry
        self.client = httpx.AsyncClient(timeout=config.timeout, limits=httpx.Limits(max_connections=4),
                                        headers={"User-Agent": "meme-scanner/0.1 (read-only monitor)"})
        self.ids = itertools.count(1)
        self.limit = asyncio.Semaphore(4)
        self.pace_lock = asyncio.Lock()
        self.next_request = 0.0

    async def close(self):
        await self.client.aclose()

    async def request(self, payload, method):
        async with self.limit:
            async with self.pace_lock:
                now = asyncio.get_running_loop().time()
                await asyncio.sleep(max(0, self.next_request - now))
                self.next_request = asyncio.get_running_loop().time() + 1 / self.config.rpc_rps
            return await self._send(payload, method)

    async def _send(self, payload, method):
        if self.telemetry:
            self.telemetry.add("http_total")
            for item in payload if isinstance(payload, list) else [payload]:
                self.telemetry.add("method:" + item["method"])
        try:
            async with self.client.stream("POST", self.config.rpc_http, json=payload) as response:
                response.raise_for_status()
                body = bytearray()
                async for part in response.aiter_bytes():
                    body.extend(part)
                    if len(body) > 8 * 1024 * 1024:
                        raise RpcError(f"{method}: response exceeds 8 MiB limit")
            return json.loads(body)
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 429 and self.telemetry:
                self.telemetry.add("http_429")
            if status == 429 or status >= 500:
                raise RetryableRpcError(f"{method}: HTTP {status}",
                                        retry_after_seconds(exc.response.headers.get("Retry-After"))) from None
            if method == "eth_getLogs" and status in (400, 413):
                raise LogRangeError("Provider rejected log range") from None
            raise RpcError(f"{method}: HTTP {exc.response.status_code}") from None
        except httpx.TransportError as exc:
            raise RetryableRpcError(f"{method}: transport failure ({type(exc).__name__})") from None
        except (httpx.HTTPError, ValueError) as exc:
            # Provider error bodies and exception strings may contain API credentials.
            raise RpcError(f"{method}: transport/JSON failure ({type(exc).__name__})") from None

    @staticmethod
    def result(raw, request_id, method):
        if (isinstance(raw, dict) and raw.get("jsonrpc") == "2.0"
                and type(raw.get("id")) is int and raw["id"] == request_id
                and isinstance(raw.get("error"), dict)):
            error = raw["error"]
            message = str(error.get("message", "")).lower()
            if error.get("code") == 429 or "rate limit" in message or "throughput" in message:
                raise RetryableRpcError(f"{method}: provider rate limit")
            if method == "eth_getLogs" and (error.get("code") in (-32602, -32005)
                    or any(word in message for word in ("block range", "too many results", "response size", "query exceeds"))):
                raise LogRangeError("Provider rejected log range")
        if (isinstance(raw, dict) and raw.get("jsonrpc") == "2.0"
                and type(raw.get("id")) is int and raw["id"] == request_id
                and method == "eth_call" and isinstance(raw.get("error"), dict)):
            error = raw["error"]
            if error.get("code") == 3 or "revert" in str(error.get("message", "")).lower():
                raise ContractCallError("Contract call reverted")
        if (not isinstance(raw, dict) or raw.get("jsonrpc") != "2.0"
                or type(raw.get("id")) is not int or raw["id"] != request_id
                or "error" in raw or "result" not in raw or raw["result"] is None):
            raise RpcError(f"{method}: invalid or failed RPC response")
        return raw["result"]

    async def call(self, method, params):
        if method not in self.ALLOWED:
            raise ValueError("RPC method is outside read-only scope")
        request_id = next(self.ids)
        return await self.execute({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                                  method, lambda raw: self.result(raw, request_id, method))

    async def execute(self, payload, method, parse):
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                return parse(await self.request(payload, method))
            except RetryableRpcError as exc:
                if self.telemetry:
                    self.telemetry.add("failed_requests")
                    if "rate limit" in str(exc):
                        self.telemetry.add("rpc_rate_limits")
                if attempt == self.config.retry_attempts:
                    log.error("%s; attempts exhausted (%d); checkpoint held", exc, attempt)
                    raise RetriesExhausted("RPC retry budget exhausted; operator review required") from None
                delay = retry_delay(self.config, attempt, exc.retry_after)
                if self.telemetry:
                    self.telemetry.add("retries")
                log.warning("%s; attempt=%d/%d backoff=%.2fs", exc, attempt, self.config.retry_attempts, delay)
                await asyncio.sleep(delay)
            except (RpcError, ContractCallError):
                if self.telemetry:
                    self.telemetry.add("failed_requests")
                raise

    async def batch(self, calls):
        # Five envelopes/second x at most five members stays within 25 calls/second.
        if not 1 <= len(calls) <= 5 or any(method not in self.ALLOWED for method, _ in calls):
            raise ValueError("Invalid read-only batch")
        payload = [{"jsonrpc": "2.0", "id": next(self.ids), "method": method, "params": params}
                   for method, params in calls]
        def parse(raw):
            if not isinstance(raw, list) or len(raw) != len(payload):
                raise RpcError("Invalid batch response")
            results = {}
            for item in raw:
                if not isinstance(item, dict) or type(item.get("id")) is not int or item["id"] in results:
                    raise RpcError("Invalid batch response ID")
                results[item["id"]] = item
            decoded = []
            for item in payload:
                try:
                    decoded.append(self.result(results.get(item["id"]), item["id"], item["method"]))
                except ContractCallError as exc:
                    decoded.append(exc)
            return decoded
        return await self.execute(payload, "read-only batch", parse)

    async def check_chain(self):
        chain = quantity(await self.call("eth_chainId", []))
        if chain != self.config.chain_id:
            raise ValueError("RPC chain ID does not match configured network")
        return chain

    async def head(self):
        return quantity(await self.call("eth_blockNumber", []))

    async def block(self, number):
        block = Block.parse(await self.call("eth_getBlockByNumber", [hex(number), False]))
        if block.number != number:
            raise ValueError("RPC returned a different block number")
        return block

    async def blocks(self, numbers):
        result = []
        for offset in range(0, len(numbers), 5):
            selected = numbers[offset:offset + 5]
            raw = await self.batch([("eth_getBlockByNumber", [hex(n), False]) for n in selected])
            parsed = [Block.parse(item) for item in raw]
            if [b.number for b in parsed] != selected:
                raise ValueError("RPC returned different block numbers in batch")
            result.extend(parsed)
        return result
