"""Read-only milestone market snapshots for verified Phase 1.6 launches."""
import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext

from eth_abi import decode, encode
from eth_utils import keccak

from app.models import utc_now
from app.pons import ZERO, hex_data
from app.rpc import RpcError

Q192 = 1 << 192


def decimal_text(raw, decimals):
    return str(Decimal(raw) / (Decimal(10) ** decimals))


def curve_price(quote_reserve, token_reserve, quote_decimals):
    if not quote_reserve or not token_reserve:
        return None
    with localcontext() as ctx:
        ctx.prec = 78
        return str((Decimal(quote_reserve) / Decimal(10) ** quote_decimals) /
                   (Decimal(token_reserve) / Decimal(10) ** 18))


def v4_price(sqrt_price_x96, token_is_currency0, token_decimals, quote_decimals):
    if not sqrt_price_x96:
        return None
    with localcontext() as ctx:
        ctx.prec = 78
        raw = Decimal(sqrt_price_x96) ** 2 / Decimal(Q192)  # raw currency1 per raw currency0
        if token_is_currency0:
            return str(raw * Decimal(10) ** token_decimals / Decimal(10) ** quote_decimals)
        if not raw:
            return None
        return str((Decimal(1) / raw) * Decimal(10) ** token_decimals / Decimal(10) ** quote_decimals)


def selector(signature):
    return "0x" + keccak(text=signature).hex()[:8]


class MarketResolver:
    def __init__(self, config, rpc, db, telemetry):
        self.config, self.rpc, self.db, self.telemetry = config, rpc, db, telemetry

    async def call_batch(self, calls, category):
        self.telemetry.add("market_rpc_calls", len(calls))
        return await self.rpc.batch(calls)

    async def quote_decimals(self, quote):
        if quote == ZERO:
            return 18
        key = "quote_decimals:" + quote.lower()
        cached = self.db.market_static(key)
        if cached is not None:
            return int(cached)
        raw = (await self.call_batch([("eth_call", [{"to": quote, "data": selector("decimals()")}, "latest"])], "static"))[0]
        decimals = int.from_bytes(hex_data(raw), "big")
        if not 0 <= decimals <= 36:
            raise ValueError("quote decimals unavailable")
        self.db.set_market_static(key, decimals)
        self.telemetry.add("market_static_cache_fill")
        return decimals

    async def supply(self, token):
        key = "supply:" + token.lower()
        cached = self.db.market_static(key)
        if cached is not None:
            return int(cached)
        raw = (await self.call_batch([("eth_call", [{"to": token, "data": selector("totalSupply()")}, "latest"])], "static"))[0]
        supply = int.from_bytes(hex_data(raw), "big")
        if supply <= 0:
            raise ValueError("zero supply")
        self.db.set_market_static(key, supply)
        self.telemetry.add("market_static_cache_fill")
        return supply

    async def snapshot(self, target):
        graduation = self.db.conn.execute("SELECT * FROM graduations WHERE token_address=? ORDER BY block_number DESC LIMIT 1",
                                          (target["token_address"],)).fetchone()
        if graduation:
            return await self.v4(target, dict(graduation))
        return await self.curve(target)

    async def curve(self, target):
        curve, token, quote = target["curve_address"], target["token_address"], target["quote_asset_address"]
        calls = [("eth_call", [{"to": curve, "data": selector("getReserves()")}, "latest"]),
                 ("eth_call", [{"to": curve, "data": selector("realQuoteReserve()")}, "latest"])]
        reserves, real = await self.call_batch(calls, "curve")
        quote_reserve, token_reserve = decode(["uint256", "uint256"], hex_data(reserves))
        real_quote = int.from_bytes(hex_data(real), "big")
        decimals, supply = await asyncio.gather(self.quote_decimals(quote), self.supply(token))
        price = curve_price(quote_reserve, token_reserve, decimals)
        fdv = str(Decimal(price) * Decimal(supply) / Decimal(10) ** 18) if price else None
        return dict(market_phase="curve", price_quote=price, fdv_quote=fdv, token_supply=str(supply),
                    quote_reserve=str(quote_reserve), token_reserve=str(token_reserve), curve_address=curve,
                    pool_id=None, v4_active_liquidity=None, liquidity_metric_type="curve_real_quote_reserve",
                    liquidity_quote_estimate=decimal_text(real_quote, decimals), data_quality="verified" if price else "unavailable",
                    source_method="curve.getReserves+realQuoteReserve", error_code=None)

    async def v4(self, target, grad):
        # Uniswap v4 StateLibrary: stateSlot=keccak256(poolId || bytes32(6)); slot0 at stateSlot, liquidity at +3.
        pool_id = grad["pool_id"]
        state_slot = "0x" + keccak(encode(["bytes32", "bytes32"], [hex_data(pool_id), (6).to_bytes(32, "big")])).hex()
        liquidity_slot = "0x" + (int(state_slot, 16) + 3).to_bytes(32, "big").hex()
        calls = [("eth_call", [{"to": grad["pool_manager_address"], "data": selector("extsload(bytes32)") + state_slot[2:]}, "latest"]),
                 ("eth_call", [{"to": grad["pool_manager_address"], "data": selector("extsload(bytes32)") + liquidity_slot[2:]}, "latest"])]
        slot0, liquidity = await self.call_batch(calls, "v4")
        packed = int.from_bytes(hex_data(slot0), "big")
        sqrt = packed & ((1 << 160) - 1)
        active = int.from_bytes(hex_data(liquidity), "big") & ((1 << 128) - 1)
        decimals, supply = await asyncio.gather(self.quote_decimals(target["quote_asset_address"]), self.supply(target["token_address"]))
        price = v4_price(sqrt, target["token_address"].lower() == grad["currency0"].lower(), 18, decimals)
        fdv = str(Decimal(price) * Decimal(supply) / Decimal(10) ** 18) if price else None
        return dict(market_phase="v4", price_quote=price, fdv_quote=fdv, token_supply=str(supply), quote_reserve=None,
                    token_reserve=None, curve_address=target["curve_address"], pool_id=pool_id,
                    v4_active_liquidity=str(active), liquidity_metric_type="v4_active_liquidity_raw",
                    liquidity_quote_estimate=None, data_quality="verified" if price and active else "partial",
                    source_method="v4.extsload(StateLibrary)", error_code=None)


class MarketWorker:
    def __init__(self, config, rpc, db, telemetry):
        self.config, self.rpc, self.db, self.telemetry = config, rpc, db, telemetry
        self.resolver = MarketResolver(config, rpc, db, telemetry)

    def allowed(self):
        now = datetime.now(timezone.utc)
        day = int(now.timestamp()) // 86400 * 86400
        minute = int(now.timestamp()) // 60 * 60
        return (self.db.market_calls(day) < self.config.market_daily_calls and
                self.db.market_calls(minute) < self.config.market_minute_calls)

    async def run(self):
        while True:
            if not self.allowed():
                self.telemetry.add("market_targets_skipped_budget")
                await asyncio.sleep(60)
                continue
            targets = self.db.due_market_targets(utc_now())
            if not targets:
                await asyncio.sleep(self.config.market_poll)
                continue
            for target in targets:
                self.telemetry.add("market_target_due")
                try:
                    values = await self.resolver.snapshot(target)
                    now = datetime.now(timezone.utc)
                    due = datetime.fromisoformat(target["due_at"])
                    snapshot = dict(launch_id=target["launch_id"], token_address=target["token_address"],
                        quote_asset_address=target["quote_asset_address"], target_age_seconds=target["target_age_seconds"],
                        observed_at=utc_now(), age_seconds=max(0, int((now-datetime.fromisoformat(target["block_timestamp"])).total_seconds())),
                        delay_seconds=max(0, int((now-due).total_seconds())), **values)
                    self.db.finish_market_target(target, snapshot)
                    self.telemetry.add("market_targets_completed")
                    self.telemetry.add("market_snapshot_" + values["market_phase"])
                except (RpcError, ValueError, InvalidOperation, OverflowError) as exc:
                    permanent = isinstance(exc, ValueError)
                    self.db.finish_market_target(target, error=type(exc).__name__, permanent=permanent)
                    self.telemetry.add("market_targets_failed" if permanent else "market_retry")
