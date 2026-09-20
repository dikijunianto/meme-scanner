"""Bounded, read-only curve audit. Model estimates are NOT independent live quotes."""
import _bootstrap  # noqa: F401
import asyncio
from collections import Counter
from dataclasses import replace
from decimal import Decimal, localcontext
import json
import sqlite3

from eth_abi import decode
from eth_utils import keccak

from app.config import Config
from app.market import curve_price, selector
from app.models import utc_now
from app.rpc import Rpc

SOURCE = "https://github.com/ponsdotdev/pons-labs/blob/162310fbd1217717e2f5e4cde794d6a11322b469/contractsV2/src/v2/PonsV2BondingCurve.sol"


def model_comparison(q, t, decimals, fee, tax):
    """Published constant-product model; no claim of deployed-code equivalence."""
    if min(q, t) <= 0 or not 0 <= decimals <= 36 or min(fee, tax) < 0 or fee + tax >= 10000:
        raise ValueError("Invalid reserves, decimals or fee")
    # One millionth of each reserve: small but not an infinitesimal live quote.
    buy_input, sell_input = max(1, q // 10**6), max(1, t // 10**6)
    buy_net = buy_input - buy_input * fee // 10000 - buy_input * tax // 10000
    buy_output = buy_net * t // (q + buy_net)
    sell_gross = sell_input * q // (t + sell_input)
    sell_output = sell_gross - sell_gross * fee // 10000 - sell_gross * tax // 10000
    if min(buy_output, sell_output) <= 0:
        raise ValueError("Quote rounds to zero")
    with localcontext() as ctx:
        ctx.prec = 78
        scale = Decimal(10) ** (18 - decimals)
        p = Decimal(curve_price(q, t, decimals))
        buy = Decimal(buy_input) / buy_output * scale
        sell = Decimal(sell_output) / sell_input * scale
        fraction = Decimal(10000 - fee - tax) / 10000
        return {"label": "INFERRED: source-model estimates, not eth_call execution quotes",
                "reserve_price_quote": str(p), "buy_input_raw": str(buy_input),
                "buy_output_raw": str(buy_output), "sell_input_raw": str(sell_input),
                "sell_output_raw": str(sell_output), "buy_implied_price_quote": str(buy),
                "sell_implied_price_quote": str(sell),
                "buy_relative_difference": str(buy / p - 1),
                "sell_relative_difference": str(sell / p - 1),
                "buy_fee_adjusted_relative_difference": str(buy * fraction / p - 1),
                "sell_fee_adjusted_relative_difference": str(sell / fraction / p - 1)}


class Counts(Counter):
    def __bool__(self):
        return True

    def add(self, metric, count=1):
        self[metric] += count


async def audit():
    config = replace(Config.load(), rpc_rps=1, retry_attempts=2)
    # Existing DB only; no migration, telemetry writes, or market data changes.
    with sqlite3.connect(config.database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("""SELECT token_address,curve_address,quote_asset_address
            FROM launches WHERE is_stock_quote=1 ORDER BY id DESC LIMIT 3""").fetchall()
    counts = Counts()
    rpc = Rpc(config, counts)
    result = {"observed_at": utc_now(), "source": SOURCE, "samples": [],
              "deployment_gate": "NOT_READY",
              "reason": "Independent buy/sell execution quotes and deployed-source equivalence unverified"}
    try:
        await rpc.check_chain()
        block = await rpc.call("eth_blockNumber", [])
        result["block"] = int(block, 16)
        for row in rows:
            item = dict(row)
            curve, quote = item["curve_address"], item["quote_asset_address"]
            signatures = ["getReserves()", "feeBps()", "creatorTaxBps()", "graduated()"]
            calls = [("eth_call", [{"to": curve, "data": selector(s)}, block]) for s in signatures]
            calls.append(("eth_call", [{"to": quote, "data": selector("decimals()")}, block]))
            raw = await rpc.batch(calls)
            if any(isinstance(value, Exception) for value in raw):
                item["error"] = "Contract read failed; no model comparison"
                result["samples"].append(item)
                continue
            q, t = decode(["uint256", "uint256"], bytes.fromhex(raw[0][2:]))
            fee, tax, graduated, decimals = [decode(["uint256"], bytes.fromhex(v[2:]))[0] for v in raw[1:]]
            item.update(quote_reserve_raw=str(q), token_reserve_raw=str(t), fee_bps=fee,
                        creator_tax_bps=tax, quote_decimals=decimals, graduated=bool(graduated))
            code = await rpc.call("eth_getCode", [curve, block])
            item["runtime_keccak"] = "0x" + keccak(bytes.fromhex(code[2:])).hex()
            # Byte-string presence is only a candidate search, not an ABI proof.
            item["quote_selector_byte_presence_not_abi_proof"] = {
                s: selector(s)[2:] in code for s in ["getBuyQuote(uint256)", "getSellQuote(uint256)",
                                                    "quoteBuy(uint256)", "quoteSell(uint256)"]}
            if not graduated:
                try:
                    item["source_model"] = model_comparison(q, t, decimals, fee, tax)
                except ValueError as exc:
                    item["error"] = str(exc)
            result["samples"].append(item)
    finally:
        await rpc.close()
    result["rpc_attempt_counters"] = dict(counts)
    return result


if __name__ == "__main__":
    print(json.dumps(asyncio.run(audit()), indent=2))
