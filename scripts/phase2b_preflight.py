"""Recheck committed chain evidence offline; no DB access or RPC by default."""
import _bootstrap  # noqa: F401
import json
from decimal import Decimal, localcontext
from pathlib import Path
from eth_abi import decode, encode
from eth_utils import keccak

FIXTURES = Path(__file__).resolve().parents[1] / "tests/fixtures/phase2b"
BUY_SIGNATURE = "CurveBuy(address,address,uint256,uint256,uint256,uint256)"
SELL_SIGNATURE = "CurveSell(address,address,uint256,uint256,uint256,uint256)"
SWAP_SIGNATURE = "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)"


def topic(signature):
    return "0x" + keccak(text=signature).hex()


def address(word):
    return decode(["address"], bytes.fromhex(word[2:]))[0]


def curve_event(log):
    if len(log["topics"]) != 3 or log.get("removed", False):
        raise ValueError("Invalid or removed curve event")
    signature = log["topics"][0].lower()
    if signature not in (topic(BUY_SIGNATURE), topic(SELL_SIGNATURE)):
        raise ValueError("Unknown curve event")
    if len(bytes.fromhex(log["data"][2:])) != 128:
        raise ValueError("Invalid curve event length")
    first, second, fee, tax = decode(["uint256"] * 4, bytes.fromhex(log["data"][2:]))
    buy = signature == topic(BUY_SIGNATURE)
    return {"direction": "buy" if buy else "sell", "caller": address(log["topics"][1]),
            "recipient": address(log["topics"][2]), "token_amount": second if buy else first,
            "quote_amount": first if buy else second, "base_fee_quote": fee,
            "creator_tax_quote": tax, "economic_actor": None}


def curve_model(direction, amount, q, t, fee_bps, tax_bps, reserved=None):
    """Solidity integer math; amount is credited input, before any partial-fill refund."""
    if direction not in ("buy", "sell") or min(amount, q, t) <= 0:
        raise ValueError("Invalid trade inputs")
    if min(fee_bps, tax_bps) < 0 or fee_bps + tax_bps >= 10000:
        raise ValueError("Invalid fees")
    gross = amount if direction == "buy" else amount * q // (t + amount)
    fee, tax = gross * fee_bps // 10000, gross * tax_bps // 10000
    net = gross - fee - tax
    output = net * t // (q + net) if direction == "buy" else net
    if direction == "buy" and reserved is not None:
        if not 0 < reserved < t:
            raise ValueError("Invalid reserved allocation")
        if output > t-reserved:
            output = t-reserved
            needed = output*q//(t-output)+1
            denominator = 10000-fee_bps-tax_bps
            gross = min((needed*10000+denominator-1)//denominator, amount)
            fee, tax = gross*fee_bps//10000, gross*tax_bps//10000
            net = gross-fee-tax
    return {"gross_quote": gross, "net_quote": net, "fee": fee, "tax": tax, "output": output}


def swap_direction(token, currency0, currency1, amount0, amount1):
    if currency0.lower() == currency1.lower() or token.lower() not in (currency0.lower(), currency1.lower()):
        raise ValueError("Invalid currencies")
    td, qd = (amount0, amount1) if token.lower() == currency0.lower() else (amount1, amount0)
    if td > 0 and qd < 0:
        return "buy"
    if td < 0 and qd > 0:
        return "sell"
    raise ValueError("Zero or same-sign deltas cannot establish trade direction")


def swap_event(log, pool):
    if (len(log["topics"]) != 3 or log["topics"][0].lower() != topic(SWAP_SIGNATURE)
            or log.get("removed", False) or log["address"].lower() != pool["pool_manager_address"].lower()):
        raise ValueError("Invalid swap emitter/event")
    key = [pool["currency0"], pool["currency1"], pool["fee"], pool["tick_spacing"], pool["hooks"]]
    if (int(key[0], 16) >= int(key[1], 16)
            or {pool["token_address"].lower(), pool["quote_asset_address"].lower()} != {key[0].lower(), key[1].lower()}):
        raise ValueError("Pool currencies do not match launch")
    pool_id = "0x" + keccak(encode(["address", "address", "uint24", "int24", "address"], key)).hex()
    if pool_id != pool["pool_id"].lower() or pool_id != log["topics"][1].lower():
        raise ValueError("PoolId mismatch")
    if len(bytes.fromhex(log["data"][2:])) != 192:
        raise ValueError("Invalid swap length")
    a, b, sqrt, liquidity, tick, fee = decode(["int128", "int128", "uint160", "uint128", "int24", "uint24"], bytes.fromhex(log["data"][2:]))
    return {"direction": swap_direction(pool["token_address"], *key[:2], a, b),
            "amount0": a, "amount1": b, "sqrtPriceX96": sqrt, "liquidity": liquidity,
            "tick": tick, "fee": fee, "swap_sender": address(log["topics"][2]),
            "economic_actor": None, "amount_scope": "pool_swap_before_afterSwap_hook"}


def verify_curve(f):
    log, pool, state = f["log"], f["launch"], f["state"]
    event = curve_event(log)
    if log["address"].lower() != pool["curve_address"].lower():
        raise ValueError("Curve emitter mismatch")
    # Block-1 is accepted only when there are no earlier curve-emitted logs.
    # Replay and actual Transfer checks provide independent corroboration.
    if any(int(z["logIndex"], 16) < int(log["logIndex"], 16) for z in f["all_curve_logs_in_block"]):
        raise ValueError("Earlier intra-block curve activity requires explicit reconstruction")
    if f["pre_block"] != int(log["blockNumber"], 16) - 1:
        raise ValueError("Pre-state block mismatch")
    if (f["receipt"]["transactionHash"] != log["transactionHash"]
            or f["receipt"]["blockHash"] != log["blockHash"]
            or f["receipt"]["status"] != "0x1"
            or f["transaction"]["hash"] != log["transactionHash"]
            or log not in f["receipt"]["logs"]):
        raise ValueError("Receipt/transaction evidence mismatch")
    q, t = decode(["uint256"] * 2, bytes.fromhex(state["getReserves()"][2:]))
    amount = event["quote_amount"] if event["direction"] == "buy" else event["token_amount"]
    model = curve_model(event["direction"], amount, q, t, int(state["feeBps()"], 16), int(state["creatorTaxBps()"], 16))
    if event["direction"] == "buy" and model["output"] >= t - int(state["reservedTokens()"], 16):
        raise ValueError("Allocation-boundary trade outside this verifier's scope")
    actual = event["token_amount"] if event["direction"] == "buy" else event["quote_amount"]
    deltas = {"output": model["output"] - actual, "fee": model["fee"] - event["base_fee_quote"],
              "tax": model["tax"] - event["creator_tax_quote"]}
    transfers = []
    for z in f["receipt"]["logs"]:
        if len(z["topics"]) == 3 and z["topics"][0] == topic("Transfer(address,address,uint256)"):
            transfers.append((z["address"].lower(), address(z["topics"][1]), address(z["topics"][2]), int(z["data"], 16)))
    curve = pool["curve_address"].lower()
    token, quote = pool["token_address"].lower(), pool["quote_asset_address"].lower()
    if event["direction"] == "buy":
        expected = [(quote, event["caller"], curve, event["quote_amount"]),
                    (token, curve, event["recipient"], event["token_amount"])]
    else:
        expected = [(token, event["caller"], curve, event["token_amount"]),
                    (quote, curve, event["recipient"], event["quote_amount"])]
    return {"event": event, "model": model, "integer_deltas": deltas,
            "transfers_match": all(x in transfers for x in expected),
            "verified": not any(deltas.values()) and all(x in transfers for x in expected)}


def verify_boundary(f):
    event, state, log = curve_event(f["log"]), f["state"], f["log"]
    refund_topic = topic("CurveBuyRefunded(address,uint256)")
    prior = [z for z in f["all_curve_logs_in_block"] if int(z["logIndex"],16) < int(log["logIndex"],16)]
    if (event["direction"] != "buy" or len(prior) != 1 or prior[0]["topics"][0] != refund_topic
            or prior[0]["transactionHash"] != log["transactionHash"]
            or address(prior[0]["topics"][1]) != event["caller"]):
        raise ValueError("Partial fill requires isolated same-transaction refund")
    refund = int(prior[0]["data"],16)
    q,t = decode(["uint256"]*2,bytes.fromhex(state["getReserves()"][2:]))
    model = curve_model("buy",event["quote_amount"]+refund,q,t,int(state["feeBps()"],16),
                        int(state["creatorTaxBps()"],16),int(state["reservedTokens()"],16))
    deltas = {"output":model["output"]-event["token_amount"],"spent":model["gross_quote"]-event["quote_amount"],
              "fee":model["fee"]-event["base_fee_quote"],"tax":model["tax"]-event["creator_tax_quote"]}
    return {"refund":refund,"integer_deltas":deltas,"verified":not any(deltas.values())}


def verify_swap(f):
    log, pool = f["log"], f["launch"]
    event = swap_event(log, pool)
    if (f["receipt"]["status"] != "0x1" or log not in f["receipt"]["logs"]
            or f["receipt"]["blockHash"] != log["blockHash"]
            or f["receipt"]["transactionHash"] != log["transactionHash"]
            or f["transaction"]["hash"] != log["transactionHash"]):
        raise ValueError("Swap receipt mismatch")
    manager, hook = pool["pool_manager_address"].lower(), pool["hooks"].lower()
    manager_swaps = [z for z in f["receipt"]["logs"] if z["address"].lower() == manager and z["topics"][0] == topic(SWAP_SIGNATURE)]
    if len(manager_swaps) != 1:
        raise ValueError("Multi-swap receipt requires per-swap settlement analysis")
    later = [z for z in f["receipt"]["logs"] if int(z["logIndex"], 16) > int(log["logIndex"], 16)]
    net_out = {pool["currency0"].lower(): 0, pool["currency1"].lower(): 0}
    hook_transfers = {}
    for z in later:
        currency = z["address"].lower()
        if currency in net_out and z["topics"][0] == topic("Transfer(address,address,uint256)"):
            sender, recipient = address(z["topics"][1]), address(z["topics"][2])
            amount = int(z["data"], 16)
            net_out[currency] += amount * ((sender == manager) - (recipient == manager))
            if sender == manager and recipient == hook:
                hook_transfers[currency] = hook_transfers.get(currency, 0) + amount
    fees = []
    for z in later:
        if (z["address"].lower() == hook and z["topics"][0] == topic("HookFeeCollected(bytes32,address,uint256,uint256)")
                and z["topics"][1].lower() == pool["pool_id"].lower()):
            currency, fee, tax = decode(["address", "uint256", "uint256"], bytes.fromhex(z["data"][2:]))
            if hook_transfers.get(currency, 0) != fee + tax:
                raise ValueError("Hook fee transfer mismatch")
            fees.append({"currency": currency, "base_fee": fee, "creator_tax": tax})
    expected = dict(zip([pool["currency0"].lower(), pool["currency1"].lower()], [event["amount0"], event["amount1"]]))
    return {**event, "pool_transfer_deltas": net_out, "hook_fees": fees,
            "verified": net_out == expected and bool(fees)}


def verify_tiny(f):
    q, t = decode(["uint256"] * 2, bytes.fromhex(f["state"]["getReserves()"][2:]))
    fee = int(f["state"]["feeBps()"], 16)
    tax = int(f["state"]["creatorTaxBps()"], 16)
    results = []
    for s in f["simulations"]:
        if s["status"] != "success":
            results.append({"signature": s["signature"], "status": s["status"]})
            continue
        call = s["call"]
        if (call["from"].lower() != f["transaction"]["from"].lower()
                or call["to"].lower() != f["launch"]["curve_address"].lower()
                or int(call["value"], 16) != 0
                or int(f["block"], 16) != int(f["transaction"]["blockNumber"], 16) - 1):
            raise ValueError("Simulation actor/state mismatch")
        direction = "buy" if s["signature"].startswith("buy(") else "sell"
        signature = direction + "(uint256,uint256,address)"
        if call["data"][:10] != topic(signature)[:10]:
            raise ValueError("Simulation selector mismatch")
        amount, minimum, recipient = decode(["uint256", "uint256", "address"], bytes.fromhex(call["data"][10:]))
        if (amount != int(s["amount"]) or amount != (q if direction == "buy" else t)//10**6
                or minimum != 1 or recipient != call["from"].lower()):
            raise ValueError("Simulation calldata mismatch")
        actual = decode(["uint256"], bytes.fromhex(s["result"][2:]))[0]
        expected = curve_model(direction, amount, q, t, fee, tax)["output"]
        with localcontext() as ctx:
            ctx.prec = 78
            scale = Decimal(10) ** (int(f["state"]["token_decimals"],16) - int(f["state"]["quote_decimals"],16))
            marginal = Decimal(q) / t * scale
            effective = (Decimal(amount) / actual if direction == "buy" else Decimal(actual) / amount) * scale
            factor = Decimal(10000-fee-tax)/10000
            adjusted = effective * factor if direction == "buy" else effective / factor
            results.append({"direction": direction, "status": "success", "integer_delta": expected-actual,
                            "marginal_price": str(marginal), "effective_price": str(effective),
                            "fee_adjusted_relative_difference": str(adjusted/marginal-1)})
    return results


def report():
    results, errors, identities = {}, [], set()
    for file in sorted(FIXTURES.glob("curve_*.json")):
        try:
            f = json.loads(file.read_text())
            identity = (f["log"]["transactionHash"], f["log"]["logIndex"])
            if identity in identities:
                raise ValueError("Duplicate fixture identity")
            identities.add(identity)
            results[file.stem] = verify_curve(f)
        except (ValueError, KeyError) as exc:
            errors.append(file.name + ": " + str(exc))
    swaps = {}
    for file in sorted(FIXTURES.glob("v4_*.json")):
        try:
            f = json.loads(file.read_text())
            swaps[file.stem] = verify_swap(f)
        except (ValueError, KeyError) as exc:
            errors.append(file.name + ": " + str(exc))
    evidence_path = FIXTURES / "verification.json"
    evidence = json.loads(evidence_path.read_text()) if evidence_path.exists() else {}
    boundary_path = FIXTURES / "boundary_samples.json"
    boundaries = [verify_boundary(f) for f in json.loads(boundary_path.read_text())] if boundary_path.exists() else []
    tiny_path = FIXTURES / "tiny_simulations.json"
    tiny = []
    if tiny_path.exists():
        for f in json.loads(tiny_path.read_text()):
            tiny.append({"curve": f["launch"]["curve_address"], "checks": verify_tiny(f)})
    tiny_ok = (all(len({r["curve"] for r in tiny for s in r["checks"]
                       if s.get("direction") == d and s.get("integer_delta") == 0}) >= 2 for d in ("buy", "sell"))
               and all(s.get("integer_delta", 0) == 0 for r in tiny for s in r["checks"]))
    buys = sum(r["verified"] and r["event"]["direction"] == "buy" for r in results.values())
    sells = sum(r["verified"] and r["event"]["direction"] == "sell" for r in results.values())
    v4_ok = {v["direction"] for v in swaps.values() if v["verified"]} == {"buy", "sell"}
    behavior = buys >= 3 and sells >= 3 and not errors
    unresolved = evidence.get("unresolved", ["Audit evidence manifest missing"]) + errors
    return {"label": "OFFLINE REVALIDATION OF RECORDED CHAIN EVIDENCE",
            "deployment_source_equivalence": evidence.get("deployment_source_equivalence", "UNRESOLVED"),
            "curve_buy_behavior": "VERIFIED" if buys >= 3 else "UNRESOLVED",
            "curve_sell_behavior": "VERIFIED" if sells >= 3 else "UNRESOLVED",
            "price_metric": "VALID_MARGINAL_PRICE" if behavior else "UNRESOLVED",
            "curve_event_semantics": "VERIFIED" if behavior else "PARTIAL",
            "v4_swap_semantics": "VERIFIED" if v4_ok else "PARTIAL",
            "actor_identity": "VERIFIED explicit caller/recipient/swap_sender; economic_actor UNKNOWN",
            "fixtures": {"curve": results, "boundary_buys": boundaries, "v4": swaps, "tiny_simulations": tiny}, "rpc_usage": evidence.get("rpc_usage", {}),
            "gate": "READY" if behavior and v4_ok and tiny_ok and boundaries and all(b["verified"] for b in boundaries) and not unresolved else "NOT_READY",
            "unresolved": unresolved, "nonblocking_limitations": evidence.get("nonblocking_limitations", [])}


if __name__ == "__main__":
    print(json.dumps(report(), indent=2))
