import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch, AsyncMock
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from phase2b_preflight import (FIXTURES, verify_curve, verify_swap, verify_tiny,
                               curve_event, curve_model, swap_direction, swap_event, verify_boundary)
from phase2b_audit_rpc import AuditRpc
from app.config import Config
from app.rpc import Rpc, RpcError


def fixture(name):
    return json.loads((FIXTURES / (name + ".json")).read_text())


class EvidenceTests(unittest.TestCase):
    def test_partial_fills_use_refund_and_reserved_allocation(self):
        for f in fixture("boundary_samples"):
            r = verify_boundary(f)
            self.assertTrue(r["verified"])
            self.assertGreater(r["refund"],0)

    def test_six_real_curve_trades_integer_math_and_transfers(self):
        for side in ("buy", "sell"):
            for n in range(1, 4):
                with self.subTest(side=side, n=n):
                    result = verify_curve(fixture(f"curve_{side}_{n}"))
                    self.assertTrue(result["verified"])
                    self.assertEqual(result["integer_deltas"], {"output": 0, "fee": 0, "tax": 0})
                    self.assertEqual(result["event"]["direction"], side)

    def test_fee_legs_round_separately(self):
        r = curve_model("buy", 199, 10000, 100000, 100, 100)
        self.assertEqual((r["fee"], r["tax"], r["net_quote"]), (1, 1, 197))
        self.assertNotEqual(r["fee"] + r["tax"], 199 * 200 // 10000)

    def test_bad_inputs_rejected(self):
        with self.assertRaises(ValueError):
            curve_model("buy", 1, 0, 1, 0, 0)
        with self.assertRaises(ValueError):
            curve_model("sell", 1, 1, 1, 10000, 0)

    def test_real_caller_recipient_are_distinct_and_not_merged(self):
        f = fixture("curve_buy_2")
        e = curve_event(f["log"])
        self.assertNotEqual(e["caller"], e["recipient"])
        self.assertEqual(e["recipient"], f["transaction"]["from"].lower())
        self.assertIsNone(e["economic_actor"])

    def test_router_recipient_does_not_become_transaction_user(self):
        f = fixture("curve_sell_1")
        e = curve_event(f["log"])
        self.assertEqual(e["caller"], e["recipient"])
        self.assertNotEqual(e["caller"], f["transaction"]["from"].lower())
        self.assertIsNone(e["economic_actor"])

    def test_wrong_emitter_and_earlier_activity_are_rejected(self):
        f = fixture("curve_buy_1")
        f["log"]["address"] = "0x" + "01" * 20
        with self.assertRaises(ValueError):
            verify_curve(f)
        f = fixture("curve_buy_1")
        previous = copy.deepcopy(f["log"])
        previous["logIndex"] = hex(int(previous["logIndex"], 16)-1)
        f["all_curve_logs_in_block"].append(previous)
        with self.assertRaises(ValueError):
            verify_curve(f)

    def test_removed_curve_event_rejected(self):
        log = fixture("curve_buy_1")["log"]
        log["removed"] = True
        with self.assertRaises(ValueError):
            curve_event(log)

    def test_real_v4_buy_and_sell_pool_settlement_and_hook(self):
        for side in ("buy", "sell"):
            f = fixture("v4_" + side)
            result = verify_swap(f)
            self.assertTrue(result["verified"])
            self.assertEqual(result["direction"], side)
            self.assertTrue(result["hook_fees"])
            self.assertNotEqual(result["swap_sender"], f["transaction"]["from"].lower())
            self.assertIsNone(result["economic_actor"])

    def test_currency_order_direction_truth_table(self):
        for quote in ("0x" + "00"*20, "0x" + "22"*20):
            token = "0x" + "11"*20
            for c0, c1 in ((token, quote), (quote, token)):
                for a, b, expected in ((7, -3, "buy"), (-7, 3, "sell")):
                    x, y = (a, b) if c0 == token else (b, a)
                    self.assertEqual(swap_direction(token, c0, c1, x, y), expected)

    def test_ambiguous_deltas_and_pool_id_rejected(self):
        for a, b in ((0, 1), (1, 0), (1, 1), (-1, -1)):
            with self.assertRaises(ValueError):
                swap_direction("a", "a", "b", a, b)
        f = fixture("v4_buy")
        f["launch"]["pool_id"] = "0x" + "00"*32
        with self.assertRaises(ValueError):
            swap_event(f["log"], f["launch"])

    def test_tiny_deployed_results_match_math_and_fee_adjusted_limit(self):
        successes = {"buy": 0, "sell": 0}
        for f in fixture("tiny_simulations"):
            for r in verify_tiny(f):
                if r["status"] == "success":
                    successes[r["direction"]] += 1
                    self.assertEqual(r["integer_delta"], 0)
                    self.assertLess(abs(Decimal(r["fee_adjusted_relative_difference"])), Decimal(".000002"))
        self.assertEqual(successes, {"buy": 2, "sell": 2})


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_budget_persists_before_network_and_rejects_next_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "usage.json"
            ledger.write_text(json.dumps({"http": 499}))
            config = Config("https://example.invalid", 4663, (), "", Path(tmp)/"db", Path(tmp)/"log")
            rpc = AuditRpc(config, ledger)
            payload = {"method": "eth_call"}
            try:
                with patch.object(Rpc, "_send", AsyncMock(side_effect=RpcError("offline"))) as network:
                    with self.assertRaises(RpcError):
                        await rpc._send(payload, "eth_call")
                    self.assertEqual(json.loads(ledger.read_text())["http"], 500)
                    with self.assertRaises(RpcError):
                        await rpc._send(payload, "eth_call")
                    self.assertEqual(network.await_count, 1)
                with self.assertRaises(ValueError):
                    await rpc.call("eth_sendRawTransaction", [])
            finally:
                await rpc.close()
