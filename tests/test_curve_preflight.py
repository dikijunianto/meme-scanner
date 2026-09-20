import sys
from pathlib import Path
import unittest
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from curve_price_preflight import model_comparison


class PreflightTests(unittest.TestCase):
    def test_fee_spread_converges_to_marginal_price(self):
        for decimals in (6, 18):
            r = model_comparison(10**9 * 10**decimals, 10**12 * 10**18, decimals, 100, 200)
            self.assertEqual(Decimal(r["reserve_price_quote"]), Decimal(".001"))
            self.assertGreater(Decimal(r["buy_relative_difference"]), Decimal(".03"))
            self.assertLess(Decimal(r["sell_relative_difference"]), Decimal("-.03"))
            for field in ("buy_fee_adjusted_relative_difference", "sell_fee_adjusted_relative_difference"):
                self.assertLess(abs(Decimal(r[field])), Decimal(".00001"))
            self.assertIn("INFERRED", r["label"])

    def test_invalid_or_unquotable_state_is_rejected(self):
        for args in [(0, 10, 18, 0, 0), (10, 10, 18, 10000, 0),
                     (1, 1, 18, 0, 0), (10, 10, 37, 0, 0)]:
            with self.assertRaises(ValueError):
                model_comparison(*args)

    def test_integer_fee_rounding_is_not_hidden(self):
        r = model_comparison(1000 * 10**6, 1000000 * 10**18, 6, 100, 200)
        self.assertEqual(Decimal(r["sell_relative_difference"]), Decimal("-.029"))
