import unittest

from kalman_quant.portfolio import evaluate_entry, position_size


class RiskTests(unittest.TestCase):
    def test_position_size_uses_budget_cap(self):
        qty = position_size(50, 10000, {"budget_per_trade": 600})
        self.assertEqual(qty, 12)

    def test_entry_blocks_when_position_already_open(self):
        decision = evaluate_entry("AMD", 100, 1000, 0, True, True, {"max_open_trades": 4, "budget_per_trade": 600})
        self.assertFalse(decision.allowed)
        self.assertIn("already_open", decision.checks)


if __name__ == "__main__":
    unittest.main()
