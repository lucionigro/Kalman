import unittest

from kalman_quant.research.promotion import evaluate_promotion


class PromotionTests(unittest.TestCase):
    def test_promotion_requires_all_gates(self):
        approved, checks = evaluate_promotion(
            {
                "sharpe": 1.1,
                "max_drawdown_pct": -8.0,
                "profit_factor": 1.5,
                "total_trades": 30,
            }
        )
        self.assertTrue(approved)
        self.assertTrue(all(checks.values()))

    def test_rejects_low_profit_factor(self):
        approved, checks = evaluate_promotion(
            {
                "sharpe": 1.1,
                "max_drawdown_pct": -8.0,
                "profit_factor": 1.0,
                "total_trades": 30,
            }
        )
        self.assertFalse(approved)
        self.assertFalse(checks["profit_factor_ok"])


if __name__ == "__main__":
    unittest.main()
