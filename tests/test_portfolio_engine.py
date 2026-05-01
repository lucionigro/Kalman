import unittest

import pandas as pd

from kalman_quant.portfolio import evaluate_risk_state, volatility_position_size


class PortfolioEngineTests(unittest.TestCase):
    def test_risk_state_reduces_on_drawdown(self):
        state = evaluate_risk_state(89000, 100000, risk={"reduce_drawdown_pct": 0.10, "max_drawdown_pct": 0.15})
        self.assertEqual(state.state, "reduced")

    def test_risk_state_halts_on_max_drawdown(self):
        state = evaluate_risk_state(84000, 100000, risk={"reduce_drawdown_pct": 0.10, "max_drawdown_pct": 0.15})
        self.assertEqual(state.state, "halted")

    def test_volatility_position_size_returns_int(self):
        idx = pd.date_range("2024-01-01", periods=30)
        df = pd.DataFrame({"close": [100 + i for i in range(30)]}, index=idx)
        qty = volatility_position_size(120, 100000, df, {"budget_per_trade": 1000})
        self.assertIsInstance(qty, int)


if __name__ == "__main__":
    unittest.main()
