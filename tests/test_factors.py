import unittest

import pandas as pd

from kalman_quant.research.factors import market_breadth, score_multifactor


class FactorTests(unittest.TestCase):
    def test_multifactor_signal_has_components(self):
        idx = pd.date_range("2024-01-01", periods=220, freq="B")
        close = [100 + i * 0.2 for i in range(220)]
        df = pd.DataFrame(
            {
                "open": close,
                "high": [x + 1 for x in close],
                "low": [x - 1 for x in close],
                "close": close,
                "volume": [1_000_000] * 220,
            },
            index=idx,
        )
        signal = score_multifactor("AAPL", df, df, breadth=0.8, config={"score_entry_min": -1})
        self.assertIn("trend", signal.components)
        self.assertGreaterEqual(signal.confidence, 0)
        self.assertLessEqual(signal.confidence, 1)

    def test_market_breadth(self):
        idx = pd.date_range("2024-01-01", periods=60, freq="B")
        df = pd.DataFrame({"close": range(60), "open": range(60), "high": range(60), "low": range(60), "volume": [1] * 60}, index=idx)
        self.assertGreaterEqual(market_breadth({"A": df}, sma_len=20), 0)


if __name__ == "__main__":
    unittest.main()
