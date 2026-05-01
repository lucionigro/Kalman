import unittest

import pandas as pd

from kalman_quant.research.signals import add_kalman_supertrend_signals, relative_strength_score


class SignalTests(unittest.TestCase):
    def test_adds_shared_signal_columns(self):
        df = pd.DataFrame(
            {
                "open": [10, 11, 12, 13, 14, 15],
                "high": [11, 12, 13, 14, 15, 16],
                "low": [9, 10, 11, 12, 13, 14],
                "close": [10, 11, 12, 13, 14, 15],
                "volume": [1000] * 6,
            },
            index=pd.date_range("2024-01-01", periods=6),
        )
        out = add_kalman_supertrend_signals(df, {"atr_period": 2, "atr_factor": 1.5})
        for col in ["src", "kalman_hma", "supertrend", "direction", "signal"]:
            self.assertIn(col, out.columns)

    def test_relative_strength_score_vs_benchmark(self):
        idx = pd.date_range("2024-01-01", periods=25)
        sym = pd.DataFrame({"close": list(range(100, 125))}, index=idx)
        bench = pd.DataFrame({"close": list(range(100, 125))}, index=idx)
        self.assertAlmostEqual(relative_strength_score(sym, bench, 20), 0.0)


if __name__ == "__main__":
    unittest.main()
