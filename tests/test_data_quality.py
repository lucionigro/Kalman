import unittest

import pandas as pd

from kalman_quant.data.quality import analyze_data_quality


class DataQualityTests(unittest.TestCase):
    def test_reports_ok_for_clean_data(self):
        idx = pd.date_range("2026-04-01", periods=30, freq="B", tz="UTC")
        df = pd.DataFrame(
            {
                "open": [100.0] * 30,
                "high": [101.0] * 30,
                "low": [99.0] * 30,
                "close": [100.0] * 30,
                "volume": [1_000_000] * 30,
            },
            index=idx,
        )
        report = analyze_data_quality("SPY", df, stale_days=3650)
        self.assertEqual(report.missing_ohlcv_rows, 0)
        self.assertFalse(report.split_warning)


if __name__ == "__main__":
    unittest.main()
