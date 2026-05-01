import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from kalman_quant.config import load_config
from kalman_quant.execution import ExecutionEngine
from kalman_quant.live import DryRunCycle
from kalman_quant.models import StrategySignal, utc_now_iso
from kalman_quant.storage import SQLiteStore


class DryRunTests(unittest.TestCase):
    def test_dry_run_records_intent_without_broker_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "dry.yaml"
            db_path = Path(tmp) / "dry.db"
            cfg_path.write_text(
                """
{
  "profile": "dry_run",
  "mode": "dry_run",
  "db_path": "%s",
  "runs_dir": "%s",
  "strategy": {"rs_benchmark": "SPY", "rs_lookback_bars": 2, "rs_min": -999},
  "risk": {
    "initial_capital": 1000,
    "max_open_trades": 4,
    "budget_per_trade": 500,
    "min_price": 1,
    "require_market_uptrend": false
  },
  "execution": {},
  "universe": {"symbols": ["AMD", "SPY"]}
}
"""
                % (str(db_path), tmp),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            store = SQLiteStore(cfg.db_path)
            broker = Mock()
            engine = ExecutionEngine(cfg, store, broker=broker)
            idx = pd.date_range("2024-01-01", periods=5)
            base = pd.DataFrame(
                {
                    "open": [10, 11, 12, 13, 14],
                    "high": [11, 12, 13, 14, 15],
                    "low": [9, 10, 11, 12, 13],
                    "close": [10, 11, 12, 13, 14],
                    "volume": [1000] * 5,
                },
                index=idx,
            )
            strategy_signal = StrategySignal(
                ticker="AMD",
                timestamp=utc_now_iso(),
                signal=1,
                score=0.8,
                confidence=0.9,
                price=14.0,
                regime="bull",
                components={"trend": 1.0},
                explanation="test",
            )
            with patch("kalman_quant.live.dry_run.score_multifactor", return_value=strategy_signal):
                intents = DryRunCycle(cfg, store, engine).run_once({"AMD": base, "SPY": base}, cash=1000)
            try:
                self.assertEqual(intents, 1)
                self.assertFalse(broker.place_order.called)
                self.assertEqual(store.latest_order_intents()[0]["ticker"], "AMD")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
