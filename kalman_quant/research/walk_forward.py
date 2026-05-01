from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from kalman_quant.config import AppConfig
from kalman_quant.research.backtest import QuantBacktester


def run_walk_forward(config: AppConfig, data: Dict[str, pd.DataFrame], window_days: int = 252, test_days: int = 63) -> List[dict]:
    if not data:
        raise RuntimeError("No data loaded for walk-forward")
    all_dates = sorted(set().union(*[set(df.index) for df in data.values()]))
    rows = []
    start = 0
    fold = 1
    while start + window_days + test_days <= len(all_dates):
        train_dates = all_dates[start : start + window_days]
        test_dates = all_dates[start + window_days : start + window_days + test_days]
        train_data = _slice_data(data, train_dates[0], train_dates[-1])
        test_data = _slice_data(data, test_dates[0], test_dates[-1])
        train = QuantBacktester(config, train_data).run("wf_%02d_train" % fold)
        test = QuantBacktester(config, test_data).run("wf_%02d_test" % fold)
        rows.append(
            {
                "fold": fold,
                "train_start": str(train_dates[0]),
                "train_end": str(train_dates[-1]),
                "test_start": str(test_dates[0]),
                "test_end": str(test_dates[-1]),
                "train_return_pct": train.metrics.get("total_return_pct", 0),
                "test_return_pct": test.metrics.get("total_return_pct", 0),
                "train_sharpe": train.metrics.get("sharpe", 0),
                "test_sharpe": test.metrics.get("sharpe", 0),
                "test_max_drawdown_pct": test.metrics.get("max_drawdown_pct", 0),
            }
        )
        start += test_days
        fold += 1
    run_dir = Path(config.runs_dir) / ("walk_forward_%s" % datetime.now().strftime("%Y-%m-%d_%H%M%S"))
    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(run_dir / "walk_forward.csv", index=False)
    return rows


def _slice_data(data: Dict[str, pd.DataFrame], start, end) -> Dict[str, pd.DataFrame]:
    return {symbol: df.loc[start:end].copy() for symbol, df in data.items() if not df.loc[start:end].empty}
