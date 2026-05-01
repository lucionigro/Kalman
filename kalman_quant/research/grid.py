import itertools
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from kalman_quant.config import AppConfig
from kalman_quant.research.backtest import QuantBacktester


def run_research_grid(config: AppConfig, data: Dict, grid: Dict[str, Iterable] = None) -> pd.DataFrame:
    grid = grid or {
        "measurement_noise": [0.2, 0.25, 0.35],
        "process_noise": [0.05, 0.07],
        "atr_factor": [1.8, 2.0, 2.3],
        "score_entry_min": [0.25, 0.35, 0.45],
    }
    rows: List[Dict] = []
    keys = list(grid.keys())
    for idx, values in enumerate(itertools.product(*[grid[k] for k in keys]), 1):
        raw = dict(config.raw)
        raw["strategy"] = dict(config.strategy)
        for key, value in zip(keys, values):
            raw["strategy"][key] = value
        trial_cfg = AppConfig(raw=raw, path=config.path)
        result = QuantBacktester(trial_cfg, data).run("grid_%03d" % idx)
        row = {"run": str(result.run_dir), **{k: v for k, v in zip(keys, values)}, **result.metrics}
        rows.append(row)
    df = pd.DataFrame(rows).sort_values(["sharpe", "max_drawdown_pct"], ascending=[False, False])
    out_dir = Path(config.runs_dir) / "research_grid"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / "grid_results.csv", index=False)
    return df
