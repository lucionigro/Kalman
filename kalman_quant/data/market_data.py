from pathlib import Path
from typing import Dict, Optional

import pandas as pd


class LocalDataProvider:
    """Reads cached historical data without connecting to IBKR."""

    def __init__(self, data_cache_dir: str = "data_cache", backtest_cache_dir: str = "backtest_cache"):
        self.data_cache_dir = Path(data_cache_dir)
        self.backtest_cache_dir = Path(backtest_cache_dir)

    def load_symbol(self, symbol: str) -> Optional[pd.DataFrame]:
        candidates = [
            self.data_cache_dir / ("%s_3Y_1day_RTH.csv" % symbol),
            self.data_cache_dir / ("%s_3M_1day_RTH.parquet" % symbol),
            self.data_cache_dir / ("%s_2M_1day_RTH.parquet" % symbol),
            self.backtest_cache_dir / ("%s_1_day.pkl" % symbol),
        ]
        for path in candidates:
            if not path.exists():
                continue
            try:
                if path.suffix == ".parquet":
                    df = pd.read_parquet(path)
                elif path.suffix == ".csv":
                    df = pd.read_csv(path)
                elif path.suffix == ".pkl":
                    cached = pd.read_pickle(path)
                    df = cached.get("data", cached) if isinstance(cached, dict) else cached
                else:
                    continue
                return normalize_ohlcv(df)
            except Exception:
                continue
        return None

    def load_many(self, symbols) -> Dict[str, pd.DataFrame]:
        out = {}
        for symbol in symbols:
            df = self.load_symbol(symbol)
            if df is not None and not df.empty:
                out[symbol] = df
        return out


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce")
        out = out.dropna(subset=["date"]).set_index("date")
    elif not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
        out = out[~out.index.isna()]
    mapping = {}
    for col in out.columns:
        lower = str(col).lower()
        if lower in {"open", "high", "low", "close", "volume"}:
            mapping[col] = lower
    out = out.rename(columns=mapping)
    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(out.columns)
    if missing:
        raise ValueError("Missing OHLCV columns: %s" % sorted(missing))
    out = out.sort_index()
    out["dollar_volume"] = out["close"] * out["volume"]
    return out
