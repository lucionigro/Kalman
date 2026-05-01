from datetime import datetime, timezone
from typing import Iterable, List

import pandas as pd

from kalman_quant.models import DataQualityReport, utc_now_iso


def analyze_data_quality(ticker: str, df: pd.DataFrame, stale_days: int = 5) -> DataQualityReport:
    notes: List[str] = []
    if df is None or df.empty:
        return DataQualityReport(
            ticker=ticker,
            timestamp=utc_now_iso(),
            rows=0,
            start="",
            end="",
            missing_ohlcv_rows=0,
            stale=True,
            gap_count=0,
            split_warning=False,
            dividend_gap_warning=False,
            status="missing",
            notes=["no_data"],
        )
    required = ["open", "high", "low", "close", "volume"]
    missing_ohlcv = int(df[required].isna().any(axis=1).sum()) if all(c in df.columns for c in required) else len(df)
    if missing_ohlcv:
        notes.append("missing_ohlcv")
    idx = pd.DatetimeIndex(df.index).sort_values()
    start = str(idx[0])
    end = str(idx[-1])
    end_ts = idx[-1]
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize(timezone.utc)
    age_days = (datetime.now(timezone.utc) - end_ts.to_pydatetime()).days
    stale = age_days > stale_days
    if stale:
        notes.append("stale_%sd" % age_days)
    gaps = _trading_gap_count(idx)
    if gaps:
        notes.append("date_gaps")
    close = df["close"].astype(float)
    overnight_ratio = (df["open"].astype(float) / close.shift()).replace([float("inf"), -float("inf")], pd.NA)
    split_warning = bool(((overnight_ratio > 1.8) | (overnight_ratio < 0.55)).fillna(False).any())
    if split_warning:
        notes.append("possible_split")
    returns = close.pct_change().abs()
    dividend_gap_warning = bool((returns > 0.18).fillna(False).any()) and not split_warning
    if dividend_gap_warning:
        notes.append("large_gap")
    status = "ok" if not notes else "warn"
    if missing_ohlcv or len(df) < 60:
        status = "bad"
    return DataQualityReport(
        ticker=ticker,
        timestamp=utc_now_iso(),
        rows=int(len(df)),
        start=start,
        end=end,
        missing_ohlcv_rows=missing_ohlcv,
        stale=stale,
        gap_count=gaps,
        split_warning=split_warning,
        dividend_gap_warning=dividend_gap_warning,
        status=status,
        notes=notes,
    )


def _trading_gap_count(index: Iterable) -> int:
    idx = pd.DatetimeIndex(index).sort_values()
    if len(idx) < 2:
        return 0
    gaps = idx.to_series().diff().dt.days.fillna(1)
    return int((gaps > 5).sum())
