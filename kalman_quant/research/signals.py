from typing import Dict, Optional

import numpy as np
import pandas as pd


def _col(df: pd.DataFrame, lower: str) -> str:
    if lower in df.columns:
        return lower
    upper = lower.capitalize()
    if upper in df.columns:
        return upper
    raise KeyError("Missing column %s" % lower)


def f_kalman_streaming(prices: pd.Series, measurement_noise: float = 1.0, process_noise: float = 0.01) -> pd.Series:
    if prices.empty:
        return pd.Series(dtype=float)
    state = float(prices.iloc[0])
    p = 1.0
    out = [state]
    for z in prices.iloc[1:]:
        p = p + process_noise
        k = p / (p + measurement_noise)
        state = state + k * (float(z) - state)
        p = (1 - k) * p
        out.append(state)
    return pd.Series(out, index=prices.index)


def khma(series: pd.Series, measurement_noise: float = 1.0, process_noise: float = 0.01) -> pd.Series:
    return f_kalman_streaming(series, measurement_noise, process_noise)


def true_range(df: pd.DataFrame) -> pd.Series:
    high = df[_col(df, "high")]
    low = df[_col(df, "low")]
    close = df[_col(df, "close")]
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    return pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)


def rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1.0 / float(length), adjust=False).mean()


def supertrend_backquant(df: pd.DataFrame, factor: float = 1.5, atr_period: int = 14, src_col: str = "kalman_hma") -> pd.DataFrame:
    out = df.copy()
    if "atr" not in out.columns:
        out["tr"] = true_range(out)
        out["atr"] = rma(out["tr"], atr_period)
    src = out[src_col]
    upper_band = src + factor * out["atr"]
    lower_band = src - factor * out["atr"]
    direction = np.ones(len(out), dtype=int)
    supertrend = np.full(len(out), np.nan)
    final_upper = np.full(len(out), np.nan)
    final_lower = np.full(len(out), np.nan)
    close = out[_col(out, "close")]

    for i in range(len(out)):
        if i == 0 or pd.isna(out["atr"].iloc[i]):
            final_upper[i] = upper_band.iloc[i]
            final_lower[i] = lower_band.iloc[i]
            supertrend[i] = np.nan
            continue
        final_upper[i] = upper_band.iloc[i] if (
            upper_band.iloc[i] < final_upper[i - 1] or close.iloc[i - 1] > final_upper[i - 1]
        ) else final_upper[i - 1]
        final_lower[i] = lower_band.iloc[i] if (
            lower_band.iloc[i] > final_lower[i - 1] or close.iloc[i - 1] < final_lower[i - 1]
        ) else final_lower[i - 1]
        if direction[i - 1] == -1:
            direction[i] = 1 if close.iloc[i] < final_lower[i] else -1
        else:
            direction[i] = -1 if close.iloc[i] > final_upper[i] else 1
        supertrend[i] = final_lower[i] if direction[i] == -1 else final_upper[i]

    out["supertrend"] = supertrend
    out["direction"] = direction
    return out


def add_kalman_supertrend_signals(df: pd.DataFrame, strategy: Optional[Dict] = None) -> pd.DataFrame:
    strategy = strategy or {}
    out = df.copy()
    high = out[_col(out, "high")]
    low = out[_col(out, "low")]
    close = out[_col(out, "close")]
    source_name = str(strategy.get("price_source", "hl2")).lower()
    if source_name == "hl2":
        out["src"] = (high + low) / 2.0
    elif source_name == "hlc3":
        out["src"] = (high + low + close) / 3.0
    else:
        out["src"] = close
    out["kalman_hma"] = khma(
        out["src"],
        float(strategy.get("measurement_noise", 0.25)),
        float(strategy.get("process_noise", 0.07)),
    )
    out = supertrend_backquant(
        out,
        factor=float(strategy.get("atr_factor", 2.0)),
        atr_period=int(strategy.get("atr_period", 10)),
        src_col="kalman_hma",
    )
    out["signal"] = 0
    cross_long = (out["direction"].shift(1) > 0) & (out["direction"] < 0)
    cross_short = (out["direction"].shift(1) < 0) & (out["direction"] > 0)
    out.loc[cross_long, "signal"] = 1
    out.loc[cross_short, "signal"] = -1
    return out


def relative_strength_score(df_symbol: pd.DataFrame, df_benchmark: Optional[pd.DataFrame], lookback: int = 20) -> float:
    if df_symbol is None or len(df_symbol) <= lookback:
        return -1e9
    close = _col(df_symbol, "close")
    r_symbol = float(df_symbol[close].iloc[-1] / df_symbol[close].iloc[-lookback] - 1.0)
    if df_benchmark is None or len(df_benchmark) <= lookback:
        return r_symbol
    bclose = _col(df_benchmark, "close")
    r_bench = float(df_benchmark[bclose].iloc[-1] / df_benchmark[bclose].iloc[-lookback] - 1.0)
    return r_symbol - r_bench
