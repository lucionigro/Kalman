from typing import Dict, Optional

import pandas as pd

from kalman_quant.models import StrategySignal, utc_now_iso
from kalman_quant.research.signals import add_kalman_supertrend_signals, relative_strength_score


def score_multifactor(
    ticker: str,
    df: pd.DataFrame,
    benchmark: Optional[pd.DataFrame],
    breadth: float = 0.5,
    config: Dict = None,
) -> StrategySignal:
    config = config or {}
    scored = add_kalman_supertrend_signals(df, config)
    last = scored.iloc[-1]
    close = scored["close"].astype(float)
    price = float(last["close"])
    rs20 = relative_strength_score(scored, benchmark, 20)
    rs60 = relative_strength_score(scored, benchmark, 60)
    rs120 = relative_strength_score(scored, benchmark, 120)
    trend = 1.0 if int(last.get("direction", 1)) == -1 else -1.0
    breakout = _breakout_score(close)
    compression = _vol_compression_score(close)
    sma_distance = _sma_distance_score(close)
    regime = _regime_label(benchmark, breadth)
    regime_score = {"bull": 1.0, "neutral": 0.0, "risk_off": -1.0}.get(regime, 0.0)
    components = {
        "trend": trend,
        "rs20": _clip(rs20 * 5),
        "rs60": _clip(rs60 * 3),
        "rs120": _clip(rs120 * 2),
        "breakout": breakout,
        "compression": compression,
        "sma_distance": sma_distance,
        "breadth": _clip((breadth - 0.5) * 2),
        "regime": regime_score,
    }
    weights = {
        "trend": 0.22,
        "rs20": 0.18,
        "rs60": 0.12,
        "rs120": 0.08,
        "breakout": 0.12,
        "compression": 0.08,
        "sma_distance": 0.08,
        "breadth": 0.06,
        "regime": 0.06,
    }
    score = sum(components[k] * weights[k] for k in weights)
    raw_signal = int(last.get("signal", 0))
    signal = 1 if score >= float(config.get("score_entry_min", 0.35)) and raw_signal >= 0 else raw_signal
    confidence = min(1.0, max(0.0, (score + 1.0) / 2.0))
    explanation = "score=%.3f trend=%.2f rs20=%.2f breakout=%.2f regime=%s" % (
        score,
        components["trend"],
        components["rs20"],
        components["breakout"],
        regime,
    )
    return StrategySignal(
        ticker=ticker,
        timestamp=utc_now_iso(),
        signal=signal,
        score=float(score),
        confidence=float(confidence),
        price=price,
        regime=regime,
        components=components,
        explanation=explanation,
    )


def market_breadth(data: Dict[str, pd.DataFrame], sma_len: int = 50) -> float:
    ok = 0
    total = 0
    for df in data.values():
        if df is None or len(df) < sma_len:
            continue
        close = df["close"].astype(float)
        total += 1
        if float(close.iloc[-1]) > float(close.rolling(sma_len).mean().iloc[-1]):
            ok += 1
    return ok / total if total else 0.5


def _breakout_score(close: pd.Series, lookback: int = 55) -> float:
    if len(close) < lookback:
        return 0.0
    high = float(close.iloc[-lookback:-1].max())
    if high <= 0:
        return 0.0
    return _clip((float(close.iloc[-1]) / high - 1.0) * 20)


def _vol_compression_score(close: pd.Series, short: int = 10, long: int = 60) -> float:
    if len(close) < long:
        return 0.0
    returns = close.pct_change()
    short_vol = float(returns.tail(short).std())
    long_vol = float(returns.tail(long).std())
    if long_vol <= 0:
        return 0.0
    return _clip(1.0 - short_vol / long_vol)


def _sma_distance_score(close: pd.Series, sma_len: int = 50) -> float:
    if len(close) < sma_len:
        return 0.0
    sma = float(close.rolling(sma_len).mean().iloc[-1])
    if sma <= 0:
        return 0.0
    dist = float(close.iloc[-1]) / sma - 1.0
    return _clip(dist * 5)


def _regime_label(benchmark: Optional[pd.DataFrame], breadth: float) -> str:
    if benchmark is None or len(benchmark) < 200:
        return "neutral"
    close = benchmark["close"].astype(float)
    above_200 = float(close.iloc[-1]) > float(close.rolling(200).mean().iloc[-1])
    above_50 = float(close.iloc[-1]) > float(close.rolling(50).mean().iloc[-1])
    if above_200 and above_50 and breadth >= 0.45:
        return "bull"
    if not above_200 or breadth < 0.35:
        return "risk_off"
    return "neutral"


def _clip(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))
