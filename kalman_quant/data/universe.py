from typing import Dict, Iterable, List, Tuple

import pandas as pd

from kalman_quant.models import UniverseSnapshot, utc_now_iso


DEFAULT_TOP_US_LIQUID = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOG", "GOOGL", "AVGO", "TSLA", "AMD",
    "JPM", "V", "MA", "UNH", "LLY", "XOM", "COST", "WMT", "HD", "PG",
    "BAC", "NFLX", "CRM", "ORCL", "ADBE", "CSCO", "INTC", "QCOM", "TXN", "AMAT",
    "SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "XLV", "XLY", "XLI",
]


def build_liquid_universe(
    data: Dict[str, pd.DataFrame],
    candidates: Iterable[str] = None,
    min_price: float = 5.0,
    min_adv_usd: float = 25_000_000,
    lookback: int = 20,
    max_symbols: int = 100,
) -> UniverseSnapshot:
    candidates = list(candidates or DEFAULT_TOP_US_LIQUID)
    accepted: List[Tuple[str, float]] = []
    rejected: Dict[str, str] = {}
    for symbol in candidates:
        df = data.get(symbol)
        if df is None or df.empty:
            rejected[symbol] = "no_data"
            continue
        if len(df) < lookback:
            rejected[symbol] = "insufficient_history"
            continue
        last = df.iloc[-1]
        price = float(last["close"])
        if price < min_price:
            rejected[symbol] = "min_price"
            continue
        adv = float((df.tail(lookback)["close"] * df.tail(lookback)["volume"]).mean())
        if adv < min_adv_usd:
            rejected[symbol] = "min_adv"
            continue
        accepted.append((symbol, adv))
    accepted.sort(key=lambda x: x[1], reverse=True)
    return UniverseSnapshot(
        timestamp=utc_now_iso(),
        name="top_us_liquid",
        symbols=[s for s, _ in accepted[:max_symbols]],
        rejected=rejected,
        source="cache_or_ibkr",
    )
