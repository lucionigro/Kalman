from pathlib import Path
from typing import Dict, Iterable

import pandas as pd


class IBKRDailyDownloader:
    """Reproducible daily downloader. Imports ib_insync only when used."""

    def __init__(self, config):
        self.config = config

    def sync(self, symbols: Iterable[str], duration: str = "3 Y", bar_size: str = "1 day") -> Dict[str, str]:
        from ib_insync import IB, Stock, util

        cache_dir = Path(self.config.raw.get("data_cache_dir", "data_cache"))
        cache_dir.mkdir(parents=True, exist_ok=True)
        ibkr = self.config.ibkr
        ib = IB()
        results: Dict[str, str] = {}
        try:
            ib.connect(
                ibkr.get("host", "127.0.0.1"),
                int(ibkr.get("port", 7497)),
                clientId=int(ibkr.get("client_id", 21)),
            )
            ib.reqMarketDataType(int(ibkr.get("market_data_type", 3)))
            for symbol in symbols:
                try:
                    contract = Stock(symbol, "SMART", "USD")
                    qualified = ib.qualifyContracts(contract)
                    if not qualified:
                        results[symbol] = "contract_failed"
                        continue
                    bars = ib.reqHistoricalData(
                        qualified[0],
                        endDateTime="",
                        durationStr=duration,
                        barSizeSetting=bar_size,
                        whatToShow="TRADES",
                        useRTH=True,
                        formatDate=1,
                    )
                    if not bars:
                        results[symbol] = "no_bars"
                        continue
                    df = util.df(bars)
                    path = cache_dir / ("%s_%s_%s_RTH.csv" % (symbol, duration.replace(" ", ""), bar_size.replace(" ", "")))
                    df.to_csv(path, index=False)
                    results[symbol] = str(path)
                    ib.sleep(float(self.config.raw.get("pacing_seconds", 0.6)))
                except Exception as exc:
                    results[symbol] = "error:%s" % exc
        finally:
            if ib.isConnected():
                ib.disconnect()
        return results
