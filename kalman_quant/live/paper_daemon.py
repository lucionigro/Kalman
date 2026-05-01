import time
from datetime import datetime, time as dtime
from typing import Dict

import pytz

from kalman_quant.data import LocalDataProvider
from kalman_quant.execution import ExecutionEngine
from kalman_quant.execution.ibkr_broker import IBKRBroker
from kalman_quant.live import DryRunCycle
from kalman_quant.storage import SQLiteStore


class PaperDaemon:
    def __init__(self, config):
        if config.mode != "paper":
            raise RuntimeError("paper-daemon requires config mode=paper")
        self.config = config
        self.provider = LocalDataProvider(config.raw.get("data_cache_dir", "data_cache"), config.raw.get("backtest_cache_dir", "backtest_cache"))

    def run_forever(self, interval_minutes: int = 15) -> None:
        tz = pytz.timezone("US/Eastern")
        while True:
            now = datetime.now(tz)
            if now.weekday() < 5 and dtime(8, 45) <= now.time() <= dtime(16, 5):
                self.run_once()
                time.sleep(interval_minutes * 60)
            else:
                time.sleep(300)

    def run_once(self) -> int:
        data = self.provider.load_many(self.config.symbols)
        store = SQLiteStore(self.config.db_path)
        broker = IBKRBroker(self.config)
        try:
            engine = ExecutionEngine(self.config, store, broker=broker)
            return DryRunCycle(self.config, store, engine).run_once(data)
        finally:
            broker.disconnect()
            store.close()
