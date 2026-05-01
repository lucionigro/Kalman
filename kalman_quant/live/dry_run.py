from typing import Dict

import pandas as pd

from kalman_quant.config import AppConfig
from kalman_quant.execution import ExecutionEngine
from kalman_quant.models import DecisionRecord, OrderIntent, PortfolioSnapshot, utc_now_iso
from kalman_quant.portfolio import evaluate_entry, evaluate_risk_state, exposure_allows_new_position, volatility_position_size
from kalman_quant.research.factors import market_breadth, score_multifactor
from kalman_quant.storage import SQLiteStore


class DryRunCycle:
    def __init__(self, config: AppConfig, store: SQLiteStore, engine: ExecutionEngine):
        self.config = config
        self.store = store
        self.engine = engine

    def run_once(self, data: Dict[str, pd.DataFrame], cash: float = None) -> int:
        cash = float(cash if cash is not None else self.config.risk.get("initial_capital", 100000))
        benchmark = data.get(str(self.config.strategy.get("rs_benchmark", "SPY")))
        benchmark_symbol = str(self.config.strategy.get("rs_benchmark", "SPY"))
        breadth = market_breadth(data)
        risk_state = evaluate_risk_state(
            equity=cash,
            peak_equity=float(self.config.risk.get("initial_capital", cash)),
            risk=self.config.risk,
        )
        self.store.record_risk_state(risk_state)
        open_positions = 0
        intents = 0
        for symbol, raw in data.items():
            if symbol == benchmark_symbol:
                continue
            if raw is None or raw.empty:
                continue
            strategy_signal = score_multifactor(
                symbol,
                raw,
                benchmark,
                breadth=breadth,
                config=self.config.strategy,
            )
            self.store.record_strategy_signal(strategy_signal)
            if strategy_signal.signal != 1:
                continue
            if not exposure_allows_new_position(open_positions / max(1, int(self.config.risk.get("max_open_trades", 4))), risk_state, self.config.risk):
                self._record_rejected_decision(symbol, strategy_signal, risk_state, "risk_state_blocks_entry", ["risk_state"])
                continue
            decision = evaluate_entry(
                symbol,
                strategy_signal.price,
                cash,
                open_positions,
                False,
                strategy_signal.regime != "risk_off",
                self.config.risk,
            )
            if not decision.allowed:
                self._record_rejected_decision(symbol, strategy_signal, risk_state, "risk_checks_failed", decision.checks)
                continue
            qty = volatility_position_size(strategy_signal.price, cash, raw, self.config.risk)
            if qty < 1:
                self._record_rejected_decision(symbol, strategy_signal, risk_state, "volatility_size_zero", ["insufficient_size"])
                continue
            intent = OrderIntent(
                ticker=symbol,
                timestamp=utc_now_iso(),
                action="BUY",
                quantity=qty,
                estimated_price=strategy_signal.price,
                mode=self.config.mode,
                reason=strategy_signal.explanation,
                risk_checks=decision.checks,
            )
            event = self.engine.submit(intent)
            self.store.record_decision(
                DecisionRecord(
                    ticker=symbol,
                    timestamp=utc_now_iso(),
                    mode=self.config.mode,
                    action="BUY",
                    quantity=qty,
                    estimated_price=strategy_signal.price,
                    strategy_score=strategy_signal.score,
                    risk_state=risk_state.state,
                    approved=True,
                    reason=strategy_signal.explanation,
                    risk_checks=decision.checks,
                    signal=strategy_signal.to_dict(),
                    broker_result=event.to_dict(),
                )
            )
            cash -= qty * strategy_signal.price
            open_positions += 1
            intents += 1
        snapshot = PortfolioSnapshot(
            timestamp=utc_now_iso(),
            cash=cash,
            equity=cash,
            positions_value=0.0,
            pnl=cash - float(self.config.risk.get("initial_capital", 100000)),
            exposure=0.0,
            positions={},
        )
        self.store.record_snapshot(snapshot)
        return intents

    def _record_rejected_decision(self, symbol, strategy_signal, risk_state, reason, checks) -> None:
        self.store.record_decision(
            DecisionRecord(
                ticker=symbol,
                timestamp=utc_now_iso(),
                mode=self.config.mode,
                action="BUY",
                quantity=0,
                estimated_price=strategy_signal.price,
                strategy_score=strategy_signal.score,
                risk_state=risk_state.state,
                approved=False,
                reason=reason,
                risk_checks=checks,
                signal=strategy_signal.to_dict(),
            )
        )

    def _market_uptrend(self, benchmark):
        if benchmark is None or benchmark.empty:
            return True
        window = int(self.config.risk.get("sma_uptrend_len", 200))
        if len(benchmark) < window:
            return True
        close = benchmark["close"]
        return float(close.iloc[-1]) > float(close.rolling(window).mean().iloc[-1])
