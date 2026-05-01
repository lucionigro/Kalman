import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from kalman_quant.config import AppConfig
from kalman_quant.portfolio import evaluate_risk_state, exposure_allows_new_position, volatility_position_size
from kalman_quant.research.factors import market_breadth, score_multifactor
from kalman_quant.research.signals import add_kalman_supertrend_signals


@dataclass
class BacktestResult:
    run_dir: Path
    metrics: Dict[str, float]
    trades: pd.DataFrame
    equity: pd.DataFrame


class QuantBacktester:
    def __init__(self, config: AppConfig, data: Dict[str, pd.DataFrame]):
        self.config = config
        self.data = {
            symbol: add_kalman_supertrend_signals(df, config.strategy)
            for symbol, df in data.items()
            if df is not None and not df.empty
        }
        self.risk = config.risk
        self.execution = config.execution
        self.initial_capital = float(self.risk.get("initial_capital", 100000))
        self.cash = self.initial_capital
        self.positions = {}
        self.trades = []
        self.equity_rows = []
        self.decision_rows = []
        self.peak_equity = self.initial_capital

    def run(self, run_name: Optional[str] = None) -> BacktestResult:
        if not self.data:
            raise RuntimeError("No data loaded for backtest")
        dates = sorted(set().union(*[set(df.index) for df in self.data.values()]))
        if not dates:
            raise RuntimeError("No trading dates available")
        for idx, current_date in enumerate(dates):
            self._process_exits(current_date)
            self._process_entries(current_date)
            self._snapshot(current_date)
        self._close_remaining(dates[-1])
        return self._persist(run_name)

    def _process_exits(self, current_date) -> None:
        for symbol in list(self.positions.keys()):
            if symbol not in self.data or current_date not in self.data[symbol].index:
                continue
            row = self.data[symbol].loc[current_date]
            pos = self.positions[symbol]
            close_px = float(row["close"])
            max_loss_hit = close_px <= pos["entry_price"] * (1.0 - float(self.risk.get("max_loss_pct", 0.05)))
            trend_exit = int(row.get("signal", 0)) == -1 or int(row.get("direction", 0)) == 1
            if max_loss_hit or trend_exit:
                reason = "MAX_LOSS" if max_loss_hit else "TREND_EXIT"
                self._sell(symbol, current_date, close_px, reason)

    def _process_entries(self, current_date) -> None:
        if len(self.positions) >= int(self.risk.get("max_open_trades", 4)):
            return
        if not self._market_uptrend(current_date):
            return
        candidates = []
        benchmark = self._benchmark_slice(current_date)
        current_equity = self._current_equity(current_date)
        self.peak_equity = max(self.peak_equity, current_equity)
        risk_state = evaluate_risk_state(current_equity, self.peak_equity, risk=self.risk)
        current_exposure = self._current_exposure(current_date)
        if not exposure_allows_new_position(current_exposure, risk_state, self.risk):
            return
        breadth = market_breadth({s: df.loc[:current_date] for s, df in self.data.items() if current_date in df.index})
        for symbol, df in self.data.items():
            if symbol in self.positions or current_date not in df.index:
                continue
            row = df.loc[current_date]
            if float(row["close"]) < float(self.risk.get("min_price", 0)):
                continue
            if "dollar_volume" in df.columns and float(row.get("dollar_volume", 0)) < float(self.risk.get("adv_min_usd", 0)):
                continue
            strategy_signal = score_multifactor(symbol, df.loc[:current_date], benchmark, breadth, self.config.strategy)
            if strategy_signal.signal != 1:
                continue
            candidates.append((symbol, strategy_signal))
        candidates.sort(key=lambda x: x[1].score, reverse=True)
        for symbol, strategy_signal in candidates:
            if len(self.positions) >= int(self.risk.get("max_open_trades", 4)):
                break
            row = self.data[symbol].loc[current_date]
            price = float(row["close"])
            qty = volatility_position_size(price, current_equity, self.data[symbol].loc[:current_date], self.risk)
            if qty < 1:
                continue
            slippage = self._slippage(price)
            entry = price + slippage
            total_cost = qty * entry + float(self.execution.get("commission_open", 0))
            if total_cost > self.cash:
                continue
            self.cash -= total_cost
            self.positions[symbol] = {
                "entry_date": str(current_date),
                "entry_price": entry,
                "quantity": qty,
                "score": strategy_signal.score,
            }
            self.decision_rows.append(
                {
                    "date": current_date,
                    "ticker": symbol,
                    "approved": True,
                    "action": "BUY",
                    "quantity": qty,
                    "estimated_price": entry,
                    "strategy_score": strategy_signal.score,
                    "risk_state": risk_state.state,
                    "reason": strategy_signal.explanation,
                }
            )
            self.trades.append(
                {
                    "date": current_date,
                    "ticker": symbol,
                    "action": "BUY",
                    "price": entry,
                    "quantity": qty,
                    "score": strategy_signal.score,
                    "reason": "KALMAN_SIGNAL_RS",
                    "pnl": 0.0,
                    "return_pct": 0.0,
                }
            )

    def _sell(self, symbol: str, current_date, close_px: float, reason: str) -> None:
        pos = self.positions.pop(symbol)
        exit_price = close_px - self._slippage(close_px)
        qty = int(pos["quantity"])
        gross = (exit_price - float(pos["entry_price"])) * qty
        pnl = gross - float(self.execution.get("commission_close", 0))
        ret = (exit_price / float(pos["entry_price"]) - 1.0) * 100.0
        self.cash += qty * exit_price - float(self.execution.get("commission_close", 0))
        self.trades.append(
            {
                "date": current_date,
                "ticker": symbol,
                "action": "SELL",
                "price": exit_price,
                "quantity": qty,
                "score": pos.get("score", 0.0),
                "reason": reason,
                "pnl": pnl,
                "return_pct": ret,
            }
        )

    def _snapshot(self, current_date) -> None:
        positions_value = 0.0
        for symbol, pos in self.positions.items():
            df = self.data.get(symbol)
            if df is None or current_date not in df.index:
                continue
            positions_value += int(pos["quantity"]) * float(df.loc[current_date]["close"])
        equity = self.cash + positions_value
        self.peak_equity = max(self.peak_equity, equity)
        exposure = positions_value / equity if equity else 0.0
        self.equity_rows.append(
            {
                "date": current_date,
                "cash": self.cash,
                "positions_value": positions_value,
                "total_value": equity,
                "pnl": equity - self.initial_capital,
                "exposure": exposure,
                "num_positions": len(self.positions),
            }
        )

    def _close_remaining(self, current_date) -> None:
        for symbol in list(self.positions.keys()):
            df = self.data.get(symbol)
            if df is not None and current_date in df.index:
                self._sell(symbol, current_date, float(df.loc[current_date]["close"]), "END_OF_BACKTEST")

    def _current_equity(self, current_date) -> float:
        value = self.cash
        for symbol, pos in self.positions.items():
            df = self.data.get(symbol)
            if df is not None and current_date in df.index:
                value += int(pos["quantity"]) * float(df.loc[current_date]["close"])
        return value

    def _current_exposure(self, current_date) -> float:
        equity = self._current_equity(current_date)
        if equity <= 0:
            return 0.0
        positions_value = 0.0
        for symbol, pos in self.positions.items():
            df = self.data.get(symbol)
            if df is not None and current_date in df.index:
                positions_value += int(pos["quantity"]) * float(df.loc[current_date]["close"])
        return positions_value / equity

    def _market_uptrend(self, current_date) -> bool:
        if not bool(self.risk.get("require_market_uptrend", True)):
            return True
        benchmark = self.data.get(str(self.config.strategy.get("rs_benchmark", "SPY")))
        if benchmark is None or current_date not in benchmark.index:
            return True
        window = int(self.risk.get("sma_uptrend_len", 200))
        close = benchmark.loc[:current_date]["close"]
        if len(close) < window:
            return True
        return float(close.iloc[-1]) > float(close.rolling(window).mean().iloc[-1])

    def _benchmark_slice(self, current_date):
        benchmark = self.data.get(str(self.config.strategy.get("rs_benchmark", "SPY")))
        if benchmark is None:
            return None
        return benchmark.loc[:current_date]

    def _slippage(self, price: float) -> float:
        per_share = float(self.execution.get("slippage_per_share", 0))
        pct = float(self.execution.get("slippage_pct", 0))
        return max(per_share, price * pct)

    def _persist(self, run_name: Optional[str]) -> BacktestResult:
        stamp = run_name or datetime.now().strftime("%Y-%m-%d_%H%M%S")
        run_dir = Path(self.config.runs_dir) / stamp
        run_dir.mkdir(parents=True, exist_ok=True)
        trades_df = pd.DataFrame(self.trades)
        equity_df = pd.DataFrame(self.equity_rows)
        decisions_df = pd.DataFrame(self.decision_rows)
        metrics = calculate_metrics(equity_df, trades_df, self.initial_capital)
        trades_df.to_csv(run_dir / "trades.csv", index=False)
        equity_df.to_csv(run_dir / "equity.csv", index=False)
        decisions_df.to_csv(run_dir / "decisions.csv", index=False)
        pd.DataFrame().to_csv(run_dir / "fills.csv", index=False)
        with (run_dir / "risk.json").open("w", encoding="utf-8") as fh:
            json.dump({"risk": self.risk, "peak_equity": self.peak_equity}, fh, indent=2, default=str)
        with (run_dir / "config.json").open("w", encoding="utf-8") as fh:
            json.dump(self.config.raw, fh, indent=2, default=str)
        with (run_dir / "summary.json").open("w", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2, default=str)
        return BacktestResult(run_dir=run_dir, metrics=metrics, trades=trades_df, equity=equity_df)


def calculate_metrics(equity_df: pd.DataFrame, trades_df: pd.DataFrame, initial_capital: float) -> Dict[str, float]:
    if equity_df.empty:
        return {}
    final_value = float(equity_df["total_value"].iloc[-1])
    total_return = (final_value / initial_capital - 1.0) * 100.0
    returns = equity_df["total_value"].pct_change().dropna()
    days = max(1, len(equity_df))
    cagr = ((final_value / initial_capital) ** (252.0 / days) - 1.0) * 100.0
    sharpe = float(returns.mean() / returns.std() * math.sqrt(252)) if len(returns) > 1 and returns.std() else 0.0
    downside = returns[returns < 0]
    sortino = float(returns.mean() / downside.std() * math.sqrt(252)) if len(downside) > 1 and downside.std() else 0.0
    curve = equity_df["total_value"]
    dd = (curve / curve.cummax() - 1.0) * 100.0
    max_dd = float(dd.min()) if len(dd) else 0.0
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else 0.0
    sells = trades_df[trades_df["action"] == "SELL"] if not trades_df.empty else pd.DataFrame()
    wins = sells[sells["pnl"] > 0] if not sells.empty else pd.DataFrame()
    losses = sells[sells["pnl"] <= 0] if not sells.empty else pd.DataFrame()
    gross_profit = float(wins["pnl"].sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses["pnl"].sum())) if not losses.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    win_rate = float(len(wins) / len(sells) * 100.0) if len(sells) else 0.0
    avg_win = float(wins["return_pct"].mean()) if not wins.empty else 0.0
    avg_loss = float(losses["return_pct"].mean()) if not losses.empty else 0.0
    expectancy = (win_rate / 100.0 * avg_win) + ((100.0 - win_rate) / 100.0 * avg_loss) if len(sells) else 0.0
    exposure = float(equity_df["exposure"].mean() * 100.0)
    turnover = float(len(trades_df) / max(1, days) * 252.0)
    return {
        "initial_capital": initial_capital,
        "final_value": final_value,
        "total_return_pct": total_return,
        "cagr_pct": cagr,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown_pct": max_dd,
        "calmar": calmar,
        "total_trades": int(len(sells)),
        "win_rate_pct": win_rate,
        "profit_factor": profit_factor,
        "expectancy_pct": expectancy,
        "avg_exposure_pct": exposure,
        "annualized_turnover": turnover,
    }
