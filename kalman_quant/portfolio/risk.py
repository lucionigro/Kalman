from dataclasses import dataclass
from typing import Dict


@dataclass
class RiskDecision:
    allowed: bool
    checks: list


def position_size(price: float, available_cash: float, risk: Dict) -> int:
    if price <= 0:
        return 0
    budget = min(float(risk.get("budget_per_trade", 0)), max(0.0, available_cash))
    return int(budget // price)


def evaluate_entry(
    ticker: str,
    price: float,
    available_cash: float,
    open_positions: int,
    already_open: bool,
    market_uptrend: bool,
    risk: Dict,
) -> RiskDecision:
    checks = []
    if already_open:
        checks.append("already_open")
    if open_positions >= int(risk.get("max_open_trades", 0)):
        checks.append("max_open_trades")
    if price < float(risk.get("min_price", 0)):
        checks.append("min_price")
    if not market_uptrend and bool(risk.get("require_market_uptrend", True)):
        checks.append("market_regime")
    qty = position_size(price, available_cash, risk)
    if qty < 1:
        checks.append("insufficient_cash")
    return RiskDecision(allowed=len(checks) == 0, checks=checks or ["ok"])
