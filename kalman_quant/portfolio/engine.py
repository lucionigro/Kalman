from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from kalman_quant.models import RiskState, utc_now_iso


@dataclass
class PortfolioRiskLimits:
    target_volatility_pct: float = 12.0
    max_drawdown_pct: float = 0.15
    reduce_drawdown_pct: float = 0.10
    max_exposure_pct: float = 0.80
    max_single_name_pct: float = 0.08
    max_daily_loss_pct: float = 0.03
    max_weekly_loss_pct: float = 0.06
    max_correlated_positions: int = 2


def limits_from_config(risk: Dict) -> PortfolioRiskLimits:
    return PortfolioRiskLimits(
        target_volatility_pct=float(risk.get("target_volatility_pct", 12.0)),
        max_drawdown_pct=float(risk.get("max_drawdown_pct", 0.15)),
        reduce_drawdown_pct=float(risk.get("reduce_drawdown_pct", 0.10)),
        max_exposure_pct=float(risk.get("max_exposure_pct", 0.80)),
        max_single_name_pct=float(risk.get("max_single_name_pct", 0.08)),
        max_daily_loss_pct=float(risk.get("max_daily_loss_pct", 0.03)),
        max_weekly_loss_pct=float(risk.get("max_weekly_loss_pct", 0.06)),
        max_correlated_positions=int(risk.get("max_correlated_positions", 2)),
    )


def evaluate_risk_state(
    equity: float,
    peak_equity: float,
    daily_pnl_pct: float = 0.0,
    weekly_pnl_pct: float = 0.0,
    risk: Dict = None,
) -> RiskState:
    limits = limits_from_config(risk or {})
    peak = max(float(peak_equity or equity), float(equity))
    drawdown = 0.0 if peak <= 0 else float(equity / peak - 1.0)
    reasons: List[str] = []
    state = "normal"
    if drawdown <= -limits.max_drawdown_pct:
        state = "halted"
        reasons.append("max_drawdown")
    elif daily_pnl_pct <= -limits.max_daily_loss_pct:
        state = "halted"
        reasons.append("max_daily_loss")
    elif weekly_pnl_pct <= -limits.max_weekly_loss_pct:
        state = "risk_off"
        reasons.append("max_weekly_loss")
    elif drawdown <= -limits.reduce_drawdown_pct:
        state = "reduced"
        reasons.append("drawdown_reduce")
    if not reasons:
        reasons.append("ok")
    return RiskState(
        timestamp=utc_now_iso(),
        state=state,
        equity=float(equity),
        peak_equity=peak,
        drawdown_pct=drawdown,
        daily_pnl_pct=float(daily_pnl_pct),
        weekly_pnl_pct=float(weekly_pnl_pct),
        reasons=reasons,
    )


def volatility_position_size(price: float, equity: float, df: pd.DataFrame, risk: Dict) -> int:
    if price <= 0 or equity <= 0:
        return 0
    limits = limits_from_config(risk)
    returns = df["close"].astype(float).pct_change().tail(20)
    daily_vol = float(returns.std()) if len(returns.dropna()) else 0.0
    annual_vol = daily_vol * (252 ** 0.5)
    if annual_vol <= 0:
        annual_vol = limits.target_volatility_pct / 100.0
    target_name_risk = (limits.target_volatility_pct / 100.0) / max(annual_vol, 0.01) * 0.04
    target_value = equity * max(0.0, min(limits.max_single_name_pct, target_name_risk))
    budget_cap = float(risk.get("budget_per_trade", target_value))
    target_value = min(target_value, budget_cap)
    return int(target_value // price)


def exposure_allows_new_position(current_exposure: float, risk_state: RiskState, risk: Dict) -> bool:
    limits = limits_from_config(risk)
    if risk_state.state in {"halted", "risk_off"}:
        return False
    cap = limits.max_exposure_pct * (0.5 if risk_state.state == "reduced" else 1.0)
    return current_exposure < cap
