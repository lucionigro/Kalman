import json
from pathlib import Path
from typing import Dict, Tuple


def evaluate_promotion(metrics: Dict, gates: Dict = None) -> Tuple[bool, Dict]:
    gates = gates or {}
    min_sharpe = float(gates.get("min_sharpe", 0.0))
    max_drawdown = float(gates.get("max_drawdown_pct", -15.0))
    min_profit_factor = float(gates.get("min_profit_factor", 1.2))
    min_trades = int(gates.get("min_trades", 20))
    checks = {
        "sharpe_positive": float(metrics.get("sharpe", 0.0)) > min_sharpe,
        "drawdown_ok": float(metrics.get("max_drawdown_pct", 0.0)) >= max_drawdown,
        "profit_factor_ok": float(metrics.get("profit_factor", 0.0)) >= min_profit_factor,
        "enough_trades": int(metrics.get("total_trades", 0)) >= min_trades,
    }
    approved = all(checks.values())
    return approved, checks


def write_promotion_report(run_dir: str, gates: Dict = None) -> Path:
    path = Path(run_dir)
    summary = path / "summary.json"
    if not summary.exists():
        raise FileNotFoundError("summary.json not found in %s" % path)
    metrics = json.loads(summary.read_text(encoding="utf-8"))
    approved, checks = evaluate_promotion(metrics, gates)
    report = path / "promotion_report.md"
    lines = [
        "# Promotion Report",
        "",
        "Approved: `%s`" % approved,
        "",
        "## Checks",
    ]
    for key, value in checks.items():
        lines.append("- `%s`: %s" % (key, value))
    lines += ["", "## Metrics"]
    for key, value in metrics.items():
        lines.append("- `%s`: %s" % (key, value))
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report
