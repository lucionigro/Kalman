from pathlib import Path
from typing import Dict, List

from kalman_quant.models import BrokerHealth, utc_now_iso


def health_check(config, data_symbols=None) -> Dict:
    errors: List[str] = []
    warnings: List[str] = []
    db_path = Path(config.db_path)
    if not db_path.exists():
        warnings.append("db_missing")
    runs_dir = Path(config.runs_dir)
    if not runs_dir.exists():
        warnings.append("runs_dir_missing")
    if config.mode == "live" and not bool(config.raw.get("enabled", False)):
        errors.append("live_blocked")
    if not config.ibkr.get("account_id"):
        warnings.append("ibkr_account_id_empty")
    if data_symbols is not None and not data_symbols:
        errors.append("no_data_loaded")
    status = "ok" if not errors else "bad"
    return {
        "timestamp": utc_now_iso(),
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "profile": config.profile,
        "mode": config.mode,
    }


def broker_health_from_config(config, connected: bool = False, errors=None) -> BrokerHealth:
    return BrokerHealth(
        timestamp=utc_now_iso(),
        connected=connected,
        mode=config.mode,
        market_data_type=str(config.ibkr.get("market_data_type", "")),
        account_id=str(config.ibkr.get("account_id", "")),
        errors=list(errors or []),
    )
