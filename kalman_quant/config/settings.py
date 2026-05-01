import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - exercised in minimal envs
    yaml = None


DEFAULT_CONFIG = "config/dry_run.yaml"


@dataclass
class AppConfig:
    raw: Dict[str, Any]
    path: Path

    @property
    def profile(self) -> str:
        return str(self.raw.get("profile", "dry_run"))

    @property
    def mode(self) -> str:
        return str(self.raw.get("mode", self.profile))

    @property
    def db_path(self) -> str:
        return str(self.raw.get("db_path", "trades_live.db"))

    @property
    def runs_dir(self) -> str:
        return str(self.raw.get("runs_dir", "runs"))

    @property
    def symbols(self) -> List[str]:
        return list(self.raw.get("universe", {}).get("symbols", []))

    @property
    def strategy(self) -> Dict[str, Any]:
        return dict(self.raw.get("strategy", {}))

    @property
    def risk(self) -> Dict[str, Any]:
        return dict(self.raw.get("risk", {}))

    @property
    def execution(self) -> Dict[str, Any]:
        return dict(self.raw.get("execution", {}))

    @property
    def ibkr(self) -> Dict[str, Any]:
        return dict(self.raw.get("ibkr", {}))

    def require_safe_mode(self) -> None:
        if self.mode == "live" and not bool(self.raw.get("enabled", False)):
            reason = self.raw.get("blocked_reason", "live mode is disabled")
            raise RuntimeError(str(reason))


def _coerce_env_value(value: str, current: Any) -> Any:
    if isinstance(current, bool):
        return value.lower() in {"1", "true", "yes", "on"}
    if isinstance(current, int):
        return int(value)
    if isinstance(current, float):
        return float(value)
    return value


def _apply_env_overrides(raw: Dict[str, Any]) -> Dict[str, Any]:
    overrides = {
        ("profile",): "KALMAN_PROFILE",
        ("mode",): "KALMAN_MODE",
        ("db_path",): "KALMAN_DB_PATH",
        ("runs_dir",): "KALMAN_RUNS_DIR",
        ("ibkr", "host"): "KALMAN_IB_HOST",
        ("ibkr", "port"): "KALMAN_IB_PORT",
        ("ibkr", "client_id"): "KALMAN_IB_CLIENT_ID",
        ("ibkr", "account_id"): "KALMAN_ACCOUNT_ID",
    }
    for path, env_key in overrides.items():
        if env_key not in os.environ:
            continue
        node = raw
        for key in path[:-1]:
            node = node.setdefault(key, {})
        leaf = path[-1]
        node[leaf] = _coerce_env_value(os.environ[env_key], node.get(leaf, ""))
    return raw


def load_config(path: str = None) -> AppConfig:
    config_path = Path(path or os.environ.get("KALMAN_CONFIG", DEFAULT_CONFIG))
    if not config_path.exists():
        raise FileNotFoundError("Config not found: %s" % config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        if yaml is not None:
            raw = yaml.safe_load(fh) or {}
        else:
            text = fh.read()
            raw = json.loads(text) if text.lstrip().startswith("{") else _load_simple_yaml(text)
    raw = _apply_env_overrides(raw)
    cfg = AppConfig(raw=raw, path=config_path)
    cfg.require_safe_mode()
    return cfg


def _load_simple_yaml(text: str) -> Dict[str, Any]:
    root: Dict[str, Any] = {}
    stack = [(-1, root)]
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key_value = line.strip()
        if ":" not in key_value:
            continue
        key, value = key_value.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child: Dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_scalar(value)
    return root


def _parse_scalar(value: str) -> Any:
    if value.startswith("[") and value.endswith("]"):
        body = value[1:-1].strip()
        if not body:
            return []
        return [_parse_scalar(part.strip()) for part in body.split(",")]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    try:
        if any(ch in value for ch in [".", "e", "E"]):
            return float(value)
        return int(value)
    except ValueError:
        return value
