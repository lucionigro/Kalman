from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SignalEvent:
    ticker: str
    timestamp: str
    signal: int
    score: float
    price: float
    market_regime: str
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OrderIntent:
    ticker: str
    timestamp: str
    action: str
    quantity: int
    estimated_price: float
    mode: str
    reason: str
    risk_checks: List[str] = field(default_factory=list)
    status: str = "created"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionEvent:
    ticker: str
    timestamp: str
    action: str
    quantity: int
    mode: str
    status: str
    broker_order_id: Optional[str] = None
    fill_price: Optional[float] = None
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioSnapshot:
    timestamp: str
    cash: float
    equity: float
    positions_value: float
    pnl: float
    exposure: float
    positions: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DataQualityReport:
    ticker: str
    timestamp: str
    rows: int
    start: str
    end: str
    missing_ohlcv_rows: int
    stale: bool
    gap_count: int
    split_warning: bool
    dividend_gap_warning: bool
    status: str
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class UniverseSnapshot:
    timestamp: str
    name: str
    symbols: List[str]
    rejected: Dict[str, str] = field(default_factory=dict)
    source: str = "ibkr_cache"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StrategySignal:
    ticker: str
    timestamp: str
    signal: int
    score: float
    confidence: float
    price: float
    regime: str
    components: Dict[str, float]
    explanation: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RiskState:
    timestamp: str
    state: str
    equity: float
    peak_equity: float
    drawdown_pct: float
    daily_pnl_pct: float = 0.0
    weekly_pnl_pct: float = 0.0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioTarget:
    ticker: str
    action: str
    quantity: int
    target_value: float
    risk_state: str
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BrokerHealth:
    timestamp: str
    connected: bool
    mode: str
    market_data_type: str
    account_id: str
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DecisionRecord:
    ticker: str
    timestamp: str
    mode: str
    action: str
    quantity: int
    estimated_price: float
    strategy_score: float
    risk_state: str
    approved: bool
    reason: str
    risk_checks: List[str] = field(default_factory=list)
    signal: Dict[str, Any] = field(default_factory=dict)
    broker_result: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
