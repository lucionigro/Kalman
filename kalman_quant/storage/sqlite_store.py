import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from kalman_quant.models import (
    BrokerHealth,
    DataQualityReport,
    DecisionRecord,
    ExecutionEvent,
    OrderIntent,
    PortfolioSnapshot,
    RiskState,
    SignalEvent,
    StrategySignal,
    UniverseSnapshot,
)


class SQLiteStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self.init_schema()

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quant_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                ticker TEXT,
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quant_order_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                estimated_price REAL NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                risk_checks TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quant_portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                cash REAL NOT NULL,
                equity REAL NOT NULL,
                positions_value REAL NOT NULL,
                pnl REAL NOT NULL,
                exposure REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quant_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                mode TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                estimated_price REAL NOT NULL,
                strategy_score REAL NOT NULL,
                risk_state TEXT NOT NULL,
                approved INTEGER NOT NULL,
                reason TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quant_state (
                key TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def record_signal(self, event: SignalEvent) -> None:
        self._record_event("signal", event.ticker, event.timestamp, event.to_dict())

    def record_execution(self, event: ExecutionEvent) -> None:
        self._record_event("execution", event.ticker, event.timestamp, event.to_dict())

    def record_strategy_signal(self, event: StrategySignal) -> None:
        self._record_event("strategy_signal", event.ticker, event.timestamp, event.to_dict())

    def record_data_quality(self, report: DataQualityReport) -> None:
        self._record_event("data_quality", report.ticker, report.timestamp, report.to_dict())

    def record_universe(self, snapshot: UniverseSnapshot) -> None:
        self._put_state("universe:%s" % snapshot.name, snapshot.timestamp, snapshot.to_dict())
        self._record_event("universe", None, snapshot.timestamp, snapshot.to_dict())

    def record_risk_state(self, state: RiskState) -> None:
        self._put_state("risk_state", state.timestamp, state.to_dict())
        self._record_event("risk_state", None, state.timestamp, state.to_dict())

    def record_broker_health(self, health: BrokerHealth) -> None:
        self._put_state("broker_health", health.timestamp, health.to_dict())
        self._record_event("broker_health", None, health.timestamp, health.to_dict())

    def record_decision(self, decision: DecisionRecord) -> None:
        payload = decision.to_dict()
        self.conn.execute(
            """
            INSERT INTO quant_decisions
            (ticker, timestamp, mode, action, quantity, estimated_price, strategy_score, risk_state, approved, reason, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision.ticker,
                decision.timestamp,
                decision.mode,
                decision.action,
                decision.quantity,
                decision.estimated_price,
                decision.strategy_score,
                decision.risk_state,
                1 if decision.approved else 0,
                decision.reason,
                json.dumps(payload),
            ),
        )
        self.conn.commit()

    def record_intent(self, intent: OrderIntent) -> None:
        payload = intent.to_dict()
        self.conn.execute(
            """
            INSERT INTO quant_order_intents
            (ticker, timestamp, action, quantity, estimated_price, mode, status, reason, risk_checks, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                intent.ticker,
                intent.timestamp,
                intent.action,
                intent.quantity,
                intent.estimated_price,
                intent.mode,
                intent.status,
                intent.reason,
                json.dumps(intent.risk_checks),
                json.dumps(payload),
            ),
        )
        self.conn.commit()

    def record_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        payload = snapshot.to_dict()
        self.conn.execute(
            """
            INSERT INTO quant_portfolio_snapshots
            (timestamp, cash, equity, positions_value, pnl, exposure, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp,
                snapshot.cash,
                snapshot.equity,
                snapshot.positions_value,
                snapshot.pnl,
                snapshot.exposure,
                json.dumps(payload),
            ),
        )
        self.conn.commit()

    def _record_event(self, event_type: str, ticker: Optional[str], timestamp: str, payload: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO quant_events (event_type, ticker, timestamp, payload) VALUES (?, ?, ?, ?)",
            (event_type, ticker, timestamp, json.dumps(payload)),
        )
        self.conn.commit()

    def _put_state(self, key: str, timestamp: str, payload: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO quant_state (key, timestamp, payload)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET timestamp=excluded.timestamp, payload=excluded.payload
            """,
            (key, timestamp, json.dumps(payload)),
        )
        self.conn.commit()

    def latest_order_intents(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self._rows(
            "SELECT * FROM quant_order_intents ORDER BY id DESC LIMIT ?",
            (limit,),
        )

    def latest_snapshots(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self._rows(
            "SELECT * FROM quant_portfolio_snapshots ORDER BY id DESC LIMIT ?",
            (limit,),
        )

    def latest_events(self, event_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        if event_type:
            return self._rows(
                "SELECT * FROM quant_events WHERE event_type=? ORDER BY id DESC LIMIT ?",
                (event_type, limit),
            )
        return self._rows("SELECT * FROM quant_events ORDER BY id DESC LIMIT ?", (limit,))

    def latest_decisions(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self._rows("SELECT * FROM quant_decisions ORDER BY id DESC LIMIT ?", (limit,))

    def get_state(self, key: str) -> Optional[Dict[str, Any]]:
        rows = self._rows("SELECT * FROM quant_state WHERE key=?", (key,))
        if not rows:
            return None
        row = rows[0]
        return {"key": row["key"], "timestamp": row["timestamp"], "payload": json.loads(row["payload"])}

    def latest_state(self, prefix: str, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self._rows(
            "SELECT * FROM quant_state WHERE key LIKE ? ORDER BY timestamp DESC LIMIT ?",
            (prefix + "%", limit),
        )
        for row in rows:
            row["payload"] = json.loads(row["payload"])
        return rows

    def _rows(self, sql: str, params: Iterable = ()) -> List[Dict[str, Any]]:
        cur = self.conn.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]
