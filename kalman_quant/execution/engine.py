from typing import Any, Optional

from kalman_quant.config import AppConfig
from kalman_quant.models import ExecutionEvent, OrderIntent, utc_now_iso
from kalman_quant.storage import SQLiteStore


class ExecutionEngine:
    def __init__(self, config: AppConfig, store: SQLiteStore, broker: Optional[Any] = None):
        self.config = config
        self.store = store
        self.broker = broker

    def submit(self, intent: OrderIntent) -> ExecutionEvent:
        self.store.record_intent(intent)
        if self.config.mode == "dry_run":
            event = ExecutionEvent(
                ticker=intent.ticker,
                timestamp=utc_now_iso(),
                action=intent.action,
                quantity=intent.quantity,
                mode=self.config.mode,
                status="dry_run_recorded",
                message="OrderIntent recorded; no broker call made.",
            )
            self.store.record_execution(event)
            return event
        if self.config.mode == "paper":
            if self.broker is None:
                event = ExecutionEvent(
                    ticker=intent.ticker,
                    timestamp=utc_now_iso(),
                    action=intent.action,
                    quantity=intent.quantity,
                    mode=self.config.mode,
                    status="rejected",
                    message="Paper mode requires a broker adapter.",
                )
                self.store.record_execution(event)
                return event
            result = self.broker.place_order(intent)
            event = ExecutionEvent(
                ticker=intent.ticker,
                timestamp=utc_now_iso(),
                action=intent.action,
                quantity=intent.quantity,
                mode=self.config.mode,
                status=str(result.get("status", "submitted")),
                broker_order_id=str(result.get("order_id", "")) or None,
                message=str(result.get("message", "")),
            )
            self.store.record_execution(event)
            return event
        raise RuntimeError("Unsupported execution mode: %s" % self.config.mode)
