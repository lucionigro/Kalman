from typing import Dict

from ib_insync import IB, MarketOrder, Stock

from kalman_quant.config import AppConfig
from kalman_quant.models import OrderIntent


class IBKRBroker:
    def __init__(self, config: AppConfig):
        self.config = config
        self.ib = IB()

    def connect(self) -> None:
        ibkr = self.config.ibkr
        self.ib.connect(
            ibkr.get("host", "127.0.0.1"),
            int(ibkr.get("port", 7497)),
            clientId=int(ibkr.get("client_id", 41)),
        )
        self.ib.reqMarketDataType(int(ibkr.get("market_data_type", 3)))

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()

    def place_order(self, intent: OrderIntent) -> Dict:
        if not self.ib.isConnected():
            self.connect()
        contract = Stock(intent.ticker, "SMART", "USD")
        qualified = self.ib.qualifyContracts(contract)
        if not qualified:
            return {"status": "rejected", "message": "contract qualification failed"}
        order = MarketOrder(intent.action, intent.quantity)
        account = self.config.ibkr.get("account_id")
        if account:
            order.account = account
        trade = self.ib.placeOrder(qualified[0], order)
        return {
            "status": "submitted",
            "order_id": getattr(trade.order, "orderId", ""),
            "message": "submitted to IBKR paper",
        }
