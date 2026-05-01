import json
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal
from textual.widgets import DataTable, Footer, Header, Static

from kalman_quant.config import load_config
from kalman_quant.storage import SQLiteStore


class BloombergTerminal(App):
    CSS = """
    Screen {
        background: #050b0f;
        color: #d7f9ff;
    }
    #topline {
        background: #071820;
        color: #62e7ff;
        height: 3;
        padding: 0 1;
    }
    .panel {
        border: solid #164b5d;
        padding: 0 1;
    }
    #overview {
        height: 9;
    }
    #signals, #orders, #risk, #runs, #research, #universe, #broker, #decisions {
        height: 1fr;
    }
    """
    BINDINGS = [
        ("r", "refresh", "Refresh"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, config_path=None):
        super().__init__()
        self.config = load_config(config_path)
        self.store = SQLiteStore(self.config.db_path)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("KALMAN QUANT TERMINAL | DRY-RUN/PAPER OBSERVABILITY | LIVE REAL BLOQUEADO", id="topline")
        yield Container(
            Static(id="overview", classes="panel"),
            Horizontal(
                DataTable(id="signals", classes="panel"),
                DataTable(id="decisions", classes="panel"),
            ),
            Horizontal(
                DataTable(id="risk", classes="panel"),
                DataTable(id="universe", classes="panel"),
            ),
            Horizontal(
                DataTable(id="research", classes="panel"),
                DataTable(id="broker", classes="panel"),
            ),
            DataTable(id="runs", classes="panel"),
        )
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(5, self.refresh_data)
        self.refresh_data()

    def action_refresh(self) -> None:
        self.refresh_data()

    def refresh_data(self) -> None:
        snapshots = self.store.latest_snapshots(1)
        latest = snapshots[0] if snapshots else None
        overview = self.query_one("#overview", Static)
        if latest:
            overview.update(
                "PROFILE=%s MODE=%s DB=%s\nEQUITY %.2f | CASH %.2f | PNL %.2f | EXPOSURE %.2f%%"
                % (
                    self.config.profile.upper(),
                    self.config.mode.upper(),
                    self.config.db_path,
                    latest["equity"],
                    latest["cash"],
                    latest["pnl"],
                    latest["exposure"] * 100.0,
                )
            )
        else:
            overview.update(
                "PROFILE=%s MODE=%s DB=%s\nNo portfolio snapshots yet. Run: kalman-quant dry-run"
                % (self.config.profile.upper(), self.config.mode.upper(), self.config.db_path)
            )
        self._fill_events("#signals", "Signals", self.store.latest_events("strategy_signal", 12))
        self._fill_decisions()
        self._fill_risk()
        self._fill_universe()
        self._fill_research()
        self._fill_broker()
        self._fill_runs()

    def _fill_events(self, selector, title, rows):
        table = self.query_one(selector, DataTable)
        table.clear(columns=True)
        table.add_columns(title, "Ticker", "Time", "Value")
        for row in rows:
            payload = json.loads(row["payload"])
            table.add_row(
                row["event_type"].upper(),
                str(row["ticker"] or ""),
                str(row["timestamp"])[11:19],
                "sig=%s score=%.3f px=%.2f"
                % (
                    payload.get("signal", ""),
                    float(payload.get("score", 0)),
                    float(payload.get("price", 0)),
                ),
            )

    def _fill_decisions(self):
        table = self.query_one("#decisions", DataTable)
        table.clear(columns=True)
        table.add_columns("Decision", "Ticker", "Action", "Qty", "State", "OK")
        for row in self.store.latest_decisions(12):
            table.add_row(
                str(row["timestamp"])[11:19],
                row["ticker"],
                row["action"],
                str(row["quantity"]),
                row["risk_state"],
                "Y" if row["approved"] else "N",
            )

    def _fill_risk(self):
        table = self.query_one("#risk", DataTable)
        table.clear(columns=True)
        table.add_columns("Risk State", "Value")
        risk = self.config.risk
        state = self.store.get_state("risk_state")
        if state:
            payload = state["payload"]
            table.add_row("state", str(payload.get("state")))
            table.add_row("drawdown", "%.2f%%" % (float(payload.get("drawdown_pct", 0)) * 100.0))
            table.add_row("reasons", ",".join(payload.get("reasons", [])))
        table.add_row("max_open_trades", str(risk.get("max_open_trades")))
        table.add_row("max_dd", str(risk.get("max_drawdown_pct")))
        table.add_row("target_vol", str(risk.get("target_volatility_pct")))
        table.add_row("max_exposure", str(risk.get("max_exposure_pct")))

    def _fill_universe(self):
        table = self.query_one("#universe", DataTable)
        table.clear(columns=True)
        table.add_columns("Universe", "Value")
        state = self.store.get_state("universe:top_us_liquid")
        if not state:
            table.add_row("status", "Run: kalman-quant universe-refresh")
            return
        payload = state["payload"]
        symbols = payload.get("symbols", [])
        table.add_row("name", payload.get("name", ""))
        table.add_row("count", str(len(symbols)))
        table.add_row("symbols", ", ".join(symbols[:12]))

    def _fill_research(self):
        table = self.query_one("#research", DataTable)
        table.clear(columns=True)
        table.add_columns("Research", "Ticker", "Status", "Notes")
        for row in self.store.latest_events("data_quality", 10):
            payload = json.loads(row["payload"])
            table.add_row(
                str(row["timestamp"])[11:19],
                str(row["ticker"] or ""),
                str(payload.get("status", "")),
                ",".join(payload.get("notes", [])),
            )

    def _fill_broker(self):
        table = self.query_one("#broker", DataTable)
        table.clear(columns=True)
        table.add_columns("Broker Health", "Value")
        state = self.store.get_state("broker_health")
        if not state:
            table.add_row("status", "Run: kalman-quant health")
            return
        payload = state["payload"]
        table.add_row("connected", str(payload.get("connected")))
        table.add_row("mode", str(payload.get("mode")))
        table.add_row("market_data_type", str(payload.get("market_data_type")))
        table.add_row("errors", ",".join(payload.get("errors", [])))

    def _fill_runs(self):
        table = self.query_one("#runs", DataTable)
        table.clear(columns=True)
        table.add_columns("Run", "Return", "Sharpe", "DD")
        runs_dir = Path(self.config.runs_dir)
        if not runs_dir.exists():
            return
        summaries = sorted(runs_dir.glob("*/summary.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:8]
        for path in summaries:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            table.add_row(
                path.parent.name,
                "%.2f" % float(data.get("total_return_pct", 0)),
                "%.2f" % float(data.get("sharpe", 0)),
                "%.2f" % float(data.get("max_drawdown_pct", 0)),
            )

    def on_unmount(self) -> None:
        self.store.close()


def main() -> None:
    BloombergTerminal().run()


if __name__ == "__main__":
    main()
