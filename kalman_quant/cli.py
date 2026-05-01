import argparse
import json
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal envs
    Console = None
    Table = None

from kalman_quant.config import load_config
from kalman_quant.data import DEFAULT_TOP_US_LIQUID, LocalDataProvider, analyze_data_quality, build_liquid_universe
from kalman_quant.data.ibkr_downloader import IBKRDailyDownloader
from kalman_quant.execution import ExecutionEngine
from kalman_quant.live import DryRunCycle
from kalman_quant.research.backtest import QuantBacktester
from kalman_quant.research.grid import run_research_grid
from kalman_quant.research.promotion import write_promotion_report
from kalman_quant.research.walk_forward import run_walk_forward
from kalman_quant.ops.health import broker_health_from_config, health_check
from kalman_quant.storage import SQLiteStore


console = Console() if Console else None


def main() -> None:
    parser = argparse.ArgumentParser(prog="kalman-quant")
    parser.add_argument("--config", default=None, help="YAML config path")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("backtest")
    wf = sub.add_parser("walk-forward")
    wf.add_argument("--window-days", type=int, default=252)
    wf.add_argument("--test-days", type=int, default=63)
    dr = sub.add_parser("dry-run")
    dr.add_argument("--cash", type=float, default=None)
    ds = sub.add_parser("data-sync")
    ds.add_argument("--duration", default="3 Y")
    uf = sub.add_parser("universe-refresh")
    uf.add_argument("--max-symbols", type=int, default=100)
    sub.add_parser("research-grid")
    pr = sub.add_parser("promote-strategy")
    pr.add_argument("run_dir")
    pd = sub.add_parser("paper-daemon")
    pd.add_argument("--once", action="store_true")
    pd.add_argument("--interval-minutes", type=int, default=15)
    sub.add_parser("health")
    sub.add_parser("status")
    args = parser.parse_args()

    cfg = load_config(args.config)
    provider = LocalDataProvider(
        cfg.raw.get("data_cache_dir", "data_cache"),
        cfg.raw.get("backtest_cache_dir", "backtest_cache"),
    )

    if args.command == "backtest":
        data = provider.load_many(cfg.symbols)
        result = QuantBacktester(cfg, data).run()
        _print_metrics(result.metrics, "Backtest %s" % result.run_dir)
        return

    if args.command == "walk-forward":
        data = provider.load_many(cfg.symbols)
        rows = run_walk_forward(cfg, data, args.window_days, args.test_days)
        if Table is None:
            for row in rows:
                print(row)
            return
        table = Table(title="Walk Forward")
        for col in ["fold", "train_return_pct", "test_return_pct", "train_sharpe", "test_sharpe"]:
            table.add_column(col)
        for row in rows:
            table.add_row(
                str(row["fold"]),
                "%.2f" % row["train_return_pct"],
                "%.2f" % row["test_return_pct"],
                "%.2f" % row["train_sharpe"],
                "%.2f" % row["test_sharpe"],
            )
        _emit(table)
        return

    if args.command == "dry-run":
        data = provider.load_many(cfg.symbols)
        store = SQLiteStore(cfg.db_path)
        try:
            engine = ExecutionEngine(cfg, store)
            intents = DryRunCycle(cfg, store, engine).run_once(data, cash=args.cash)
        finally:
            store.close()
        _emit("[cyan]Dry-run complete[/cyan]: %s order intents recorded." % intents)
        return

    if args.command == "data-sync":
        results = IBKRDailyDownloader(cfg).sync(cfg.symbols, duration=args.duration)
        for symbol, status in results.items():
            print("%s: %s" % (symbol, status))
        return

    if args.command == "universe-refresh":
        candidates = list(dict.fromkeys(cfg.symbols + DEFAULT_TOP_US_LIQUID))
        data = provider.load_many(candidates)
        store = SQLiteStore(cfg.db_path)
        try:
            for symbol, df in data.items():
                store.record_data_quality(analyze_data_quality(symbol, df))
            snapshot = build_liquid_universe(
                data,
                candidates=candidates,
                min_price=float(cfg.risk.get("min_price", 5)),
                min_adv_usd=float(cfg.risk.get("adv_min_usd", 25_000_000)),
                max_symbols=args.max_symbols,
            )
            store.record_universe(snapshot)
        finally:
            store.close()
        print("Universe %s: %s symbols" % (snapshot.name, len(snapshot.symbols)))
        return

    if args.command == "research-grid":
        data = provider.load_many(cfg.symbols)
        df = run_research_grid(cfg, data)
        print(df.head(10).to_string(index=False))
        return

    if args.command == "promote-strategy":
        report = write_promotion_report(args.run_dir)
        print("Promotion report: %s" % report)
        return

    if args.command == "paper-daemon":
        from kalman_quant.live.paper_daemon import PaperDaemon

        daemon = PaperDaemon(cfg)
        if args.once:
            count = daemon.run_once()
            print("paper-daemon once complete: %s decisions submitted" % count)
        else:
            daemon.run_forever(interval_minutes=args.interval_minutes)
        return

    if args.command == "health":
        data = provider.load_many(cfg.symbols)
        result = health_check(cfg, data_symbols=list(data.keys()))
        store = SQLiteStore(cfg.db_path)
        try:
            store.record_broker_health(broker_health_from_config(cfg, connected=False, errors=result["errors"]))
        finally:
            store.close()
        print(json.dumps(result, indent=2))
        return

    if args.command == "status":
        _print_status(cfg)


def _print_metrics(metrics, title):
    if Table is None:
        print(title)
        for key, value in metrics.items():
            print("%s: %s" % (key, value))
        return
    table = Table(title=title)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")
    for key, value in metrics.items():
        table.add_row(str(key), "%.4f" % value if isinstance(value, float) else str(value))
    _emit(table)


def _print_status(cfg):
    if Table is None:
        print("Kalman Quant Status")
        print("profile: %s" % cfg.profile)
        print("mode: %s" % cfg.mode)
        print("db_path: %s" % cfg.db_path)
        print("runs_dir: %s" % cfg.runs_dir)
        print("symbols: %s" % ", ".join(cfg.symbols))
        print("ibkr: %s" % json.dumps(cfg.ibkr))
        return
    table = Table(title="Kalman Quant Status")
    table.add_column("Key", style="cyan")
    table.add_column("Value")
    table.add_row("profile", cfg.profile)
    table.add_row("mode", cfg.mode)
    table.add_row("db_path", cfg.db_path)
    table.add_row("runs_dir", cfg.runs_dir)
    table.add_row("symbols", ", ".join(cfg.symbols))
    table.add_row("ibkr", json.dumps(cfg.ibkr))
    _emit(table)


def _emit(obj):
    if console is not None:
        console.print(obj)
    else:
        print(obj)


if __name__ == "__main__":
    main()
