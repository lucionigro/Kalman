#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Spot swing bot (1h) — RS gating + rotación por RS + cupo
Estrategia inspirada en tu versión de IBKR: Kalman+Hull+Supertrend, selección por RS vs BTC, cupo máximo y rotación inteligente.

- Mercado: Binance SPOT (USDT)
- Timeframe: 1h
- Señales: long-only; cierra posición al perder señal (venta total)
- Gestión: cupo de posiciones abiertas, presupuesto fijo por trade
- Fricción: slippage_pct + fee_pct (aprox.)
- Modo backtest y modo live

Requisitos:
  pip install ccxt pandas numpy python-dotenv

Variables de entorno para LIVE:
  BINANCE_API_KEY=... 
  BINANCE_API_SECRET=...

Uso:
  python binance_bot.py --mode backtest --capital 1000
  python binance_bot.py --mode live --capital 1000

Notas:
- Este es un punto de partida sólido; ajusta parámetros y agrega guardrails (earnings no aplica en cripto).
- Para LIVE, el bot asume que solo opera USDT y que las ventas son para cerrar posiciones existentes (no hace short en spot).
"""
from __future__ import annotations
import os
import time
import math
import json
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import ccxt  # type: ignore
except Exception:
    ccxt = None  # permitirá correr el backtest offline si no se instala ccxt

from dotenv import load_dotenv
load_dotenv()

# ===================== CONFIGURACIÓN BASE =====================
TIMEFRAME = '1h'
BENCHMARK = 'BTCUSDT'  # para RS

SYMBOLS = [
    # ---- MAJORS ----
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT',
    'AVAXUSDT', 'DOGEUSDT', 'TONUSDT', 'TRXUSDT',
    # ---- L2 / INFRA ----
    'MATICUSDT', 'ARBUSDT', 'OPUSDT', 'APTUSDT', 'SUIUSDT',
    # ---- DeFi / Web3 ----
    'UNIUSDT', 'LINKUSDT', 'AAVEUSDT', 'MKRUSDT', 'INJUSDT',
    # ---- AI / Narrativas ----
    'RNDRUSDT', 'FETUSDT', 'TAOUSDT',
    # ---- Otros grandes ----
    'DOTUSDT', 'ATOMUSDT', 'NEARUSDT', 'ETCUSDT', 'LTCUSDT'
]

# Parámetros de estrategia (alineados a lo conversado)
RS_LOOKBACK = 10           # velas para RS (1h)
RS_MIN = 0.02              # exige +2% vs BTC
RS_ROTATION_MARGIN = 0.05  # rotar solo si supera al peor por +5% de RS
ALLOW_ROTATION = True

BT_MAX_OPEN = 5            # cupo de posiciones simultáneas
BT_BUDGET_TRADE = 200.0    # USDT por operación
CAPITAL_TOTAL_DEFAULT = 1000.0

SLIPPAGE_PCT = 0.001       # 0.1% spot
FEE_PCT = 0.001            # 0.1% fee aprox en Binance spot (ajusta si tenés descuento)

# Indicadores (Supertrend, Hull, Kalman) — períodos por defecto
ST_PERIOD = 10
ST_MULT   = 3.0
HMA_FAST  = 16
HMA_SLOW  = 36

# ===================== UTILIDADES =====================

def ts_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def fetch_ohlcv(exchange, symbol: str, timeframe: str = TIMEFRAME, limit: int = 1000) -> pd.DataFrame:
    """Descarga OHLCV desde Binance (ccxt). Devuelve DataFrame con índice datetime UTC.
    Para backtest, llamaremos múltiples veces y alineamos por índice.
    """
    data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(data, columns=cols)
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df.set_index('datetime', inplace=True)
    df.drop(columns=['timestamp'], inplace=True)
    return df


def _wma(series: pd.Series, period: int) -> pd.Series:
    weights = np.arange(1, period + 1)
    return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)


def hma(series: pd.Series, period: int) -> pd.Series:
    """Hull Moving Average"""
    if period <= 1:
        return series
    half = int(period / 2)
    sqrt_p = int(math.sqrt(period))
    wma_half = _wma(series, half)
    wma_full = _wma(series, period)
    hull_series = 2 * wma_half - wma_full
    return _wma(hull_series, sqrt_p)


def kalman_1d(series: pd.Series, R: float = 0.01, Q: float = 0.001) -> pd.Series:
    """Suavizado Kalman sencillo 1D."""
    xhat = np.zeros(len(series))
    P = np.zeros(len(series))
    xhatminus = np.zeros(len(series))
    Pminus = np.zeros(len(series))
    K = np.zeros(len(series))

    xhat[0] = series.iloc[0]
    P[0] = 1.0
    for k in range(1, len(series)):
        # Predicción
        xhatminus[k] = xhat[k-1]
        Pminus[k] = P[k-1] + Q
        # Actualización
        K[k] = Pminus[k] / (Pminus[k] + R)
        xhat[k] = xhatminus[k] + K[k] * (series.iloc[k] - xhatminus[k])
        P[k] = (1 - K[k]) * Pminus[k]
    return pd.Series(xhat, index=series.index)


def supertrend(df: pd.DataFrame, period: int = ST_PERIOD, multiplier: float = ST_MULT) -> pd.DataFrame:
    """Calcula Supertrend. Agrega columnas: 'st', 'st_dir' (1/-1)."""
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift()).abs()
    tr3 = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()

    hl2 = (df['high'] + df['low']) / 2
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    st = pd.Series(index=df.index, dtype=float)
    dir_series = pd.Series(index=df.index, dtype=int)

    st.iloc[0] = upper.iloc[0]
    dir_series.iloc[0] = 1

    for i in range(1, len(df)):
        if df['close'].iloc[i] > st.iloc[i-1]:
            dir_series.iloc[i] = 1
        elif df['close'].iloc[i] < st.iloc[i-1]:
            dir_series.iloc[i] = -1
        else:
            dir_series.iloc[i] = dir_series.iloc[i-1]

        if dir_series.iloc[i] == 1:
            st.iloc[i] = min(upper.iloc[i], st.iloc[i-1])
        else:
            st.iloc[i] = max(lower.iloc[i], st.iloc[i-1])

    out = df.copy()
    out['st'] = st
    out['st_dir'] = dir_series
    return out


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Kalman suavizado (sobre close)
    df['kal'] = kalman_1d(df['close'])
    # Hull MA
    df['hma_fast'] = hma(df['kal'], HMA_FAST)
    df['hma_slow'] = hma(df['kal'], HMA_SLOW)
    # Supertrend
    st = supertrend(df, ST_PERIOD, ST_MULT)
    df['st'] = st['st']
    df['st_dir'] = st['st_dir']
    # Señales básicas
    df['bull'] = (df['close'] > df['st']) & (df['hma_fast'] > df['hma_slow'])
    df['bear'] = (df['close'] < df['st']) | (df['hma_fast'] < df['hma_slow'])
    return df


def compute_rs(asset_df: pd.DataFrame, bench_df: pd.DataFrame, lookback: int = RS_LOOKBACK) -> pd.Series:
    """RS = (Ret_asset - Ret_bench) sobre ventana.
    Retorno simple: close / close.shift(lookback) - 1
    """
    a = asset_df['close']
    b = bench_df['close']
    ret_a = a / a.shift(lookback) - 1.0
    ret_b = b / b.shift(lookback) - 1.0
    rs = ret_a - ret_b
    return rs

# ===================== BACKTEST =====================
@dataclass
class Position:
    symbol: str
    entry_time: pd.Timestamp
    entry_price: float
    size: float
    rs_at_entry: float

@dataclass
class Trade:
    symbol: str
    side: str
    time: pd.Timestamp
    price: float
    size: float
    pnl: float

class RSRotatorBacktester:
    def __init__(self,
                 symbols: List[str],
                 timeframe: str = TIMEFRAME,
                 capital: float = CAPITAL_TOTAL_DEFAULT,
                 budget_per_trade: float = BT_BUDGET_TRADE,
                 max_open: int = BT_MAX_OPEN,
                 rs_min: float = RS_MIN,
                 rs_rotation_margin: float = RS_ROTATION_MARGIN,
                 allow_rotation: bool = ALLOW_ROTATION,
                 slippage_pct: float = SLIPPAGE_PCT,
                 fee_pct: float = FEE_PCT,
                 exchange: Optional[object] = None):
        self.symbols = symbols
        self.timeframe = timeframe
        self.capital = capital
        self.cash = capital
        self.budget_per_trade = budget_per_trade
        self.max_open = max_open
        self.rs_min = rs_min
        self.rs_rotation_margin = rs_rotation_margin
        self.allow_rotation = allow_rotation
        self.slippage_pct = slippage_pct
        self.fee_pct = fee_pct
        self.exchange = exchange
        self.history: Dict[str, pd.DataFrame] = {}
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []

    def load_history(self, limit: int = 1000):
        if self.exchange is None:
            raise RuntimeError("Se requiere ccxt.Exchange para descargar data.")
        for s in self.symbols:
            df = fetch_ohlcv(self.exchange, s, timeframe=self.timeframe, limit=limit)
            df = compute_indicators(df)
            self.history[s] = df
        # benchmark
        if BENCHMARK not in self.history:
            dfb = fetch_ohlcv(self.exchange, BENCHMARK, timeframe=self.timeframe, limit=limit)
            self.history[BENCHMARK] = compute_indicators(dfb)

    def align_index(self) -> pd.DatetimeIndex:
        # intersección de índices para que todas las series tengan timestamps comunes
        idx = None
        for df in self.history.values():
            idx = df.index if idx is None else idx.intersection(df.index)
        # reindex y forward-fill mínimos
        for s, df in self.history.items():
            self.history[s] = df.reindex(idx).ffill()
        return idx

    def close_position(self, symbol: str, t: pd.Timestamp, price: float):
        pos = self.positions.pop(symbol)
        # aplicar slippage y fee
        exec_price = price * (1 - self.slippage_pct)
        gross = (exec_price - pos.entry_price) * pos.size
        fee = (pos.entry_price + exec_price) * pos.size * self.fee_pct
        pnl = gross - fee
        self.cash += exec_price * pos.size - (exec_price * pos.size * self.fee_pct)
        self.trades.append(Trade(symbol, 'sell', t, exec_price, pos.size, pnl))

    def open_position(self, symbol: str, t: pd.Timestamp, price: float, rs_val: float):
        if self.cash < 10:
            return
        budget = min(self.budget_per_trade, self.cash)
        if budget < 10:
            return
        # slippage + fee en la compra
        buy_price = price * (1 + self.slippage_pct)
        size = (budget * (1 - self.fee_pct)) / buy_price
        self.cash -= buy_price * size * (1 + self.fee_pct)
        self.positions[symbol] = Position(symbol, t, buy_price, size, rs_val)
        self.trades.append(Trade(symbol, 'buy', t, buy_price, size, 0.0))

    def run(self) -> Dict[str, float]:
        idx = self.align_index()
        bench = self.history[BENCHMARK]
        # pre-compute RS series
        rs_map: Dict[str, pd.Series] = {}
        for s, df in self.history.items():
            if s == BENCHMARK:
                continue
            rs_map[s] = compute_rs(df, bench, RS_LOOKBACK)

        for t in idx[RS_LOOKBACK+max(HMA_SLOW, ST_PERIOD)+1:]:
            # 1) exits
            to_close = []
            for s, pos in list(self.positions.items()):
                df = self.history[s]
                row = df.loc[t]
                if bool(row['bear']):
                    # cerrar al open de la prox barra (o al close actual si simplificamos)
                    # implementación: usamos close actual para backtest discreto
                    self.close_position(s, t, row['close'])
                    to_close.append(s)
            # 2) candidatos
            cands = []
            for s in self.symbols:
                if s == BENCHMARK:
                    continue
                df = self.history[s]
                row = df.loc[t]
                rs_val = rs_map[s].loc[t]
                if not np.isfinite(rs_val):
                    continue
                if bool(row['bull']) and (rs_val >= self.rs_min):
                    cands.append((s, float(rs_val), float(row['close'])))
            # rank por RS desc
            cands.sort(key=lambda x: x[1], reverse=True)
            held_symbols = list(self.positions.keys())
            open_slots = self.max_open - len(held_symbols)

            # 3) abrir nuevas si hay cupo
            for (s, rs_val, px) in cands:
                if s in self.positions:
                    continue
                if open_slots <= 0:
                    break
                self.open_position(s, t, px, rs_val)
                open_slots -= 1

            # 4) rotación si está lleno
            if self.allow_rotation and open_slots <= 0 and len(cands) > 0 and len(self.positions) > 0:
                # peor RS de los held vs mejor RS de candidatos no held
                held = [(s, rs_map[s].loc[t]) for s in self.positions]
                held.sort(key=lambda x: x[1])  # ascendente: peor primero
                worst_sym, worst_rs = held[0]
                # mejor candidato que no esté en held
                for (s, rs_new, px) in cands:
                    if s in self.positions:
                        continue
                    if rs_new >= worst_rs + self.rs_rotation_margin:
                        # rotar: cerrar worst y abrir s
                        wpx = self.history[worst_sym].loc[t]['close']
                        self.close_position(worst_sym, t, wpx)
                        self.open_position(s, t, px, rs_new)
                        break

        # Cerrar todas al final para contabilizar PnL
        last_t = idx[-1]
        for s, pos in list(self.positions.items()):
            px = self.history[s].iloc[-1]['close']
            self.close_position(s, last_t, px)

        pnl = sum(tr.pnl for tr in self.trades)
        ret = (self.cash - self.capital) / self.capital
        buys = [tr for tr in self.trades if tr.side == 'buy']
        sells = [tr for tr in self.trades if tr.side == 'sell']
        wins = [tr for tr in sells if tr.pnl > 0]
        losses = [tr for tr in sells if tr.pnl <= 0]
        pf = (sum(tr.pnl for tr in wins) / max(1e-9, abs(sum(tr.pnl for tr in losses)))) if losses else np.inf
        return {
            'ops': len(sells),
            'win_pct': 100.0 * len(wins) / max(1, len(sells)),
            'pf': pf,
            'pnl': pnl,
            'cash_final': self.cash,
            'ret_pct': 100.0 * ret,
        }

# ===================== LIVE (SPOT) =====================
class BinanceSpotExecutor:
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        if ccxt is None:
            raise RuntimeError("Se requiere ccxt para el modo LIVE.")
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'apiKey': api_key or os.getenv('BINANCE_API_KEY'),
            'secret': api_secret or os.getenv('BINANCE_API_SECRET'),
            'options': {
                'defaultType': 'spot',
            }
        })
        self.exchange.load_markets()

    def fetch_ohlcv_df(self, symbol: str, timeframe: str = TIMEFRAME, limit: int = 1000) -> pd.DataFrame:
        return fetch_ohlcv(self.exchange, symbol, timeframe=timeframe, limit=limit)

    def get_balance(self, asset: str = 'USDT') -> float:
        bal = self.exchange.fetch_balance()
        return float(bal['free'].get(asset, 0.0))

    def get_position_amount(self, symbol: str) -> float:
        # Spot: cantidad que tenés del asset base (antes del slash), ej. BTC en BTC/USDT
        market = self.exchange.market(symbol)
        base = market['base']
        bal = self.exchange.fetch_balance()
        return float(bal['free'].get(base, 0.0))

    def _amount_to_lot(self, symbol: str, amount: float) -> float:
        market = self.exchange.market(symbol)
        step = market['limits']['amount']['min'] or 0.0
        precision = market.get('precision', {}).get('amount', 8)
        if step:
            qty = math.floor(amount / step) * step
        else:
            qty = round(amount, precision)
        return max(0.0, float(qty))

    def market_buy(self, symbol: str, usdt_budget: float) -> Optional[dict]:
        ticker = self.exchange.fetch_ticker(symbol)
        price = float(ticker['last'])
        size = usdt_budget / price
        size = self._amount_to_lot(symbol, size)
        if size <= 0:
            return None
        return self.exchange.create_order(symbol, 'market', 'buy', size)

    def market_sell_all(self, symbol: str) -> Optional[dict]:
        amt = self.get_position_amount(symbol)
        amt = self._amount_to_lot(symbol, amt)
        if amt <= 0:
            return None
        return self.exchange.create_order(symbol, 'market', 'sell', amt)


class BinanceSpotBot:
    def __init__(self,
                 symbols: List[str] = SYMBOLS,
                 timeframe: str = TIMEFRAME,
                 capital: float = CAPITAL_TOTAL_DEFAULT,
                 budget_per_trade: float = BT_BUDGET_TRADE,
                 max_open: int = BT_MAX_OPEN,
                 rs_min: float = RS_MIN,
                 rs_rotation_margin: float = RS_ROTATION_MARGIN,
                 allow_rotation: bool = ALLOW_ROTATION):
        if ccxt is None:
            raise RuntimeError("Instala ccxt para modo LIVE y descarga de datos.")
        self.exec = BinanceSpotExecutor()
        self.symbols = symbols
        self.timeframe = timeframe
        self.capital = capital
        self.budget_per_trade = budget_per_trade
        self.max_open = max_open
        self.rs_min = rs_min
        self.rs_rotation_margin = rs_rotation_margin
        self.allow_rotation = allow_rotation

    def _load_histories(self, limit: int = 1000) -> Dict[str, pd.DataFrame]:
        hist = {}
        for s in self.symbols:
            df = self.exec.fetch_ohlcv_df(s, self.timeframe, limit)
            hist[s] = compute_indicators(df)
        if BENCHMARK not in hist:
            bdf = self.exec.fetch_ohlcv_df(BENCHMARK, self.timeframe, limit)
            hist[BENCHMARK] = compute_indicators(bdf)
        return hist

    def _align(self, history: Dict[str, pd.DataFrame]) -> pd.DatetimeIndex:
        idx = None
        for df in history.values():
            idx = df.index if idx is None else idx.intersection(df.index)
        for s in history:
            history[s] = history[s].reindex(idx).ffill()
        return idx

    def run_backtest(self, limit: int = 1000) -> Dict[str, float]:
        exch = self.exec.exchange
        bt = RSRotatorBacktester(
            symbols=self.symbols,
            timeframe=self.timeframe,
            capital=self.capital,
            budget_per_trade=self.budget_per_trade,
            max_open=self.max_open,
            rs_min=self.rs_min,
            rs_rotation_margin=self.rs_rotation_margin,
            allow_rotation=self.allow_rotation,
            exchange=exch
        )
        bt.load_history(limit=limit)
        res = bt.run()
        print("===== BACKTEST SPOT (1h) =====")
        print(json.dumps(res, indent=2))
        return res

    def run_live_once(self, limit: int = 400):
        """Una pasada de evaluación/ejecución: cierra ventas, abre compras, rota si corresponde."""
        # 1) descargar históricos y calcular señales
        hist = self._load_histories(limit)
        idx = self._align(hist)
        bench = hist[BENCHMARK]
        rs_map = {s: compute_rs(df, bench, RS_LOOKBACK) for s, df in hist.items() if s != BENCHMARK}
        t = idx[-1]

        # 2) posiciones actuales (Spot): inferimos por balances > 0 del asset base
        held = []
        for s in self.symbols:
            if s == BENCHMARK:
                continue
            amt = self.exec.get_position_amount(s)
            if amt > 0:
                held.append(s)

        # 3) salidas: si señal bear para símbolos held → vender market
        for s in list(held):
            row = hist[s].loc[t]
            if bool(row['bear']):
                print(f"[LIVE] Cerrar {s} por señal bear")
                try:
                    self.exec.market_sell_all(s)
                except Exception as e:
                    print(f"[LIVE][ERROR] sell {s}: {e}")

        # Recalcular held por si cerramos
        held = []
        for s in self.symbols:
            if s == BENCHMARK:
                continue
            if self.exec.get_position_amount(s) > 0:
                held.append(s)

        # 4) candidatos long por RS
        cands = []
        for s in self.symbols:
            if s == BENCHMARK:
                continue
            row = hist[s].loc[t]
            rs_val = rs_map[s].loc[t]
            if bool(row['bull']) and np.isfinite(rs_val) and rs_val >= self.rs_min:
                cands.append((s, float(rs_val)))
        cands.sort(key=lambda x: x[1], reverse=True)

        # 5) abrir nuevas si hay cupo
        open_slots = self.max_open - len(held)
        usdt_free = self.exec.get_balance('USDT')
        budget = min(self.budget_per_trade, max(0.0, usdt_free))
        for (s, rs_val) in cands:
            if open_slots <= 0:
                break
            if s in held:
                continue
            try:
                print(f"[LIVE] Comprar {s} (RS={rs_val:.3f}) budget={budget:.2f} USDT")
                self.exec.market_buy(s, budget)
                open_slots -= 1
            except Exception as e:
                print(f"[LIVE][ERROR] buy {s}: {e}")

        # 6) rotación si está lleno
        if self.allow_rotation and open_slots <= 0 and len(cands) > 0 and len(held) > 0:
            # peor RS de held vs mejor candidato fuera
            held_rs = []
            for s in held:
                if s in rs_map:
                    held_rs.append((s, float(rs_map[s].loc[t])))
            if held_rs:
                held_rs.sort(key=lambda x: x[1])
                worst_sym, worst_rs = held_rs[0]
                for (s, rs_new) in cands:
                    if s in held:
                        continue
                    if rs_new >= worst_rs + self.rs_rotation_margin:
                        try:
                            print(f"[LIVE] Rotar: vender {worst_sym} (RS={worst_rs:.3f}) → comprar {s} (RS={rs_new:.3f})")
                            self.exec.market_sell_all(worst_sym)
                            # actualizar balance USDT y comprar
                            usdt_free = self.exec.get_balance('USDT')
                            budget = min(self.budget_per_trade, usdt_free)
                            self.exec.market_buy(s, budget)
                        except Exception as e:
                            print(f"[LIVE][ERROR] rotate {worst_sym}->{s}: {e}")
                        break

    def run_live_loop(self, sleep_sec: int = 60):
        """Loop sencillo: chequea cada minuto si hay nueva vela y ejecuta.
        Nota: para producción, conviene programar por sincronía de cierre de vela exacto.
        """
        last_bar_time: Optional[pd.Timestamp] = None
        while True:
            try:
                hist_btc = self.exec.fetch_ohlcv_df(BENCHMARK, self.timeframe, limit=5)
                cur_last = hist_btc.index[-1]
                if last_bar_time is None or cur_last > last_bar_time:
                    print(f"[LIVE] Nueva vela detectada: {cur_last} → evaluar")
                    self.run_live_once(limit=400)
                    last_bar_time = cur_last
                else:
                    print("[LIVE] Esperando cierre de vela...")
            except Exception as e:
                print(f"[LIVE][ERROR] loop: {e}")
            time.sleep(sleep_sec)


# ===================== CLI =====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['backtest', 'live', 'live-once'], default='backtest')
    parser.add_argument('--capital', type=float, default=CAPITAL_TOTAL_DEFAULT)
    parser.add_argument('--budget', type=float, default=BT_BUDGET_TRADE)
    parser.add_argument('--max-open', type=int, default=BT_MAX_OPEN)
    parser.add_argument('--rs-min', type=float, default=RS_MIN)
    parser.add_argument('--rs-rot', type=float, default=RS_ROTATION_MARGIN)
    parser.add_argument('--no-rotation', action='store_true')
    args = parser.parse_args()

    allow_rot = not args.no_rotation

    if args.mode == 'backtest':
        if ccxt is None:
            raise RuntimeError("Instala ccxt para backtest en Binance.")
        ex = ccxt.binance({'enableRateLimit': True})
        ex.load_markets()
        bot = BinanceSpotBot(
            symbols=SYMBOLS,
            timeframe=TIMEFRAME,
            capital=args.capital,
            budget_per_trade=args.budget,
            max_open=args.max_open,
            rs_min=args.rs_min,
            rs_rotation_margin=args.rs_rot,
            allow_rotation=allow_rot
        )
        bot.run_backtest(limit=900)

    elif args.mode == 'live-once':
        bot = BinanceSpotBot(
            symbols=SYMBOLS,
            timeframe=TIMEFRAME,
            capital=args.capital,
            budget_per_trade=args.budget,
            max_open=args.max_open,
            rs_min=args.rs_min,
            rs_rotation_margin=args.rs_rot,
            allow_rotation=allow_rot
        )
        bot.run_live_once(limit=400)

    elif args.mode == 'live':
        bot = BinanceSpotBot(
            symbols=SYMBOLS,
            timeframe=TIMEFRAME,
            capital=args.capital,
            budget_per_trade=args.budget,
            max_open=args.max_open,
            rs_min=args.rs_min,
            rs_rotation_margin=args.rs_rot,
            allow_rotation=allow_rot
        )
        bot.run_live_loop(sleep_sec=60)


if __name__ == '__main__':
    main()
