# simulate_live_portfolio.py
# Simulador "cartera viva" día a día, independiente del main.py
# - Usa IBKR Paper SOLO para descargar y cachear datos si faltan (no envía órdenes)
# - Sin DB: imprime métricas por consola
# - Respeta RS, filtros, TP parcial al +8% con stop BE, stops por Supertrend/ATR/%
# - Limita posiciones simultáneas y gestiona caja como cartera real

from ib_insync import *
import pandas as pd
import numpy as np
import os, math, time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

# ===================== PARÁMETROS GENERALES (IGUALES A TU LIVE) =====================
IB_HOST = '127.0.0.1'
IB_PORT = 7497            # Paper TWS/Gateway
IB_CLIENT_ID = 99         # Cliente para backtest (no interfiere con el live)
EXCHANGE = 'SMART'
CURRENCY = 'USD'
CACHE_DIR = 'data_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

# Universo (mismo que tu main)
SYMBOLS = [
    # TECH / GROWTH
    'NVDA','AMD','INTC','SMCI','ARM','TSM','MU','AAPL','MSFT','META','GOOG','AMZN','NFLX',
    'ADBE','CRM','ORCL','NOW','SNOW','DDOG','CRWD','NET','ZS','OKTA','MDB','SHOP','ABNB','UBER','RBLX',
    'PLTR','RDDT','HOOD','COIN','SOFI','AFRM','AI','IONQ','PATH',
    # INDUSTRIAL / VALUE
    'GE','BA','CAT','DE','HON','LMT','RTX',
    # FINANCE
    'JPM','GS','MS','BAC','C','WFC','USB','SCHW','AXP','BLK','IBKR','TROW','V','MA','SUPV','NU',
    # HEALTH
    'LLY','UNH','JNJ','ABBV','HIMS','VRTX','ISRG','XBI',
    # INTERNATIONAL / EM
    'MELI','YPF','PAGS','BBD','JD','BABA','TCEHY','PDD','FXI','EWZ','EWY','EWT','INDA',
    # ENERGY / MATERIALS
    'XOM','CVX','OXY','COP','HAL','FCX','CLF','VALE','SCCO','RIO','URA','CCJ',
    # DEFENSIVE
    'PEP','COST','WMT','NKE'
]

# ===================== ESTRATEGIA / INDICADORES =====================
PRICE_SOURCE       = "hl2"  # "hl2" o "close"
MEASUREMENT_NOISE  = 0.08
PROCESS_NOISE      = 0.02
ATR_PERIOD         = 1
ATR_FACTOR         = 0.3
TIMEFRAME          = '1 day'

# RS / gating
RS_BENCHMARK       = 'SPY'
RS_LOOKBACK_BARS   = 20
RS_MIN             = 0.07     # >7% vs SPY

# Filtros universo / régimen (idéntico al enfoque live)
MIN_PRICE          = 5.0
ADV_MIN_USD        = 25_000_000
REQUIRE_MARKET_UPTREND = True
SMA_UPTREND_LEN    = 50

# Riesgo / ejecución (simulada)
STOP_ATR_MULT      = 2.5
MAX_LOSS_PCT       = 0.05      # hard stop % desde entrada
TP_PARTIAL_PCT     = 8.0       # TP parcial único al +8%
COMMISSION_OPEN    = 1.0
COMMISSION_CLOSE   = 1.0
SLIPPAGE_PER_SH    = 0.02      # +/- 2c por lado

# ===================== CARTERA (PARAMETRIZABLE) =====================
STARTING_CASH      = 4000.0
MAX_OPEN_TRADES    = 4
BUDGET_PER_TRADE   = STARTING_CASH / MAX_OPEN_TRADES   # fijo como en tu live

# ===================== CONEXIÓN IBKR (solo datos) =====================
ib = IB()
try:
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, readonly=True)
    print(f"[IBKR] Conectado a Paper para descarga de históricos.")
except Exception as e:
    print(f"[IBKR][WARN] No se pudo conectar ahora ({e}). Si el cache ya existe, se usará sin descargar.")

# ===================== INDICADORES =====================
def f_kalman(prices: pd.Series, measurement_noise=1.0, process_noise=0.01) -> pd.Series:
    state = float(prices.iloc[0]); p = 1.0; out = [state]
    for z in prices.iloc[1:]:
        p += process_noise
        k = p / (p + measurement_noise)
        state = state + k * (z - state)
        p = (1 - k) * p
        out.append(state)
    return pd.Series(out, index=prices.index)

def khma(series: pd.Series, length=1.0, process_noise=0.01) -> pd.Series:
    inner1 = f_kalman(series, length/2, process_noise)
    inner2 = f_kalman(series, length, process_noise)
    diff = 2*inner1 - inner2
    return f_kalman(diff, math.sqrt(length), process_noise)

def rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1/max(1, length), adjust=False).mean()

def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def supertrend(df: pd.DataFrame, factor=1.5, atr_period=14, src_col="kalman_hma") -> pd.DataFrame:
    df['tr'] = true_range(df)
    df['atr'] = rma(df['tr'], max(1, atr_period))
    upper = df[src_col] + factor * df['atr']
    lower = df[src_col] - factor * df['atr']

    st = np.full(len(df), np.nan)
    direction = np.zeros(len(df))

    for i in range(1, len(df)):
        prev_super = st[i-1]
        if np.isnan(df['atr'].iloc[i-1]):
            direction[i] = 1
            st[i] = upper.iloc[i]
            continue
        if prev_super == upper.iloc[i-1]:
            direction[i] = -1 if df['close'].iloc[i] > upper.iloc[i] else 1
        else:
            direction[i] = 1 if df['close'].iloc[i] < lower.iloc[i] else -1
        st[i] = lower.iloc[i] if direction[i] == -1 else upper.iloc[i]

    df['supertrend'] = st
    df['direction'] = direction
    return df

# ===================== HISTÓRICOS (cache + IBKR si falta) =====================
def _cache_path(symbol: str) -> str:
    return os.path.join(CACHE_DIR, f"{symbol}_2Y_1d.parquet")

def _load_cache(symbol: str) -> Optional[pd.DataFrame]:
    p = _cache_path(symbol)
    if os.path.exists(p):
        try:
            df = pd.read_parquet(p)
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            return None
    return None

def _save_cache(symbol: str, df: pd.DataFrame):
    p = _cache_path(symbol); tmp = p + ".tmp"
    cols = [c for c in ['date','open','high','low','close','volume'] if c in df.columns]
    df[cols].to_parquet(tmp, index=False); os.replace(tmp, p)

def fetch_history(symbol: str) -> Optional[pd.DataFrame]:
    # Intentar cache
    df = _load_cache(symbol)
    if df is not None and len(df) >= RS_LOOKBACK_BARS + 60:
        return df.sort_values('date', ignore_index=True)

    # Descargar si hay conexión
    try:
        c = Stock(symbol, EXCHANGE, CURRENCY)
        bars = ib.reqHistoricalData(c, '', '2 Y', '1 day', 'TRADES', True, 1)
        df = util.df(bars)
        if df is None or df.empty:
            return None
        df = df.drop_duplicates('date').sort_values('date', ignore_index=True)
        _save_cache(symbol, df)
        time.sleep(0.3)
        return df
    except Exception as e:
        print(f"[DATA][{symbol}] {e}")
        return _load_cache(symbol)  # último recurso

def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df = df.copy()
    df['src'] = (df['high'] + df['low'])/2.0 if PRICE_SOURCE == 'hl2' else df['close']
    df['kalman_hma'] = khma(df['src'], MEASUREMENT_NOISE, PROCESS_NOISE)
    df = supertrend(df, ATR_FACTOR, ATR_PERIOD, 'kalman_hma')
    df['signal'] = 0
    cross_long  = (df['direction'].shift(1) > 0) & (df['direction'] < 0)
    cross_short = (df['direction'].shift(1) < 0) & (df['direction'] > 0)
    df.loc[cross_long, 'signal'] = 1
    df.loc[cross_short,'signal'] = -1
    return df

# ===================== RS / FILTROS =====================
def rs20_score(df_sym: pd.DataFrame, df_bench: pd.DataFrame, i: int) -> float:
    """RS de símbolo vs benchmark usando datos hasta índice i-1 (lookback de 20 barras)."""
    j = i - RS_LOOKBACK_BARS
    if j < 1: return -1e9
    try:
        r_t = df_sym['close'].iloc[i-1] / df_sym['close'].iloc[j-1] - 1.0
        r_b = df_bench['close'].iloc[i-1] / df_bench['close'].iloc[j-1] - 1.0
        return float(r_t - r_b)
    except Exception:
        return -1e9

def avg_dollar_volume(df: pd.DataFrame, i: int, lookback: int = 20) -> float:
    if df is None or i < lookback: return 0.0
    tail = df.iloc[i-lookback:i]
    return float((tail['close'] * tail['volume']).mean())

def market_uptrend_ok(df_bench: pd.DataFrame, i: int) -> bool:
    if not REQUIRE_MARKET_UPTREND: return True
    if df_bench is None or i < SMA_UPTREND_LEN: return True
    sma = df_bench['close'].rolling(SMA_UPTREND_LEN).mean()
    return bool(df_bench['close'].iloc[i-1] > sma.iloc[i-1])

# ===================== ESTRUCTURAS DE POSICIÓN =====================
@dataclass
class Position:
    symbol: str
    qty: int
    entry: float
    open_idx: int
    tp_done: bool = False

@dataclass
class TradeRecord:
    symbol: str
    qty: int
    entry: float
    exit: float
    open_date: pd.Timestamp
    close_date: pd.Timestamp
    pnl: float

# ===================== SIMULADOR CARTERA VIVA =====================
def simulate_portfolio():
    # 1) Descargar/leer benchmark y features
    bench = fetch_history(RS_BENCHMARK)
    if bench is None or len(bench) < RS_LOOKBACK_BARS + 50:
        print("[SIM] No hay suficiente benchmark para simular.")
        return
    bench = prepare_features(bench)

    # 2) Descargar/leer universo con features
    series: Dict[str, pd.DataFrame] = {}
    for s in SYMBOLS:
        df = fetch_history(s)
        if df is None or len(df) < RS_LOOKBACK_BARS + 50:
            continue
        series[s] = prepare_features(df)

    # 3) Construir calendario común (fechas del benchmark)
    cal = bench['date'].tolist()
    if len(cal) < RS_LOOKBACK_BARS + 2:
        print("[SIM] Calendario insuficiente.")
        return

    cash = STARTING_CASH
    positions: Dict[str, Position] = {}
    trades: List[TradeRecord] = []
    equity_curve: List[float] = []

    print(f"[SIM] Iniciando simulación cartera viva con {len(series)} símbolos.")
    print(f"[SIM] Caja inicial: {cash:.2f} USD | Slots: {MAX_OPEN_TRADES} | Budget/Trade: {BUDGET_PER_TRADE:.2f}\n")

    # 4) Loop día a día (usar índices del benchmark como timeline)
    for i in range(RS_LOOKBACK_BARS + 1, len(cal)):
        day = cal[i]
        # === 4.1. Cierres/Stops/TP parciales sobre posiciones abiertas ===
        to_close = []
        for sym, pos in list(positions.items()):
            df = series.get(sym)
            if df is None: 
                to_close.append(sym); continue
            # buscar índice de la fecha en df (algunos símbolos pueden no cotizar ese día)
            # usamos el close más reciente <= day
            idx_list = df.index[df['date'] == day].tolist()
            if not idx_list:
                continue
            k = idx_list[0]
            row = df.iloc[k]
            px_close = float(row['close'])
            st = float(row['supertrend']) if not pd.isna(row['supertrend']) else None
            atr = float(row['atr']) if 'atr' in row.index and not pd.isna(row['atr']) else None
            sig = int(row['signal']); direction = int(row['direction'])

            # Ganancia actual
            gain_pct = (px_close / pos.entry - 1.0) * 100.0

            # TP parcial único al +8 %
            if (not pos.tp_done) and gain_pct >= TP_PARTIAL_PCT and pos.qty > 1:
                sell_qty = pos.qty // 2
                fill_px = max(0.01, px_close - SLIPPAGE_PER_SH)
                cash += sell_qty * fill_px - COMMISSION_CLOSE
                # Registrar trade parcial
                trades.append(TradeRecord(
                    symbol=sym, qty=sell_qty, entry=pos.entry, exit=fill_px,
                    open_date=series[sym]['date'].iloc[pos.open_idx],
                    close_date=row['date'], pnl=(fill_px - pos.entry) * sell_qty - COMMISSION_CLOSE
                ))
                pos.qty -= sell_qty
                pos.tp_done = True
                # BE stop implícito: una de las reglas de salida ya lo cubre (close <= entry)
                # (ver chequeo más abajo)

            # Reglas de stop/salida (orden: hard stop %, supertrend, ATR, señal contraria)
            hard_stop = px_close <= pos.entry * (1.0 - MAX_LOSS_PCT)
            st_stop = (st is not None) and (px_close < st)
            atr_stop = (atr is not None) and (px_close <= (pos.entry - STOP_ATR_MULT * atr))
            be_stop = pos.tp_done and (px_close <= pos.entry)  # BE tras TP
            reverse_sig = (sig == -1) or (direction == 1)

            if hard_stop or st_stop or atr_stop or be_stop or reverse_sig:
                fill_px = max(0.01, px_close - SLIPPAGE_PER_SH)
                cash += pos.qty * fill_px - COMMISSION_CLOSE
                trades.append(TradeRecord(
                    symbol=sym, qty=pos.qty, entry=pos.entry, exit=fill_px,
                    open_date=series[sym]['date'].iloc[pos.open_idx],
                    close_date=row['date'], pnl=(fill_px - pos.entry) * pos.qty - COMMISSION_CLOSE
                ))
                to_close.append(sym)

        for sym in to_close:
            positions.pop(sym, None)

        # === 4.2. Entradas nuevas (si hay slots/cash) ===
        open_slots = MAX_OPEN_TRADES - len(positions)
        if open_slots > 0 and market_uptrend_ok(bench, i):
            # candidatos filtrados por precio/ADV y que no estén ya abiertos
            candidates = []
            for s, df in series.items():
                if s in positions: 
                    continue
                # encontrar índice para day
                idx_list = df.index[df['date'] == day].tolist()
                if not idx_list: 
                    continue
                k = idx_list[0]
                px = float(df['close'].iloc[k])
                if px < MIN_PRICE: 
                    continue
                adv = avg_dollar_volume(df, k, 20)
                if adv < ADV_MIN_USD:
                    continue
                # RS hasta i-1 (día previo del benchmark)
                sc = rs20_score(df, bench, i)
                if sc <= RS_MIN: 
                    continue
                sig = int(df['signal'].iloc[k]); direction = int(df['direction'].iloc[k])
                if (sig == 1) or (direction == -1):
                    candidates.append((s, sc, px, k))

            # rankear por RS desc y tomar hasta llenar slots
            candidates.sort(key=lambda x: x[1], reverse=True)
            for s, sc, px, k in candidates:
                if open_slots <= 0: break
                # sizing por presupuesto fijo (como live)
                qty = int((BUDGET_PER_TRADE - COMMISSION_OPEN) // (px + SLIPPAGE_PER_SH))
                cost = qty * (px + SLIPPAGE_PER_SH) + COMMISSION_OPEN
                if qty >= 1 and cash >= cost:
                    cash -= cost
                    positions[s] = Position(symbol=s, qty=qty, entry=px + SLIPPAGE_PER_SH, open_idx=k)
                    open_slots -= 1

        # === 4.3. Equity mark-to-market ===
        mtm = 0.0
        for sym, pos in positions.items():
            df = series[sym]
            idx_list = df.index[df['date'] == day].tolist()
            if not idx_list:
                # usar último close disponible
                mtm += pos.qty * float(df['close'].iloc[pos.open_idx])
            else:
                mtm += pos.qty * float(df['close'].iloc[idx_list[0]])
        equity_curve.append(cash + mtm)

    # 5) Liquidar todo al final (si quedó algo)
    last_day = cal[-1]
    for sym, pos in list(positions.items()):
        df = series[sym]
        k = df.index[df['date'] == last_day].tolist()
        if not k:
            k = [-1]
        px = float(df['close'].iloc[k[0]])
        fill_px = max(0.01, px - SLIPPAGE_PER_SH)
        cash += pos.qty * fill_px - COMMISSION_CLOSE
        trades.append(TradeRecord(
            symbol=sym, qty=pos.qty, entry=pos.entry, exit=fill_px,
            open_date=series[sym]['date'].iloc[pos.open_idx],
            close_date=df['date'].iloc[k[0]],
            pnl=(fill_px - pos.entry) * pos.qty - COMMISSION_CLOSE
        ))
        positions.pop(sym, None)
        # agregar último equity
    equity_curve.append(cash)

    # ===================== MÉTRICAS =====================
    total_trades = len(trades)
    winners = sum(1 for t in trades if t.pnl > 0)
    losers = total_trades - winners
    pnl_total = sum(t.pnl for t in trades)
    # Profit factor
    gross_profit = sum(t.pnl for t in trades if t.pnl > 0)
    gross_loss = -sum(t.pnl for t in trades if t.pnl < 0)
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')
    # Max drawdown
    ec = pd.Series(equity_curve)
    roll_max = ec.cummax()
    dd = (ec / roll_max - 1.0)
    max_dd = dd.min() if len(dd) else 0.0
    # Top 5 símbolos por PnL agregado
    by_sym: Dict[str, float] = {}
    for t in trades:
        by_sym[t.symbol] = by_sym.get(t.symbol, 0.0) + t.pnl
    top5 = sorted(by_sym.items(), key=lambda x: x[1], reverse=True)[:5]

    print("\n==== RESULTADO FINAL (CARTERA VIVA) ====")
    print(f"Capital inicial: {STARTING_CASH:.2f} USD")
    print(f"Capital final:   {cash:.2f} USD ({(cash/STARTING_CASH - 1)*100:+.2f}%)")
    print(f"Trades: {total_trades} | Ganadores: {winners} ({(winners/total_trades*100 if total_trades else 0):.1f}%) | Perdedores: {losers}")
    print(f"PnL total: {pnl_total:+.2f} USD | Profit Factor: {profit_factor:.2f}")
    print(f"Máx. Drawdown: {max_dd*100:.2f}%")
    print("Top 5 símbolos:")
    for s, v in top5:
        print(f"{s:<6} {v:+.2f}")

if __name__ == "__main__":
    simulate_portfolio()
