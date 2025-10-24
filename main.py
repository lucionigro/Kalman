from ib_insync import *
import pandas as pd
import numpy as np
import sqlite3
import math
import time
import os
import pytz
from datetime import datetime, timezone, time as dtime
from xml.etree import ElementTree as ET

"""
Main bot con mejoras de ejecución y riesgo para retail:
- Modos: BACKTEST | LIVE | PREOPEN
- PREOPEN: analiza última vela diaria cerrada y coloca MOO/LOO para la apertura.
- LIVE: loop con cierres/entradas, stops, kill-switch y reconcile de fills.
- Backtest de cartera con next-open fill, slippage, comisiones y RS.

Cambios clave respecto a la versión anterior:
  ✔ Toggle MOO/LOO con banda configurable.
  ✔ Stops: ATR trailing (sobre Supertrend) + Max Loss por trade. 
  ✔ Kill-switch por drawdown diario.
  ✔ Filtros de universo: precio mínimo y ADV$ mínimo.
  ✔ Filtro de earnings ±N días (best-effort via FundamentalData; si no hay datos, omite filtro).
  ✔ Filtro de régimen de mercado (SPY > SMA200) para entradas long.
  ✔ Reconciliación de fills al iniciar LIVE (actualiza precio_entrada con avgFillPrice real).
  ✔ Cache de históricos con prefetch y logs detallados.
  ✔ Sin datetime.utcnow() (usa timezone-aware UTC).
"""

# ===================== CONFIGURACIÓN GENERAL =====================
MODE = "LIVE"                   # "BACKTEST", "LIVE" o "PREOPEN"
MODE_N = MODE.strip().upper()
IS_BACKTEST = (MODE_N == "BACKTEST")
IS_LIVE = (MODE_N == "LIVE")

IB_PORT = 7497
IB_CLIENT_ID = 1
EXCHANGE = 'SMART'
CURRENCY = 'USD'

# Universo (puede reducirse para pruebas y calentar cache más rápido)
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
SYMBOLS = list(dict.fromkeys(SYMBOLS))  # dedupe manteniendo orden

# ===================== PARÁMETROS DE ESTRATEGIA =====================
PRICE_SOURCE       = "hl2"      # "hl2" o "close"
MEASUREMENT_NOISE  = 1.0
PROCESS_NOISE      = 0.01
ATR_PERIOD         = 14         # más estable que 1
ATR_FACTOR         = 1.5        # más conservador que 0.4
TIMEFRAME          = '1 day'    # swing sobre diaria

# ===================== RANKING / RS =====================
RS_BENCHMARK       = 'SPY'
RS_LOOKBACK_BARS   = 20
RS_MIN             = 0.03       # gating mínimo de RS (> 3% vs SPY)

# ===================== RIESGO / EJECUCIÓN =====================
MAX_OPEN_TRADES     = 5
BUDGET_PER_TRADE    = 2000.0      # para LIVE sizing por cash (fallback a BP)
RESERVE_CASH_PCT    = 0.05
USE_BUYING_POWER    = False
BP_TRADE_CAP        = 1800.0
BP_RESERVE_PCT      = 0.10

USE_LOO             = True       # True=Limit-On-Open, False=Market-On-Open
LOO_BAND_PCT_BUY    = 0.05       # BUY hasta +5% del cierre
LOO_BAND_PCT_SELL   = 0.05       # SELL hasta -5% del cierre

STOP_ATR_MULT       = 2.5        # trailing con ATR*mult (apoya al Supertrend)
MAX_LOSS_PCT        = 0.05       # hard stop vs entry
KILL_SWITCH_DD_PCT  = 0.02       # cierra todo si DD estimado del día <-2% del NLV

MIN_PRICE           = 5.0       # filtro universo: precio mínimo
ADV_MIN_USD         = 25_000_000 # filtro universo: dólar volumen promedio (≈20 días)

EARNINGS_FILTER_ENABLED = False
EARNINGS_DAYS_WINDOW   = 3       # excluir ±3 días de earnings

REQUIRE_MARKET_UPTREND = True    # filtra entradas long si mercado bajista
SMA_UPTREND_LEN        = 200

# ===================== BACKTEST =====================
BT_DURATION_STR   = '2 Y'
BT_USE_RTH        = True
COMMISSION_OPEN   = 1.0
COMMISSION_CLOSE  = 1.0
SLIPPAGE_PER_SH   = 0.02
RUN_PORTFOLIO_BT  = True
BT_STARTING_CASH  = 4_000.0

# ===================== CACHE =====================
CACHE_DIR = 'data_cache'
ENABLE_PREFETCH = True
PACING_SECONDS = 0.8
os.makedirs(CACHE_DIR, exist_ok=True)

# ===================== DB =====================
DB_FILE = 'trades.db'
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS operaciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT,
    tipo TEXT,
    cantidad REAL,
    precio_entrada REAL,
    precio_salida REAL,
    fecha_apertura TEXT,
    fecha_cierre TEXT,
    estado TEXT,
    pnl REAL,
    retorno_pct REAL,
    duracion_horas REAL,
    estrategia TEXT,
    comentario TEXT
)
''')
conn.commit()
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_op_open
ON operaciones(ticker, estado)
WHERE estado = 'ABIERTA';
""")
conn.commit()

# ===================== CONEXIÓN IBKR =====================
ib = IB()
ib.connect('127.0.0.1', IB_PORT, clientId=IB_CLIENT_ID)
resync_from_ibkr()
print(f"Conectado a IBKR | MODE={MODE}")

# ===================== HELPERS IBKR / CONTRATOS =====================
def resolve_contract(symbol: str) -> Contract | None:
    try:
        c = Stock(symbol, EXCHANGE, CURRENCY)
        q = ib.qualifyContracts(c)
        if q:
            return q[0]
    except Exception as e:
        print(f"[QUALIFY][{symbol}] {e}")
    for px in ("NYSE", "NASDAQ", "ARCA"):
        try:
            c = Stock(symbol, EXCHANGE, CURRENCY, primaryExchange=px)
            q = ib.qualifyContracts(c)
            if q:
                return q[0]
        except Exception:
            pass
    print(f"[QUALIFY][{symbol}] No se pudo calificar")
    return None


def ensure_ib_connection():
    global ib
    if ib is None or not ib.isConnected():
        try:
            ib.disconnect()
        except Exception:
            pass
        ib.connect('127.0.0.1', IB_PORT, clientId=IB_CLIENT_ID)
        print("[IBKR] Reconectado.")


def market_is_open() -> bool:
    now = datetime.now(pytz.timezone("US/Eastern")).time()
    return dtime(9, 30) <= now <= dtime(16, 0)

# ===================== INDICADORES =====================
def f_kalman_streaming(prices, measurement_noise=1.0, process_noise=0.01):
    state = float(prices.iloc[0])
    p = 1.0
    out = [state]
    for z in prices.iloc[1:]:
        p += process_noise
        k = p / (p + measurement_noise)
        state = state + k * (z - state)
        p = (1 - k) * p
        out.append(state)
    return pd.Series(out, index=prices.index)

def khma(series, length=1.0, process_noise=0.01):
    inner1 = f_kalman_streaming(series, length/2, process_noise)
    inner2 = f_kalman_streaming(series, length, process_noise)
    diff = 2*inner1 - inner2
    return f_kalman_streaming(diff, math.sqrt(length), process_noise)

def rma(series, length):
    return series.ewm(alpha=1/length, adjust=False).mean()

def true_range(df):
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - df['close'].shift(1)).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def resync_from_ibkr():
    print(f"VERIFICANDO POSICIONES EN IBKR")
    for p in ib.positions():
        sym = p.contract.symbol
        qty = int(p.position)
        if qty == 0:
            continue
        avg_cost = float(p.avgCost)
        if not symbol_has_open(sym):
            insert_trade_open_db(sym, qty, avg_cost, comentario='Resync from IBKR')
            print(f"[RESYNC] {sym} x{qty} @ {avg_cost:.2f}")


def supertrend_backquant(df, factor=1.5, atr_period=14, src_col="kalman_hma"):
    df['tr'] = true_range(df)
    df['atr'] = rma(df['tr'], max(1, atr_period))
    upper = df[src_col] + factor * df['atr']
    lower = df[src_col] - factor * df['atr']

    supertrend = np.full(len(df), np.nan)
    direction = np.zeros(len(df))
    upper = upper.copy(); lower = lower.copy()

    for i in range(1, len(df)):
        prev_lower = lower.iloc[i-1]
        prev_upper = upper.iloc[i-1]
        prev_super = supertrend[i-1]
        if not (lower.iloc[i] > prev_lower or df['close'].iloc[i-1] < prev_lower):
            lower.iloc[i] = prev_lower
        if not (upper.iloc[i] < prev_upper or df['close'].iloc[i-1] > prev_upper):
            upper.iloc[i] = prev_upper
        if np.isnan(df['atr'].iloc[i-1]):
            direction[i] = 1
            supertrend[i] = upper.iloc[i]
            continue
        if prev_super == prev_upper:
            direction[i] = -1 if df['close'].iloc[i] > upper.iloc[i] else 1
        else:
            direction[i] = 1 if df['close'].iloc[i] < lower.iloc[i] else -1
        supertrend[i] = lower.iloc[i] if direction[i] == -1 else upper.iloc[i]
    df['supertrend'] = supertrend
    df['direction'] = direction
    return df

# ===================== CACHE =====================
def _cache_path(symbol: str, durationStr: str, barSize: str, useRTH: bool, ext="parquet") -> str:
    safe = lambda s: str(s).replace(" ", "")
    return os.path.join(CACHE_DIR, f"{symbol}_{safe(durationStr)}_{safe(barSize)}_{'RTH' if useRTH else 'ALL'}.{ext}")

def _load_cached(symbol, durationStr, barSize, useRTH):
    p = _cache_path(symbol, durationStr, barSize, useRTH, 'parquet')
    if os.path.exists(p):
        try:
            df = pd.read_parquet(p)
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            pass
    p = _cache_path(symbol, durationStr, barSize, useRTH, 'csv')
    if os.path.exists(p):
        try:
            return pd.read_csv(p, parse_dates=['date'])
        except Exception:
            pass
    return None

def _save_cache(df, symbol, durationStr, barSize, useRTH):
    cols = [c for c in ['date','open','high','low','close','volume'] if c in df.columns]
    df2 = df[cols].copy()
    try:
        p = _cache_path(symbol, durationStr, barSize, useRTH, 'parquet'); tmp=p+'.tmp'
        df2.to_parquet(tmp, index=False); os.replace(tmp, p); return
    except Exception:
        p = _cache_path(symbol, durationStr, barSize, useRTH, 'csv'); tmp=p+'.tmp'
        df2.to_csv(tmp, index=False); os.replace(tmp, p)

# ===================== HISTÓRICO + FEATURES =====================
def _get_history_ib(contract, durationStr, barSize, useRTH, retries=3, base_sleep=1.2):
    for i in range(retries):
        try:
            bars = ib.reqHistoricalData(contract, '', durationStr, barSize, 'TRADES', useRTH, 1)
            return util.df(bars)
        except Exception as e:
            wait = base_sleep * (2**i)
            print(f"[HIST][WARN] {contract.localSymbol}: {e} (retry {i+1}/{retries} {wait:.1f}s)")
            time.sleep(wait); ensure_ib_connection()
    return 

def refresh_cache_incremental(symbol, durationStr, barSize='1 day', useRTH=True, max_back_days=10):
    """
    Actualiza el cache 'data_cache' solo con velas faltantes.
    """
    cached = _load_cached(symbol, durationStr, barSize, useRTH)
    if cached is None or cached.empty:
        ensure_ib_connection()
        c = resolve_contract(symbol)
        if c is None:
            print(f"[REFRESH][{symbol}] sin contrato IBKR")
            return None
        df = _get_history_ib(c, durationStr, barSize, useRTH)
        if df is None or df.empty:
            print(f"[REFRESH][{symbol}] sin datos iniciales")
            return None
        # forzar timezone UTC
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df = df.drop_duplicates('date').sort_values('date', ignore_index=True)
        _save_cache(df, symbol, durationStr, barSize, useRTH)
        print(f"[REFRESH][{symbol}] cache inicial creado ({len(df)} velas)")
        time.sleep(PACING_SECONDS)
        return df

    # --- cache caliente ---
    cached['date'] = pd.to_datetime(cached['date'], utc=True)   # <<--- 🔥 fuerza UTC
    cached = cached.drop_duplicates('date').sort_values('date', ignore_index=True)
    last_dt = cached['date'].iloc[-1]
    now_et = pd.Timestamp(datetime.now(pytz.timezone('US/Eastern'))).tz_convert('UTC')  # 🔥 también en UTC

    # Heurística: ¿hay vela nueva?
    if barSize == '1 day':
        need_update = now_et.normalize() > last_dt.normalize()
    else:
        need_update = (now_et - last_dt) > pd.Timedelta(hours=1)

    if not need_update:
        return cached

    # Calcular días a descargar (corrigiendo tipo)
    days_gap = max(1, int((now_et.date() - last_dt.date()).days) + 2)
    days_pull = min(max_back_days, days_gap)

    ensure_ib_connection()
    c = resolve_contract(symbol)
    if c is None:
        print(f"[REFRESH][{symbol}] no se pudo calificar contrato")
        return cached

    tail = _get_history_ib(c, f'{days_pull} D', barSize, useRTH)
    tail['date'] = pd.to_datetime(tail['date'], utc=True)

    if tail is None or tail.empty:
        print(f"[REFRESH][{symbol}] sin nuevas velas")
        return cached

    tail = tail.drop_duplicates('date').sort_values('date', ignore_index=True)
    merged = pd.concat([cached, tail], ignore_index=True)
    merged = merged.drop_duplicates('date').sort_values('date', ignore_index=True)

    added = len(merged) - len(cached)
    _save_cache(merged, symbol, durationStr, barSize, useRTH)
    print(f"[REFRESH][{symbol}] cache actualizado (+{added} nuevas velas, última={merged['date'].iloc[-1].date()})")
    time.sleep(PACING_SECONDS)
    return merged



def fetch_history(symbol, durationStr, barSize='1 day', useRTH=True, use_cache=True, refresh_cache=False):
    # 1) Traer/crear cache
    raw = None
    if use_cache and not refresh_cache:
        raw = _load_cached(symbol, durationStr, barSize, useRTH)

    if refresh_cache:
        raw = refresh_cache_incremental(symbol, durationStr, barSize, useRTH)
    elif raw is None:
        # cache ausente → descarga completa
        ensure_ib_connection()
        c = resolve_contract(symbol)
        if c is None: return None
        df = _get_history_ib(c, durationStr, barSize, useRTH)
        if df is None or df.empty: return None
        df = df.drop_duplicates('date').sort_values('date', ignore_index=True)
        _save_cache(df, symbol, durationStr, barSize, useRTH)
        raw = df
        time.sleep(PACING_SECONDS)
    else:
        raw = raw.drop_duplicates('date').sort_values('date', ignore_index=True)

    # 2) Features/indicadores (idéntico a lo que ya tenés)
    df = raw.copy()
    df['src'] = (df['high'] + df['low'])/2.0 if PRICE_SOURCE=='hl2' else df['close']
    df['kalman_hma'] = khma(df['src'], MEASUREMENT_NOISE, PROCESS_NOISE)
    df = supertrend_backquant(df, factor=ATR_FACTOR, atr_period=ATR_PERIOD, src_col='kalman_hma')
    df['signal'] = 0
    cross_long  = (df['direction'].shift(1) > 0) & (df['direction'] < 0)
    cross_short = (df['direction'].shift(1) < 0) & (df['direction'] > 0)
    df.loc[cross_long, 'signal'] = 1
    df.loc[cross_short, 'signal'] = -1
    return df


# ===================== RS / RÉGIMEN / FILTROS =====================
def _rs20_score(df_sym: pd.DataFrame, df_bench: pd.DataFrame | None) -> float:
    try:
        if df_sym is None or len(df_sym) < 21: return -1e9
        r_t = float(df_sym['close'].iloc[-1] / df_sym['close'].iloc[-20] - 1.0)
        if df_bench is None or len(df_bench) < 21: return r_t
        r_b = float(df_bench['close'].iloc[-1] / df_bench['close'].iloc[-20] - 1.0)
        return r_t - r_b
    except Exception:
        return -1e9

def rank_candidates_rs20(symbols: list[str], timeframe: str) -> list[str]:
    bench = fetch_history(RS_BENCHMARK, '2 M', timeframe, True, use_cache=True) if RS_BENCHMARK else None
    scored = []
    for i, sym in enumerate(symbols, 1):
        df = fetch_history(sym, '2 M', TIMEFRAME, True, use_cache=True, refresh_cache=True)
        if df is None or df.empty: continue
        score = _rs20_score(df, bench)
        scored.append((sym, score))
        if i % 25 == 0: print(f"[PREOPEN][RS] rankeados {i}/{len(symbols)}...")
    scored.sort(key=lambda x: x[1], reverse=True)
    return [s for s,_ in scored]


def avg_dollar_volume_usd(symbol: str, lookback_days: int = 20) -> float:
    df = fetch_history(symbol, '3 M', '1 day', True, use_cache=True)
    if df is None or len(df) < lookback_days+1: return 0.0
    tail = df.iloc[-lookback_days:]
    return float((tail['close'] * tail['volume']).mean())


def get_last_close(symbol: str) -> float:
    df = fetch_history(symbol, '2 M', '1 day', True, use_cache=True)
    if df is None or df.empty: return 0.0
    return float(df['close'].iloc[-1])


def market_uptrend_ok() -> bool:
    if not REQUIRE_MARKET_UPTREND: return True
    df = fetch_history(RS_BENCHMARK, '2 Y', '1 day', True, use_cache=True)
    if df is None or len(df) < SMA_UPTREND_LEN+1: return True
    sma = df['close'].rolling(SMA_UPTREND_LEN).mean()
    return bool(df['close'].iloc[-1] > sma.iloc[-1])

# ===================== EARNINGS (best-effort) =====================
def _parse_calendar_dates(xml_str: str) -> list[datetime]:
    try:
        root = ET.fromstring(xml_str)
        out = []
        for elem in root.iter():
            tag = elem.tag.lower()
            if 'earn' in tag and ('date' in tag or tag.endswith('date')):
                try:
                    d = pd.to_datetime(elem.text).to_pydatetime()
                    out.append(d)
                except Exception:
                    pass
        return out
    except Exception:
        return []


def is_in_earnings_window(symbol: str, window_days: int = EARNINGS_DAYS_WINDOW) -> bool:
    if not EARNINGS_FILTER_ENABLED: return False
    try:
        c = resolve_contract(symbol)
        if c is None: return False
        xml = ib.reqFundamentalData(c, reportType='CalendarReport')
        if not xml: return False
        dates = _parse_calendar_dates(xml)
        if not dates: return False
        today = datetime.now(timezone.utc)
        for d in dates:
            # comparar en días absolutos
            if abs((d - today).days) <= window_days:
                return True
        return False
    except Exception:
        # si no hay permiso/no disponible, no bloquea
        return False

# ===================== DB OPS =====================
def insert_trade_open_db(symbol: str, qty: int, entry_price: float, comentario: str):
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT INTO operaciones
        (ticker, tipo, cantidad, precio_entrada, precio_salida,
         fecha_apertura, fecha_cierre, estado, pnl, retorno_pct,
         duracion_horas, estrategia, comentario)
        VALUES (?, 'LONG', ?, ?, NULL, ?, NULL, 'ABIERTA', NULL, NULL, NULL, ?, ?)
    """, (symbol, qty, entry_price, now, 'KalmanHullST_BackQuant', comentario))
    conn.commit()


def get_open_trade_info(symbol: str):
    cur.execute("""
        SELECT id, cantidad, precio_entrada, fecha_apertura
        FROM operaciones
        WHERE ticker=? AND estado='ABIERTA'
        ORDER BY id DESC
        LIMIT 1
    """, (symbol,))
    return cur.fetchone()


def open_trades_count() -> int:
    cur.execute("SELECT COUNT(*) FROM operaciones WHERE estado='ABIERTA'")
    x = cur.fetchone(); return int(x[0]) if x and x[0] is not None else 0


def symbol_has_open(symbol: str) -> bool:
    cur.execute("SELECT 1 FROM operaciones WHERE ticker=? AND estado='ABIERTA' LIMIT 1", (symbol,))
    return cur.fetchone() is not None

def get_open_symbols_db() -> list[str]:
    cur.execute("SELECT ticker FROM operaciones WHERE estado='ABIERTA'")
    rows = cur.fetchall()
    return [r[0] for r in rows] if rows else []



def close_trade_db(symbol: str, exit_price: float, comentario_extra: str = 'LIVE CLOSE'):
    row = get_open_trade_info(symbol)
    if not row:
        print(f"[DB] No ABIERTA para {symbol}"); return
    op_id, qty, entry_price, fecha_apertura = row
    if qty is None or entry_price is None or fecha_apertura is None:
        print(f"[DB] Datos incompletos {symbol}"); return
    now_iso = datetime.now(timezone.utc).isoformat()
    pnl = (exit_price - float(entry_price)) * float(qty) - COMMISSION_CLOSE
    retorno_pct = (exit_price / float(entry_price) - 1.0) * 100.0
    try:
        dur_h = (pd.to_datetime(now_iso) - pd.to_datetime(fecha_apertura)).total_seconds() / 3600.0
    except Exception:
        dur_h = None
    cur.execute("""
        UPDATE operaciones
        SET precio_salida=?, fecha_cierre=?, estado='CERRADA',
            pnl=?, retorno_pct=?, duracion_horas=?,
            comentario=COALESCE(comentario,'') || ' | ' || ?
        WHERE id=?
    """, (exit_price, now_iso, pnl, retorno_pct, dur_h, comentario_extra, op_id))
    conn.commit()

# ===================== CUENTA / SIZING =====================
def _acct_val(tag: str, currency: str = "USD") -> float:
    try:
        for x in ib.accountSummary():
            if x.tag == tag and x.currency == currency:
                return float(x.value)
    except Exception:
        pass
    return 0.0

def get_available_funds_usd() -> float:
    v = _acct_val("AvailableFunds", "USD")
    if v == 0.0: v = _acct_val("TotalCashValue", "USD")
    return max(0.0, v)

def get_buying_power_usd() -> float:
    return max(0.0, _acct_val("BuyingPower", "USD"))

def get_net_liq_usd() -> float:
    v = _acct_val("NetLiquidation", "USD")
    if v == 0.0: v = _acct_val("TotalCashValue", "USD")
    return max(0.0, v)

def calc_qty_by_cash(price: float, available_usd: float) -> int:
    if price <= 0: return 0
    budget_eff = min(BUDGET_PER_TRADE, max(0.0, available_usd * (1.0 - RESERVE_CASH_PCT)))
    if budget_eff <= COMMISSION_OPEN: return 0
    return max(0, math.floor((budget_eff - COMMISSION_OPEN) / (price + SLIPPAGE_PER_SH)))

def calc_qty_by_bp(price: float, buying_power_usd: float) -> int:
    if price <= 0: return 0
    bp_eff = min(BP_TRADE_CAP, max(0.0, buying_power_usd * (1.0 - BP_RESERVE_PCT)))
    if bp_eff <= COMMISSION_OPEN: return 0
    return max(0, math.floor((bp_eff - COMMISSION_OPEN) / (price + SLIPPAGE_PER_SH)))

# ===================== PREOPEN: COLOCAR ÓRDENES PARA APERTURA =====================
VERBOSE_PREOPEN = True

def _place_MOO(contract: Contract, action: str, qty: int):
    o = MarketOrder(action, qty); o.tif='OPG'; o.outsideRth=False; ib.placeOrder(contract, o)

def _place_LOO(contract: Contract, action: str, qty: int, lmt: float):
    o = LimitOrder(action, qty, lmt); o.tif='OPG'; o.outsideRth=False; ib.placeOrder(contract, o)


def _passes_universe_filters(sym: str) -> tuple[bool, str, float]:
    px = get_last_close(sym)
    if px <= 0: return (False, "sin_datos", px)
    if px < MIN_PRICE: return (False, f"precio<{MIN_PRICE}", px)
    adv = avg_dollar_volume_usd(sym, 20)
    if adv < ADV_MIN_USD: return (False, f"ADV${adv:,.0f}<{ADV_MIN_USD:,.0f}", px)
    if is_in_earnings_window(sym, EARNINGS_DAYS_WINDOW):
        return (False, "earnings_window", px)
    return (True, "ok", px)


def queue_orders_for_next_open():
    print(" [PREOPEN] Preparando órdenes para la próxima apertura según la última vela cerrada...")

    # Cierres primero
    open_syms = [s for s in get_open_symbols_db()]
    for sym in open_syms:
        df = fetch_history(sym, '2 M', TIMEFRAME, True, use_cache=True, refresh_cache=True)
        if df is None or df.empty: continue
        last = df.iloc[-1]
        sig = int(last['signal']); direction = int(last['direction']); close_px = float(last['close'])
        if (sig == -1) or (direction == 1):
            info = get_open_trade_info(sym)
            if not info: continue
            _, qty_open, _, _ = info
            qty = int(qty_open) if qty_open else 0
            if qty < 1: continue
            c = resolve_contract(sym)
            if c is None: continue
            if USE_LOO:
                lmt = close_px * (1.0 - LOO_BAND_PCT_SELL)
                print(f"[PREOPEN] SELL LOO OPG: {sym} x{qty} @>= {lmt:.2f}")
                _place_LOO(c, 'SELL', qty, lmt)
            else:
                print(f"[PREOPEN] SELL MOO OPG: {sym} x{qty}")
                _place_MOO(c, 'SELL', qty)
            cur.execute("""
                UPDATE operaciones
                SET comentario=COALESCE(comentario,'') || ' | PREOPEN: SELL OPG queued'
                WHERE ticker=? AND estado='ABIERTA'
            """, (sym,)); conn.commit()
        elif VERBOSE_PREOPEN:
            print(f"[PREOPEN][HOLD] {sym}: sig={sig}, dir={direction}")

    # Entradas
    if REQUIRE_MARKET_UPTREND and not market_uptrend_ok():
        print("[PREOPEN] Mercado NO en uptrend (SPY vs SMA200). No se colocan nuevas entradas.")
        return

    current_open = open_trades_count()
    free_slots = max(0, MAX_OPEN_TRADES - current_open)
    if free_slots <= 0:
        print(f"[PREOPEN] Cupo completo {current_open}/{MAX_OPEN_TRADES}. Sin nuevas entradas.")
        return

    base = [s for s in SYMBOLS if not symbol_has_open(s)]
    # filtros de universo
    filtered = []
    for s in base:
        ok, why, px = _passes_universe_filters(s)
        if ok:
            filtered.append(s)
        elif VERBOSE_PREOPEN:
            print(f"[PREOPEN][SKIP] {s}: {why}")

    if not filtered:
        print("[PREOPEN] No quedan candidatos tras filtros de universo.")
        return

    ranked = rank_candidates_rs20(filtered, TIMEFRAME)

    for sym in ranked:
        if open_trades_count() >= MAX_OPEN_TRADES: break
        df = fetch_history(sym, '2 M', TIMEFRAME, True, use_cache=True, refresh_cache=True)
        if df is None or df.empty: continue
        last = df.iloc[-1]
        sig = int(last['signal']); direction = int(last['direction']); close_px = float(last['close'])
        rs_val = _rs20_score(df, fetch_history(RS_BENCHMARK, '2 M', TIMEFRAME, True, use_cache=True))
        if pd.isna(rs_val) or (rs_val <= RS_MIN):
            if VERBOSE_PREOPEN: print(f"[PREOPEN][SKIP RS] {sym}: rs={rs_val:.4f}")
            continue
        if (sig == 1) or (direction == -1):
            avail_cash = get_available_funds_usd()
            qty = calc_qty_by_cash(close_px, avail_cash); src = 'CASH'
            if qty < 1 and USE_BUYING_POWER:
                bp = get_buying_power_usd(); qbp = calc_qty_by_bp(close_px, bp)
                if qbp >= 1: qty = qbp; src = 'BP'
            if qty < 1:
                print(f"[PREOPEN][SKIP $] {sym}: sin saldo suficiente.")
                continue
            c = resolve_contract(sym)
            if c is None: continue
            if USE_LOO:
                lmt = close_px * (1.0 + LOO_BAND_PCT_BUY)
                print(f"[PREOPEN] BUY LOO OPG: {sym} x{qty} @<= {lmt:.2f} (via {src}) | rs={rs_val:.4f}")
                _place_LOO(c, 'BUY', qty, lmt)
            else:
                print(f"[PREOPEN] BUY MOO OPG: {sym} x{qty} (via {src}) | rs={rs_val:.4f}")
                _place_MOO(c, 'BUY', qty)
            insert_trade_open_db(sym, qty, entry_price=close_px, comentario='PREOPEN OPG queued')
        elif VERBOSE_PREOPEN:
            print(f"[PREOPEN][NO-ENTRY] {sym}: sig={sig} dir={direction}")

# ===================== RECONCILIAR FILLS (LIVE) =====================
def reconcile_fills_update_db():
    """
    Sincroniza los fills ejecutados en IBKR con la base local:
      - Actualiza precios de entrada si hubo BUY fills.
      - Cierra operaciones ABIERTAS si se detecta una venta total.
      - Reduce cantidad si se trata de una venta parcial.
      - Marca comentarios claros ('STOP BE fill', 'SELL parcial fill').
    """
    try:
        fills = ib.fills()  # lista de Fill objects recientes
    except Exception as e:
        print(f"[RECONCILE][WARN] {e}")
        return

    if not fills:
        print("[RECONCILE] Sin fills para actualizar.")
        return

    updated_entry = 0
    closed_total = 0
    closed_partial = 0

    for f in fills:
        try:
            sym = f.contract.symbol
            avg = float(f.execution.avgPrice or f.execution.price)
            side = f.execution.side.upper()
            shares = int(abs(f.execution.shares))
            fill_time = f.execution.time

            # === Actualización de BUY / entrada ===
            if side in ('BOT', 'BUY'):
                row = get_open_trade_info(sym)
                if row:
                    op_id, qty, entry_price, _ = row
                    if entry_price is not None and abs(avg - float(entry_price)) > 1e-6:
                        cur.execute("UPDATE operaciones SET precio_entrada=? WHERE id=?", (avg, op_id))
                        conn.commit()
                        updated_entry += 1
                        print(f"[RECONCILE][BUY] {sym} actualizado entry @ {avg:.2f}")

            # === Manejo de ventas / stops ===
            if side in ('SLD', 'SELL'):
                cur.execute("""
                    SELECT id, cantidad, precio_entrada
                    FROM operaciones
                    WHERE ticker=? AND estado='ABIERTA'
                """, (sym,))
                row = cur.fetchone()
                if not row:
                    continue

                op_id, qty_db, entry_px = row
                qty_db = int(qty_db or 0)
                if qty_db <= 0:
                    continue

                if shares >= qty_db:
                    # 🟥 Venta total → cerrar operación
                    cur.execute("""
                        UPDATE operaciones
                        SET precio_salida=?, fecha_cierre=datetime('now'),
                            estado='CERRADA',
                            comentario=COALESCE(comentario,'') || ' | STOP BE fill'
                        WHERE id=?
                    """, (avg, op_id))
                    conn.commit()
                    closed_total += 1
                    print(f"[RECONCILE][STOP] {sym} cerrado totalmente @ {avg:.2f}")
                else:
                    # 🟨 Venta parcial → actualizar cantidad restante
                    nueva_cant = qty_db - shares
                    cur.execute("""
                        UPDATE operaciones
                        SET cantidad=?,
                            comentario=COALESCE(comentario,'') || ' | SELL parcial fill'
                        WHERE id=?
                    """, (nueva_cant, op_id))
                    conn.commit()
                    closed_partial += 1
                    print(f"[RECONCILE][PARTIAL] {sym} venta parcial ({shares}/{qty_db}) @ {avg:.2f} → quedan {nueva_cant}")

        except Exception as e:
            print(f"[RECONCILE][ERROR] {e}")

    print(f"[RECONCILE] Entradas actualizadas: {updated_entry} | Cierres totales: {closed_total} | Parciales: {closed_partial}")


# ===================== STOPS / LIVE ANALYSIS =====================

def _should_stop(symbol: str, df: pd.DataFrame, entry_px: float) -> bool:
    last = df.iloc[-1]
    close_px = float(last['close'])
    # Hard stop por %
    if close_px <= entry_px * (1.0 - MAX_LOSS_PCT):
        return True
    # Trailing por Supertrend (si pasa por debajo de la línea en long)
    st = float(last['supertrend']) if not pd.isna(last['supertrend']) else None
    if st is not None and close_px < st:
        return True
    # ATR trailing opcional (si querés más estricto: entry - ATR*mult)
    atr = float(last['atr']) if 'atr' in last.index and not pd.isna(last['atr']) else None
    if atr is not None and close_px <= (entry_px - STOP_ATR_MULT * atr):
        return True
    return False


def analyze_symbol_live(symbol):
    df = fetch_history(symbol, '2 M', TIMEFRAME, True, use_cache=False)
    if df is None or df.empty:
        print(f"[LIVE][WARN] Sin datos {symbol}")
        return

    last = df.iloc[-1]
    sig = int(last['signal'])
    direction = int(last['direction'])
    close_px = float(last['close'])
    is_open = symbol_has_open(symbol)

    # =============== POSICIONES ABIERTAS ===============
    if is_open:
        info = get_open_trade_info(symbol)
        if not info:
            print(f"[LIVE][DB] sin info de ABIERTA {symbol}")
            return
        _, qty, entry_px, _ = info
        qty = int(qty) if qty else 0
        if qty < 1:
            print(f"[LIVE][DB] qty inválida {symbol}")
            return

        gain_pct = (close_px / float(entry_px) - 1.0) * 100.0

        # 🟢 Take Profit parcial al +8% y Stop BE
        if gain_pct >= 8.0 and qty > 1:
            half_qty = qty // 2
            remaining_qty = qty - half_qty
            c = resolve_contract(symbol)
            if c:
                print(f"[LIVE][TP] {symbol}: +{gain_pct:.2f}% → vendiendo {half_qty} y colocando stop BE")
                # 1️⃣ Vende mitad
                ib.placeOrder(c, MarketOrder('SELL', half_qty))
                close_trade_db(symbol, exit_price=close_px, comentario_extra='TP parcial 8%')
                # 2️⃣ Reinsertar la mitad restante
                insert_trade_open_db(symbol, remaining_qty, entry_price=float(entry_px), comentario='Reentry BE')
                # 3️⃣ Colocar Stop en Break-Even
                stop_order = StopOrder('SELL', remaining_qty, stopPrice=float(entry_px))
                ib.placeOrder(c, stop_order)
                print(f"[LIVE][STOP] Stop BE colocado @ {entry_px:.2f} por {remaining_qty} acciones")
            return  # corta acá si hubo TP parcial

        # 🔴 Stop o señal opuesta (evaluado siempre)
        if _should_stop(symbol, df, float(entry_px)) or (sig == -1) or (direction == 1):
            c = resolve_contract(symbol)
            if c is None:
                return
            print(f"[LIVE] CLOSE {symbol} x{qty} (stop/signal)")
            ib.placeOrder(c, MarketOrder('SELL', qty))
            close_trade_db(symbol, exit_price=close_px, comentario_extra='LIVE stop/signal')
        else:
            print(f"[LIVE] HOLD {symbol} | sig={sig} dir={direction} | +{gain_pct:.2f}%")
        return

    # =============== ENTRADAS NUEVAS ===============
    if open_trades_count() >= MAX_OPEN_TRADES:
        print(f"[LIVE] Cupo lleno {open_trades_count()}/{MAX_OPEN_TRADES}")
        return

    if REQUIRE_MARKET_UPTREND and not market_uptrend_ok():
        print("[LIVE] Mercado no en uptrend. No nuevas entradas.")
        return

    ok, why, px = _passes_universe_filters(symbol)
    if not ok:
        print(f"[LIVE][SKIP] {symbol}: {why}")
        return

    if (sig == 1) or (direction == -1):
        avail_cash = get_available_funds_usd()
        qty = calc_qty_by_cash(px, avail_cash)
        src = 'CASH'
        if qty < 1 and USE_BUYING_POWER:
            bp = get_buying_power_usd()
            qbp = calc_qty_by_bp(px, bp)
            if qbp >= 1:
                qty = qbp
                src = 'BP'
        if qty < 1:
            print(f"[LIVE] sin saldo para {symbol}")
            return
        c = resolve_contract(symbol)
        if c is None:
            return
        print(f"[LIVE] BUY {symbol} x{qty} ({src})")
        ib.placeOrder(c, MarketOrder('BUY', qty))
        insert_trade_open_db(symbol, qty, entry_price=px, comentario='LIVE entry')
    else:
        print(f"[LIVE] No-Entry {symbol} | sig={sig} dir={direction}")


# ===================== KILL-SWITCH (DD diario) =====================

def _daily_pnl_estimate() -> float:
    # Realizado hoy (CERRADA con fecha de hoy)
    today = datetime.now(timezone.utc).date()
    cur.execute("""
        SELECT COALESCE(SUM(pnl),0) FROM operaciones
        WHERE estado='CERRADA' AND DATE(fecha_cierre) = DATE(?)
    """, (today.isoformat(),))
    realized = float(cur.fetchone()[0])
    # No realizado aprox (open vs entry al último close)
    cur.execute("SELECT ticker, cantidad, precio_entrada FROM operaciones WHERE estado='ABIERTA'")
    rows = cur.fetchall()
    unreal = 0.0
    for t, q, e in rows:
        last_px = get_last_close(t)
        if last_px and q and e:
            unreal += (last_px - float(e)) * float(q)
    return realized + unreal


def kill_switch_check_and_close_all():
    nlv = get_net_liq_usd(); limit_dd = -abs(KILL_SWITCH_DD_PCT) * nlv
    est = _daily_pnl_estimate()
    if est <= limit_dd:
        print(f"[KILL-SWITCH] DD estimado {est:.2f} <= {limit_dd:.2f}. Cerrando TODO.")
        cur.execute("SELECT ticker, cantidad FROM operaciones WHERE estado='ABIERTA'")
        rows = cur.fetchall()
        for t, q in rows:
            try:
                if not q or int(q) < 1: continue
                c = resolve_contract(t)
                if c is None: continue
                ib.placeOrder(c, MarketOrder('SELL', int(q)))
                px = get_last_close(t) or 0.0
                close_trade_db(t, exit_price=px, comentario_extra='KILL-SWITCH')
                time.sleep(0.2)
            except Exception as e:
                print(f"[KILL-SWITCH][{t}] {e}")
        return True
    return False



# ===================== PREFETCH =====================

def prefetch_universe(symbols: list[str], durationStr: str, barSize: str, useRTH: bool):
    print(f"\n[PREFETCH] Cacheando/actualizando {len(symbols)} símbolos @ {durationStr}/{barSize} ...")
    ok, fail = 0, 0
    for s in symbols:
        try:
            _ = fetch_history(s, durationStr, barSize, useRTH, use_cache=True, refresh_cache=True)
            ok += 1
        except Exception as e:
            print(f"[PREFETCH][{s}] {e}"); fail += 1
    print(f"[PREFETCH] OK={ok} | FAIL={fail}\n")


# ===================== BACKTEST (por símbolo y cartera) =====================


# ===================== BACKTEST (por símbolo y cartera) =====================

def backtest_symbol(symbol):
    print(f"\n[BT] {symbol} | {BT_DURATION_STR} @ {TIMEFRAME} ...")
    df = fetch_history(symbol, BT_DURATION_STR, TIMEFRAME, BT_USE_RTH, use_cache=True)
    if df is None or len(df) < 50:
        print(f"[BT][WARN] {symbol}: sin datos suficientes.")
        return {'symbol': symbol, 'trades': 0, 'wins': 0, 'wr': 0.0, 'pf': 0.0, 'ret_pct_avg': 0.0, 'pnl': 0.0}
    pos_open=False; qty=0; entry_price=None; entry_time=None
    trades=[]; gp=0.0; gl=0.0
    for i in range(1, len(df)-1):
        sig=int(df['signal'].iloc[i]); direction=int(df['direction'].iloc[i])
        if not pos_open and (sig==1 or direction==-1):
            price_next_open = float(df['open'].iloc[i+1]) + SLIPPAGE_PER_SH
            budget_eff = max(0.0, BUDGET_PER_TRADE - COMMISSION_OPEN)
            qty = max(1, math.floor(budget_eff / price_next_open))
            if qty<1: continue
            entry_price = price_next_open; entry_time = df['date'].iloc[i+1]; pos_open=True
        elif pos_open and (sig==-1 or direction==1):
            exit_price = float(df['open'].iloc[i+1]) + SLIPPAGE_PER_SH
            pnl = (exit_price - entry_price)*qty - COMMISSION_CLOSE
            ret = (exit_price/entry_price - 1)*100.0
            trades.append({'entry_dt':entry_time,'exit_dt':df['date'].iloc[i+1],'entry':entry_price,'exit':exit_price,'qty':qty,'pnl':pnl,'ret_pct':ret})
            if pnl>=0: gp+=pnl
            else: gl+=-pnl
            pos_open=False; qty=0; entry_price=None; entry_time=None
    if pos_open and entry_price is not None:
        exit_price = float(df['close'].iloc[-1])
        pnl = (exit_price - entry_price)*qty - COMMISSION_CLOSE
        ret = (exit_price/entry_price - 1)*100.0
        trades.append({'entry_dt':entry_time,'exit_dt':df['date'].iloc[-1],'entry':entry_price,'exit':exit_price,'qty':qty,'pnl':pnl,'ret_pct':ret})
        if pnl>=0: gp+=pnl
        else: gl+=-pnl
    n=len(trades); wins=sum(1 for t in trades if t['pnl']>=0)
    wr=(wins/max(1,n))*100.0; ret_avg=np.mean([t['ret_pct'] for t in trades]) if n else 0.0
    pf=(gp/gl) if gl>0 else (float('inf') if gp>0 else 0.0); total_pnl=sum(t['pnl'] for t in trades)
    print(f"[BT] {symbol} → trades={n} | win%={wr:.1f} | pf={pf:.2f} | avg%={ret_avg:.2f} | pnl=${total_pnl:.2f}")
    return {'symbol':symbol,'trades':n,'wins':wins,'wr':wr,'pf':pf,'ret_pct_avg':ret_avg,'pnl':total_pnl}


def backtest_portfolio(symbols: list[str], durationStr: str, barSize: str, useRTH: bool):
    print(f"\n=== BACKTEST CARTERA | {durationStr}/{barSize} ===")

    # --- Carga de datos ---
    data = {}
    for s in symbols:
        df = fetch_history(s, durationStr, barSize, useRTH, use_cache=True)
        if df is None or len(df) < RS_LOOKBACK_BARS + 5:
            continue
        df = df.drop_duplicates('date').reset_index(drop=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        data[s] = df
    if not data:
        print("[PORT] Sin datos válidos.")
        return

    print(f"[PORT] Símbolos cargados: {len(data)} / {len(symbols)}")

    # --- Inicialización ---
    cash = BT_STARTING_CASH
    positions = {}
    closed = []

    # --- Loop principal (día a día sincronizado por fecha mínima común) ---
    # Crear set de fechas comunes entre todos los símbolos
    all_dates = sorted(set().union(*[set(df['date']) for df in data.values()]))

    for i in range(1, len(all_dates)):
        t_prev, t = all_dates[i - 1], all_dates[i]

        # === Cierres ===
        for s, pos in list(positions.items()):
            df = data[s]
            if t not in set(df['date']):
                continue
            k = df.index[df['date'] == t_prev]
            k1 = df.index[df['date'] == t]
            if len(k) == 0 or len(k1) == 0:
                continue
            k, k1 = k[0], k1[0]

            # cerrar si hay señal de salida
            if df.loc[k, 'signal'] == -1:
                exit_px = float(df.loc[k1, 'open']) + SLIPPAGE_PER_SH
                pnl = (exit_px - pos['entry_px']) * pos['qty'] - COMMISSION_CLOSE
                cash += exit_px * pos['qty']
                closed.append({
                    'symbol': s,
                    'entry_px': pos['entry_px'],
                    'exit_px': exit_px,
                    'qty': pos['qty'],
                    'entry_dt': pos['entry_dt'],
                    'exit_dt': df.loc[k1, 'date'],
                    'pnl': pnl
                })
                print(f"[PORT][CLOSE] {s} x{pos['qty']} @ {exit_px:.2f} | pnl=${pnl:.2f} | cash=${cash:.2f}")
                del positions[s]

        # === Entradas ===
        free_slots = max(0, MAX_OPEN_TRADES - len(positions))
        if free_slots <= 0:
            continue

        candidates = []
        for s, df in data.items():
            if s in positions:
                continue
            if t not in set(df['date']):
                continue
            k = df.index[df['date'] == t_prev]
            k1 = df.index[df['date'] == t]
            if len(k) == 0 or len(k1) == 0:
                continue
            k, k1 = k[0], k1[0]
            sig = df.loc[k, 'signal']
            rs = df.loc[k, 'rs'] if 'rs' in df.columns else 0.5
            if sig != 1 or rs < RS_MIN:
                continue
            price = float(df.loc[k1, 'open'])
            eff = max(0.0, BUDGET_PER_TRADE - COMMISSION_OPEN)
            q = math.floor(eff / (price + SLIPPAGE_PER_SH))
            if q < 1 or (price * q) > cash:
                continue
            candidates.append((s, rs, price, k1, q))

        # Ordenar por RS descendente
        candidates.sort(key=lambda x: x[1], reverse=True)

        for s, rs, px, k1, q in candidates[:free_slots]:
            notional = px * q + COMMISSION_OPEN + SLIPPAGE_PER_SH * q
            if notional > cash:
                continue
            cash -= notional
            df = data[s]
            positions[s] = {'qty': q, 'entry_px': px + SLIPPAGE_PER_SH, 'entry_dt': df.loc[k1, 'date']}
            print(f"[PORT][BUY] {s} x{q} @ {px:.2f} | RS={rs:.3f} | cash=${cash:.2f}")

    # === Cierre forzado ===
    for s, pos in list(positions.items()):
        df = data[s]
        px = float(df.iloc[-1]['close'])
        pnl = (px - pos['entry_px']) * pos['qty'] - COMMISSION_CLOSE
        cash += px * pos['qty']
        closed.append({
            'symbol': s,
            'entry_px': pos['entry_px'],
            'exit_px': px,
            'qty': pos['qty'],
            'entry_dt': pos['entry_dt'],
            'exit_dt': df.iloc[-1]['date'],
            'pnl': pnl
        })
        print(f"[PORT][FORCED CLOSE] {s} @ {px:.2f} | pnl=${pnl:.2f} | cash=${cash:.2f}")
        del positions[s]

    # === Resultados ===
    if not closed:
        print("[PORT] Sin operaciones (sin señales).")
        return

    dft = pd.DataFrame(closed)
    total = dft['pnl'].sum()
    wr = 100.0 * (dft['pnl'] >= 0).mean()
    pf = (dft.loc[dft['pnl'] > 0, 'pnl'].sum() /
          max(1e-9, -dft.loc[dft['pnl'] < 0, 'pnl'].sum()))
    print(f"\n[PORT] Operaciones={len(dft)} | Win%={wr:.1f} | PF={pf:.2f} | PnL=${total:.2f} | Cash=${cash:.2f}")

    print("\nTop PnL por símbolo:")
    for sym, val in dft.groupby('symbol')['pnl'].sum().sort_values(ascending=False).head(15).items():
        print(f" {sym:<6}  ${val:,.2f}")

# ===================== MAIN =====================
if __name__ == '__main__':
    if MODE_N == "BACKTEST":
        if ENABLE_PREFETCH: prefetch_universe(SYMBOLS, BT_DURATION_STR, TIMEFRAME, BT_USE_RTH)
        results=[]
        for s in SYMBOLS:
            try:
                results.append(backtest_symbol(s)); time.sleep(PACING_SECONDS)
            except Exception as e:
                print(f"[BT][ERROR] {s}: {e}")
        if RUN_PORTFOLIO_BT:
            backtest_portfolio(SYMBOLS, BT_DURATION_STR, TIMEFRAME, BT_USE_RTH)

    elif MODE_N == "PREOPEN":
        try:
            ensure_ib_connection()
            if ENABLE_PREFETCH:
                # Calienta cache sólo con diaria (rápido y suficiente para PREOPEN)
                prefetch_universe(SYMBOLS, '2 M', TIMEFRAME, True)
            queue_orders_for_next_open()
        finally:
            try: ib.disconnect()
            except Exception: pass

    elif MODE_N == "LIVE":
    # Reconciliar fills (por ejemplo, tras un PREOPEN anterior)
        preopen_done_date = None  # fecha ET para no duplicar PREOPEN
        reconciled_for_open = False

        reconcile_fills_update_db()
        while True:
            print(f"\n========== NUEVA ITERACIÓN (LIVE) [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ==========")
            ensure_ib_connection()
            if kill_switch_check_and_close_all():
                print("[LIVE] Kill-switch activado. Pausando 1h."); time.sleep(3600); continue

            now_et = datetime.now(pytz.timezone('US/Eastern'))
            if not market_is_open():
                # Ejecuta PREOPEN una única vez por día ET y espera apertura
                if preopen_done_date != now_et.date():
                    print(f"[LIVE] Mercado cerrado. Ejecutando PREOPEN único para {now_et.date()}...")
                    try:
                        if ENABLE_PREFETCH:
                            prefetch_universe(SYMBOLS, '2 M', TIMEFRAME, True)
                        queue_orders_for_next_open()
                        preopen_done_date = now_et.date()
                    except Exception as e:
                        print(f"[LIVE][PREOPEN][ERROR] {e}")
                else:
                    print(f"[LIVE] Mercado cerrado (PREOPEN ya ejecutado para {preopen_done_date}).")
                reconciled_for_open = False  # forzar reconcile al abrir
                print("[LIVE] Espera 15m..."); time.sleep(900); continue

            # Mercado ABIERTO → reconcile una vez post-apertura
            if not reconciled_for_open:
                print("[LIVE] Apertura detectada. Reconciliando fills de OPG...")
                reconcile_fills_update_db()
                reconciled_for_open = True

            # Gestionar abiertos primero
            open_syms = [s for s in get_open_symbols_db()]
            if open_syms: print(f"[LIVE][DB] Abiertos: {open_syms}")
            for s in open_syms:
                try: analyze_symbol_live(s); time.sleep(1)
                except Exception as e: print(f"[LIVE][ERR exit-check] {s}: {e}")

            # Buscar entradas si hay cupo (filtros + ranking RS)
            if open_trades_count() < MAX_OPEN_TRADES:
                base = [s for s in SYMBOLS if s not in open_syms]
                filt = [s for s in base if _passes_universe_filters(s)[0]]
                ranked = rank_candidates_rs20(filt, TIMEFRAME)
                print(f"[LIVE][RS20] prioridad: {ranked[:15]} ...")
                for s in ranked:
                    if open_trades_count() >= MAX_OPEN_TRADES: break
                    try: analyze_symbol_live(s); time.sleep(1)
                    except Exception as e: print(f"[LIVE][ERR entry-check] {s}: {e}")
            else:
                print(f"[LIVE] Cupo completo {open_trades_count()}/{MAX_OPEN_TRADES}")

            print("\nEsperando próxima revisión (15m) ...\n"); ib.sleep(60*15)

