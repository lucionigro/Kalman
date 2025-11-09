from ib_insync import *
import pandas as pd
import numpy as np
import sqlite3
import math
import time
import os
import pytz
from datetime import datetime, timezone, time as dtime, timedelta
from xml.etree import ElementTree as ET
import threading, schedule
from datetime import datetime
from email_ibkr import obtener_posiciones_ibkr, obtener_cerradas_db, generar_html, enviar_mail, mail_orden
import asyncio

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

IB_PORT = 4001  #7497 DEMO
IB_CLIENT_ID = 1
ACCOUNT_ID = "U22866664"
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

# ===================== PARÁMETROS DE ESTRATEGIA MAS CONSERVADORES =====================
# PRICE_SOURCE       = "hl2"      # "hl2" o "close"
# MEASUREMENT_NOISE  = 1.0
# PROCESS_NOISE      = 0.01
# ATR_PERIOD         = 1         # más estable que 1
# ATR_FACTOR         = 0.4        # más conservador que 0.4
# TIMEFRAME          = '1 day'    # swing sobre diaria

PRICE_SOURCE       = "hl2"      # usa (H + L) / 2
MEASUREMENT_NOISE  = 0.08       # Kalman ultra sensible
PROCESS_NOISE      = 0.02       # respuesta más rápida a cambios
ATR_PERIOD         = 1          # ATR instantáneo
ATR_FACTOR         = 0.3        # banda muy ajustada
TIMEFRAME          = '1 day'    # swing diario


# ===================== RANKING / RS =====================
RS_BENCHMARK       = 'SPY'
RS_LOOKBACK_BARS   = 20
RS_MIN             = 0.07       # gating mínimo de RS (> 7% vs SPY)

# ===================== RIESGO / EJECUCIÓN =====================
MAX_OPEN_TRADES     = 4
BUDGET_PER_TRADE    = 300.0      # para LIVE sizing por cash (fallback a BP)
RESERVE_CASH_PCT    = 0.00
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
SMA_UPTREND_LEN        =  50

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
DB_FILE = 'trades_live.db'
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
ib.account = ACCOUNT_ID

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
    cached['date'] = pd.to_datetime(cached['date'], utc=True)
    cached = cached.drop_duplicates('date').sort_values('date', ignore_index=True)
    last_dt = cached['date'].iloc[-1]
    now_et = pd.Timestamp(datetime.now(pytz.timezone('US/Eastern'))).tz_convert('UTC')

    # Heurística: ¿hay vela nueva?
    if barSize == '1 day':
        need_update = now_et.normalize() > last_dt.normalize()
    else:
        need_update = (now_et - last_dt) > pd.Timedelta(hours=1)

    if not need_update:
        print(f"[REFRESH][{symbol}] cache al día (última={last_dt.date()})")
        return cached

    # Calcular días a descargar (corrigiendo tipo)
    days_gap = max(1, int((now_et.date() - last_dt.date()).days))
    days_pull = min(2, days_gap)  # 🔹 solo descarga 1–2 días como máximo

    ensure_ib_connection()
    c = resolve_contract(symbol)
    if c is None:
        print(f"[REFRESH][{symbol}] no se pudo calificar contrato")
        return cached

    tail = _get_history_ib(c, f'{days_pull} D', barSize, useRTH)
    if tail is None or tail.empty:
        print(f"[REFRESH][{symbol}] sin nuevas velas")
        return cached

    tail['date'] = pd.to_datetime(tail['date'], utc=True)
    tail = tail.drop_duplicates('date').sort_values('date', ignore_index=True)

    merged = pd.concat([cached, tail], ignore_index=True)
    merged = merged.drop_duplicates('date').sort_values('date', ignore_index=True)

    added = len(merged) - len(cached)
    _save_cache(merged, symbol, durationStr, barSize, useRTH)

    if added <= 0:
        print(f"[REFRESH][{symbol}] cache al día (última={merged['date'].iloc[-1].date()})")
    else:
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
    o = MarketOrder(action, qty); 
    o.tif='OPG'; 
    o.outsideRth=False; 
    o.account = ACCOUNT_ID
    ib.placeOrder(contract, o)

def _place_LOO(contract: Contract, action: str, qty: int, lmt: float):
    o = LimitOrder(action, qty, lmt); o.tif='OPG'; o.outsideRth=False; o.account = ACCOUNT_ID;ib.placeOrder(contract, o)


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
                mail_orden(sym, "SELL", qty, lmt, "PREOPEN LOO")

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
                mail_orden(sym, "BUY", qty, lmt, "PREOPEN LOO")
            else:
                print(f"[PREOPEN] BUY MOO OPG: {sym} x{qty} (via {src}) | rs={rs_val:.4f}")
                _place_MOO(c, 'BUY', qty)
            insert_trade_open_db(sym, qty, entry_price=close_px, comentario='PREOPEN OPG queued')
        elif VERBOSE_PREOPEN:
            print(f"[PREOPEN][NO-ENTRY] {sym}: sig={sig} dir={direction}")


def reconcile_positions_vs_ibkr():
    """
    Sincroniza DB vs IBKR sin cerrar entradas recientes ni mientras haya órdenes abiertas.
    """
    try:
        # Posiciones reales IBKR (qty por símbolo)
        positions = ib.positions()[:]  # crea una copia del snapshot actual
        ib_positions = {
            p.contract.symbol: int(p.position)
            for p in positions
            if int(p.position) != 0
        }



        # Órdenes/trades abiertos (BUY aún sin completar, etc.)
        pending_buys = set()
        for tr in ib.openTrades():
            try:
                sym = tr.contract.symbol
                act = (tr.order.action or '').upper()
                rem = int(tr.orderStatus.remaining or 0)
                if act in ('BUY', 'BOT') and rem > 0:
                    pending_buys.add(sym)
            except Exception:
                pass

        # Posiciones ABIERTAS en DB con fecha_apertura para gracia
        cur.execute("""
            SELECT ticker, precio_entrada, fecha_apertura
            FROM operaciones
            WHERE estado='ABIERTA'
        """)
        db_open = cur.fetchall()

        closed_count = 0
        now_utc = datetime.now(timezone.utc)

        for sym, entry_px, fa in db_open:
            # 1) Si IB ya tiene qty > 0, todo ok
            if ib_positions.get(sym, 0) != 0:
                continue

            # 2) Si hay BUY pendiente encolado → no cerrar
            if sym in pending_buys:
                continue

            # 3) Gracia: no cerrar si la apertura fue hace < 5 minutos
            grace_ok = False
            try:
                opened_at = pd.to_datetime(fa, utc=True)
                age_sec = (now_utc - opened_at).total_seconds()
                if age_sec < 300:   # 5 minutos
                    grace_ok = True
            except Exception:
                pass
            if grace_ok:
                continue

            # 4) Si realmente no existe en IBKR (y sin órdenes pendientes)
            last_px = get_last_close(sym) or float(entry_px or 0.0)
            close_trade_db(sym, exit_price=last_px, comentario_extra='Sync auto IBKR')
            closed_count += 1
            print(f"[SYNC] {sym}: no aparece en IBKR, cerrado localmente (sync).")

        if closed_count > 0:
            print(f"[SYNC] {closed_count} operaciones cerradas por reconciliación IBKR.")
        else:
            print("[SYNC] DB e IBKR sincronizados.")

    except Exception as e:
        print(f"[SYNC][ERROR] {e}")

# ===================== RECONCILIAR FILLS (LIVE) =====================
def reconcile_fills_update_db():
    """
    Sincroniza los fills ejecutados en IBKR con la base local:
      - Actualiza precios de entrada si hubo BUY fills.
      - Cierra operaciones ABIERTAS si se detecta una venta total.
      - Reduce cantidad si se trata de una venta parcial.
      - Usa las posiciones reales en IBKR para validar cantidad restante.
      - Evita cierres erróneos durante TP parciales o resync recientes.
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
            avg = float(f.execution.avgPrice or f.execution.price or 0)
            side = f.execution.side.upper()
            shares = int(abs(f.execution.shares or 0))
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
                    SELECT id, cantidad, precio_entrada, comentario
                    FROM operaciones
                    WHERE ticker=? AND estado='ABIERTA'
                """, (sym,))
                row = cur.fetchone()
                if not row:
                    continue

                op_id, qty_db, entry_px, comm = row
                qty_db = int(qty_db or 0)
                if qty_db <= 0:
                    continue

                # Chequear cantidad real actual en IBKR
                qty_ib = 0
                try:
                    for p in ib.positions():
                        if p.contract.symbol == sym:
                            qty_ib = int(p.position or 0)
                            break
                except Exception:
                    pass

                # Si la DB tenía comentario de TP parcial, evitar doble cierre
                if comm and 'TP parcial' in str(comm):
                    print(f"[RECONCILE][SKIP] {sym}: fill ignorado (TP parcial ya procesado).")
                    continue

                if qty_ib <= 0:
                    # Venta total → cerrar operación
                    cur.execute("""
                        UPDATE operaciones
                        SET precio_salida=?, fecha_cierre=datetime('now'),
                            estado='CERRADA',
                            comentario=COALESCE(comentario,'') || ' | STOP/SELL fill total'
                        WHERE id=?
                    """, (avg, op_id))
                    conn.commit()
                    closed_total += 1
                    print(f"[RECONCILE][STOP] {sym} cerrado totalmente @ {avg:.2f}")
                elif qty_ib < qty_db:
                    # Venta parcial → actualizar cantidad restante a lo que IBKR mantiene
                    cur.execute("""
                        UPDATE operaciones
                        SET cantidad=?,
                            comentario=COALESCE(comentario,'') || ' | SELL parcial fill'
                        WHERE id=?
                    """, (qty_ib, op_id))
                    conn.commit()
                    closed_partial += 1
                    print(f"[RECONCILE][PARTIAL] {sym} venta parcial ({shares}/{qty_db}) @ {avg:.2f} → quedan {qty_ib}")

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

        # 🔍 Detectar si ya se hizo un TP parcial (según comentario en DB)
        tp_done = False
        cur.execute("""
            SELECT comentario FROM operaciones
            WHERE ticker=? AND estado='ABIERTA'
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0] and 'TP parcial 8%' in row[0]:
            tp_done = True

        # 🟢 Take Profit parcial único al +8% y Stop BE (solo si no se hizo antes)
        if not tp_done and gain_pct >= 8.0 and qty > 1:
            half_qty = qty // 2
            remaining_qty = qty - half_qty
            c = resolve_contract(symbol)
            if c:
                print(f"[LIVE][TP] {symbol}: +{gain_pct:.2f}% → vendiendo {half_qty} y colocando stop BE")

                # 1️⃣ Vende mitad
                order_tp = MarketOrder('SELL', half_qty)
                order_tp.account = ACCOUNT_ID           # ✅ especifica cuenta
                ib.placeOrder(c, order_tp)
                close_trade_db(symbol, exit_price=close_px, comentario_extra='TP parcial 8%')
                mail_orden(symbol, "SELL", half_qty, close_px, "TP parcial 8%")

                # 2️⃣ Reinsertar la mitad restante y marcar TP parcial hecho
                insert_trade_open_db(symbol, remaining_qty, entry_price=float(entry_px),
                                     comentario='Reentry BE | TP parcial 8%')

                # 🚫 Cancelar stops previos activos antes de colocar uno nuevo (solo de esta cuenta)
                open_orders = ib.openOrders()
                for o in open_orders:
                    if getattr(o, "account", None) != ACCOUNT_ID:
                        continue  # ignora órdenes de otra cuenta
                    if hasattr(o, "contract") and o.contract and getattr(o.contract, "symbol", None) == symbol:
                        if o.action == 'SELL' and o.orderType == 'STP':
                            print(f"[LIVE][CANCEL] Cancelando stop previo de {symbol}")
                            ib.cancelOrder(o)

                # 3️⃣ Colocar nuevo Stop en Break-Even (GTC)
                stop_order = StopOrder('SELL', remaining_qty, stopPrice=float(entry_px))
                stop_order.tif = 'GTC'
                stop_order.account = ACCOUNT_ID         # ✅ especifica cuenta
                ib.placeOrder(c, stop_order)
                print(f"[LIVE][STOP] Stop BE colocado @ {entry_px:.2f} por {remaining_qty} acciones")

            return  # corta acá si hubo TP parcial

        # 🔴 Stop o señal opuesta (evaluado siempre)
        if _should_stop(symbol, df, float(entry_px)) or (sig == -1) or (direction == 1):
            c = resolve_contract(symbol)
            if c is None:
                return
            print(f"[LIVE] CLOSE {symbol} x{qty} (stop/signal)")
            order_close = MarketOrder('SELL', qty)
            order_close.account = ACCOUNT_ID           # ✅ especifica cuenta
            ib.placeOrder(c, order_close)
            close_trade_db(symbol, exit_price=close_px, comentario_extra='LIVE stop/signal')
            mail_orden(symbol, "SELL", qty, close_px, "Salida LIVE stop/signal")
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

        order_buy = MarketOrder('BUY', qty)
        order_buy.account = ACCOUNT_ID                # ✅ especifica cuenta
        ib.placeOrder(c, order_buy)

        insert_trade_open_db(symbol, qty, entry_price=px, comentario='LIVE entry')
        mail_orden(symbol, "BUY", qty, px, "Entrada LIVE KalmanHullST")
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
    nlv = get_net_liq_usd()
    limit_dd = -abs(KILL_SWITCH_DD_PCT) * nlv
    est = _daily_pnl_estimate()

    if est <= limit_dd:
        print(f"[KILL-SWITCH] DD estimado {est:.2f} <= {limit_dd:.2f}. Cerrando TODO.")
        cur.execute("SELECT ticker, cantidad FROM operaciones WHERE estado='ABIERTA'")
        rows = cur.fetchall()

        for t, q in rows:
            try:
                if not q or int(q) < 1:
                    continue

                c = resolve_contract(t)
                if c is None:
                    continue

                # ✅ especificar la cuenta
                order_kill = MarketOrder('SELL', int(q))
                order_kill.account = ACCOUNT_ID
                ib.placeOrder(c, order_kill)

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

def enviar_resumen_diario():
    try:
        print("[SCHEDULER] Enviando resumen diario IBKR...")
        abiertas = obtener_posiciones_ibkr(port=IB_PORT, client_id=99, account_id=ACCOUNT_ID)
        cerradas = obtener_cerradas_db()
        html = generar_html(abiertas, cerradas)
        enviar_mail("📊 Estado diario con IBKR", html)
        print("[SCHEDULER] Mail de resumen diario enviado correctamente.")
    except Exception as e:
        print(f"[SCHEDULER][ERROR] {e}")

def run_24h_loop(interval_open_minutes=10):
    """
    Loop inteligente 24/7 que:
      - Detecta estado del mercado.
      - Ejecuta preopen 1h antes de la apertura.
      - Corre análisis cada X minutos con mercado abierto.
      - Envía mail al cierre y actualiza caches.
    """
    tz = pytz.timezone("US/Eastern")
    preopen_done = None

    while True:
        now_et = datetime.now(tz)
        hora = now_et.time()

        # 1️⃣ Detectar si estamos 1h antes de apertura (8:30 ET)
        if dtime(8, 25) <= hora < dtime(9, 30) and preopen_done != now_et.date():
            print(f"[24H] Ejecutando PREOPEN para {now_et.date()}...")
            try:
                ensure_ib_connection()
                prefetch_universe(SYMBOLS, '2 M', TIMEFRAME, True)
                queue_orders_for_next_open()
                preopen_done = now_et.date()
            except Exception as e:
                print(f"[24H][PREOPEN][ERROR] {e}")

        # 2️⃣ Mercado abierto: 9:30–16:00
        elif dtime(9, 30) <= hora <= dtime(16, 0):
            print(f"[24H] Mercado abierto — análisis cada {interval_open_minutes}m (hora NY: {hora})")
            try:
                ensure_ib_connection()
                reconcile_fills_update_db()

                open_syms = get_open_symbols_db()
                for s in open_syms:
                    analyze_symbol_live(s); ib.sleep(1)

                if open_trades_count() < MAX_OPEN_TRADES:
                    base = [s for s in SYMBOLS if s not in open_syms]
                    filt = [s for s in base if _passes_universe_filters(s)[0]]
                    ranked = rank_candidates_rs20(filt, TIMEFRAME)
                    for s in ranked:
                        if open_trades_count() >= MAX_OPEN_TRADES: break
                        analyze_symbol_live(s); ib.sleep(1)

                reconcile_positions_vs_ibkr()
                print(f"\n[24H] Esperando próxima revisión en {interval_open_minutes} minutos...\n")
                ib.sleep(interval_open_minutes * 60)

            except Exception as e:
                print(f"[24H][LIVE][ERROR] {e}")
                ib.sleep(30)
                continue


        # 3️⃣ Después del cierre (16:00–17:30): resumen diario y refresh
        elif dtime(16, 0) <= hora < dtime(17, 30):
            print("[24H] Mercado cerrado — enviando mail diario y actualizando precios")
            try:
                enviar_resumen_diario()
                for s in SYMBOLS:
                    refresh_cache_incremental(s, '3 M', '1 day', True)
            except Exception as e:
                print(f"[24H][POST][ERROR] {e}")
            time.sleep(3600)  # espera 1h

        else:
            # 4️⃣ Horas nocturnas → dormir hasta la próxima hora
            print(f"[24H] Horario nocturno {hora}, esperando próxima ventana...")
            time.sleep(1800)


# Función que mantiene el schedule corriendo en hilo aparte
def iniciar_scheduler_diario():
    DAILY_MAIL_ET = "16:05"
    schedule.clear('daily_mail')
    schedule.every().day.at(DAILY_MAIL_ET).do(enviar_resumen_diario).tag('daily_mail')
    
    def loop_schedule():
        while True:
            schedule.run_pending()
            time.sleep(60)
    t = threading.Thread(target=loop_schedule, daemon=True)
    t.start()
    print(f"[SCHEDULER] Tarea diaria programada ({DAILY_MAIL_ET} ET).")

def programar_reinicio_market_open():
    tz = pytz.timezone('US/Eastern')
    now = datetime.now(tz)
    # Crear fecha/hora aware en misma zona
    next_open = tz.localize(datetime.combine(now.date(), dtime(9, 30)))

    if now.time() > dtime(9, 30):  # si ya pasó la apertura, apunta al día siguiente
        next_open = tz.localize(datetime.combine(now.date() + timedelta(days=1), dtime(9, 30)))

    segundos_faltantes = (next_open - now).total_seconds()
    if segundos_faltantes < 0:
        segundos_faltantes = 60

    print(f"[SCHEDULER] Mercado abre a las 9:30 ET → esperando {segundos_faltantes/3600:.2f} h...")

    def despertar():
        print("[SCHEDULER] 🌅 Apertura de mercado detectada — retomando análisis LIVE.")

    threading.Timer(segundos_faltantes, despertar).start()

# ===================== MAIN =====================
if __name__ == '__main__':
    if MODE_N == "LIVE":
        ensure_ib_connection()
        iniciar_scheduler_diario()
        run_24h_loop(interval_open_minutes=10)


