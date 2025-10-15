from ib_insync import *
import pandas as pd
import numpy as np
import sqlite3
import math
import time
import os
from datetime import datetime

# =============== CONFIGURACIÓN ===============
MODE = "BACKTEST"  # "BACKTEST" o "LIVE"  <<< CAMBIAR A "LIVE" PARA PAPER TRADING
MODE_N = MODE.strip().upper()
IS_BACKTEST = (MODE_N == "BACKTEST")
IS_LIVE = (MODE_N == "LIVE")
IB_PORT = 7497
IB_CLIENT_ID = 1
EXCHANGE = 'SMART'
CURRENCY = 'USD'

SYMBOLS = [
    # Tech / Growth
    'NVDA','META','PLTR','HIMS','AMD','HOOD','TSLA','AAPL','GOOG','AMZN','INTC','NFLX','RDDT','PINS','MSFT','MU',
    # Industrial / Value
    'GE','BA','NKE','PG',
    # Finanzas
    'JPM','GS','MS','BAC','COIN',
    # Salud / Pharma
    'LLY','UNH',
    # Latam / China
    'MELI','BBD','NU','PAGS','GGAL','YPF','SUPV','JD','BABA'
]

#SYMBOLS = ['RGTI']

# Parámetros (alineados con tu Pine)
PRICE_SOURCE       = "hl2"    # (H+L)/2 o "close"
MEASUREMENT_NOISE  = 1.0
PROCESS_NOISE      = 0.01
ATR_PERIOD         = 1
ATR_FACTOR         = 0.4
TIMEFRAME          = '4 hours'  # timeframe para señales

# Backtest
BT_DURATION_STR = '12 M'
BT_USE_RTH      = True
POSITION_SIZE   = 5
STRATEGY        = 'KalmanHullST_BackQuant'

# === SIZING (LIVE) POR MONTO FIJO Y FALLBACK A BUYING POWER ===
MAX_OPEN_TRADES   = 6        # máximo de operaciones ABIERTAS simultáneas
BUDGET_PER_TRADE  = 600.0    # USD por trade usando CASH
COMMISSION_OPEN   = 1.0      # comisión estimada por orden (apertura)
RESERVE_CASH_PCT  = 0.05     # deja 5% del cash libre

USE_BUYING_POWER  = True     # permitir uso de Buying Power si el cash no alcanza
BP_TRADE_CAP      = 1800.0   # tope USD por trade cuando se usa Buying Power
BP_RESERVE_PCT    = 0.10     # deja 10% del BP libre

# =============== BASE DE DATOS ===============
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

# Índice único para evitar más de una ABIERTA por ticker
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_op_open
ON operaciones(ticker, estado)
WHERE estado = 'ABIERTA';
""")
conn.commit()

# =============== CONEXIÓN A IBKR (para históricos y LIVE) ===============
ib = IB()
ib.connect('127.0.0.1', IB_PORT, clientId=IB_CLIENT_ID)
print(f"Conectado a IBKR | MODE={MODE}")

# =============== UTILIDADES INDICADORES ===============
def f_kalman_streaming(prices, measurement_noise=1.0, process_noise=0.01):
    """Kalman simple iterativo (como en Pine)."""
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
    """Kalman-Hull: Kalman(length/2), Kalman(length), diff -> Kalman(sqrt(length))."""
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

def supertrend_backquant(df, factor=0.4, atr_period=1, src_col="kalman_hma"):
    """
    Supertrend (lógica BackQuant). Convención de Pine:
      direction = -1 → tendencia alcista (línea abajo → LONG)
      direction =  1 → tendencia bajista  (línea arriba → SHORT)
    """
    df['tr'] = true_range(df)
    df['atr'] = rma(df['tr'], max(1, atr_period))

    upper = df[src_col] + factor * df['atr']
    lower = df[src_col] - factor * df['atr']

    supertrend = np.full(len(df), np.nan)
    direction = np.zeros(len(df))

    # Arrastre de bandas
    upper = upper.copy()
    lower = lower.copy()

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
        elif prev_super == prev_upper:
            direction[i] = -1 if df['close'].iloc[i] > upper.iloc[i] else 1
        else:
            direction[i] = 1 if df['close'].iloc[i] < lower.iloc[i] else -1

        supertrend[i] = lower.iloc[i] if direction[i] == -1 else upper.iloc[i]

    df['supertrend'] = supertrend
    df['direction'] = direction
    return df

# =============== DATA & FEATURES ===============
def fetch_history(symbol, durationStr, barSize='1 hour', useRTH=True):
    contract = Stock(symbol, EXCHANGE, CURRENCY)
    bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr=durationStr,
        barSizeSetting=barSize,
        whatToShow='TRADES',
        useRTH=useRTH,
        formatDate=1
    )
    df = util.df(bars)
    if df is None or df.empty:
        return None
    df.drop_duplicates(subset=['date'], inplace=True)
    df.sort_values('date', inplace=True, ignore_index=True)
    # Fuente de precio
    if PRICE_SOURCE.lower() == 'hl2':
        df['src'] = (df['high'] + df['low']) / 2.0
    else:
        df['src'] = df['close']
    # Indicadores
    df['kalman_hma'] = khma(df['src'], MEASUREMENT_NOISE, PROCESS_NOISE)
    df = supertrend_backquant(df, factor=ATR_FACTOR, atr_period=ATR_PERIOD, src_col='kalman_hma')
    # Señales (al cierre de vela): cruces de direction por 0
    df['signal'] = 0
    cross_long  = (df['direction'].shift(1) > 0) & (df['direction'] < 0)   # +1 -> -1
    cross_short = (df['direction'].shift(1) < 0) & (df['direction'] > 0)   # -1 -> +1
    df.loc[cross_long, 'signal'] = 1
    df.loc[cross_short, 'signal'] = -1
    return df

# =============== PERSISTENCIA DE TRADES (BACKTEST) ===============
def insert_trade_closed(symbol, qty, entry_price, exit_price, entry_dt, exit_dt, comentario):
    pnl = (exit_price - entry_price) * qty
    retorno_pct = (exit_price / entry_price - 1) * 100.0
    dur_h = (pd.to_datetime(exit_dt) - pd.to_datetime(entry_dt)).total_seconds() / 3600.0
    cur.execute('''
        INSERT INTO operaciones (ticker, tipo, cantidad, precio_entrada, precio_salida,
                                 fecha_apertura, fecha_cierre, estado, pnl, retorno_pct,
                                 duracion_horas, estrategia, comentario)
        VALUES (?, 'LONG', ?, ?, ?, ?, ?, 'CERRADA', ?, ?, ?, ?, ?)
    ''', (symbol, qty, entry_price, exit_price, str(entry_dt), str(exit_dt),
          pnl, retorno_pct, dur_h, STRATEGY, comentario))
    conn.commit()

# =============== ACCOUNT / SIZING HELPERS (LIVE) ===============
def _acct_val(tag: str, currency: str = "USD") -> float:
    """Lee un tag puntual del Account Summary (USD)."""
    try:
        for x in ib.accountSummary():
            if x.tag == tag and x.currency == currency:
                return float(x.value)
    except Exception:
        pass
    return 0.0

def get_available_funds_usd() -> float:
    """Fondos disponibles para abrir posiciones (cash/margen ya considerados por IBKR)."""
    val = _acct_val("AvailableFunds", "USD")
    if val == 0.0:
        # Fallback razonable para cash accounts
        val = _acct_val("TotalCashValue", "USD")
    return max(0.0, val)

def get_buying_power_usd() -> float:
    """Buying Power (Reg-T). En margen suele ser ~4× 'AvailableFunds' intradía."""
    return max(0.0, _acct_val("BuyingPower", "USD"))

def calc_qty_by_cash(price: float, available_usd: float) -> int:
    """Cantidad por presupuesto de cash fijo (BUDGET_PER_TRADE), con reserva y comisión."""
    if price <= 0:
        return 0
    budget_eff = min(BUDGET_PER_TRADE, max(0.0, available_usd * (1.0 - RESERVE_CASH_PCT)))
    if budget_eff <= COMMISSION_OPEN:
        return 0
    return max(0, math.floor((budget_eff - COMMISSION_OPEN) / price))

def calc_qty_by_bp(price: float, buying_power_usd: float) -> int:
    """
    Cantidad por presupuesto de buying power. Usamos el menor entre:
    - CAP por trade (BP_TRADE_CAP)
    - Buying Power disponible (con reserva)
    """
    if price <= 0:
        return 0
    bp_eff = min(BP_TRADE_CAP, max(0.0, buying_power_usd * (1.0 - BP_RESERVE_PCT)))
    if bp_eff <= COMMISSION_OPEN:
        return 0
    return max(0, math.floor((bp_eff - COMMISSION_OPEN) / price))

# =============== HELPERS DB PARA LIVE (estado 100% desde DB) ===============
def open_trades_count() -> int:
    cur.execute("SELECT COUNT(*) FROM operaciones WHERE estado='ABIERTA'")
    x = cur.fetchone()
    return int(x[0]) if x and x[0] is not None else 0

def symbol_has_open(symbol: str) -> bool:
    cur.execute("SELECT 1 FROM operaciones WHERE ticker=? AND estado='ABIERTA' LIMIT 1", (symbol,))
    return cur.fetchone() is not None

def get_open_symbols_db() -> list[str]:
    cur.execute("SELECT ticker FROM operaciones WHERE estado='ABIERTA'")
    rows = cur.fetchall()
    return [r[0] for r in rows] if rows else []

def get_open_trade_info(symbol: str):
    """
    Devuelve (id, cantidad, precio_entrada, fecha_apertura) para el ticker ABIERTA más reciente.
    """
    cur.execute("""
        SELECT id, cantidad, precio_entrada, fecha_apertura
        FROM operaciones
        WHERE ticker=? AND estado='ABIERTA'
        ORDER BY id DESC
        LIMIT 1
    """, (symbol,))
    return cur.fetchone()

def insert_trade_open_db(symbol: str, qty: int, entry_price: float, comentario: str = 'LIVE'):
    now = datetime.utcnow().isoformat()
    cur.execute("""
        INSERT INTO operaciones
        (ticker, tipo, cantidad, precio_entrada, precio_salida,
         fecha_apertura, fecha_cierre, estado, pnl, retorno_pct,
         duracion_horas, estrategia, comentario)
        VALUES (?, 'LONG', ?, ?, NULL, ?, NULL, 'ABIERTA', NULL, NULL, NULL, ?, ?)
    """, (symbol, qty, entry_price, now, STRATEGY, comentario))
    conn.commit()

def close_trade_db(symbol: str, exit_price: float, comentario_extra: str = 'LIVE CLOSE'):
    """
    Cierra la operación ABIERTA del símbolo calculando PnL/retorno/duración y marcando estado=CERRADA.
    """
    row = get_open_trade_info(symbol)
    if not row:
        print(f"[DB] No encontré ABIERTA para {symbol} al cerrar.")
        return
    op_id, qty, entry_price, fecha_apertura = row
    if qty is None or entry_price is None or fecha_apertura is None:
        print(f"[DB] Datos incompletos para cerrar {symbol} (op_id={op_id}).")
        return

    now_iso = datetime.utcnow().isoformat()
    pnl = (exit_price - float(entry_price)) * float(qty)
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

# =============== BACKTEST ENGINE ===============
def backtest_symbol(symbol):
    print(f"\n[BT] {symbol} | Descargando {BT_DURATION_STR} @ {TIMEFRAME} ...")
    df = fetch_history(symbol, BT_DURATION_STR, TIMEFRAME, BT_USE_RTH)
    if df is None or len(df) < 50:
        print(f"[BT][WARN] {symbol}: sin datos suficientes.")
        return {'symbol': symbol, 'trades': 0, 'wins': 0, 'wr': 0.0, 'pf': 0.0, 'ret_pct_avg': 0.0, 'pnl': 0.0}

    pos_open = False
    qty = 0
    entry_price = None
    entry_time = None
    trades = []
    gross_profit = 0.0
    gross_loss = 0.0

    # Recorremos hasta len-2 para poder llenar en la vela siguiente (i+1)
    for i in range(1, len(df) - 1):
        sig = int(df['signal'].iloc[i])
        direction = int(df['direction'].iloc[i])

        # ======= ENTRADA (en la apertura de la próxima vela) =======
        if not pos_open and (sig == 1 or direction == -1):
            price_next_open = float(df['open'].iloc[i+1])
            budget_eff = max(0.0, BUDGET_PER_TRADE - COMMISSION_OPEN)
            qty = max(1, math.floor(budget_eff / price_next_open))
            if qty < 1:
                continue
            entry_price = price_next_open
            entry_time  = df['date'].iloc[i+1]
            pos_open = True

        # ======= SALIDA (en la apertura de la próxima vela) =======
        elif pos_open and (sig == -1 or direction == 1):
            exit_price = float(df['open'].iloc[i+1])
            exit_time  = df['date'].iloc[i+1]
            pnl = (exit_price - entry_price) * qty
            ret_pct = (exit_price / entry_price - 1) * 100.0
            trades.append({'entry_dt': entry_time, 'exit_dt': exit_time,
                           'entry': entry_price, 'exit': exit_price,
                           'qty': qty, 'pnl': pnl, 'ret_pct': ret_pct})
            if pnl >= 0:
                gross_profit += pnl
            else:
                gross_loss += -pnl
            # Persistimos en DB como CERRADA
            insert_trade_closed(symbol, qty, entry_price, exit_price, entry_time, exit_time,
                                comentario='BACKTEST 12M 4H')
            # reset
            pos_open = False
            entry_price = None
            entry_time = None
            qty = 0

    # Si quedó abierta, cerramos a close final para contabilizar
    if pos_open and entry_price is not None:
        exit_price = float(df['close'].iloc[-1])
        exit_time  = df['date'].iloc[-1]
        pnl = (exit_price - entry_price) * qty
        ret_pct = (exit_price / entry_price - 1) * 100.0
        trades.append({'entry_dt': entry_time, 'exit_dt': exit_time,
                       'entry': entry_price, 'exit': exit_price,
                       'qty': qty, 'pnl': pnl, 'ret_pct': ret_pct})
        if pnl >= 0:
            gross_profit += pnl
        else:
            gross_loss += -pnl
        insert_trade_closed(symbol, qty, entry_price, exit_price, entry_time, exit_time,
                            comentario='BACKTEST 12M 4H (forced close)')

    n = len(trades)
    wins = sum(1 for t in trades if t['pnl'] >= 0)
    wr = (wins / n * 100.0) if n else 0.0
    ret_pct_avg = np.mean([t['ret_pct'] for t in trades]) if n else 0.0
    pf = (gross_profit / gross_loss) if gross_loss > 0 else float('inf') if gross_profit > 0 else 0.0
    total_pnl = sum(t['pnl'] for t in trades)

    print(f"[BT] {symbol} → trades={n} | win%={wr:.1f} | pf={pf:.2f} | avg%={ret_pct_avg:.2f} | pnl=${total_pnl:.2f}")
    return {'symbol': symbol, 'trades': n, 'wins': wins, 'wr': wr, 'pf': pf, 'ret_pct_avg': ret_pct_avg, 'pnl': total_pnl}

# =============== LIVE TRADING (DB-first) ===============
def analyze_symbol_live(symbol):
    print(f"\n[LIVE] Analizando {symbol} ...")
    contract = Stock(symbol, EXCHANGE, CURRENCY)

    # Histórico corto para señal actual
    bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='2 M',
        barSizeSetting=TIMEFRAME,
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )
    df = util.df(bars)
    if df is None or df.empty:
        print(f"[LIVE][WARN] Sin datos para {symbol}")
        return

    df.drop_duplicates(subset=['date'], inplace=True)
    df.sort_values(by='date', inplace=True, ignore_index=True)
    df['src'] = (df['high'] + df['low']) / 2.0 if PRICE_SOURCE.lower() == 'hl2' else df['close']
    df['kalman_hma'] = khma(df['src'], MEASUREMENT_NOISE, PROCESS_NOISE)
    df = supertrend_backquant(df, factor=ATR_FACTOR, atr_period=ATR_PERIOD, src_col='kalman_hma')
    df['signal'] = 0
    cross_long  = (df['direction'].shift(1) > 0) & (df['direction'] < 0)
    cross_short = (df['direction'].shift(1) < 0) & (df['direction'] > 0)
    df.loc[cross_long, 'signal'] = 1
    df.loc[cross_short, 'signal'] = -1

    latest = df.iloc[-1]
    sig = int(latest['signal'])
    direction = int(latest['direction'])
    close_px = float(latest['close'])

    if MODE_N == "BACKTEST":
        print("[SAFEGUARD] BACKTEST activo: no se envían órdenes.")
        return

    # ======== Estado DB ========
    is_open = symbol_has_open(symbol)

    # ======== CIERRE si está ABIERTA en DB ========
    if is_open:
        # Criterio de salida: señal opuesta o cambio de dirección a bajista (direction == 1)
        if (sig == -1) or (direction == 1):
            info = get_open_trade_info(symbol)
            if not info:
                print(f"[LIVE][DB] No pude leer qty abierto de DB para {symbol}.")
                return
            _, qty_open, entry_px, _ = info
            qty_exit = int(qty_open) if qty_open else 0
            if qty_exit < 1:
                print(f"[LIVE] {symbol}: qty DB inválida ({qty_exit}). No cierro.")
                return
            print(f"[LIVE] CLOSE LONG {symbol} x{qty_exit}")
            ib.placeOrder(contract, MarketOrder('SELL', qty_exit))
            # Marcar cierre en DB con el precio de mercado actual (aprox)
            close_trade_db(symbol, exit_price=close_px, comentario_extra='LIVE close by signal')
        else:
            print(f"[LIVE] HOLD {symbol} (ABIERTA en DB) | Señal={sig} | Dir={direction}")
        return  # Importante: no evaluar entrada si ya está ABIERTA

    # ======== ENTRADA solo si NO está ABIERTA y hay cupo (DB) ========
    current_open = open_trades_count()
    if current_open >= MAX_OPEN_TRADES:
        print(f"[LIVE] Cupo lleno ({current_open}/{MAX_OPEN_TRADES}). Sin nuevas entradas en {symbol}.")
        return

    if (sig == 1) or (direction == -1):
        # Sizing por CASH con fallback a BP
        avail_cash = get_available_funds_usd()
        qty = calc_qty_by_cash(close_px, avail_cash)
        sizing_src = "CASH"

        if qty < 1 and USE_BUYING_POWER:
            bp = get_buying_power_usd()
            qty_bp = calc_qty_by_bp(close_px, bp)
            if qty_bp >= 1:
                qty = qty_bp
                sizing_src = "BP"

        if qty < 1:
            print(f"[LIVE] {symbol}: sin saldo suficiente. No entro.")
            return

        notional = qty * close_px
        if sizing_src == "CASH":
            print(f"[LIVE] BUY {symbol} x{qty} ~${notional:.2f} (CASH)")
        else:
            print(f"[LIVE] BUY {symbol} x{qty} ~${notional:.2f} (via BUYING POWER)")
        ib.placeOrder(contract, MarketOrder('BUY', qty))

        # Registrar ABIERTA en DB
        insert_trade_open_db(symbol, qty, entry_price=close_px, comentario='LIVE entry')
    else:
        print(f"[LIVE] No-Entry {symbol} | Señal={sig} | Dir={direction}")

# =============== MAIN LOOP ===============
if __name__ == '__main__':
    if MODE_N == "BACKTEST":
        results = []
        for sym in SYMBOLS:
            try:
                res = backtest_symbol(sym)
                results.append(res)
                time.sleep(0.3)  # pacing
            except Exception as e:
                print(f"[BT][ERROR] {sym}: {e}")

        # Resumen global
        if results:
            df_res = pd.DataFrame(results).sort_values('pnl', ascending=False)
            total_trades = int(df_res['trades'].sum())
            win_rate_avg = (df_res['wins'].sum() / max(1, total_trades)) * 100.0
            profit_factor_med = df_res.replace([np.inf, -np.inf], np.nan)['pf'].median(skipna=True)
            total_pnl = df_res['pnl'].sum()
            print(f"\n========== RESUMEN BACKTEST {BT_DURATION_STR} / {TIMEFRAME} ==========")
            print(df_res[['symbol','trades','wr','pf','ret_pct_avg','pnl']].to_string(index=False, formatters={
                'wr': lambda x: f"{x:.1f}",
                'pf': lambda x: f"{x:.2f}" if np.isfinite(x) else "inf",
                'ret_pct_avg': lambda x: f"{x:.2f}",
                'pnl': lambda x: f"${x:.2f}"
            }))
            print("----------------------------------------------")
            print(f"TOTAL trades: {total_trades} | Win% global: {win_rate_avg:.1f} | PF mediano: {profit_factor_med:.2f} | PnL total: ${total_pnl:.2f}")
    elif MODE_N == "LIVE":
        # LIVE
        while True:
            print("\n========== NUEVA ITERACIÓN (LIVE) ==========")

            # 1) Revisar y gestionar PRIMERO los que ya están ABIERTOS en DB
            open_syms = sorted(get_open_symbols_db())
            if open_syms:
                print(f"[LIVE][DB] Abiertos actuales: {open_syms}")
            for sym in open_syms:
                try:
                    analyze_symbol_live(sym)
                    time.sleep(1)
                except Exception as e:
                    print(f"[LIVE][ERROR exit-check] {sym}: {e}")

            # 2) Si hay cupo, buscar ENTRADAS en el resto (excluye los ya abiertos en DB)
            if open_trades_count() < MAX_OPEN_TRADES:
                candidates = [s for s in SYMBOLS if s not in open_syms]
                for sym in candidates:
                    if open_trades_count() >= MAX_OPEN_TRADES:
                        break
                    try:
                        analyze_symbol_live(sym)
                        time.sleep(1)
                    except Exception as e:
                        print(f"[LIVE][ERROR entry-check] {sym}: {e}")
            else:
                print(f"[LIVE] Cupo completo: {open_trades_count()}/{MAX_OPEN_TRADES}")

            print("\nEsperando próxima revisión (30m)...\n")
            ib.sleep(60 * 30)  # cada 30 minutos
