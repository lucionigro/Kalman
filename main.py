from ib_insync import *
import pandas as pd
import numpy as np
import sqlite3
import math
import time

# === CONFIGURACIÓN ===
MODE = "LIVE"  # "BACKTEST" (simula) | "LIVE" (opera)
IB_PORT = 7497
IB_CLIENT_ID = 1
EXCHANGE = 'SMART'
CURRENCY = 'USD'
SYMBOLS = [
    # Tech / Growth
    'NVDA', 'META', 'PLTR', 'HIMS', 'AMD', 'HOOD', 'TSLA', 'AAPL', 'GOOG', 'AMZN', 'INTC','NFLX', 'RDDT','PINS','MSFT','MU',
    # Industrial / Value
   'GE', 'BA', 'NKE', 'PG',
    # Finanzas
    'JPM', 'GS', 'MS', 'BAC', 'COIN',
    # Energía
    # Salud / Pharma
     'LLY', 'UNH',
    # Latinoamérica
    'MELI', 'BBD', 'NU', 'PAGS', 'GGAL', 'YPF','SUPV', 'JD','BABA'
]

# === Parámetros de tu configuración actual ===
PRICE_SOURCE = "hl2"         # (H + L) / 2
MEASUREMENT_NOISE = 1.0
PROCESS_NOISE = 0.01
ATR_PERIOD = 1
ATR_FACTOR = 0.4
TIMEFRAME = '1 hour' # 4 hours
POSITION_SIZE = 5
STRATEGY = 'KalmanHullST_BackQuant'

# === BASE DE DATOS ===
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

# === CONEXIÓN A IBKR ===
ib = IB()
ib.connect('127.0.0.1', IB_PORT, clientId=IB_CLIENT_ID)
print(f"Conectado a IBKR (modo {MODE})")

# === CARGA DE POSICIONES ABIERTAS DESDE LA BASE ===
positions = {}

cur.execute("SELECT ticker, estado FROM operaciones WHERE estado='ABIERTA'")
rows = cur.fetchall()
if rows:
    for ticker, estado in rows:
        positions[ticker] = 'LONG'
    print(f"Posiciones abiertas cargadas desde DB: {[r[0] for r in rows]}")
else:
    print("No hay posiciones abiertas registradas en la base.")


# ----------------------------- #
#  KALMAN HULL SUPERTREND CORE #
# ----------------------------- #

def f_kalman_streaming(prices, measurement_noise=1.0, process_noise=0.01):
    """Kalman filtro iterativo bar-to-bar igual que Pine."""
    state = prices.iloc[0]
    p = 1.0
    estimates = [state]

    for z in prices.iloc[1:]:
        p += process_noise
        k = p / (p + measurement_noise)
        state = state + k * (z - state)
        p = (1 - k) * p
        estimates.append(state)

    return pd.Series(estimates, index=prices.index)


def khma(series, length=1.0, process_noise=0.01):
    """KHMA estilo BackQuant."""
    inner1 = f_kalman_streaming(series, length / 2, process_noise)
    inner2 = f_kalman_streaming(series, length, process_noise)
    diff = 2 * inner1 - inner2
    return f_kalman_streaming(diff, math.sqrt(length), process_noise)


def rma(series, length):
    """RMA (Wilder’s ATR)."""
    return series.ewm(alpha=1 / length, adjust=False).mean()


def true_range(df):
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def supertrend_backquant(df, factor=0.4, atr_period=1, src_col="kalman_hma"):
    """
    Supertrend fiel a BackQuant (TradingView).
    Convención ORIGINAL de Pine:
        direction = -1  → tendencia alcista (línea debajo, LONG)
        direction =  1  → tendencia bajista  (línea arriba, SHORT)
    """

    # ATR (Wilder)
    df['tr'] = true_range(df)
    df['atr'] = rma(df['tr'], atr_period)

    upper = df[src_col] + factor * df['atr']
    lower = df[src_col] - factor * df['atr']

    supertrend = np.full(len(df), np.nan)
    direction = np.full(len(df), 0.0)

    for i in range(1, len(df)):
        prev_lower = lower.iloc[i-1]
        prev_upper = upper.iloc[i-1]
        prev_super = supertrend[i-1]

        # mismas reglas que Pine para “arrastrar” bandas
        if not (lower.iloc[i] > prev_lower or df['close'].iloc[i-1] < prev_lower):
            lower.iloc[i] = prev_lower
        if not (upper.iloc[i] < prev_upper or df['close'].iloc[i-1] > prev_upper):
            upper.iloc[i] = prev_upper

        # dirección (idéntica a Pine)
        if np.isnan(df['atr'].iloc[i-1]):
            direction[i] = 1
        elif prev_super == prev_upper:
            direction[i] = -1 if df['close'].iloc[i] > upper.iloc[i] else 1
        else:
            direction[i] = 1 if df['close'].iloc[i] < lower.iloc[i] else -1

        # supertrend final
        supertrend[i] = lower.iloc[i] if direction[i] == -1 else upper.iloc[i]

    df['supertrend'] = supertrend
    df['direction'] = direction  # ← SIN inversión
    return df




def registrar_operacion(symbol, tipo, cantidad, precio):
    cur.execute('''
        INSERT INTO operaciones (ticker, tipo, cantidad, precio_entrada, fecha_apertura, estado, estrategia)
        VALUES (?, ?, ?, ?, datetime('now'), 'ABIERTA', ?)
    ''', (symbol, tipo, cantidad, precio, STRATEGY))
    conn.commit()


def cerrar_operacion(symbol, precio_salida):
    cur.execute('''
        UPDATE operaciones
        SET precio_salida = ?, fecha_cierre = datetime('now'),
            estado = 'CERRADA',
            pnl = (? - precio_entrada) * cantidad,
            retorno_pct = ((? / precio_entrada) - 1) * 100,
            duracion_horas = (julianday(datetime('now')) - julianday(fecha_apertura)) * 24
        WHERE ticker = ? AND estado = 'ABIERTA'
    ''', (precio_salida, precio_salida, precio_salida, symbol))
    conn.commit()


# ----------------------------- #
#        ANALYSIS CORE          #
# ----------------------------- #

def analyze_symbol(symbol):
    print(f"\nAnalizando {symbol} ...")

    try:
        contract = Stock(symbol, EXCHANGE, CURRENCY)

        # === DATOS desde IBKR ===
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
            print(f"[WARN] Sin datos para {symbol}")
            return

        df.drop_duplicates(subset=['date'], inplace=True)
        df.sort_values(by='date', inplace=True, ignore_index=True)

        # === Fuente de precio (H+L)/2 ===
        df['hl2'] = (df['high'] + df['low']) / 2.0
        src = df['hl2']

        # === Kalman Hull ===
        df['kalman_hma'] = khma(src, MEASUREMENT_NOISE, PROCESS_NOISE)

        # === Supertrend ===
        df = supertrend_backquant(df, factor=ATR_FACTOR, atr_period=ATR_PERIOD, src_col='kalman_hma')

        # === Señales ===
        df['signal'] = 0
        cross_long = (df['direction'].shift(1) > 0) & (df['direction'] < 0)
        cross_short = (df['direction'].shift(1) < 0) & (df['direction'] > 0)
        df.loc[cross_long, 'signal'] = 1
        df.loc[cross_short, 'signal'] = -1

        latest = df.iloc[-1]
        sig = int(latest['signal'])
        direction = int(latest['direction'])
        close = float(latest['close'])
        pos = positions.get(symbol, 'NONE')

        # === LIVE TRADING ===
        # Entrar si la señal es 1 (cruce long) o la tendencia es alcista (-1)
        if (sig == 1 or direction == -1) and pos == 'NONE':
            ib.placeOrder(contract, MarketOrder('BUY', POSITION_SIZE))
            positions[symbol] = 'LONG'
            registrar_operacion(symbol, 'LONG', POSITION_SIZE, close)
            print(f"BUY {symbol} @ {close:.2f}  |  direction={direction}")

        # Salir si la señal es -1 (cruce short) o la tendencia es bajista (1)
        elif (sig == -1 or direction == 1) and pos == 'LONG':
            ib.placeOrder(contract, MarketOrder('SELL', POSITION_SIZE))
            positions[symbol] = 'NONE'
            cerrar_operacion(symbol, close)
            print(f"CLOSE LONG {symbol} @ {close:.2f}  |  direction={direction}")

        else:
            print(f"HOLD {symbol} ({pos}) | Señal={sig} | Dir={direction}")

    except Exception as e:
        print(f"[ERROR] {symbol}: {e}")



# ----------------------------- #
#          LOOP PRINCIPAL       #
# ----------------------------- #

if __name__ == '__main__':
    while True:
        print("\n========== NUEVA ITERACIÓN ==========")
        for sym in SYMBOLS:
            try:
                analyze_symbol(sym)
                time.sleep(1)
            except Exception as e:
                print(f"Error procesando {sym}: {e}")
        print("\nEsperando próxima revisión...\n")
        if MODE == "LIVE":
            ib.sleep(60 * 60 * 1)
        else:
            break
