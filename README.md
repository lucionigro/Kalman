# 🧠 Kalman Hull Supertrend Bot

Bot de trading automático desarrollado en **Python** sobre la API de **Interactive Brokers (IBKR)** utilizando `ib_insync`.

Implementa una estrategia de **swing trading cuantitativo** basada en el indicador **Kalman Hull Supertrend**, con control completo de riesgo, gestión de capital y sincronización en tiempo real con la cuenta de IBKR.

---

## ⚙️ Arquitectura principal

### Modos de ejecución

| Modo | Descripción |
|------|--------------|
| **BACKTEST** | Evalúa la estrategia en datos históricos, simulando fills en la próxima apertura. |
| **PREOPEN** | Analiza la última vela diaria y coloca órdenes MOO/LOO para la próxima apertura. |
| **LIVE** | Opera en tiempo real, monitoreando posiciones abiertas y buscando nuevas señales. |

---

## 📈 Estrategia de trading

### Indicadores principales
- **Kalman Filter + Hull MA** → suaviza el precio reduciendo ruido.  
- **Supertrend adaptativo** → determina la tendencia dominante.  
- **Señales (signal)**:
  - `+1`: reversión alcista → **entrada long**  
  - `-1`: reversión bajista → **salida / cierre**

---

## 💰 Gestión de capital y riesgo

| Parámetro | Propósito |
|------------|------------|
| `MAX_OPEN_TRADES` | Máximo de operaciones simultáneas. |
| `BUDGET_PER_TRADE` | Monto asignado por operación. |
| `MAX_LOSS_PCT` | Stop-loss duro (% desde la entrada). |
| `STOP_ATR_MULT` | Stop dinámico (ATR trailing). |
| `KILL_SWITCH_DD_PCT` | Cierra todo si el drawdown diario supera cierto límite. |

El sizing puede basarse en:
- **CASH disponible**, o  
- **Buying Power (BP)** con tope configurable.

---

## 🧩 Gestión de salidas (Take Profit + Stop BE)

### 🔹 Toma parcial de ganancias al +8 %

Cuando una posición supera +8 %:

1. Cierra **la mitad** de la posición (`MarketOrder SELL`).  
2. Inserta en la base una nueva posición por la mitad restante.  
3. Coloca una orden **Stop real en IBKR al precio de entrada** (Break-Even).

```python
if gain_pct >= 8.0 and qty > 1:
    ib.placeOrder(c, MarketOrder('SELL', half_qty))
    close_trade_db(symbol, exit_price=close_px, comentario_extra='TP parcial 8%')
    insert_trade_open_db(symbol, remaining_qty, entry_price=entry_px, comentario='Reentry BE')
    stop_order = StopOrder('SELL', remaining_qty, stopPrice=entry_px)
    ib.placeOrder(c, stop_order)
💡 Resultado: si el precio cae luego del +8 %, se protege el capital sin perder la ganancia ya realizada.

🧮 Reconciliación automática de fills
Función: reconcile_fills_update_db()

Sincroniza en cada ciclo LIVE los fills ejecutados en IBKR con la base local trades.db:

Actualiza precios de entrada (BUY fills).

Cierra operaciones abiertas si hay una venta total.

Ajusta cantidad si hay ventas parciales.

Marca comentarios como STOP BE fill o SELL parcial fill.

Esto garantiza que el estado del bot siempre coincida con la cuenta real, incluso si los stops se ejecutan mientras el bot no está activo.

🧠 Lógica general de operación (modo LIVE)
Reconcile → sincroniza fills de IBKR ↔ DB.

Analiza posiciones abiertas:

Si señal opuesta o stop → cerrar.

Si +8 % → tomar mitad y mover stop a BE.

Escanea nuevas oportunidades:

Filtra por precio, volumen y régimen de mercado (SPY > SMA200).

Rankeado por RS (Relative Strength) de 20 días.

Envía órdenes (MarketOrder o Limit-On-Open).

Actualiza DB y duerme 15 minutos.

🧱 Persistencia y estructura de datos
SQLite (trades.db) con tabla:

sql
Copiar código
CREATE TABLE operaciones (
    id INTEGER PRIMARY KEY,
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
);
Cada registro representa una operación (abierta o cerrada).
Se mantiene un índice único (ticker, estado='ABIERTA').

🔐 Conexión con IBKR
Usa ib_insync.IB() para conectarse al TWS o IB Gateway (port 7497).

Soporta reconexión automática (ensure_ib_connection()).

Detecta y reimporta posiciones reales si la DB se borra (resync_from_ibkr()).

📊 Backtest y métricas
Modo BACKTEST:

Simula operaciones a la apertura siguiente (next open).

Calcula:

Profit Factor (PF)

Win Rate

Avg Return %

PnL total

También permite evaluar carteras completas con ranking RS y gestión de slots simultáneos.

🧩 Tecnologías principales
Python 3.11+

ib_insync

pandas / numpy

sqlite3

(opcional) matplotlib para reporting

⚙️ Ejecución
bash
Copiar código
# Backtest
MODE=BACKTEST python main.py

# Live (Paper Trading o real)
MODE=LIVE python main.py

# Preopen (coloca órdenes OPG para la apertura)
MODE=PREOPEN python main.py
🧠 Flujo de una operación
vbnet
Copiar código
BUY AMD x6 @ 100.00
↓
Sube +8 % → vende 3 y coloca Stop BE en 100.00
↓
Stop BE ejecutado @ 100.00
↓
Resultado neto: +4 % total sobre posición original
📈 Ejemplo de resultados
Métrica	Valor
Profit Factor	2.5 – 7.0
Win Rate	40 – 60 %
Drawdown estimado	< 10 %
Capital sugerido	Desde 900 USD (3 posiciones de 300 USD)

💬 Créditos y autor
Desarrollado por Lucio Nigro
Estrategia y código original basados en el indicador Kalman Hull Supertrend (adaptación BackQuant).
Incluye mejoras de gestión de riesgo, reconciliación y manejo de fills para IBKR Paper/Live Trading.