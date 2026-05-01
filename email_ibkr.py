import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import sqlite3
from ib_insync import IB, util
import pandas as pd
from ib_insync import IB, util
import pandas as pd

# ==================== CONFIG SMTP ====================
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
TO_EMAIL = os.environ.get("SMTP_TO", SMTP_USER)

# ==================== CONFIG DB ======================
DB_FILE = "trades_live.db"

# ==================== FUNCIONES GENERALES ======================

def enviar_mail(asunto: str, cuerpo_html: str):
    if not SMTP_USER or not SMTP_PASS or not TO_EMAIL:
        raise RuntimeError("SMTP no configurado. Setear SMTP_USER, SMTP_PASS y SMTP_TO en el entorno.")
    msg = MIMEMultipart("alternative")
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL
    msg["Subject"] = asunto
    msg.attach(MIMEText(cuerpo_html, "html"))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
    print(f"✅ Mail enviado: {asunto}")


# ==================== MAIL ORDEN (BUY/SELL) ======================
def mail_orden(symbol: str, tipo: str, cantidad: int, precio: float, comentario: str = ""):
    estilos = """
    <style>
    body { font-family: Arial, sans-serif; background-color:#fafafa; color:#222; }
    .card {
        background-color: #fff;
        border-radius: 12px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.15);
        padding: 16px 24px;
        width: 450px;
        margin: 20px auto;
    }
    h2 { color:#1a73e8; text-align:center; }
    .buy { color: #0b8f18; font-weight:bold; }
    .sell { color: #d93025; font-weight:bold; }
    .meta { color:#555; font-size:14px; margin-top:10px; }
    .footer { font-size:13px; color:#888; margin-top:15px; text-align:center; }
    </style>
    """

    tipo_upper = tipo.upper()
    color_class = "buy" if tipo_upper == "BUY" else "sell"
    emoji = "🟢" if tipo_upper == "BUY" else "🔴"
    verbo = "COMPRA" if tipo_upper == "BUY" else "VENTA"

    cuerpo = f"""
    <html>
    <head>{estilos}</head>
    <body>
        <div class="card">
            <h2>{emoji} Orden de {verbo}</h2>
            <p><b>Ticker:</b> {symbol}</p>
            <p><b>Tipo:</b> <span class="{color_class}">{tipo_upper}</span></p>
            <p><b>Cantidad:</b> {cantidad}</p>
            <p><b>Precio ejecutado:</b> ${precio:.2f}</p>
            <p class="meta"><b>Fecha:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            {'<p class="meta"><b>Comentario:</b> '+comentario+'</p>' if comentario else ''}
            <div class="footer">⚙️ Enviado automáticamente por el bot Kalman-Hull-Supertrend</div>
        </div>
    </body>
    </html>
    """

    enviar_mail(f"📈 Orden {tipo_upper} ejecutada - {symbol}", cuerpo)


# ==================== MAIL RESUMEN DIARIO ======================



def obtener_posiciones_ibkr(port=None, client_id=None, account_id=None):
    """
    Trae posiciones actuales y PnL% desde IBKR (usa datos delayed si no hay realtime).
    Se conecta al puerto indicado (por defecto 4001 = LIVE; 7497 = PAPER).
    Filtra por la cuenta IBKR especificada.
    """
    port = int(port or os.environ.get("KALMAN_IB_PORT", "4001"))
    client_id = int(client_id or os.environ.get("KALMAN_IB_CLIENT_ID", "99"))
    account_id = account_id or os.environ.get("KALMAN_ACCOUNT_ID", "")
    util.startLoop()
    ib = IB()
    try:
        ib.connect('127.0.0.1', port, clientId=client_id)
        ib.reqMarketDataType(3)  # usar datos en diferido si no hay realtime
    except Exception as e:
        print(f"[IBKR][ERROR] No se pudo conectar al puerto {port}: {e}")
        return pd.DataFrame()

    posiciones = []

    try:
        for p in ib.positions():
            # 🔹 Filtrar sólo posiciones de la cuenta activa
            if account_id and getattr(p, "account", None) != account_id:
                continue

            sym = p.contract.symbol
            qty = int(p.position)
            avg_cost = float(p.avgCost)
            if qty == 0 or avg_cost == 0:
                continue

            try:
                ticker = ib.reqMktData(p.contract, "", False, False)
                ib.sleep(1.5)
                last_px = ticker.last or ticker.close or 0.0
                if not last_px:
                    bars = ib.reqHistoricalData(p.contract, '', '2 D', '1 day', 'TRADES', True)
                    if bars:
                        last_px = bars[-1].close
                if not last_px:
                    print(f"[WARN] {sym}: sin datos de precio (premercado o sin feed)")
                    continue

                pnl_pct = (last_px / avg_cost - 1) * 100
                posiciones.append({
                    "ticker": sym,
                    "cantidad": qty,
                    "entrada": avg_cost,
                    "ultimo": last_px,
                    "pnl_pct": pnl_pct
                })

            except Exception as e:
                print(f"[IBKR][{sym}] {e}")

    except Exception as e:
        print(f"[IBKR][ERROR] {e}")

    ib.disconnect()
    return pd.DataFrame(posiciones)



def obtener_cerradas_db():
    """Extrae operaciones cerradas del día desde trades.db."""
    conn = sqlite3.connect(DB_FILE)
    query = """
        SELECT ticker, cantidad, precio_entrada, precio_salida, ROUND(retorno_pct,2)
        FROM operaciones
        WHERE estado='CERRADA' 
        AND DATE(fecha_cierre) = DATE('now')
        ORDER BY ticker
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def generar_html(pos_abiertas: pd.DataFrame, pos_cerradas: pd.DataFrame):
    """Genera el cuerpo HTML con estilo y totales."""
    estilos = """
    <style>
    body { font-family: Arial, sans-serif; background-color:#fafafa; color:#222; }
    h2 { color:#1a73e8; }
    h3 { color:#444; margin-top:25px; }
    table { border-collapse: collapse; width: 90%; margin: 10px 0; font-size: 14px; }
    th, td { border: 1px solid #ddd; padding: 6px 8px; text-align: center; }
    th { background-color: #f1f1f1; }
    tr:nth-child(even) { background-color: #f9f9f9; }
    .pos { color: #0b8f18; font-weight:bold; }
    .neg { color: #d93025; font-weight:bold; }
    .neutral { color: #555; }
    .total { background-color:#f1f1f1; font-weight:bold; }
    .footer { font-size:12px; color:#888; margin-top:20px; text-align:center; }
    </style>
    """

    # ==== Abiertas ====
    if not pos_abiertas.empty:
        rows_abiertas = ""
        for _, r in pos_abiertas.iterrows():
            color_class = "pos" if r.pnl_pct > 0 else "neg" if r.pnl_pct < 0 else "neutral"
            rows_abiertas += f"""
                <tr>
                    <td>{r.ticker}</td>
                    <td>{r.cantidad}</td>
                    <td>${r.entrada:.2f}</td>
                    <td>${r.ultimo:.2f}</td>
                    <td class="{color_class}">{r.pnl_pct:.2f}%</td>
                </tr>
            """
        avg_pnl = pos_abiertas['pnl_pct'].mean()
        color_avg = "pos" if avg_pnl > 0 else "neg" if avg_pnl < 0 else "neutral"
        total_html = f"<tr class='total'><td colspan='4'>Promedio PnL%</td><td class='{color_avg}'>{avg_pnl:.2f}%</td></tr>"
        abiertas_html = f"""
            <h3>📈 Posiciones Abiertas (IBKR)</h3>
            <table>
                <tr><th>Ticker</th><th>Cant.</th><th>Entrada</th><th>Último</th><th>%PnL</th></tr>
                {rows_abiertas}{total_html}
            </table>
        """
    else:
        abiertas_html = "<p>No hay posiciones abiertas actualmente.</p>"

    # ==== Cerradas ====
    if not pos_cerradas.empty:
        rows_cerradas = ""
        for _, r in pos_cerradas.iterrows():
            color_class = "pos" if r.retorno_pct > 0 else "neg" if r.retorno_pct < 0 else "neutral"
            rows_cerradas += f"""
                <tr>
                    <td>{r.ticker}</td>
                    <td>{r.cantidad}</td>
                    <td>${r.precio_entrada:.2f}</td>
                    <td>${r.precio_salida:.2f}</td>
                    <td class="{color_class}">{r.retorno_pct:.2f}%</td>
                </tr>
            """
        avg_close = pos_cerradas['retorno_pct'].mean()
        color_avg2 = "pos" if avg_close > 0 else "neg" if avg_close < 0 else "neutral"
        total_html2 = f"<tr class='total'><td colspan='4'>Promedio Ret%</td><td class='{color_avg2}'>{avg_close:.2f}%</td></tr>"
        cerradas_html = f"""
            <h3>💼 Operaciones Cerradas (hoy)</h3>
            <table>
                <tr><th>Ticker</th><th>Cant.</th><th>Entrada</th><th>Salida</th><th>%Ret</th></tr>
                {rows_cerradas}{total_html2}
            </table>
        """
    else:
        cerradas_html = "<p>No hubo operaciones cerradas hoy.</p>"

    cuerpo = f"""
    <html>
    <head>{estilos}</head>
    <body>
        <h2>📊 Estado General - {datetime.now().strftime('%Y-%m-%d')}</h2>
        {abiertas_html}
        {cerradas_html}
        <p class="footer">ℹ️ Datos de mercado en diferido (IBKR Paper/Pre-market)</p>
    </body>
    </html>
    """
    return cuerpo


# ==================== MAIN (solo si se ejecuta directo) ======================
if __name__ == "__main__":
    abiertas = obtener_posiciones_ibkr()
    cerradas = obtener_cerradas_db()
    html = generar_html(abiertas, cerradas)
    enviar_mail("📊 Estado diario con IBKR", html)
    # Test individual:
    # mail_orden("NVDA", "BUY", 5, 126.45, "Entrada LIVE KalmanHullST")
