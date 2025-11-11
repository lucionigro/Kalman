from ib_insync import *

# === CONFIGURACIÓN ===
IB_HOST = '127.0.0.1'
IB_PORT = 4001           # cambia a 7497 si es demo
CLIENT_ID = 10           # cualquier número libre
ACCOUNT_ID = 'U22866664' # tu cuenta
SYMBOL = 'AAPL'          # acción de prueba

# === CONEXIÓN ===
ib = IB()
ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID)
print(f"✅ Conectado a IBKR | Account: {ACCOUNT_ID} | Server time: {ib.reqCurrentTime()}")

# === CALIFICAR CONTRATO ===
contract = Stock(SYMBOL, 'SMART', 'USD')
ib.qualifyContracts(contract)

# === SOLICITAR MARKET DATA ===
ticker = ib.reqMktData(contract, '', False, False)
ib.sleep(2)  # esperar a que llegue data

print(f"\n📊 Datos recibidos para {SYMBOL}:")
print(f"Último precio (last): {ticker.last}")
print(f"Precio de mercado (marketPrice): {ticker.marketPrice()}")
print(f"Bid: {ticker.bid}, Ask: {ticker.ask}")

# === VALIDACIÓN ===
if ticker.last or ticker.marketPrice():
    print("\n✅ Market data recibido correctamente (realtime o último tick).")
else:
    print("\n⚠️ No llegó market data. Revisa permisos o suscripción.")

ib.disconnect()
print("\n🔌 Desconectado.")
