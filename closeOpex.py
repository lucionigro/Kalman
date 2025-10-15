from ib_insync import *

# === Conectarse a IBKR Paper Trading ===
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)  # 7497 = paper, 7496 = real

# === Obtener posiciones abiertas ===
positions = ib.positions()

if not positions:
    print("✅ No hay posiciones abiertas.")
else:
    print(f"🔹 Cerrando {len(positions)} posiciones abiertas...\n")

    for pos in positions:
        contract = pos.contract
        position = pos.position

        # Determinar acción contraria (para cerrar)
        action = 'SELL' if position > 0 else 'BUY'
        qty = abs(position)

        # Crear orden a mercado
        order = MarketOrder(action, qty)

        # Enviar orden
        trade = ib.placeOrder(contract, order)
        print(f"➡️  {action} {qty} de {contract.symbol}")

    # Esperar confirmación de ejecuciones
    ib.sleep(2)
    print("\n✅ Todas las órdenes fueron enviadas a mercado.")

ib.disconnect()
