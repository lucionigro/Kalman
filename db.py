import sqlite3

conn = sqlite3.connect('trades.db')
cur = conn.cursor()

cur.execute("DELETE FROM operaciones;")
conn.commit()

print("✅ Todos los registros fueron eliminados.")
conn.close()
