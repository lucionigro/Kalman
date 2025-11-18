"""
IBKR SCANNER – Versión Estable 2025
Autor: Lucio Trading System
Funciona 100% con ib_insync y API moderna.
"""

from ib_insync import *
import time
import json
import os
from datetime import datetime, timedelta


# =============================================================================
# CONFIG GLOBAL
# =============================================================================

CACHE_FILE = "scanner_cache.json"
CACHE_HOURS = 1

MIN_PRICE = 5.0
MAX_RESULTS = 50
PACING = 1.0                # Delay para no romper pacing rules


# =============================================================================
# UTILIDADES
# =============================================================================

def log(msg):
    print(f"[SCANNER] {msg}")


def save_cache(data):
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}

    try:
        with open(CACHE_FILE, "r") as f:
            raw = json.load(f)
        out = {}
        for k, v in raw.items():
            timestamp = datetime.fromisoformat(v["timestamp"])
            out[k] = (timestamp, v["symbols"])
        return out
    except:
        return {}


# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class IBKRUniverseScanner:
    """
    Clase principal de scanner funcional.
    """

    def __init__(self, ib: IB):
        self.ib = ib
        self.cache = load_cache()

    # -------------------------------------------------------------------------

    def _run_scan(self, scanCode, max_results=MAX_RESULTS, min_price=MIN_PRICE):
        """
        Ejecuta un scanner válido (sin cancelSubscription).
        """
        try:
            sub = ScannerSubscription(
                instrument="STK",
                locationCode="STK.US",
                scanCode=scanCode,
            )

            sub.abovePrice = min_price

            results = self.ib.reqScannerData(sub)
            time.sleep(PACING)

            symbols = []
            for r in results[:max_results]:
                try:
                    sym = r.contract.symbol
                    if sym and sym.isalpha() and len(sym) <= 5:
                        symbols.append(sym)
                except:
                    continue

            log(f"{scanCode}: {len(symbols)} símbolos")
            return symbols

        except Exception as e:
            log(f"ERROR en {scanCode}: {e}")
            return []

    # -------------------------------------------------------------------------

    def _get_universe_raw(self, strategy):
        """
        Genera universo según estrategia.
        Solo scanCodes 100% válidos hoy.
        """

        universe = set()

        if strategy == "conservative":
            log("Estrategia CONSERVATIVE")
            universe |= set(self._run_scan("MOST_ACTIVE", 100))
            universe |= set(self._run_scan("TOP_PERC_GAIN", 30))

        elif strategy == "balanced":
            log("Estrategia BALANCED")
            universe |= set(self._run_scan("MOST_ACTIVE", 100))
            universe |= set(self._run_scan("HOT_BY_VOLUME", 50))
            universe |= set(self._run_scan("TOP_PERC_GAIN", 40))

        elif strategy == "momentum":
            log("Estrategia MOMENTUM")
            universe |= set(self._run_scan("TOP_PERC_GAIN", 60))
            universe |= set(self._run_scan("HOT_BY_VOLUME", 40))

        elif strategy == "aggressive":
            log("Estrategia AGGRESSIVE")
            universe |= set(self._run_scan("TOP_PERC_GAIN", 70))
            universe |= set(self._run_scan("HOT_BY_VOLUME", 70))
            universe |= set(self._run_scan("TOP_PERC_LOSE", 40))

        else:
            raise ValueError("Estrategia inválida")

        return sorted(list(universe))

    # -------------------------------------------------------------------------

    def get_universe(self, strategy="balanced", force=False):
        """
        Retorna universo con cache moderno.
        """
        key = f"universe_{strategy}"

        # Cache existente
        if not force and key in self.cache:
            t, symbols = self.cache[key]
            age = datetime.now() - t
            if age < timedelta(hours=CACHE_HOURS):
                log(f"Usando cache ({len(symbols)} símbolos)")
                return symbols

        # Generar nuevo
        symbols = self._get_universe_raw(strategy)
        self.cache[key] = (datetime.now(), symbols)

        # Guardar
        save_cache({
            k: {
                "timestamp": v[0].isoformat(),
                "symbols": v[1]
            }
            for k, v in self.cache.items()
        })

        return symbols


# =============================================================================
# MODO DEMO / TEST
# =============================================================================

if __name__ == "__main__":
    print("\n============================================================")
    print("                 IBKR SCANNER - TEST MODE")
    print("============================================================")

    ib = IB()
    log("Conectando a IBKR...")

    try:
        ib.connect("127.0.0.1", 4001, clientId=99)
        log("Conectado OK\n")

        scanner = IBKRUniverseScanner(ib)

        for strat in ["conservative", "balanced", "momentum", "aggressive"]:
            print("\n------------------------------------------------------------")
            print(f"ESTRATEGIA: {strat.upper()}")
            print("------------------------------------------------------------")

            syms = scanner.get_universe(strat, force=True)
            print(f"\nTotal símbolos: {len(syms)}")
            print("Primeros 30:")
            print(", ".join(syms[:30]))

        ib.disconnect()
        log("Desconectado")

    except Exception as e:
        print("ERROR:", e)
        import traceback
        traceback.print_exc()
