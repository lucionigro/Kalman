"""
Backtester Profesional con Datos de IBKR
Usa el gateway de Interactive Brokers para datos históricos reales
Compatible con el código de producción existente
"""

from ib_insync import *
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta, timezone
import time
import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Optional
import pickle
import pytz

# ===================== CONFIGURACIÓN ESTRATEGIA =====================
class Config:
    """Configuración centralizada - idéntica a producción"""
    
    # Conexión IBKR
    IB_PORT = 4001  # 7497 para papel
    IB_CLIENT_ID = 2  # Usar diferente ID para backtest
    
    # Universo de símbolos (mismo que producción)
    SYMBOLS = [
        # TECH / GROWTH
        'NVDA','AMD','INTC','SMCI','ARM','TSM','MU','AAPL','MSFT','META','GOOG','AMZN','NFLX',
        'ADBE','CRM','ORCL','NOW','SNOW','DDOG','CRWD','NET','ZS','OKTA','MDB','SHOP','ABNB',
        'UBER','RBLX','PLTR','RDDT','HOOD','COIN','SOFI','AFRM','AI','IONQ','PATH',
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
    
    # Parámetros de estrategia (empezamos con los agresivos del código original)
    PRICE_SOURCE = "hl2"          
    MEASUREMENT_NOISE = 0.25      #0.25
    PROCESS_NOISE = 0.07          #0.07
    ATR_PERIOD = 10                #10
    ATR_FACTOR = 2.0              #2.0
    
    # Risk Management
    MAX_OPEN_TRADES = 5
    BUDGET_PER_TRADE = 1000.0      # Mismo que producción
    RESERVE_CASH_PCT = 0.00
    USE_BUYING_POWER = False
    
    # Costos reales IBKR
    COMMISSION_OPEN = 1.0         # Comisión mínima IBKR
    COMMISSION_CLOSE = 1.0
    SLIPPAGE_PER_SH = 0.02        # Slippage por acción
    SLIPPAGE_PCT = 0.05           # Slippage porcentual (0.05%)
    
    # Stops
    STOP_ATR_MULT = 2.5
    APPLY_ATR_TRAIL = False       # Como en tu config
    MAX_LOSS_PCT = 0.05           # 5% stop loss
    KILL_SWITCH_DD_PCT = 0.05     # 5% daily drawdown
    
    # Filtros
    MIN_PRICE = 5.0
    ADV_MIN_USD = 25_000_000
    RS_LOOKBACK_BARS = 20
    RS_MIN = 0.03                 # 7% outperformance vs SPY
    
    REQUIRE_MARKET_UPTREND = True
    SMA_UPTREND_LEN = 50
    
    # Backtesting
    START_DATE = datetime(2022, 1, 1)
    END_DATE = datetime(2024, 12, 31)
    INITIAL_CAPITAL = 100_000
    
    # Cache
    CACHE_DIR = 'backtest_cache'
    ENABLE_CACHE = True
    PACING_SECONDS = 0.5  # Para no sobrecargar IBKR


# ===================== GESTIÓN DE DATOS IBKR =====================
class IBKRDataManager:
    """Gestión de datos históricos usando IBKR directamente"""
    
    def __init__(self, ib_connection):
        self.ib = ib_connection
        self.cache_dir = Config.CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)
        self.data_cache = {}
        
    def get_cache_path(self, symbol: str, bar_size: str) -> str:
        """Path para cache local"""
        return os.path.join(self.cache_dir, f"{symbol}_{bar_size.replace(' ', '_')}.pkl")
    
    def load_from_cache(self, symbol: str, bar_size: str) -> Optional[pd.DataFrame]:
        """Cargar datos del cache si existen y son recientes"""
        if not Config.ENABLE_CACHE:
            return None
            
        cache_path = self.get_cache_path(symbol, bar_size)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    cached_data = pickle.load(f)
                    # Verificar si el cache es reciente (menos de 1 día)
                    if datetime.now() - cached_data['timestamp'] < timedelta(days=1):
                        return cached_data['data']
            except Exception as e:
                print(f"⚠️ Error leyendo cache para {symbol}: {e}")
        return None
    
    def save_to_cache(self, symbol: str, bar_size: str, data: pd.DataFrame):
        """Guardar datos en cache"""
        if not Config.ENABLE_CACHE:
            return
            
        cache_path = self.get_cache_path(symbol, bar_size)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump({
                    'timestamp': datetime.now(),
                    'data': data
                }, f)
        except Exception as e:
            print(f"⚠️ Error guardando cache para {symbol}: {e}")
    
    def fetch_historical_data(self, symbol: str, duration: str = "2 Y", 
                            bar_size: str = "1 day", use_rth: bool = True) -> pd.DataFrame:
        """Obtener datos históricos de IBKR"""
        
        # Intentar cargar del cache primero
        cached = self.load_from_cache(symbol, bar_size)
        if cached is not None:
            return cached
        
        try:
            # Crear contrato
            contract = Stock(symbol, 'SMART', 'USD')
            
            # Calificar contrato
            qualified = self.ib.qualifyContracts(contract)
            if not qualified:
                print(f"❌ No se pudo calificar {symbol}")
                return pd.DataFrame()
            
            contract = qualified[0]
            
            # Solicitar datos históricos
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=use_rth,
                formatDate=1
            )
            
            if not bars:
                print(f"⚠️ Sin datos para {symbol}")
                return pd.DataFrame()
            
            # Convertir a DataFrame
            df = util.df(bars)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Agregar columnas calculadas (compatibles con tu código)
            df['HL2'] = (df['high'] + df['low']) / 2
            df['HLC3'] = (df['high'] + df['low'] + df['close']) / 3
            df['Returns'] = df['close'].pct_change()
            df['Dollar_Volume'] = df['volume'] * df['close']
            
            # Calcular True Range y ATR
            df['TR'] = self.calculate_true_range(df)
            df['ATR'] = df['TR'].rolling(Config.ATR_PERIOD).mean()
            
            # Renombrar columnas para compatibilidad
            df.rename(columns={
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }, inplace=True)
            
            # Guardar en cache
            self.save_to_cache(symbol, bar_size, df)
            
            # Rate limiting
            time.sleep(Config.PACING_SECONDS)
            
            return df
            
        except Exception as e:
            print(f"❌ Error obteniendo datos para {symbol}: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def calculate_true_range(df: pd.DataFrame) -> pd.Series:
        """Calcula True Range - compatible con tu código original"""
        high = df['high'] if 'high' in df.columns else df['High']
        low = df['low'] if 'low' in df.columns else df['Low']
        close = df['close'] if 'close' in df.columns else df['Close']
        
        high_low = high - low
        high_close = abs(high - close.shift())
        low_close = abs(low - close.shift())
        
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr


# ===================== INDICADORES (usando tus funciones originales) =====================
class Indicators:
    """Indicadores técnicos - copiados de tu código original"""
    
    @staticmethod
    def f_kalman_streaming(prices, measurement_noise=1.0, process_noise=0.01):
        """Filtro de Kalman - versión de tu código"""
        state = float(prices.iloc[0])
        p = 1.0
        out = [state]
        
        for z in prices.iloc[1:]:
            # Predicción
            p = p + process_noise
            
            # Actualización
            k = p / (p + measurement_noise)
            state = state + k * (z - state)
            p = (1 - k) * p
            
            out.append(state)
        
        return pd.Series(out, index=prices.index)
    
    @staticmethod
    def f_supertrend(df, atr_period=14, factor=3.0):
        """Supertrend - versión de tu código"""
        df = df.copy()
        
        # Calcular ATR si no existe
        if 'ATR' not in df.columns:
            df['TR'] = IBKRDataManager.calculate_true_range(df)
            df['ATR'] = df['TR'].rolling(atr_period).mean()
        
        hl2 = (df['High'] + df['Low']) / 2
        
        # Bandas
        up = hl2 - (factor * df['ATR'])
        dn = hl2 + (factor * df['ATR'])
        
        df['up'] = 0.0
        df['dn'] = 0.0
        df['trend'] = 1
        df['supertrend'] = 0.0
        
        for i in range(1, len(df)):
            # Upper band
            if pd.notna(up.iloc[i]):
                df.loc[df.index[i], 'up'] = max(up.iloc[i], df['up'].iloc[i-1]) \
                    if df['Close'].iloc[i-1] > df['up'].iloc[i-1] else up.iloc[i]
            
            # Lower band  
            if pd.notna(dn.iloc[i]):
                df.loc[df.index[i], 'dn'] = min(dn.iloc[i], df['dn'].iloc[i-1]) \
                    if df['Close'].iloc[i-1] < df['dn'].iloc[i-1] else dn.iloc[i]
            
            # Trend
            if df['Close'].iloc[i] > df['dn'].iloc[i-1]:
                df.loc[df.index[i], 'trend'] = 1
            elif df['Close'].iloc[i] < df['up'].iloc[i-1]:
                df.loc[df.index[i], 'trend'] = -1
            else:
                df.loc[df.index[i], 'trend'] = df['trend'].iloc[i-1]
            
            # Supertrend
            if df['trend'].iloc[i] == 1:
                df.loc[df.index[i], 'supertrend'] = df['up'].iloc[i]
            else:
                df.loc[df.index[i], 'supertrend'] = df['dn'].iloc[i]
        
        return df


# ===================== SISTEMA DE TRADING =====================
class Position:
    """Posición individual - compatible con tu DB"""
    
    def __init__(self, ticker: str, entry_date: datetime, entry_price: float,
                 cantidad: int, stop_loss: float = None):
        self.ticker = ticker
        self.tipo = "LONG"
        self.cantidad = cantidad
        self.precio_entrada = entry_price
        self.fecha_apertura = entry_date
        self.stop_loss = stop_loss
        self.highest_price = entry_price
        self.precio_salida = None
        self.fecha_cierre = None
        self.estado = "ABIERTA"
        self.pnl = 0
        self.retorno_pct = 0
        self.comentario = ""
        
    def update_trailing_stop(self, current_price: float, atr: float):
        """Actualizar trailing stop basado en ATR"""
        if not Config.APPLY_ATR_TRAIL:
            return
            
        self.highest_price = max(self.highest_price, current_price)
        new_stop = self.highest_price - (atr * Config.STOP_ATR_MULT)
        
        if self.stop_loss is None or new_stop > self.stop_loss:
            self.stop_loss = new_stop
    
    def should_exit(self, current_price: float, signal: int, 
                    supertrend_value: float) -> Tuple[bool, str]:
        """Verificar si debe cerrar - lógica de tu código"""
        
        # 1. Hard stop loss
        if current_price <= self.precio_entrada * (1 - Config.MAX_LOSS_PCT):
            return True, "MAX_LOSS"
        
        # 2. Señal de Supertrend
        if signal == -1 or current_price < supertrend_value:
            return True, "SIGNAL_EXIT"
        
        # 3. Trailing stop (si está activo)
        if Config.APPLY_ATR_TRAIL and self.stop_loss and current_price <= self.stop_loss:
            return True, "TRAILING_STOP"
        
        return False, ""
    
    def close(self, exit_date: datetime, exit_price: float, comentario: str = ""):
        """Cerrar posición"""
        self.fecha_cierre = exit_date
        self.precio_salida = exit_price
        self.estado = "CERRADA"
        self.pnl = (exit_price - self.precio_entrada) * self.cantidad
        self.retorno_pct = ((exit_price - self.precio_entrada) / self.precio_entrada) * 100
        self.comentario = comentario
        
        # Restar comisiones
        self.pnl -= (Config.COMMISSION_OPEN + Config.COMMISSION_CLOSE)
        
        # Restar slippage estimado
        slippage_cost = Config.SLIPPAGE_PER_SH * self.cantidad * 2  # entrada y salida
        self.pnl -= slippage_cost


class Backtester:
    """Motor de backtesting usando datos IBKR"""
    
    def __init__(self):
        # Conectar a IBKR
        self.ib = IB()
        self.connect_ibkr()
        
        # Managers
        self.data_mgr = IBKRDataManager(self.ib)
        
        # Portfolio
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Position] = []
        self.cash = Config.INITIAL_CAPITAL
        self.initial_capital = Config.INITIAL_CAPITAL
        
        # Tracking
        self.portfolio_history = []
        self.trades_log = []
        self.daily_stats = []
        
    def connect_ibkr(self):
        """Conectar a IBKR con reintentos"""
        max_retries = 3
        for i in range(max_retries):
            try:
                self.ib.connect('127.0.0.1', Config.IB_PORT, clientId=Config.IB_CLIENT_ID)
                print(f"✅ Conectado a IBKR (puerto {Config.IB_PORT})")
                return
            except Exception as e:
                print(f"⚠️ Intento {i+1}/{max_retries} falló: {e}")
                time.sleep(2)
        
        raise RuntimeError("No se pudo conectar a IBKR")
    
    def calculate_position_size(self, price: float) -> int:
        """Calcular tamaño de posición - usa lógica de tu código"""
        available_slots = Config.MAX_OPEN_TRADES - len(self.positions)
        if available_slots <= 0:
            return 0
        
        # Usar budget fijo como en tu código
        position_value = Config.BUDGET_PER_TRADE
        shares = int(position_value / price)
        
        # Verificar que tenemos suficiente cash
        slippage = max(price * Config.SLIPPAGE_PCT / 100, Config.SLIPPAGE_PER_SH)
        total_cost = (shares * (price + slippage)) + Config.COMMISSION_OPEN
        
        if total_cost > self.cash:
            # Ajustar shares si no hay suficiente cash
            available_for_position = self.cash - Config.COMMISSION_OPEN
            shares = int(available_for_position / (price + slippage))
        
        return max(0, shares)
    
    def calculate_rs_score(self, symbol: str, date: datetime, lookback: int = 20) -> float:
        """Calcular Relative Strength vs SPY"""
        try:
            # Obtener datos del símbolo y SPY
            symbol_data = self.data_mgr.fetch_historical_data(symbol)
            spy_data = self.data_mgr.fetch_historical_data('SPY')
            
            if symbol_data.empty or spy_data.empty:
                return -999
            
            # Calcular retornos
            if date in symbol_data.index and date in spy_data.index:
                # Encontrar fecha de lookback
                dates = symbol_data.index[symbol_data.index <= date]
                if len(dates) > lookback:
                    start_date = dates[-lookback-1]
                    
                    symbol_return = (symbol_data.loc[date, 'Close'] / 
                                   symbol_data.loc[start_date, 'Close'] - 1)
                    spy_return = (spy_data.loc[date, 'Close'] / 
                                spy_data.loc[start_date, 'Close'] - 1)
                    
                    return symbol_return - spy_return
            
            return -999
            
        except Exception as e:
            print(f"⚠️ Error calculando RS para {symbol}: {e}")
            return -999
    
    def check_market_regime(self, date: datetime) -> bool:
        """Verificar si SPY está en tendencia alcista"""
        if not Config.REQUIRE_MARKET_UPTREND:
            return True
        
        try:
            spy_data = self.data_mgr.fetch_historical_data('SPY')
            if date not in spy_data.index:
                return False
            
            sma = spy_data['Close'][:date].rolling(Config.SMA_UPTREND_LEN).mean()
            if len(sma) > 0 and not pd.isna(sma.iloc[-1]):
                return spy_data.loc[date, 'Close'] > sma.iloc[-1]
            
            return True
            
        except Exception:
            return True  # Default a True si hay error
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generar señales usando tus indicadores"""
        if df.empty or len(df) < Config.ATR_PERIOD + 1:
            return df
        
        # Hacer copia explícita para evitar warnings
        df = df.copy()
        
        # Aplicar Kalman
        df['Kalman'] = Indicators.f_kalman_streaming(
            df['HL2'],
            Config.MEASUREMENT_NOISE,
            Config.PROCESS_NOISE
        )
        
        # Aplicar Supertrend
        df = Indicators.f_supertrend(df, Config.ATR_PERIOD, Config.ATR_FACTOR)
        
        # Generar señales
        df['Signal'] = 0
        
        # LONG cuando Kalman sube Y precio > supertrend
        long_cond = (
            (df['Kalman'] > df['Kalman'].shift(1)) &
            (df['Close'] > df['supertrend']) &
            (df['trend'] == 1)
        )
        
        # EXIT cuando Kalman baja O precio < supertrend
        exit_cond = (
            (df['Kalman'] < df['Kalman'].shift(1)) |
            (df['Close'] < df['supertrend']) |
            (df['trend'] == -1)
        )
        
        df.loc[long_cond, 'Signal'] = 1
        df.loc[exit_cond, 'Signal'] = -1
        
        return df
    
    def check_filters(self, symbol: str, date: datetime, df: pd.DataFrame) -> bool:
        """Verificar todos los filtros de entrada"""
        try:
            if date not in df.index:
                return False
            
            # Precio mínimo
            if df.loc[date, 'Close'] < Config.MIN_PRICE:
                return False
            
            # Volumen mínimo en dólares
            if df.loc[date, 'Dollar_Volume'] < Config.ADV_MIN_USD:
                return False
            
            # RS mínimo
            rs_score = self.calculate_rs_score(symbol, date, Config.RS_LOOKBACK_BARS)
            if rs_score < Config.RS_MIN:
                return False
            
            return True
            
        except Exception:
            return False
    
    def run_backtest(self):
        """Ejecutar backtest principal"""
        print("\n" + "="*60)
        print("🚀 INICIANDO BACKTEST CON DATOS IBKR")
        print("="*60)
        print(f"📊 Capital Inicial: ${Config.INITIAL_CAPITAL:,.0f}")
        print(f"📅 Periodo: {Config.START_DATE.date()} a {Config.END_DATE.date()}")
        print(f"🎯 Universo: {len(Config.SYMBOLS)} símbolos")
        print(f"🔌 Conectado a IBKR puerto {Config.IB_PORT}\n")
        
        # Obtener fechas de trading de SPY
        spy_data = self.data_mgr.fetch_historical_data('SPY', '3 Y', '1 day')
        if spy_data.empty:
            raise RuntimeError("No se pudieron obtener datos de SPY")
        
        # Filtrar por rango de fechas
        mask = (spy_data.index >= Config.START_DATE) & (spy_data.index <= Config.END_DATE)
        trading_dates = spy_data.index[mask]
        
        print(f"📆 Total días de trading: {len(trading_dates)}\n")
        
        # Prefetch de datos para todos los símbolos
        print("📥 Descargando datos históricos...")
        for i, symbol in enumerate(Config.SYMBOLS):
            if i % 10 == 0:
                print(f"  Progreso: {i}/{len(Config.SYMBOLS)} símbolos...")
            _ = self.data_mgr.fetch_historical_data(symbol, '3 Y', '1 day')
        
        print("\n🔄 Iniciando simulación...\n")
        
        # Variables para kill switch
        daily_start_value = Config.INITIAL_CAPITAL
        
        # Loop principal
        for date_idx, current_date in enumerate(trading_dates):
            
            # Resetear valor inicial del día
            if date_idx == 0 or trading_dates[date_idx-1].day != current_date.day:
                positions_value = sum(
                    pos.cantidad * self.data_mgr.fetch_historical_data(sym).loc[current_date, 'Close']
                    for sym, pos in self.positions.items()
                    if current_date in self.data_mgr.fetch_historical_data(sym).index
                )
                daily_start_value = self.cash + positions_value
            
            # PASO 1: Actualizar y cerrar posiciones existentes
            positions_to_close = []
            
            for symbol, pos in self.positions.items():
                df = self.data_mgr.fetch_historical_data(symbol)
                if current_date not in df.index:
                    continue
                
                # Generar señales
                df_signals = self.generate_signals(df[:current_date])
                if df_signals.empty:
                    continue
                
                current_price = df.loc[current_date, 'Close']
                current_signal = df_signals['Signal'].iloc[-1]
                supertrend_value = df_signals['supertrend'].iloc[-1]
                current_atr = df.loc[current_date, 'ATR']
                
                # Actualizar trailing stop
                pos.update_trailing_stop(current_price, current_atr)
                
                # Verificar salida
                should_exit, exit_reason = pos.should_exit(
                    current_price, current_signal, supertrend_value
                )
                
                if should_exit:
                    positions_to_close.append((symbol, exit_reason))
            
            # Cerrar posiciones
            for symbol, exit_reason in positions_to_close:
                pos = self.positions[symbol]
                df = self.data_mgr.fetch_historical_data(symbol)
                
                # Simular ejecución con slippage
                exit_price = df.loc[current_date, 'Close']
                # Usar slippage porcentual o por acción (el mayor)
                slippage_pct = exit_price * Config.SLIPPAGE_PCT / 100
                slippage_per_sh = Config.SLIPPAGE_PER_SH
                actual_slippage = max(slippage_pct, slippage_per_sh)
                exit_price -= actual_slippage  # Slippage negativo en venta
                
                pos.close(current_date, exit_price, exit_reason)
                self.cash += (exit_price * pos.cantidad) - Config.COMMISSION_CLOSE
                
                self.closed_positions.append(pos)
                del self.positions[symbol]
                
                # Log
                print(f"📉 {current_date.date()} | VENTA {symbol:5} @ ${exit_price:7.2f} | "
                      f"PnL: ${pos.pnl:7.2f} ({pos.retorno_pct:6.2f}%) | {exit_reason}")
                
                self.trades_log.append({
                    'Date': current_date,
                    'Symbol': symbol,
                    'Action': 'SELL',
                    'Price': exit_price,
                    'Shares': pos.cantidad,
                    'PnL': pos.pnl,
                    'Return%': pos.retorno_pct,
                    'Reason': exit_reason
                })
            
            # PASO 2: Kill Switch - verificar drawdown diario
            current_positions_value = sum(
                pos.cantidad * self.data_mgr.fetch_historical_data(sym).loc[current_date, 'Close']
                for sym, pos in self.positions.items()
                if current_date in self.data_mgr.fetch_historical_data(sym).index
            )
            current_total_value = self.cash + current_positions_value
            daily_dd = (current_total_value - daily_start_value) / daily_start_value
            
            if daily_dd <= -Config.KILL_SWITCH_DD_PCT:
                print(f"\n🚨 KILL SWITCH ACTIVADO! DD del día: {daily_dd*100:.2f}%")
                print(f"   Cerrando todas las posiciones...\n")
                
                # Cerrar todo
                for symbol in list(self.positions.keys()):
                    pos = self.positions[symbol]
                    df = self.data_mgr.fetch_historical_data(symbol)
                    exit_price = df.loc[current_date, 'Close']
                    # Aplicar slippage
                    slippage = max(exit_price * Config.SLIPPAGE_PCT / 100, Config.SLIPPAGE_PER_SH)
                    exit_price -= slippage
                    
                    pos.close(current_date, exit_price, "KILL_SWITCH")
                    self.cash += (exit_price * pos.cantidad) - Config.COMMISSION_CLOSE
                    
                    self.closed_positions.append(pos)
                    del self.positions[symbol]
                    
                    print(f"📉 KILL SWITCH: {symbol} @ ${exit_price:.2f}")
                
                # Esperar hasta el siguiente día para continuar
                continue
            
            # PASO 3: Buscar nuevas entradas si hay slots disponibles
            if len(self.positions) < Config.MAX_OPEN_TRADES:
                
                # Verificar régimen de mercado
                if not self.check_market_regime(current_date):
                    continue
                
                # Rankear candidatos por RS
                candidates = []
                for symbol in Config.SYMBOLS:
                    if symbol in self.positions:  # Ya tenemos posición
                        continue
                    
                    df = self.data_mgr.fetch_historical_data(symbol)
                    if df.empty or current_date not in df.index:
                        continue
                    
                    # Verificar filtros
                    if not self.check_filters(symbol, current_date, df):
                        continue
                    
                    # Calcular RS score
                    rs_score = self.calculate_rs_score(symbol, current_date, Config.RS_LOOKBACK_BARS)
                    if rs_score >= Config.RS_MIN:
                        candidates.append((symbol, rs_score))
                
                # Ordenar por RS y tomar los mejores
                candidates.sort(key=lambda x: x[1], reverse=True)
                
                # Intentar entrar en los mejores candidatos
                for symbol, rs_score in candidates[:5]:  # Top 5
                    if len(self.positions) >= Config.MAX_OPEN_TRADES:
                        break
                    
                    df = self.data_mgr.fetch_historical_data(symbol)
                    df_signals = self.generate_signals(df[:current_date])
                    
                    if df_signals.empty:
                        continue
                    
                    # Verificar señal de entrada
                    if df_signals['Signal'].iloc[-1] == 1:
                        entry_price = df.loc[current_date, 'Close']
                        shares = self.calculate_position_size(entry_price)
                        
                        if shares > 0:
                            # Aplicar slippage en compra
                            slippage_pct = entry_price * Config.SLIPPAGE_PCT / 100
                            slippage_per_sh = Config.SLIPPAGE_PER_SH
                            actual_slippage = max(slippage_pct, slippage_per_sh)
                            entry_price += actual_slippage  # Slippage positivo en compra
                            
                            # Crear posición
                            initial_stop = entry_price * (1 - Config.MAX_LOSS_PCT)
                            pos = Position(symbol, current_date, entry_price, shares, initial_stop)
                            
                            # Actualizar portfolio
                            total_cost = (entry_price * shares) + Config.COMMISSION_OPEN
                            self.cash -= total_cost
                            self.positions[symbol] = pos
                            
                            print(f"📈 {current_date.date()} | COMPRA {symbol:5} @ ${entry_price:7.2f} | "
                                  f"Shares: {shares:4} | RS: {rs_score:.3f}")
                            
                            self.trades_log.append({
                                'Date': current_date,
                                'Symbol': symbol,
                                'Action': 'BUY',
                                'Price': entry_price,
                                'Shares': shares,
                                'RS_Score': rs_score
                            })
            
            # PASO 4: Guardar estado del portfolio
            positions_value = sum(
                pos.cantidad * self.data_mgr.fetch_historical_data(sym).loc[current_date, 'Close']
                for sym, pos in self.positions.items()
                if current_date in self.data_mgr.fetch_historical_data(sym).index
            )
            
            total_value = self.cash + positions_value
            
            self.portfolio_history.append({
                'Date': current_date,
                'Cash': self.cash,
                'Positions_Value': positions_value,
                'Total_Value': total_value,
                'Num_Positions': len(self.positions),
                'Daily_DD%': daily_dd * 100
            })
            
            # Progress update
            if date_idx % 20 == 0 and date_idx > 0:
                progress = (date_idx / len(trading_dates)) * 100
                print(f"\n⏳ Progreso: {progress:.1f}% | Valor: ${total_value:,.0f} | "
                      f"Posiciones: {len(self.positions)}/{Config.MAX_OPEN_TRADES}\n")
        
        # Cerrar posiciones restantes al final
        print("\n🔚 Cerrando posiciones abiertas al final del periodo...")
        for symbol, pos in self.positions.items():
            df = self.data_mgr.fetch_historical_data(symbol)
            if trading_dates[-1] in df.index:
                exit_price = df.loc[trading_dates[-1], 'Close']
                pos.close(trading_dates[-1], exit_price, "END_OF_BACKTEST")
                self.closed_positions.append(pos)
                print(f"📉 Cierre final {symbol} @ ${exit_price:.2f}")
        
        # Desconectar de IBKR
        self.ib.disconnect()
        
        return self.analyze_results()
    
    def analyze_results(self) -> Dict:
        """Analizar resultados y generar métricas"""
        print("\n" + "="*60)
        print("📊 ANÁLISIS DE RESULTADOS - BACKTEST IBKR")
        print("="*60)
        
        # Crear DataFrame con historia del portfolio
        portfolio_df = pd.DataFrame(self.portfolio_history).set_index('Date')
        
        # Métricas básicas
        initial_value = Config.INITIAL_CAPITAL
        final_value = portfolio_df['Total_Value'].iloc[-1]
        total_return = (final_value - initial_value) / initial_value * 100
        
        print(f"\n💰 Capital Inicial: ${initial_value:,.0f}")
        print(f"💎 Valor Final: ${final_value:,.0f}")
        print(f"📈 Retorno Total: {total_return:.2f}%")
        print(f"💵 P&L Total: ${final_value - initial_value:,.0f}")
        
        # Calcular retornos diarios
        portfolio_df['Daily_Return'] = portfolio_df['Total_Value'].pct_change()
        
        # Trading Statistics
        total_trades = len(self.closed_positions)
        if total_trades > 0:
            winning_trades = [p for p in self.closed_positions if p.pnl > 0]
            losing_trades = [p for p in self.closed_positions if p.pnl <= 0]
            
            win_rate = (len(winning_trades) / total_trades) * 100
            avg_win = np.mean([p.retorno_pct for p in winning_trades]) if winning_trades else 0
            avg_loss = np.mean([p.retorno_pct for p in losing_trades]) if losing_trades else 0
            
            gross_profit = sum([p.pnl for p in winning_trades])
            gross_loss = abs(sum([p.pnl for p in losing_trades]))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
            
            print(f"\n🎯 Total Operaciones: {total_trades}")
            print(f"✅ Ganadoras: {len(winning_trades)} ({win_rate:.1f}%)")
            print(f"❌ Perdedoras: {len(losing_trades)} ({100-win_rate:.1f}%)")
            print(f"📊 Profit Factor: {profit_factor:.2f}")
            print(f"💚 Ganancia Promedio: {avg_win:.2f}%")
            print(f"🔴 Pérdida Promedio: {avg_loss:.2f}%")
            
            # Expectancy
            expectancy = (win_rate/100 * avg_win) + ((100-win_rate)/100 * avg_loss)
            print(f"🎲 Expectancia por trade: {expectancy:.2f}%")
        
        # Risk Metrics
        returns = portfolio_df['Daily_Return'].dropna()
        if len(returns) > 0:
            # Sharpe Ratio
            sharpe = returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0
            
            # Max Drawdown
            cum_returns = (1 + returns).cumprod()
            running_max = cum_returns.expanding().max()
            drawdown = (cum_returns - running_max) / running_max * 100
            max_drawdown = drawdown.min()
            
            # Sortino Ratio
            downside = returns[returns < 0]
            sortino = returns.mean() / downside.std() * np.sqrt(252) if len(downside) > 0 else 0
            
            print(f"\n📉 Máximo Drawdown: {max_drawdown:.2f}%")
            print(f"📊 Sharpe Ratio: {sharpe:.2f}")
            print(f"📈 Sortino Ratio: {sortino:.2f}")
            print(f"〰️ Volatilidad Anual: {returns.std() * np.sqrt(252) * 100:.2f}%")
        
        # Análisis por tipo de salida
        exit_analysis = {}
        for pos in self.closed_positions:
            reason = pos.comentario
            if reason not in exit_analysis:
                exit_analysis[reason] = {'count': 0, 'pnl': 0}
            exit_analysis[reason]['count'] += 1
            exit_analysis[reason]['pnl'] += pos.pnl
        
        print(f"\n🔍 Análisis por tipo de salida:")
        for reason, data in exit_analysis.items():
            print(f"  {reason}: {data['count']} trades | PnL: ${data['pnl']:.0f}")
        
        # Guardar resultados
        portfolio_df.to_csv('ibkr_backtest_portfolio.csv')
        pd.DataFrame(self.trades_log).to_csv('ibkr_backtest_trades.csv', index=False)
        
        print(f"\n💾 Resultados guardados:")
        print(f"   - ibkr_backtest_portfolio.csv")
        print(f"   - ibkr_backtest_trades.csv")
        
        # Crear gráficos
        self.plot_results(portfolio_df)
        
        # Evaluar viabilidad
        results = {
            'total_return': total_return,
            'final_value': final_value,
            'total_trades': total_trades,
            'win_rate': win_rate if total_trades > 0 else 0,
            'profit_factor': profit_factor if total_trades > 0 else 0,
            'sharpe_ratio': sharpe if len(returns) > 0 else 0,
            'max_drawdown': max_drawdown if len(returns) > 0 else 0,
            'expectancy': expectancy if total_trades > 0 else 0
        }
        
        self.evaluate_strategy(results)
        
        return results
    
    def plot_results(self, portfolio_df):
        """Generar gráficos de resultados"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # 1. Curva de Equity
        ax = axes[0, 0]
        portfolio_df['Total_Value'].plot(ax=ax, color='blue', linewidth=2)
        ax.axhline(y=Config.INITIAL_CAPITAL, color='gray', linestyle='--', alpha=0.5)
        ax.set_title('Curva de Equity', fontsize=12, fontweight='bold')
        ax.set_ylabel('Valor Portfolio ($)')
        ax.grid(True, alpha=0.3)
        
        # 2. Drawdown
        ax = axes[0, 1]
        returns = portfolio_df['Daily_Return'].fillna(0)
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.expanding().max()
        drawdown = ((cum_returns - running_max) / running_max * 100)
        
        drawdown.plot(ax=ax, color='red', linewidth=1)
        ax.fill_between(drawdown.index, drawdown, 0, color='red', alpha=0.3)
        ax.set_title('Drawdown', fontsize=12, fontweight='bold')
        ax.set_ylabel('Drawdown (%)')
        ax.grid(True, alpha=0.3)
        
        # 3. Distribución de Retornos
        ax = axes[1, 0]
        if self.closed_positions:
            returns_list = [p.retorno_pct for p in self.closed_positions]
            ax.hist(returns_list, bins=30, color='skyblue', edgecolor='black', alpha=0.7)
            ax.axvline(x=0, color='red', linestyle='--', alpha=0.5)
            ax.axvline(x=np.mean(returns_list), color='green', linestyle='-', 
                      label=f'Media: {np.mean(returns_list):.2f}%')
            ax.set_title('Distribución de Retornos', fontsize=12, fontweight='bold')
            ax.set_xlabel('Retorno (%)')
            ax.set_ylabel('Frecuencia')
            ax.legend()
        ax.grid(True, alpha=0.3)
        
        # 4. Composición del Portfolio
        ax = axes[1, 1]
        ax.stackplot(portfolio_df.index,
                    portfolio_df['Cash'],
                    portfolio_df['Positions_Value'],
                    labels=['Cash', 'Posiciones'],
                    colors=['gold', 'steelblue'],
                    alpha=0.7)
        ax.set_title('Composición del Portfolio', fontsize=12, fontweight='bold')
        ax.set_ylabel('Valor ($)')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        
        plt.suptitle('Resultados Backtest IBKR', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig('ibkr_backtest_results.png', dpi=150, bbox_inches='tight')
        plt.show()
        
        print("📊 Gráficos guardados en 'ibkr_backtest_results.png'")
    
    def evaluate_strategy(self, results):
        """Evaluar si la estrategia es viable para producción"""
        print("\n" + "="*60)
        print("🔍 EVALUACIÓN DE VIABILIDAD PARA PRODUCCIÓN")
        print("="*60)
        
        viable = True
        issues = []
        
        # Criterios de evaluación
        if results['sharpe_ratio'] < 1.0:
            viable = False
            issues.append(f"Sharpe Ratio bajo: {results['sharpe_ratio']:.2f} (necesitas > 1.0)")
        
        if results['max_drawdown'] < -20:
            viable = False
            issues.append(f"Drawdown excesivo: {results['max_drawdown']:.2f}% (límite -20%)")
        
        if results['win_rate'] < 40:
            viable = False
            issues.append(f"Win rate bajo: {results['win_rate']:.1f}% (necesitas > 40%)")
        
        if results['profit_factor'] < 1.2:
            viable = False
            issues.append(f"Profit factor insuficiente: {results['profit_factor']:.2f} (necesitas > 1.2)")
        
        if results['expectancy'] <= 0:
            viable = False
            issues.append(f"Expectancia negativa: {results['expectancy']:.2f}%")
        
        if viable:
            print("\n✅ ESTRATEGIA POTENCIALMENTE VIABLE")
            print("\nRecomendaciones antes de producción:")
            print("1. Paper trade mínimo 1 mes")
            print("2. Comenzar con 10-20% del capital")
            print("3. Monitoreo diario las primeras semanas")
            print("4. Tener plan de contingencia para drawdowns")
        else:
            print("\n⚠️ ESTRATEGIA NO VIABLE CON CONFIGURACIÓN ACTUAL")
            print("\nProblemas detectados:")
            for issue in issues:
                print(f"  ❌ {issue}")
            
            print("\n📝 Sugerencias de mejora:")
            print("1. Aumentar MEASUREMENT_NOISE a 0.3-0.5")
            print("2. Usar ATR_PERIOD de 14-20 días")
            print("3. Ajustar ATR_FACTOR a 2.0-3.0")
            print("4. Aumentar BUDGET_PER_TRADE a $1000+")
            print("5. Revisar filtro RS_MIN (bajarlo a 0.03-0.05)")


def main():
    """Función principal"""
    print("\n" + "="*80)
    print("     BACKTESTER PROFESIONAL CON DATOS REALES IBKR")
    print("     Estrategia: Kalman + Supertrend + Risk Management")
    print("="*80)
    
    try:
        # Crear y ejecutar backtester
        backtester = Backtester()
        results = backtester.run_backtest()
        
        print("\n✨ BACKTEST COMPLETADO EXITOSAMENTE")
        
        return results
        
    except Exception as e:
        print(f"\n❌ ERROR EN BACKTEST: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    results = main()