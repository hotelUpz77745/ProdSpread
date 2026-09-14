# ============================================================
# FILE: CORE/math_core.py
# ROLE: Mathematical core, signal evaluation, spreads and fee calculations.
# ============================================================
import numpy as np
from numba import njit
from typing import List, Any

@njit(fastmath=True, cache=True)
def calc_vwap_usd_jit(book_array: np.ndarray, target_usd: float, volatility_discount: float) -> float:
    """
    Calculates the Volume-Weighted Average Price (VWAP) for entering a given USD volume.
    book_array: array of shape (N, 2), where [:, 0] are prices, [:, 1] are volumes.
    """
    total_coins = 0.0
    needed_usd = target_usd
    
    for i in range(book_array.shape[0]):
        price = book_array[i, 0]
        qty = book_array[i, 1] * volatility_discount
        level_usd = price * qty
        
        if needed_usd >= level_usd:
            total_coins += qty
            needed_usd -= level_usd
        else:
            coins_to_buy = needed_usd / price
            total_coins += coins_to_buy
            needed_usd = 0.0
            break
            
    if needed_usd > 0.0 or total_coins == 0.0:
        return 0.0
        
    return target_usd / total_coins

@njit(fastmath=True, cache=True)
def calc_vwap_qty_jit(book_array: np.ndarray, target_qty: float, volatility_discount: float) -> float:
    """
    Calculates the Volume-Weighted Average Price (VWAP) for exiting a given coin quantity.
    book_array: array of shape (N, 2), where [:, 0] are prices, [:, 1] are volumes.
    """
    total_usd = 0.0
    needed_qty = target_qty
    
    for i in range(book_array.shape[0]):
        price = book_array[i, 0]
        qty = book_array[i, 1] * volatility_discount
        
        if needed_qty >= qty:
            total_usd += price * qty
            needed_qty -= qty
        else:
            total_usd += price * needed_qty
            needed_qty = 0.0
            break
            
    if needed_qty > 0.0 or target_qty == 0.0:
        return 0.0
        
    return total_usd / target_qty

@njit(fastmath=True, cache=True)
def calc_vwap_and_deepest_price_jit(book_array: np.ndarray, target_qty: float, volatility_discount: float):
    """
    Calculates VWAP and deepest touched price for absorbing target_qty.
    Returns (vwap_price, deepest_price). If insufficient volume, returns (0.0, 0.0).
    """
    total_usd = 0.0
    needed_qty = target_qty
    deepest_price = 0.0
    
    for i in range(book_array.shape[0]):
        price = book_array[i, 0]
        qty = book_array[i, 1] * volatility_discount
        deepest_price = price
        
        if needed_qty >= qty:
            total_usd += price * qty
            needed_qty -= qty
        else:
            total_usd += price * needed_qty
            needed_qty = 0.0
            break
            
    if needed_qty > 0.0 or target_qty == 0.0:
        return 0.0, 0.0
        
    return total_usd / target_qty, deepest_price

@njit(fastmath=True, cache=True)
def calc_execution_qty_limit_jit(book_array: np.ndarray, target_qty: float, limit_price: float, is_buy: bool, volatility_discount: float):
    """
    Calculates the filled quantity and VWAP price constrained by a limit price.
    Returns (vwap_price, filled_qty)
    """
    total_usd = 0.0
    needed_qty = target_qty
    filled_qty = 0.0
    
    for i in range(book_array.shape[0]):
        price = book_array[i, 0]
        qty = book_array[i, 1] * volatility_discount
        
        # Stop if price is worse than limit price
        if is_buy and price > limit_price:
            break
        if not is_buy and price < limit_price:
            break
            
        if needed_qty >= qty:
            total_usd += price * qty
            needed_qty -= qty
            filled_qty += qty
        else:
            total_usd += price * needed_qty
            filled_qty += needed_qty
            needed_qty = 0.0
            break
            
    if filled_qty == 0.0:
        return 0.0, 0.0
        
    return total_usd / filled_qty, filled_qty

class OrderbookUtils:
    """
    Utilities for orderbook processing. Wrappers over JIT functions to safely pass
    input data from Python structures (lists) into NumPy arrays for Numba.
    """
    
    @staticmethod
    def calculate_vwap_by_usd(book_side: List[Any], target_usd: float, volatility_discount: float) -> float:
        arr = np.array(book_side, dtype=np.float64)
        if arr.shape[0] == 0:
            return 0.0
        return calc_vwap_usd_jit(arr, target_usd, volatility_discount)

    @staticmethod
    def calculate_vwap_by_qty(book_side: List[Any], target_qty: float, volatility_discount: float) -> float:
        arr = np.array(book_side, dtype=np.float64)
        if arr.shape[0] == 0:
            return 0.0
        return calc_vwap_qty_jit(arr, target_qty, volatility_discount)

    @staticmethod
    def calculate_vwap_and_deepest_price(book_side: List[Any], target_qty: float, volatility_discount: float = 1.0) -> tuple:
        arr = np.array(book_side, dtype=np.float64)
        if arr.shape[0] == 0:
            return 0.0, 0.0
        return calc_vwap_and_deepest_price_jit(arr, target_qty, volatility_discount)

    @staticmethod
    def calculate_execution_by_qty_and_limit(book_side: List[Any], target_qty: float, limit_price: float, is_buy: bool, volatility_discount: float) -> tuple:
        arr = np.array(book_side, dtype=np.float64)
        if arr.shape[0] == 0:
            return 0.0, 0.0
        return calc_execution_qty_limit_jit(arr, target_qty, limit_price, is_buy, volatility_discount)

    @staticmethod
    def find_first_qualified_level(levels: List[Any], min_usd: float, is_ask: bool) -> tuple:
        """
        Finds first orderbook level with volume >= min_usd.
        Returns (index, price, usd_volume).
        If not found, returns (-1, np.inf if is_ask else 0.0, 0.0).
        """
        if not levels:
            return -1, np.inf if is_ask else 0.0, 0.0
            
        for i in range(len(levels)):
            lvl = levels[i]
            p = float(lvl[0])
            q = float(lvl[1])
            usd_vol = p * q
            if usd_vol >= min_usd or min_usd <= 0.0:
                return i, p, usd_vol
                
        return -1, np.inf if is_ask else 0.0, 0.0


@njit(fastmath=True, cache=True)
def pre_calculate_orderbook(prices: np.ndarray, active_routes: np.ndarray, top_n: int, min_top_depth_usd: float = 0.0) -> np.ndarray:
    """
    Calculates mathematical spread for all active routes (up to 21),
    sorts descending by spread and returns top N candidates.
    If prices has 4 columns: [ask_p, ask_usd, bid_p, bid_usd],
    filters out routes with top volume below min_top_depth_usd.
    """
    M = active_routes.shape[0]
    out = np.zeros((M, 5), dtype=np.float64)
    has_depth = (prices.shape[1] >= 4)
    
    for i in range(M):
        ex1 = int(active_routes[i, 0])
        ex2 = int(active_routes[i, 1])
        
        if has_depth:
            ask1 = prices[ex1, 0]
            ask1_usd = prices[ex1, 1]
            bid1 = prices[ex1, 2]
            bid1_usd = prices[ex1, 3]
            
            ask2 = prices[ex2, 0]
            ask2_usd = prices[ex2, 1]
            bid2 = prices[ex2, 2]
            bid2_usd = prices[ex2, 3]
        else:
            ask1 = prices[ex1, 0]
            ask1_usd = 999999.0
            bid1 = prices[ex1, 1]
            bid1_usd = 999999.0
            
            ask2 = prices[ex2, 0]
            ask2_usd = 999999.0
            bid2 = prices[ex2, 1]
            bid2_usd = 999999.0
        
        if ask1 == np.inf or bid1 == 0.0 or ask2 == np.inf or bid2 == 0.0:
            out[i, 0] = -1.0
            out[i, 1] = -1.0
            out[i, 2] = -999.0
            out[i, 3] = 0.0
            out[i, 4] = 0.0
            continue
            
        # Direction 1: Long ex1, Short ex2 (buy ask1, sell bid2)
        if min_top_depth_usd > 0.0 and (ask1_usd < min_top_depth_usd or bid2_usd < min_top_depth_usd):
            spread_1 = -999.0
        else:
            spread_1 = (bid2 - ask1) / bid2
            
        # Direction 2: Long ex2, Short ex1 (buy ask2, sell bid1)
        if min_top_depth_usd > 0.0 and (ask2_usd < min_top_depth_usd or bid1_usd < min_top_depth_usd):
            spread_2 = -999.0
        else:
            spread_2 = (bid1 - ask2) / bid1
            
        if spread_1 <= -999.0 and spread_2 <= -999.0:
            out[i, 0] = -1.0
            out[i, 1] = -1.0
            out[i, 2] = -999.0
            out[i, 3] = 0.0
            out[i, 4] = 0.0
            continue
            
        if spread_1 >= spread_2:
            out[i, 0] = ex1       
            out[i, 1] = ex2       
            out[i, 2] = spread_1  
            out[i, 3] = ask1      
            out[i, 4] = bid2      
        else:
            out[i, 0] = ex2       
            out[i, 1] = ex1       
            out[i, 2] = spread_2  
            out[i, 3] = ask2      
            out[i, 4] = bid1      
            
    # Bubble sort descending by spread
    for i in range(M):
        for j in range(0, M - i - 1):
            if out[j, 2] < out[j + 1, 2]:
                for k in range(5):
                    temp = out[j, k]
                    out[j, k] = out[j + 1, k]
                    out[j + 1, k] = temp
                    
    if top_n > 0 and top_n < M:
        return out[:top_n]
    return out[:M]

@njit(fastmath=True, cache=True)
def is_stale_jit(binance_ts: float, kucoin_ts: float, now_ts: float, timeout: float = 5.0) -> bool:
    if binance_ts <= 0.0 or kucoin_ts <= 0.0:
        return True
    if (now_ts - binance_ts) > timeout or (now_ts - kucoin_ts) > timeout:
        return True
    return False

from collections import deque
import math
import time
from typing import Dict, Tuple, Optional

class StaticDetector:
    """
    Детектор покоя стоячей ноги (Static Leg Detector).
    1. Накапливает историю цен за скользящее окно buffer_window_sec.
       Подрезка хвостов (popleft) и вычисление среднего арифметического
       накопленного ряда (math.fsum) выполняются на низком уровне Си в CPython.
    2. При обнаружении нестояка (delta > max_static_leg_pct) включается
       карантин-кулдаун ровно на окно buffer_window_sec. В течение этого окна
       метод возвращает False, пока входящие тики вхолостую обновляют буфер,
       полностью вымывая аномальные цены и устраняя эффект 'резинового буфера'.
    """
    def __init__(self, cfg: dict):
        static_cfg = cfg["trading_rules"]["entry"]["static_detector"]
        self.is_enabled = bool(static_cfg["enabled"])
        self.static_leg = str(static_cfg["static_leg"]).upper()
        self.max_static_leg_pct = float(static_cfg["max_static_leg_pct"])
        self.buffer_window_sec = float(static_cfg["buffer_window_sec"])
            
        # Хранилище тиков на низкоуровневых Си-деках CPython:
        # (symbol, exchange) -> deque of float timestamps
        self._ts_buffers: Dict[Tuple[str, str], deque] = {}
        # (symbol, exchange) -> deque of float prices
        self._price_buffers: Dict[Tuple[str, str], deque] = {}
        
        # Кулдаун после обнаружения нестояка: (symbol, exchange) -> float (deadline ts_mono)
        self._cool_off_until: Dict[Tuple[str, str], float] = {}

    @property
    def _buffers(self) -> Dict[Tuple[str, str], deque]:
        """Свойство совместимости для прямого доступа к парам (ts, price)."""
        res = {}
        for k in self._ts_buffers:
            res[k] = deque(zip(self._ts_buffers[k], self._price_buffers[k]))
        return res

    @staticmethod
    def calc_top3_mid_price(bids: list, asks: list) -> float:
        """
        Экономный и точный расчет цены: полусумма средних первых 3 асков и 3 бидов.
        ((ask1 + ask2 + ask3)/3 + (bid1 + bid2 + bid3)/3) / 2
        """
        b_slice = bids[:3]
        a_slice = asks[:3]
        if not b_slice or not a_slice:
            return 0.0
        sum_b = sum(float(b[0]) for b in b_slice) / len(b_slice)
        sum_a = sum(float(a[0]) for a in a_slice) / len(a_slice)
        return (sum_b + sum_a) / 2.0

    def update(self, sym: str, ex: str, price: float, ts_mono: float):
        """
        Параллельное накопление тика в буфер с подрезкой хвостов на Си.
        popleft() и append() выполняются на уровне языка Си в CPython (_collectionsmodule.c).
        """
        if price <= 0.0:
            return
        key = (sym, ex)
        if key not in self._ts_buffers:
            self._ts_buffers[key] = deque()
            self._price_buffers[key] = deque()
            
        ts_buf = self._ts_buffers[key]
        price_buf = self._price_buffers[key]
        
        # 1. Низкоуровневая подрезка устаревших тиков на Си: popleft() в CPython _collectionsmodule.c
        cutoff = ts_mono - self.buffer_window_sec
        while ts_buf and ts_buf[0] < cutoff:
            ts_buf.popleft()
            price_buf.popleft()

        # 2. Накопление нового значения (вхолостую во время кулдауна или штатно)
        ts_buf.append(ts_mono)
        price_buf.append(price)

    def is_leg_static(self, sym: str, ex: str, current_price: float, ts_mono: Optional[float] = None) -> Tuple[bool, str, float]:
        """
        Проверяет покой стоячей ноги ex:
        1. Если действует кулдаун после нестояка — возвращает False, ожидая полного выбывания аномалии.
        2. Считает среднее арифметическое на Си через math.fsum.
        3. Если delta > max_static_leg_pct — ставит кулдаун на buffer_window_sec и возвращает False.
        """
        if not self.is_enabled:
            return True, "STATIC_CHECK_DISABLED", 0.0
            
        if ts_mono is None:
            ts_mono = time.monotonic()
            
        key = (sym, ex)
        
        # 1. Проверка активного кулдауна после нестояка (вымывание 'резинового буфера')
        cool_deadline = self._cool_off_until.get(key, 0.0)
        if ts_mono < cool_deadline:
            remaining = cool_deadline - ts_mono
            return False, f"LEG_COOLING_OFF (remaining {remaining:.3f}s)", 0.0
            
        price_buf = self._price_buffers.get(key)
        if not price_buf or len(price_buf) < 1:
            # Первый тик / пустой буфер — 1-е значение тоже значение
            return True, "FIRST_TICK_BASELINE", 0.0
            
        # 2. Среднее арифметическое накопленного ряда на Си (math.fsum)
        mean_price = math.fsum(price_buf) / len(price_buf)
        if mean_price <= 0.0:
            return True, "INVALID_MEAN_PRICE", 0.0
            
        # 3. Относительное отклонение текущей цены
        deviation = abs(current_price - mean_price) / mean_price
        
        if deviation > self.max_static_leg_pct:
            # Нестояк зафиксирован: взводим кулдаун ровно на окно buffer_window_sec,
            # чтобы аномальная цена и переходной шум полностью выбыли из буфера!
            self._cool_off_until[key] = ts_mono + self.buffer_window_sec
            return False, f"LEG_NOT_STATIC (dev {deviation*100:.3f}% > max {self.max_static_leg_pct*100:.3f}%, cooldown {self.buffer_window_sec:.3f}s)", deviation
            
        return True, "LEG_STATIC_OK", deviation

    def check_impulse(self, sym: str, oracle_ex: str, target_ex: str, oracle_price: float, target_price: float, side: Optional[str] = None, ts_mono: Optional[float] = None) -> Tuple[bool, str, float, float]:
        static_ex = target_ex if self.static_leg == "TARGET" else oracle_ex
        chk_price = target_price if self.static_leg == "TARGET" else oracle_price
        is_st, reason, dev = self.is_leg_static(sym, static_ex, chk_price, ts_mono)
        return is_st, reason, 0.0, dev

ImpulseDetector = StaticDetector
