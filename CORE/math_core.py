# ============================================================
# FILE: CORE/math_core.py
# ROLE: Математическое ядро, расчет сигналов, спредов и комиссий.
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

@njit(fastmath=True, cache=True)
def calc_book_decay_velocity_jit(current_book: np.ndarray, prev_book: np.ndarray, dt: float, top_k: int = 3) -> float:
    """
    Рассчитывает скорость таяния ликвидности стакана (Book Decay Velocity / OFI):
    Отрицательное значение означает, что ликвидность на верхних уровнях тает (съедается или снимается).
    Возвращает относительную скорость изменения объема (% / сек).
    """
    if dt <= 0.0 or current_book.shape[0] == 0 or prev_book.shape[0] == 0:
        return 0.0
    
    k_curr = min(top_k, current_book.shape[0])
    k_prev = min(top_k, prev_book.shape[0])
    
    vol_curr = 0.0
    for i in range(k_curr):
        vol_curr += current_book[i, 1]
        
    vol_prev = 0.0
    for i in range(k_prev):
        vol_prev += prev_book[i, 1]
        
    if vol_prev <= 0.0:
        return 0.0
        
    return (vol_curr - vol_prev) / vol_prev / dt

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
    def calculate_execution_by_qty_and_limit(book_side: List[Any], target_qty: float, limit_price: float, is_buy: bool, volatility_discount: float) -> tuple:
        arr = np.array(book_side, dtype=np.float64)
        if arr.shape[0] == 0:
            return 0.0, 0.0
        return calc_execution_qty_limit_jit(arr, target_qty, limit_price, is_buy, volatility_discount)

    @staticmethod
    def calculate_book_decay_velocity(current_side: List[Any], prev_side: List[Any], dt: float, top_k: int = 3) -> float:
        if not current_side or not prev_side or dt <= 0.0:
            return 0.0
        arr_curr = np.array(current_side, dtype=np.float64)
        arr_prev = np.array(prev_side, dtype=np.float64)
        return calc_book_decay_velocity_jit(arr_curr, arr_prev, dt, top_k)


@njit(fastmath=True, cache=True)
def pre_calculate_orderbook(prices: np.ndarray, active_routes: np.ndarray, top_n: int) -> np.ndarray:
    """
    Рассчитывает математический спред для всех активных связок (до 21),
    сортирует их по убыванию спреда и возвращает топ N кандидатов.
    """
    M = active_routes.shape[0]
    out = np.zeros((M, 5), dtype=np.float64)
    
    for i in range(M):
        ex1 = int(active_routes[i, 0])
        ex2 = int(active_routes[i, 1])
        
        ask1 = prices[ex1, 0]
        bid1 = prices[ex1, 1]
        ask2 = prices[ex2, 0]
        bid2 = prices[ex2, 1]
        
        if ask1 == np.inf or bid1 == 0.0 or ask2 == np.inf or bid2 == 0.0:
            out[i, 0] = -1.0
            out[i, 1] = -1.0
            out[i, 2] = -999.0
            out[i, 3] = 0.0
            out[i, 4] = 0.0
            continue
            
        # [ИСТОРИЧЕСКАЯ СПРАВКА]
        # Возвращена формула деления на верхнюю точку (Шорт):
        # spread_1 = (bid2 - ask1) / bid2 * 100.0
        # Это дает более жесткий фильтр от шума, синхронизировано с PapperSpread.
        spread_1 = (bid2 - ask1) / bid2 * 100.0
        spread_2 = (bid1 - ask2) / bid1 * 100.0
        
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
            
    # Сортировка пузырьком по убыванию спреда
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
