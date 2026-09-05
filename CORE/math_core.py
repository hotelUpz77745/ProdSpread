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


@njit(fastmath=True, cache=True)
def pre_calculate_orderbook(prices: np.ndarray, active_routes: np.ndarray, top_n: int, min_top_depth_usd: float = 0.0) -> np.ndarray:
    """
    Рассчитывает математический спред для всех активных связок (до 21),
    сортирует их по убыванию спреда и возвращает топ N кандидатов.
    Если prices имеет 4 колонки: [ask_p, ask_usd, bid_p, bid_usd],
    отсекает связки с объемом первого уровня ниже min_top_depth_usd.
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
            
        # Направление 1: Long ex1, Short ex2 (покупаем ask1, продаем bid2)
        if min_top_depth_usd > 0.0 and (ask1_usd < min_top_depth_usd or bid2_usd < min_top_depth_usd):
            spread_1 = -999.0
        else:
            spread_1 = (bid2 - ask1) / bid2 * 100.0
            
        # Направление 2: Long ex2, Short ex1 (покупаем ask2, продаем bid1)
        if min_top_depth_usd > 0.0 and (ask2_usd < min_top_depth_usd or bid1_usd < min_top_depth_usd):
            spread_2 = -999.0
        else:
            spread_2 = (bid1 - ask2) / bid1 * 100.0
            
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
