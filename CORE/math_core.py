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
        entry_cfg = cfg["trading_rules"]["entry"]
        if "signal_filters" in entry_cfg and "static_detector" in entry_cfg["signal_filters"]:
            static_cfg = entry_cfg["signal_filters"]["static_detector"]
        elif "static_detector" in entry_cfg:
            static_cfg = entry_cfg["static_detector"]
        elif "static_detector" in cfg:
            static_cfg = cfg["static_detector"]
        else:
            static_cfg = entry_cfg

        self.is_enabled = bool(static_cfg["enabled"])
        leg_val = static_cfg["static_leg"]
        if isinstance(leg_val, list):
            self.static_leg = "ANY" if set(str(x).upper() for x in leg_val) >= {"TARGET", "ORACLE"} else str(leg_val[0]).upper()
        else:
            self.static_leg = str(leg_val).upper()

        if "max_static_leg_ratio" in static_cfg:
            self.max_static_leg_ratio = float(static_cfg["max_static_leg_ratio"])
        elif "max_static_leg_pct" in static_cfg:
            self.max_static_leg_ratio = float(static_cfg["max_static_leg_pct"]) / 100.0
        else:
            self.max_static_leg_ratio = float(static_cfg["max_static_leg_ratio"])
        self.max_static_leg_pct = self.max_static_leg_ratio  # backward compatibility alias
        self.buffer_window_sec = float(static_cfg["buffer_window_sec"])
            
        # Хранилище тиков на низкоуровневых Си-деках CPython:
        # (symbol, exchange) -> deque of float timestamps
        self._ts_buffers: Dict[Tuple[str, str], deque] = {}
        # (symbol, exchange) -> deque of float prices
        self._price_buffers: Dict[Tuple[str, str], deque] = {}
        
        # Кулдаун после обнаружения нестояка: (symbol, exchange) -> float (deadline ts_mono)
        self._cool_off_until: Dict[Tuple[str, str], float] = {}

        # Предсигнальный спокойный базис (Quiescent Baseline Tracking):
        # (symbol, oracle_ex, target_ex) -> (ts_mono, oracle_base_price, target_base_price)
        self._quiescent_baselines: Dict[Tuple[str, str, str], Tuple[float, float, float]] = {}

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
            
        ts_buf = self._ts_buffers.get(key)
        price_buf = self._price_buffers.get(key)
        if not price_buf or not ts_buf:
            # Первый тик / пустой буфер — 1-е значение тоже значение
            return True, "FIRST_TICK_BASELINE", 0.0

        cutoff = ts_mono - self.buffer_window_sec
        while ts_buf and ts_buf[0] < cutoff:
            ts_buf.popleft()
            price_buf.popleft()

        if len(price_buf) < 1:
            return True, "FIRST_TICK_BASELINE", 0.0
            
        # 2. Среднее арифметическое накопленного ряда на Си (math.fsum)
        mean_price = math.fsum(price_buf) / len(price_buf)
        if mean_price <= 0.0:
            return True, "INVALID_MEAN_PRICE", 0.0
            
        # 3. Относительное отклонение текущей цены
        deviation = abs(current_price - mean_price) / mean_price
        
        if deviation > self.max_static_leg_ratio:
            # Нестояк зафиксирован: взводим кулдаун ровно на окно buffer_window_sec,
            # чтобы аномальная цена и переходной шум полностью выбыли из буфера!
            self._cool_off_until[key] = ts_mono + self.buffer_window_sec
            return False, f"LEG_NOT_STATIC (dev {deviation*100:.3f}% > max {self.max_static_leg_ratio*100:.3f}%, cooldown {self.buffer_window_sec:.3f}s)", deviation
            
        return True, "LEG_STATIC_OK", deviation

    def evaluate_pair_stability(
        self,
        sym: str,
        oracle_ex: str,
        target_ex: str,
        oracle_price: float,
        target_price: float,
        pre_filter_spread: float,
        ts_mono: Optional[float] = None
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Оценивает стабильность связки (Oracle, Target) через механизм предсигнального спокойного базиса.
        1. Пока спред < pre_filter_spread (когерентное движение ног 'гуськом'), базис непрерывно
           следует за ценами обеих бирж, исключая ложные срабатывания от общего рыночного тренда.
        2. При расширении спреда >= pre_filter_spread базис фиксируется на время импульса.
        3. Замеряются относительные смещения delta_target и delta_oracle относительно предсигнального базиса.
        4. Классифицируются кейсы:
           - Кейс Б (Oracle импульс, Target статика): Target <= max_static, Oracle > max_static
           - Кейс В (Target импульс, Oracle статика): Oracle <= max_static, Target > max_static
           - Кейс А (Обе ноги хаотично разлетелись): Target > max_static, Oracle > max_static -> REJECT
        """
        if not self.is_enabled:
            return True, "STATIC_CHECK_DISABLED", {"case": "DISABLED", "static_leg": self.static_leg}

        if ts_mono is None:
            ts_mono = time.monotonic()

        if oracle_price <= 0.0 or target_price <= 0.0:
            return False, "INVALID_MID_PRICE", {"case": "INVALID"}

        key = (sym, oracle_ex, target_ex)
        max_price = max(oracle_price, target_price)
        raw_spread = abs(oracle_price - target_price) / max_price if max_price > 0.0 else 0.0

        base_record = self._quiescent_baselines.get(key)
        if base_record is None:
            self._quiescent_baselines[key] = (ts_mono, oracle_price, target_price)
            if raw_spread < pre_filter_spread:
                return True, "QUIESCENT_BASELINE_INITIALIZED", {"case": "QUIESCENT", "static_leg": self.static_leg}
            return True, "FIRST_TICK_BASELINE", {"case": "FIRST_TICK", "static_leg": self.static_leg}

        base_ts, base_oracle, base_target = base_record

        # Если спред в пределах нормы (спокойный рынок / ноги идут 'гуськом')
        if raw_spread < pre_filter_spread:
            self._quiescent_baselines[key] = (ts_mono, oracle_price, target_price)
            return True, "QUIESCENT_STATE", {"case": "QUIESCENT", "static_leg": self.static_leg}

        # Спред расширился (импульс)! Базис зафиксирован. Проверяем возраст импульса
        age = ts_mono - base_ts
        max_impulse_age = max(1.5, self.buffer_window_sec * 4.0)
        if age > max_impulse_age:
            # Импульс затух или спред стал структурным (устаревший сигнал)
            self._quiescent_baselines[key] = (ts_mono, oracle_price, target_price)
            return False, f"STALE_IMPULSE (spread persisted {age:.2f}s > {max_impulse_age:.2f}s)", {"case": "STALE"}

        # Расчет относительных смещений от предсигнального базиса
        delta_oracle = abs(oracle_price - base_oracle) / base_oracle if base_oracle > 0.0 else 0.0
        delta_target = abs(target_price - base_target) / base_target if base_target > 0.0 else 0.0

        is_target_static = (delta_target <= self.max_static_leg_ratio)
        is_oracle_static = (delta_oracle <= self.max_static_leg_ratio)
        target_shot = (delta_target > self.max_static_leg_ratio)
        oracle_shot = (delta_oracle > self.max_static_leg_ratio)

        allowed_legs = {"TARGET", "ORACLE"} if self.static_leg in ("ANY", "BOTH", "EITHER", "ALL") else {self.static_leg}

        # Кейс А: Обе ноги разлетелись (хаос)
        if target_shot and oracle_shot:
            return False, f"CASE_A_REJECTED (Both legs moved: Target {delta_target*100:.3f}%, Oracle {delta_oracle*100:.3f}% > max {self.max_static_leg_ratio*100:.3f}%)", {
                "case": "CASE_A", "target_delta": delta_target, "oracle_delta": delta_oracle
            }

        # Кейс Б: Oracle выстрелил, Target стоит
        if is_target_static and oracle_shot:
            if "TARGET" in allowed_legs:
                return True, f"CASE_B_OK (Target static {delta_target*100:.3f}% <= {self.max_static_leg_ratio*100:.3f}%, Oracle impulse {delta_oracle*100:.3f}%)", {
                    "case": "CASE_B", "static_leg": "TARGET", "target_delta": delta_target, "oracle_delta": delta_oracle
                }
            else:
                return False, f"CASE_B_NOT_ALLOWED (config static_leg is {self.static_leg}, requires ORACLE static)", {
                    "case": "CASE_B", "target_delta": delta_target, "oracle_delta": delta_oracle
                }

        # Кейс В: Target выстрелил, Oracle стоит (Опасность токсичного потока!)
        if is_oracle_static and target_shot:
            if "ORACLE" in allowed_legs:
                return True, f"CASE_C_OK (Oracle static {delta_oracle*100:.3f}% <= {self.max_static_leg_ratio*100:.3f}%, Target impulse {delta_target*100:.3f}%)", {
                    "case": "CASE_C", "static_leg": "ORACLE", "target_delta": delta_target, "oracle_delta": delta_oracle
                }
            else:
                return False, f"CASE_C_REJECTED (Target moved {delta_target*100:.3f}%, Oracle static {delta_oracle*100:.3f}% - toxic orderflow danger)", {
                    "case": "CASE_C", "target_delta": delta_target, "oracle_delta": delta_oracle
                }

        # Обе ноги сместились меньше max_static_leg_ratio (умеренный импульс):
        # Та нога, смещение которой больше, признается импульсом, а меньшая - стоячей.
        # Если ни одна нога не сместилась от базиса (delta == 0), направленного импульса не было.
        min_move_threshold = 1e-6
        if delta_target < delta_oracle and delta_oracle > min_move_threshold:
            if "TARGET" in allowed_legs:
                return True, f"CASE_B_OK (Target static {delta_target*100:.3f}%, Oracle impulse {delta_oracle*100:.3f}%)", {
                    "case": "CASE_B", "static_leg": "TARGET", "target_delta": delta_target, "oracle_delta": delta_oracle
                }
        elif delta_oracle < delta_target and delta_target > min_move_threshold:
            if "ORACLE" in allowed_legs:
                return True, f"CASE_C_OK (Oracle static {delta_oracle*100:.3f}%, Target impulse {delta_target*100:.3f}%)", {
                    "case": "CASE_C", "static_leg": "ORACLE", "target_delta": delta_target, "oracle_delta": delta_oracle
                }
            else:
                return False, f"CASE_C_REJECTED (Target moved {delta_target*100:.3f}% > Oracle {delta_oracle*100:.3f}% - toxic orderflow danger)", {
                    "case": "CASE_C", "target_delta": delta_target, "oracle_delta": delta_oracle
                }

        return False, "NO_VALID_STATIC_LEG", {"case": "REJECTED"}

    def check_impulse(self, sym: str, oracle_ex: str, target_ex: str, oracle_price: float, target_price: float, side: Optional[str] = None, ts_mono: Optional[float] = None) -> Tuple[bool, str, float, float]:
        if self.static_leg in ("ANY", "BOTH", "EITHER", "ALL"):
            pre_spread = self.max_static_leg_ratio
            ok, reason, case_info = self.evaluate_pair_stability(sym, oracle_ex, target_ex, oracle_price, target_price, pre_spread, ts_mono)
            dev = case_info.get("target_delta" if case_info.get("static_leg") == "TARGET" else "oracle_delta", 0.0)
            return ok, reason, 0.0, dev
        static_ex = target_ex if self.static_leg == "TARGET" else oracle_ex
        chk_price = target_price if self.static_leg == "TARGET" else oracle_price
        is_st, reason, dev = self.is_leg_static(sym, static_ex, chk_price, ts_mono)
        return is_st, reason, 0.0, dev

ImpulseDetector = StaticDetector


class OrderbookHunter:
    """
    Ядро управления выходом через динамический хантинг стакана (Orderbook Hunting v11).
    Перенесено и адаптировано из Rucheiok Bot 2.0.
    Принцип: Все выходы СТРОГО ЛИМИТНЫЕ (LIMIT_IOC), никаких слепых маркетов.
    """
    @staticmethod
    def calc_virtual_tp(entry_price: float, base_target_price: float, current_rate: float, side: str) -> float:
        """
        Вычисляет виртуальный Take-Profit порог сканирования стакана.
        """
        if entry_price <= 0.0 or base_target_price <= 0.0:
            return entry_price
        if side.upper() == "LONG":
            return entry_price + (base_target_price - entry_price) * current_rate
        else:
            return entry_price - (entry_price - base_target_price) * current_rate

    @staticmethod
    def find_liquidity_target(depth_levels: list, virtual_tp: float, side: str, min_vol: float = 0.0) -> Optional[float]:
        """
        Сканирует стакан Мишени в поисках уровня с максимальным объемом у или выгоднее virtual_tp.
        Для LONG: ищет в bids уровень >= virtual_tp с максимальным объемом.
        Для SHORT: ищет в asks уровень <= virtual_tp с максимальным объемом.
        """
        if not depth_levels or virtual_tp <= 0.0:
            return None
        
        ideal_target_price = None
        max_vol = -1.0
        is_long = side.upper() == "LONG"
        
        for item in depth_levels:
            price = float(item[0])
            vol = float(item[1])
            if is_long:
                if price >= virtual_tp:
                    if vol > max_vol and vol >= min_vol:
                        max_vol = vol
                        ideal_target_price = price
                else:
                    break
            else:
                if price <= virtual_tp:
                    if vol > max_vol and vol >= min_vol:
                        max_vol = vol
                        ideal_target_price = price
                else:
                    break
                    
        return ideal_target_price

    @staticmethod
    def calc_breakeven_price(entry_price: float, taker_fee: float, min_net_profit_ratio: float = 0.0004, side: str = "LONG") -> float:
        """
        Рассчитывает цену безубыточного выхода (включая двойную комиссию входа/выхода + микро-профит).
        """
        if entry_price <= 0.0:
            return entry_price
        required_markup = (taker_fee * 2.0) + min_net_profit_ratio
        if side.upper() == "LONG":
            return entry_price * (1.0 + required_markup)
        else:
            return entry_price * (1.0 - required_markup)

    @staticmethod
    def calc_extrime_price(
        best_bid: float,
        best_ask: float,
        side: str,
        retry_count: int = 0,
        orientation: float = 0.0,
        increase_fraction: float = 0.05
    ) -> float:
        """
        Рассчитывает лимитную цену для ступенчатого Extrime Close.
        mid = (ask1 + bid1) / 2
        base_price = mid + (spread * orientation)
        shift = spread * increase_fraction * retry_count
        target_price = base_price - shift (LONG) / base_price + shift (SHORT)
        """
        if best_bid <= 0.0 or best_ask <= 0.0:
            return 0.0
        mid = (best_ask + best_bid) / 2.0
        spread = best_ask - best_bid
        base_price = mid + (spread * orientation)
        shift = spread * increase_fraction * max(0, retry_count)
        
        if side.upper() == "LONG":
            return max(0.0, base_price - shift)
        else:
            return base_price + shift
