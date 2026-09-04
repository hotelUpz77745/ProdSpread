# ============================================================
# FILE: CORE/trading_engine.py
# ROLE: Оценка входа и выхода из позиций.
# ============================================================

from typing import Tuple, Dict, Any
from CORE.math_core import OrderbookUtils

class TradingEngine:
    def __init__(self, cfg: dict, exchanges: dict):
        """
        cfg: весь json-конфиг
        exchanges: маппинг {0: "BINANCE", 1: "KUCOIN", ...} для перевода индексов в имена
        """
        self.cfg = cfg
        self.exchanges = exchanges
        
        # Строго читаем через [''] без get(..., default)
        self.spread_entry = float(self.cfg["trading_rules"]["entry"]["spread_entry"])
        self.check_synthetic_exit = bool(self.cfg["trading_rules"]["entry"]["check_synthetic_exit"])
        self.check_synthetic_slippage = bool(self.cfg["trading_rules"]["entry"]["check_synthetic_slippage"])
        self.max_slippage_ratio = float(self.cfg["trading_rules"]["entry"]["max_slippage_ratio"])
        self.hard_max_slippage = float(self.cfg["trading_rules"]["entry"]["hard_max_slippage"])
        self.check_book_decay = bool(self.cfg["trading_rules"]["entry"].get("check_book_decay", False))
        self.max_book_decay_velocity = float(self.cfg["trading_rules"]["entry"].get("max_book_decay_velocity", -5.0))
        self.decay_map = self.cfg["trading_rules"]["exit"]["profit_decay_map"]
        self.trading_risks = self.cfg["trading_risks"]
        self._book_history = {}

    def _get_vol_discount_entry(self, exchange_name: str) -> float:
        return float(self.trading_risks[exchange_name.lower()]["volatility_discount_entry"])

    def _get_vol_discount_exit(self, exchange_name: str) -> float:
        return float(self.trading_risks[exchange_name.lower()]["volatility_discount_exit"])

    def _get_fee(self, exchange_name: str) -> float:
        return float(self.trading_risks[exchange_name.lower()]["taker_fee"])

    def evaluate_entry(self, 
                       long_book: Dict[str, Any], 
                       short_book: Dict[str, Any], 
                       cand: list,
                       size_usd: float,
                       sym: str = None,
                       now_mono: float = 0.0) -> Tuple[bool, Dict[str, Any]]:
        
        long_idx = int(cand[0])
        short_idx = int(cand[1])
        long_ex = self.exchanges[long_idx]
        short_ex = self.exchanges[short_idx]

        # Защита от токсичного потока: расчет Book Decay Velocity (OFI)
        if self.check_book_decay and sym and now_mono > 0.0:
            hist_long = self._book_history.get((long_ex, sym))
            if hist_long:
                dt_l = now_mono - hist_long["ts"]
                if 0.005 <= dt_l <= 1.0:
                    v_decay_l = OrderbookUtils.calculate_book_decay_velocity(
                        long_book.get("asks", []), hist_long.get("asks", []), dt_l, top_k=3
                    )
                    if v_decay_l < self.max_book_decay_velocity:
                        return False, {"reason": f"BOOK_DECAY_TOXIC (Long Asks draining {v_decay_l:.1f}/s)"}

            hist_short = self._book_history.get((short_ex, sym))
            if hist_short:
                dt_s = now_mono - hist_short["ts"]
                if 0.005 <= dt_s <= 1.0:
                    v_decay_s = OrderbookUtils.calculate_book_decay_velocity(
                        short_book.get("bids", []), hist_short.get("bids", []), dt_s, top_k=3
                    )
                    if v_decay_s < self.max_book_decay_velocity:
                        return False, {"reason": f"BOOK_DECAY_TOXIC (Short Bids draining {v_decay_s:.1f}/s)"}

            self._book_history[(long_ex, sym)] = {
                "asks": long_book.get("asks", [])[:3],
                "bids": long_book.get("bids", [])[:3],
                "ts": now_mono
            }
            self._book_history[(short_ex, sym)] = {
                "asks": short_book.get("asks", [])[:3],
                "bids": short_book.get("bids", [])[:3],
                "ts": now_mono
            }
        
        long_vol = self._get_vol_discount_entry(long_ex)
        short_vol = self._get_vol_discount_entry(short_ex)
        
        # Для лонга - покупаем из асков. Для шорта - продаем в биды.
        long_vwap_ask = OrderbookUtils.calculate_vwap_by_usd(long_book["asks"], size_usd, long_vol)
        short_vwap_bid = OrderbookUtils.calculate_vwap_by_usd(short_book["bids"], size_usd, short_vol)
        
        if long_vwap_ask <= 0 or short_vwap_bid <= 0:
            return False, {"reason": "INSUFFICIENT_VOLUME"}
            
        long_qty = size_usd / long_vwap_ask
        short_qty = size_usd / short_vwap_bid
        
        vwap_spread = (short_vwap_bid - long_vwap_ask) / long_vwap_ask
        
        # Учет собственной комиссии на входе
        entry_long_fee = self._get_fee(long_ex)
        entry_short_fee = self._get_fee(short_ex)
        entry_comm = entry_long_fee + entry_short_fee
        net_spread = vwap_spread - entry_comm
        
        if net_spread < self.spread_entry:
            return False, {
                "reason": f"LOW_SPREAD (Net: {net_spread * 100:.3f}% < {self.spread_entry * 100:.3f}%, Gross: {vwap_spread * 100:.3f}%, Fee: {entry_comm * 100:.3f}%)"
            }
            
        # СИНТЕТИЧЕСКАЯ ПРОВЕРКА ВЫХОДА (Round-Trip Liquidity Check)
        # Опциональный рубильник в конфиге. Симулирует немедленный выход из позиции.
        if self.check_synthetic_exit:
            # Продаем купленный лонг (в BIDS) и откупаем проданный шорт (из ASKS)
            long_vol_exit = self._get_vol_discount_exit(long_ex)
            short_vol_exit = self._get_vol_discount_exit(short_ex)
            long_exit_vwap_bid = OrderbookUtils.calculate_vwap_by_qty(long_book.get("bids", []), long_qty, long_vol_exit)
            short_exit_vwap_ask = OrderbookUtils.calculate_vwap_by_qty(short_book.get("asks", []), short_qty, short_vol_exit)
            
            # Если обратный стакан пуст или не может поглотить наш сайз - отбой
            if long_exit_vwap_bid <= 0 or short_exit_vwap_ask <= 0:
                return False, {"reason": "NO_REVERSE_LIQUIDITY"}
                
            # Защита от конского внутреннего проскальзывания (Synthetic Slippage Check)
            if self.check_synthetic_slippage:
                long_synthetic_slip = (long_vwap_ask - long_exit_vwap_bid) / long_vwap_ask
                short_synthetic_slip = (short_exit_vwap_ask - short_vwap_bid) / short_vwap_bid
                total_slippage = long_synthetic_slip + short_synthetic_slip
                
                max_allowed = net_spread * self.max_slippage_ratio
                
                if total_slippage > max_allowed:
                    return False, {"reason": f"HIGH_REVERSE_SLIPPAGE (Slip: {total_slippage*100:.2f}% > DynMax: {max_allowed*100:.2f}%)"}
                    
                if total_slippage > self.hard_max_slippage:
                    return False, {"reason": f"HARD_SLIPPAGE_LIMIT (Slip: {total_slippage*100:.2f}% > HardMax: {self.hard_max_slippage*100:.2f}%)"}
                    
        return True, {
            "vwap_spread": vwap_spread,
            "net_spread": net_spread,
            "entry_comm": entry_comm,
            "long_avg_price": long_vwap_ask,
            "short_avg_price": short_vwap_bid,
            "long_qty": long_qty,
            "short_qty": short_qty,
            "details": f"Net Spread:{net_spread * 100:+.3f}% (Gross:{vwap_spread * 100:+.3f}%, Fee:{entry_comm * 100:.3f}%)",
            "long_ex": long_ex,
            "short_ex": short_ex
        }

    def evaluate_exit(self, 
                      long_book: Dict[str, Any], 
                      short_book: Dict[str, Any], 
                      long_ex: str,
                      short_ex: str,
                      long_qty: float,
                      short_qty: float,
                      entry_long_price: float,
                      entry_short_price: float,
                      duration_sec: float = 0.0,
                      is_stakan_valid: bool = True,
                      long_executed_volume_rate: float = 1.0,
                      short_executed_volume_rate: float = 1.0) -> Tuple[bool, Dict[str, Any]]:
        
        target_val, exit_level_index = self.get_exit_target_val(duration_sec)
        is_ttl = target_val <= -999.0
        reported_target = None if is_ttl else target_val
        
        if not is_stakan_valid:
            if is_ttl:
                return True, {"net_yield": None, "target_val": reported_target, "vwap_spread_out": None, "long_close_price": None, "short_close_price": None, "reason": "TTL_EXPIRED_STALE_DATA", "exit_level_index": exit_level_index}
            return False, {"net_yield": None, "target_val": reported_target, "reason": "STALE_DATA", "exit_level_index": exit_level_index}
            
        long_vol_exit = self._get_vol_discount_exit(long_ex)
        short_vol_exit = self._get_vol_discount_exit(short_ex)
        
        # Для закрытия лонга - продаем в биды. Для закрытия шорта - покупаем из асков.
        long_vwap_bid = OrderbookUtils.calculate_vwap_by_qty(long_book.get("bids", []), long_qty, long_vol_exit)
        short_vwap_ask = OrderbookUtils.calculate_vwap_by_qty(short_book.get("asks", []), short_qty, short_vol_exit)
        
        if long_vwap_bid <= 0 or short_vwap_ask <= 0:
            if is_ttl:
                return True, {"net_yield": None, "target_val": reported_target, "vwap_spread_out": None, "long_close_price": None, "short_close_price": None, "reason": "TTL_EXPIRED_NO_LIQUIDITY", "exit_level_index": exit_level_index}
            return False, {"net_yield": None, "target_val": reported_target, "reason": "INSUFFICIENT_VOLUME", "exit_level_index": exit_level_index}
             
        long_realized_pnl = (long_vwap_bid - entry_long_price) / entry_long_price
        short_realized_pnl = (entry_short_price - short_vwap_ask) / entry_short_price
            
        long_fee = self._get_fee(long_ex) * long_executed_volume_rate
        short_fee = self._get_fee(short_ex) * short_executed_volume_rate
        total_comm = (long_fee * 2.0) + (short_fee * 2.0)  # Полный цикл комиссий: вход + выход обеих ног
        
        net_yield = (long_realized_pnl * long_executed_volume_rate) + (short_realized_pnl * short_executed_volume_rate) - total_comm
        
        vwap_spread_out = (short_vwap_ask - long_vwap_bid) / long_vwap_bid
        is_exit = net_yield >= target_val
        reason = "TTL_EXPIRED" if is_ttl else "PROFIT_DECAY"
        
        return is_exit, {
            "net_yield": net_yield,
            "target_val": reported_target,
            "vwap_spread_out": vwap_spread_out,
            "long_close_price": long_vwap_bid,
            "short_close_price": short_vwap_ask,
            "reason": reason,
            "exit_level_index": exit_level_index
        }

    def evaluate_execution(self,
                           long_book: Dict[str, Any],
                           short_book: Dict[str, Any],
                           long_ex: str,
                           short_ex: str,
                           expected_res: Dict[str, Any]) -> Dict[str, Any]:
                           
        long_vol = self._get_vol_discount_entry(long_ex)
        short_vol = self._get_vol_discount_entry(short_ex)
        
        long_qty = expected_res["long_qty"]
        short_qty = expected_res["short_qty"]
        
        expected_long = expected_res["long_avg_price"]
        expected_short = expected_res["short_avg_price"]
        
        actual_long_price = OrderbookUtils.calculate_vwap_by_qty(
            long_book.get("asks", []), long_qty, volatility_discount=long_vol)
            
        actual_short_price = OrderbookUtils.calculate_vwap_by_qty(
            short_book.get("bids", []), short_qty, volatility_discount=short_vol)
        
        actual_long_qty = long_qty if actual_long_price > 0 else 0.0
        actual_short_qty = short_qty if actual_short_price > 0 else 0.0
        
        long_executed_volume_rate = actual_long_qty / long_qty if long_qty > 0 else 0.0
        short_executed_volume_rate = actual_short_qty / short_qty if short_qty > 0 else 0.0
        
        if actual_long_price > 0 and actual_short_price > 0:
            long_slip = (actual_long_price - expected_long) / expected_long
            short_slip = (expected_short - actual_short_price) / expected_short
            real_slippage = long_slip + short_slip
            actual_spread = (actual_short_price - actual_long_price) / actual_long_price
        else:
            actual_spread = expected_res["vwap_spread"]
            real_slippage = 0.0
            actual_long_price = expected_res["long_avg_price"]
            actual_short_price = expected_res["short_avg_price"]
                
        return {
            "actual_long_price": actual_long_price,
            "actual_short_price": actual_short_price,
            "actual_spread": actual_spread,
            "real_slippage": real_slippage,
            "long_executed_volume_rate": long_executed_volume_rate,
            "short_executed_volume_rate": short_executed_volume_rate
        }

    def get_exit_target_val(self, duration_sec: float) -> Tuple[float, int]:
        target = self.decay_map[0]["target_val"]
        idx = int(self.decay_map[0]["index"])
        for rule in self.decay_map:
            if duration_sec >= float(rule["seconds"]):
                target = float(rule["target_val"])
                idx = int(rule["index"])
        return target, idx
