# ============================================================
# FILE: CORE/trading_engine.py
# ROLE: Оценка входа и выхода из позиций.
# ============================================================

from typing import Tuple, Dict, Any, Optional
from CORE.math_core import OrderbookUtils

class TradingEngine:
    def __init__(self, cfg: dict, exchanges: dict):
        """
        cfg: весь json-конфиг
        exchanges: маппинг {0: "BINANCE", 1: "KUCOIN", ...} для перевода индексов в имена
        """
        self.cfg = cfg
        self.exchanges = exchanges
        
        # Читаем параметры фильтра сигналов из signal_filters (с fallback на flat entry)
        signal_cfg = self.cfg["trading_rules"]["entry"].get("signal_filters", self.cfg["trading_rules"]["entry"])
        self.spread_entry = float(signal_cfg["spread_entry"])
        synth_cfg = signal_cfg.get("synthetic_exit", {})
        self.check_synthetic_exit = bool(synth_cfg.get("enabled", True))
        self.check_synthetic_slippage = bool(synth_cfg.get("check_slippage", True))
        self.max_slippage_ratio = float(synth_cfg.get("max_slippage_ratio", 0.65))
        self.hard_max_slippage = float(synth_cfg.get("hard_max_slippage", 0.008))
        
        obi_cfg = signal_cfg.get("orderbook_imbalance", {})
        self.check_obi_filter = bool(obi_cfg.get("enabled", True))
        self.max_adverse_imbalance = float(obi_cfg.get("max_adverse_imbalance", 0.55))
        self.obi_levels = int(obi_cfg.get("depth_levels", 5))
        
        self.min_top_depth_usd = float(signal_cfg.get("min_top_depth_usd", 0.0))
        hedged_exit = self.cfg["trading_rules"]["exit"]["hedged_exit"]
        self.decay_map = hedged_exit["normal_decay"]
        self.extreme_decay_map = hedged_exit.get("weak_entry", {}).get("decay", [
            {"step": 0, "after_sec": 0, "target_spread": 0.0000},
            {"step": 1, "after_sec": 60, "target_spread": -999.0}
        ])
        self.trading_risks = self.cfg["trading_risks"]

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
                       long_ask_offset: int = 0,
                       short_bid_offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        
        long_idx = int(cand[0])
        short_idx = int(cand[1])
        long_ex = self.exchanges[long_idx]
        short_ex = self.exchanges[short_idx]
        
        # Если смещения не переданы, определяем первые квалифицированные уровни (мусор перед ними отсекается)
        if long_ask_offset <= 0 and self.min_top_depth_usd > 0.0:
            idx, _, _ = OrderbookUtils.find_first_qualified_level(
                long_book.get("asks", []), self.min_top_depth_usd, is_ask=True
            )
            if idx < 0:
                return False, {"reason": "NO_QUALIFIED_ASK_DEPTH"}
            long_ask_offset = idx

        if short_bid_offset <= 0 and self.min_top_depth_usd > 0.0:
            idx, _, _ = OrderbookUtils.find_first_qualified_level(
                short_book.get("bids", []), self.min_top_depth_usd, is_ask=False
            )
            if idx < 0:
                return False, {"reason": "NO_QUALIFIED_BID_DEPTH"}
            short_bid_offset = idx

        long_vol = self._get_vol_discount_entry(long_ex)
        short_vol = self._get_vol_discount_entry(short_ex)
        
        # Срез стакана строго от первого квалифицированного уровня с объемом >= min_top_depth_usd
        asks_slice = long_book["asks"][long_ask_offset:] if long_ask_offset > 0 else long_book.get("asks", [])
        bids_slice = short_book["bids"][short_bid_offset:] if short_bid_offset > 0 else short_book.get("bids", [])
        
        # Для лонга - покупаем из асков. Для шорта - продаем в биды.
        long_vwap_ask = OrderbookUtils.calculate_vwap_by_usd(asks_slice, size_usd, long_vol)
        short_vwap_bid = OrderbookUtils.calculate_vwap_by_usd(bids_slice, size_usd, short_vol)
        
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
            
        # ФИЛЬТР ДАВЛЕНИЯ СТАКАНА HEDGE НОГИ (Order Book Imbalance)
        if self.check_obi_filter:
            route_key = f"{long_ex}_{short_ex}"
            rev_route_key = f"{short_ex}_{long_ex}"
            role_info = self.exchange_roles.get(route_key) or self.exchange_roles.get(rev_route_key)
            if role_info:
                hedge_name = role_info.get("hedge")
                hedge_book = long_book if long_ex == hedge_name else (short_book if short_ex == hedge_name else None)
                if hedge_book:
                    h_bids = hedge_book.get("bids", [])[:self.obi_levels]
                    h_asks = hedge_book.get("asks", [])[:self.obi_levels]
                    sum_bids = sum(float(b[1]) for b in h_bids) if h_bids else 0.0
                    sum_asks = sum(float(a[1]) for a in h_asks) if h_asks else 0.0
                    total_vol = sum_bids + sum_asks
                    if total_vol > 0.0:
                        imbalance = (sum_bids - sum_asks) / total_vol
                        # Если Hedge идет в Long (покупка из asks), перекос в bids (давление вверх) неблагоприятен
                        if long_ex == hedge_name and imbalance > self.max_adverse_imbalance:
                            return False, {"reason": f"ADVERSE_OBI_HEDGE_BUY (Imbalance: {imbalance:+.2f} > +{self.max_adverse_imbalance:.2f})"}
                        # Если Hedge идет в Short (продажа в bids), перекос в asks (давление вниз) неблагоприятен
                        if short_ex == hedge_name and imbalance < -self.max_adverse_imbalance:
                            return False, {"reason": f"ADVERSE_OBI_HEDGE_SELL (Imbalance: {imbalance:+.2f} < -{self.max_adverse_imbalance:.2f})"}
            
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
            "short_ex": short_ex,
            "long_ask_offset": long_ask_offset,
            "short_bid_offset": short_bid_offset
        }

    def evaluate_hedge_entry(
        self,
        hedge_book: Optional[Dict[str, Any]],
        lead_direction: str,  # "BUY" (Lead Long) or "SELL" (Lead Short)
        lead_price: float,
        lead_qty: float,
        target_net_spread: float,
        lead_ex: str,
        hedge_ex: str,
        live_price_fallback: float = 0.0
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Оценка возможности входа/дожима Hedge-ноги с зафиксированной ценой Lead-ноги.
        Использует ровно ту же формулу, что и evaluate_entry:
            vwap_spread = (short_price - long_price) / long_price
            net_spread = vwap_spread - (lead_fee + hedge_fee)
        
        lead_direction: 'BUY' (Lead открыл Long) или 'SELL' (Lead открыл Short).
        """
        lead_fee = self._get_fee(lead_ex)
        hedge_fee = self._get_fee(hedge_ex)
        total_fees = lead_fee + hedge_fee
        hedge_vol = self._get_vol_discount_entry(hedge_ex)

        if lead_direction == "BUY":
            # Lead встал в Long -> Hedge обязан встать в Short (Sell по bids)
            bids = hedge_book.get("bids", []) if isinstance(hedge_book, dict) else []
            if bids:
                vwap_bid, deep_price = OrderbookUtils.calculate_vwap_and_deepest_price(bids, lead_qty, hedge_vol)
            elif live_price_fallback > 0.0:
                vwap_bid = live_price_fallback
                deep_price = live_price_fallback
            else:
                return False, {"reason": "EMPTY_HEDGE_BIDS"}

            if vwap_bid <= 0.0:
                return False, {"reason": "INSUFFICIENT_HEDGE_LIQUIDITY"}

            gross_spread = (vwap_bid - lead_price) / lead_price
            net_spread = gross_spread - total_fees

            # Порог цены: ниже продавать нельзя, иначе чистый спред упадет ниже target_net_spread
            limit_floor = lead_price * (1.0 + target_net_spread + total_fees)
            order_price = limit_floor

            is_valid = (net_spread >= target_net_spread) and (vwap_bid >= limit_floor)

            return is_valid, {
                "side": "SELL",
                "vwap_price": vwap_bid,
                "deep_price": deep_price,
                "limit_floor": limit_floor,
                "order_price": order_price,
                "net_spread": net_spread,
                "gross_spread": gross_spread,
                "target_net_spread": target_net_spread,
                "total_fees": total_fees,
                "reason": "OK" if is_valid else f"NET_SPREAD_LOW (Net:{net_spread*100:+.3f}% < Target:{target_net_spread*100:+.3f}%, VWAP:{vwap_bid:.6f} < Floor:{limit_floor:.6f})"
            }

        else:
            # Lead встал в Short -> Hedge обязан встать в Long (Buy по asks)
            asks = hedge_book.get("asks", []) if isinstance(hedge_book, dict) else []
            if asks:
                vwap_ask, deep_price = OrderbookUtils.calculate_vwap_and_deepest_price(asks, lead_qty, hedge_vol)
            elif live_price_fallback > 0.0:
                vwap_ask = live_price_fallback
                deep_price = live_price_fallback
            else:
                return False, {"reason": "EMPTY_HEDGE_ASKS"}

            if vwap_ask <= 0.0:
                return False, {"reason": "INSUFFICIENT_HEDGE_LIQUIDITY"}

            gross_spread = (lead_price - vwap_ask) / vwap_ask
            net_spread = gross_spread - total_fees

            # Порог цены: выше покупать нельзя, иначе чистый спред упадет ниже target_net_spread
            limit_ceiling = lead_price / (1.0 + target_net_spread + total_fees)
            order_price = limit_ceiling

            is_valid = (net_spread >= target_net_spread) and (vwap_ask <= limit_ceiling)

            return is_valid, {
                "side": "BUY",
                "vwap_price": vwap_ask,
                "deep_price": deep_price,
                "limit_ceiling": limit_ceiling,
                "order_price": order_price,
                "net_spread": net_spread,
                "gross_spread": gross_spread,
                "target_net_spread": target_net_spread,
                "total_fees": total_fees,
                "reason": "OK" if is_valid else f"NET_SPREAD_LOW (Net:{net_spread*100:+.3f}% < Target:{target_net_spread*100:+.3f}%, VWAP:{vwap_ask:.6f} > Ceiling:{limit_ceiling:.6f})"
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
                      short_executed_volume_rate: float = 1.0,
                      decay_map: list = None,
                      actual_net_spread_entry: float = 0.0) -> Tuple[bool, Dict[str, Any]]:
        
        target_val, exit_level_index = self.get_exit_target_val(duration_sec, actual_net_spread_entry, decay_map=decay_map)
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

    def get_exit_target_val(self, duration_sec: float, actual_net_spread_entry: float = 0.0, decay_map: list = None) -> Tuple[float, int]:
        m = decay_map if decay_map is not None else self.decay_map
        if "target_spread" in m[0]:
            # Абсолютная карта (weak_entry / single_leg): target_spread — порог спреда
            target = m[0]["target_spread"]
            idx = int(m[0].get("step", 0))
            for rule in m:
                if duration_sec >= float(rule["after_sec"]):
                    target = float(rule["target_spread"])
                    idx = int(rule.get("step", 0))
        elif "price_slip" in m[0]:
            # Чейзинг-карта (single_leg_exit): price_slip — проскальзывание от цены
            target = m[0]["price_slip"]
            idx = int(m[0].get("step", 0))
            for rule in m:
                if duration_sec >= float(rule["after_sec"]):
                    target = float(rule["price_slip"])
                    idx = int(rule.get("step", 0))
        else:
            # Относительная карта (normal_decay): min_profit_ratio — доля от входного спреда
            ratio = m[0].get("min_profit_ratio", 0.0)
            idx = int(m[0].get("step", 0))
            for rule in m:
                if duration_sec >= float(rule["after_sec"]):
                    ratio = float(rule.get("min_profit_ratio", 0.0))
                    idx = int(rule.get("step", 0))
            if ratio <= -900.0:
                target = -999.0
            else:
                target = actual_net_spread_entry * ratio
        return target, idx
