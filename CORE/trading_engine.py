# ============================================================
# FILE: CORE/trading_engine.py
# ROLE: Evaluation of entry and exit conditions for positions.
# ============================================================

from typing import Tuple, Dict, Any, Optional
from CORE.math_core import OrderbookUtils, StaticDetector, ImpulseDetector

class TradingEngine:
    def __init__(self, cfg: dict, exchanges: dict):
        """
        cfg: full json config
        exchanges: mapping {0: "BINANCE", 1: "KUCOIN", ...} translating indices to names
        """
        self.cfg = cfg
        self.exchanges = exchanges
        
        if "static_detector" in self.cfg["trading_rules"]["entry"]:
            self.static_detector = StaticDetector(cfg)
        elif "impulse_detector" in self.cfg["trading_rules"]["entry"]:
            self.static_detector = StaticDetector(cfg)
        else:
            self.static_detector = StaticDetector({
                "trading_rules": {
                    "entry": {
                        "static_detector": {
                            "enabled": False,
                            "static_leg": "TARGET",
                            "max_static_leg_pct": 0.0020,
                            "buffer_window_sec": 0.25
                        }
                    }
                }
            })
        self.impulse = self.static_detector
        
        entry_rules = self.cfg["trading_rules"]["entry"]
        signal_cfg = entry_rules["signal_filters"]
        
        # Support [min, max] list format, separate spread_entry_pre / spread_entry_max, and null values
        spread_val = signal_cfg.get("spread_entry_pre")
        if spread_val is None:
            # Fallback for old configs
            spread_val = signal_cfg.get("spread_entry")

        if isinstance(spread_val, (list, tuple)):
            self.spread_entry_pre_min = float(spread_val[0]) if len(spread_val) > 0 and spread_val[0] is not None else None
            self.spread_entry_max = float(spread_val[1]) if len(spread_val) > 1 and spread_val[1] is not None else None
        else:
            self.spread_entry_pre_min = float(spread_val) if spread_val is not None else None
            max_val = signal_cfg["spread_entry_max"] if "spread_entry_max" in signal_cfg else None
            self.spread_entry_max = float(max_val) if max_val is not None else None
            
        self.spread_entry = self.spread_entry_pre_min if self.spread_entry_pre_min is not None else 0.0
        self.spread_entry_min = self.spread_entry_pre_min
        
        # Load spread_entry_base, default to spread_entry_pre_min if not present
        if "spread_entry_base" in signal_cfg:
            self.spread_entry_base = float(signal_cfg["spread_entry_base"])
        else:
            self.spread_entry_base = self.spread_entry_pre_min
            
        self.min_top_depth_usd = float(signal_cfg["min_top_depth_usd"])
        
        # v9: exchange roles
        self.exchange_roles = self.cfg["exchange_roles"] if "exchange_roles" in self.cfg else {}
        
        # v9: target exit params (TTL is derived directly from decay_map)
        target_exit_cfg = self.cfg["trading_rules"]["exit"]["target_exit"]
        
        # v9: stop loss can be null / <= 0 (disabled)
        stop_loss_val = target_exit_cfg["stop_loss_pct"] if "stop_loss_pct" in target_exit_cfg else None
        if stop_loss_val is not None and not isinstance(stop_loss_val, bool):
            try:
                val = float(stop_loss_val)
                if val > 0:
                    # Защита от ввода в процентах: если указано >= 0.05 (например 0.5 вместо 0.005)
                    if val >= 0.05:
                        log(f"⚠️ [CONFIG WARNING] stop_loss_pct задан как {val} (>= 5%). "
                            f"Автоматически нормализовано: {val}% -> {val / 100.0:.4f}", level="WARNING")
                        val = val / 100.0
                    self.stop_loss_pct = val
                else:
                    self.stop_loss_pct = None
            except (ValueError, TypeError):
                self.stop_loss_pct = None
        else:
            self.stop_loss_pct = None
        
        self.decay_map = target_exit_cfg["decay_map"]
        derived_ttl = None
        for rule in self.decay_map:
            ratio = rule["min_profit_ratio"] if "min_profit_ratio" in rule else None
            spread = rule["target_spread"] if "target_spread" in rule else None
            if (ratio is None and spread is None) or (isinstance(ratio, (int, float)) and ratio <= -900.0):
                derived_ttl = float(rule["after_sec"])
                break
        if derived_ttl is not None:
            self.ttl_sec = derived_ttl
        elif "ttl_sec" in target_exit_cfg and target_exit_cfg["ttl_sec"] is not None:
            self.ttl_sec = float(target_exit_cfg["ttl_sec"])
        elif self.decay_map:
            self.ttl_sec = float(self.decay_map[-1]["after_sec"])
        else:
            self.ttl_sec = 60.0
            
        # v9: emergency decay params for deflated/evaporated spread
        self.min_spread_entry = float(target_exit_cfg["min_spread_entry"]) if "min_spread_entry" in target_exit_cfg else 0.0030
        self.emergency_decay_map = target_exit_cfg["emergency_decay_map"] if "emergency_decay_map" in target_exit_cfg else [
            {"step": 0, "after_sec": 0, "min_profit_ratio": 0.0},
            {"step": 1, "after_sec": 3, "min_profit_ratio": -999.0}
        ]
        derived_emergency_ttl = None
        for rule in self.emergency_decay_map:
            ratio = rule.get("min_profit_ratio")
            spread = rule.get("target_spread")
            if (ratio is None and spread is None) or (isinstance(ratio, (int, float)) and ratio <= -900.0):
                derived_emergency_ttl = float(rule["after_sec"])
                break
        self.emergency_ttl_sec = derived_emergency_ttl if derived_emergency_ttl is not None else 3.0
        
        # Backward compatibility for old configs
        if "synthetic_exit" in signal_cfg:
            synth_cfg = signal_cfg["synthetic_exit"]
            self.check_synthetic_exit = bool(synth_cfg["enabled"])
            self.check_synthetic_slippage = bool(synth_cfg["check_slippage"])
            self.max_slippage_ratio = float(synth_cfg["max_slippage_ratio"])
            self.hard_max_slippage = float(synth_cfg["hard_max_slippage"])
        else:
            self.check_synthetic_exit = False
            self.check_synthetic_slippage = False
            self.max_slippage_ratio = 0.5
            self.hard_max_slippage = 0.008
            
        if "orderbook_imbalance" in signal_cfg:
            obi_cfg = signal_cfg["orderbook_imbalance"]
            self.check_obi_filter = bool(obi_cfg["enabled"])
            self.max_adverse_imbalance = float(obi_cfg["max_adverse_imbalance"])
            self.obi_levels = int(obi_cfg["depth_levels"])
        else:
            self.check_obi_filter = False
            self.max_adverse_imbalance = 0.55
            self.obi_levels = 5
        
        self.trading_risks = self.cfg["trading_risks"]

    def _get_vol_discount_entry(self, exchange_name: str) -> float:
        return float(self.trading_risks[exchange_name.lower()]["volatility_discount_entry"])

    def _get_vol_discount_exit(self, exchange_name: str) -> float:
        return float(self.trading_risks[exchange_name.lower()]["volatility_discount_exit"])

    def _get_fee(self, exchange_name: str) -> float:
        return float(self.trading_risks[exchange_name.lower()]["taker_fee"])

    def update_market_data(self, sym: str, ex: str, book: dict, ts_mono: float):
        """Called on every incoming websocket tick to maintain static detector state."""
        if not self.static_detector.is_enabled:
            return
            
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if bids and asks:
            mid_p = StaticDetector.calc_top3_mid_price(bids, asks)
            self.static_detector.update(sym, ex, mid_p, ts_mono)

    def evaluate_entry(
        self, 
        long_book: Dict[str, Any], 
        short_book: Dict[str, Any], 
        cand: list,
        size_usd: float,
        long_ask_offset: int = 0,
        short_bid_offset: int = 0
    ) -> Tuple[bool, Dict[str, Any]]:
        long_idx = int(cand[0])
        short_idx = int(cand[1])
        long_ex = self.exchanges[long_idx]
        short_ex = self.exchanges[short_idx]
        
        # If offsets not provided, find first qualified levels (filtering front junk)
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
        
        # Order book slice strictly from first qualified level with volume >= min_top_depth_usd
        asks_slice = long_book["asks"][long_ask_offset:] if long_ask_offset > 0 else long_book.get("asks", [])
        bids_slice = short_book["bids"][short_bid_offset:] if short_bid_offset > 0 else short_book.get("bids", [])
        
        # For Long - buy from asks. For Short - sell into bids.
        long_vwap_ask = OrderbookUtils.calculate_vwap_by_usd(asks_slice, size_usd, long_vol)
        short_vwap_bid = OrderbookUtils.calculate_vwap_by_usd(bids_slice, size_usd, short_vol)
        
        if long_vwap_ask <= 0 or short_vwap_bid <= 0:
            return False, {"reason": "INSUFFICIENT_VOLUME"}
            
        long_qty = size_usd / long_vwap_ask
        short_qty = size_usd / short_vwap_bid
        
        vwap_spread = (short_vwap_bid - long_vwap_ask) / short_vwap_bid
        
        # Account for own entry commission
        entry_long_fee = self._get_fee(long_ex)
        entry_short_fee = self._get_fee(short_ex)
        entry_comm = entry_long_fee + entry_short_fee
        net_spread = vwap_spread - entry_comm
        
        target_spread = self.spread_entry_base if self.spread_entry_base is not None else self.spread_entry
        if target_spread is not None and net_spread < target_spread:
            return False, {
                "reason": f"LOW_SPREAD (Net: {net_spread * 100:.3f}% < {target_spread * 100:.3f}%, Gross: {vwap_spread * 100:.3f}%, Fee: {entry_comm * 100:.3f}%)"
            }
            
        # ORDERBOOK IMBALANCE FILTER (OBI) - Evaluate on BOTH legs
        if self.check_obi_filter:
            l_bids = long_book.get("bids", [])[:self.obi_levels]
            l_asks = long_book.get("asks", [])[:self.obi_levels]
            sum_l_bids = sum(float(b[1]) for b in l_bids) if l_bids else 0.0
            sum_l_asks = sum(float(a[1]) for a in l_asks) if l_asks else 0.0
            if sum_l_bids + sum_l_asks > 0.0:
                l_imbalance = (sum_l_bids - sum_l_asks) / (sum_l_bids + sum_l_asks)
                if l_imbalance < -self.max_adverse_imbalance:
                    return False, {"reason": f"ADVERSE_OBI_LONG (Ask skew: {l_imbalance:+.2f} < -{self.max_adverse_imbalance:.2f})"}

            s_bids = short_book.get("bids", [])[:self.obi_levels]
            s_asks = short_book.get("asks", [])[:self.obi_levels]
            sum_s_bids = sum(float(b[1]) for b in s_bids) if s_bids else 0.0
            sum_s_asks = sum(float(a[1]) for a in s_asks) if s_asks else 0.0
            if sum_s_bids + sum_s_asks > 0.0:
                s_imbalance = (sum_s_bids - sum_s_asks) / (sum_s_bids + sum_s_asks)
                if s_imbalance > self.max_adverse_imbalance:
                    return False, {"reason": f"ADVERSE_OBI_SHORT (Bid skew: {s_imbalance:+.2f} > +{self.max_adverse_imbalance:.2f})"}
            
        # ROUND-TRIP SYNTHETIC LIQUIDITY CHECK
        if self.check_synthetic_exit:
            long_vol_exit = self._get_vol_discount_exit(long_ex)
            short_vol_exit = self._get_vol_discount_exit(short_ex)
            long_exit_vwap_bid = OrderbookUtils.calculate_vwap_by_qty(long_book.get("bids", []), long_qty, long_vol_exit)
            short_exit_vwap_ask = OrderbookUtils.calculate_vwap_by_qty(short_book.get("asks", []), short_qty, short_vol_exit)
            
            if long_exit_vwap_bid <= 0 or short_exit_vwap_ask <= 0:
                return False, {"reason": "NO_REVERSE_LIQUIDITY"}
                
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

    def evaluate_entry_v9(
        self,
        sym: str,
        oracle_book: dict,
        target_book: dict,
        oracle_ex: str,
        target_ex: str,
        size_usd: float
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        v9: Одноногий арбитраж на Мишени с фильтром стоячей ноги (StaticDetector).
        """
        # 1. Проверка покоя стоячей ноги
        static_ex = target_ex if self.static_detector.static_leg == "TARGET" else oracle_ex
        static_book = target_book if static_ex == target_ex else oracle_book
        bids = static_book.get("bids", [])
        asks = static_book.get("asks", [])
        if not bids or not asks:
            return False, {"reason": "EMPTY_BOOK"}
            
        curr_mid = StaticDetector.calc_top3_mid_price(bids, asks)
        is_st, reason, dev = self.static_detector.is_leg_static(sym, static_ex, curr_mid)
        if not is_st:
            return False, {"reason": reason}
            
        # 2. Оценка спреда и исполнения через глубокий evaluate_entry
        ex_to_idx = {v: k for k, v in self.exchanges.items()}
        oracle_idx = ex_to_idx.get(oracle_ex, 0)
        target_idx = ex_to_idx.get(target_ex, 3)
        
        # Направление 1: LONG Target, SHORT Oracle
        cand_long = [target_idx, oracle_idx, 0.0, 0.0, 0.0]
        ok_long, res_long = self.evaluate_entry(target_book, oracle_book, cand_long, size_usd)
        
        # Направление 2: SHORT Target, LONG Oracle
        cand_short = [oracle_idx, target_idx, 0.0, 0.0, 0.0]
        ok_short, res_short = self.evaluate_entry(oracle_book, target_book, cand_short, size_usd)
        
        if ok_long and (not ok_short or res_long["net_spread"] >= res_short["net_spread"]):
            entry_price = res_long["long_avg_price"]
            qty = res_long["long_qty"]
            net_spread = res_long["net_spread"]
            return True, {
                "side": "LONG",
                "target_ex": target_ex,
                "oracle_ex": oracle_ex,
                "entry_price": entry_price,
                "qty": qty,
                "raw_spread": res_long["vwap_spread"],
                "net_spread": net_spread,
                "target_fee": self._get_fee(target_ex),
                "details": f"v9 LONG Target:{target_ex} | Net:{net_spread*100:+.3f}% | {res_long['details']}"
            }
        elif ok_short:
            entry_price = res_short["short_avg_price"]
            qty = res_short["short_qty"]
            net_spread = res_short["net_spread"]
            return True, {
                "side": "SHORT",
                "target_ex": target_ex,
                "oracle_ex": oracle_ex,
                "entry_price": entry_price,
                "qty": qty,
                "raw_spread": res_short["vwap_spread"],
                "net_spread": net_spread,
                "target_fee": self._get_fee(target_ex),
                "details": f"v9 SHORT Target:{target_ex} | Net:{net_spread*100:+.3f}% | {res_short['details']}"
            }
        else:
            reason = res_long.get("reason") if res_long else (res_short.get("reason") if res_short else "NO_SPREAD")
            return False, {"reason": reason}

    def evaluate_exit_v9(
        self,
        target_book: dict,
        target_ex: str,
        entry_price: float,
        qty: float,
        side: str,
        duration_sec: float,
        actual_net_spread_entry: float,
        oracle_book: Optional[dict] = None,
        oracle_ex: Optional[str] = None,
        is_emergency: bool = False,
        emergency_duration_sec: Optional[float] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        v9: Выход по локальному профиту/стопу/TTL на Мишени с контролем остаточного спреда к Оракулу.
        Если спред относительно Оракула сдулся ниже min_spread_entry -> переход на emergency_decay_map.
        """
        target_fee = self._get_fee(target_ex)
        
        # 1. Расчет реального текущего спреда относительно живого стакана Oracle
        current_oracle_net_spread = None
        spread_evaporated = False
        
        if oracle_book:
            o_bids = oracle_book.get("bids", [])
            o_asks = oracle_book.get("asks", [])
            if o_bids and o_asks:
                if side == "LONG":
                    oracle_p = float(o_bids[0][0])
                    gross_spread = (oracle_p - entry_price) / oracle_p if oracle_p > 0 else 0.0
                else:
                    oracle_p = float(o_asks[0][0])
                    gross_spread = (entry_price - oracle_p) / entry_price if entry_price > 0 else 0.0
                
                # Чистый остаточный спред с учетом taker fee мишени (вход + выход)
                current_oracle_net_spread = gross_spread - (target_fee * 2.0)
                if current_oracle_net_spread < self.min_spread_entry:
                    spread_evaporated = True

        use_emergency = is_emergency or spread_evaporated
        active_decay_map = self.emergency_decay_map if use_emergency else self.decay_map
        active_ttl = self.emergency_ttl_sec if use_emergency else self.ttl_sec
        active_duration = emergency_duration_sec if (use_emergency and emergency_duration_sec is not None) else duration_sec

        target_val, exit_level_index = self.get_exit_target_val(
            active_duration, actual_net_spread_entry, decay_map=active_decay_map
        )
        is_ttl = target_val <= -999.0
        reported_target = None if is_ttl else target_val
        
        target_vol = self._get_vol_discount_exit(target_ex)
        
        exit_price = 0.0
        if target_book:
            if side == "LONG":
                exit_price = OrderbookUtils.calculate_vwap_by_qty(
                    target_book.get("bids", []), qty, target_vol
                )
            else:
                exit_price = OrderbookUtils.calculate_vwap_by_qty(
                    target_book.get("asks", []), qty, target_vol
                )
        
        if exit_price > 0 and entry_price > 0:
            if side == "LONG":
                gross_pnl_pct = (exit_price - entry_price) / entry_price
            else:
                gross_pnl_pct = (entry_price - exit_price) / entry_price
            net_pnl_pct = gross_pnl_pct - (target_fee * 2.0)
        else:
            gross_pnl_pct = None
            net_pnl_pct = None
            exit_price = None

        # TTL check (unconditional market exit)
        if active_duration >= active_ttl or is_ttl:
            reason = "EMERGENCY_TTL" if use_emergency else "TTL_EXPIRED"
            return True, {
                "reason": reason,
                "net_pnl_pct": net_pnl_pct,
                "gross_pnl_pct": gross_pnl_pct,
                "exit_price": exit_price,
                "entry_price": entry_price,
                "duration_sec": duration_sec,
                "target_val": reported_target,
                "exit_level_index": exit_level_index,
                "use_emergency_decay": use_emergency,
                "oracle_net_spread": current_oracle_net_spread
            }
        
        if exit_price is None or exit_price <= 0:
            return False, {
                "reason": "NO_EXIT_LIQUIDITY", 
                "net_pnl_pct": None, 
                "gross_pnl_pct": None,
                "exit_price": None, 
                "entry_price": entry_price,
                "duration_sec": duration_sec,
                "target_val": reported_target,
                "exit_level_index": exit_level_index,
                "use_emergency_decay": use_emergency,
                "oracle_net_spread": current_oracle_net_spread
            }
        
        result = {
            "net_pnl_pct": net_pnl_pct,
            "gross_pnl_pct": gross_pnl_pct,
            "exit_price": exit_price,
            "entry_price": entry_price,
            "duration_sec": duration_sec,
            "target_val": reported_target,
            "exit_level_index": exit_level_index,
            "use_emergency_decay": use_emergency,
            "oracle_net_spread": current_oracle_net_spread
        }
        
        # Stop-Loss
        if self.stop_loss_pct is not None and net_pnl_pct <= -self.stop_loss_pct:
            result["reason"] = "STOP_LOSS"
            return True, result
        
        # Take-Profit / Emergency Breakeven
        if net_pnl_pct >= target_val:
            reason = "EMERGENCY_BREAKEVEN" if (use_emergency and target_val <= 0.0) else "TAKE_PROFIT"
            result["reason"] = reason
            return True, result
        
        result["reason"] = "HOLD"
        return False, result

    def get_exit_target_val(self, duration_sec: float, actual_net_spread_entry: float = 0.0, decay_map: list = None) -> Tuple[float, int]:
        m = decay_map if decay_map is not None else self.decay_map
        if not m:
            return -999.0, 0
        if actual_net_spread_entry is None:
            actual_net_spread_entry = 0.0
        if "target_spread" in m[0]:
            first_ts = m[0]["target_spread"] if "target_spread" in m[0] else None
            target = float(first_ts) if first_ts is not None else -999.0
            idx = int(m[0]["step"]) if "step" in m[0] else 0
            for rule in m:
                if duration_sec >= float(rule["after_sec"]):
                    val = rule["target_spread"] if "target_spread" in rule else None
                    target = float(val) if val is not None else -999.0
                    idx = int(rule["step"]) if "step" in rule else idx
        elif "price_slip" in m[0]:
            first_ps = m[0]["price_slip"] if "price_slip" in m[0] else None
            target = float(first_ps) if first_ps is not None else -999.0
            idx = int(m[0]["step"]) if "step" in m[0] else 0
            for rule in m:
                if duration_sec >= float(rule["after_sec"]):
                    val = rule["price_slip"] if "price_slip" in rule else None
                    target = float(val) if val is not None else -999.0
                    idx = int(rule["step"]) if "step" in rule else idx
        else:
            first_r = m[0]["min_profit_ratio"] if "min_profit_ratio" in m[0] else None
            ratio = float(first_r) if first_r is not None else None
            idx = int(m[0]["step"]) if "step" in m[0] else 0
            for rule in m:
                if duration_sec >= float(rule["after_sec"]):
                    r_val = rule["min_profit_ratio"] if "min_profit_ratio" in rule else None
                    ratio = float(r_val) if r_val is not None else None
                    idx = int(rule["step"]) if "step" in rule else idx
            if ratio is None or (isinstance(ratio, (int, float)) and ratio <= -900.0):
                target = -999.0
            else:
                target = actual_net_spread_entry * ratio
        return target, idx

    # =========================================================================
    # DEPRECATED v8 METHODS (Kept for backward compatibility with older tests)
    # =========================================================================

    def evaluate_entry(self, 
                       long_book: Dict[str, Any], 
                       short_book: Dict[str, Any], 
                       cand: list,
                       size_usd: float,
                       long_ask_offset: int = 0,
                       short_bid_offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """DEPRECATED (v8 hedged mode). Use evaluate_entry_v9() for single-leg."""
        long_idx = int(cand[0])
        short_idx = int(cand[1])
        long_ex = self.exchanges[long_idx]
        short_ex = self.exchanges[short_idx]
        
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
        
        asks_slice = long_book["asks"][long_ask_offset:] if long_ask_offset > 0 else long_book.get("asks", [])
        bids_slice = short_book["bids"][short_bid_offset:] if short_bid_offset > 0 else short_book.get("bids", [])
        
        long_vwap_ask = OrderbookUtils.calculate_vwap_by_usd(asks_slice, size_usd, long_vol)
        short_vwap_bid = OrderbookUtils.calculate_vwap_by_usd(bids_slice, size_usd, short_vol)
        
        if long_vwap_ask <= 0 or short_vwap_bid <= 0:
            return False, {"reason": "INSUFFICIENT_VOLUME"}
            
        long_qty = size_usd / long_vwap_ask
        short_qty = size_usd / short_vwap_bid
        vwap_spread = (short_vwap_bid - long_vwap_ask) / short_vwap_bid
        
        entry_long_fee = self._get_fee(long_ex)
        entry_short_fee = self._get_fee(short_ex)
        entry_comm = entry_long_fee + entry_short_fee
        net_spread = vwap_spread - entry_comm
        
        if self.spread_entry_min is not None and net_spread < self.spread_entry_min:
            return False, {"reason": f"LOW_SPREAD (Net: {net_spread * 100:.3f}% < {self.spread_entry_min * 100:.3f}%)"}
            
        if self.spread_entry_max is not None and net_spread > self.spread_entry_max:
            return False, {"reason": f"HIGH_SPREAD (Net: {net_spread * 100:.3f}% > Max: {self.spread_entry_max * 100:.3f}%)"}
            
        if self.check_obi_filter:
            l_bids = long_book.get("bids", [])[:self.obi_levels]
            l_asks = long_book.get("asks", [])[:self.obi_levels]
            sum_l_bids = sum(float(b[1]) for b in l_bids) if l_bids else 0.0
            sum_l_asks = sum(float(a[1]) for a in l_asks) if l_asks else 0.0
            if sum_l_bids + sum_l_asks > 0.0:
                l_imbalance = (sum_l_bids - sum_l_asks) / (sum_l_bids + sum_l_asks)
                if l_imbalance < -self.max_adverse_imbalance:
                    return False, {"reason": f"ADVERSE_OBI_LONG (Ask skew: {l_imbalance:+.2f} < -{self.max_adverse_imbalance:.2f})"}

            s_bids = short_book.get("bids", [])[:self.obi_levels]
            s_asks = short_book.get("asks", [])[:self.obi_levels]
            sum_s_bids = sum(float(b[1]) for b in s_bids) if s_bids else 0.0
            sum_s_asks = sum(float(a[1]) for a in s_asks) if s_asks else 0.0
            if sum_s_bids + sum_s_asks > 0.0:
                s_imbalance = (sum_s_bids - sum_s_asks) / (sum_s_bids + sum_s_asks)
                if s_imbalance > self.max_adverse_imbalance:
                    return False, {"reason": f"ADVERSE_OBI_SHORT (Bid skew: {s_imbalance:+.2f} > +{self.max_adverse_imbalance:.2f})"}
            
        if self.check_synthetic_exit:
            long_vol_exit = self._get_vol_discount_exit(long_ex)
            short_vol_exit = self._get_vol_discount_exit(short_ex)
            long_exit_vwap_bid = OrderbookUtils.calculate_vwap_by_qty(long_book.get("bids", []), long_qty, long_vol_exit)
            short_exit_vwap_ask = OrderbookUtils.calculate_vwap_by_qty(short_book.get("asks", []), short_qty, short_vol_exit)
            
            if long_exit_vwap_bid <= 0 or short_exit_vwap_ask <= 0:
                return False, {"reason": "NO_REVERSE_LIQUIDITY"}
                
            if self.check_synthetic_slippage:
                long_synthetic_slip = (long_vwap_ask - long_exit_vwap_bid) / long_vwap_ask
                short_synthetic_slip = (short_exit_vwap_ask - short_vwap_bid) / short_vwap_bid
                total_slippage = long_synthetic_slip + short_synthetic_slip
                max_allowed = net_spread * self.max_slippage_ratio
                
                if total_slippage > max_allowed:
                    return False, {"reason": f"HIGH_REVERSE_SLIPPAGE"}
                if total_slippage > self.hard_max_slippage:
                    return False, {"reason": f"HARD_SLIPPAGE_LIMIT"}
                    
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
        """DEPRECATED (v8 hedged mode). Use evaluate_exit_v9() for single-leg."""
        target_val, exit_level_index = self.get_exit_target_val(duration_sec, actual_net_spread_entry, decay_map=decay_map)
        is_ttl = target_val <= -999.0
        reported_target = None if is_ttl else target_val
        
        if not is_stakan_valid:
            if is_ttl:
                return True, {"net_yield": None, "target_val": reported_target, "vwap_spread_out": None, "long_close_price": None, "short_close_price": None, "reason": "TTL_EXPIRED_STALE_DATA", "exit_level_index": exit_level_index}
            return False, {"net_yield": None, "target_val": reported_target, "reason": "STALE_DATA", "exit_level_index": exit_level_index}
            
        long_vol_exit = self._get_vol_discount_exit(long_ex)
        short_vol_exit = self._get_vol_discount_exit(short_ex)
        
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
        total_comm = (long_fee * 2.0) + (short_fee * 2.0)
        
        net_yield = (long_realized_pnl * long_executed_volume_rate) + (short_realized_pnl * short_executed_volume_rate) - total_comm
        vwap_spread_out = (short_vwap_ask - long_vwap_bid) / short_vwap_ask
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
        """DEPRECATED (v8 hedged mode)."""
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
            actual_spread = (actual_short_price - actual_long_price) / actual_short_price if actual_short_price > 0 else 0.0
        else:
            actual_spread = expected_res.get("vwap_spread", 0.0)
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
