# ============================================================
# FILE: CORE/position_fsm.py
# ROLE: Position lifecycle FSM (entry, exit, emergency unwind)
# ============================================================

import asyncio
import time
from enum import Enum
from typing import Dict, Any, Optional, Tuple

from c_log import log
from CORE.ipc_socket import async_write_msg
from API.orders import InsufficientMarginError
from CORE.trading_engine import TradingEngine

class PositionState(str, Enum):
    IDLE = "IDLE"
    SUBMITTING = "SUBMITTING"      # Ордер летит в Target
    VERIFYING_FILL = "VERIFYING_FILL"  # Ждём WS-подтверждения налива
    ACTIVE = "ACTIVE"              # Позиция открыта, мониторим P&L
    CLOSING = "CLOSING"            # Закрываем позицию
    SETTLED = "SETTLED"            # Позиция закрыта, PnL посчитан
    ABORTED = "ABORTED"            # Нулевой налив / ошибка
    FAILED = "FAILED"              # Критическая ошибка
    EMERGENCY_UNWIND = "EMERGENCY_UNWIND"  # Экстренный сброс
    ACTIVE_HEDGED = "ACTIVE"       # Alias for backward compatibility with v8 tests
    SINGLE_LEG_EXPOSURE = "ACTIVE" # Alias for backward compatibility

class PositionFSM:
    def __init__(
        self,
        sym: str,
        route: str,
        target_ex: str = None,        # v9: только Target (исполнитель)
        oracle_ex: str = None,        # v9: только для логов (поводырь)
        side: str = "LONG",           # "LONG" | "SHORT"
        engine_res: dict = None,
        cfg: dict = None,
        orders: dict = None,
        coin_to_native: dict = None,
        pm: Any = None,
        writer: Optional[asyncio.StreamWriter] = None,
        ban_coin_cb: Any = None,
        on_settle_cb: Any = None,
        **kwargs
    ):
        self.sym = sym
        self.route = route
        
        # Support both v9 (target_ex, oracle_ex) and legacy v8 (short_ex, long_ex)
        if target_ex is None and "short_ex" in kwargs:
            target_ex = kwargs["short_ex"]
        if oracle_ex is None and "long_ex" in kwargs:
            oracle_ex = kwargs["long_ex"]
            
        self.target_ex = target_ex or "BITGET"
        self.oracle_ex = oracle_ex or "BINANCE"
        self.long_ex = kwargs.get("long_ex", self.oracle_ex)
        self.short_ex = kwargs.get("short_ex", self.target_ex)
        self.side = side
        self.engine_res = engine_res or {}
        self.cfg = cfg or {}
        self.orders = orders or {}
        self.coin_to_native = coin_to_native or {}
        self.pm = pm
        self.writer = writer
        self.ban_coin_cb = ban_coin_cb or (lambda *args, **kw: None)
        self.on_settle_cb = on_settle_cb
        
        self.native_target = self.coin_to_native.get(sym, {}).get(self.target_ex, sym)
        self.state = PositionState.IDLE
        self.engine = TradingEngine(self.cfg, {0:"BINANCE",1:"KUCOIN",2:"OKX",3:"BITGET"})
        
        ban_cfg = self.cfg.get("ban_rules") or self.cfg.get("trading_rules", {}).get("ban_rules", {})
        ban_q = ban_cfg.get("quarantine_sec", {"entry_error": 60, "zero_fill": 10})
        self.q_entry_error = float(ban_q.get("entry_error", 60))
        self.q_zero_fill = float(ban_q.get("zero_fill", 10))
        
        target_ex_upper = self.target_ex.upper()
        if "exchanges" in self.cfg and target_ex_upper in self.cfg["exchanges"]:
            ex_sec = self.cfg["exchanges"][target_ex_upper]
            if "exit" in ex_sec and "target_exit" in ex_sec["exit"]:
                target_exit_cfg = ex_sec["exit"]["target_exit"]
            elif "target_exit" in ex_sec:
                target_exit_cfg = ex_sec["target_exit"]
            else:
                target_exit_cfg = {}
        elif "exit" in self.cfg.get("trading_rules", {}) and "target_exit" in self.cfg["trading_rules"]["exit"]:
            target_exit_cfg = self.cfg["trading_rules"]["exit"]["target_exit"]
        elif "target_exit" in self.cfg.get("trading_rules", {}):
            target_exit_cfg = self.cfg["trading_rules"]["target_exit"]
        else:
            target_exit_cfg = self.cfg.get("target_exit", {})
            
        decay_map = target_exit_cfg.get("decay_map", [])
        derived_ttl = None
        for rule in decay_map:
            ratio = rule["min_profit_ratio"] if "min_profit_ratio" in rule else None
            spread = rule["target_spread"] if "target_spread" in rule else None
            if (ratio is None and spread is None) or (isinstance(ratio, (int, float)) and ratio <= -900.0):
                derived_ttl = float(rule["after_sec"])
                break
        if derived_ttl is not None:
            self.ttl_sec = derived_ttl
        elif "ttl_sec" in target_exit_cfg and target_exit_cfg["ttl_sec"] is not None:
            self.ttl_sec = float(target_exit_cfg["ttl_sec"])
        elif decay_map:
            self.ttl_sec = float(decay_map[-1]["after_sec"])
        else:
            self.ttl_sec = 60.0
        self.exit_order_type = target_exit_cfg.get("exit_order_type", "MARKET")
        self.exit_slip_ratio = float(target_exit_cfg.get("exit_slip_ratio", 0.0010))
        if "ioc_close_confirm_timeout_sec" in target_exit_cfg:
            self.ioc_close_confirm_timeout_sec = float(target_exit_cfg["ioc_close_confirm_timeout_sec"])
        elif "ioc_chase_timeout_sec" in target_exit_cfg:
            self.ioc_close_confirm_timeout_sec = float(target_exit_cfg["ioc_chase_timeout_sec"])
        else:
            self.ioc_close_confirm_timeout_sec = 0.2
        self.ioc_chase_timeout_sec = self.ioc_close_confirm_timeout_sec

        route_info = None
        if "routes" in self.cfg:
            for rk in [self.route, f"{self.target_ex}_{self.oracle_ex}", f"{self.oracle_ex}_{self.target_ex}"]:
                if rk in self.cfg["routes"]:
                    route_info = self.cfg["routes"][rk]
                    break
        if route_info and isinstance(route_info, dict):
            self.close_confirm_timeout = float(route_info.get("market_close_confirm_timeout_sec", 1.8))
            self.fill_confirm_timeout = float(route_info.get("fill_confirm_timeout_sec", 0.3))
        else:
            self.close_confirm_timeout = 1.8
            self.fill_confirm_timeout = 0.3
        
        if "exchanges" in self.cfg and target_ex_upper in self.cfg["exchanges"] and "entry" in self.cfg["exchanges"][target_ex_upper]:
            entry_cfg = self.cfg["exchanges"][target_ex_upper]["entry"]
        elif "entry" in self.cfg.get("trading_rules", {}):
            entry_cfg = self.cfg["trading_rules"]["entry"]
        else:
            entry_cfg = {}
            
        if "entry_api_timeout_sec" in entry_cfg:
            self.entry_api_timeout = float(entry_cfg["entry_api_timeout_sec"])
        elif "target_entry_logic" in self.cfg.get("exchanges", {}).get(target_ex_upper, {}):
            self.entry_api_timeout = float(self.cfg["exchanges"][target_ex_upper]["target_entry_logic"]["entry_api_timeout_sec"])
        elif "target_entry_logic" in entry_cfg:
            self.entry_api_timeout = float(entry_cfg["target_entry_logic"]["entry_api_timeout_sec"])
        else:
            self.entry_api_timeout = 5.0
        
        if "exchanges" in self.cfg and target_ex_upper in self.cfg["exchanges"] and "trading_risks" in self.cfg["exchanges"][target_ex_upper]:
            self.entry_slip_ratio = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"].get("limit_slip_ratio", 0.0015))
        elif "trading_risks" in self.cfg:
            ex_key = self.target_ex.lower() if self.target_ex.lower() in self.cfg["trading_risks"] else target_ex_upper
            if ex_key in self.cfg["trading_risks"]:
                self.entry_slip_ratio = float(self.cfg["trading_risks"][ex_key].get("limit_slip_ratio", 0.0015))
            else:
                self.entry_slip_ratio = 0.0015
        else:
            self.entry_slip_ratio = 0.0015
        
        unwind_cfg = self.cfg.get("trading_rules", {}).get("emergency_unwind", {})
        self.unwind_max_attempts = int(unwind_cfg.get("max_attempts", 2))
        self.ws_verify_timeout = float(unwind_cfg.get("ws_verify_timeout_sec", 0.3))
        self.unwind_retry_pause = float(unwind_cfg.get("retry_pause_sec", 0.05))

        ext_cfg = target_exit_cfg.get("orderbook_hunting", {}).get("extrime_close", {})
        self.extrime_max_retries = int(ext_cfg.get("max_retries", 10))
        self.extrime_retry_pause = float(ext_cfg.get("retry_ttl_sec", 0.3))
        self.extrime_increase_fraction = float(ext_cfg.get("increase_fraction", 0.05))
        self.extrime_orientation = float(ext_cfg.get("bid_to_ask_orientation", 0.0))
        self.unwind_max_attempts = max(self.unwind_max_attempts, self.extrime_max_retries)
        
        ban_cfg = self.cfg.get("ban_rules") or self.cfg.get("trading_rules", {}).get("ban_rules", {})
        if "perm_ban_loss_ratio" in ban_cfg:
            self.perm_ban_loss_ratio = float(ban_cfg["perm_ban_loss_ratio"])
        elif "perm_ban_loss_pct" in ban_cfg:
            self.perm_ban_loss_ratio = float(ban_cfg["perm_ban_loss_pct"]) / 100.0
        else:
            self.perm_ban_loss_ratio = float(ban_cfg.get("perm_ban_loss_ratio", 0.0075))
        self.perm_ban_loss_pct = self.perm_ban_loss_ratio  # backward compatibility alias
        
        self.target_pos: dict = {"size": 0.0, "price": 0.0}
        self.open_time: float = 0.0
        self.open_time_ms: int = 0
        self.exec_res: dict = {}

    def _set_state(self, new_state: PositionState):
        self.state = new_state
        
    async def _wait_for_fill_v9(self, req_qty: float, ev_target: asyncio.Event) -> Tuple[dict, float]:
        t0 = time.perf_counter()
        filled_qty = 0.0
        
        # Reactive await via event
        if isinstance(ev_target, asyncio.Event):
            try:
                await asyncio.wait_for(ev_target.wait(), timeout=self.fill_confirm_timeout)
            except asyncio.TimeoutError:
                pass
                
        # Check immediate WS position fill
        pos = self.orders[self.target_ex].get_executed_position(self.native_target, self.side)
        if pos:
            filled_qty = pos.get("size", 0.0)
            if filled_qty > 0.0:
                return pos, (filled_qty / req_qty if req_qty > 0 else 0.0)

        # Check immediate WS order terminal event (canceled / expired / rejected with 0 fill)
        if hasattr(self.orders[self.target_ex], "get_last_order_event"):
            order_ev = self.orders[self.target_ex].get_last_order_event(self.native_target, self.side)
            if order_ev:
                status = str(order_ev.get("status", "")).lower()
                cum_qty = float(order_ev.get("cum_qty", 0.0))
                if status in ("canceled", "cancelled", "rejected", "expired") and cum_qty == 0.0:
                    elapsed_ms = (time.perf_counter() - t0) * 1000
                    log(f"[{self.sym}] v9 Immediate Zero Fill on {self.target_ex} (order {status} in {elapsed_ms:.1f}ms).", level="INFO")
                    return {"size": 0.0, "price": 0.0}, 0.0

        # Polling fallback if time permits
        while (time.perf_counter() - t0) < self.fill_confirm_timeout:
            pos = self.orders[self.target_ex].get_executed_position(self.native_target, self.side)
            if pos:
                filled_qty = pos.get("size", 0.0)
                if filled_qty > 0.0:
                    return pos, (filled_qty / req_qty if req_qty > 0 else 0.0)
            if hasattr(self.orders[self.target_ex], "get_last_order_event"):
                order_ev = self.orders[self.target_ex].get_last_order_event(self.native_target, self.side)
                if order_ev:
                    status = str(order_ev.get("status", "")).lower()
                    cum_qty = float(order_ev.get("cum_qty", 0.0))
                    if status in ("canceled", "cancelled", "rejected", "expired") and cum_qty == 0.0:
                        elapsed_ms = (time.perf_counter() - t0) * 1000
                        log(f"[{self.sym}] v9 Immediate Zero Fill on {self.target_ex} (order {status} in {elapsed_ms:.1f}ms).", level="INFO")
                        return {"size": 0.0, "price": 0.0}, 0.0
            await asyncio.sleep(0)
                
        # If we got here, neither a position fill nor an order cancel event was received within timeout
        log(f"[{self.sym}] v9 Fill confirmation timeout on {self.target_ex}.", level="WARNING")
        
        # REST fallback
        try:
            pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, self.side)
            if pos:
                filled_qty = pos.get("size", 0.0)
                return pos, (filled_qty / req_qty if req_qty > 0 else 0.0)
        except Exception as e:
            log(f"[{self.sym}] Error fetching REST position on {self.target_ex}: {e}", level="ERROR")
            
        return {"size": 0.0, "price": 0.0}, 0.0

    async def run_open(self) -> bool:
        """
        v9: IDLE -> SUBMITTING -> VERIFYING_FILL -> ACTIVE / ABORTED
        Кидаем 1 LIMIT_IOC в Target, ждём WS-подтверждения.
        """
        self._set_state(PositionState.SUBMITTING)
        
        entry_price = self.engine_res["entry_price"]
        target_ex_upper = self.target_ex.upper()
        if "exchanges" in self.cfg and target_ex_upper in self.cfg["exchanges"] and "trading_risks" in self.cfg["exchanges"][target_ex_upper]:
            size_usd = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"]["trade_size_usd"])
        elif "trading_risks" in self.cfg:
            ex_key = self.target_ex.lower() if self.target_ex.lower() in self.cfg["trading_risks"] else target_ex_upper
            if ex_key in self.cfg["trading_risks"]:
                size_usd = float(self.cfg["trading_risks"][ex_key]["trade_size_usd"])
            else:
                size_usd = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"]["trade_size_usd"])
        else:
            size_usd = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"]["trade_size_usd"])
        
        planned_profit = float(self.engine_res.get("net_spread", 0.0))
        slip = self.entry_slip_ratio
        
        if self.side == "LONG":
            order_side = "BUY"
            limit_price = entry_price * (1 + slip)
            position_side = "LONG"
        else:
            order_side = "SELL"
            limit_price = entry_price * (1 - slip)
            position_side = "SHORT"
        
        log(f"[{self.sym}] v9 Opening {self.side} on {self.target_ex} | "
            f"Price: {limit_price:.6f} (VWAP: {entry_price:.6f}, Slip: {slip*100:.3f}%) | "
            f"Net Spread: {planned_profit*100:.3f}%", level="INFO")
        
        ev_target = None
        if hasattr(self.orders[self.target_ex], "subscribe_position_update"):
            ev_target = self.orders[self.target_ex].subscribe_position_update(
                self.native_target, position_side
            )
        
        req_qty = self.engine_res.get("qty", size_usd / entry_price)
        
        try:
            await asyncio.wait_for(
                self.orders[self.target_ex].place_order(
                    self.native_target, order_side, size_usd, limit_price,
                    order_type="LIMIT_IOC", position_side=position_side
                ),
                timeout=self.entry_api_timeout
            )
        except InsufficientMarginError as e:
            log(f"[{self.sym}] Target Insufficient Margin: {e}", level="WARNING")
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("MARGIN_ERROR")
            return False
        except asyncio.TimeoutError:
            log(f"[{self.sym}] API Timeout on {self.target_ex}", level="ERROR")
            self.ban_coin_cb(self.sym, reason="Entry API Timeout", duration_sec=self.q_entry_error)
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("ENTRY_TIMEOUT")
            return False
        except Exception as e:
            log(f"[{self.sym}] Entry error on {self.target_ex}: {e}", level="ERROR")
            self.ban_coin_cb(self.sym, reason=str(e), duration_sec=self.q_entry_error)
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("ENTRY_ERROR")
            return False
        
        self._set_state(PositionState.VERIFYING_FILL)
        filled_pos, fill_rate = await self._wait_for_fill_v9(req_qty, ev_target)
        
        if hasattr(self.orders[self.target_ex], "unsubscribe_position_update"):
            self.orders[self.target_ex].unsubscribe_position_update(self.native_target, position_side)
        
        filled_qty = filled_pos.get("size", 0.0)
        filled_price = filled_pos.get("price", 0.0)
        
        if filled_qty <= 0.0:
            log(f"[{self.sym}] Zero Fill on {self.target_ex}. Quarantine {self.q_zero_fill:.0f}s.", level="WARNING")
            self.ban_coin_cb(self.sym, reason="Zero Fill (v9)", duration_sec=self.q_zero_fill)
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("ZERO_FILL")
            return False
        
        self.target_pos = filled_pos
        self._finalize_open_v9(filled_qty, filled_price, fill_rate)
        return True

    def _finalize_open_v9(self, filled_qty: float, filled_price: float, fill_rate: float):
        self.open_time = time.time()
        self.open_time_ms = int(self.open_time * 1000)
        
        target_ex_upper = self.target_ex.upper()
        if "exchanges" in self.cfg and target_ex_upper in self.cfg["exchanges"] and "trading_risks" in self.cfg["exchanges"][target_ex_upper]:
            target_fee = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"]["taker_fee"])
        elif "trading_risks" in self.cfg:
            ex_key = self.target_ex.lower() if self.target_ex.lower() in self.cfg["trading_risks"] else target_ex_upper
            if ex_key in self.cfg["trading_risks"]:
                target_fee = float(self.cfg["trading_risks"][ex_key]["taker_fee"])
            else:
                target_fee = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"]["taker_fee"])
        else:
            target_fee = float(self.cfg["exchanges"][target_ex_upper]["trading_risks"]["taker_fee"])
        
        expected_price = self.engine_res.get("entry_price", filled_price)
        slippage = 0.0
        if expected_price > 0 and filled_price > 0:
            if self.side == "LONG":
                slippage = (filled_price - expected_price) / expected_price
            else:
                slippage = (expected_price - filled_price) / expected_price
        actual_net_spread = self.engine_res.get("net_spread", 0.0) - slippage
        
        if "exchanges" in self.cfg and target_ex_upper in self.cfg["exchanges"]:
            ex_sec = self.cfg["exchanges"][target_ex_upper]
            if "exit" in ex_sec and "target_exit" in ex_sec["exit"]:
                target_exit_cfg = ex_sec["exit"]["target_exit"]
            elif "target_exit" in ex_sec:
                target_exit_cfg = ex_sec["target_exit"]
            else:
                target_exit_cfg = {}
        elif "exit" in self.cfg.get("trading_rules", {}) and "target_exit" in self.cfg["trading_rules"]["exit"]:
            target_exit_cfg = self.cfg["trading_rules"]["exit"]["target_exit"]
        elif "target_exit" in self.cfg.get("trading_rules", {}):
            target_exit_cfg = self.cfg["trading_rules"]["target_exit"]
        else:
            target_exit_cfg = {}
        min_spread_entry = float(target_exit_cfg["min_spread_entry"]) if "min_spread_entry" in target_exit_cfg else 0.0030
        use_emergency = actual_net_spread < min_spread_entry
        
        self.exec_res = {
            "engine_res": self.engine_res,
            "target_ex": self.target_ex,
            "oracle_ex": self.oracle_ex,
            "long_ex": self.oracle_ex,   # backward compatibility with IPC / PM
            "short_ex": self.target_ex,  # backward compatibility with IPC / PM
            "side": self.side,
            "entry_price": filled_price,
            "qty": filled_qty,
            "executed_volume_rate": fill_rate,
            "net_spread": actual_net_spread,
            "planned_net_spread": self.engine_res.get("net_spread", 0.0),
            "entry_slippage": slippage,
            "use_emergency_decay": use_emergency,
            "emergency_since": self.open_time if use_emergency else None,
            "open_time": self.open_time,
            "open_time_ms": self.open_time_ms
        }
        
        self._set_state(PositionState.ACTIVE)
        if self.pm:
            self.pm.confirm_entry(self.oracle_ex, self.target_ex, self.sym, self.exec_res, self.open_time)
            
        decay_str = "⚠️ EMERGENCY DECAY" if use_emergency else "STANDARD DECAY"
        log(f"[{self.sym}] Position opened! Filled: {filled_qty:.4f} @ {filled_price:.6f} | "
            f"Net Spread: {actual_net_spread*100:+.3f}% (Min: {min_spread_entry*100:+.3f}%) -> {decay_str}", level="INFO")

        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_OPENED", {
                "route": self.route,
                "sym": self.sym,
                "exec_res": self.exec_res,
                "open_time": self.open_time
            }))

    def _notify_pos_failed(self, reason: str):
        if self.pm:
            self.pm.rollback_entry(self.oracle_ex, self.target_ex, self.sym)
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                "route": self.route,
                "sym": self.sym,
                "oracle_ex": self.oracle_ex,
                "target_ex": self.target_ex,
                "long_ex": self.oracle_ex,
                "short_ex": self.target_ex,
                "reason": reason
            }))

    def _notify_pos_exit_failed(self):
        if self.pm:
            self.pm.rollback_exit(self.route, self.sym)
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_EXIT_FAILED", {
                "route": self.route,
                "sym": self.sym
            }))

    async def _wait_for_close_v9(self, ev_target: asyncio.Event, timeout: float = None) -> bool:
        t0 = time.perf_counter()
        wait_timeout = timeout if timeout is not None else self.close_confirm_timeout
        
        if isinstance(ev_target, asyncio.Event):
            try:
                await asyncio.wait_for(ev_target.wait(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                pass
                
        # Check if position is already closed
        pos = self.orders[self.target_ex].get_executed_position(self.native_target, self.side)
        if pos and pos.get("size", 0.0) == 0.0:
            return True

        # Check if exit order was cancelled / expired while position remains open
        if hasattr(self.orders[self.target_ex], "get_last_order_event"):
            order_ev = self.orders[self.target_ex].get_last_order_event(self.native_target, self.side)
            if order_ev:
                status = str(order_ev.get("status", "")).lower()
                if status in ("canceled", "cancelled", "rejected", "expired"):
                    return False
                
        while (time.perf_counter() - t0) < wait_timeout:
            pos = self.orders[self.target_ex].get_executed_position(self.native_target, self.side)
            if pos and pos.get("size", 0.0) == 0.0:
                return True
            if hasattr(self.orders[self.target_ex], "get_last_order_event"):
                order_ev = self.orders[self.target_ex].get_last_order_event(self.native_target, self.side)
                if order_ev:
                    status = str(order_ev.get("status", "")).lower()
                    if status in ("canceled", "cancelled", "rejected", "expired"):
                        return False
            await asyncio.sleep(0)
            
        return False

    async def run_close(self, exit_res: dict, reason: str = "TAKE_PROFIT") -> bool:
        """
        v9: ACTIVE -> CLOSING -> SETTLED
        1 ордер в Target (LIMIT_IOC или MARKET при TTL/SL).
        """
        self._set_state(PositionState.CLOSING)
        
        pos_side = self.side
        close_side = "SELL" if self.side == "LONG" else "BUY"
        
        ws_pos = self.orders[self.target_ex].get_executed_position(self.native_target, pos_side)
        qty = ws_pos.get("size", 0.0) if ws_pos else self.target_pos.get("size", 0.0)
        
        if qty <= 0:
            rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(
                self.native_target, pos_side
            )
            qty = rest_pos.get("size", 0.0)
        
        if qty <= 0:
            log(f"[{self.sym}] Position already flat on {self.target_ex}", level="WARNING")
            self._set_state(PositionState.SETTLED)
            if self.pm:
                self.pm.confirm_exit(self.route, self.sym)
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {
                    "route": self.route, "sym": self.sym, "reason": reason
                }))
            return True
        
        entry_price = self.exec_res.get("entry_price") or self.engine_res.get("entry_price", 0.0)
        exit_price = exit_res.get("exit_price") or entry_price
        
        # В v11: Аварийный MARKET допустим ТОЛЬКО при жестком STOP_LOSS, развороте поводыря (ORACLE_REVERSAL_STOP) или отсутствии ликвидности/стакана на TTL.
        # Все штатные выходы (TAKE_PROFIT, BREAKEVEN, EXTRIME_CLOSE, TTL) выполняются СТРОГО через LIMIT_IOC.
        if reason in ("STOP_LOSS", "ORACLE_REVERSAL_STOP", "TTL_EXPIRED_STALE_DATA", "TTL_EXPIRED_NO_LIQUIDITY"):
            o_type = "MARKET"
        else:
            o_type = exit_res.get("order_type", self.exit_order_type)
            if o_type not in ("LIMIT_IOC", "MARKET"):
                o_type = "LIMIT_IOC"
        
        if o_type == "LIMIT_IOC" and exit_price > 0:
            if close_side == "SELL":
                limit_price = exit_price * (1 - self.exit_slip_ratio)
            else:
                limit_price = exit_price * (1 + self.exit_slip_ratio)
        else:
            limit_price = exit_price
        
        usd = qty * limit_price
        
        log(f"[{self.sym}] v11 Closing {self.side} on {self.target_ex} | "
            f"{o_type} {close_side} {qty:.4f} @ {limit_price:.6f} | Reason: {reason}", level="INFO")
        
        ev_target = None
        if hasattr(self.orders[self.target_ex], "subscribe_position_update"):
            ev_target = self.orders[self.target_ex].subscribe_position_update(self.native_target, pos_side)
        
        try:
            await self.orders[self.target_ex].place_order(
                self.native_target, close_side, usd, limit_price,
                order_type=o_type, position_side=pos_side, reduce_only=True
            )
        except Exception as e:
            log(f"[{self.sym}] Close order error on {self.target_ex}: {e}", level="ERROR")
        
        if o_type == "LIMIT_IOC":
            # For IOC, match engine execution is instant. If WS event not received in ioc_close_confirm_timeout_sec,
            # remainder was cancelled by exchange -> immediately fall back without lag
            is_closed = await self._wait_for_close_v9(ev_target, timeout=min(self.ioc_close_confirm_timeout_sec, self.close_confirm_timeout))
        else:
            is_closed = await self._wait_for_close_v9(ev_target)
        
        if hasattr(self.orders[self.target_ex], "unsubscribe_position_update"):
            self.orders[self.target_ex].unsubscribe_position_update(self.native_target, pos_side)
        
        actual_exit_price = exit_price
        if hasattr(self.orders[self.target_ex], "get_last_close_price"):
            try:
                p = self.orders[self.target_ex].get_last_close_price(self.native_target)
                if isinstance(p, (int, float)) and p > 0:
                    actual_exit_price = float(p)
            except Exception:
                pass
                
        if not is_closed:
            # Extrime / Step unwind with LIMIT_IOC (10 attempts over ~3.0 seconds)
            retry_pause = getattr(self, "extrime_retry_pause", self.unwind_retry_pause)
            for attempt in range(self.unwind_max_attempts):
                rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, pos_side)
                rem = rest_pos.get("size", 0.0)
                if rem <= 0:
                    break
                p = rest_pos.get("price", exit_price)
                if not isinstance(p, (int, float)) or p <= 0:
                    p = entry_price
                
                # Progressive step limit IOC for remainder: progressively shifts deeper into book
                shift_fraction = 0.0005 * (attempt + 1)
                shift_mult = 1.0 - shift_fraction if close_side == "SELL" else 1.0 + shift_fraction
                step_limit_p = p * shift_mult
                log(f"[{self.sym}] Remainder {rem} on {self.target_ex}. Extrime LIMIT_IOC close (attempt {attempt+1}/{self.unwind_max_attempts}) @ {step_limit_p:.6f}.", level="WARNING")
                try:
                    await self.orders[self.target_ex].place_order(
                        self.native_target, close_side, rem * step_limit_p, step_limit_p,
                        order_type="LIMIT_IOC", position_side=pos_side, reduce_only=True
                    )
                except Exception as e:
                    log(f"[{self.sym}] Extrime close limit retry {attempt+1} error: {e}", level="WARNING")
                await asyncio.sleep(retry_pause)
                
            rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, pos_side)
            if rest_pos.get("size", 0.0) > 0:
                # Guaranteed Final Emergency MARKET sweep if extreme IOC exhausted all attempts (3.0 seconds)
                rem = rest_pos.get("size", 0.0)
                p = rest_pos.get("price", entry_price)
                log(f"[{self.sym}] 🚨 Extrime Close retries exhausted. Executing Guaranteed Emergency MARKET sweep for remainder {rem} on {self.target_ex}!", level="WARNING")
                for m_att in range(3):
                    try:
                        await self.orders[self.target_ex].place_order(
                            self.native_target, close_side, rem * p, p,
                            order_type="MARKET", position_side=pos_side, reduce_only=True
                        )
                        break
                    except Exception as me:
                        log(f"[{self.sym}] Emergency market sweep error (attempt {m_att+1}/3): {me}", level="ERROR")
                        await asyncio.sleep(0.05)
                        
                rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, pos_side)
                if rest_pos.get("size", 0.0) > 0:
                    log(f"[{self.sym}] CRITICAL ERROR: Failed to close position on {self.target_ex}! Remainder: {rest_pos.get('size')}", level="ERROR")
                    self._notify_pos_exit_failed()
                    return False

        log(f"[{self.sym}] Position fully liquidated on {self.target_ex}.", level="INFO")
        
        if hasattr(self.orders[self.target_ex], "cancel_all_orders"):
            try:
                res = self.orders[self.target_ex].cancel_all_orders(self.native_target)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
            
        self._set_state(PositionState.SETTLED)
        self._finalize_close_v9(actual_exit_price, qty, reason)
        return True

    def _finalize_close_v9(self, exit_price: float, qty: float, reason: str):
        if self.pm:
            self.pm.confirm_exit(self.route, self.sym)
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {
                "route": self.route, "sym": self.sym, "reason": reason
            }))
        if self.on_settle_cb:
            entry_price = self.exec_res.get("entry_price", 0.0)
            actual_usd = qty * exit_price
            try:
                res = self.on_settle_cb(
                    sym=self.sym,
                    route=self.route,
                    target_ex=self.target_ex,
                    oracle_ex=self.oracle_ex,
                    side=self.side,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    actual_usd=actual_usd,
                    reason=reason
                )
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as e:
                log(f"[{self.sym}] Error invoking on_settle_cb: {e}", level="ERROR")

    async def _emergency_unwind_single(self, ex: str, native_sym: str, qty: float, price: float, open_side: str, pos_side: str):
        self._set_state(PositionState.EMERGENCY_UNWIND)
        usd = qty * price
        reduce_side = "SELL" if open_side == "BUY" else "BUY"
        log(f"[{self.sym}] Immediate unwind {ex} ({qty} qty, {usd:.2f}$)...", level="WARNING")
        try:
            await self.orders[ex].place_order(native_sym, reduce_side, usd, price, order_type="MARKET", position_side=pos_side, reduce_only=True, is_full_unwind=True)
        except Exception as e:
            log(f"[{self.sym}] Unwind error on {ex}: {e}", level="ERROR")

    async def _run_single_leg_exposure(
        self,
        l_qty: float = 0.0,
        s_qty: float = 0.0,
        l_price: float = 0.0,
        s_price: float = 0.0,
        long_qty: float = 0.0,
        short_qty: float = 0.0,
        long_entry_price: float = 0.0,
        short_entry_price: float = 0.0,
        **kwargs
    ):
        """Emergency unwind / closing of a single exposed position leg."""
        qty_l = l_qty if l_qty > 0 else long_qty
        qty_s = s_qty if s_qty > 0 else short_qty
        price_l = l_price if l_price > 0 else long_entry_price
        price_s = s_price if s_price > 0 else short_entry_price

        self._set_state(PositionState.EMERGENCY_UNWIND)

        ex_l = self.long_ex if (hasattr(self, "long_ex") and self.long_ex in self.orders) else (
            self.target_ex if self.target_ex in self.orders else (
                self.oracle_ex if self.oracle_ex in self.orders else next(iter(self.orders), "BITGET")
            )
        )
        ex_s = self.short_ex if (hasattr(self, "short_ex") and self.short_ex in self.orders) else (
            self.target_ex if self.target_ex in self.orders else (
                self.oracle_ex if self.oracle_ex in self.orders else next(iter(self.orders), "BITGET")
            )
        )

        if qty_l > 0:
            native_l = self.coin_to_native.get(self.sym, {}).get(ex_l, self.sym)
            await self._emergency_unwind_single(ex_l, native_l, qty_l, price_l, "BUY", "LONG")

        if qty_s > 0:
            native_s = self.coin_to_native.get(self.sym, {}).get(ex_s, self.sym)
            await self._emergency_unwind_single(ex_s, native_s, qty_s, price_s, "SELL", "SHORT")

        self._set_state(PositionState.ABORTED)
