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
        
        entry_cfg = self.cfg["trading_rules"]["entry"]
        target_entry_cfg = entry_cfg["target_entry_logic"] if "target_entry_logic" in entry_cfg else entry_cfg["parallel_entry_logic"]
        ban_q = self.cfg["trading_rules"]["ban_rules"]["quarantine_sec"]
        
        self.q_entry_error = float(ban_q["entry_error"])
        self.q_zero_fill = float(ban_q["zero_fill"])
        
        target_exit_cfg = self.cfg["trading_rules"]["exit"]["target_exit"]
        decay_map = target_exit_cfg["decay_map"]
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
        self.exit_order_type = target_exit_cfg["exit_order_type"]
        self.exit_slip_ratio = float(target_exit_cfg["exit_slip_ratio"])
        ioc_timeout = target_exit_cfg.get("ioc_close_confirm_timeout_sec", target_exit_cfg.get("ioc_chase_timeout_sec", 0.2))
        self.ioc_close_confirm_timeout_sec = float(ioc_timeout)
        self.ioc_chase_timeout_sec = self.ioc_close_confirm_timeout_sec
        self.close_poll_interval = float(target_exit_cfg.get("close_poll_interval_sec", 0.005))
        
        timeout_cfg = target_entry_cfg["fill_confirm_timeout_sec"]
        pair_key1 = f"{self.target_ex}_{self.oracle_ex}".upper()
        pair_key2 = f"{self.oracle_ex}_{self.target_ex}".upper()
        if isinstance(timeout_cfg, dict):
            if pair_key1 in timeout_cfg:
                self.fill_confirm_timeout = float(timeout_cfg[pair_key1])
            elif pair_key2 in timeout_cfg:
                self.fill_confirm_timeout = float(timeout_cfg[pair_key2])
            else:
                self.fill_confirm_timeout = 0.5
        else:
            self.fill_confirm_timeout = float(timeout_cfg)
        
        self.fill_confirm_poll_interval = float(target_entry_cfg["fill_confirm_poll_interval_sec"])
        self.entry_api_timeout = float(target_entry_cfg["entry_api_timeout_sec"])
        
        # Entry slip ratio: единый статический допустимый предел (0.0020 = 0.20%)
        if "entry_slip_ratio" in target_entry_cfg:
            slip_cfg = target_entry_cfg["entry_slip_ratio"]
            if isinstance(slip_cfg, dict):
                self.entry_slip_ratio = float(slip_cfg[self.target_ex] if self.target_ex in slip_cfg else slip_cfg[self.target_ex.lower()])
            else:
                self.entry_slip_ratio = float(slip_cfg)
        elif "limit_slip_ratio" in target_entry_cfg:
            slip_cfg = target_entry_cfg["limit_slip_ratio"]
            if isinstance(slip_cfg, dict):
                self.entry_slip_ratio = float(slip_cfg[self.target_ex] if self.target_ex in slip_cfg else slip_cfg[self.target_ex.lower()])
            else:
                self.entry_slip_ratio = float(slip_cfg)
        elif self.target_ex.lower() in self.cfg["trading_risks"] and "limit_slip_ratio" in self.cfg["trading_risks"][self.target_ex.lower()]:
            self.entry_slip_ratio = float(self.cfg["trading_risks"][self.target_ex.lower()]["limit_slip_ratio"])
        else:
            self.entry_slip_ratio = 0.0020
        
        unwind_cfg = self.cfg["trading_rules"]["emergency_unwind"]
        self.unwind_max_attempts = int(unwind_cfg["max_attempts"])
        self.ws_verify_timeout = float(unwind_cfg["ws_verify_timeout_sec"])
        self.unwind_retry_pause = float(unwind_cfg["retry_pause_sec"])
        
        exit_cfg = self.cfg["trading_rules"]["exit"]
        close_timeout_cfg = (
            exit_cfg["market_close_confirm_timeout_sec"]
            if "market_close_confirm_timeout_sec" in exit_cfg
            else exit_cfg["close_confirm_timeout_sec"]
        )
        if isinstance(close_timeout_cfg, dict):
            if pair_key1 in close_timeout_cfg:
                self.close_confirm_timeout = float(close_timeout_cfg[pair_key1])
            elif pair_key2 in close_timeout_cfg:
                self.close_confirm_timeout = float(close_timeout_cfg[pair_key2])
            else:
                self.close_confirm_timeout = 1.8
        else:
            self.close_confirm_timeout = float(close_timeout_cfg)
        
        ban_cfg = self.cfg["trading_rules"]["ban_rules"]
        raw_perm = ban_cfg.get("perm_ban_loss_ratio")
        if raw_perm is None:
            raw_perm = ban_cfg.get("perm_ban_loss_pct", 0.0075)
            if float(raw_perm) >= 0.05:
                raw_perm = float(raw_perm) / 100.0
        self.perm_ban_loss_ratio = float(raw_perm)
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
            poll_sleep = self.fill_confirm_poll_interval if self.fill_confirm_poll_interval > 0 else 0.005
            await asyncio.sleep(poll_sleep)
                
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
        size_usd = float(self.cfg["trading_risks"][self.target_ex.lower()]["trade_size_usd"])
        
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
        
        target_fee = float(self.cfg["trading_risks"][self.target_ex.lower()]["taker_fee"])
        
        expected_price = self.engine_res.get("entry_price", filled_price)
        slippage = 0.0
        if expected_price > 0 and filled_price > 0:
            if self.side == "LONG":
                slippage = (filled_price - expected_price) / expected_price
            else:
                slippage = (expected_price - filled_price) / expected_price
        actual_net_spread = self.engine_res.get("net_spread", 0.0) - slippage
        
        target_exit_cfg = self.cfg.get("trading_rules", {}).get("target_exit", {})
        min_spread_entry = float(target_exit_cfg.get("min_spread_entry", 0.0030))
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
            await asyncio.sleep(self.close_poll_interval)
            
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
        
        if reason in ("TTL_EXPIRED", "STOP_LOSS", "TTL_EXPIRED_NO_LIQUIDITY", "TTL_EXPIRED_STALE_DATA", "EMERGENCY_TTL"):
            o_type = "MARKET"
        else:
            o_type = self.exit_order_type
        
        if o_type == "LIMIT_IOC" and exit_price > 0:
            if close_side == "SELL":
                limit_price = exit_price * (1 - self.exit_slip_ratio)
            else:
                limit_price = exit_price * (1 + self.exit_slip_ratio)
        else:
            limit_price = exit_price
        
        usd = qty * limit_price
        
        log(f"[{self.sym}] v9 Closing {self.side} on {self.target_ex} | "
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
            # REST fallback
            for attempt in range(self.unwind_max_attempts):
                rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, pos_side)
                rem = rest_pos.get("size", 0.0)
                if rem <= 0:
                    break
                p = rest_pos.get("price", exit_price)
                if not isinstance(p, (int, float)) or p <= 0:
                    p = entry_price
                log(f"[{self.sym}] Remainder {rem} on {self.target_ex}. Emergency MARKET close (attempt {attempt+1}).", level="WARNING")
                try:
                    await self.orders[self.target_ex].place_order(
                        self.native_target, close_side, rem * p, p,
                        order_type="MARKET", position_side=pos_side, reduce_only=True
                    )
                except Exception as e:
                    pass
                await asyncio.sleep(self.unwind_retry_pause)
                
            rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, pos_side)
            if rest_pos.get("size", 0.0) > 0:
                log(f"[{self.sym}] CRITICAL ERROR: Failed to close position on {self.target_ex}! Remainder: {rest_pos.get('size')}", level="ERROR")
                self._notify_pos_exit_failed()
                return False

        log(f"[{self.sym}] Position fully liquidated on {self.target_ex}.", level="INFO")
        
        if hasattr(self.orders[self.target_ex], "cancel_all_orders"):
            await self.orders[self.target_ex].cancel_all_orders(self.native_target)
            
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
