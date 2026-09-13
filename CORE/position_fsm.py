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

class PositionFSM:
    def __init__(
        self,
        sym: str,
        route: str,
        target_ex: str,        # v9: только Target (исполнитель)
        oracle_ex: str,        # v9: только для логов (поводырь)
        side: str,             # "LONG" | "SHORT"
        engine_res: dict,
        cfg: dict,
        orders: dict,
        coin_to_native: dict,
        pm: Any,
        writer: Optional[asyncio.StreamWriter],
        ban_coin_cb: Any,
        on_settle_cb: Any = None
    ):
        self.sym = sym
        self.route = route
        self.target_ex = target_ex
        self.oracle_ex = oracle_ex
        self.side = side
        self.engine_res = engine_res
        self.cfg = cfg
        self.orders = orders
        self.coin_to_native = coin_to_native
        self.pm = pm
        self.writer = writer
        self.ban_coin_cb = ban_coin_cb
        self.on_settle_cb = on_settle_cb
        
        self.native_target = self.coin_to_native[sym][target_ex]
        self.state = PositionState.IDLE
        self.engine = TradingEngine(self.cfg, {0:"BINANCE",1:"KUCOIN",2:"OKX",3:"BITGET"})
        
        entry_cfg = self.cfg["trading_rules"]["entry"]
        parallel_cfg = entry_cfg["parallel_entry_logic"]
        ban_q = self.cfg["trading_rules"]["ban_rules"]["quarantine_sec"]
        
        self.q_entry_error = float(ban_q["entry_error"])
        self.q_zero_fill = float(ban_q["zero_fill"])
        
        target_exit_cfg = self.cfg["trading_rules"]["exit"].get("target_exit", {})
        self.ttl_sec = float(target_exit_cfg.get("ttl_sec", 60.0))
        self.exit_order_type = target_exit_cfg.get("exit_order_type", "LIMIT_IOC")
        self.exit_slip_ratio = float(target_exit_cfg.get("exit_slip_ratio", 0.001))
        
        timeout_cfg = parallel_cfg["fill_confirm_timeout_sec"]
        pair_key1 = f"{target_ex}_{oracle_ex}".upper()
        pair_key2 = f"{oracle_ex}_{target_ex}".upper()
        if pair_key1 in timeout_cfg:
            self.fill_confirm_timeout = float(timeout_cfg[pair_key1])
        elif pair_key2 in timeout_cfg:
            self.fill_confirm_timeout = float(timeout_cfg[pair_key2])
        else:
            self.fill_confirm_timeout = 0.5  # default
        
        self.fill_confirm_poll_interval = float(parallel_cfg["fill_confirm_poll_interval_sec"])
        self.entry_api_timeout = float(parallel_cfg["entry_api_timeout_sec"])
        
        unwind_cfg = self.cfg["trading_rules"]["emergency_unwind"]
        self.unwind_max_attempts = int(unwind_cfg["max_attempts"])
        self.ws_verify_timeout = float(unwind_cfg["ws_verify_timeout_sec"])
        self.unwind_retry_pause = float(unwind_cfg.get("retry_pause_sec", 0.05))
        
        ban_cfg = self.cfg["trading_rules"]["ban_rules"]
        self.perm_ban_loss_pct = float(ban_cfg["perm_ban_loss_pct"])
        
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
        if ev_target:
            try:
                await asyncio.wait_for(ev_target.wait(), timeout=self.fill_confirm_timeout)
            except asyncio.TimeoutError:
                pass
                
        # Polling fallback
        while (time.perf_counter() - t0) < self.fill_confirm_timeout:
            pos = self.orders[self.target_ex].get_executed_position(self.native_target, self.side)
            if pos:
                filled_qty = pos.get("size", 0.0)
                if filled_qty > 0.0:
                    return pos, (filled_qty / req_qty if req_qty > 0 else 0.0)
            if self.fill_confirm_poll_interval > 0:
                await asyncio.sleep(self.fill_confirm_poll_interval)
            else:
                await asyncio.sleep(0.005)
                
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
        slip = float(self.cfg["trading_risks"][self.target_ex.lower()]["limit_slip_ratio"])
        
        if self.side == "LONG":
            order_side = "BUY"
            limit_price = entry_price * (1 + slip)
            position_side = "LONG"
        else:
            order_side = "SELL"
            limit_price = entry_price * (1 - slip)
            position_side = "SHORT"
        
        log(f"[{self.sym}] v9 Opening {self.side} on {self.target_ex} | "
            f"Price: {limit_price:.6f} (VWAP: {entry_price:.6f}) | "
            f"Net Spread: {self.engine_res.get('net_spread', 0)*100:.3f}%", level="INFO")
        
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
        
        self.exec_res = {
            "engine_res": self.engine_res,
            "target_ex": self.target_ex,
            "oracle_ex": self.oracle_ex,
            "side": self.side,
            "entry_price": filled_price,
            "qty": filled_qty,
            "executed_volume_rate": fill_rate,
            "net_spread": self.engine_res.get("net_spread"),
            "open_time": self.open_time,
            "open_time_ms": self.open_time_ms
        }
        
        self._set_state(PositionState.ACTIVE)
        if self.pm:
            self.pm.confirm_entry(self.oracle_ex, self.target_ex, self.sym, self.exec_res, self.open_time)
            
        log(f"[{self.sym}] Position opened! Filled: {filled_qty:.4f} @ {filled_price:.6f}", level="INFO")

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

    async def _wait_for_close_v9(self, ev_target: asyncio.Event) -> bool:
        t0 = time.perf_counter()
        
        if ev_target:
            try:
                await asyncio.wait_for(ev_target.wait(), timeout=self.ws_verify_timeout)
            except asyncio.TimeoutError:
                pass
                
        while (time.perf_counter() - t0) < self.ws_verify_timeout:
            pos = self.orders[self.target_ex].get_executed_position(self.native_target, self.side)
            if pos and pos.get("size", 0.0) == 0.0:
                return True
            await asyncio.sleep(0.005)
            
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
            self._finalize_close_v9(0.0, qty, reason)
            return True
        
        entry_price = self.exec_res.get("entry_price", self.engine_res["entry_price"])
        exit_price = exit_res.get("exit_price") or entry_price
        
        if reason in ("TTL_EXPIRED", "STOP_LOSS", "TTL_EXPIRED_NO_LIQUIDITY", "TTL_EXPIRED_STALE_DATA"):
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
        
        is_closed = await self._wait_for_close_v9(ev_target)
        
        if hasattr(self.orders[self.target_ex], "unsubscribe_position_update"):
            self.orders[self.target_ex].unsubscribe_position_update(self.native_target, pos_side)
        
        actual_exit_price = exit_price
        if hasattr(self.orders[self.target_ex], "get_last_close_price"):
            p = self.orders[self.target_ex].get_last_close_price(self.native_target)
            if p > 0:
                actual_exit_price = p
                
        if not is_closed:
            # REST fallback
            for attempt in range(self.unwind_max_attempts):
                rest_pos = await self.orders[self.target_ex].get_exact_position_guarded(self.native_target, pos_side)
                rem = rest_pos.get("size", 0.0)
                if rem <= 0:
                    break
                p = rest_pos.get("price", exit_price)
                if p <= 0: p = entry_price
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
            asyncio.create_task(self.on_settle_cb(
                sym=self.sym,
                route=self.route,
                target_ex=self.target_ex,
                oracle_ex=self.oracle_ex,
                side=self.side,
                entry_price=entry_price,
                exit_price=exit_price,
                actual_usd=actual_usd,
                reason=reason
            ))

    async def _emergency_unwind_single(self, ex: str, native_sym: str, qty: float, price: float, open_side: str, pos_side: str):
        self._set_state(PositionState.EMERGENCY_UNWIND)
        usd = qty * price
        reduce_side = "SELL" if open_side == "BUY" else "BUY"
        log(f"[{self.sym}] Immediate unwind {ex} ({qty} qty, {usd:.2f}$)...", level="WARNING")
        try:
            await self.orders[ex].place_order(native_sym, reduce_side, usd, price, order_type="MARKET", position_side=pos_side, reduce_only=True, is_full_unwind=True)
        except Exception as e:
            log(f"[{self.sym}] Unwind error on {ex}: {e}", level="ERROR")
