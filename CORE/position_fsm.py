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
        
        entry_cfg = self.cfg.get("trading_rules", {}).get("entry", {})
        target_entry_cfg = entry_cfg.get("target_entry_logic") or entry_cfg.get("parallel_entry_logic", {})
        ban_q = self.cfg.get("trading_rules", {}).get("ban_rules", {}).get("quarantine_sec", {})
        
        self.q_entry_error = float(ban_q.get("entry_error", 3600))
        self.q_zero_fill = float(ban_q.get("zero_fill", 10))
        
        target_exit_cfg = self.cfg.get("trading_rules", {}).get("exit", {}).get("target_exit", {})
        decay_map = target_exit_cfg.get("decay_map", [])
        derived_ttl = None
        for rule in decay_map:
            ratio = rule.get("min_profit_ratio")
            spread = rule.get("target_spread")
            if (ratio is None and spread is None) or (isinstance(ratio, (int, float)) and ratio <= -900.0):
                derived_ttl = float(rule.get("after_sec", 60.0))
                break
        if derived_ttl is not None:
            self.ttl_sec = derived_ttl
        elif "ttl_sec" in target_exit_cfg and target_exit_cfg["ttl_sec"] is not None:
            self.ttl_sec = float(target_exit_cfg["ttl_sec"])
        elif decay_map:
            self.ttl_sec = float(decay_map[-1].get("after_sec", 60.0))
        else:
            self.ttl_sec = 60.0
        self.exit_order_type = target_exit_cfg.get("exit_order_type", "LIMIT_IOC")
        self.exit_slip_ratio = float(target_exit_cfg.get("exit_slip_ratio", 0.001))
        
        timeout_cfg = target_entry_cfg.get("fill_confirm_timeout_sec", {})
        pair_key1 = f"{self.target_ex}_{self.oracle_ex}".upper()
        pair_key2 = f"{self.oracle_ex}_{self.target_ex}".upper()
        if isinstance(timeout_cfg, dict):
            if pair_key1 in timeout_cfg:
                self.fill_confirm_timeout = float(timeout_cfg[pair_key1])
            elif pair_key2 in timeout_cfg:
                self.fill_confirm_timeout = float(timeout_cfg[pair_key2])
            else:
                self.fill_confirm_timeout = 0.5  # default
        else:
            self.fill_confirm_timeout = float(timeout_cfg) if timeout_cfg else 0.5
        
        self.fill_confirm_poll_interval = float(target_entry_cfg.get("fill_confirm_poll_interval_sec", 0.0))
        self.entry_api_timeout = float(target_entry_cfg.get("entry_api_timeout_sec", 5.0))
        
        unwind_cfg = self.cfg.get("trading_rules", {}).get("emergency_unwind", {})
        self.unwind_max_attempts = int(unwind_cfg.get("max_attempts", 2))
        self.ws_verify_timeout = float(unwind_cfg.get("ws_verify_timeout_sec", 0.3))
        self.unwind_retry_pause = float(unwind_cfg.get("retry_pause_sec", 0.05))
        
        exit_cfg = self.cfg.get("trading_rules", {}).get("exit", {})
        close_timeout_cfg = (
            exit_cfg.get("market_close_confirm_timeout_sec")
            or exit_cfg.get("close_confirm_timeout_sec", {})
        )
        if isinstance(close_timeout_cfg, dict):
            self.close_confirm_timeout = float(close_timeout_cfg.get(pair_key1) or close_timeout_cfg.get(pair_key2) or 1.8)
        elif close_timeout_cfg:
            self.close_confirm_timeout = float(close_timeout_cfg)
        else:
            self.close_confirm_timeout = 1.8
        
        ban_cfg = self.cfg.get("trading_rules", {}).get("ban_rules", {})
        self.perm_ban_loss_pct = float(ban_cfg.get("perm_ban_loss_pct", 0.0075))
        
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
        
        target_fee = float(self.cfg.get("trading_risks", {}).get(self.target_ex.lower(), {}).get("taker_fee", 0.0006))
        
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
            "net_spread": self.engine_res.get("net_spread", 0.0),
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
                
        while (time.perf_counter() - t0) < wait_timeout:
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
            if self.pm:
                self.pm.confirm_exit(self.route, self.sym)
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {
                    "route": self.route, "sym": self.sym, "reason": reason
                }))
            return True
        
        entry_price = self.exec_res.get("entry_price") or self.engine_res.get("entry_price", 0.0)
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
        
        if o_type == "LIMIT_IOC":
            # For IOC, match engine execution is instant. If WS event not received in 0.2s,
            # remainder was cancelled by exchange -> immediately fall back without 1.8s lag
            is_closed = await self._wait_for_close_v9(ev_target, timeout=min(0.2, self.close_confirm_timeout))
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

    async def _run_single_leg_exposure(self, long_qty: float, short_qty: float, long_entry_price: float, short_entry_price: float):
        """DEPRECATED v8 compatibility method for tests."""
        self._set_state(PositionState.ABORTED)
        ex = self.oracle_ex if long_qty > 0 else self.target_ex
        qty = long_qty if long_qty > 0 else short_qty
        price = long_entry_price if long_qty > 0 else short_entry_price
        side = "LONG" if long_qty > 0 else "SHORT"
        native = self.coin_to_native.get(self.sym, {}).get(ex, self.sym)
        await self._emergency_unwind_single(ex, native, qty, price, "BUY" if side == "LONG" else "SELL", side)
