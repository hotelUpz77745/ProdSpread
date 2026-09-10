# File: CORE/position_fsm.py
# Role: Конечный автомат состояний (FSM) жизненного цикла торговой позиции (HFT)

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
    SUBMITTING = "SUBMITTING"
    RESTING_BOOK = "RESTING_BOOK"
    CANCELLING = "CANCELLING"
    VERIFYING_FILL = "VERIFYING_FILL"
    ACTIVE_HEDGED = "ACTIVE_HEDGED"
    EMERGENCY_UNWIND = "EMERGENCY_UNWIND"
    CLOSING = "CLOSING"
    SETTLED = "SETTLED"
    ABORTED = "ABORTED"
    FAILED = "FAILED"


class PositionFSM:
    def __init__(
        self,
        sym: str,
        route: str,
        long_ex: str,
        short_ex: str,
        engine_res: Dict[str, Any],
        cfg: Dict[str, Any],
        orders: Dict[str, Any],
        coin_to_native: Dict[str, Dict[str, str]],
        pm: Any,
        writer: Optional[asyncio.StreamWriter],
        ban_coin_cb: Any,
        on_settle_cb: Any = None
    ):
        self.sym = sym
        self.route = route
        self.long_ex = long_ex
        self.short_ex = short_ex
        self.engine_res = engine_res
        self.cfg = cfg
        self.orders = orders
        self.coin_to_native = coin_to_native
        self.pm = pm
        self.writer = writer
        self.ban_coin_cb = ban_coin_cb
        self.on_settle_cb = on_settle_cb

        self.native_long = self.coin_to_native[sym][long_ex] if sym in self.coin_to_native and long_ex in self.coin_to_native[sym] else sym
        self.native_short = self.coin_to_native[sym][short_ex] if sym in self.coin_to_native and short_ex in self.coin_to_native[sym] else sym

        self.state = PositionState.IDLE
        self.engine = TradingEngine(self.cfg, {0: "BINANCE", 1: "KUCOIN", 2: "OKX", 3: "BITGET"})
        entry_cfg = self.cfg["trading_rules"]["entry"]
        lead_cfg = entry_cfg.get("phase1_lead_leg", entry_cfg)
        hedge_cfg = entry_cfg.get("phase3_hedge_leg", entry_cfg)

        self.order_policy = entry_cfg.get("order_execution_type", "ASYMMETRIC_LIMIT_IOC").upper()
        self.min_fill_rate = float(hedge_cfg.get("min_hedge_fill_rate", entry_cfg.get("min_fill_rate", 0.75)))

        # Параметры подтверждения налива (из phase1_lead_leg с fallback на flat entry)
        timeout_cfg = lead_cfg.get("fill_confirm_timeout_sec", entry_cfg.get("fill_confirm_timeout_sec"))
        if isinstance(timeout_cfg, dict):
            pair_key1 = f"{long_ex}_{short_ex}".upper()
            pair_key2 = f"{short_ex}_{long_ex}".upper()
            if pair_key1 in timeout_cfg:
                self.fill_confirm_timeout = float(timeout_cfg[pair_key1])
            elif pair_key2 in timeout_cfg:
                self.fill_confirm_timeout = float(timeout_cfg[pair_key2])
            else:
                raise KeyError(
                    f"Параметры fill_confirm_timeout_sec не содержат пару {pair_key1} или {pair_key2} в cfg.json"
                )
        else:
            self.fill_confirm_timeout = float(timeout_cfg)
            
        self.fill_confirm_poll_interval = float(lead_cfg.get("fill_confirm_poll_interval_sec", entry_cfg.get("fill_confirm_poll_interval_sec", 0.0)))

        # Параметры подтверждения закрытия позиции (из секции exit с fallback на entry * 2)
        exit_timeout_cfg = self.cfg["trading_rules"].get("exit", {}).get("close_confirm_timeout_sec")
        if exit_timeout_cfg:
            if isinstance(exit_timeout_cfg, dict):
                pair_key1 = f"{long_ex}_{short_ex}".upper()
                pair_key2 = f"{short_ex}_{long_ex}".upper()
                if pair_key1 in exit_timeout_cfg:
                    self.close_confirm_timeout = float(exit_timeout_cfg[pair_key1])
                elif pair_key2 in exit_timeout_cfg:
                    self.close_confirm_timeout = float(exit_timeout_cfg[pair_key2])
                else:
                    self.close_confirm_timeout = self.fill_confirm_timeout * 2.0
            else:
                self.close_confirm_timeout = float(exit_timeout_cfg)
        else:
            self.close_confirm_timeout = self.fill_confirm_timeout * 2.0

        # Параметры аварийного сброса (из конфига строго через [''])
        unwind_cfg = self.cfg["trading_rules"]["emergency_unwind"]
        self.unwind_max_attempts = int(unwind_cfg["max_attempts"])
        self.unwind_retry_pause = float(unwind_cfg["retry_pause_sec"])

        self.exec_res: Dict[str, Any] = {}
        self.long_pos: Dict[str, float] = {"size": 0.0, "price": 0.0}
        self.short_pos: Dict[str, float] = {"size": 0.0, "price": 0.0}
        self.open_time: float = 0.0
        self.open_time_ms: int = 0
        self.ws_fill_timings: Dict[str, float] = {}
        self.ws_close_timings: Dict[str, float] = {}

    def _set_state(self, new_state: PositionState):
        prev = self.state
        self.state = new_state
        log(f"[{self.sym}][FSM] {prev} -> {new_state}", level="DEBUG")

    async def _wait_for_fill_confirmation(
        self,
        req_long_qty: float,
        req_short_qty: float,
        ev_long: Optional[asyncio.Event] = None,
        ev_short: Optional[asyncio.Event] = None,
    ) -> Tuple[Dict[str, float], Dict[str, float], float, float]:
        """
        Реактивный опрос локального WS-кэша позиций до подтверждения налива обеих ног (min_fill_rate)
        или истечения предельного таймаута fill_confirm_timeout_sec.
        Работает по предикатному Event-driven циклу без джиттера системных таймеров (0.05-0.15 мс).
        """
        start_time = time.perf_counter()
        deadline = start_time + self.fill_confirm_timeout
        l_rate = 0.0
        s_rate = 0.0
        self.ws_fill_timings = {self.long_ex: 0.0, self.short_ex: 0.0}

        while True:
            # Безопасное чтение из локального WS-кэша
            if self.long_ex in self.orders:
                try:
                    p_long = self.orders[self.long_ex].get_executed_position(self.native_long, "LONG")
                    if p_long and p_long.get("size", 0.0) > 0:
                        self.long_pos = p_long
                except Exception as e:
                    log(f"[{self.sym}] Ошибка чтения WS-кэша {self.long_ex}: {e}", level="WARNING")

            if self.short_ex in self.orders:
                try:
                    p_short = self.orders[self.short_ex].get_executed_position(self.native_short, "SHORT")
                    if p_short and p_short.get("size", 0.0) > 0:
                        self.short_pos = p_short
                except Exception as e:
                    log(f"[{self.sym}] Ошибка чтения WS-кэша {self.short_ex}: {e}", level="WARNING")

            l_size = self.long_pos.get("size", 0.0)
            s_size = self.short_pos.get("size", 0.0)

            l_rate = (l_size / req_long_qty) if req_long_qty > 0 else 0.0
            s_rate = (s_size / req_short_qty) if req_short_qty > 0 else 0.0

            now = time.perf_counter()
            elapsed_now_ms = (now - start_time) * 1000.0

            if req_long_qty > 0 and l_rate >= self.min_fill_rate and self.ws_fill_timings.get(self.long_ex, 0.0) == 0.0:
                self.ws_fill_timings[self.long_ex] = elapsed_now_ms
            if req_short_qty > 0 and s_rate >= self.min_fill_rate and self.ws_fill_timings.get(self.short_ex, 0.0) == 0.0:
                self.ws_fill_timings[self.short_ex] = elapsed_now_ms

            # Предикат готовности: проверяем только запрашиваемые ноги (> 0)
            l_ok = (l_rate >= self.min_fill_rate) if req_long_qty > 0 else True
            s_ok = (s_rate >= self.min_fill_rate) if req_short_qty > 0 else True

            if l_ok and s_ok:
                leg_desc = "Обе ноги" if (req_long_qty > 0 and req_short_qty > 0) else ("Lead" if self.state == PositionState.VERIFYING_FILL else "Hedge")
                log(f"[{self.sym}] 🚀 {leg_desc} подтвержден(ы) реактивно за {elapsed_now_ms:.2f} мс (L:{l_rate*100:.1f}%, S:{s_rate*100:.1f}%)", level="INFO")
                break

            remaining = deadline - now
            if remaining <= 0:
                log(f"[{self.sym}] ⏱ Таймаут подтверждения налива ({elapsed_now_ms:.1f} мс). L:{l_rate*100:.1f}%, S:{s_rate*100:.1f}%", level="WARNING")
                break

            # Если пуш уже успел взвести событие до входа в ожидание
            if (ev_long and ev_long.is_set()) or (ev_short and ev_short.is_set()):
                if ev_long:
                    ev_long.clear()
                if ev_short:
                    ev_short.clear()
                continue

            # Реактивное ожидание пуша от любой ноги с защитным таймаутом
            wait_tasks = []
            if ev_long:
                wait_tasks.append(asyncio.create_task(ev_long.wait()))
            if ev_short:
                wait_tasks.append(asyncio.create_task(ev_short.wait()))

            if not wait_tasks:
                await asyncio.sleep(self.fill_confirm_poll_interval)
                continue

            done, pending = await asyncio.wait(
                wait_tasks,
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            if ev_long:
                ev_long.clear()
            if ev_short:
                ev_short.clear()

        return self.long_pos, self.short_pos, l_rate, s_rate

    async def _wait_for_close_confirmation(
        self,
        ev_long: Optional[asyncio.Event] = None,
        ev_short: Optional[asyncio.Event] = None,
    ) -> Tuple[bool, float, float]:
        """
        Реактивный опрос локального WS-кэша позиций до подтверждения обнуления обеих ног (size == 0.0)
        или истечения предельного таймаута close_confirm_timeout_sec.
        Возвращает (is_closed, close_price_long, close_price_short).
        """
        start_time = time.perf_counter()
        deadline = start_time + self.close_confirm_timeout
        close_p_long = 0.0
        close_p_short = 0.0
        self.ws_close_timings = {self.long_ex: 0.0, self.short_ex: 0.0}

        while True:
            l_closed = True
            s_closed = True

            if self.long_ex in self.orders:
                try:
                    p_long = self.orders[self.long_ex].get_executed_position(self.native_long, "LONG")
                    if p_long and p_long.get("size", 0.0) > 0:
                        l_closed = False
                    if hasattr(self.orders[self.long_ex], "get_last_close_price"):
                        p = self.orders[self.long_ex].get_last_close_price(self.native_long)
                        if p > 0:
                            close_p_long = p
                except Exception as e:
                    log(f"[{self.sym}] Ошибка чтения WS-кэша закрытия {self.long_ex}: {e}", level="WARNING")

            if self.short_ex in self.orders:
                try:
                    p_short = self.orders[self.short_ex].get_executed_position(self.native_short, "SHORT")
                    if p_short and p_short.get("size", 0.0) > 0:
                        s_closed = False
                    if hasattr(self.orders[self.short_ex], "get_last_close_price"):
                        p = self.orders[self.short_ex].get_last_close_price(self.native_short)
                        if p > 0:
                            close_p_short = p
                except Exception as e:
                    log(f"[{self.sym}] Ошибка чтения WS-кэша закрытия {self.short_ex}: {e}", level="WARNING")

            now = time.perf_counter()
            elapsed_now_ms = (now - start_time) * 1000.0

            if l_closed and self.ws_close_timings.get(self.long_ex, 0.0) == 0.0:
                self.ws_close_timings[self.long_ex] = elapsed_now_ms
            if s_closed and self.ws_close_timings.get(self.short_ex, 0.0) == 0.0:
                self.ws_close_timings[self.short_ex] = elapsed_now_ms

            if l_closed and s_closed:
                log(f"[{self.sym}] 🚀 Обе ноги подтверждены закрытыми реактивно за {elapsed_now_ms:.2f} мс (0.0)", level="INFO")
                return True, close_p_long, close_p_short

            remaining = deadline - now
            if remaining <= 0:
                log(f"[{self.sym}] ⏱ Таймаут подтверждения закрытия по WS ({elapsed_now_ms:.1f} мс), переход к контрольной проверке...", level="WARNING")
                return False, close_p_long, close_p_short

            if (ev_long and ev_long.is_set()) or (ev_short and ev_short.is_set()):
                if ev_long:
                    ev_long.clear()
                if ev_short:
                    ev_short.clear()
                continue

            wait_tasks = []
            if ev_long:
                wait_tasks.append(asyncio.create_task(ev_long.wait()))
            if ev_short:
                wait_tasks.append(asyncio.create_task(ev_short.wait()))

            if not wait_tasks:
                await asyncio.sleep(self.fill_confirm_poll_interval)
                continue

            done, pending = await asyncio.wait(
                wait_tasks,
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            if ev_long:
                ev_long.clear()
            if ev_short:
                ev_short.clear()

    async def run_open(self) -> bool:
        """
        Запуск пайплайна открытия позиции (ASYMMETRIC_LIMIT_IOC):
        IDLE -> SUBMITTING (Lead Leg) -> VERIFYING_FILL -> ACTIVE_HEDGED / EMERGENCY_UNWIND / ABORTED
        """
        self._set_state(PositionState.SUBMITTING)
        
        entry_cfg = self.cfg["trading_rules"]["entry"]
        roles_cfg = entry_cfg["exchange_roles"].get(self.route)
        if not roles_cfg:
            log(f"[{self.sym}] ⛔ Нет ролей для связки {self.route} (ASYMMETRIC_LIMIT_IOC невозможен).", level="WARNING")
            self._set_state(PositionState.IDLE)
            self._notify_pos_failed("NO_ROLES")
            return False
            
        lead_ex = roles_cfg["lead"]
        hedge_ex = roles_cfg["hedge"]
        
        # Получаем фазовые блоки настроек
        phase1_cfg = entry_cfg.get("phase1_lead_leg", entry_cfg)
        phase2_cfg = entry_cfg.get("phase2_lead_validation", entry_cfg)
        phase3_cfg = entry_cfg.get("phase3_hedge_leg", entry_cfg)
        phase4_cfg = entry_cfg.get("phase4_resolution", entry_cfg)
        signal_cfg = entry_cfg.get("signal_filters", entry_cfg)
        quarantine_cfg = entry_cfg.get("quarantine_durations_sec", {})
        
        # Mapping to long/short roles
        is_lead_long = (lead_ex == self.long_ex)
        native_lead = self.native_long if is_lead_long else self.native_short
        native_hedge = self.native_short if is_lead_long else self.native_long
        
        lead_side = "BUY" if is_lead_long else "SELL"
        hedge_side = "SELL" if is_lead_long else "BUY"
        
        lead_pos_side = "LONG" if is_lead_long else "SHORT"
        hedge_pos_side = "SHORT" if is_lead_long else "LONG"
        
        spread_val = self.engine_res.get("net_spread", self.engine_res.get("vwap_spread", 0.0))
        gross_val = self.engine_res.get("vwap_spread", 0.0)
        
        log(f"[{self.sym}] Открываем (ASYMMETRIC_LIMIT_IOC): Lead={lead_ex} | Hedge={hedge_ex} | Net Spread: {spread_val * 100:.2f}%", level="INFO")
        
        size_long_usd = float(self.cfg["trading_risks"][self.long_ex.lower()]["trade_size_usd"])
        size_short_usd = float(self.cfg["trading_risks"][self.short_ex.lower()]["trade_size_usd"])
        
        size_lead_usd = size_long_usd if is_lead_long else size_short_usd
        price_lead_calc = self.engine_res.get("long_avg_price", 0.0) if is_lead_long else self.engine_res.get("short_avg_price", 0.0)
        
        lead_max_slip = float(phase1_cfg.get("max_slippage_pct", entry_cfg.get("lead_max_slippage_pct", 0.0005)))
        if is_lead_long:
            price_lead_limit = price_lead_calc * (1 + lead_max_slip)
        else:
            price_lead_limit = price_lead_calc * (1 - lead_max_slip)
            
        # =========================================================================
        # PHASE 1: Выстрел в Lead Leg
        # =========================================================================
        ev_lead = None
        if lead_ex in self.orders and hasattr(self.orders[lead_ex], "subscribe_position_update"):
            ev_lead = self.orders[lead_ex].subscribe_position_update(native_lead, lead_pos_side)
            
        try:
            log(f"[{self.sym}] Phase 1: Sending LIMIT_IOC to Lead ({lead_ex}) | Price: {price_lead_limit:.6f}", level="INFO")
            await self.orders[lead_ex].place_order(
                native_lead, lead_side, size_lead_usd, price_lead_limit, order_type="LIMIT_IOC", position_side=lead_pos_side
            )
        except Exception as e:
            log(f"[{self.sym}] 🚨 Ошибка входа Lead Leg: {e}.", level="ERROR")
            if hasattr(self.orders[lead_ex], "unsubscribe_position_update"):
                self.orders[lead_ex].unsubscribe_position_update(native_lead, lead_pos_side)
            self.ban_coin_cb(self.sym, reason=str(e), duration_sec=3600)
            self._notify_pos_failed(f"LEAD_ERR: {e}")
            return False
            
        req_lead_qty = self.engine_res.get("long_qty", 0.0) if is_lead_long else self.engine_res.get("short_qty", 0.0)
        
        # Ожидание налива с таймаутом (используем тот же wait, но только для одной ноги)
        self._set_state(PositionState.VERIFYING_FILL)
        l_pos, s_pos, l_rate, s_rate = await self._wait_for_fill_confirmation(
            req_lead_qty if is_lead_long else 0.0,
            req_lead_qty if not is_lead_long else 0.0,
            ev_long=ev_lead if is_lead_long else None,
            ev_short=ev_lead if not is_lead_long else None
        )
        if hasattr(self.orders[lead_ex], "unsubscribe_position_update"):
            self.orders[lead_ex].unsubscribe_position_update(native_lead, lead_pos_side)
            
        lead_pos = l_pos if is_lead_long else s_pos
        lead_qty_actual = lead_pos.get("size", 0.0)
        lead_price_actual = lead_pos.get("price", 0.0)
        lead_rate_actual = l_rate if is_lead_long else s_rate

        log(f"[{self.sym}] Phase 1: Факт налива Lead ({lead_ex}): {lead_qty_actual:.4f}/{req_lead_qty:.4f} ({lead_rate_actual*100:.1f}%) @ {lead_price_actual:.6f}", level="INFO")
        
        if lead_qty_actual <= 0.0:
            zero_fill_sec = float(phase1_cfg.get("quarantine_zero_fill_sec", quarantine_cfg.get("zero_fill", 300)))
            log(f"[{self.sym}] Ветка Б (Zero Fill): Lead Leg не налился. Карантин {zero_fill_sec:.0f}с.", level="WARNING")
            self.ban_coin_cb(self.sym, reason="Zero Fill (Lead Leg)", duration_sec=zero_fill_sec)
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("ZERO_FILL")
            return False
            
        # =========================================================================
        # PHASE 2: Validation (Branch A)
        # =========================================================================
        notional_usd = lead_qty_actual * lead_price_actual
        min_notional_usd = float(phase2_cfg.get("min_notional_usd", 5.0))
        hedge_failed_sec = float(phase3_cfg.get("quarantine_hedge_failed_sec", quarantine_cfg.get("hedge_failed", 1800)))
        
        if notional_usd < min_notional_usd:
            log(f"[{self.sym}] Lead Notional < {min_notional_usd}$ ({notional_usd:.2f}$). Сброс ноги, карантин.", level="WARNING")
            await self._emergency_unwind_single(lead_ex, native_lead, lead_qty_actual, lead_price_actual, lead_side, lead_pos_side)
            self.ban_coin_cb(self.sym, reason="Min Notional Failed", duration_sec=hedge_failed_sec)
            self._set_state(PositionState.ABORTED)
            return False
            
        # Проверка жизнеспособности спреда через evaluate_hedge_entry с порогом безубытка (Soft Floor)
        min_acceptable_net_spread = float(phase2_cfg.get("min_acceptable_net_spread", 0.0000))
        hedge_book = self.engine_res.get("hedge_book")
        price_hedge_live = self.engine_res.get("short_avg_price", 0.0) if is_lead_long else self.engine_res.get("long_avg_price", 0.0)
        
        is_viable, viable_eval = self.engine.evaluate_hedge_entry(
            hedge_book=hedge_book,
            lead_direction=lead_side,
            lead_price=lead_price_actual,
            lead_qty=lead_qty_actual,
            target_net_spread=min_acceptable_net_spread,
            lead_ex=lead_ex,
            hedge_ex=hedge_ex,
            live_price_fallback=price_hedge_live
        )
        
        if not is_viable:
            drift_quarantine_sec = float(phase2_cfg.get("quarantine_model_drift_sec", quarantine_cfg.get("model_drift", 3600)))
            log(f"[{self.sym}] Ветка А1 (Spread Collapsed): Невозможно захеджировать с мин. спредом {min_acceptable_net_spread*100:.2f}%. {viable_eval.get('reason')}. Сброс ноги.", level="WARNING")
            await self._emergency_unwind_single(lead_ex, native_lead, lead_qty_actual, lead_price_actual, lead_side, lead_pos_side)
            self.ban_coin_cb(self.sym, reason="Spread Collapsed", duration_sec=drift_quarantine_sec)
            self._set_state(PositionState.ABORTED)
            return False
            
        # =========================================================================
        # PHASE 3: Hedge Leg (Branch A2)
        # =========================================================================
        hedge_decay_map = phase3_cfg.get("decay_map", entry_cfg.get("hedge_decay_map", [{"iter": 0, "decay_rate": 1.0, "timeout_ms": 300}]))
        hedge_qty_actual = 0.0
        req_hedge_qty = lead_qty_actual # Мы хотим налить ровно столько, сколько налили в Lead
        spread_entry = float(signal_cfg.get("spread_entry", entry_cfg.get("spread_entry", 0.008)))
        
        for step in hedge_decay_map:
            decay_rate = float(step.get("decay_rate", 1.0))
            timeout_ms = int(step.get("timeout_ms", 300))
            
            # Таргет спреда для текущей итерации дожима
            target_step_spread = max(min_acceptable_net_spread, spread_entry * decay_rate)
            
            qty_needed = req_hedge_qty - hedge_qty_actual
            if qty_needed <= 0.001:
                break
                
            is_step_valid, hedge_eval = self.engine.evaluate_hedge_entry(
                hedge_book=hedge_book,
                lead_direction=lead_side,
                lead_price=lead_price_actual,
                lead_qty=qty_needed,
                target_net_spread=target_step_spread,
                lead_ex=lead_ex,
                hedge_ex=hedge_ex,
                live_price_fallback=price_hedge_live
            )
            
            price_hedge_limit = hedge_eval["order_price"]
            usd_needed = qty_needed * price_hedge_limit
            log(f"[{self.sym}] Phase 3 (Iter {step.get('iter')}): Hedge LIMIT_IOC | Qty: {qty_needed:.4f} | Limit: {price_hedge_limit:.6f} (SnapVWAP: {hedge_eval.get('vwap_price', 0.0):.6f}) | Net: {hedge_eval.get('net_spread', 0.0)*100:+.3f}% (Target: {target_step_spread*100:+.3f}%)", level="INFO")
            
            ev_hedge = None
            if hedge_ex in self.orders and hasattr(self.orders[hedge_ex], "subscribe_position_update"):
                ev_hedge = self.orders[hedge_ex].subscribe_position_update(native_hedge, hedge_pos_side)
                
            try:
                await self.orders[hedge_ex].place_order(
                    native_hedge, hedge_side, usd_needed, price_hedge_limit, order_type="LIMIT_IOC", position_side=hedge_pos_side, exact_qty=qty_needed
                )
            except Exception as e:
                log(f"[{self.sym}] Ошибка отправки Hedge: {e}", level="WARNING")
                if hasattr(self.orders[hedge_ex], "unsubscribe_position_update"):
                    self.orders[hedge_ex].unsubscribe_position_update(native_hedge, hedge_pos_side)
                continue
                
            # Ждем с таймаутом текущей итерации
            _old_timeout = self.fill_confirm_timeout
            self.fill_confirm_timeout = timeout_ms / 1000.0
            
            l_pos, s_pos, l_rate, s_rate = await self._wait_for_fill_confirmation(
                req_hedge_qty if not is_lead_long else 0.0,
                req_hedge_qty if is_lead_long else 0.0,
                ev_long=ev_hedge if not is_lead_long else None,
                ev_short=ev_hedge if is_lead_long else None
            )
            self.fill_confirm_timeout = _old_timeout
            
            if hasattr(self.orders[hedge_ex], "unsubscribe_position_update"):
                self.orders[hedge_ex].unsubscribe_position_update(native_hedge, hedge_pos_side)
                
            hedge_pos = l_pos if not is_lead_long else s_pos
            hedge_qty_actual = hedge_pos.get("size", 0.0)
            hedge_rate_actual = (hedge_qty_actual / req_hedge_qty * 100.0) if req_hedge_qty > 0 else 0.0
            log(f"[{self.sym}] Phase 3 (Iter {step.get('iter')}): Факт налива Hedge ({hedge_ex}): {hedge_qty_actual:.4f}/{req_hedge_qty:.4f} ({hedge_rate_actual:.1f}%)", level="INFO")
            
            if hedge_qty_actual >= req_hedge_qty * 0.99:
                break
                
        # =========================================================================
        # PHASE 4: Resolution
        # =========================================================================
        hedge_fill_rate = hedge_qty_actual / req_hedge_qty if req_hedge_qty > 0 else 0.0
        min_hedge_rate = float(phase3_cfg.get("min_hedge_fill_rate", entry_cfg.get("min_hedge_fill_rate", 0.75)))
        
        if hedge_fill_rate >= min_hedge_rate:
            log(f"[{self.sym}] Ветка А3: Частичный/Полный налив Hedge ({hedge_fill_rate*100:.1f}%). Выравнивание объема.", level="INFO")
            delta_qty = lead_qty_actual - hedge_qty_actual
            trim_excess = bool(phase4_cfg.get("trim_excess_lead", True))
            if trim_excess and delta_qty > 0.01: # Подрезаем излишек Lead Leg (MARKET reduceOnly=True)
                delta_usd = delta_qty * lead_price_actual
                reduce_side = "SELL" if is_lead_long else "BUY"
                log(f"[{self.sym}] Подрезка излишка Lead Leg на {delta_qty:.4f}", level="WARNING")
                try:
                    await self.orders[lead_ex].place_order(
                        native_lead, reduce_side, delta_usd, lead_price_actual, order_type="MARKET", position_side=lead_pos_side, reduce_only=True, exact_qty=delta_qty
                    )
                except Exception as e:
                    log(f"[{self.sym}] Ошибка подрезки Lead Leg: {e}", level="ERROR")
            
            self._finalize_open(
                hedge_qty_actual if not is_lead_long else lead_qty_actual,
                hedge_qty_actual if is_lead_long else lead_qty_actual,
                lead_price_actual if is_lead_long else self.long_pos.get("price", 0.0),
                lead_price_actual if not is_lead_long else self.short_pos.get("price", 0.0)
            )
            return True
        else:
            log(f"[{self.sym}] Ветка А4 (Hedge Failed): Налив {hedge_fill_rate*100:.1f}% < {min_hedge_rate*100:.1f}%. Полный сброс.", level="WARNING")
            await self._emergency_unwind()
            hedge_failed_sec = float(phase3_cfg.get("quarantine_hedge_failed_sec", quarantine_cfg.get("hedge_failed", 1800)))
            self.ban_coin_cb(self.sym, reason="Hedge Failed", duration_sec=hedge_failed_sec)
            self._set_state(PositionState.ABORTED)
            return False

    async def _emergency_unwind_single(self, ex: str, native_sym: str, qty: float, price: float, side: str, pos_side: str):
        self._set_state(PositionState.EMERGENCY_UNWIND)
        usd = qty * price
        reduce_side = "SELL" if side == "BUY" else "BUY"
        log(f"[{self.sym}] Мгновенный сброс {ex} ({qty} шт, {usd:.2f}$)...", level="WARNING")
        try:
            await self.orders[ex].place_order(native_sym, reduce_side, usd, price, order_type="MARKET", position_side=pos_side)
        except Exception as e:
            log(f"[{self.sym}] Ошибка сброса {ex}: {e}", level="ERROR")
            
        self._notify_pos_failed("UNWIND_SINGLE")

    def _notify_pos_failed(self, reason: str):
        if self.pm:
            self.pm.rollback_entry(self.long_ex, self.short_ex, self.sym)
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                "route": self.route,
                "sym": self.sym,
                "long_ex": self.long_ex,
                "short_ex": self.short_ex,
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
            
    def _finalize_open(self, qty_long: float, qty_short: float, p_long: float, p_short: float):
        self.open_time = time.time()
        self.open_time_ms = int(self.open_time * 1000)
        
        entry_fee_l = float(self.cfg["trading_risks"][self.long_ex.lower()]["taker_fee"])
        entry_fee_s = float(self.cfg["trading_risks"][self.short_ex.lower()]["taker_fee"])
        entry_comm = entry_fee_l + entry_fee_s
        
        actual_gross_spread = (p_short - p_long) / p_long if p_long > 0 else 0.0
        actual_net_spread = actual_gross_spread - entry_comm
        
        # Если было подрезание, всегда используем extreme_decay
        use_extreme_decay = True
        
        self.exec_res = {
            "engine_res": self.engine_res,
            "long_ex": self.long_ex,
            "short_ex": self.short_ex,
            "entry_long_price": p_long,
            "entry_short_price": p_short,
            "actual_long_price": p_long,
            "actual_short_price": p_short,
            "actual_gross_spread": actual_gross_spread,
            "actual_net_spread": actual_net_spread,
            "use_extreme_decay": use_extreme_decay,
            "long_executed_volume_rate": 1.0,
            "short_executed_volume_rate": 1.0,
            "open_time": self.open_time,
            "open_time_ms": self.open_time_ms
        }
        
        self._set_state(PositionState.ACTIVE_HEDGED)
        if self.pm:
            self.pm.confirm_entry(self.long_ex, self.short_ex, self.sym, self.exec_res, self.open_time)
            
        log(f"[{self.sym}] 🟢 Позиция открыта! Факт Net Spread: {actual_net_spread*100:.3f}%", level="INFO")

        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_OPENED", {
                "route": self.route,
                "sym": self.sym,
                "exec_res": self.exec_res,
                "open_time": self.open_time
            }))

    async def _emergency_unwind(self):
        """
        Мгновенный 1-Shot HFT Market Kill-Switch.
        Поскольку ордера бьют строго MARKET, при сбое входа не крутятся медленные циклы:
        1. Если одна нога успела налиться, мгновенно выстреливаем 1 встречный MARKET-ордер на ее ликвидацию.
        2. Подтверждаем обнуление по WS за 15-30 мс (с аварийным REST только при таймауте).
        """
        self._set_state(PositionState.EMERGENCY_UNWIND)
        log(f"[{self.sym}] 🚨 Запуск 1-Shot Market Kill-Switch (ликвидация асимметрии входа)...", level="WARNING")

        l_size = self.long_pos.get("size", 0.0)
        s_size = self.short_pos.get("size", 0.0)

        # Контрольное чтение локального WS-кэша
        if l_size <= 0 and self.long_ex in self.orders:
            p_long = self.orders[self.long_ex].get_executed_position(self.native_long, "LONG")
            if p_long and p_long.get("size", 0.0) > 0:
                l_size = p_long["size"]
                self.long_pos = p_long

        if s_size <= 0 and self.short_ex in self.orders:
            p_short = self.orders[self.short_ex].get_executed_position(self.native_short, "SHORT")
            if p_short and p_short.get("size", 0.0) > 0:
                s_size = p_short["size"]
                self.short_pos = p_short

        kill_tasks = []
        if l_size > 0 and self.long_ex in self.orders:
            p = self.long_pos.get("price", 0.0) or self.engine_res.get("long_avg_price", 1.0)
            usd = l_size * p
            log(f"[{self.sym}] ⚡ Мгновенный сброс зависшего лонга ({l_size} шт, {usd:.2f}$) на {self.long_ex}...", level="WARNING")
            kill_tasks.append(self.orders[self.long_ex].place_order(
                self.native_long, "SELL", usd, p, order_type="MARKET", position_side="LONG"
            ))

        if s_size > 0 and self.short_ex in self.orders:
            p = self.short_pos.get("price", 0.0) or self.engine_res.get("short_avg_price", 1.0)
            usd = s_size * p
            log(f"[{self.sym}] ⚡ Мгновенный сброс зависшего шорта ({s_size} шт, {usd:.2f}$) на {self.short_ex}...", level="WARNING")
            kill_tasks.append(self.orders[self.short_ex].place_order(
                self.native_short, "BUY", usd, p, order_type="MARKET", position_side="SHORT"
            ))

        if kill_tasks:
            await asyncio.gather(*kill_tasks, return_exceptions=True)

        # Быстрая проверка обнуления по WS (до 300 мс)
        is_flat = False
        t_deadline = time.perf_counter() + 0.3
        while time.perf_counter() < t_deadline:
            l_flat = True
            s_flat = True
            if self.long_ex in self.orders:
                p = self.orders[self.long_ex].get_executed_position(self.native_long, "LONG")
                if p and p.get("size", 0.0) > 0:
                    l_flat = False
            if self.short_ex in self.orders:
                p = self.orders[self.short_ex].get_executed_position(self.native_short, "SHORT")
                if p and p.get("size", 0.0) > 0:
                    s_flat = False
            if l_flat and s_flat:
                is_flat = True
                break
            await asyncio.sleep(0.01)

        if not is_flat:
            # Fallback контрольный REST только если сокет не подтвердил за 300 мс
            log(f"[{self.sym}] WS не подтвердил 0.0 за 300 мс, контрольный запрос через REST...", level="WARNING")
            l_check = await self.orders[self.long_ex].get_exact_position_guarded(self.native_long, "LONG") if self.long_ex in self.orders else {"size": 0.0}
            s_check = await self.orders[self.short_ex].get_exact_position_guarded(self.native_short, "SHORT") if self.short_ex in self.orders else {"size": 0.0}
            if l_check.get("size", 0.0) > 0:
                p = l_check.get("price", 0.0) or self.engine_res.get("long_avg_price", 1.0)
                await self.orders[self.long_ex].place_order(self.native_long, "SELL", l_check["size"] * p, p, order_type="MARKET", position_side="LONG")
            if s_check.get("size", 0.0) > 0:
                p = s_check.get("price", 0.0) or self.engine_res.get("short_avg_price", 1.0)
                await self.orders[self.short_ex].place_order(self.native_short, "BUY", s_check["size"] * p, p, order_type="MARKET", position_side="SHORT")

        self._set_state(PositionState.ABORTED)
        self._notify_pos_failed("ASYMMETRIC_FILL_UNWOUND")
        log(f"[{self.sym}] ✅ Асимметрия полностью ликвидирована. Итерация завершена.", level="INFO")

        # Отправляем монету во временный бан, чтобы бот не входил снова в асимметричный стакан
        self.ban_coin_cb(self.sym, reason="Асимметрия налива (сброс входа)", duration_sec=1800)

    async def run_close(self, exit_res: Dict[str, Any], reason: str = "PROFIT_DECAY") -> bool:
        """
        Плановое закрытие обеих ног позиции:
        ACTIVE_HEDGED -> CLOSING -> SETTLED
        """
        self._set_state(PositionState.CLOSING)
        log(f"[{self.sym}] Закрываем позицию: LONG {self.long_ex} | SHORT {self.short_ex} ({reason})", level="INFO")

        # Проверяем фактические объемы в позициях перед закрытием из локального WS-кэша
        l_ws = self.orders[self.long_ex].get_executed_position(self.native_long, "LONG") if self.long_ex in self.orders else {}
        s_ws = self.orders[self.short_ex].get_executed_position(self.native_short, "SHORT") if self.short_ex in self.orders else {}

        long_qty = l_ws.get("size", 0.0) or self.long_pos.get("size", 0.0)
        short_qty = s_ws.get("size", 0.0) or self.short_pos.get("size", 0.0)

        # Если локальный кэш пуст, делаем fallback на REST
        if long_qty <= 0 and self.long_ex in self.orders:
            l_pos = await self.orders[self.long_ex].get_exact_position_guarded(self.native_long, "LONG")
            long_qty = l_pos.get("size", 0.0)
        if short_qty <= 0 and self.short_ex in self.orders:
            s_pos = await self.orders[self.short_ex].get_exact_position_guarded(self.native_short, "SHORT")
            short_qty = s_pos.get("size", 0.0)

        price_long = exit_res.get("long_close_price") or self.exec_res.get("entry_long_price", 1.0)
        price_short = exit_res.get("short_close_price") or self.exec_res.get("entry_short_price", 1.0)

        # 1. Регистрация реактивных событий ДО отправки ордеров закрытия
        ev_long = None
        ev_short = None
        if self.long_ex in self.orders and hasattr(self.orders[self.long_ex], "subscribe_position_update"):
            ev_long = self.orders[self.long_ex].subscribe_position_update(self.native_long, "LONG")
        if self.short_ex in self.orders and hasattr(self.orders[self.short_ex], "subscribe_position_update"):
            ev_short = self.orders[self.short_ex].subscribe_position_update(self.native_short, "SHORT")

        t_close_shot_start = time.perf_counter()
        close_latencies: Dict[str, float] = {}

        async def timed_close_order(ex: str, symbol: str, side: str, size_usd: float, price: float, order_type: str, position_side: str):
            t0 = time.perf_counter()
            try:
                res = await self.orders[ex].place_order(
                    symbol, side, size_usd, price, order_type=order_type, position_side=position_side
                )
                close_latencies[ex] = (time.perf_counter() - t0) * 1000.0
                return res
            except Exception as err:
                close_latencies[ex] = (time.perf_counter() - t0) * 1000.0
                raise err

        try:
            # Запуск мониторинга закрытия по WS параллельно с отправкой ордеров
            close_wait_task = asyncio.create_task(self._wait_for_close_confirmation(ev_long, ev_short))

            tasks = []
            if long_qty > 0 and self.long_ex in self.orders:
                size_usd = long_qty * price_long
                o_type = "MARKET" if reason == "TTL_EXPIRED" else "LIMIT_IOC"
                tasks.append(timed_close_order(
                    self.long_ex, self.native_long, "SELL", size_usd, price_long, o_type, "LONG"
                ))
            if short_qty > 0 and self.short_ex in self.orders:
                size_usd = short_qty * price_short
                o_type = "MARKET" if reason == "TTL_EXPIRED" else "LIMIT_IOC"
                tasks.append(timed_close_order(
                    self.short_ex, self.native_short, "BUY", size_usd, price_short, o_type, "SHORT"
                ))

            close_gather_ms = 0.0
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                close_gather_ms = (time.perf_counter() - t_close_shot_start) * 1000.0

            # 2. Ожидаем быстрого реактивного подтверждения обнуления по WS (< 0.1 мс)
            is_closed_fast, fast_p_long, fast_p_short = await close_wait_task
            total_close_ms = (time.perf_counter() - t_close_shot_start) * 1000.0
            cl_rest_ms = close_latencies.get(self.long_ex, 0.0)
            cs_rest_ms = close_latencies.get(self.short_ex, 0.0)
            cl_ws_ms = self.ws_close_timings.get(self.long_ex, 0.0)
            cs_ws_ms = self.ws_close_timings.get(self.short_ex, 0.0)
            cl_lag = max(0.0, cl_ws_ms - cl_rest_ms) if cl_ws_ms > 0 else 0.0
            cs_lag = max(0.0, cs_ws_ms - cs_rest_ms) if cs_ws_ms > 0 else 0.0
            log(
                f"[{self.sym}] ⏱ ТЕЛЕМЕТРИЯ ВЫХОДА (Итого: {total_close_ms:.1f} мс | HTTP Gather: {close_gather_ms:.1f} мс):\n"
                f"      • {self.long_ex}: REST {cl_rest_ms:.1f} мс | WS закрытие {cl_ws_ms:.1f} мс (лаг сокета: +{cl_lag:.1f} мс)\n"
                f"      • {self.short_ex}: REST {cs_rest_ms:.1f} мс | WS закрытие {cs_ws_ms:.1f} мс (лаг сокета: +{cs_lag:.1f} мс)",
                level="INFO"
            )
        finally:
            if self.long_ex in self.orders and hasattr(self.orders[self.long_ex], "unsubscribe_position_update"):
                self.orders[self.long_ex].unsubscribe_position_update(self.native_long, "LONG")
            if self.short_ex in self.orders and hasattr(self.orders[self.short_ex], "unsubscribe_position_update"):
                self.orders[self.short_ex].unsubscribe_position_update(self.native_short, "SHORT")

        if is_closed_fast:
            log(f"[{self.sym}] Позиция полностью ликвидирована на обеих биржах (0.0) через быстрый WS-стрим.", level="INFO")
            if self.long_ex in self.orders:
                await self.orders[self.long_ex].cancel_all_orders(self.native_long)
            if self.short_ex in self.orders:
                await self.orders[self.short_ex].cancel_all_orders(self.native_short)

            self._set_state(PositionState.SETTLED)
            if self.pm:
                self.pm.confirm_exit(self.route, self.sym)
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {
                    "route": self.route, "sym": self.sym, "reason": reason
                }))

            if self.on_settle_cb:
                entry_l = self.exec_res.get("entry_long_price", price_long)
                entry_s = self.exec_res.get("entry_short_price", price_short)
                exit_l = fast_p_long if fast_p_long > 0 else (exit_res.get("long_close_price") or price_long)
                exit_s = fast_p_short if fast_p_short > 0 else (exit_res.get("short_close_price") or price_short)
                actual_long_usd = long_qty * entry_l
                actual_short_usd = short_qty * entry_s

                asyncio.create_task(self.on_settle_cb(
                    sym=self.sym,
                    route=self.route,
                    long_ex=self.long_ex,
                    short_ex=self.short_ex,
                    entry_long_price=entry_l,
                    entry_short_price=entry_s,
                    exit_long_price=exit_l,
                    exit_short_price=exit_s,
                    actual_long_usd=actual_long_usd,
                    actual_short_usd=actual_short_usd,
                    exit_res=exit_res,
                    reason=reason
                ))
            return True

        # 3. Fallback: Контрольный опрос и подчистка остатков через REST (если WS превысил таймаут)
        for attempt in range(3):
            await asyncio.sleep(self.unwind_retry_pause)
            l_check = await self.orders[self.long_ex].get_exact_position_guarded(self.native_long, "LONG") if self.long_ex in self.orders else {"size": 0.0}
            s_check = await self.orders[self.short_ex].get_exact_position_guarded(self.native_short, "SHORT") if self.short_ex in self.orders else {"size": 0.0}

            l_rem = l_check.get("size", 0.0)
            s_rem = s_check.get("size", 0.0)

            if l_rem == 0.0 and s_rem == 0.0:
                log(f"[{self.sym}] Позиция полностью ликвидирована на обеих биржах (0.0).", level="INFO")
                
                if self.long_ex in self.orders:
                    await self.orders[self.long_ex].cancel_all_orders(self.native_long)
                if self.short_ex in self.orders:
                    await self.orders[self.short_ex].cancel_all_orders(self.native_short)

                self._set_state(PositionState.SETTLED)
                if self.pm:
                    self.pm.confirm_exit(self.route, self.sym)
                if self.writer:
                    asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {
                        "route": self.route, "sym": self.sym, "reason": reason
                    }))
                if self.on_settle_cb:
                    entry_l = self.exec_res.get("entry_long_price", price_long)
                    entry_s = self.exec_res.get("entry_short_price", price_short)
                    exit_l = fast_p_long if fast_p_long > 0 else (exit_res.get("long_close_price") or price_long)
                    exit_s = fast_p_short if fast_p_short > 0 else (exit_res.get("short_close_price") or price_short)
                    actual_long_usd = long_qty * entry_l
                    actual_short_usd = short_qty * entry_s

                    asyncio.create_task(self.on_settle_cb(
                        sym=self.sym,
                        route=self.route,
                        long_ex=self.long_ex,
                        short_ex=self.short_ex,
                        entry_long_price=entry_l,
                        entry_short_price=entry_s,
                        exit_long_price=exit_l,
                        exit_short_price=exit_s,
                        actual_long_usd=actual_long_usd,
                        actual_short_usd=actual_short_usd,
                        exit_res=exit_res,
                        reason=reason
                    ))
                return True
                
            if attempt == 2:
                log(f"[{self.sym}] 🛑 КРИТИЧЕСКАЯ ОШИБКА: Не удалось закрыть позицию (run_close) после 3 попыток очистки! Остаток L:{l_rem} S:{s_rem}. Позиция возвращается в очередь на закрытие!", level="ERROR")
                self._set_state(PositionState.CLOSING)
                self._notify_pos_exit_failed()
                return False

            log(f"[{self.sym}] ⚠️ После закрытия обнаружен остаток: L:{l_rem} S:{s_rem}. Попытка аварийного сброса #{attempt+1}...", level="WARNING")
            cleanup_tasks = []
            if l_rem > 0 and self.long_ex in self.orders:
                p = l_check.get("price", 0.0)
                if p <= 0: p = self.engine_res.get("long_avg_price", 1.0)
                usd = l_rem * p
                cleanup_tasks.append(self.orders[self.long_ex].place_order(
                    self.native_long, "SELL", usd, p, order_type="MARKET", position_side="LONG"
                ))
            if s_rem > 0 and self.short_ex in self.orders:
                p = s_check.get("price", 0.0)
                if p <= 0: p = self.engine_res.get("short_avg_price", 1.0)
                usd = s_rem * p
                cleanup_tasks.append(self.orders[self.short_ex].place_order(
                    self.native_short, "BUY", usd, p, order_type="MARKET", position_side="SHORT"
                ))
            if cleanup_tasks:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)


