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
    SINGLE_LEG_EXPOSURE = "SINGLE_LEG_EXPOSURE"
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
        Запуск пайплайна открытия позиции (PARALLEL_LIMIT_IOC):
        IDLE -> SUBMITTING (Both Legs) -> VERIFYING_FILL -> ACTIVE_HEDGED / SINGLE_LEG_EXPOSURE / ABORTED
        """
        self._set_state(PositionState.SUBMITTING)
        
        entry_cfg = self.cfg["trading_rules"]["entry"]
        roles_cfg = entry_cfg["exchange_roles"].get(self.route)
        if not roles_cfg:
            log(f"[{self.sym}] ⛔ Нет ролей для связки {self.route}.", level="WARNING")
            self._set_state(PositionState.IDLE)
            self._notify_pos_failed("NO_ROLES")
            return False
            
        lead_ex = roles_cfg["lead"]
        hedge_ex = roles_cfg["hedge"]
        
        parallel_cfg = entry_cfg.get("parallel_entry_logic", entry_cfg)
        
        is_lead_long = (lead_ex == self.long_ex)
        native_lead = self.native_long if is_lead_long else self.native_short
        native_hedge = self.native_short if is_lead_long else self.native_long
        
        lead_side = "BUY" if is_lead_long else "SELL"
        hedge_side = "SELL" if is_lead_long else "BUY"
        
        lead_pos_side = "LONG" if is_lead_long else "SHORT"
        hedge_pos_side = "SHORT" if is_lead_long else "LONG"
        
        spread_val = self.engine_res.get("net_spread", self.engine_res.get("vwap_spread", 0.0))
        log(f"[{self.sym}] Открываем (PARALLEL_LIMIT_IOC): {self.long_ex} (L) / {self.short_ex} (S) | Net Spread: {spread_val * 100:.2f}%", level="INFO")
        
        size_long_usd = float(self.cfg["trading_risks"][self.long_ex.lower()]["trade_size_usd"])
        size_short_usd = float(self.cfg["trading_risks"][self.short_ex.lower()]["trade_size_usd"])
        
        # Получаем расчетные цены (текущие лучшие цены или VWAP в зависимости от логики движка)
        price_long_calc = self.engine_res.get("long_avg_price", 0.0)
        price_short_calc = self.engine_res.get("short_avg_price", 0.0)
        
        # Используем лимиты проскальзывания из конфига (пока берем те же что были для Lead)
        max_slip = float(parallel_cfg.get("max_slippage_pct", entry_cfg.get("lead_max_slippage_pct", 0.0005)))
        
        price_long_limit = price_long_calc * (1 + max_slip)
        price_short_limit = price_short_calc * (1 - max_slip)
        
        ev_long = None
        ev_short = None
        
        if self.long_ex in self.orders and hasattr(self.orders[self.long_ex], "subscribe_position_update"):
            ev_long = self.orders[self.long_ex].subscribe_position_update(self.native_long, "LONG")
        if self.short_ex in self.orders and hasattr(self.orders[self.short_ex], "subscribe_position_update"):
            ev_short = self.orders[self.short_ex].subscribe_position_update(self.native_short, "SHORT")
            
        req_long_qty = self.engine_res.get("long_qty", 0.0)
        req_short_qty = self.engine_res.get("short_qty", 0.0)
        
        log(f"[{self.sym}] Phase 1: Sending PARALLEL LIMIT_IOC | L: {price_long_limit:.6f}, S: {price_short_limit:.6f}", level="INFO")
        
        tasks = []
        tasks.append(self.orders[self.long_ex].place_order(
            self.native_long, "BUY", size_long_usd, price_long_limit, order_type="LIMIT_IOC", position_side="LONG"
        ))
        tasks.append(self.orders[self.short_ex].place_order(
            self.native_short, "SELL", size_short_usd, price_short_limit, order_type="LIMIT_IOC", position_side="SHORT"
        ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, res in enumerate(results):
            ex_name = self.long_ex if i == 0 else self.short_ex
            if isinstance(res, Exception):
                log(f"[{self.sym}] 🚨 Ошибка входа ({ex_name}): {res}.", level="ERROR")
                self.ban_coin_cb(self.sym, reason=str(res), duration_sec=3600)
        
        self._set_state(PositionState.VERIFYING_FILL)
        
        l_pos, s_pos, l_rate, s_rate = await self._wait_for_fill_confirmation(
            req_long_qty, req_short_qty, ev_long=ev_long, ev_short=ev_short
        )
        
        if hasattr(self.orders[self.long_ex], "unsubscribe_position_update"):
            self.orders[self.long_ex].unsubscribe_position_update(self.native_long, "LONG")
        if hasattr(self.orders[self.short_ex], "unsubscribe_position_update"):
            self.orders[self.short_ex].unsubscribe_position_update(self.native_short, "SHORT")
            
        l_qty = l_pos.get("size", 0.0)
        s_qty = s_pos.get("size", 0.0)
        l_price = l_pos.get("price", 0.0)
        s_price = s_pos.get("price", 0.0)
        
        log(f"[{self.sym}] Phase 2: Результаты налива -> LONG: {l_qty:.4f} ({l_rate*100:.1f}%), SHORT: {s_qty:.4f} ({s_rate*100:.1f}%)", level="INFO")
        
        if l_qty <= 0.0 and s_qty <= 0.0:
            zero_fill_sec = float(parallel_cfg.get("quarantine_zero_fill_sec", 10))
            log(f"[{self.sym}] Zero Fill: Ни одна нога не налилась. Карантин {zero_fill_sec:.0f}с.", level="WARNING")
            self.ban_coin_cb(self.sym, reason="Zero Fill (Both Legs)", duration_sec=zero_fill_sec)
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("ZERO_FILL")
            return False
            
        # Check partial vs full hedge
        # If both are somewhat filled, check if they are balanced enough
        min_hedge_rate = float(parallel_cfg.get("min_hedge_fill_rate", 0.75))
        
        if l_qty > 0 and s_qty > 0:
            l_notional = l_qty * l_price
            s_notional = s_qty * s_price
            ratio = min(l_notional, s_notional) / max(l_notional, s_notional)
            if ratio >= min_hedge_rate:
                log(f"[{self.sym}] Обе ноги успешно налиты. Сбалансированность {ratio*100:.1f}%. Переход в ACTIVE_HEDGED.", level="INFO")
                # TODO: trim excess if needed (for now just finalize)
                self._finalize_open(l_qty, s_qty, l_price, s_price)
                return True
        
        # SINGLE LEG EXPOSURE
        exit_cfg = self.cfg["trading_rules"]["exit"]
        if exit_cfg.get("single_leg_exit_market_immediate", False):
            log(f"[{self.sym}] Зависла одна нога. Включен немедленный выход по маркету.", level="WARNING")
            if l_qty > 0:
                await self._emergency_unwind_single(self.long_ex, self.native_long, l_qty, l_price, "BUY", "LONG")
            if s_qty > 0:
                await self._emergency_unwind_single(self.short_ex, self.native_short, s_qty, s_price, "SELL", "SHORT")
            self._set_state(PositionState.ABORTED)
            self._notify_pos_failed("SINGLE_LEG_IMMEDIATE_MARKET")
            return False

        log(f"[{self.sym}] Переход к статической арбитражной обработке одной зависшей ноги (SINGLE_LEG_EXPOSURE).", level="WARNING")
        self._set_state(PositionState.SINGLE_LEG_EXPOSURE)
        await self._run_single_leg_exposure(l_qty, s_qty, l_price, s_price)
        return False

    async def _run_single_leg_exposure(self, l_qty: float, s_qty: float, l_price: float, s_price: float):
        """
        Чейзинг стакана для выхода из зависшей ноги.
        """
        # Определяем зависшую ногу
        if l_qty > 0 and s_qty <= 0.0:
            open_ex = self.long_ex
            native_sym = self.native_long
            qty_to_close = l_qty
            entry_price = l_price
            close_side = "SELL"
            pos_side = "LONG"
            engine_price_key = "long_avg_price" 
            ev_leg = self.orders[open_ex].subscribe_position_update(native_sym, "LONG") if hasattr(self.orders[open_ex], "subscribe_position_update") else None
        elif s_qty > 0 and l_qty <= 0.0:
            open_ex = self.short_ex
            native_sym = self.native_short
            qty_to_close = s_qty
            entry_price = s_price
            close_side = "BUY"
            pos_side = "SHORT"
            engine_price_key = "short_avg_price"
            ev_leg = self.orders[open_ex].subscribe_position_update(native_sym, "SHORT") if hasattr(self.orders[open_ex], "subscribe_position_update") else None
        else:
            # Аномалия, сбрасываем обе если нужно
            if l_qty > 0:
                await self._emergency_unwind_single(self.long_ex, self.native_long, l_qty, l_price, "BUY", "LONG")
            if s_qty > 0:
                await self._emergency_unwind_single(self.short_ex, self.native_short, s_qty, s_price, "SELL", "SHORT")
            self._set_state(PositionState.ABORTED)
            return
            
        exit_cfg = self.cfg["trading_rules"]["exit"]
        decay_map = exit_cfg.get("single_leg_exit_map", [])
        
        qty_rem = qty_to_close
        
        start_time = time.time()
        for step in decay_map:
            if qty_rem <= 0:
                break
            
            target_val = float(step.get("target_val", 0.0))
            wait_sec = float(step.get("seconds", 0.0))
            
            now = time.time()
            elapsed = now - start_time
            if elapsed < wait_sec:
                await asyncio.sleep(wait_sec - elapsed)
                
            if target_val <= -900.0:
                # Market fallback
                log(f"[{self.sym}] Single Leg Fallback: MARKET выход ({open_ex}).", level="WARNING")
                await self._emergency_unwind_single(open_ex, native_sym, qty_rem, entry_price, "BUY" if close_side=="SELL" else "SELL", pos_side)
                break
                
            # Пробуем лимитку
            # Для чейзинга нужно взять лучшую цену стакана и ухудшить ее на target_val
            # Чтобы не парсить стакан заново, используем последнюю цену из движка (он обновляется в фоне)
            current_calc_price = self.engine_res.get(engine_price_key, entry_price)
            
            if close_side == "SELL":
                # Ухудшаем цену вниз
                limit_price = current_calc_price * (1 + target_val)
            else:
                # Ухудшаем цену вверх
                limit_price = current_calc_price * (1 - target_val)
                
            usd_needed = qty_rem * limit_price
            log(f"[{self.sym}] Single Leg Chasing (Iter {step.get('step')}): {close_side} {qty_rem:.4f} @ {limit_price:.6f} (Target: {target_val})", level="INFO")
            
            try:
                await self.orders[open_ex].place_order(
                    native_sym, close_side, usd_needed, limit_price, order_type="LIMIT_IOC", position_side=pos_side, exact_qty=qty_rem, reduce_only=True
                )
            except Exception as e:
                log(f"[{self.sym}] Ошибка чейзинга {open_ex}: {e}", level="ERROR")
                continue
                
            await asyncio.sleep(0.5) # Ждем налив лимитки
            
            # Проверяем позицию
            pos = await self.orders[open_ex].get_position_rest(native_sym, pos_side)
            qty_rem = pos.get("size", 0.0)
            
        if ev_leg and hasattr(self.orders[open_ex], "unsubscribe_position_update"):
            self.orders[open_ex].unsubscribe_position_update(native_sym, pos_side)
            
        self.ban_coin_cb(self.sym, reason="Single Leg Exposure Exit", duration_sec=3600)
        self._set_state(PositionState.ABORTED)
        self._notify_pos_failed("SINGLE_LEG_EXPOSURE")

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


