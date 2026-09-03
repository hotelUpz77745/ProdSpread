# ============================================================
# FILE: CORE/position_fsm.py
# ROLE: Finite State Machine (FSM) жизненного цикла сделок.
#       Устраняет race conditions, защищает от сетевых глитчей
#       и гарантирует детерминированное закрытие ног.
# ============================================================
import asyncio
import time
from enum import Enum
from typing import Dict, Any, Optional

from c_log import log
from CORE.ipc_socket import async_write_msg
from API.orders import InsufficientMarginError


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

        self.native_long = self.coin_to_native.get(sym, {}).get(long_ex, sym)
        self.native_short = self.coin_to_native.get(sym, {}).get(short_ex, sym)

        self.state = PositionState.IDLE
        self.order_policy = self.cfg["trading_rules"]["entry"]["order_execution_type"].upper()
        self.min_fill_rate = float(self.cfg["trading_rules"]["entry"]["min_fill_rate"])
        self.execution_pause = float(self.cfg["EXECUTION_PAUSE"])

        self.exec_res: Dict[str, Any] = {}
        self.long_pos: Dict[str, float] = {"size": 0.0, "price": 0.0}
        self.short_pos: Dict[str, float] = {"size": 0.0, "price": 0.0}
        self.open_time: float = 0.0
        self.open_time_ms: int = 0

    def _set_state(self, new_state: PositionState):
        prev = self.state
        self.state = new_state
        log(f"[{self.sym}][FSM] {prev} -> {new_state}", level="DEBUG")

    async def run_open(self) -> bool:
        """
        Запуск пайплайна открытия позиции:
        IDLE -> SUBMITTING -> RESTING_BOOK -> CANCELLING -> VERIFYING_FILL -> ACTIVE_HEDGED / EMERGENCY_UNWIND / ABORTED
        """
        self._set_state(PositionState.SUBMITTING)
        spread_val = self.engine_res.get("vwap_spread", 0.0)
        log(f"[{self.sym}] Открываем: LONG {self.long_ex} | SHORT {self.short_ex} | Spread: {spread_val * 100:.2f}%", level="INFO")

        tif = "GTC" if self.order_policy == "LIMIT_GTC" else "IOC"
        long_dist = float(self.cfg["trading_risks"][self.long_ex.lower()]["limit_allow_distance"]) if self.long_ex in self.orders else 1.1
        short_dist = float(self.cfg["trading_risks"][self.short_ex.lower()]["limit_allow_distance"]) if self.short_ex in self.orders else 1.1

        price_long_limit = self.engine_res.get("long_avg_price", 0.0) * long_dist
        size_long_usd = self.engine_res.get("long_qty", 0.0) * price_long_limit

        price_short_limit = self.engine_res.get("short_avg_price", 0.0) / short_dist
        size_short_usd = self.engine_res.get("short_qty", 0.0) * price_short_limit

        # 1. Pre-flight проверка размеров ордеров
        try:
            if self.long_ex in self.orders:
                self.orders[self.long_ex].check_order_size(self.native_long, size_long_usd, price_long_limit)
            if self.short_ex in self.orders:
                self.orders[self.short_ex].check_order_size(self.native_short, size_short_usd, price_short_limit)
        except Exception as e:
            log(f"[{self.sym}] 🚨 Ошибка валидации размера ордеров до входа: {e}", level="ERROR")
            self.ban_coin_cb(self.sym, reason=f"Order Size Validation Error: {e}", duration_sec=3600)
            self._set_state(PositionState.FAILED)
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                    "route": self.route, "sym": self.sym, "long_ex": self.long_ex, "short_ex": self.short_ex
                }))
            return False

        # 2. Отправка ордеров
        tasks = []
        if self.long_ex in self.orders:
            tasks.append(self.orders[self.long_ex].place_order(
                self.native_long, "BUY", size_long_usd, price_long_limit, position_side="LONG", time_in_force=tif
            ))
        if self.short_ex in self.orders:
            tasks.append(self.orders[self.short_ex].place_order(
                self.native_short, "SELL", size_short_usd, price_short_limit, position_side="SHORT", time_in_force=tif
            ))

        has_submit_error = False
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    has_submit_error = True
                    log(f"[{self.sym}] 🚨 Ошибка входа! Рассинхрон ног: {res}.", level="ERROR")
                    if isinstance(res, InsufficientMarginError):
                        self.ban_coin_cb(self.sym, reason="Недостаточно маржи (Margin Error)", duration_sec=86400)
                    else:
                        self.ban_coin_cb(self.sym, reason=str(res), duration_sec=3600)
                    break

        # 3. Фаза ожидания в стакане (RESTING_BOOK)
        if self.order_policy == "LIMIT_GTC":
            self._set_state(PositionState.RESTING_BOOK)
            await asyncio.sleep(self.execution_pause)

            # 4. Фаза отмены остатков (CANCELLING) - ТЕПЕРЬ БЛОКИРУЮЩАЯ
            self._set_state(PositionState.CANCELLING)
            cancel_tasks = []
            if self.long_ex in self.orders:
                cancel_tasks.append(self.orders[self.long_ex].cancel_all_orders(self.native_long))
            if self.short_ex in self.orders:
                cancel_tasks.append(self.orders[self.short_ex].cancel_all_orders(self.native_short))
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)

        # 5. Фаза верификации налива (VERIFYING_FILL) - ЛОКАЛЬНОЕ ЧТЕНИЕ (СМАРТ)
        self._set_state(PositionState.VERIFYING_FILL)
        req_long_qty = self.engine_res.get("long_qty", 0.0)
        req_short_qty = self.engine_res.get("short_qty", 0.0)

        # Опрашиваем локальную память (обновляемую бухгалтером WS) мгновенно!
        if self.long_ex in self.orders:
            self.long_pos = self.orders[self.long_ex].get_executed_position(self.native_long, "LONG")
        if self.short_ex in self.orders:
            self.short_pos = self.orders[self.short_ex].get_executed_position(self.native_short, "SHORT")

        l_filled = (self.long_pos.get("size", 0.0) / req_long_qty >= self.min_fill_rate) if req_long_qty > 0 else True
        s_filled = (self.short_pos.get("size", 0.0) / req_short_qty >= self.min_fill_rate) if req_short_qty > 0 else True

        self.open_time = time.time()
        self.open_time_ms = int(self.open_time * 1000)

        l_size = self.long_pos.get("size", 0.0)
        s_size = self.short_pos.get("size", 0.0)
        l_rate = min(1.0, l_size / req_long_qty) if req_long_qty > 0 else 1.0
        s_rate = min(1.0, s_size / req_short_qty) if req_short_qty > 0 else 1.0

        self.exec_res = {
            "engine_res": self.engine_res,
            "long_ex": self.long_ex,
            "short_ex": self.short_ex,
            "entry_long_price": self.long_pos.get("price", self.engine_res.get("long_avg_price", 0.0)),
            "entry_short_price": self.short_pos.get("price", self.engine_res.get("short_avg_price", 0.0)),
            "actual_long_price": self.long_pos.get("price", 0.0),
            "actual_short_price": self.short_pos.get("price", 0.0),
            "long_executed_volume_rate": l_rate,
            "short_executed_volume_rate": s_rate,
            "open_time": self.open_time,
            "open_time_ms": self.open_time_ms
        }

        # 6. Ветвление результатов верификации
        if l_size == 0.0 and s_size == 0.0:
            self._set_state(PositionState.ABORTED)
            log(f"[{self.sym}] ⚠️ Ни одна нога не была залита (0.0). Сброс входа.", level="WARNING")
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                    "route": self.route, "sym": self.sym, "long_ex": self.long_ex, "short_ex": self.short_ex
                }))
            return False

        if has_submit_error or l_rate < self.min_fill_rate or s_rate < self.min_fill_rate:
            self._set_state(PositionState.EMERGENCY_UNWIND)
            log(f"[{self.sym}] 🚨 Низкий fill_rate! L:{l_rate*100:.1f}% S:{s_rate*100:.1f}%. Экстренный сброс!", level="ERROR")
            if self.pm:
                self.pm.confirm_entry(self.long_ex, self.short_ex, self.sym, self.exec_res, self.open_time)
                self.pm.lock_for_exit(self.route, self.sym)
            await self._emergency_unwind()
            return False

        # Успешный вход в обе ноги
        self._set_state(PositionState.ACTIVE_HEDGED)
        if self.pm:
            self.pm.confirm_entry(self.long_ex, self.short_ex, self.sym, self.exec_res, self.open_time)
        log(f"[{self.sym}] 🟢 Позиция успешно открыта! Fill rate -> L: {l_rate*100:.1f}% | S: {s_rate*100:.1f}%", level="INFO")

        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_OPENED", {
                "route": self.route,
                "sym": self.sym,
                "exec_res": self.exec_res,
                "open_time": self.open_time
            }))
        return True

    async def _emergency_unwind(self):
        """
        Гарантированный аварийный сброс залитой ноги до подтвержденного 0.0.
        """
        log(f"[{self.sym}] Запуск Emergency Unwind для незахеджированных объемов...", level="WARNING")
        for attempt in range(3):
            close_tasks = []
            if self.long_pos.get("size", 0.0) > 0 and self.long_ex in self.orders:
                qty = self.long_pos["size"]
                price = self.long_pos.get("price", 0.0)
                if price <= 0: price = self.engine_res.get("long_avg_price", 1.0)
                size_usd = qty * price
                close_tasks.append(self.orders[self.long_ex].place_order(
                    self.native_long, "SELL", size_usd, price, order_type="MARKET", position_side="LONG"
                ))
            if self.short_pos.get("size", 0.0) > 0 and self.short_ex in self.orders:
                qty = self.short_pos["size"]
                price = self.short_pos.get("price", 0.0)
                if price <= 0: price = self.engine_res.get("short_avg_price", 1.0)
                size_usd = qty * price
                close_tasks.append(self.orders[self.short_ex].place_order(
                    self.native_short, "BUY", size_usd, price, order_type="MARKET", position_side="SHORT"
                ))

            if close_tasks:
                results = await asyncio.gather(*close_tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        log(f"[{self.sym}] Emergency Unwind task error: {res}", level="ERROR")

            await asyncio.sleep(0.050)

            # Проверяем фактическое обнуление
            l_check = await self.orders[self.long_ex].get_exact_position_guarded(self.native_long, "LONG") if self.long_ex in self.orders else {"size": 0.0}
            s_check = await self.orders[self.short_ex].get_exact_position_guarded(self.native_short, "SHORT") if self.short_ex in self.orders else {"size": 0.0}

            if l_check.get("size", 0.0) == 0.0 and s_check.get("size", 0.0) == 0.0:
                log(f"[{self.sym}] Emergency Unwind успешно завершен: обе ноги обнулены.", level="INFO")
                self._set_state(PositionState.SETTLED)
                if self.pm:
                    self.pm.confirm_exit(self.route, self.sym)
                break
            
            if attempt == 2:
                log(f"[{self.sym}] 🛑 КРИТИЧЕСКАЯ ОШИБКА: Не удалось закрыть позиции после 3 попыток Unwind! Остаток L:{l_check.get('size')} S:{s_check.get('size')}. Позиция остается в памяти!", level="ERROR")
                self._set_state(PositionState.CLOSING) # Оставляем в процессе закрытия, чтобы watchdog мог подобрать
                return

        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {
                "route": self.route,
                "sym": self.sym,
                "reason": "LOW_FILL_RATE"
            }))

        if self.on_settle_cb:
            asyncio.create_task(self.on_settle_cb(
                self.sym, self.native_long, self.native_short, self.long_ex, self.short_ex, self.open_time_ms
            ))

    async def run_close(self, exit_res: Dict[str, Any], reason: str = "PROFIT_DECAY") -> bool:
        """
        Плановое закрытие обеих ног позиции:
        ACTIVE_HEDGED -> CLOSING -> SETTLED
        """
        self._set_state(PositionState.CLOSING)
        log(f"[{self.sym}] Закрываем позицию: LONG {self.long_ex} | SHORT {self.short_ex} ({reason})", level="INFO")

        # Проверяем фактические объемы в позициях перед закрытием
        l_pos = await self.orders[self.long_ex].get_exact_position_guarded(self.native_long, "LONG") if self.long_ex in self.orders else {"size": 0.0, "price": 0.0}
        s_pos = await self.orders[self.short_ex].get_exact_position_guarded(self.native_short, "SHORT") if self.short_ex in self.orders else {"size": 0.0, "price": 0.0}

        long_qty = l_pos.get("size", 0.0)
        short_qty = s_pos.get("size", 0.0)

        is_panic = (reason == "TTL_EXPIRED" or reason == "LOW_FILL_RATE")
        order_type = "MARKET" if is_panic else "LIMIT"
        tif = "IOC"

        long_dist = float(self.cfg["trading_risks"].get(self.long_ex.lower(), {}).get("limit_allow_distance", 1.002))
        short_dist = float(self.cfg["trading_risks"].get(self.short_ex.lower(), {}).get("limit_allow_distance", 1.002))

        tasks = []
        if long_qty > 0 and self.long_ex in self.orders:
            price_long = exit_res.get("long_close_price") or l_pos.get("price", 0.0)
            if price_long <= 0: price_long = self.engine_res.get("long_avg_price", 1.0)
            # При продаже агрессивная лимитка ставится чуть ниже рынка для гарантированного забора бидов
            price_long_limit = (price_long / long_dist) if order_type == "LIMIT" else price_long
            size_usd = long_qty * price_long_limit
            tasks.append(self.orders[self.long_ex].place_order(
                self.native_long, "SELL", size_usd, price_long_limit, order_type=order_type, position_side="LONG", time_in_force=tif
            ))
        if short_qty > 0 and self.short_ex in self.orders:
            price_short = exit_res.get("short_close_price") or s_pos.get("price", 0.0)
            if price_short <= 0: price_short = self.engine_res.get("short_avg_price", 1.0)
            # При покупке агрессивная лимитка ставится чуть выше рынка для гарантированного забора асков
            price_short_limit = (price_short * short_dist) if order_type == "LIMIT" else price_short
            size_usd = short_qty * price_short_limit
            tasks.append(self.orders[self.short_ex].place_order(
                self.native_short, "BUY", size_usd, price_short_limit, order_type=order_type, position_side="SHORT", time_in_force=tif
            ))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Контрольный опрос и подчистка остатков (до 3 попыток до подтверждения строго 0.0)
        for attempt in range(3):
            await asyncio.sleep(0.050)
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
                    asyncio.create_task(self.on_settle_cb(
                        self.sym, self.native_long, self.native_short, self.long_ex, self.short_ex, self.open_time_ms
                    ))
                return True
                
            if attempt == 2:
                log(f"[{self.sym}] 🛑 КРИТИЧЕСКАЯ ОШИБКА: Не удалось закрыть позицию (run_close) после 3 попыток очистки! Остаток L:{l_rem} S:{s_rem}. Позиция остается в памяти!", level="ERROR")
                self._set_state(PositionState.CLOSING)
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


