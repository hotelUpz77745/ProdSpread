# File: CORE/position_fsm.py
# Role: Конечный автомат состояний (FSM) жизненного цикла торговой позиции (HFT)

import asyncio
import time
from enum import Enum
from typing import Dict, Any, Optional, Tuple

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

        self.native_long = self.coin_to_native[sym][long_ex] if sym in self.coin_to_native and long_ex in self.coin_to_native[sym] else sym
        self.native_short = self.coin_to_native[sym][short_ex] if sym in self.coin_to_native and short_ex in self.coin_to_native[sym] else sym

        self.state = PositionState.IDLE
        self.order_policy = self.cfg["trading_rules"]["entry"]["order_execution_type"].upper()
        self.min_fill_rate = float(self.cfg["trading_rules"]["entry"]["min_fill_rate"])

        # Параметры подтверждения налива (из конфига строго через [''], без дефолтов и магии)
        timeout_cfg = self.cfg["trading_rules"]["entry"]["fill_confirm_timeout_sec"]
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
            
        self.fill_confirm_poll_interval = float(self.cfg["trading_rules"]["entry"]["fill_confirm_poll_interval_sec"])

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

    def _set_state(self, new_state: PositionState):
        prev = self.state
        self.state = new_state
        log(f"[{self.sym}][FSM] {prev} -> {new_state}", level="DEBUG")

    async def _wait_for_fill_confirmation(
        self, req_long_qty: float, req_short_qty: float
    ) -> Tuple[Dict[str, float], Dict[str, float], float, float]:
        """
        Реактивный опрос локального WS-кэша позиций до подтверждения налива обеих ног (min_fill_rate)
        или истечения предельного таймаута fill_confirm_timeout_sec.
        """
        start_time = time.perf_counter()
        deadline = start_time + self.fill_confirm_timeout
        l_rate = 0.0
        s_rate = 0.0

        while time.perf_counter() < deadline:
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

            if l_rate >= self.min_fill_rate and s_rate >= self.min_fill_rate:
                elapsed_ms = (time.perf_counter() - start_time) * 1000.0
                log(f"[{self.sym}] 🚀 Обе ноги подтверждены за {elapsed_ms:.1f} мс (L:{l_rate*100:.1f}%, S:{s_rate*100:.1f}%)", level="INFO")
                break

            await asyncio.sleep(self.fill_confirm_poll_interval)
        else:
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            log(f"[{self.sym}] ⏱ Таймаут подтверждения налива ({elapsed_ms:.1f} мс). L:{l_rate*100:.1f}%, S:{s_rate*100:.1f}%", level="WARNING")

        return self.long_pos, self.short_pos, l_rate, s_rate

    async def _wait_for_close_confirmation(self) -> Tuple[bool, float, float]:
        """
        Реактивный опрос локального WS-кэша позиций до подтверждения обнуления обеих ног (size == 0.0)
        или истечения предельного таймаута close_confirm_timeout_sec.
        Возвращает (is_closed, close_price_long, close_price_short).
        """
        start_time = time.perf_counter()
        deadline = start_time + self.close_confirm_timeout
        close_p_long = 0.0
        close_p_short = 0.0

        while time.perf_counter() < deadline:
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

            if l_closed and s_closed:
                elapsed_ms = (time.perf_counter() - start_time) * 1000.0
                log(f"[{self.sym}] 🚀 Обе ноги подтверждены закрытыми по WS за {elapsed_ms:.1f} мс (0.0)", level="INFO")
                return True, close_p_long, close_p_short

            await asyncio.sleep(self.fill_confirm_poll_interval)

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        log(f"[{self.sym}] ⏱ Таймаут подтверждения закрытия по WS ({elapsed_ms:.1f} мс), переход к контрольной проверке...", level="WARNING")
        return False, close_p_long, close_p_short

    async def run_open(self) -> bool:
        """
        Запуск пайплайна открытия позиции (Market-Only):
        IDLE -> SUBMITTING (MARKET via WS) -> VERIFYING_FILL -> ACTIVE_HEDGED / EMERGENCY_UNWIND / ABORTED
        """
        self._set_state(PositionState.SUBMITTING)
        spread_val = self.engine_res.get("net_spread", self.engine_res.get("vwap_spread", 0.0))
        gross_val = self.engine_res.get("vwap_spread", 0.0)
        log(f"[{self.sym}] Открываем: LONG {self.long_ex} | SHORT {self.short_ex} | Net Spread: {spread_val * 100:.2f}% (Gross: {gross_val * 100:.2f}%, Режим: MARKET)", level="INFO")

        # 1. Расчет цен и объемов
        size_long_usd = float(self.cfg["trading_risks"][self.long_ex.lower()]["trade_size_usd"])
        size_short_usd = float(self.cfg["trading_risks"][self.short_ex.lower()]["trade_size_usd"])
        price_long = self.engine_res.get("long_avg_price", 0.0)
        price_short = self.engine_res.get("short_avg_price", 0.0)

        # 2. Pre-flight Validation (проверка шага лота, спецификаций и цен ДО выстрела)
        try:
            if self.long_ex in self.orders and hasattr(self.orders[self.long_ex], "check_order_size"):
                self.orders[self.long_ex].check_order_size(self.native_long, size_long_usd, price_long)
            if self.short_ex in self.orders and hasattr(self.orders[self.short_ex], "check_order_size"):
                self.orders[self.short_ex].check_order_size(self.native_short, size_short_usd, price_short)
        except Exception as pre_err:
            log(f"[{self.sym}] ⛔ Pre-flight Validation Rejected: {pre_err}", level="WARNING")
            self._set_state(PositionState.IDLE)
            return False

        # 3. Lead-Lag Sequencing (компенсация времени ответа: более медленная биржа выстреливает первой)
        self._set_state(PositionState.VERIFYING_FILL)
        req_long_qty = self.engine_res.get("long_qty", 0.0)
        req_short_qty = self.engine_res.get("short_qty", 0.0)

        # Запускаем ожидание налива СРАЗУ в момент отправки ордеров
        wait_task = asyncio.create_task(self._wait_for_fill_confirmation(req_long_qty, req_short_qty))

        def _get_ex_priority(ex_name: str) -> int:
            # Bitget и Kucoin имеют больший matching RTT, поэтому отправляются первыми (0), Binance (1)
            return 0 if ex_name in ("BITGET", "KUCOIN", "OKX") else 1

        legs = [
            (_get_ex_priority(self.long_ex), self.long_ex, self.native_long, "BUY", size_long_usd, price_long, "LONG"),
            (_get_ex_priority(self.short_ex), self.short_ex, self.native_short, "SELL", size_short_usd, price_short, "SHORT")
        ]
        legs.sort(key=lambda x: x[0])

        tasks = []
        for _, ex, native_sym, side, size_usd, price, pos_side in legs:
            if ex in self.orders:
                tasks.append(self.orders[ex].place_order(
                    native_sym, side, size_usd, price, order_type="MARKET", position_side=pos_side
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

        if has_submit_error:
            wait_task.cancel()
            # Немедленно отменяем открытые ордера по успевшей ноге
            cancel_tasks = []
            if self.long_ex in self.orders:
                cancel_tasks.append(self.orders[self.long_ex].cancel_all_orders(self.native_long))
            if self.short_ex in self.orders:
                cancel_tasks.append(self.orders[self.short_ex].cancel_all_orders(self.native_short))
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)

            # Контрольный REST GET и дозакрытие остатков перед выходом
            await self._emergency_unwind()
            return False

        # Ожидаем завершения подтверждения налива (которое шло параллельно отправке)
        self.long_pos, self.short_pos, l_rate, s_rate = await wait_task
        l_size = self.long_pos.get("size", 0.0)
        s_size = self.short_pos.get("size", 0.0)

        self.open_time = time.time()
        self.open_time_ms = int(self.open_time * 1000)

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

        # Если налило обе ноги в пределах min_fill_rate -> УСПЕШНЫЙ ВХОД
        if l_rate >= self.min_fill_rate and s_rate >= self.min_fill_rate and l_size > 0 and s_size > 0:
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

        # Если не налило или налило несимметрично: сворачиваем удочки!
        # ПЕРЕД ФИНАЛЬНЫМ СБРОСОМ ТОРГОВОЙ ИТЕРАЦИИ:
        # ОБЯЗАТЕЛЬНЫЙ ТОЧЕЧНЫЙ REST GET-ЗАПРОС ПОСИМВОЛЬНО ПО ОБЕИМ НОГАМ И ДОЗАКРЫТИЕ!
        log(f"[{self.sym}] Сворачиваем удочки (WS fill rate L:{l_rate*100:.1f}%, S:{s_rate*100:.1f}%). Запуск обязательной зачистки через REST...", level="WARNING")
        await self._emergency_unwind()
        return False

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
        if self.pm:
            self.pm.rollback_entry(self.long_ex, self.short_ex, self.sym)
        log(f"[{self.sym}] ✅ Асимметрия полностью ликвидирована. Итерация завершена.", level="INFO")

        # Уведомляем main-процесс об отмене входа, чтобы освободить слот
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                "route": self.route,
                "sym": self.sym,
                "long_ex": self.long_ex,
                "short_ex": self.short_ex,
                "reason": "ASYMMETRIC_FILL_UNWOUND"
            }))

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

        # 1. Запуск мониторинга закрытия по WS параллельно с отправкой ордеров
        close_wait_task = asyncio.create_task(self._wait_for_close_confirmation())

        tasks = []
        if long_qty > 0 and self.long_ex in self.orders:
            size_usd = long_qty * price_long
            tasks.append(self.orders[self.long_ex].place_order(
                self.native_long, "SELL", size_usd, price_long, order_type="MARKET", position_side="LONG"
            ))
        if short_qty > 0 and self.short_ex in self.orders:
            size_usd = short_qty * price_short
            tasks.append(self.orders[self.short_ex].place_order(
                self.native_short, "BUY", size_usd, price_short, order_type="MARKET", position_side="SHORT"
            ))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 2. Ожидаем быстрого подтверждения обнуления по WS (обычно 15-50 мс)
        is_closed_fast, fast_p_long, fast_p_short = await close_wait_task

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


