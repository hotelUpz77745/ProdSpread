# ============================================================
# FILE: CORE/executor_process.py
# ROLE: Независимый процесс исполнения ордеров, приватных вебсокетов и клиринга PnL.
# ============================================================

import asyncio
import os
import time
import json
import traceback
from typing import Dict, Any
from dotenv import load_dotenv
# import aiohttp

load_dotenv()

from c_log import log
from utils import SessionManager
from API.orders import BinanceOrder, KucoinOrder, OkxOrder, BitgetOrder, InsufficientMarginError
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.settlement import ExchangeSettlement
from CORE.position_manager import PositionManager
from CORE.leverage_setter import LeverageSetter
from analytics import TradeAnalytics, generate_global_report
from CORE.ipc_socket import async_write_msg, async_read_msg


EXCHANGES = ["BINANCE", "KUCOIN", "OKX", "BITGET"]

class ExecutorProcess:
    def __init__(self, port: int, cfg: dict):
        self.port = port
        self.cfg = cfg
        self.reader = None
        self.writer = None
        self.session = None
        
        # Streams
        self.binance_pos_stream = BinancePositionStream(
            api_key=os.environ.get("BINANCE_API_KEY", ""),
            api_secret=os.environ.get("BINANCE_API_SECRET", "")
        )
        self.kucoin_pos_stream = KucoinPositionStream(
            api_key=os.environ.get("KUCOIN_API_KEY", ""),
            api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
            api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
        )
        self.bitget_pos_stream = BitgetPositionStream(
            api_key=os.environ.get("BITGET_API_KEY", ""),
            api_secret=os.environ.get("BITGET_API_SECRET", ""),
            api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", "")
        )
        
        # Orders
        self.orders = {
            "BINANCE": BinanceOrder(
                api_key=os.environ.get("BINANCE_API_KEY", ""),
                api_secret=os.environ.get("BINANCE_API_SECRET", ""),
                session=None,
                position_stream=self.binance_pos_stream
            ),
            "KUCOIN": KucoinOrder(
                api_key=os.environ.get("KUCOIN_API_KEY", ""),
                api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
                api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
                session=None,
                position_stream=self.kucoin_pos_stream,
                margin_settings=self.cfg["margin_settings"]["KUCOIN"]
            ),
            "OKX": OkxOrder(),
            "BITGET": BitgetOrder(
                api_key=os.environ.get("BITGET_API_KEY", ""),
                api_secret=os.environ.get("BITGET_API_SECRET", ""),
                api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
                margin_settings=self.cfg["margin_settings"]["BITGET"],
                session=None,
                position_stream=self.bitget_pos_stream
            )
        }
        
        # Settlement
        self.settlement = ExchangeSettlement(
            binance_key=os.environ.get("BINANCE_API_KEY", ""),
            binance_secret=os.environ.get("BINANCE_API_SECRET", ""),
            kucoin_key=os.environ.get("KUCOIN_API_KEY", ""),
            kucoin_secret=os.environ.get("KUCOIN_API_SECRET", ""),
            kucoin_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
            bitget_key=os.environ.get("BITGET_API_KEY", ""),
            bitget_secret=os.environ.get("BITGET_API_SECRET", ""),
            bitget_passphrase=os.environ.get("BITGET_API_PASSPHRASE", "")
        )
        
        self.order_execution_type = self.cfg["trading_rules"]["entry"]["order_execution_type"].upper()
        self.min_fill_rate = float(self.cfg["trading_rules"]["entry"]["min_fill_rate"])
        self.execution_pause = float(self.cfg["EXECUTION_PAUSE"])
        
        self.pm = None
        self.analytics_map = {}
        self.banned_symbols = {}
        self.coin_to_native = {}
        self._running = True

    def _load_banned(self):
        if os.path.exists("banned_symbols.json"):
            try:
                with open("banned_symbols.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        now = time.time()
                        self.banned_symbols = {k: v for k, v in data.items() if v is None or v > now}
                    elif isinstance(data, list):
                        self.banned_symbols = {k: None for k in data}
            except Exception:
                pass

    def _save_banned(self):
        try:
            with open("banned_symbols.json", "w", encoding="utf-8") as f:
                json.dump(self.banned_symbols, f, indent=4)
        except Exception:
            pass

    def ban_coin(self, sym: str, reason: str = "", duration_sec: float = None):
        expire_time = (time.time() + duration_sec) if duration_sec else None
        self.banned_symbols[sym] = expire_time
        self._save_banned()
        if expire_time:
            log(f"[{sym}] ⏱ Монета помещена в карантин на {int(duration_sec)} сек. Причина: {reason}", level="WARNING")
        else:
            log(f"[{sym}] 🚫 Монета заблокирована ({reason}).", level="WARNING")
        # Notify Market Process
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "BAN_UPDATE", {"symbol": sym, "expire_time": expire_time}))

    async def init_runtime(self):
        self._load_banned()
        self.session = await SessionManager().get_session()
        self.orders["BINANCE"].session = self.session
        self.orders["KUCOIN"].session = self.session
        self.orders["BITGET"].session = self.session
        self.settlement._session = self.session
        
        self.orders["BINANCE"].start()
        self.orders["KUCOIN"].start()
        self.orders["BITGET"].start()
        
        if os.environ.get("BINANCE_API_KEY"):
            asyncio.create_task(self.binance_pos_stream.start())
        if os.environ.get("KUCOIN_API_KEY"):
            asyncio.create_task(self.kucoin_pos_stream.start())
        if os.environ.get("BITGET_API_KEY"):
            asyncio.create_task(self.bitget_pos_stream.start())

    async def execute_open(self, data: dict):
        sym = data["sym"]
        route = data["route"]
        long_ex = data["long_ex"]
        short_ex = data["short_ex"]
        engine_res = data["engine_res"]
        spread_val = engine_res.get("vwap_spread", 0.0)
        
        log(f"[{sym}] Открываем: LONG {long_ex} | SHORT {short_ex} | Spread: {spread_val * 100:.2f}%", level="INFO")
        
        native_long = self.coin_to_native.get(sym, {}).get(long_ex, sym)
        native_short = self.coin_to_native.get(sym, {}).get(short_ex, sym)
        
        order_policy = self.order_execution_type
        tif = "GTC" if order_policy == "LIMIT_GTC" else "IOC"

        long_dist = float(self.cfg["trading_risks"][long_ex.lower()]["limit_allow_distance"]) if long_ex in self.orders else 1.1
        short_dist = float(self.cfg["trading_risks"][short_ex.lower()]["limit_allow_distance"]) if short_ex in self.orders else 1.1
        
        price_long_limit = engine_res.get("long_avg_price") * long_dist
        size_long_usd = engine_res.get("long_qty") * price_long_limit
        
        price_short_limit = engine_res.get("short_avg_price") / short_dist
        size_short_usd = engine_res.get("short_qty") * price_short_limit
        
        # Pre-flight validation
        try:
            if long_ex in self.orders:
                self.orders[long_ex].check_order_size(native_long, size_long_usd, price_long_limit)
            if short_ex in self.orders:
                self.orders[short_ex].check_order_size(native_short, size_short_usd, price_short_limit)
        except Exception as e:
            log(f"[{sym}] 🚨 Ошибка валидации размера ордеров до входа: {e}", level="ERROR")
            self.ban_coin(sym, reason=f"Order Size Validation Error: {e}", duration_sec=3600)
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                    "route": route, "sym": sym, "long_ex": long_ex, "short_ex": short_ex
                }))
            return

        tasks = []
        if long_ex in self.orders:
            tasks.append(self.orders[long_ex].place_order(native_long, "BUY", size_long_usd, price_long_limit, position_side="LONG", time_in_force=tif))
            
        if short_ex in self.orders:
            tasks.append(self.orders[short_ex].place_order(native_short, "SELL", size_short_usd, price_short_limit, position_side="SHORT", time_in_force=tif))
            
        has_error = False
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    has_error = True
                    log(f"[{sym}] 🚨 Ошибка входа! Рассинхрон ног: {res}.", level="ERROR")
                    if isinstance(res, InsufficientMarginError):
                        self.ban_coin(sym, reason="Недостаточно маржи (Margin Error)", duration_sec=86400)
                    else:
                        self.ban_coin(sym, reason=str(res), duration_sec=3600)
                    break
                    
        # Ждем EXECUTION_PAUSE
        await asyncio.sleep(self.execution_pause)

        # Если режим LIMIT_GTC, отправляем отмену остатков неналитых ордеров (асинхронно, не блокируя event loop)
        if order_policy == "LIMIT_GTC":
            cancel_tasks = []
            if long_ex in self.orders:
                cancel_tasks.append(self.orders[long_ex].cancel_all_orders(native_long))
            if short_ex in self.orders:
                cancel_tasks.append(self.orders[short_ex].cancel_all_orders(native_short))
            if cancel_tasks:
                asyncio.create_task(asyncio.gather(*cancel_tasks, return_exceptions=True))

        # Высокоскоростной мониторинг заливки (микро-петля каждые 2 мс с таймаутом = EXECUTION_PAUSE)
        long_pos = {"size": 0.0, "price": 0.0}
        short_pos = {"size": 0.0, "price": 0.0}
        req_long_qty = engine_res.get("long_qty", 0.0)
        req_short_qty = engine_res.get("short_qty", 0.0)
        min_fill = self.min_fill_rate

        poll_interval = 0.002  # 2 мс
        max_iterations = max(5, int(self.execution_pause / poll_interval))

        for _ in range(max_iterations):
            if long_ex in self.orders:
                long_pos = self.orders[long_ex].get_executed_position(native_long, "LONG")
            if short_ex in self.orders:
                short_pos = self.orders[short_ex].get_executed_position(native_short, "SHORT")

            l_filled = (long_pos.get("size", 0.0) / req_long_qty >= min_fill) if req_long_qty > 0 else True
            s_filled = (short_pos.get("size", 0.0) / req_short_qty >= min_fill) if req_short_qty > 0 else True

            # Если обе ноги уже залились выше min_fill_rate - мгновенно выходим
            if l_filled and s_filled:
                break

            await asyncio.sleep(poll_interval)
            
        # Страховочный REST фоллбек: если какая-то нога показывает 0.0, контрольный опрос через REST
        if long_ex in self.orders and long_pos.get("size", 0.0) == 0.0:
            long_pos = await self.orders[long_ex].get_exact_position(native_long, "LONG")
        if short_ex in self.orders and short_pos.get("size", 0.0) == 0.0:
            short_pos = await self.orders[short_ex].get_exact_position(native_short, "SHORT")
            
        exec_res = {
            "engine_res": engine_res,
            "long_ex": long_ex,
            "short_ex": short_ex,
            "entry_long_price": engine_res.get("long_avg_price", 0.0),
            "entry_short_price": engine_res.get("short_avg_price", 0.0),
            "open_time": time.time(),
            "open_time_ms": int(time.time() * 1000)
        }
        
        if long_pos["size"] > 0:
            exec_res["long_executed_volume_rate"] = min(1.0, long_pos["size"] / engine_res["long_qty"]) if engine_res.get("long_qty", 0) > 0 else 1.0
            exec_res["actual_long_price"] = long_pos["price"]
        else:
            exec_res["long_executed_volume_rate"] = 0.0
            
        if short_pos["size"] > 0:
            exec_res["short_executed_volume_rate"] = min(1.0, short_pos["size"] / engine_res["short_qty"]) if engine_res.get("short_qty", 0) > 0 else 1.0
            exec_res["actual_short_price"] = short_pos["price"]
        else:
            exec_res["short_executed_volume_rate"] = 0.0
            
        min_fill = self.min_fill_rate
        l_rate = exec_res["long_executed_volume_rate"]
        s_rate = exec_res["short_executed_volume_rate"]
        
        if has_error or l_rate < min_fill or s_rate < min_fill:
            if long_pos["size"] == 0.0 and short_pos["size"] == 0.0:
                log(f"[{sym}] ⚠️ Ни одна нога не была залита (0.0). Сброс входа.", level="WARNING")
                if self.writer:
                    asyncio.create_task(async_write_msg(self.writer, "POS_FAILED", {
                        "route": route, "sym": sym, "long_ex": long_ex, "short_ex": short_ex
                    }))
                return

            log(f"[{sym}] 🚨 Низкий fill_rate! L:{l_rate*100:.1f}% S:{s_rate*100:.1f}%. Экстренный выход!", level="ERROR")
            self.pm.confirm_entry(long_ex, short_ex, sym, exec_res, time.time())
            self.pm.lock_for_exit(route, sym)
            await self.execute_close({"route": route, "sym": sym, "reason": "LOW_FILL_RATE", "data": exec_res})
            return
            
        self.pm.confirm_entry(long_ex, short_ex, sym, exec_res, time.time())
        log(f"[{sym}] 🟢 Позиция успешно открыта! Fill rate -> L: {l_rate*100:.1f}% | S: {s_rate*100:.1f}%", level="INFO")
        
        if sym not in self.analytics_map:
            self.analytics_map[sym] = TradeAnalytics(sym, self.cfg["trading_risks"])
        self.analytics_map[sym].record_open(route, "LONG_SHORT", long_ex, short_ex, exec_res["entry_long_price"], exec_res["entry_short_price"], spread_val, 0.0)
        
        # Notify Market Process
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_OPENED", {
                "route": route,
                "sym": sym,
                "exec_res": exec_res,
                "open_time": exec_res["open_time"]
            }))

    async def execute_close(self, data: dict):
        route = data["route"]
        sym = data["sym"]
        state = self.pm.positions[route][sym]
        details = state.get("details", {})
        long_ex = details.get("long_ex")
        short_ex = details.get("short_ex")
        
        if not long_ex or not short_ex:
            long_ex, short_ex = route.split('_')
        open_time_ms = details.get("open_time_ms", int(details.get("open_time", time.time()) * 1000))
        
        log(f"[{sym}] Закрываем позицию: LONG {long_ex} | SHORT {short_ex}", level="INFO")
        
        native_long = self.coin_to_native.get(sym, {}).get(long_ex, sym)
        native_short = self.coin_to_native.get(sym, {}).get(short_ex, sym)
        engine_res = details.get("engine_res", {})
        
        long_pos = await self.orders[long_ex].get_exact_position(native_long, "LONG") if long_ex in self.orders else {"size": 0.0}
        short_pos = await self.orders[short_ex].get_exact_position(native_short, "SHORT") if short_ex in self.orders else {"size": 0.0}
        
        if long_pos.get("size", 0.0) <= 0 and short_pos.get("size", 0.0) <= 0:
            log(f"[{sym}] Обе ноги уже закрыты на биржах (0.0). Завершаем выход.", level="INFO")
            self.pm.confirm_exit(route, sym)
            if self.writer:
                asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {"route": route, "sym": sym}))
            asyncio.create_task(self._post_close_settlement(sym, native_long, native_short, long_ex, short_ex, open_time_ms))
            return
            
        tasks = []
        exit_res = data.get("exit_res", {})
        emergency_data = data.get("data", {})
        
        order_policy = self.order_execution_type
        tif = "GTC" if order_policy == "LIMIT_GTC" else "IOC"
        
        long_qty_to_close = long_pos.get("size", 0.0)
        if long_ex in self.orders and long_qty_to_close > 0:
            long_dist = float(self.cfg["trading_risks"][long_ex.lower()]["limit_allow_distance"])
            price_long = float(exit_res.get("long_close_price") or 0.0)
            if price_long <= 0:
                price_long = float(long_pos.get("price", 0.0))
            if price_long <= 0:
                price_long = float(details.get("entry_long_price", 0.0)) or float(details.get("actual_long_price", 0.0))
            if price_long <= 0:
                price_long = float(emergency_data.get("entry_long_price", 0.0)) or float(emergency_data.get("actual_long_price", 0.0))
            if price_long <= 0:
                price_long = float(engine_res.get("long_avg_price", 0.0))
                
            if price_long > 0:
                price_long_limit = price_long / long_dist
                size_long_usd = long_qty_to_close * price_long_limit
                tasks.append(self.orders[long_ex].place_order(native_long, "SELL", size_long_usd, price_long_limit, position_side="LONG", time_in_force=tif))
            else:
                log(f"[{sym}] 🚨 Не удалось определить цену закрытия для LONG {long_ex}", level="ERROR")
            
        short_qty_to_close = short_pos.get("size", 0.0)
        if short_ex in self.orders and short_qty_to_close > 0:
            short_dist = float(self.cfg["trading_risks"][short_ex.lower()]["limit_allow_distance"])
            price_short = float(exit_res.get("short_close_price") or 0.0)
            if price_short <= 0:
                price_short = float(short_pos.get("price", 0.0))
            if price_short <= 0:
                price_short = float(details.get("entry_short_price", 0.0)) or float(details.get("actual_short_price", 0.0))
            if price_short <= 0:
                price_short = float(emergency_data.get("entry_short_price", 0.0)) or float(emergency_data.get("actual_short_price", 0.0))
            if price_short <= 0:
                price_short = float(engine_res.get("short_avg_price", 0.0))
                
            if price_short > 0:
                price_short_limit = price_short * short_dist
                size_short_usd = short_qty_to_close * price_short_limit
                tasks.append(self.orders[short_ex].place_order(native_short, "BUY", size_short_usd, price_short_limit, position_side="SHORT", time_in_force=tif))
            else:
                log(f"[{sym}] 🚨 Не удалось определить цену закрытия для SHORT {short_ex}", level="ERROR")
            
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    err_str = str(res).lower()
                    if "position" in err_str or "reduce" in err_str or "no open" in err_str:
                        log(f"[{sym}] ⚠️ Ордер на закрытие отклонен (поза уже пуста): {res}", level="WARNING")
                        continue
                    log(f"[{sym}] 🚨 Ошибка закрытия: {res}", level="ERROR")
                    self.pm.rollback_exit(route, sym)
                    return
                    
        # Ждем EXECUTION_PAUSE
        await asyncio.sleep(self.execution_pause)
        
        # Если режим LIMIT_GTC, отменяем остатки неналитых ордеров закрытия
        if order_policy == "LIMIT_GTC":
            cancel_tasks = []
            if long_ex in self.orders:
                cancel_tasks.append(self.orders[long_ex].cancel_all_orders(native_long))
            if short_ex in self.orders:
                cancel_tasks.append(self.orders[short_ex].cancel_all_orders(native_short))
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)
        
        self.pm.confirm_exit(route, sym)
        if self.writer:
            asyncio.create_task(async_write_msg(self.writer, "POS_CLOSED", {"route": route, "sym": sym}))
        asyncio.create_task(self._post_close_settlement(sym, native_long, native_short, long_ex, short_ex, open_time_ms))

    async def _post_close_settlement(self, sym: str, native_long: str, native_short: str, long_ex: str, short_ex: str, open_time_ms: int):
        """Фоновый расчет реального PnL и клиринг сделки через 1.8 секунды."""
        total_inv = self.cfg["trading_risks"][long_ex.lower()].get("trade_size_usd", 7.0) + \
                    self.cfg["trading_risks"][short_ex.lower()].get("trade_size_usd", 7.0)
                    
        res = await self.settlement.settle_trade(
            sym, native_long, native_short, long_ex, short_ex, open_time_ms, total_inv, delay_sec=1.8
        )
        
        net_usd = res["total_net_pnl_usd"]
        net_yield = res["net_yield_pct"]
        
        if sym in self.analytics_map:
            # Записываем фактический PnL в лог аналитики
            self.analytics_map[sym].cumulative_pnl_usd += net_usd
            
        # Обновляем глобальный баланс
        generate_global_report()
        
        # Проверяем на убыток
        if net_usd < 0:
            self.ban_coin(sym, reason=f"Убыточная сделка, Net: {net_usd:+.4f}$ ({net_yield*100:+.3f}%)")
        else:
            log(f"[{sym}] 🎉 Прибыльная сделка: Net: {net_usd:+.4f}$ ({net_yield*100:+.3f}%)", level="INFO")

    async def run(self):
        await self.init_runtime()
        log("[ExecutorProcess] Подключение к IPC серверу...", level="INFO")
        
        # Подключаемся к локальному порту
        for _ in range(10):
            try:
                self.reader, self.writer = await asyncio.open_connection('127.0.0.1', self.port)
                break
            except Exception:
                await asyncio.sleep(0.5)
                
        if not self.reader:
            log("[ExecutorProcess] Не удалось подключиться к IPC серверу.", level="ERROR")
            return
            
        log("[ExecutorProcess] Процесс исполнения ордеров подключен и готов к командам.", level="INFO")
        
        try:
            while self._running:
                try:
                    msg_type, payload = await async_read_msg(self.reader)
                    if msg_type == "INIT_TOPOLOGY":
                        self.coin_to_native = payload.get("coin_to_native", {})
                        routes = payload.get("routes", [])
                        active_symbols = payload.get("active_symbols", [])
                        self.pm = PositionManager(self.cfg, EXCHANGES, routes, active_symbols)
                        asyncio.create_task(LeverageSetter(self.cfg, self.orders, self.coin_to_native).setup())
                    elif msg_type == "CMD_OPEN":
                        async def safe_execute_open(data):
                            try:
                                await self.execute_open(data)
                            except Exception as e:
                                log(f"[ExecutorProcess] Ошибка в execute_open: {e}\n{traceback.format_exc()}", level="ERROR")
                        asyncio.create_task(safe_execute_open(payload))
                    elif msg_type == "CMD_CLOSE":
                        async def safe_execute_close(data):
                            try:
                                await self.execute_close(data)
                            except Exception as e:
                                log(f"[ExecutorProcess] Ошибка в execute_close: {e}\n{traceback.format_exc()}", level="ERROR")
                        asyncio.create_task(safe_execute_close(payload))
                    elif msg_type == "SHUTDOWN":
                        self._running = False
                        break
                except EOFError:
                    log("[ExecutorProcess] IPC соединение разорвано.", level="WARNING")
                    break
                except Exception as e:
                    log(f"[ExecutorProcess] Ошибка обработки команды: {e}", level="ERROR")
        finally:
            log("[ExecutorProcess] Завершение работы, закрытие сессий...", level="INFO")
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
            from utils import SessionManager
            await SessionManager().close_all()

def run_executor_process(port: int, cfg: dict):
    """Entrypoint для отдельного процесса multiprocessing."""
    try:
        executor = ExecutorProcess(port, cfg)
        asyncio.run(executor.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(f"[ExecutorProcess] Fatal error: {e}\n{traceback.format_exc()}", level="ERROR")
