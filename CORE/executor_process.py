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
from CORE.position_fsm import PositionFSM
from CORE.leverage_setter import LeverageSetter
from analytics import TradeAnalytics, generate_global_report, update_total_balance
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
        net_cfg = self.cfg.get("network_settings", {})
        self.orders = {
            "BINANCE": BinanceOrder(
                api_key=os.environ.get("BINANCE_API_KEY", ""),
                api_secret=os.environ.get("BINANCE_API_SECRET", ""),
                session=None,
                position_stream=self.binance_pos_stream,
                network_settings=net_cfg
            ),
            "KUCOIN": KucoinOrder(
                api_key=os.environ.get("KUCOIN_API_KEY", ""),
                api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
                api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
                session=None,
                position_stream=self.kucoin_pos_stream,
                margin_settings=self.cfg["margin_settings"]["KUCOIN"],
                network_settings=net_cfg
            ),
            "OKX": OkxOrder(),
            "BITGET": BitgetOrder(
                api_key=os.environ.get("BITGET_API_KEY", ""),
                api_secret=os.environ.get("BITGET_API_SECRET", ""),
                api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
                margin_settings=self.cfg["margin_settings"]["BITGET"],
                session=None,
                position_stream=self.bitget_pos_stream,
                network_settings=net_cfg
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
        
        self.pm = None
        self.analytics_map = {}
        self.active_fsm = {}
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
        update_total_balance(self.cfg, is_startup=True)
        self.session = await SessionManager().get_session()
        self.orders["BINANCE"].session = self.session
        self.orders["KUCOIN"].session = self.session
        self.orders["BITGET"].session = self.session
        self.settlement._session = self.session
        
        self.orders["BINANCE"].start()
        self.orders["KUCOIN"].start()
        self.orders["BITGET"].start()
        
        # Гарантированный прогрев REST сессий (TCP/TLS keepalive) ДО старта торговли
        warmup_tasks = []
        for ex in ("BINANCE", "BITGET", "KUCOIN"):
            if ex in self.orders and hasattr(self.orders[ex], "warmup"):
                warmup_tasks.append(self.orders[ex].warmup())
        if warmup_tasks:
            await asyncio.gather(*warmup_tasks, return_exceptions=True)
            log("[ExecutorProcess] 🔥 Все REST торговые сессии (TCP/TLS keepalive) прогреты ДО старта торговли.", level="INFO")
        
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

        fsm = PositionFSM(
            sym=sym,
            route=route,
            long_ex=long_ex,
            short_ex=short_ex,
            engine_res=engine_res,
            cfg=self.cfg,
            orders=self.orders,
            coin_to_native=self.coin_to_native,
            pm=self.pm,
            writer=self.writer,
            ban_coin_cb=self.ban_coin,
            on_settle_cb=self._post_close_settlement
        )
        self.active_fsm[sym] = fsm
        success = await fsm.run_open()
        if success:
            spread_val = engine_res.get("vwap_spread", 0.0)
            if sym not in self.analytics_map:
                self.analytics_map[sym] = TradeAnalytics(sym, self.cfg["trading_risks"])
            self.analytics_map[sym].record_open(
                route, "LONG_SHORT", long_ex, short_ex,
                fsm.exec_res.get("entry_long_price", 0.0),
                fsm.exec_res.get("entry_short_price", 0.0),
                spread_val, 0.0
            )
        else:
            self.active_fsm.pop(sym, None)

    async def execute_close(self, data: dict):
        sym = data["sym"]
        route = data["route"]
        exit_res = data.get("exit_res", {})
        reason = exit_res.get("reason", data.get("reason", "PROFIT_DECAY"))

        fsm = self.active_fsm.get(sym)
        if not fsm:
            # Восстановление FSM для позиций, подгруженных из стейта после перезапуска бота
            state = self.pm.positions.get(route, {}).get(sym, {})
            details = state.get("details", {})
            long_ex = details.get("long_ex") or route.split('_')[0]
            short_ex = details.get("short_ex") or route.split('_')[1]
            fsm = PositionFSM(
                sym=sym,
                route=route,
                long_ex=long_ex,
                short_ex=short_ex,
                engine_res=details.get("engine_res", {}),
                cfg=self.cfg,
                orders=self.orders,
                coin_to_native=self.coin_to_native,
                pm=self.pm,
                writer=self.writer,
                ban_coin_cb=self.ban_coin,
                on_settle_cb=self._post_close_settlement
            )
            fsm.open_time = details.get("open_time", time.time())
            fsm.open_time_ms = details.get("open_time_ms", int(fsm.open_time * 1000))
            fsm.exec_res = {
                "entry_long_price": details.get("entry_long_price", 0.0),
                "entry_short_price": details.get("entry_short_price", 0.0),
                "long_executed_volume_rate": details.get("long_executed_volume_rate", 1.0),
                "short_executed_volume_rate": details.get("short_executed_volume_rate", 1.0),
            }

        await fsm.run_close(exit_res, reason=reason)
        self.active_fsm.pop(sym, None)


    async def _post_close_settlement(self, sym: str, route: str, long_ex: str, short_ex: str,
                                     entry_long_price: float, entry_short_price: float,
                                     exit_long_price: float, exit_short_price: float,
                                     actual_long_usd: float, actual_short_usd: float,
                                     exit_res: dict = None, reason: str = ""):
        """Моментальный локальный расчет PnL за 0 мс по реальным ценам исполнения."""
        try:
            if sym not in self.analytics_map:
                self.analytics_map[sym] = TradeAnalytics(sym, self.cfg["trading_risks"])

            if not self.analytics_map[sym].active_trade:
                self.analytics_map[sym].record_open(
                    route=route, direction="LONG_SHORT",
                    long_ex=long_ex, short_ex=short_ex,
                    long_price=entry_long_price, short_price=entry_short_price,
                    spread=0.0, slippage=0.0
                )

            spread_out = exit_res.get("vwap_spread_out", 0.0) if exit_res else 0.0
            trade_obj = self.analytics_map[sym].record_close(
                long_price_close=exit_long_price,
                short_price_close=exit_short_price,
                spread_out=spread_out,
                slippage_out=0.0,
                long_executed_usd=actual_long_usd,
                short_executed_usd=actual_short_usd
            )

            net_usd = trade_obj.get("Net_PnL_USD", 0.0) if trade_obj else 0.0
            net_yield = trade_obj.get("Net_PnL", 0.0) if trade_obj else 0.0

            # Моментальное обновление total_balance.json в памяти (O(1))
            update_total_balance(self.cfg, extra_pnl=net_usd)

            if net_usd < 0:
                self.ban_coin(sym, reason=f"Убыточная сделка ({reason}), Net: {net_usd:+.4f}$ ({net_yield*100:+.3f}%)")
            else:
                log(f"[{sym}] 🎉 Прибыльная сделка ({reason}): Net: {net_usd:+.4f}$ ({net_yield*100:+.3f}%)", level="INFO")
        except Exception as e:
            log(f"[{sym}] Ошибка локального клиринга PnL: {e}", level="ERROR")

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
