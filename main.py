# ============================================================
# FILE: main.py
# ROLE: Дирижер, оркестрация потоков, DiscoveryManager, семафоры и выполнение.
# ============================================================

import asyncio
import time
import traceback
import os
import json
import numpy as np
from dotenv import load_dotenv
import aiohttp

load_dotenv()

from CORE.utils import Utils
from CORE.trading_engine import TradingEngine
from CORE.math_core import pre_calculate_orderbook
from CORE.position_manager import PositionManager

from API.orders import BinanceOrder, KucoinOrder, OkxOrder, BitgetOrder
from analytics import TradeAnalytics
from c_log import log

from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream

from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.OKX.stakan import OkxStakanStream
from API.BITGET.stakan import BitgetStakanStream


from API.discovery import DiscoveryManager
from API.orders import InsufficientMarginError
from CORE.leverage_setter import LeverageSetter
from analytics import generate_global_report
from utils import SessionManager

EXCHANGES = ["BINANCE", "KUCOIN", "OKX", "BITGET"]
EX_TO_IDX = {ex: i for i, ex in enumerate(EXCHANGES)}
IDX_TO_EX = {i: ex for i, ex in enumerate(EXCHANGES)}

class Main:
    def __init__(self):
        self.utils = Utils()
        
        # Load config
        with open("cfg.json", "r", encoding="utf-8") as f:
            self.cfg = json.load(f)
            
        self.engine = TradingEngine(self.cfg, IDX_TO_EX)
        
        # Initialize position streams
        self.binance_pos_stream = BinancePositionStream(
            api_key=os.environ.get("BINANCE_API_KEY", ""),
            api_secret=os.environ.get("BINANCE_API_SECRET", "")
        )
        self.kucoin_pos_stream = KucoinPositionStream(
            api_key=os.environ.get("KUCOIN_API_KEY", ""),
            api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
            api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
        )
        
        # We mock orders for other exchanges or use dummy for now as they are not fully migrated
        self.orders = {
            "BINANCE": BinanceOrder(
                api_key=os.environ.get("BINANCE_API_KEY", ""),
                api_secret=os.environ.get("BINANCE_API_SECRET", ""),
                session=None,
                position_stream=self.binance_pos_stream
            ),
            "KUCOIN": KucoinOrder(
                api_key=os.environ["KUCOIN_API_KEY"],
                api_secret=os.environ["KUCOIN_API_SECRET"],
                api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
                session=None,
                position_stream=self.kucoin_pos_stream,
                margin_settings=self.cfg["margin_settings"]["KUCOIN"]
            ),
            "OKX": OkxOrder(),
            "BITGET": BitgetOrder()
        }
        
        # Configs
        self.entry_desync_limit = self.cfg["trading_rules"]["entry"]["max_desync_ms"]  # None = отключён
        self.top_n_candidates   = self.cfg["trading_rules"]["entry"]["top_n_candidates"]
        
        # None = только при запуске; число = пересоздавать каждые N секунд
        self.topology_rebuild_interval = self.cfg.get("topology_rebuild_interval_sec", None)
        
        self.funding_is_active = self.cfg["trading_rules"]["funding_filter"]["is_active"]
        self.funding_skip_sec = self.cfg["trading_rules"]["funding_filter"]["skip_if_less_than_sec"]
        self.funding_skip_after_sec = self.cfg["trading_rules"]["funding_filter"]["skip_if_after_while_sec"]
        
        self.ban_is_active = self.cfg["trading_rules"]["ban_rules"]["is_active"]
        self.ban_level_threshold = self.cfg["trading_rules"]["ban_rules"]["ban_if_exit_level_ge"]
        self.banned_symbols = {}
        
        if os.path.exists("banned_symbols.json"):
            try:
                with open("banned_symbols.json", "r") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.banned_symbols = {sym: None for sym in data}
                    elif isinstance(data, dict):
                        now = time.time()
                        self.banned_symbols = {
                            k: v for k, v in data.items()
                            if v is None or v > now
                        }
            except Exception as e:
                log(f"Error loading banned symbols: {e}", level="WARNING")
                
            try:
                with open("banned_symbols.json", "w") as f:
                    json.dump(self.banned_symbols, f, indent=4)
            except Exception:
                pass
        else:
            try:
                with open("banned_symbols.json", "w") as f:
                    json.dump({}, f, indent=4)
            except Exception:
                pass

        self.discovery = DiscoveryManager(quote=self.cfg["QUOTE"])
        
        # Managers & Analytics
        self.pm = None
        self.analytics_map = {}
        
        # Предзагружаем аналитику по всем монетам из истории, чтобы global_pnl был точным

        analytics_dir = os.path.join("logs", "analytics")
        if os.path.exists(analytics_dir):
            for filename in os.listdir(analytics_dir):
                if filename.endswith(".json"):
                    sym = filename[:-5]
                    self.analytics_map[sym] = TradeAnalytics(sym, self.cfg["trading_risks"])
        
        
        self.books = {ex: {} for ex in EXCHANGES}
        self.ts = {ex: {} for ex in EXCHANGES}
        self.event_ts = {ex: {} for ex in EXCHANGES}
        self.streams = {}
        
        self.route_names = []
        self.active_routes_array = None

    def _make_depth_handler(self, exchange_name: str):
        async def on_depth(d):
            sym = self.discovery.coin_to_native.get(d.symbol, {}).get(exchange_name)
            base_coin = None
            for coin, mapping in self.discovery.coin_to_native.items():
                if mapping.get(exchange_name) == d.symbol:
                    base_coin = coin
                    break
            
            if base_coin and getattr(d, 'bids', None) and getattr(d, 'asks', None):
                self.books[exchange_name][base_coin] = {"bids": d.bids, "asks": d.asks}
                self.ts[exchange_name][base_coin] = time.time()
                self.event_ts[exchange_name][base_coin] = getattr(d, 'event_time_ms', time.time()*1000)
        return on_depth

    def _is_funding_skip(self) -> bool:
        if not self.funding_is_active:
            return False
        current_sec = int(time.time()) % 3600
        return current_sec >= (3600 - self.funding_skip_sec) or current_sec < self.funding_skip_after_sec

    def _update_total_balance(self, now: float, is_startup: bool = False):
        try:
            active_exchanges = set()
            for route, is_active in self.cfg.get("active_routes", {}).items():
                if is_active:
                    ex1, ex2 = route.split('_')
                    active_exchanges.add(ex1.lower())
                    active_exchanges.add(ex2.lower())
                    
            base_total = sum(
                risk.get("paper_start_balance", 0.0) 
                for ex, risk in self.cfg.get("trading_risks", {}).items() 
                if ex.lower() in active_exchanges
            )
            global_pnl = sum(a.cumulative_pnl_usd for a in self.analytics_map.values())
            total = base_total + global_pnl
            with open("total_balance.json", "w") as f:
                json.dump({"timestamp": now, "total_balance_usd": total}, f)
            prefix = "Initial Total balance" if is_startup else "Total balance updated"
            log(f"{prefix}: {total:.2f} USD (Base: {base_total:.2f}, PnL: {global_pnl:.2f})", level="INFO")
            
            # Также обновляем глобальный отчет
            try:

                generate_global_report()
            except Exception as e:
                log(f"Error generating global report: {e}", level="WARNING")
                
        except Exception as e:
            log(f"Error updating total balance: {e}", level="ERROR")

    def _quarantine_symbol(self, sym: str, duration_sec: int, reason: str):
        self.banned_symbols[sym] = time.time() + duration_sec
        with open("banned_symbols.json", "w") as f:
            json.dump(self.banned_symbols, f, indent=4)
        
        async def unban_later():
            await asyncio.sleep(duration_sec)
            if sym in self.banned_symbols and self.banned_symbols[sym] is not None:
                del self.banned_symbols[sym]
                with open("banned_symbols.json", "w") as f:
                    json.dump(self.banned_symbols, f, indent=4)
                log(f"[{sym}] ⏱ Карантин завершен, монета снова доступна для торгов.", level="INFO")
                
        asyncio.create_task(unban_later())

    async def execute_open(self, sym: str, long_ex: str, short_ex: str, engine_res: dict):
        try:
            spread = engine_res["vwap_spread"]
            log(f"[{sym}] Открываем: LONG {long_ex} | SHORT {short_ex} | Spread: {spread * 100:.2f}%", level="INFO")
            
            native_long = self.discovery.coin_to_native.get(sym, {}).get(long_ex, sym)
            native_short = self.discovery.coin_to_native.get(sym, {}).get(short_ex, sym)
            
            tasks = []
            if long_ex in self.orders:
                size_long = self.cfg["trading_risks"][long_ex.lower()]["trade_size_usd"]
                price_long = engine_res.get("long_avg_price")
                tasks.append(self.orders[long_ex].place_order(native_long, "BUY", size_long, price_long))
            if short_ex in self.orders:
                size_short = self.cfg["trading_risks"][short_ex.lower()]["trade_size_usd"]
                price_short = engine_res.get("short_avg_price")
                tasks.append(self.orders[short_ex].place_order(native_short, "SELL", size_short, price_short))
                
            has_error = False
            error_msgs = []
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        log(f"[{sym}] 🚨 Ошибка входа! Рассинхрон ног: {res}.", level="ERROR")
                        has_error = True
                        error_msgs.append(str(res))

                        if isinstance(res, InsufficientMarginError):
                            self.banned_symbols[sym] = None
                            with open("banned_symbols.json", "w") as f:
                                json.dump(self.banned_symbols, f, indent=4)
                            log(f"[{sym}] 🚫 Связка заблокирована из-за нехватки маржи/ошибки.", level="WARNING")
                        else:
                            # Apply quarantine if configured
                            ban_rules = self.cfg["trading_rules"].get("ban_rules", {})
                            if ban_rules.get("quarantine_on_entry_error", True):
                                duration = ban_rules.get("quarantine_duration_sec", 3600)
                                self._quarantine_symbol(sym, duration, str(res))
                                log(f"[{sym}] ⏱ Монета помещена в карантин на {duration} сек. Причина: {res}", level="WARNING")
                
            # If BOTH orders failed completely, we can rollback immediately
            if has_error and len(error_msgs) == len(tasks):
                log(f"[{sym}] 🚨 Обе ноги завершились с ошибкой. Откат.", level="ERROR")
                self.pm.rollback_entry(long_ex, short_ex, sym)
                return

            await asyncio.sleep(self.cfg["EXECUTION_PAUSE"])
            
            cancel_tasks = []
            if long_ex in self.orders:
                cancel_tasks.append(self.orders[long_ex].cancel_all_orders(native_long))
            if short_ex in self.orders:
                cancel_tasks.append(self.orders[short_ex].cancel_all_orders(native_short))
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
            exec_res = self.engine.evaluate_execution(
                self.books[long_ex].get(sym, {}),
                self.books[short_ex].get(sym, {}),
                long_ex, short_ex, engine_res
            )
            
            long_pos = self.orders[long_ex].get_executed_position(native_long, "LONG") if long_ex in self.orders else {"size": 0.0, "price": 0.0}
            short_pos = self.orders[short_ex].get_executed_position(native_short, "SHORT") if short_ex in self.orders else {"size": 0.0, "price": 0.0}
            
            if long_pos["size"] > 0:
                exec_res["long_executed_volume_rate"] = long_pos["size"] / engine_res["long_qty"] if engine_res.get("long_qty", 0) > 0 else 1.0
                exec_res["actual_long_price"] = long_pos["price"]
            else:
                exec_res["long_executed_volume_rate"] = 0.0
                
            if short_pos["size"] > 0:
                exec_res["short_executed_volume_rate"] = short_pos["size"] / engine_res["short_qty"] if engine_res.get("short_qty", 0) > 0 else 1.0
                exec_res["actual_short_price"] = short_pos["price"]
            else:
                exec_res["short_executed_volume_rate"] = 0.0
                
            long_rate = exec_res.get("long_executed_volume_rate", 0.0)
            short_rate = exec_res.get("short_executed_volume_rate", 0.0)
            
            if long_rate <= 0.0 and short_rate <= 0.0:
                log(f"[{sym}] 🚨 Обе ноги не были исполнены (fill rate 0.0). Отменяем вход без экстренного выхода.", level="WARNING")
                self.pm.rollback_entry(long_ex, short_ex, sym)
                return
            
            self.analytics_map[sym].record_open(
                route=f"{long_ex}_{short_ex}",
                direction=f"{long_ex}_L_{short_ex}_S",
                long_ex=long_ex,
                short_ex=short_ex,
                long_price=exec_res.get("actual_long_price", 0.0),
                short_price=exec_res.get("actual_short_price", 0.0),
                spread=exec_res.get("actual_spread", 0.0),
                slippage=exec_res.get("real_slippage", 0.0)
            )
            
            self.pm.confirm_entry(long_ex, short_ex, sym, exec_res, time.time())
            
            min_fill = self.cfg["trading_rules"]["entry"]["min_fill_rate"]
            long_rate = exec_res.get("long_executed_volume_rate", 0.0)
            short_rate = exec_res.get("short_executed_volume_rate", 0.0)
            
            if long_rate < min_fill or short_rate < min_fill:
                log(f"[{sym}] 🚨 Низкий fill_rate! L:{long_rate * 100:.1f}% S:{short_rate * 100:.1f}%. Экстренный выход!", level="ERROR")
                route = f"{long_ex}_{short_ex}"
                self.pm.lock_for_exit(route, sym)
                asyncio.create_task(self.execute_close(route, sym))
            else:
                log(f"[{sym}] 🟢 Позиция успешно открыта! Fill rate -> L: {long_rate * 100:.1f}% | S: {short_rate * 100:.1f}%", level="INFO")
            
        except Exception as e:
            log(f"[{sym}] Error opening position: {e}", level="ERROR")
            self.pm.rollback_entry(long_ex, short_ex, sym)
            ban_rules = self.cfg["trading_rules"].get("ban_rules", {})
            if ban_rules.get("quarantine_on_entry_error", True):
                duration = ban_rules.get("quarantine_duration_sec", 3600)
                self._quarantine_symbol(sym, duration, f"Unhandled entry error: {e}")

    async def execute_close(self, route: str, sym: str):
        state = self.pm.positions[route][sym]
        long_ex = state["details"]["long_ex"]
        short_ex = state["details"]["short_ex"]
        open_time = state["details"].get("open_time", time.time())
        duration_sec = time.time() - open_time
        try:
            log(f"[{sym}] Закрываем позицию: LONG {long_ex} | SHORT {short_ex}", level="INFO")
            
            native_long = self.discovery.coin_to_native.get(sym, {}).get(long_ex, sym)
            native_short = self.discovery.coin_to_native.get(sym, {}).get(short_ex, sym)
            
            long_rate = state["details"].get("long_executed_volume_rate", 1.0)
            short_rate = state["details"].get("short_executed_volume_rate", 1.0)
            
            tasks = []
            if long_ex in self.orders and long_rate > 0:
                size_long = self.cfg["trading_risks"][long_ex.lower()]["trade_size_usd"] * long_rate
                price_long = self.books[long_ex].get(sym, {}).get("bids", [[0]])[0][0] # rough exit price
                tasks.append(self.orders[long_ex].place_order(native_long, "SELL", size_long, float(price_long)))
            if short_ex in self.orders and short_rate > 0:
                size_short = self.cfg["trading_risks"][short_ex.lower()]["trade_size_usd"] * short_rate
                price_short = self.books[short_ex].get(sym, {}).get("asks", [[0]])[0][0]
                tasks.append(self.orders[short_ex].place_order(native_short, "BUY", size_short, float(price_short)))
                
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        log(f"[{sym}] 🚨 Ошибка закрытия: {res}", level="ERROR")
                        self.pm.rollback_exit(route, sym)
                        state["details"]["next_close_attempt"] = time.time() + 10.0  # Cooldown 10s
                        return
                        
            await asyncio.sleep(self.cfg["EXECUTION_PAUSE"])
            
            cancel_tasks = []
            if long_ex in self.orders:
                cancel_tasks.append(self.orders[long_ex].cancel_all_orders(native_long))
            if short_ex in self.orders:
                cancel_tasks.append(self.orders[short_ex].cancel_all_orders(native_short))
            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
            engine_res = state["details"].get("engine_res", {})
            long_rate = state["details"].get("long_executed_volume_rate", 1.0)
            short_rate = state["details"].get("short_executed_volume_rate", 1.0)
            
            _, exit_res = self.engine.evaluate_exit(
                self.books[long_ex].get(sym, {}),
                self.books[short_ex].get(sym, {}),
                long_ex, short_ex,
                engine_res.get("long_qty", 0.0) * long_rate,
                engine_res.get("short_qty", 0.0) * short_rate,
                state["details"].get("entry_long_price", 0.0),
                state["details"].get("entry_short_price", 0.0),
                duration_sec,
                True,
                long_rate,
                short_rate
            )
            
            long_close_price = exit_res.get("long_close_price")
            if not long_close_price:
                long_close_price = state["details"].get("entry_long_price", 0.0)
                
            short_close_price = exit_res.get("short_close_price")
            if not short_close_price:
                short_close_price = state["details"].get("entry_short_price", 0.0)
                
            self.analytics_map[sym].record_close(
                long_price_close=long_close_price,
                short_price_close=short_close_price,
                spread_out=exit_res.get("vwap_spread_out") or 0.0, 
                slippage_out=0.0
            )
            
            net_yield = exit_res.get("net_yield")
            exit_level_idx = exit_res.get("exit_level_index", 0)
            
            should_ban = False
            ban_reason = ""
            
            if self.ban_is_active:
                if self.ban_level_threshold is not None and exit_level_idx >= self.ban_level_threshold:
                    should_ban = True
                    ban_reason = f"Достигнут уровень {exit_level_idx}"
                elif net_yield is not None and net_yield < 0.0:
                    should_ban = True
                    ban_reason = f"Убыточная сделка, Net Yield: {net_yield * 100:.3f}%"
            
            if should_ban:
                self.banned_symbols[sym] = None
                with open("banned_symbols.json", "w") as f:
                    json.dump(self.banned_symbols, f, indent=4)
                log(f"[{sym}] 🚫 Монета заблокирована ({ban_reason}).", level="WARNING")
                
            self.pm.confirm_exit(route, sym)
            
            if net_yield is not None:
                log(f"[{sym}] 🔴 Позиция закрыта! Net Yield: {net_yield * 100:.3f}% (Уровень: {exit_level_idx})", level="INFO")
            else:
                log(f"[{sym}] 🔴 Позиция закрыта экстренно (Пустой стакан)!", level="WARNING")
                
            self._update_total_balance(time.time())

                
        except Exception as e:
            log(f"[{sym}] Error closing position: {e}", level="ERROR")
            self.pm.rollback_exit(route, sym)
            # Add cooldown to prevent 5ms loop spam
            if "details" in state:
                state["details"]["next_close_attempt"] = time.time() + 10.0
                
    async def _topology_rebuild_loop(self, stream_classes: dict):
        """Периодически пересоздаёт топологию символов и перезапускает WS-стримы."""
        interval = self.topology_rebuild_interval
        if not interval:
            return  # null = только при запуске
        while True:
            await asyncio.sleep(interval)
            log(f"[TOPOLOGY] Таймер сработал ({interval}с). Начинаем ребазу символов...", level="INFO")
            try:
                # Останавливаем старые стримы
                for stream in self.streams.values():
                    try:
                        await stream.aclose()
                    except Exception:
                        pass
                self.streams.clear()
                
                # Перестраиваем топологию
                await self.discovery.build_topology(banned_symbols=self.banned_symbols)
                
                # Добавляем аналитику для новых монет
                for coin in self.discovery.active_pairs_map:
                    if coin not in self.analytics_map:
                        self.analytics_map[coin] = TradeAnalytics(coin, self.cfg["trading_risks"])
                
                # Запускаем новые стримы
                for ex, routes in self.discovery.ws_routes.items():
                    if routes and ex in stream_classes:
                        handler = self._make_depth_handler(ex)
                        stream_inst = stream_classes[ex](routes)
                        self.streams[ex] = stream_inst
                        asyncio.create_task(stream_inst.run(handler))
                
                log(f"[TOPOLOGY] Ребаза завершена: {len(self.discovery.active_pairs_map)} монет активно.", level="INFO")
            except Exception as e:
                log(f"[TOPOLOGY] Ошибка ребазы: {e}", level="ERROR")

    async def run(self):
        self.session = aiohttp.ClientSession()
        self.orders["BINANCE"].session = self.session
        self.orders["KUCOIN"].session = self.session
        
        # Запускаем фоновые таски адаптеров (ПОСЛЕ старта event loop)
        self.orders["BINANCE"].start()
        self.orders["KUCOIN"].start()
        
        log("Initializing DiscoveryManager...", level="INFO")
        # Initialize discovery with global whitelist if provided
        whitelist = self.cfg["SYMBOLS_GLOBAL_WHITELIST"]
        if whitelist:
            self.discovery.SYMBOLS = whitelist
            
        await self.discovery.build_topology(banned_symbols=self.banned_symbols)
        
        # RECONCILIATION
        log("Performing Startup Position Reconciliation...", level="INFO")
        try:
            # Binance
            if self.orders.get("BINANCE") and self.orders["BINANCE"].api_key:
                binance_positions = await self.orders["BINANCE"].get_active_positions()
                for p in binance_positions:
                    log(f"[RECONCILIATION] Orphaned Binance position found: {p['symbol']} size: {p['size']}", level="WARNING")
            
            # Kucoin
            if self.orders.get("KUCOIN") and self.orders["KUCOIN"].api_key:
                kucoin_positions = await self.orders["KUCOIN"].get_active_positions()
                for p in kucoin_positions:
                    log(f"[RECONCILIATION] Orphaned Kucoin position found: {p['symbol']} size: {p['size']}", level="WARNING")
        except Exception as e:
            log(f"Reconciliation error: {e}", level="WARNING")
            
        # SETUP LEVERAGE & MARGIN

        leverage_setter = LeverageSetter(self)
        await leverage_setter.setup()
            
        # WARMUP
        log("Warming up POST API sessions...", level="INFO")
        warmup_tasks = []
        if self.orders["BINANCE"].api_key:
            warmup_tasks.append(self.orders["BINANCE"].warmup())
        if self.orders["KUCOIN"].api_key:
            warmup_tasks.append(self.orders["KUCOIN"].warmup())
        if warmup_tasks:
            await asyncio.gather(*warmup_tasks, return_exceptions=True)
        
        # Prepare JIT matrix — только активные маршруты из cfg.json active_routes
        active_routes_cfg = self.cfg.get("active_routes", {})
        routes_indices = []
        
        supported_exchanges = {"BINANCE", "KUCOIN"}
        
        for i in range(len(EXCHANGES)):
            for j in range(i + 1, len(EXCHANGES)):
                route_key = f"{EXCHANGES[i]}_{EXCHANGES[j]}"
                if active_routes_cfg.get(route_key, False):
                    if EXCHANGES[i] not in supported_exchanges or EXCHANGES[j] not in supported_exchanges:
                        log(f"ВНИМАНИЕ: Маршрут {route_key} включен, но API для этих бирж еще не готово для реальной торговли!", level="WARNING")
                        log(f"Отключаем маршрут {route_key} для безопасности.", level="WARNING")
                        continue
                    routes_indices.append([i, j])
                    self.route_names.append(route_key)

        if not routes_indices:
            log("КРИТИЧНО: ни один маршрут не активен в active_routes!", level="ERROR")

        self.active_routes_array = np.array(routes_indices, dtype=np.int64)
        
        self.pm = PositionManager(self.cfg, EXCHANGES, self.route_names, list(self.discovery.active_pairs_map.keys()))
                
        for coin in self.discovery.active_pairs_map.keys():
            self.analytics_map[coin] = TradeAnalytics(coin, self.cfg["trading_risks"])

        # Initial balance write
        self._update_total_balance(time.time(), is_startup=True)

        # Start streams
        stream_classes = {
            "BINANCE": BinanceStakanStream, "KUCOIN": KucoinStakanStream,
            "OKX": OkxStakanStream, "BITGET": BitgetStakanStream
        }

        for ex, routes in self.discovery.ws_routes.items():
            if routes and ex in stream_classes:
                handler = self._make_depth_handler(ex)
                stream_inst = stream_classes[ex](routes)
                self.streams[ex] = stream_inst
                asyncio.create_task(stream_inst.run(handler))
                log(f"Started {ex} WS stream", level="INFO")
                
        # Start private WS streams for positions
        if os.environ.get("BINANCE_API_KEY"):
            asyncio.create_task(self.binance_pos_stream.start())
        if os.environ.get("KUCOIN_API_KEY"):
            asyncio.create_task(self.kucoin_pos_stream.start())
        
        # Периодическая ребаза: запускаем только если interval != null
        if self.topology_rebuild_interval is not None:
            log(f"[TOPOLOGY] Ребаза будет выполняться каждые {self.topology_rebuild_interval}с.", level="INFO")
            asyncio.create_task(self._topology_rebuild_loop(stream_classes))
        else:
            log("[TOPOLOGY] topology_rebuild_interval_sec=null, ребаза отключена (только при запуске).", level="INFO")
                
        prices_array = np.full((7, 2), [np.inf, 0.0], dtype=np.float64)

        try:
            while True:
                iter_start = time.perf_counter()
                try:
                    now = time.time()
                    funding_skip = self._is_funding_skip()
                    
                    # EXIT LOGIC
                    open_positions = self.pm.get_open_positions()
                    for route, sym, state in open_positions:
                        open_time = state["details"].get("open_time", time.time())
                        duration_sec = time.time() - open_time
                        
                        long_ex = state["details"]["long_ex"]
                        short_ex = state["details"]["short_ex"]
                        
                        long_book = self.books[long_ex].get(sym, {})
                        short_book = self.books[short_ex].get(sym, {})
                        
                        is_stakan_valid = bool(long_book.get("bids") and short_book.get("asks"))
                        

                        long_rate = state["details"].get("long_executed_volume_rate", 1.0)
                        short_rate = state["details"].get("short_executed_volume_rate", 1.0)
                        min_fill = self.cfg["trading_rules"]["entry"].get("min_fill_rate", 0.95)
                        
                        if long_rate < min_fill or short_rate < min_fill:
                            is_exit = True
                            exit_res = {"exit_level_index": 99, "target_val": -999.0}
                        else:
                            is_exit, exit_res = self.engine.evaluate_exit(
                                long_book, short_book, long_ex, short_ex,
                                state["details"]["engine_res"].get("long_qty", 0.0) * long_rate,
                                state["details"]["engine_res"].get("short_qty", 0.0) * short_rate,
                                state["details"].get("entry_long_price", 0.0),
                                state["details"].get("entry_short_price", 0.0),
                                duration_sec,
                                is_stakan_valid=is_stakan_valid,
                                long_executed_volume_rate=long_rate,
                                short_executed_volume_rate=short_rate
                            )
                        
                        current_level = state["details"].get("exit_level_index", 0)
                        new_level = exit_res.get("exit_level_index", current_level)
                        
                        if new_level > current_level:
                            state["details"]["exit_level_index"] = new_level
                            target_val = exit_res.get("target_val")
                            if target_val is None or target_val <= -999.0:
                                log(f"[{sym}] ⚠️ Деградация профита: Уровень {new_level} (TTL, принудительный выход)", level="WARNING")
                            else:
                                log(f"[{sym}] 📉 Деградация профита: Уровень {new_level}, новый таргет: {target_val * 100:.3f}%", level="INFO")
                        
                        if is_exit:
                            if time.time() < state["details"].get("next_close_attempt", 0):
                                continue
                            self.pm.lock_for_exit(route, sym)
                            asyncio.create_task(self.execute_close(route, sym))
                    
                    # ENTRY LOGIC
                    for sym in self.discovery.active_pairs_map.keys():
                        if sym in self.banned_symbols:
                            continue
                            
                        prices_array[:] = [np.inf, 0.0]
                        valid_data_exchanges = 0
                        
                        for ex in EXCHANGES:
                            book = self.books[ex].get(sym)
                            ts = self.ts[ex].get(sym, 0)
                            if book and book.get("asks") and book.get("bids"):
                                if (time.time() - ts) <= 5.0:  # is stale check
                                    prices_array[EX_TO_IDX[ex], 0] = float(book["asks"][0][0])
                                    prices_array[EX_TO_IDX[ex], 1] = float(book["bids"][0][0])
                                    valid_data_exchanges += 1

                        if valid_data_exchanges >= 2 and not funding_skip:
                            candidates = pre_calculate_orderbook(prices_array, self.active_routes_array, self.top_n_candidates)
                            for cand in candidates:
                                if cand[0] == -1.0 or cand[2] <= 0.0:
                                    break
                                    
                                long_idx = int(cand[0])
                                short_idx = int(cand[1])
                                long_ex = IDX_TO_EX[long_idx]
                                short_ex = IDX_TO_EX[short_idx]
                                
                                # Защита от фантомных спредов (Desync check)
                                long_ts = self.ts[long_ex].get(sym, 0.0)
                                short_ts = self.ts[short_ex].get(sym, 0.0)
                                diff_ms = abs(long_ts - short_ts) * 1000.0
                                
                                if self.entry_desync_limit is not None and diff_ms > self.entry_desync_limit:
                                    continue
                                
                                if self.pm.can_enter(long_ex, short_ex, sym):
                                    size_usd = self.cfg["trading_risks"][long_ex.lower()]["trade_size_usd"]
                                    
                                    is_valid_entry, engine_res = self.engine.evaluate_entry(
                                        self.books[long_ex][sym],
                                        self.books[short_ex][sym],
                                        cand,
                                        size_usd
                                    )
                                    
                                    if is_valid_entry:
                                        self.pm.lock_for_entry(long_ex, short_ex, sym, engine_res)
                                        asyncio.create_task(self.execute_open(sym, long_ex, short_ex, engine_res))
                                        break

                except asyncio.CancelledError:
                    raise
                except Exception as iter_ex:
                    log(f"Сбой в цикле (итерация пропущена): {iter_ex}", level="ERROR")
                    traceback.print_exc()
                
                # Замер скорости
                if self.cfg.get("measure_loop_speed"):
                    iter_duration = time.perf_counter() - iter_start
                    if not hasattr(self, "_loop_times"):
                        self._loop_times = []
                    self._loop_times.append(iter_duration)
                    if len(self._loop_times) >= 100:
                        avg_time = sum(self._loop_times) / 100.0
                        max_time = max(self._loop_times)
                        log(f"[Perf] Avg main loop logic time: {avg_time:.6f}s (Max: {max_time:.6f}s)", level="INFO")
                        self._loop_times.clear()

                await asyncio.sleep(self.cfg["MAIN_LOOP_DELAY"])

        except KeyboardInterrupt:
            log("⛔ Остановка по Ctrl+C", level="INFO")
        except Exception as ex:
            log(f"Сбой выполнения: {ex}", level="ERROR")
            traceback.print_exc()
        finally:
            log("Завершение работы.", level="INFO")
            for stream in self.streams.values():
                await stream.aclose()
            await self.binance_pos_stream.stop()
            await self.kucoin_pos_stream.stop()
            await self.discovery.aclose()
            if hasattr(self, 'session') and self.session:
                await self.session.close()

            await SessionManager().close_all()

if __name__ == "__main__":
    try:
        asyncio.run(Main().run())
    except KeyboardInterrupt:
        pass


## шпору не трогать!!
# # chmod 600 ssh_key.txt
# # eval "$(ssh-agent -s)" 
# # ssh-add ssh_key.txt
# # git remote set-url origin git@github.com:hotelUpz/uranus_bot.git
# # source .ssh-autostart.sh
# В терминале Git Bash, находясь в папке с проектом:
# source C:/Users/User/Desktop/My_Pro/HP_EliteBook_735_old/WORKSPACE/COMMON/.ssh-autostart.sh
# chmod 600 /home/kali/Desktop/MyProjects/COMMON/ssh_key.txt

# source /home/kali/Desktop/MyProjects/COMMON/.ssh-autostart.sh

# ssh-add /home/kali/Desktop/MyProjects/COMMON/ssh_key.txt

# git remote set-url origin git@github.com:hotelUpz77745/PapperSpread.git


# git push --set-upstream origin master
# # git config --global push.autoSetupRemote true
# # ssh -T git@github.com 
# # git log -1

# # git add .
# # git commit -m "plh37"
# # git push

# # pip install anthropic
# # npm install -g @anthropic-ai/claude-code

# # export ANTHROPIC_API_KEY=...
# taskkill /F /IM python.exe

# # claude