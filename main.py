# ============================================================
# FILE: main.py
# ROLE: Дирижер (Процесс 1): Сбор стаканов, Discovery, Numba JIT сигналы и деградация профита.
#       Оркестрирует отдельный процесс исполнения (Процесс 2: ExecutorProcess).
# ============================================================

import asyncio
import time
import traceback
import os
import json
import numpy as np
import multiprocessing as mp
from dotenv import load_dotenv
import aiohttp

load_dotenv()

from CORE.utils import Utils
from CORE.trading_engine import TradingEngine
from CORE.math_core import pre_calculate_orderbook
from CORE.position_manager import PositionManager
from CORE.executor_process import run_executor_process

from c_log import log
from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.OKX.stakan import OkxStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.discovery import DiscoveryManager
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
        
        # Configs
        self.entry_desync_limit = self.cfg["trading_rules"]["entry"]["max_desync_ms"]
        self.top_n_candidates   = self.cfg["trading_rules"]["entry"]["top_n_candidates"]
        self.topology_rebuild_interval = self.cfg.get("topology_rebuild_interval_sec", None)
        
        self.funding_is_active = self.cfg["trading_rules"]["funding_filter"]["is_active"]
        self.funding_skip_sec = self.cfg["trading_rules"]["funding_filter"]["skip_if_less_than_sec"]
        self.funding_skip_after_sec = self.cfg["trading_rules"]["funding_filter"]["skip_if_after_while_sec"]
        
        self.banned_symbols = {}
        self._load_banned()

        self.discovery = DiscoveryManager(quote=self.cfg["QUOTE"])
        self.pm = None
        
        self.books = {ex: {} for ex in EXCHANGES}
        self.ts = {ex: {} for ex in EXCHANGES}
        self.event_ts = {ex: {} for ex in EXCHANGES}
        self.streams = {}
        
        self.route_names = []
        self.active_routes_array = None
        
        # IPC to Executor Process
        self.market_pipe, self.executor_pipe = mp.Pipe(duplex=True)
        self.executor_proc = None

    def _load_banned(self):
        if os.path.exists("banned_symbols.json"):
            try:
                with open("banned_symbols.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.banned_symbols = {sym: None for sym in data}
                    elif isinstance(data, dict):
                        now = time.time()
                        self.banned_symbols = {k: v for k, v in data.items() if v is None or v > now}
            except Exception as e:
                log(f"Error loading banned symbols: {e}", level="WARNING")

    def _make_depth_handler(self, exchange_name: str):
        async def on_depth(d):
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
        if current_sec <= self.funding_skip_sec or current_sec >= (3600 - self.funding_skip_after_sec):
            return True
        return False

    async def _handle_ipc_events(self):
        """Асинхронная вычитка событий и статусов из процесса исполнения."""
        loop = asyncio.get_running_loop()
        while True:
            try:
                has_data = await loop.run_in_executor(None, self.market_pipe.poll, 0.01)
                if has_data:
                    msg_type, payload = self.market_pipe.recv()
                    if msg_type == "POS_OPENED":
                        route = payload["route"]
                        sym = payload["sym"]
                        exec_res = payload["exec_res"]
                        open_time = payload["open_time"]
                        self.pm.confirm_entry(exec_res["long_ex"], exec_res["short_ex"], sym, exec_res, open_time)
                    elif msg_type == "POS_CLOSED":
                        route = payload["route"]
                        sym = payload["sym"]
                        self.pm.confirm_exit(route, sym)
                    elif msg_type == "BAN_UPDATE":
                        sym = payload["symbol"]
                        exp = payload["expire_time"]
                        self.banned_symbols[sym] = exp
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"[MarketProcess] Ошибка чтения IPC: {e}", level="WARNING")
            await asyncio.sleep(0.01)

    async def run(self):
        log("==================================================", level="INFO")
        log("🚀 ProdSpread v2 (2-Process HFT Architecture) Запуск!", level="INFO")
        log("==================================================", level="INFO")

        # 1. Запуск процесса исполнения (Executor Process)
        self.executor_proc = mp.Process(target=run_executor_process, args=(self.executor_pipe, self.cfg), daemon=True)
        self.executor_proc.start()
        log(f"[MAIN] Executor Process запущен (PID: {self.executor_proc.pid})", level="INFO")

        # 2. Построение топологии
        log("Initializing DiscoveryManager...", level="INFO")
        await self.discovery.build_topology(self.banned_symbols)
        
        active_routes_cfg = self.cfg.get("active_routes", {})
        self.route_names = list(active_routes_cfg.keys())
        self.active_routes_array = np.array([
            [EX_TO_IDX[r.split("_")[0]], EX_TO_IDX[r.split("_")[1]]]
            for r in self.route_names
            if active_routes_cfg.get(r, False)
        ], dtype=np.int64)

        active_symbols = list(self.discovery.active_pairs_map.keys())
        self.pm = PositionManager(self.cfg, EXCHANGES, self.route_names, active_symbols)

        # Передаем топологию и роуты в процесс исполнения
        self.market_pipe.send(("INIT_TOPOLOGY", {
            "coin_to_native": self.discovery.coin_to_native,
            "routes": self.route_names,
            "active_symbols": active_symbols
        }))

        # 3. Запуск публичных сокетов стаканов
        stream_classes = {
            "BINANCE": BinanceStakanStream,
            "KUCOIN": KucoinStakanStream,
            "OKX": OkxStakanStream,
            "BITGET": BitgetStakanStream
        }
        
        for ex, syms in self.discovery.ws_routes.items():
            if syms:
                stream_cls = stream_classes.get(ex)
                if stream_cls:
                    self.streams[ex] = stream_cls(syms)
                    handler = self._make_depth_handler(ex)
                    asyncio.create_task(self.streams[ex].run(handler))
                    log(f"Started {ex} Public Orderbook WS stream ({len(syms)} symbols)", level="INFO")

        # 4. Запуск обработчика входящих IPC событий
        asyncio.create_task(self._handle_ipc_events())

        # 5. Главный вычислительный цикл (Numba JIT)
        prices_array = np.full((7, 2), [np.inf, 0.0], dtype=np.float64)

        try:
            while True:
                iter_start = time.perf_counter()
                try:
                    now = time.time()
                    funding_skip = self._is_funding_skip()
                    
                    # --- EXIT / DECAY MONITORING ---
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
                        min_fill = self.cfg["trading_rules"]["entry"].get("min_fill_rate", 0.85)
                        
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
                            self.pm.request_exit(route, sym)
                            # Отправляем команду закрытия в Executor Process
                            self.market_pipe.send(("CMD_CLOSE", {
                                "route": route,
                                "sym": sym,
                                "exit_res": exit_res,
                                "duration_sec": duration_sec
                            }))

                    # --- ENTRY MONITORING ---
                    if not funding_skip:
                        for sym in self.discovery.active_pairs_map:
                            if sym in self.banned_symbols:
                                exp = self.banned_symbols[sym]
                                if exp is not None and now > exp:
                                    del self.banned_symbols[sym]
                                else:
                                    continue
                            
                            prices_array.fill(np.inf)
                            prices_array[:, 1] = 0.0
                            
                            for ex_name, ex_idx in EX_TO_IDX.items():
                                book = self.books[ex_name].get(sym)
                                if book and book.get("bids") and book.get("asks"):
                                    prices_array[ex_idx, 0] = book["asks"][0][0]
                                    prices_array[ex_idx, 1] = book["bids"][0][0]
                            
                            candidates = pre_calculate_orderbook(prices_array, self.active_routes_array, top_n=self.top_n_candidates)
                            
                            for cand in candidates:
                                long_idx, short_idx, est_spread = int(cand[0]), int(cand[1]), float(cand[2])
                                if long_idx < 0 or short_idx < 0:
                                    continue
                                long_ex, short_ex = IDX_TO_EX[long_idx], IDX_TO_EX[short_idx]
                                
                                if sym not in self.event_ts[long_ex] or sym not in self.event_ts[short_ex]:
                                    continue
                                
                                diff_ms = abs(self.event_ts[long_ex][sym] - self.event_ts[short_ex][sym])
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
                                        route = f"{long_ex}_{short_ex}"
                                        # Отправляем команду на открытие в Executor Process
                                        self.market_pipe.send(("CMD_OPEN", {
                                            "sym": sym,
                                            "route": route,
                                            "long_ex": long_ex,
                                            "short_ex": short_ex,
                                            "engine_res": engine_res
                                        }))
                                        break

                except asyncio.CancelledError:
                    raise
                except Exception as iter_ex:
                    log(f"Сбой в цикле (итерация пропущена): {iter_ex}", level="ERROR")
                    traceback.print_exc()
                
                await asyncio.sleep(self.cfg["MAIN_LOOP_DELAY"])

        except KeyboardInterrupt:
            log("⛔ Остановка по Ctrl+C", level="INFO")
        except Exception as ex:
            log(f"Сбой выполнения: {ex}", level="ERROR")
            traceback.print_exc()
        finally:
            log("Завершение работы Market Data Engine...", level="INFO")
            for stream in self.streams.values():
                await stream.aclose()
            await self.discovery.aclose()
            
            # Остановка Executor Process
            if self.market_pipe:
                try:
                    self.market_pipe.send(("SHUTDOWN", None))
                except Exception:
                    pass
            if self.executor_proc and self.executor_proc.is_alive():
                self.executor_proc.join(timeout=2.0)
                if self.executor_proc.is_alive():
                    self.executor_proc.terminate()

            await SessionManager().close_all()

if __name__ == "__main__":
    mp.freeze_support()
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