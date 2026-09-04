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

load_dotenv()

from CORE.utils import Utils
from CORE.trading_engine import TradingEngine
from CORE.math_core import pre_calculate_orderbook
from CORE.position_manager import PositionManager
from CORE.executor_process import run_executor_process
from CORE.ipc_socket import async_write_msg, async_read_msg

from c_log import log
from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.OKX.stakan import OkxStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.discovery import DiscoveryManager
from utils import SessionManager
from analytics import update_total_balance

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
        self.exit_desync_limit  = self.cfg["trading_rules"]["exit"].get("max_desync_ms")
        self.top_n_candidates   = self.cfg["trading_rules"]["entry"]["top_n_candidates"]
        self.min_signal_dwell_ms = float(self.cfg["trading_rules"]["entry"].get("min_signal_dwell_ms", 0.0))
        self._signal_first_seen = {}
        self.topology_rebuild_interval = self.cfg["topology_rebuild_interval_sec"]
        
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
        
        # Троттлинг диагностических логов выхода (раз в 5 секунд на символ)
        self._exit_log_ts = {}
        
        # IPC to Executor Process
        self.executor_writer = None
        self.server = None
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
                bids = d.bids
                asks = d.asks
                if exchange_name == "KUCOIN" and hasattr(self.discovery.apis.get("KUCOIN"), "multipliers"):
                    mult = self.discovery.apis["KUCOIN"].multipliers.get(base_coin, 1.0)
                    if mult != 1.0:
                        bids = [(p, q * mult) for p, q in bids]
                        asks = [(p, q * mult) for p, q in asks]

                self.books[exchange_name][base_coin] = {"bids": bids, "asks": asks}
                self.ts[exchange_name][base_coin] = time.monotonic()
                self.event_ts[exchange_name][base_coin] = getattr(d, 'event_time_ms', time.time()*1000)
        return on_depth

    def _is_funding_skip(self) -> bool:
        if not self.funding_is_active:
            return False
        current_sec = int(time.time()) % 3600
        if current_sec <= self.funding_skip_sec or current_sec >= (3600 - self.funding_skip_after_sec):
            return True
        return False

    async def _handle_ipc_events(self, reader, writer):
        """Асинхронная вычитка событий и статусов из процесса исполнения."""
        self.executor_writer = writer
        try:
            while True:
                msg_type, payload = await async_read_msg(reader)
                if msg_type == "POS_OPENED":
                    route = payload["route"]
                    sym = payload["sym"]
                    exec_res = payload["exec_res"]
                    open_time = payload["open_time"]
                    self.pm.confirm_entry(exec_res["long_ex"], exec_res["short_ex"], sym, exec_res, open_time)
                elif msg_type == "POS_FAILED":
                    long_ex = payload["long_ex"]
                    short_ex = payload["short_ex"]
                    sym = payload["sym"]
                    self.pm.rollback_entry(long_ex, short_ex, sym)
                elif msg_type == "POS_CLOSED":
                    route = payload["route"]
                    sym = payload["sym"]
                    self.pm.confirm_exit(route, sym)
                elif msg_type == "BAN_UPDATE":
                    sym = payload["symbol"]
                    exp = payload["expire_time"]
                    self.banned_symbols[sym] = exp
        except EOFError:
            log("[MarketProcess] Соединение с Executor разорвано.", level="WARNING")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log(f"[MarketProcess] Ошибка чтения IPC: {e}", level="WARNING")

    async def run(self):
        log("==================================================", level="INFO")
        log("🚀 ProdSpread v2 (2-Process HFT Architecture) Запуск!", level="INFO")
        log("==================================================", level="INFO")

        # 1. Запуск IPC сервера и процесса исполнения (Executor Process)
        self.server = await asyncio.start_server(self._handle_ipc_events, '127.0.0.1', 0)
        port = self.server.sockets[0].getsockname()[1]
        log(f"[MAIN] Запущен локальный IPC TCP сервер на порту {port}", level="INFO")

        self.executor_proc = mp.Process(target=run_executor_process, args=(port, self.cfg), daemon=True)
        self.executor_proc.start()
        log(f"[MAIN] Executor Process запущен (PID: {self.executor_proc.pid})", level="INFO")

        # Ждем пока Executor подключится (writer появится)
        while self.executor_writer is None:
            await asyncio.sleep(0.01)

        # 2. Построение топологии
        log("Initializing DiscoveryManager...", level="INFO")
        await self.discovery.build_topology(self.banned_symbols)
        
        for r_name, r_count in getattr(self.discovery, "route_symbol_counts", {}).items():
            log(f"[Topology] 📊 Связка {r_name}: {r_count} общих монет", level="INFO")
            
        active_routes_cfg = self.cfg["active_routes"]
        self.route_names = list(active_routes_cfg.keys())
        self.active_routes_array = np.array([
            [EX_TO_IDX[r.split("_")[0]], EX_TO_IDX[r.split("_")[1]]]
            for r in self.route_names
            if active_routes_cfg[r]
        ], dtype=np.int64)

        active_symbols = list(self.discovery.active_pairs_map.keys())
        self.pm = PositionManager(self.cfg, EXCHANGES, self.route_names, active_symbols)

        # Передаем топологию и роуты в процесс исполнения
        asyncio.create_task(async_write_msg(self.executor_writer, "INIT_TOPOLOGY", {
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

        # Initial balance write
        update_total_balance(self.cfg, is_startup=True)

        # 5. Главный вычислительный цикл (Numba JIT)
        prices_array = np.full((7, 2), [np.inf, 0.0], dtype=np.float64)

        try:
            while True:
                iter_start = time.perf_counter()
                try:
                    now = time.time()
                    now_mono = time.monotonic()
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
                        use_extreme = state["details"].get("use_extreme_decay", False)
                        active_decay_map = self.engine.extreme_decay_map if use_extreme else self.engine.decay_map

                        is_exit, exit_res = self.engine.evaluate_exit(
                            long_book, short_book, long_ex, short_ex,
                            state["details"]["engine_res"].get("long_qty", 0.0) * long_rate,
                            state["details"]["engine_res"].get("short_qty", 0.0) * short_rate,
                            state["details"].get("entry_long_price", 0.0),
                            state["details"].get("entry_short_price", 0.0),
                            duration_sec,
                            is_stakan_valid=is_stakan_valid,
                            long_executed_volume_rate=long_rate,
                            short_executed_volume_rate=short_rate,
                            decay_map=active_decay_map
                        )
                        
                        # Защита от фантомных импульсов на выходе (только для PROFIT_DECAY)
                        # Экстренные выходы (TTL, LOW_FILL_RATE) никогда не блокируются!
                        if is_exit and exit_res.get("reason") == "PROFIT_DECAY":
                            long_ts = self.ts[long_ex].get(sym, 0.0)
                            short_ts = self.ts[short_ex].get(sym, 0.0)
                            if long_ts > 0 and short_ts > 0:
                                diff_ms = abs(long_ts - short_ts) * 1000.0
                                if self.exit_desync_limit is not None and diff_ms > self.exit_desync_limit:
                                    is_exit = False
                                    exit_res["reason"] = f"EXIT_DESYNC_SKIP ({diff_ms:.0f}ms > {self.exit_desync_limit}ms)"
                        
                        # --- ДИАГНОСТИЧЕСКОЕ ЛОГИРОВАНИЕ ВЫХОДА (каждые 5 секунд на символ) ---
                        _now_log = time.time()
                        _last_log = self._exit_log_ts.get(sym, 0.0)
                        if _now_log - _last_log >= 5.0:
                            self._exit_log_ts[sym] = _now_log
                            _net = exit_res.get("net_yield")
                            _tgt = exit_res.get("target_val")
                            _spr = exit_res.get("vwap_spread_out")
                            _reason = exit_res.get("reason", "?")
                            _lcp = exit_res.get("long_close_price")
                            _scp = exit_res.get("short_close_price")
                            _elp = state["details"].get("entry_long_price", 0.0)
                            _esp = state["details"].get("entry_short_price", 0.0)
                            _lvl = exit_res.get("exit_level_index", "?")
                            
                            _net_s = f"{_net*100:+.4f}%" if _net is not None else "N/A"
                            _tgt_s = f"{_tgt*100:+.4f}%" if _tgt is not None else "TTL"
                            _spr_s = f"{_spr*100:+.4f}%" if _spr is not None else "N/A"
                            _lcp_s = f"{_lcp:.6f}" if _lcp else "N/A"
                            _scp_s = f"{_scp:.6f}" if _scp else "N/A"
                            
                            if is_exit:
                                log(f"[{sym}] 🔍 EXIT_SIGNAL: net={_net_s} tgt={_tgt_s} spread_out={_spr_s} | "
                                    f"L_close={_lcp_s} S_close={_scp_s} (entry L={_elp:.6f} S={_esp:.6f}) | "
                                    f"dur={duration_sec:.0f}s lvl={_lvl} fill=L:{long_rate*100:.0f}%/S:{short_rate*100:.0f}% | {_reason}", level="INFO")
                            else:
                                _gap = ""
                                if _net is not None and _tgt is not None:
                                    _gap = f" gap={(_net - _tgt)*100:+.4f}%"
                                log(f"[{sym}] 🔍 EXIT_HOLD: net={_net_s} tgt={_tgt_s}{_gap} spread_out={_spr_s} | "
                                    f"L_close={_lcp_s} S_close={_scp_s} (entry L={_elp:.6f} S={_esp:.6f}) | "
                                    f"dur={duration_sec:.0f}s lvl={_lvl} fill=L:{long_rate*100:.0f}%/S:{short_rate*100:.0f}% | SKIP: {_reason}", level="INFO")
                        
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
                            # Чистим троттл-лог при выходе
                            self._exit_log_ts.pop(sym, None)
                            self.pm.lock_for_exit(route, sym)
                            # Отправляем команду закрытия в Executor Process
                            if self.executor_writer:
                                asyncio.create_task(async_write_msg(self.executor_writer, "CMD_CLOSE", {
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
                                ts_val = self.ts[ex_name].get(sym, 0.0)
                                if book and book.get("bids") and book.get("asks"):
                                    if (now_mono - ts_val) <= 5.0:  # is stale check
                                        prices_array[ex_idx, 0] = book["asks"][0][0]
                                        prices_array[ex_idx, 1] = book["bids"][0][0]
                            
                            candidates = pre_calculate_orderbook(prices_array, self.active_routes_array, top_n=self.top_n_candidates)
                            
                            for cand in candidates:
                                long_idx, short_idx, est_spread = int(cand[0]), int(cand[1]), float(cand[2])
                                if long_idx < 0 or short_idx < 0:
                                    continue
                                long_ex, short_ex = IDX_TO_EX[long_idx], IDX_TO_EX[short_idx]
                                
                                if sym not in self.ts[long_ex] or sym not in self.ts[short_ex]:
                                    continue
                                
                                diff_ms = abs(self.ts[long_ex][sym] - self.ts[short_ex][sym]) * 1000.0
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
                                    
                                    route = f"{long_ex}_{short_ex}"
                                    sig_key = (route, sym)
                                    
                                    if is_valid_entry:
                                        # Проверка выдержки сигнала (Signal Dwell Time)
                                        if self.min_signal_dwell_ms > 0:
                                            first_seen = self._signal_first_seen.get(sig_key)
                                            if first_seen is None:
                                                self._signal_first_seen[sig_key] = now_mono
                                                continue
                                            dwell_ms = (now_mono - first_seen) * 1000.0
                                            if dwell_ms < self.min_signal_dwell_ms:
                                                continue
                                            # Выдержка подтверждена - сбрасываем ключ
                                            self._signal_first_seen.pop(sig_key, None)
                                            
                                        self.pm.lock_for_entry(long_ex, short_ex, sym, engine_res)
                                        # Отправляем команду на открытие в Executor Process
                                        if self.executor_writer:
                                            asyncio.create_task(async_write_msg(self.executor_writer, "CMD_OPEN", {
                                                "sym": sym,
                                                "route": route,
                                                "long_ex": long_ex,
                                                "short_ex": short_ex,
                                                "engine_res": engine_res
                                            }))
                                        break
                                    else:
                                        # Сигнал не подтвержден/пропал - сбрасываем таймер
                                        self._signal_first_seen.pop(sig_key, None)

                            if len(self._signal_first_seen) > 100:
                                self._signal_first_seen = {
                                    k: v for k, v in self._signal_first_seen.items()
                                    if (now_mono - v) <= 1.0
                                }

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
            if self.executor_writer:
                try:
                    asyncio.create_task(async_write_msg(self.executor_writer, "SHUTDOWN", None))
                except Exception:
                    pass
            if self.executor_proc and self.executor_proc.is_alive():
                self.executor_proc.join(timeout=2.0)
                if self.executor_proc.is_alive():
                    self.executor_proc.terminate()
            if self.server:
                self.server.close()
                await self.server.wait_closed()

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



# {
#     "TRUMP": null,
#     "VET": null,
#     "PROM": null,
#     "GIGGLE": null,
#     "ANTHROPIC": null,
#     "ONG": null 
# }
