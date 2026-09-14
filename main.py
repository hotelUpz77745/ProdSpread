# ============================================================
# FILE: main.py
# ROLE: Conductor (Process 1): Book collection, Discovery, Numba JIT signals and profit decay.
#       Orchestrates separate execution process (Process 2: ExecutorProcess).
# ============================================================

import asyncio
import time
import traceback
import os
import json
import numpy as np
from typing import Optional
from datetime import datetime
import multiprocessing as mp
from dotenv import load_dotenv

load_dotenv()

from CORE.utils import Utils
from CORE.trading_engine import TradingEngine
from CORE.math_core import pre_calculate_orderbook, OrderbookUtils
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
        signal_cfg = self.cfg.get("trading_rules", {}).get("entry", {}).get("signal_filters", {})
        if not signal_cfg and "exchanges" in self.cfg:
            for ex_c in self.cfg["exchanges"].values():
                if isinstance(ex_c, dict) and "entry" in ex_c and "signal_filters" in ex_c["entry"]:
                    signal_cfg = ex_c["entry"]["signal_filters"]
                    break
        self.routes_cfg = self.cfg["routes"]
        self.top_n_candidates   = signal_cfg.get("top_n_candidates", 4)
        self.min_signal_dwell_ms = float(signal_cfg.get("min_signal_dwell_ms", 0))
        self.min_top_depth_usd = float(signal_cfg.get("min_top_depth_usd", 50.0))
        self._signal_first_seen = {}
        self.topology_rebuild_interval = self.cfg["topology_rebuild_interval_sec"]
        
        self.funding_is_active = self.cfg["trading_rules"]["funding_filter"]["is_active"]
        self.funding_skip_sec = self.cfg["trading_rules"]["funding_filter"]["skip_if_less_than_sec"]
        self.funding_skip_after_sec = self.cfg["trading_rules"]["funding_filter"]["skip_if_after_while_sec"]
        
        self.banned_symbols = {}
        self._load_banned()

        self.discovery = DiscoveryManager(quote=self.cfg["QUOTE"], whitelist=self.cfg["SYMBOLS_GLOBAL_WHITELIST"])
        self.pm = None
        
        self.books = {ex: {} for ex in EXCHANGES}
        self.ts = {ex: {} for ex in EXCHANGES}
        self.event_ts = {ex: {} for ex in EXCHANGES}
        self.streams = {}
        
        self.route_names = []
        self.active_routes_array = None
        
        # Throttle diagnostic exit logs (once every 5s per symbol)
        self._exit_log_ts = {}
        logging_cfg = self.cfg.get("logging", {})
        self.radar_enabled = bool(logging_cfg.get("radar_enabled", False))
        self.radar_interval_sec = float(logging_cfg.get("radar_interval_sec", 15.0))
        self._last_radar_ts = time.monotonic() - (self.radar_interval_sec - 5.0)
        
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
                
                # Pipe tick data to engine for impulse detection
                self.engine.update_market_data(
                    sym=base_coin, 
                    ex=exchange_name, 
                    book=self.books[exchange_name][base_coin], 
                    ts_mono=self.ts[exchange_name][base_coin]
                )
        return on_depth

    def _is_funding_skip(self) -> bool:
        if not self.funding_is_active:
            return False
        # Funding occurs every 8 hours (00:00, 08:00, 16:00 UTC), not every hour
        current_sec = int(time.time()) % (8 * 3600)
        if current_sec <= self.funding_skip_sec or current_sec >= (8 * 3600 - self.funding_skip_after_sec):
            return True
        return False

    def _get_route_desync(self, oracle_ex: str, target_ex: str, is_entry: bool = True) -> Optional[float]:
        """
        Returns max_desync_ms threshold for a given route from cfg['routes'].
        """
        r1 = f"{oracle_ex}_{target_ex}"
        r2 = f"{target_ex}_{oracle_ex}"
        key = "max_desync_ms_entry" if is_entry else "max_desync_ms_exit"
        if r1 in self.routes_cfg:
            return float(self.routes_cfg[r1][key])
        elif r2 in self.routes_cfg:
            return float(self.routes_cfg[r2][key])
        return None

    def _get_desync_limit(self, limit_cfg, long_ex: str, short_ex: str) -> Optional[float]:
        """
        Returns max_desync_ms threshold for a given route.
        Supports both dict { "BINANCE_KUCOIN": 125, "BINANCE_BITGET": 200 }
        and scalar numeric value.
        """
        if isinstance(limit_cfg, dict):
            r1 = f"{long_ex}_{short_ex}"
            r2 = f"{short_ex}_{long_ex}"
            if r1 in limit_cfg:
                val = limit_cfg[r1]
                return float(val["max_desync_ms_entry"] if isinstance(val, dict) and "max_desync_ms_entry" in val else val)
            if r2 in limit_cfg:
                val = limit_cfg[r2]
                return float(val["max_desync_ms_entry"] if isinstance(val, dict) and "max_desync_ms_entry" in val else val)
            raise KeyError(f"Neither '{r1}' nor '{r2}' found in max_desync_ms config")
        elif limit_cfg is not None:
            return float(limit_cfg)
        return None

    def _emit_radar(self):
        try:
            total_pairs = len(self.discovery.active_pairs_map)
            spread_list = []
            
            for sym in self.discovery.active_pairs_map:
                for route_key, roles in self.engine.exchange_roles.items():
                    oracle_ex = roles["oracle"]
                    target_ex = roles["target"]
                    o_book = self.books.get(oracle_ex, {}).get(sym)
                    t_book = self.books.get(target_ex, {}).get(sym)
                    if not o_book or not t_book:
                        continue
                    o_bids = o_book.get("bids")
                    o_asks = o_book.get("asks")
                    t_bids = t_book.get("bids")
                    t_asks = t_book.get("asks")
                    if not o_bids or not o_asks or not t_bids or not t_asks:
                        continue
                    # Long Target: Oracle Bid vs Target Ask
                    sp_long = (float(o_bids[0][0]) - float(t_asks[0][0])) / float(o_bids[0][0])
                    # Short Target: Target Bid vs Oracle Ask
                    sp_short = (float(t_bids[0][0]) - float(o_asks[0][0])) / float(t_bids[0][0])
                    raw_sp = max(sp_long, sp_short)
                    comm = self.engine._get_fee(oracle_ex) + self.engine._get_fee(target_ex)
                    net_sp = raw_sp - comm
                    spread_list.append((sym, net_sp))
                    
            best_by_sym = {}
            for sym, net_sp in spread_list:
                if sym not in best_by_sym or net_sp > best_by_sym[sym]:
                    best_by_sym[sym] = net_sp
                    
            top_syms = sorted(best_by_sym.items(), key=lambda x: x[1], reverse=True)[:3]
            top_str = ", ".join([f"{s} ({net*100:+.2f}%)" for s, net in top_syms]) if top_syms else "N/A"
            
            kuc_gate = self.engine.get_spread_entry_base("KUCOIN") * 100
            bitg_gate = self.engine.get_spread_entry_base("BITGET") * 100
            
            open_pos = self.pm.get_open_positions() if self.pm else []
            if open_pos:
                status = f"ПОЗИЦИЯ ОТКРЫТА ({len(open_pos)})"
            else:
                status = "ОЖИДАНИЕ ИМПУЛЬСА"
                
            time_str = datetime.now().strftime("%H:%M:%S")
            radar_msg = (
                f"[RADAR {time_str}] Мониторинг: {total_pairs} пар | "
                f"ТОП спреды: {top_str} | "
                f"Порог: KuC >= {kuc_gate:.2f}%, Bitg >= {bitg_gate:.2f}% | "
                f"Статус: {status}"
            )
            print(radar_msg, flush=True)
            log(radar_msg, level="INFO")
        except Exception:
            pass

    async def _handle_ipc_events(self, reader, writer):
        """Async reader for events and statuses from executor process."""
        self.executor_writer = writer
        try:
            while True:
                msg_type, payload = await async_read_msg(reader)
                try:
                    if msg_type == "POS_OPENED":
                        route = payload["route"]
                        sym = payload["sym"]
                        exec_res = payload["exec_res"]
                        open_time = payload["open_time"]
                        oracle_ex = exec_res.get("oracle_ex") or exec_res.get("long_ex")
                        target_ex = exec_res.get("target_ex") or exec_res.get("short_ex")
                        self.pm.confirm_entry(oracle_ex, target_ex, sym, exec_res, open_time)
                    elif msg_type == "POS_FAILED":
                        oracle_ex = payload.get("oracle_ex") or payload.get("long_ex")
                        target_ex = payload.get("target_ex") or payload.get("short_ex")
                        sym = payload["sym"]
                        self.pm.rollback_entry(oracle_ex, target_ex, sym)
                    elif msg_type == "POS_CLOSED":
                        route = payload["route"]
                        sym = payload["sym"]
                        self.pm.confirm_exit(route, sym)
                    elif msg_type == "POS_EXIT_FAILED":
                        route = payload["route"]
                        sym = payload["sym"]
                        self.pm.rollback_exit(route, sym)
                    elif msg_type == "BAN_UPDATE":
                        sym = payload["symbol"]
                        exp = payload["expire_time"]
                        self.banned_symbols[sym] = exp
                except Exception as msg_err:
                    log(f"[MarketProcess] Error handling IPC event {msg_type}: {msg_err}", level="ERROR")
        except EOFError:
            log("[MarketProcess] Connection to Executor closed.", level="WARNING")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log(f"[MarketProcess] IPC read error: {e}", level="WARNING")

    async def run(self):
        log("==================================================", level="INFO")
        log("ProdSpread v2 (2-Process HFT Architecture) Startup!", level="INFO")
        log("==================================================", level="INFO")

        # 1. Launch IPC server and Executor Process
        self.server = await asyncio.start_server(self._handle_ipc_events, '127.0.0.1', 0)
        port = self.server.sockets[0].getsockname()[1]
        log(f"[MAIN] Local IPC TCP server listening on port {port}", level="INFO")

        self.executor_proc = mp.Process(target=run_executor_process, args=(port, self.cfg), daemon=True)
        self.executor_proc.start()
        log(f"[MAIN] Executor Process launched (PID: {self.executor_proc.pid})", level="INFO")

        # Wait until Executor connects (writer available)
        while self.executor_writer is None:
            await asyncio.sleep(0.01)

        # 2. Build topology
        log("Initializing DiscoveryManager...", level="INFO")
        await self.discovery.build_topology(self.banned_symbols)
        
        for r_name, r_count in getattr(self.discovery, "route_symbol_counts", {}).items():
            log(f"[Topology] Route {r_name}: {r_count} common symbols", level="INFO")
            
        routes_cfg = self.cfg["routes"]
        self.route_names = list(routes_cfg.keys())
        self.active_routes_array = np.array([
            [EX_TO_IDX[r.split("_")[0]], EX_TO_IDX[r.split("_")[1]]]
            for r in self.route_names
            if routes_cfg[r]["active"]
        ], dtype=np.int64)

        active_symbols = list(self.discovery.active_pairs_map.keys())
        self.pm = PositionManager(self.cfg, EXCHANGES, self.route_names, active_symbols)

        # Send topology and routes to executor process
        asyncio.create_task(async_write_msg(self.executor_writer, "INIT_TOPOLOGY", {
            "coin_to_native": self.discovery.coin_to_native,
            "routes": self.route_names,
            "active_symbols": active_symbols
        }))

        # 3. Launch public orderbook streams
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

        # Balance tracking moved to ExecutorProcess only (avoids file race condition)
        # update_total_balance(self.cfg, is_startup=True)

        # 5. Main calculation loop
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
                        
                        details = state.get("details", {})
                        target_ex = details.get("target_ex") or details.get("short_ex")
                        oracle_ex = details.get("oracle_ex") or details.get("long_ex")
                        side = details.get("side") or ("LONG" if details.get("entry_long_price") else "SHORT")
                        
                        entry_price = float(details.get("entry_price") or details.get("entry_long_price") or details.get("entry_short_price", 0.0))
                        qty = float(details.get("qty") or details.get("long_qty") or details.get("short_qty", 0.0))
                        net_spread = float(details.get("net_spread") or details.get("actual_net_spread") or 0.0)
                        
                        if not target_ex or not oracle_ex or entry_price <= 0.0 or qty <= 0.0:
                            continue
                            
                        oracle_book = self.books.get(oracle_ex, {}).get(sym, {})
                        target_book = self.books.get(target_ex, {}).get(sym, {})
                        
                        is_emergency_prev = state["details"].get("use_emergency_decay", False)
                        emergency_since = state["details"].get("emergency_since")
                        emergency_duration = (now - emergency_since) if emergency_since is not None else None
                        
                        is_exit, exit_res = self.engine.evaluate_exit_v9(
                            target_book=target_book,
                            target_ex=target_ex,
                            entry_price=entry_price,
                            qty=qty,
                            side=side,
                            duration_sec=duration_sec,
                            actual_net_spread_entry=net_spread,
                            oracle_book=oracle_book,
                            oracle_ex=oracle_ex,
                            is_emergency=is_emergency_prev,
                            emergency_duration_sec=emergency_duration
                        )
                        
                        # Handle transition into emergency decay
                        if exit_res.get("use_emergency_decay") and not is_emergency_prev:
                            state["details"]["use_emergency_decay"] = True
                            state["details"]["emergency_since"] = now
                            orcl_sp = exit_res.get("oracle_net_spread")
                            sp_str = f"{orcl_sp*100:+.3f}%" if orcl_sp is not None else "N/A"
                            log(f"[{sym}] ⚠️ SPREAD EVAPORATED (Oracle Net: {sp_str} < Min: {self.engine.min_spread_entry*100:.3f}%). "
                                f"Switching to EMERGENCY DECAY map (exit in <= {self.engine.emergency_ttl_sec:.1f}s)!", level="WARNING")
                        
                        # Protection against phantom spikes on exit ONLY for TAKE_PROFIT.
                        # Emergency exits (STOP_LOSS, TTL, EMERGENCY_TTL) must NEVER be blocked!
                        if is_exit and exit_res.get("reason") in ("TAKE_PROFIT", "EMERGENCY_BREAKEVEN"):
                            oracle_ts = self.ts[oracle_ex].get(sym, 0.0)
                            target_ts = self.ts[target_ex].get(sym, 0.0)
                            if oracle_ts > 0 and target_ts > 0:
                                diff_ms = abs(oracle_ts - target_ts) * 1000.0
                                limit = self._get_route_desync(oracle_ex, target_ex, is_entry=False)
                                if limit is not None and diff_ms > limit:
                                    is_exit = False
                                    exit_res["reason"] = f"EXIT_DESYNC_SKIP ({diff_ms:.0f}ms > {limit:.0f}ms)"
                        
                        # --- DIAGNOSTIC EXIT LOGGING (every 5 seconds per symbol) ---
                        _now_log = time.time()
                        _last_log = self._exit_log_ts.get(sym, 0.0)
                        if _now_log - _last_log >= 5.0:
                            self._exit_log_ts[sym] = _now_log
                            _net = exit_res.get("net_pnl_ratio", exit_res.get("net_pnl_pct"))
                            _tgt = exit_res.get("target_val")
                            _reason = exit_res.get("reason", "?")
                            _ep = state["details"].get("entry_price", 0.0)
                            _lvl = exit_res.get("exit_level_index", "?")
                            _is_emerg = state["details"].get("use_emergency_decay", False)
                            _mode = "⚠️EMERGENCY" if _is_emerg else "STANDARD"
                            _orcl_sp = exit_res.get("oracle_net_spread")
                            _orcl_str = f" OrclSp:{_orcl_sp*100:+.3f}%" if _orcl_sp is not None else ""
                            
                            _net_s = f"{_net*100:+.4f}%" if _net is not None else "N/A"
                            _tgt_s = f"{_tgt*100:+.4f}%" if _tgt is not None else "TTL"
                            _ep_s = f"{_ep:.6f}" if _ep else "N/A"
                            
                            if is_exit:
                                log(f"[{sym}] EXIT_SIGNAL ({_mode}): net={_net_s} tgt={_tgt_s}{_orcl_str} | "
                                    f"Entry={_ep_s} | "
                                    f"dur={duration_sec:.0f}s lvl={_lvl} | {_reason}", level="INFO")
                            else:
                                _gap = ""
                                if _net is not None and _tgt is not None:
                                    _gap = f" gap={(_net - _tgt)*100:+.4f}%"
                                log(f"[{sym}] EXIT_HOLD ({_mode}): net={_net_s} tgt={_tgt_s}{_gap}{_orcl_str} | "
                                    f"Entry={_ep_s} | "
                                    f"dur={duration_sec:.0f}s lvl={_lvl} | SKIP: {_reason}", level="INFO")
                        
                        current_level = state["details"].get("exit_level_index", 0)
                        new_level = exit_res.get("exit_level_index", current_level)
                        
                        if new_level > current_level:
                            state["details"]["exit_level_index"] = new_level
                            target_val = exit_res.get("target_val")
                            if target_val is None or target_val <= -999.0:
                                log(f"[{sym}] Profit decay: Level {new_level} (TTL forced exit)", level="WARNING")
                            else:
                                log(f"[{sym}] Profit decay: Level {new_level}, target: {target_val * 100:.3f}%", level="INFO")
                        
                        if is_exit:
                            self._exit_log_ts.pop(sym, None)
                            self.pm.lock_for_exit(route, sym)
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
                                    
                            for route_key, roles in self.engine.exchange_roles.items():
                                oracle_ex = roles["oracle"]
                                target_ex = roles["target"]
                                
                                oracle_book = self.books[oracle_ex].get(sym)
                                target_book = self.books[target_ex].get(sym)
                                
                                if not oracle_book or not target_book:
                                    continue
                                    
                                oracle_ts = self.ts[oracle_ex].get(sym, 0.0)
                                target_ts = self.ts[target_ex].get(sym, 0.0)
                                
                                if (now_mono - oracle_ts) > 5.0 or (now_mono - target_ts) > 5.0:
                                    continue
                                    
                                diff_ms = abs(oracle_ts - target_ts) * 1000.0
                                limit = self._get_route_desync(oracle_ex, target_ex, is_entry=True)
                                if limit is not None and diff_ms > limit:
                                    continue

                                if self.pm.can_enter(oracle_ex, target_ex, sym):
                                    target_cfg = self.engine.get_exchange_config(target_ex)
                                    if "trading_risks" in target_cfg:
                                        size_usd = float(target_cfg["trading_risks"]["trade_size_usd"])
                                    elif "trading_risks" in self.cfg and target_ex.lower() in self.cfg["trading_risks"]:
                                        size_usd = float(self.cfg["trading_risks"][target_ex.lower()]["trade_size_usd"])
                                    else:
                                        size_usd = float(self.cfg["exchanges"][target_ex.upper()]["trading_risks"]["trade_size_usd"])
                                    
                                    is_valid_entry, engine_res = self.engine.evaluate_entry_v9(
                                        sym, oracle_book, target_book, oracle_ex, target_ex, size_usd
                                    )
                                    
                                    sig_key = (route_key, sym)
                                    
                                    if is_valid_entry:
                                        pre_min = self.engine.get_spread_entry_pre_min(target_ex)
                                        if pre_min is None:
                                            pre_min = self.engine.get_spread_entry_base(target_ex)
                                        min_dwell = self.engine.get_min_signal_dwell_ms(target_ex)
                                        if min_dwell > 0:
                                            first_seen = self._signal_first_seen.get(sig_key)
                                            if first_seen is None:
                                                # Pre-filter: MUST meet spread_entry_pre_min to start dwelling
                                                if pre_min is None or engine_res["net_spread"] >= pre_min:
                                                    self._signal_first_seen[sig_key] = now_mono
                                                continue
                                            dwell_ms = (now_mono - first_seen) * 1000.0
                                            if dwell_ms < min_dwell:
                                                continue
                                            self._signal_first_seen.pop(sig_key, None)
                                        else:
                                            # Instant entry requires strict pre-filter
                                            if pre_min is not None and engine_res["net_spread"] < pre_min:
                                                continue
                                            
                                        canonical_route = self.pm._normalize_route(oracle_ex, target_ex)
                                        self.pm.lock_for_entry(oracle_ex, target_ex, sym, engine_res)
                                        
                                        if self.executor_writer:
                                            asyncio.create_task(async_write_msg(self.executor_writer, "CMD_OPEN", {
                                                "sym": sym,
                                                "route": canonical_route,
                                                "long_ex": oracle_ex,   # keep for backward compatibility with PositionManager
                                                "short_ex": target_ex,
                                                "target_ex": target_ex,
                                                "oracle_ex": oracle_ex,
                                                "engine_res": engine_res
                                            }))
                                    else:
                                        self._signal_first_seen.pop(sig_key, None)

                        if len(self._signal_first_seen) > 100:
                            self._signal_first_seen = {
                                k: v for k, v in self._signal_first_seen.items()
                                if (now_mono - v) <= 1.0
                            }

                    # --- RADAR / HEARTBEAT ---
                    if self.radar_enabled and self.radar_interval_sec > 0:
                        if now_mono - self._last_radar_ts >= self.radar_interval_sec:
                            self._last_radar_ts = now_mono
                            self._emit_radar()

                except asyncio.CancelledError:
                    raise
                except Exception as iter_ex:
                    log(f"Calculation loop error (iteration skipped): {iter_ex}", level="ERROR")
                    traceback.print_exc()
                
                await asyncio.sleep(self.cfg["MAIN_LOOP_DELAY"])

        except KeyboardInterrupt:
            log("Stopping via Ctrl+C", level="INFO")
        except Exception as ex:
            log(f"Execution error: {ex}", level="ERROR")
            traceback.print_exc()
        finally:
            log("Shutting down Market Data Engine...", level="INFO")
            for stream in self.streams.values():
                await stream.aclose()
            await self.discovery.aclose()
            
            # Stop Executor Process
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


## CHEAT SHEET (DO NOT DELETE)
# # chmod 600 ssh_key.txt
# # eval "$(ssh-agent -s)" 
# # ssh-add ssh_key.txt
# # git remote set-url origin git@github.com:hotelUpz/uranus_bot.git
# # source .ssh-autostart.sh
# In Git Bash terminal inside project directory:
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
