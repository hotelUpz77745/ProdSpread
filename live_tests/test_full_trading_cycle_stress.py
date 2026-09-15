# ============================================================
# FILE: live_tests/test_full_trading_cycle_stress.py
# ROLE: Exhaustive end-to-end integration and stress test across
#       all 8 nodes and subsystems of the complete trading cycle.
# ============================================================

import unittest
import asyncio
import time
import os
import sys
import json
import numpy as np
from unittest.mock import MagicMock, AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from CORE.math_core import (
    calc_vwap_usd_jit,
    calc_vwap_qty_jit,
    calc_vwap_and_deepest_price_jit,
    calc_execution_qty_limit_jit,
    OrderbookUtils,
    StaticDetector,
    OrderbookHunter
)
from CORE.trading_engine import TradingEngine
from CORE.position_manager import PositionManager
from CORE.position_fsm import PositionFSM, PositionState
from CORE.ipc_socket import async_write_msg, async_read_msg
from analytics import TradeAnalytics, update_total_balance
from API.discovery import to_native


class TestFullTradingCycleStress(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = {
            "SYMBOLS_GLOBAL_WHITELIST": [],
            "QUOTE": "USDT",
            "MAIN_LOOP_DELAY": 0.0,
            "setup_margin_leverage": True,
            "margin_settings": {
                "BINANCE": {"leverage": 20, "margin_type": "CROSSED"},
                "BITGET": {"leverage": 20, "margin_type": "crossed"},
                "KUCOIN": {"leverage": 20, "margin_type": "cross"}
            },
            "routes": {
                "BINANCE_BITGET": {
                    "active": True,
                    "oracle": "BINANCE",
                    "target": "BITGET",
                    "max_desync_ms_entry": 200,
                    "max_desync_ms_exit": 450,
                    "fill_confirm_timeout_sec": 0.05,
                    "market_close_confirm_timeout_sec": 0.05
                },
                "BINANCE_KUCOIN": {
                    "active": True,
                    "oracle": "BINANCE",
                    "target": "KUCOIN",
                    "max_desync_ms_entry": 350,
                    "max_desync_ms_exit": 500,
                    "fill_confirm_timeout_sec": 0.05,
                    "market_close_confirm_timeout_sec": 0.05
                }
            },
            "exchanges": {
                "BINANCE": {
                    "trading_risks": {
                        "paper_start_balance": 100.0,
                        "trade_size_usd": 25.0,
                        "taker_fee": 0.0005,
                        "limit_slip_ratio": 0.0015,
                        "volatility_discount_entry": 0.5,
                        "volatility_discount_exit": 0.85,
                        "max_positions": 1
                    },
                    "entry": {
                        "signal_filters": {
                            "static_detector": {"enabled": True, "static_leg": "TARGET", "max_static_leg_ratio": 0.0020, "buffer_window_sec": 0.35},
                            "spread_entry_pre": [0.008, 0.05],
                            "spread_entry_base": 0.006,
                            "min_top_depth_usd": 50.0,
                            "min_signal_dwell_ms": 0,
                            "synthetic_exit": {"enabled": True, "check_slippage": True, "max_slippage_ratio": 0.30, "hard_max_slippage": 0.008}
                        }
                    },
                    "exit": {
                        "target_exit": {
                            "exit_order_type": "LIMIT_IOC",
                            "stop_loss_ratio": 0.0050,
                            "min_spread_entry": 0.0030,
                            "orderbook_hunting": {
                                "base_scenario": {"enabled": True, "target_rate": 0.80, "shift_demotion": 0.20, "min_target_rate": 0.40, "shift_ttl_sec": 3.0},
                                "breakeven_stage": {"enabled": True, "ttl_sec": 7.0, "min_net_profit_ratio": 0.0004, "wait_sec": 2.0},
                                "extrime_close": {"enabled": True, "retry_ttl_sec": 0.01, "max_retries": 10, "bid_to_ask_orientation": 0.0, "increase_fraction": 0.05},
                                "oracle_reversal_stop": {"enabled": True, "max_oracle_adverse_ratio": 0.0040}
                            }
                        }
                    }
                },
                "BITGET": {
                    "trading_risks": {
                        "paper_start_balance": 100.0,
                        "trade_size_usd": 25.0,
                        "taker_fee": 0.0006,
                        "limit_slip_ratio": 0.0015,
                        "volatility_discount_entry": 0.5,
                        "volatility_discount_exit": 0.85,
                        "max_positions": 1
                    },
                    "entry": {
                        "signal_filters": {
                            "static_detector": {"enabled": True, "static_leg": "TARGET", "max_static_leg_ratio": 0.0020, "buffer_window_sec": 0.35},
                            "spread_entry_pre": [0.008, 0.05],
                            "spread_entry_base": 0.008,
                            "min_top_depth_usd": 40.0,
                            "min_signal_dwell_ms": 0,
                            "synthetic_exit": {"enabled": True, "check_slippage": True, "max_slippage_ratio": 0.30, "hard_max_slippage": 0.008}
                        }
                    },
                    "exit": {
                        "target_exit": {
                            "exit_order_type": "LIMIT_IOC",
                            "stop_loss_ratio": 0.0050,
                            "min_spread_entry": 0.0030,
                            "orderbook_hunting": {
                                "base_scenario": {"enabled": True, "target_rate": 0.80, "shift_demotion": 0.20, "min_target_rate": 0.40, "shift_ttl_sec": 3.0},
                                "breakeven_stage": {"enabled": True, "ttl_sec": 7.0, "min_net_profit_ratio": 0.0004, "wait_sec": 2.0},
                                "extrime_close": {"enabled": True, "retry_ttl_sec": 0.01, "max_retries": 10, "bid_to_ask_orientation": 0.0, "increase_fraction": 0.05},
                                "oracle_reversal_stop": {"enabled": True, "max_oracle_adverse_ratio": 0.0040}
                            }
                        }
                    }
                }
            },
            "trading_rules": {
                "ban_rules": {
                    "is_active": True,
                    "perm_ban_loss_ratio": 0.0075,
                    "max_consecutive_losses": 2,
                    "quarantine_sec": {"entry_error": 60, "zero_fill": 10, "loss_trade": 300}
                },
                "emergency_unwind": {
                    "max_attempts": 2,
                    "retry_pause_sec": 0.01,
                    "ws_verify_timeout_sec": 0.02
                }
            }
        }
        self.engine = TradingEngine(self.cfg, {0: "BINANCE", 1: "BITGET"})

    # ============================================================
    # NODE 1: Topology & Native Symbol Mapping
    # ============================================================
    def test_node_1_native_mapping(self):
        """Проверка маппинга тикеров в нативные форматы бирж."""
        self.assertEqual(to_native("BTC", "BINANCE"), "BTCUSDT")
        self.assertEqual(to_native("BTC", "BITGET"), "BTCUSDT")
        self.assertEqual(to_native("BTC", "KUCOIN"), "XBTUSDTM")
        self.assertEqual(to_native("BTC", "OKX"), "BTC-USDT-SWAP")
        self.assertEqual(to_native("ETH", "KUCOIN"), "ETHUSDTM")

    # ============================================================
    # NODE 2: JIT Math Core & Orderbook Calculations
    # ============================================================
    def test_node_2_math_core_jit_calculations(self):
        """Проверка точности и устойчивости Numba JIT функций книги заявок."""
        # 2 уровня: 100 @ 1.0 ($100), 101 @ 2.0 ($202)
        book = np.array([[100.0, 1.0], [101.0, 2.0]], dtype=np.float64)
        
        # VWAP USD: для $150 -> $100 по 100 (1.0 шт) + $50 по 101 (0.49505 шт) -> VWAP = 150 / 1.49505 = 100.3311
        vwap_usd = calc_vwap_usd_jit(book, 150.0, 1.0)
        self.assertAlmostEqual(vwap_usd, 150.0 / (1.0 + (50.0 / 101.0)), places=4)

        # VWAP Qty: для 1.5 шт -> 1.0 шт по 100 ($100) + 0.5 шт по 101 ($50.5) -> $150.5 / 1.5 = 100.3333
        vwap_qty = calc_vwap_qty_jit(book, 1.5, 1.0)
        self.assertAlmostEqual(vwap_qty, 150.5 / 1.5, places=4)

        # Deepest touched price
        v_price, deepest = calc_vwap_and_deepest_price_jit(book, 1.5, 1.0)
        self.assertEqual(deepest, 101.0)

        # Limit order constraint: buy with limit 100.5 (only 1st level filled)
        lim_vwap, filled = calc_execution_qty_limit_jit(book, 2.0, limit_price=100.5, is_buy=True, volatility_discount=1.0)
        self.assertEqual(filled, 1.0)
        self.assertEqual(lim_vwap, 100.0)

        # Edge case: Insufficient volume -> returns 0.0
        self.assertEqual(calc_vwap_usd_jit(book, 1000.0, 1.0), 0.0)
        self.assertEqual(calc_vwap_qty_jit(book, 10.0, 1.0), 0.0)

    # ============================================================
    # NODE 3: Microstructure & StaticDetector Stability Checks
    # ============================================================
    def test_node_3_static_detector_cases(self):
        """Проверка детектора покоя: строго Case B, отклонение Case C и Case A."""
        det = StaticDetector(self.cfg)
        t = time.monotonic()
        
        # Initial baseline: 100.0 on both
        det.evaluate_pair_stability("SOL", "BINANCE", "BITGET", 100.0, 100.0, 0.008, ts_mono=t)

        # Case B: Oracle fires to 101.0 (+1.0%), Target stays at 100.05 (+0.05%) -> ACCEPT
        ok_b, reason_b, info_b = det.evaluate_pair_stability("SOL", "BINANCE", "BITGET", 101.0, 100.05, 0.008, ts_mono=t+0.1)
        self.assertTrue(ok_b)
        self.assertEqual(info_b["case"], "CASE_B")

        # Case C: Target moves to 101.0 (+1.0%), Oracle stays at 100.05 (+0.05%) -> REJECT
        det_c = StaticDetector(self.cfg)
        det_c.evaluate_pair_stability("AVAX", "BINANCE", "BITGET", 100.0, 100.0, 0.008, ts_mono=t)
        ok_c, reason_c, info_c = det_c.evaluate_pair_stability("AVAX", "BINANCE", "BITGET", 100.05, 101.0, 0.008, ts_mono=t+0.1)
        self.assertFalse(ok_c)
        self.assertIn("CASE_C_REJECTED", reason_c)

        # Case A: Both move (Oracle 102.0, Target 100.8) with spread 1.2% > 0.8% -> REJECT
        det_a = StaticDetector(self.cfg)
        det_a.evaluate_pair_stability("NEAR", "BINANCE", "BITGET", 100.0, 100.0, 0.008, ts_mono=t)
        ok_a, reason_a, info_a = det_a.evaluate_pair_stability("NEAR", "BINANCE", "BITGET", 102.0, 100.8, 0.008, ts_mono=t+0.1)
        self.assertFalse(ok_a)
        self.assertIn("CASE_A_REJECTED", reason_a)

    # ============================================================
    # NODE 4: Signal Evaluation & Synthetic Exit Filtering
    # ============================================================
    def test_node_4_signal_evaluation_and_synthetic_exit(self):
        """Проверка глубокой эвалюации входа и фильтрации широких/дырявых стаканов."""
        t = time.monotonic()
        det = self.engine.get_static_detector("BITGET")
        det._quiescent_baselines.clear()
        det.evaluate_pair_stability("TEST_COIN", "BINANCE", "BITGET", 100.0, 100.0, 0.008, ts_mono=t)

        oracle_book = {
            "bids": [[101.20, 10.0]],
            "asks": [[101.25, 10.0]]
        }

        # 1. Wide book where reverse exit slips > 30% of spread -> REJECT
        wide_target_book = {
            "bids": [[99.95, 0.001], [99.94, 0.001], [99.93, 0.001], [98.0, 10.0]],
            "asks": [[100.05, 10.0]]
        }
        ok_wide, res_wide = self.engine.evaluate_entry_v9("TEST_COIN", oracle_book, wide_target_book, "BINANCE", "BITGET", 25.0)
        self.assertFalse(ok_wide)
        self.assertTrue("HIGH_REVERSE_SLIPPAGE" in res_wide["reason"] or "HARD_SLIPPAGE_LIMIT" in res_wide["reason"])

        # 2. Liquid tight book -> APPROVE
        det._quiescent_baselines.clear()
        det.evaluate_pair_stability("TEST_COIN", "BINANCE", "BITGET", 100.0, 100.0, 0.008, ts_mono=time.monotonic())
        liquid_target_book = {
            "bids": [[99.95, 10.0]],
            "asks": [[100.05, 10.0]]
        }
        ok_liq, res_liq = self.engine.evaluate_entry_v9("TEST_COIN", oracle_book, liquid_target_book, "BINANCE", "BITGET", 25.0)
        self.assertTrue(ok_liq)
        self.assertEqual(res_liq["side"], "LONG")
        self.assertGreater(res_liq["net_spread"], 0.008)

    # ============================================================
    # NODE 5: PositionManager Lifecycle & Semaphore Locks
    # ============================================================
    def test_node_5_position_manager_locks_and_persistence(self):
        """Проверка семафоров, блокировки маршрутов и персистентности PositionManager."""
        temp_state_file = "test_active_positions_stress.json"
        if os.path.exists(temp_state_file):
            os.remove(temp_state_file)

        pm = PositionManager(self.cfg, ["BINANCE", "BITGET"], ["BINANCE_BITGET"], ["BTC", "ETH"], state_file=temp_state_file)
        
        # Route is open
        self.assertTrue(pm.can_enter("BINANCE", "BITGET", "BTC"))

        # Lock for entry
        pm.lock_for_entry("BINANCE", "BITGET", "BTC", {"net_spread": 0.010})
        # Cannot enter BTC again while pending
        self.assertFalse(pm.can_enter("BINANCE", "BITGET", "BTC"))
        # Traded exchange BITGET reached max_positions (1), so route locks for ETH too
        self.assertFalse(pm.can_enter("BINANCE", "BITGET", "ETH"))

        # Confirm entry
        pm.confirm_entry("BINANCE", "BITGET", "BTC", {"side": "LONG", "entry_price": 100.0, "qty": 1.0, "net_spread": 0.010}, time.time())
        open_pos = pm.get_open_positions()
        self.assertEqual(len(open_pos), 1)
        self.assertEqual(open_pos[0][1], "BTC")

        # Reload PM from disk to verify crash recovery
        pm_recovered = PositionManager(self.cfg, ["BINANCE", "BITGET"], ["BINANCE_BITGET"], ["BTC", "ETH"], state_file=temp_state_file)
        rec_pos = pm_recovered.get_open_positions()
        self.assertEqual(len(rec_pos), 1)
        self.assertEqual(rec_pos[0][1], "BTC")

        # Lock for exit and confirm exit
        pm.lock_for_exit("BINANCE_BITGET", "BTC")
        pm.confirm_exit("BINANCE_BITGET", "BTC")
        self.assertEqual(len(pm.get_open_positions()), 0)
        self.assertTrue(pm.can_enter("BINANCE", "BITGET", "BTC"))

        if os.path.exists(temp_state_file):
            os.remove(temp_state_file)

    # ============================================================
    # NODE 6: PositionFSM 4-Stage Exit & Guaranteed MARKET Fallback
    # ============================================================
    async def test_node_6_position_fsm_full_exit_lifecycle(self):
        """Проверка FSM выходов: Virtual TP, Breakeven, 10 retries Extrime Close, Emergency Sweep."""
        mock_order = MagicMock()
        placed_orders = []

        async def mock_place(*args, **kwargs):
            placed_orders.append(kwargs)
            return {"status": "ok"}

        mock_order.place_order = AsyncMock(side_effect=mock_place)
        mock_order.get_last_close_price.return_value = 100.0
        mock_order.cancel_all_orders = AsyncMock()

        # Scenario A: Take Profit instant fill on LIMIT_IOC
        placed_orders.clear()
        mock_order.get_executed_position.return_value = {"size": 1.0, "price": 100.0}
        mock_order.get_exact_position_guarded = AsyncMock(return_value={"size": 0.0, "price": 0.0, "status": "ok"})
        fsm_tp = PositionFSM(
            sym="BTC", route="BINANCE_BITGET", target_ex="BITGET", oracle_ex="BINANCE", side="LONG",
            engine_res={"entry_price": 100.0, "net_spread": 0.010}, cfg=self.cfg, orders={"BITGET": mock_order, "BINANCE": MagicMock()}
        )
        fsm_tp.exec_res = {"entry_price": 100.0}
        fsm_tp.target_pos = {"size": 1.0, "price": 100.0}
        fsm_tp._wait_for_close_v9 = AsyncMock(return_value=True)

        ok_tp = await fsm_tp.run_close({"reason": "TAKE_PROFIT", "exit_price": 100.80, "order_type": "LIMIT_IOC"}, reason="TAKE_PROFIT")
        self.assertTrue(ok_tp)
        self.assertEqual(fsm_tp.state, PositionState.SETTLED)
        self.assertEqual(len(placed_orders), 1)
        self.assertEqual(placed_orders[-1]["order_type"], "LIMIT_IOC")

        # Scenario B: Exhausted 10 retries of Extrime Close -> Guaranteed Emergency MARKET Sweep
        placed_orders.clear()
        mock_order.get_executed_position.return_value = {"size": 1.0, "price": 100.0}
        pos_sequence = [{"size": 1.0, "price": 100.0, "status": "ok"}] * 11 + [{"size": 0.0, "price": 0.0, "status": "ok"}]
        mock_order.get_exact_position_guarded = AsyncMock(side_effect=pos_sequence)

        fsm_ext = PositionFSM(
            sym="BTC", route="BINANCE_BITGET", target_ex="BITGET", oracle_ex="BINANCE", side="LONG",
            engine_res={"entry_price": 100.0, "net_spread": 0.010}, cfg=self.cfg, orders={"BITGET": mock_order, "BINANCE": MagicMock()}
        )
        fsm_ext.exec_res = {"entry_price": 100.0}
        fsm_ext.target_pos = {"size": 1.0, "price": 100.0}
        fsm_ext._wait_for_close_v9 = AsyncMock(return_value=False)

        ok_ext = await fsm_ext.run_close({"reason": "EXTRIME_CLOSE", "exit_price": 100.05, "order_type": "LIMIT_IOC"}, reason="EXTRIME_CLOSE")
        self.assertTrue(ok_ext)
        self.assertEqual(fsm_ext.state, PositionState.SETTLED)
        
        types = [o["order_type"] for o in placed_orders]
        self.assertEqual(types.count("LIMIT_IOC"), 11) # 1 initial + 10 progressive retries
        self.assertEqual(types.count("MARKET"), 1)    # 1 guaranteed emergency sweep
        self.assertEqual(types[-1], "MARKET")

    # ============================================================
    # NODE 7: Analytics, Settlement & Quarantine Logic
    # ============================================================
    def test_node_7_analytics_and_quarantine(self):
        """Проверка расчета PnL, карантина убыточных сделок и перманентного бана."""
        analytics = TradeAnalytics("TEST_SYM", self.cfg["exchanges"])
        
        # Open trade
        analytics.record_open(
            route="BINANCE_BITGET",
            direction="LONG",
            target_ex="BITGET",
            oracle_ex="BINANCE",
            target_price_in=100.0,
            spread_in=0.010
        )

        # Close trade with profit: exit at 100.80 (+0.8%), $25 size
        trade = analytics.record_close(target_price_close=100.80, spread_out=0.0, target_executed_usd=25.0)
        self.assertGreater(trade["Net_PnL_USD"], 0.0)
        self.assertEqual(trade["Win"], 1)

        # Test balance update O(1)
        new_balance = update_total_balance(self.cfg, extra_pnl=trade["Net_PnL_USD"])
        self.assertGreater(new_balance, 0.0)

        # Clean up test analytics logs
        if os.path.exists(analytics.filepath):
            try: os.remove(analytics.filepath)
            except Exception: pass
        if os.path.exists(analytics.readable_path):
            try: os.remove(analytics.readable_path)
            except Exception: pass

    # ============================================================
    # NODE 8: 2-Process IPC Socket Communication
    # ============================================================
    async def test_node_8_ipc_socket_protocol(self):
        """Проверка асинхронного TCP IPC протокола между процессами."""
        received = []
        async def handle_client(reader, writer):
            while True:
                try:
                    m_type, payload = await async_read_msg(reader)
                    received.append((m_type, payload))
                    if m_type == "CMD_CLOSE":
                        await async_write_msg(writer, "POS_CLOSED", {"sym": payload["sym"]})
                    elif m_type == "SHUTDOWN":
                        break
                except (EOFError, ConnectionResetError, asyncio.IncompleteReadError):
                    break

        server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection('127.0.0.1', port)

        # Send CMD_OPEN
        await async_write_msg(writer, "CMD_OPEN", {"sym": "BTC", "size": 25.0})
        # Send CMD_CLOSE
        await async_write_msg(writer, "CMD_CLOSE", {"sym": "BTC"})

        # Read response from server
        resp_type, resp_payload = await async_read_msg(reader)
        self.assertEqual(resp_type, "POS_CLOSED")
        self.assertEqual(resp_payload["sym"], "BTC")

        # Send SHUTDOWN
        await async_write_msg(writer, "SHUTDOWN", None)
        await asyncio.sleep(0.02)

        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
