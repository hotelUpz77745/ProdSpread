# ============================================================
# FILE: live_tests/test_position_fsm.py
# ROLE: Unit tests for PositionFSM transitions (v9 single-leg).
# ============================================================
import unittest
from unittest.mock import MagicMock, AsyncMock
import asyncio
import copy
import time
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.position_fsm import PositionFSM, PositionState

class TestPositionFSM(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = {
            "trading_rules": {
                "entry": {
                    "static_detector": {
                        "enabled": True,
                        "static_leg": "TARGET",
                        "max_static_leg_pct": 0.0020,
                        "buffer_window_sec": 0.25
                    },
                    "target_entry_logic": {
                        "entry_slip_ratio": 0.0020,
                        "fill_confirm_timeout_sec": {"BINANCE_BITGET": 0.05},
                        "fill_confirm_poll_interval_sec": 0.0,
                        "entry_api_timeout_sec": 1.0
                    },
                    "signal_filters": {
                        "spread_entry": [0.008, 0.05],
                        "min_top_depth_usd": 50.0
                    }
                },
                "exit": {
                    "close_confirm_timeout_sec": {"BINANCE_BITGET": 0.05},
                    "target_exit": {
                        "ttl_sec": 60.0,
                        "exit_order_type": "LIMIT_IOC",
                        "exit_slip_ratio": 0.001,
                        "ioc_chase_timeout_sec": 0.2,
                        "decay_map": [
                            {"step": 0, "after_sec": 0, "min_profit_ratio": 0.8},
                            {"step": 1, "after_sec": 60, "min_profit_ratio": -999.0}
                        ]
                    }
                },
                "ban_rules": {
                    "quarantine_sec": {"entry_error": 60, "zero_fill": 10},
                    "perm_ban_loss_pct": 0.0075
                },
                "emergency_unwind": {
                    "max_attempts": 2,
                    "retry_pause_sec": 0.01,
                    "ws_verify_timeout_sec": 0.02
                }
            },
            "trading_risks": {
                "bitget": {
                    "trade_size_usd": 100.0,
                    "limit_slip_ratio": 0.0025,
                    "taker_fee": 0.0006,
                    "volatility_discount_entry": 0.8,
                    "volatility_discount_exit": 0.85
                },
                "binance": {
                    "trade_size_usd": 100.0,
                    "limit_slip_ratio": 0.0025,
                    "taker_fee": 0.0005,
                    "volatility_discount_entry": 0.8,
                    "volatility_discount_exit": 0.85
                }
            }
        }
        self.pm_mock = MagicMock()
        self.writer_mock = MagicMock()
        
        self.mock_binance = MagicMock()
        self.mock_bitget = MagicMock()
        self.mock_bitget.check_order_size = MagicMock()
        self.mock_bitget.place_order = AsyncMock(return_value={"status": "ok"})
        self.mock_bitget.get_executed_position = MagicMock(return_value={"size": 0.0, "price": 0.0})
        self.mock_bitget.get_exact_position_guarded = AsyncMock(return_value={"size": 0.0, "price": 0.0})
        self.mock_bitget.cancel_all_orders = AsyncMock()

        self.orders_mock = {
            "BINANCE": self.mock_binance,
            "BITGET": self.mock_bitget
        }

    def _create_fsm(self, engine_res):
        return PositionFSM(
            sym="BTCUSDT",
            route="BINANCE_BITGET",
            target_ex="BITGET",
            oracle_ex="BINANCE",
            side=engine_res.get("side", "LONG"),
            engine_res=engine_res,
            cfg=self.cfg,
            orders=self.orders_mock,
            coin_to_native={},
            pm=self.pm_mock,
            writer=self.writer_mock,
            ban_coin_cb=MagicMock()
        )

    async def test_successful_open(self):
        engine_res = {
            "side": "LONG",
            "entry_price": 50010.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = self._create_fsm(engine_res)
        
        # Simulate full fill on target
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50010.0}
        
        result = await fsm.run_open()
        
        self.assertTrue(result)
        self.assertEqual(fsm.state, PositionState.ACTIVE)
        self.pm_mock.confirm_entry.assert_called_once()
        self.mock_bitget.place_order.assert_called_once()

    async def test_zero_fill_abort(self):
        engine_res = {
            "side": "LONG",
            "entry_price": 50010.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = self._create_fsm(engine_res)
        
        # Simulate zero fill
        self.mock_bitget.get_executed_position.return_value = {"size": 0.0, "price": 0.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0}
        
        result = await fsm.run_open()
        
        self.assertFalse(result)
        self.assertEqual(fsm.state, PositionState.ABORTED)
        self.pm_mock.rollback_entry.assert_called_once()

    async def test_successful_close(self):
        engine_res = {
            "side": "LONG",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = self._create_fsm(engine_res)
        fsm.target_pos = {"size": 0.002, "price": 50000.0}
        fsm.exec_res = {"entry_price": 50000.0}
        fsm.state = PositionState.ACTIVE

        # Target fills close order -> position becomes 0
        self.mock_bitget.get_executed_position.return_value = {"size": 0.0, "price": 0.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0}

        exit_res = {"exit_price": 50200.0, "reason": "TAKE_PROFIT"}
        result = await fsm.run_close(exit_res, reason="TAKE_PROFIT")

        self.assertTrue(result)
        self.assertEqual(fsm.state, PositionState.SETTLED)
        self.pm_mock.confirm_exit.assert_called_once()

    async def test_unwind_leak_protection(self):
        # Test that if close attempts fail to close the position, it doesn't get silently settled
        engine_res = {
            "side": "LONG",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = self._create_fsm(engine_res)
        fsm.target_pos = {"size": 0.002, "price": 50000.0}
        fsm.state = PositionState.ACTIVE
        
        # Position remains stuck at size 0.002 despite attempts
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50000.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.002, "price": 50000.0}
        
        result = await fsm.run_close({}, reason="TTL_EXPIRED")
        
        self.assertFalse(result)
        self.assertEqual(fsm.state, PositionState.CLOSING)
        self.pm_mock.confirm_exit.assert_not_called()
        self.pm_mock.rollback_exit.assert_called_once()

    async def test_entry_slip_ratio_override(self):
        cfg_custom = copy.deepcopy(self.cfg)
        cfg_custom["trading_rules"]["entry"]["target_entry_logic"] = {
            "entry_slip_ratio": 0.0015,
            "fill_confirm_timeout_sec": {"BINANCE_BITGET": 0.05},
            "fill_confirm_poll_interval_sec": 0.0,
            "entry_api_timeout_sec": 1.0
        }
        engine_res = {
            "side": "LONG",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = PositionFSM(
            sym="BTCUSDT",
            route="BINANCE_BITGET",
            target_ex="BITGET",
            oracle_ex="BINANCE",
            side="LONG",
            engine_res=engine_res,
            cfg=cfg_custom,
            orders=self.orders_mock,
            coin_to_native={"BTCUSDT": {"BITGET": "BTCUSDT"}},
            pm=self.pm_mock,
            writer=self.writer_mock
        )
        self.assertEqual(fsm.entry_slip_ratio, 0.0015)
        
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50075.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.002, "price": 50075.0}
        
        await fsm.run_open()
        self.mock_bitget.place_order.assert_called_once_with(
            "BTCUSDT", "BUY", 100.0, 50075.0, order_type="LIMIT_IOC", position_side="LONG"
        )

    async def test_static_entry_slippage(self):
        cfg_static = copy.deepcopy(self.cfg)
        cfg_static["trading_rules"]["entry"]["target_entry_logic"] = {
            "entry_slip_ratio": 0.0020,
            "fill_confirm_timeout_sec": {"BINANCE_BITGET": 0.05},
            "fill_confirm_poll_interval_sec": 0.0,
            "entry_api_timeout_sec": 1.0
        }
        
        # Test 1: LONG with 0.0020 (0.20%) slip regardless of net_spread
        engine_res1 = {
            "side": "LONG",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.010
        }
        fsm1 = PositionFSM(
            sym="BTCUSDT", route="BINANCE_BITGET", target_ex="BITGET", oracle_ex="BINANCE",
            side="LONG", engine_res=engine_res1, cfg=cfg_static, orders=self.orders_mock,
            coin_to_native={"BTCUSDT": {"BITGET": "BTCUSDT"}}, pm=self.pm_mock, writer=self.writer_mock
        )
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50100.0}
        await fsm1.run_open()
        # 50000 * (1 + 0.0020) = 50100.0
        args, kwargs = self.mock_bitget.place_order.call_args
        self.assertEqual(args[0], "BTCUSDT")
        self.assertEqual(args[1], "BUY")
        self.assertEqual(args[2], 100.0)
        self.assertAlmostEqual(args[3], 50100.0, places=4)
        self.assertEqual(kwargs.get("order_type"), "LIMIT_IOC")
        self.assertEqual(kwargs.get("position_side"), "LONG")

        # Test 2: SHORT with 0.0020 (0.20%) slip regardless of high net_spread (e.g. 0.035)
        engine_res2 = {
            "side": "SHORT",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.035
        }
        fsm2 = PositionFSM(
            sym="BTCUSDT", route="BINANCE_BITGET", target_ex="BITGET", oracle_ex="BINANCE",
            side="SHORT", engine_res=engine_res2, cfg=cfg_static, orders=self.orders_mock,
            coin_to_native={"BTCUSDT": {"BITGET": "BTCUSDT"}}, pm=self.pm_mock, writer=self.writer_mock
        )
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 49900.0}
        await fsm2.run_open()
        # 50000 * (1 - 0.0020) = 49900.0
        args2, kwargs2 = self.mock_bitget.place_order.call_args
        self.assertEqual(args2[0], "BTCUSDT")
        self.assertEqual(args2[1], "SELL")
        self.assertEqual(args2[2], 100.0)
        self.assertAlmostEqual(args2[3], 49900.0, places=4)
        self.assertEqual(kwargs2.get("order_type"), "LIMIT_IOC")
        self.assertEqual(kwargs2.get("position_side"), "SHORT")

    async def test_run_single_leg_exposure_compatibility(self):
        fsm = PositionFSM(
            sym="BTCUSDT", route="BINANCE_BITGET", target_ex="BITGET", oracle_ex="BINANCE",
            long_ex="BITGET", short_ex="BINANCE",
            side="LONG", engine_res={}, cfg=self.cfg, orders=self.orders_mock,
            coin_to_native={"BTCUSDT": {"BITGET": "BTCUSDT", "BINANCE": "BTCUSDT"}},
            pm=self.pm_mock, writer=self.writer_mock
        )
        await fsm._run_single_leg_exposure(l_qty=0.002, s_qty=0.0, l_price=50000.0, s_price=0.0)
        self.mock_bitget.place_order.assert_called_once()
        self.assertEqual(fsm.state, PositionState.ABORTED)

    async def test_fast_zero_fill_on_order_cancel_event(self):
        engine_res = {
            "side": "LONG",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = self._create_fsm(engine_res)
        
        # Position is 0, but exchange sent order cancellation (zero fill)
        self.mock_bitget.get_executed_position.return_value = {"size": 0.0, "price": 0.0}
        self.mock_bitget.get_last_order_event = MagicMock(return_value={
            "status": "canceled",
            "cum_qty": 0.0,
            "avg_price": 0.0,
            "timestamp": time.time()
        })
        
        ev_target = asyncio.Event()
        ev_target.set() # Instant wake-up upon order cancellation
        self.mock_bitget.subscribe_position_update = MagicMock(return_value=ev_target)
        self.mock_bitget.unsubscribe_position_update = MagicMock()
        
        result = await fsm.run_open()
        
        # Should abort immediately without waiting for timeout or querying REST
        self.assertFalse(result)
        self.assertEqual(fsm.state, PositionState.ABORTED)
        fsm.ban_coin_cb.assert_called_once_with("BTCUSDT", reason="Zero Fill (v9)", duration_sec=fsm.q_zero_fill)
        self.mock_bitget.get_exact_position_guarded.assert_not_called()

    async def test_fast_exit_on_ioc_cancel_event(self):
        engine_res = {
            "side": "LONG",
            "entry_price": 50000.0,
            "qty": 0.002,
            "net_spread": 0.015
        }
        fsm = self._create_fsm(engine_res)
        fsm.state = PositionState.ACTIVE
        fsm.target_pos = {"size": 0.002, "price": 50000.0}
        fsm.exit_order_type = "LIMIT_IOC"
        
        # Position is still open (size 0.002), but LIMIT_IOC close was cancelled by exchange
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50000.0}
        self.mock_bitget.get_last_order_event = MagicMock(return_value={
            "status": "canceled",
            "cum_qty": 0.0,
            "avg_price": 0.0
        })
        
        ev_target = asyncio.Event()
        ev_target.set()
        
        # _wait_for_close_v9 should immediately return False as order is dead
        is_closed = await fsm._wait_for_close_v9(ev_target, timeout=0.2)
        self.assertFalse(is_closed)


if __name__ == '__main__':
    unittest.main()

