# ============================================================
# FILE: live_tests/test_position_fsm.py
# ROLE: Unit tests for PositionFSM transitions (v9 single-leg).
# ============================================================
import unittest
from unittest.mock import MagicMock, AsyncMock
import asyncio
import copy
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.position_fsm import PositionFSM, PositionState

class TestPositionFSM(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = {
            "trading_rules": {
                "entry": {
                    "parallel_entry_logic": {
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
        # Verify order was placed with limit_price = 50000 * (1 + 0.0015) = 50075.0
        self.mock_bitget.place_order.assert_called_once_with(
            "BTCUSDT", "BUY", 100.0, 50075.0, order_type="LIMIT_IOC", position_side="LONG"
        )

if __name__ == '__main__':
    unittest.main()
