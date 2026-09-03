import unittest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.position_fsm import PositionFSM, PositionState

class TestPositionFSM(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = {
            "EXECUTION_PAUSE": 0.02,
            "trading_rules": {
                "entry": {
                    "order_execution_type": "IOC",
                    "min_fill_rate": 0.5,
                }
            },
            "trading_risks": {
                "binance": {"limit_allow_distance": 1.002},
                "bitget": {"limit_allow_distance": 1.002}
            }
        }
        
        self.pm_mock = MagicMock()
        self.writer_mock = MagicMock()
        self.writer_mock.write = MagicMock()
        self.writer_mock.drain = AsyncMock()

        self.mock_binance = MagicMock()
        self.mock_binance.check_order_size = MagicMock()
        self.mock_binance.place_order = AsyncMock(return_value={"status": "ok"})
        self.mock_binance.get_executed_position = MagicMock(return_value={"size": 0.0, "price": 0.0})
        self.mock_binance.get_exact_position_guarded = AsyncMock(return_value={"size": 0.0, "price": 0.0})
        self.mock_binance.cancel_all_orders = AsyncMock()

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
            long_ex="BINANCE",
            short_ex="BITGET",
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
            "size_usd": 100.0,
            "long_avg_price": 50000.0,
            "short_avg_price": 50010.0,
            "long_qty": 0.002,
            "short_qty": 0.002
        }
        fsm = self._create_fsm(engine_res)
        
        # Simulate full fill
        self.mock_binance.get_executed_position.return_value = {"size": 0.002, "price": 50000.0}
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50010.0}
        
        result = await fsm.run_open()
        
        self.assertTrue(result)
        self.assertEqual(fsm.state, PositionState.ACTIVE_HEDGED)
        self.pm_mock.confirm_entry.assert_called_once()
        self.mock_binance.place_order.assert_called_once()
        self.mock_bitget.place_order.assert_called_once()

    async def test_emergency_unwind_low_fill_rate(self):
        engine_res = {
            "size_usd": 100.0,
            "long_avg_price": 50000.0,
            "short_avg_price": 50010.0,
            "long_qty": 0.002,
            "short_qty": 0.002
        }
        fsm = self._create_fsm(engine_res)
        
        # Simulate partial fill (below min_fill_rate 0.5)
        self.mock_binance.get_executed_position.return_value = {"size": 0.0005, "price": 50000.0}
        self.mock_bitget.get_executed_position.return_value = {"size": 0.002, "price": 50010.0}
        
        # When checking exact position during emergency unwind, simulate it was successfully closed
        self.mock_binance.get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0}
        
        result = await fsm.run_open()
        
        self.assertFalse(result)
        self.assertEqual(fsm.state, PositionState.SETTLED)
        
        # Check that place_order was called twice for each exchange (1 open, 1 unwind)
        self.assertEqual(self.mock_binance.place_order.call_count, 2)
        self.assertEqual(self.mock_bitget.place_order.call_count, 2)

    async def test_zero_price_fallback(self):
        engine_res = {"long_avg_price": 50000.0, "short_avg_price": 50010.0}
        fsm = self._create_fsm(engine_res)
        
        # Simulate position with size but NO price (price=0.0)
        fsm.long_pos = {"size": 0.001, "price": 0.0}
        fsm.short_pos = {"size": 0.001, "price": 0.0}
        
        # Unwind should successfully clear it
        self.mock_binance.get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0}
        
        await fsm._emergency_unwind()
        
        # The price passed to place_order should be the fallback price from engine_res, NOT 0.0
        binance_call = self.mock_binance.place_order.call_args[0]
        self.assertEqual(binance_call[3], 50000.0) # price argument
        
        bitget_call = self.mock_bitget.place_order.call_args[0]
        self.assertEqual(bitget_call[3], 50010.0) # price argument

    async def test_unwind_leak_protection(self):
        # Test that if 3 attempts fail to close the position, it doesn't get silently settled
        engine_res = {"long_avg_price": 50000.0, "short_avg_price": 50010.0}
        fsm = self._create_fsm(engine_res)
        
        fsm.long_pos = {"size": 0.001, "price": 50000.0}
        fsm.short_pos = {"size": 0.001, "price": 50010.0}
        
        # Simulate position STUCK (always returns 0.001 despite market closes)
        self.mock_binance.get_exact_position_guarded.return_value = {"size": 0.001, "price": 50000.0}
        self.mock_bitget.get_exact_position_guarded.return_value = {"size": 0.001, "price": 50010.0}
        
        await fsm._emergency_unwind()
        
        # State should be CLOSING, not SETTLED!
        self.assertEqual(fsm.state, PositionState.CLOSING)
        # confirm_exit should NOT be called!
        self.pm_mock.confirm_exit.assert_not_called()

if __name__ == '__main__':
    unittest.main()
