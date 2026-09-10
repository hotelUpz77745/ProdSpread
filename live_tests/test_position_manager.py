import unittest
import os
import json
from unittest.mock import patch, mock_open

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.position_manager import PositionManager

class TestPositionManager(unittest.TestCase):
    def setUp(self):
        self.cfg = {
            "trading_risks": {
                "binance": {"max_positions": 1},
                "bitget": {"max_positions": 2},
                "kucoin": {"max_positions": 1}
            }
        }
        self.exchanges = ["BINANCE", "BITGET", "KUCOIN"]
        self.routes = ["BINANCE_BITGET", "BINANCE_KUCOIN", "KUCOIN_BITGET"]
        self.symbols = ["BTCUSDT", "ETHUSDT"]

        # Prevent actual file I/O
        self.patcher = patch("builtins.open", new_callable=mock_open)
        self.mock_file = self.patcher.start()
        
        self.pm = PositionManager(self.cfg, self.exchanges, self.routes, self.symbols)

    def tearDown(self):
        self.patcher.stop()

    def test_initial_state(self):
        self.assertEqual(self.pm.max_pos["BINANCE"], 1)
        self.assertEqual(self.pm.max_pos["BITGET"], 2)
        
        self.assertTrue(self.pm.can_enter("BINANCE", "BITGET", "BTCUSDT"))

    def test_lock_for_entry(self):
        self.pm.lock_for_entry("BINANCE", "BITGET", "BTCUSDT", {"price": 100})
        
        # Pending counts should increase
        self.assertEqual(self.pm.exchange_state["BINANCE"]["pending"], 1)
        self.assertEqual(self.pm.exchange_state["BITGET"]["pending"], 1)
        
        # Binance has max=1, so any route involving Binance should be locked
        self.assertTrue(self.pm.route_state["BINANCE_BITGET"]["is_locked"])
        self.assertTrue(self.pm.route_state["BINANCE_KUCOIN"]["is_locked"])
        
        # Kucoin and Bitget still have capacity (Bitget max=2, Kucoin max=1)
        self.assertFalse(self.pm.route_state["KUCOIN_BITGET"]["is_locked"])
        
        # Cannot enter because route is locked
        self.assertFalse(self.pm.can_enter("BINANCE", "BITGET", "ETHUSDT"))
        
        # Cannot enter BTCUSDT anywhere because it's pending
        self.assertFalse(self.pm.can_enter("KUCOIN", "BITGET", "BTCUSDT"))

    def test_confirm_entry(self):
        self.pm.lock_for_entry("BINANCE", "BITGET", "BTCUSDT", {})
        self.pm.confirm_entry("BINANCE", "BITGET", "BTCUSDT", {}, 12345.0)
        
        self.assertEqual(self.pm.exchange_state["BINANCE"]["pending"], 0)
        self.assertEqual(self.pm.exchange_state["BINANCE"]["current"], 1)
        
        state = self.pm.positions["BINANCE_BITGET"]["BTCUSDT"]
        self.assertTrue(state["current_position"])
        self.assertIsNone(state["pending_action"])
        
        # Still locked because Binance current=1 >= max=1
        self.assertFalse(self.pm.can_enter("BINANCE", "KUCOIN", "ETHUSDT"))

    def test_rollback_entry(self):
        self.pm.lock_for_entry("BINANCE", "BITGET", "BTCUSDT", {})
        self.pm.rollback_entry("BINANCE", "BITGET", "BTCUSDT")
        
        self.assertEqual(self.pm.exchange_state["BINANCE"]["pending"], 0)
        self.assertEqual(self.pm.exchange_state["BINANCE"]["current"], 0)
        self.assertFalse(self.pm.route_state["BINANCE_BITGET"]["is_locked"])
        
        # Now can enter again
        self.assertTrue(self.pm.can_enter("BINANCE", "BITGET", "BTCUSDT"))

    def test_confirm_exit(self):
        self.pm.lock_for_entry("BINANCE", "BITGET", "BTCUSDT", {})
        self.pm.confirm_entry("BINANCE", "BITGET", "BTCUSDT", {}, 123.0)
        self.pm.lock_for_exit("BINANCE_BITGET", "BTCUSDT")
        
        self.assertEqual(self.pm.positions["BINANCE_BITGET"]["BTCUSDT"]["pending_action"], "CLOSE")
        self.assertFalse(self.pm.can_enter("BINANCE", "BITGET", "BTCUSDT"))
        
        self.pm.confirm_exit("BINANCE_BITGET", "BTCUSDT")
        self.assertEqual(self.pm.exchange_state["BINANCE"]["current"], 0)
        self.assertFalse(self.pm.positions["BINANCE_BITGET"]["BTCUSDT"]["current_position"])
        self.assertIsNone(self.pm.positions["BINANCE_BITGET"]["BTCUSDT"]["pending_action"])
        
        # Route is unlocked
        self.assertFalse(self.pm.route_state["BINANCE_BITGET"]["is_locked"])

if __name__ == '__main__':
    unittest.main()
