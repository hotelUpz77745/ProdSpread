# ============================================================
# FILE: live_tests/test_trading_engine.py
# ROLE: Signal evaluation and filter tests for TradingEngine.
# ============================================================
import unittest
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.trading_engine import TradingEngine

class TestTradingEngine(unittest.TestCase):
    def setUp(self):
        self.cfg = {
            "trading_rules": {
                "entry": {
                    "signal_filters": {
                        "spread_entry": 0.0005,
                        "spread_entry_max": 0.025,
                        "min_top_depth_usd": 0.0,
                        "synthetic_exit": {
                            "enabled": True,
                            "check_slippage": True,
                            "max_slippage_ratio": 0.5,
                            "hard_max_slippage": 0.015
                        },
                        "orderbook_imbalance": {
                            "enabled": False,
                            "max_adverse_imbalance": 0.55,
                            "depth_levels": 5
                        }
                    }
                },
                "exit": {
                    "hedged_exit": {
                        "normal_decay": []
                    },
                    "target_exit": {
                        "stop_loss_pct": 0.01,
                        "ttl_sec": 60.0,
                        "decay_map": [
                            {"step": 0, "after_sec": 0, "min_profit_ratio": 0.8},
                            {"step": 1, "after_sec": 5, "min_profit_ratio": 0.5},
                            {"step": 2, "after_sec": 15, "min_profit_ratio": 0.25},
                            {"step": 3, "after_sec": 30, "min_profit_ratio": 0.0},
                            {"step": 4, "after_sec": 45, "min_profit_ratio": -0.2},
                            {"step": 5, "after_sec": 60, "min_profit_ratio": -999.0}
                        ]
                    }
                }
            },
            "trading_risks": {
                "binance": {"taker_fee": 0.0004, "volatility_discount_entry": 0.8, "volatility_discount_exit": 1.0},
                "bitget": {"taker_fee": 0.0004, "volatility_discount_entry": 0.8, "volatility_discount_exit": 1.0}
            }
        }
        self.engine = TradingEngine(self.cfg, {0: "BINANCE", 1: "BITGET"})
        
    def test_evaluate_entry(self):
        # Fake book: list of [price, size]
        book_long = {
            "bids": [[50000.0, 1.0], [49990.0, 2.0]],
            "asks": [[50010.0, 1.0], [50020.0, 2.0]]
        }
        book_short = {
            "bids": [[50100.0, 1.0], [50090.0, 2.0]],
            "asks": [[50110.0, 1.0], [50120.0, 2.0]]
        }
        
        target_size_usd = 20000.0
        
        passed, res = self.engine.evaluate_entry(book_long, book_short, [0, 1], target_size_usd)
        
        self.assertTrue(passed)
        self.assertEqual(res["long_avg_price"], 50010.0)
        self.assertEqual(res["short_avg_price"], 50100.0)

    def test_insufficient_liquidity(self):
        book_long = {
            "bids": [[50000.0, 1.0]],
            "asks": [[50010.0, 0.1]] # Only 0.1 BTC available, which is $5000
        }
        book_short = {
            "bids": [[50100.0, 1.0]],
            "asks": [[50110.0, 1.0]]
        }
        
        target_size_usd = 20000.0
        
        passed, res = self.engine.evaluate_entry(book_long, book_short, [0, 1], target_size_usd)
        
        self.assertFalse(passed)
        self.assertEqual(res["reason"], "INSUFFICIENT_VOLUME")

    def test_max_spread_rejection(self):
        # Book with ~7% spread (e.g. UAI anomaly)
        book_long = {
            "bids": [[0.5780, 1000.0]],
            "asks": [[0.5794, 1000.0]]
        }
        book_short = {
            "bids": [[0.6229, 1000.0]],
            "asks": [[0.6240, 1000.0]]
        }
        passed, res = self.engine.evaluate_entry(book_long, book_short, [0, 1], 25.0)
        self.assertFalse(passed)
        self.assertIn("HIGH_SPREAD", res["reason"])

    def test_evaluate_entry_v9_long(self):
        # Oracle mid is 50100 (bids: 50095, asks: 50105)
        oracle_book = {
            "bids": [[50095.0, 5.0]],
            "asks": [[50105.0, 5.0]]
        }
        # Target is lagging behind at 50000 (bids: 49995, asks: 50005)
        target_book = {
            "bids": [[49995.0, 5.0]],
            "asks": [[50005.0, 5.0]]
        }
        passed, res = self.engine.evaluate_entry_v9(oracle_book, target_book, "BINANCE", "BITGET", 1000.0)
        self.assertTrue(passed)
        self.assertEqual(res["side"], "LONG")
        self.assertEqual(res["target_ex"], "BITGET")
        self.assertEqual(res["oracle_ex"], "BINANCE")
        self.assertGreater(res["net_spread"], 0.001)

    def test_evaluate_entry_v9_short(self):
        # Oracle mid is 49900
        oracle_book = {
            "bids": [[49895.0, 5.0]],
            "asks": [[49905.0, 5.0]]
        }
        # Target is lagging at 50050
        target_book = {
            "bids": [[50045.0, 5.0]],
            "asks": [[50055.0, 5.0]]
        }
        passed, res = self.engine.evaluate_entry_v9(oracle_book, target_book, "BINANCE", "BITGET", 1000.0)
        self.assertTrue(passed)
        self.assertEqual(res["side"], "SHORT")
        self.assertEqual(res["target_ex"], "BITGET")
        self.assertGreater(res["net_spread"], 0.001)

    def test_evaluate_exit_v9_take_profit(self):
        # Long entered at 50000. Now target bid has risen to 50100 (+0.2%)
        target_book = {
            "bids": [[50100.0, 5.0]],
            "asks": [[50105.0, 5.0]]
        }
        should_exit, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=50000.0,
            qty=0.02,
            side="LONG",
            duration_sec=1.0,
            actual_net_spread_entry=0.001
        )
        self.assertTrue(should_exit)
        self.assertEqual(res["reason"], "TAKE_PROFIT")
        self.assertGreater(res["net_pnl_pct"], 0.001)

    def test_evaluate_exit_v9_stop_loss(self):
        # Long entered at 50000. Now target bid has crashed to 49000 (-2%)
        target_book = {
            "bids": [[49000.0, 5.0]],
            "asks": [[49010.0, 5.0]]
        }
        should_exit, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=50000.0,
            qty=0.02,
            side="LONG",
            duration_sec=1.0,
            actual_net_spread_entry=0.001
        )
        self.assertTrue(should_exit)
        self.assertEqual(res["reason"], "STOP_LOSS")
        self.assertLess(res["net_pnl_pct"], -0.01)

    def test_evaluate_exit_v9_ttl(self):
        target_book = {
            "bids": [[50000.0, 5.0]],
            "asks": [[50005.0, 5.0]]
        }
        should_exit, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=50000.0,
            qty=0.02,
            side="LONG",
            duration_sec=65.0,  # ttl is 60.0 in cfg
            actual_net_spread_entry=0.001
        )
        self.assertTrue(should_exit)
        self.assertEqual(res["reason"], "TTL_EXPIRED")

if __name__ == '__main__':
    unittest.main()
