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

if __name__ == '__main__':
    unittest.main()
