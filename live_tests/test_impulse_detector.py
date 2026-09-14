# ============================================================
# FILE: live_tests/test_impulse_detector.py
# ROLE: Unit tests for StaticDetector (sliding mean and 250ms buffer).
# ============================================================
import unittest
import time
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.math_core import StaticDetector

class TestStaticDetector(unittest.TestCase):
    def setUp(self):
        self.cfg = {
            "trading_rules": {
                "entry": {
                    "static_detector": {
                        "enabled": True,
                        "static_leg": "TARGET",
                        "max_static_leg_pct": 0.0020,
                        "buffer_window_sec": 0.25
                    }
                }
            }
        }
        self.detector = StaticDetector(self.cfg)

    def test_top3_mid_calculation(self):
        """Top 3 asks and top 3 bids average."""
        bids = [[100.0, 1.0], [99.0, 1.0], [98.0, 1.0]] # avg bid = 99.0
        asks = [[101.0, 1.0], [102.0, 1.0], [103.0, 1.0]] # avg ask = 102.0
        mid = StaticDetector.calc_top3_mid_price(bids, asks)
        self.assertAlmostEqual(mid, (99.0 + 102.0) / 2.0, places=5)

    def test_static_leg_within_threshold(self):
        """Price deviation within 0.2% of buffer mean should return True."""
        ts = time.monotonic()
        self.detector.update("BTC", "BITGET", 100.0, ts)
        self.detector.update("BTC", "BITGET", 100.05, ts + 0.05)
        self.detector.update("BTC", "BITGET", 100.02, ts + 0.10)
        
        # Mean is ~100.0233. Current price 100.10 is +0.076% deviation (< 0.20%)
        is_static, reason, dev = self.detector.is_leg_static("BTC", "BITGET", 100.10)
        self.assertTrue(is_static)
        self.assertEqual(reason, "LEG_STATIC_OK")
        self.assertLess(dev, 0.0020)

    def test_static_leg_exceeds_threshold(self):
        """Price deviation exceeding 0.2% of buffer mean should return False."""
        ts = time.monotonic()
        self.detector.update("BTC", "BITGET", 100.0, ts)
        self.detector.update("BTC", "BITGET", 100.0, ts + 0.05)
        
        # Mean is 100.0. Current price 100.35 is +0.35% deviation (> 0.20%)
        is_static, reason, dev = self.detector.is_leg_static("BTC", "BITGET", 100.35)
        self.assertFalse(is_static)
        self.assertIn("LEG_NOT_STATIC", reason)
        self.assertGreater(dev, 0.0020)

    def test_buffer_cleanup(self):
        """Ticks older than buffer_window_sec (0.25s) must be pruned."""
        ts = time.monotonic()
        self.detector.update("BTC", "BITGET", 100.0, ts)
        self.detector.update("BTC", "BITGET", 101.0, ts + 0.10)
        # Adding tick at ts + 0.30s should prune the tick at ts (0.30 > 0.25)
        self.detector.update("BTC", "BITGET", 102.0, ts + 0.30)
        
        buf = self.detector._buffers[("BTC", "BITGET")]
        self.assertEqual(len(buf), 2)
        self.assertEqual(buf[0][1], 101.0)
        self.assertEqual(buf[1][1], 102.0)

    def test_first_tick_baseline(self):
        """Empty buffer should return baseline True without failing."""
        is_static, reason, dev = self.detector.is_leg_static("ETH", "BITGET", 2500.0)
        self.assertTrue(is_static)
        self.assertEqual(reason, "FIRST_TICK_BASELINE")

if __name__ == '__main__':
    unittest.main()
