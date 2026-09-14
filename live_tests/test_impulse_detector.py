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

    def test_anomaly_cooldown_locks_and_flushes_buffer(self):
        """
        When an anomaly occurs (delta > max_static_leg_pct):
        1. is_leg_static returns False with cooldown notice.
        2. During buffer_window_sec (0.25s), is_leg_static returns False with LEG_COOLING_OFF.
        3. Background updates continue buffering and naturally prune old ticks.
        4. After buffer_window_sec, the anomaly is flushed out and steady price returns True.
        """
        ts = 1000.0
        # Phase 1: Establish baseline at 100.0
        self.detector.update("SOL", "BITGET", 100.0, ts)
        self.detector.update("SOL", "BITGET", 100.0, ts + 0.05)
        self.detector.update("SOL", "BITGET", 100.0, ts + 0.10)
        
        # Phase 2: Anomaly occurs (dump to 98.0 -> 2.0% deviation > 0.20%)
        is_static, reason, dev = self.detector.is_leg_static("SOL", "BITGET", 98.0, ts_mono=ts + 0.10)
        self.assertFalse(is_static)
        self.assertIn("LEG_NOT_STATIC", reason)
        self.assertIn("cooldown 0.250s", reason)
        
        # Phase 3: During cooldown (e.g. +0.10s after anomaly, remaining ~0.15s)
        # Even if someone queries 100.0 (the old price), it MUST return False (LEG_COOLING_OFF)
        is_static_cool, reason_cool, _ = self.detector.is_leg_static("SOL", "BITGET", 100.0, ts_mono=ts + 0.20)
        self.assertFalse(is_static_cool)
        self.assertIn("LEG_COOLING_OFF", reason_cool)
        
        # Phase 4: Idle background accumulation of new ticks at 98.0
        self.detector.update("SOL", "BITGET", 98.0, ts + 0.15)
        self.detector.update("SOL", "BITGET", 98.0, ts + 0.25)
        self.detector.update("SOL", "BITGET", 98.0, ts + 0.36)  # 0.36 > 0.10 + 0.25 -> old ticks at 100.0 pruned!
        
        # Phase 5: Cooldown expired at ts + 0.36 (> ts + 0.10 + 0.25 = 1000.35)
        # The buffer now contains only 98.0 ticks! Checking 98.0 must return True!
        is_static_settled, reason_settled, dev_settled = self.detector.is_leg_static("SOL", "BITGET", 98.0, ts_mono=ts + 0.36)
        self.assertTrue(is_static_settled)
        self.assertEqual(reason_settled, "LEG_STATIC_OK")
        self.assertAlmostEqual(dev_settled, 0.0, places=5)

    def test_incoming_update_anomaly_triggers_cooldown(self):
        """When an anomaly is queried, it triggers cooldown, and subsequent queries are locked in LEG_COOLING_OFF."""
        ts = 2000.0
        self.detector.update("XRP", "BITGET", 1.00, ts)
        self.detector.update("XRP", "BITGET", 1.00, ts + 0.05)
        
        # Huge jump arrives
        self.detector.update("XRP", "BITGET", 1.05, ts + 0.10) # 5% jump
        
        # First query detects the non-static move and sets cooldown
        is_static, reason, _ = self.detector.is_leg_static("XRP", "BITGET", 1.05, ts_mono=ts + 0.12)
        self.assertFalse(is_static)
        self.assertIn("LEG_NOT_STATIC", reason)
        self.assertIn("cooldown 0.250s", reason)
        
        # Subsequent query at ts + 0.15 is locked in cooldown
        is_static2, reason2, _ = self.detector.is_leg_static("XRP", "BITGET", 1.05, ts_mono=ts + 0.15)
        self.assertFalse(is_static2)
        self.assertIn("LEG_COOLING_OFF", reason2)

if __name__ == '__main__':
    unittest.main()

