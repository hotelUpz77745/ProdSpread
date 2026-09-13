import unittest
import time
from CORE.math_core import ImpulseDetector

class TestImpulseDetector(unittest.TestCase):
    def setUp(self):
        cfg = {
            "trading_rules": {
                "entry": {
                    "impulse_detector": {
                        "enabled": True,
                        "buffer_window_ms": 250,
                        "static_leg": "TARGET",
                        "max_static_leg_ratio": 0.25
                    }
                }
            }
        }
        self.detector = ImpulseDetector(cfg)
        self.detector_oracle = ImpulseDetector({
            "trading_rules": {
                "entry": {
                    "impulse_detector": {
                        "enabled": True,
                        "buffer_window_ms": 250,
                        "static_leg": "ORACLE",
                        "max_static_leg_ratio": 0.25
                    }
                }
            }
        })

    def test_target_static_valid(self):
        """Target noise is 0.1%, Oracle jumps 1.0%. Spread delta = 0.9%. Allowed = 0.225%. 0.1% < 0.225%. Valid."""
        ts = time.monotonic()
        self.detector.update("BTC", "BINANCE", 10000.0, ts)
        self.detector.update("BTC", "BITGET", 10000.0, ts)
        
        is_impulse, reason, o_delta, t_delta = self.detector.check_impulse(
            sym="BTC",
            oracle_ex="BINANCE",
            target_ex="BITGET",
            oracle_price=10100.0,  # +1.0%
            target_price=10010.0   # +0.1%
        )
        self.assertTrue(is_impulse)
        self.assertEqual(reason, "VALID_IMPULSE")
        
    def test_target_not_static_rejected(self):
        """Target noise is 0.5%, Oracle jumps 1.0%. Spread delta = 0.5%. Allowed = 0.125%. 0.5% > 0.125%. Invalid."""
        ts = time.monotonic()
        self.detector.update("BTC", "BINANCE", 10000.0, ts)
        self.detector.update("BTC", "BITGET", 10000.0, ts)
        
        is_impulse, reason, o_delta, t_delta = self.detector.check_impulse(
            sym="BTC",
            oracle_ex="BINANCE",
            target_ex="BITGET",
            oracle_price=10100.0,  # +1.0%
            target_price=10050.0   # +0.5%
        )
        self.assertFalse(is_impulse)
        self.assertIn("TARGET_NOT_STATIC", reason)

    def test_oracle_static_valid(self):
        """Oracle noise is 0.1%, Target jumps 1.0%. Spread delta = 0.9%. Allowed = 0.225%. 0.1% < 0.225%. Valid."""
        ts = time.monotonic()
        self.detector_oracle.update("BTC", "BINANCE", 10000.0, ts)
        self.detector_oracle.update("BTC", "BITGET", 10000.0, ts)
        
        is_impulse, reason, o_delta, t_delta = self.detector_oracle.check_impulse(
            sym="BTC",
            oracle_ex="BINANCE",
            target_ex="BITGET",
            oracle_price=10010.0,  # +0.1%
            target_price=10100.0   # +1.0%
        )
        self.assertTrue(is_impulse)
        self.assertEqual(reason, "VALID_IMPULSE")

    def test_buffer_cleanup(self):
        """Ticks older than buffer_window_sec should be removed."""
        ts = time.monotonic()
        
        self.detector.update("BTC", "BINANCE", 10000.0, ts)
        self.detector.update("BTC", "BINANCE", 10010.0, ts + 0.1)
        self.detector.update("BTC", "BINANCE", 10020.0, ts + 0.3) # This should evict the first tick (0.250s window)
        
        buf = self.detector._buffers[("BTC", "BINANCE")]
        self.assertEqual(len(buf), 2)
        self.assertEqual(buf[0][1], 10010.0) # Base price is now 10010
        
        delta = self.detector.get_delta("BTC", "BINANCE", 10030.0)
        # Delta from 10010 to 10030 is ~0.1998%
        self.assertAlmostEqual(delta, 20.0/10010.0, places=5)

if __name__ == '__main__':
    unittest.main()
