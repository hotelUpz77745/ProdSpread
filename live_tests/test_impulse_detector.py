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
                    "signal_filters": {
                        "static_detector": {
                            "enabled": True,
                            "static_leg": "TARGET",
                            "max_static_leg_ratio": 0.0020,
                            "buffer_window_sec": 0.25
                        }
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

    def test_correlated_walk_then_oracle_impulse_case_b(self):
        """
        Проверка сценария 'гуськом' + выстрел Оракула (Кейс Б):
        1. Обе ноги активно растут вместе (+0.50%), спред равен 0.0.
        2. Затем Оракул делает резкий рывок еще на +0.80%, а Мишень замирает.
        3. Система обязана подтвердить Кейс Б без ложного отсечения по кулдауну.
        """
        cfg = {
            "trading_rules": {
                "entry": {
                    "signal_filters": {
                        "static_detector": {
                            "enabled": True,
                            "static_leg": "ANY",
                            "max_static_leg_ratio": 0.0020,
                            "buffer_window_sec": 0.35
                        }
                    }
                }
            }
        }
        # В JSON true -> в python True
        cfg["trading_rules"]["entry"]["signal_filters"]["static_detector"]["enabled"] = True
        det = StaticDetector(cfg)

        pre_spread = 0.006  # 0.6% порог для фиксации базиса

        # Шаг 1: Ноги идут 'гуськом' (рыночный тренд +0.5%)
        # 100.0 -> 100.2 -> 100.5
        det.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, pre_spread, ts_mono=100.0)
        det.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.2, 100.2, pre_spread, ts_mono=100.1)
        ok_calm, r_calm, info_calm = det.evaluate_pair_stability(
            "BTC", "BINANCE", "BITGET", 100.5, 100.5, pre_spread, ts_mono=100.2
        )
        self.assertTrue(ok_calm)
        self.assertEqual(info_calm["case"], "QUIESCENT")

        # Шаг 2: Всплеск Оракула до 101.30 (+0.80% к базису 100.50), Мишень стоит на 100.50
        ok_shot, r_shot, info_shot = det.evaluate_pair_stability(
            "BTC", "BINANCE", "BITGET", 101.30, 100.50, pre_spread, ts_mono=100.25
        )
        self.assertTrue(ok_shot)
        self.assertEqual(info_shot["case"], "CASE_B")
        self.assertEqual(info_shot["static_leg"], "TARGET")
        self.assertIn("CASE_B_OK", r_shot)
        self.assertAlmostEqual(info_shot["target_delta"], 0.0, places=5)
        self.assertGreater(info_shot["oracle_delta"], 0.007)

    def test_correlated_walk_then_target_dump_case_c(self):
        """
        Проверка сценария 'гуськом' + локальный сброс на Мишени (Кейс В):
        1. Обе ноги падают вместе (-0.50%), спред 0.0.
        2. Затем Мишень локально продавливают на -0.80%, а Оракул стоит как якорь.
        3. Система обязана подтвердить Кейс В (Mean-Reversion).
        """
        cfg = {
            "trading_rules": {
                "entry": {
                    "signal_filters": {
                        "static_detector": {
                            "enabled": True,
                            "static_leg": "ANY",
                            "max_static_leg_ratio": 0.0020,
                            "buffer_window_sec": 0.35
                        }
                    }
                }
            }
        }
        det = StaticDetector(cfg)
        pre_spread = 0.006

        # Шаг 1: Когерентное падение 200.0 -> 199.5 -> 199.0
        det.evaluate_pair_stability("ETH", "BINANCE", "BITGET", 200.0, 200.0, pre_spread, ts_mono=200.0)
        det.evaluate_pair_stability("ETH", "BINANCE", "BITGET", 199.5, 199.5, pre_spread, ts_mono=200.1)
        det.evaluate_pair_stability("ETH", "BINANCE", "BITGET", 199.0, 199.0, pre_spread, ts_mono=200.2)

        # Шаг 2: Мишень продавили до 197.40 (-0.80% к 199.0), Оракул стоит на 199.0
        ok_shot, r_shot, info_shot = det.evaluate_pair_stability(
            "ETH", "BINANCE", "BITGET", 199.0, 197.40, pre_spread, ts_mono=200.25
        )
        self.assertTrue(ok_shot)
        self.assertEqual(info_shot["case"], "CASE_C")
        self.assertEqual(info_shot["static_leg"], "ORACLE")
        self.assertIn("CASE_C_OK", r_shot)
        self.assertAlmostEqual(info_shot["oracle_delta"], 0.0, places=5)
        self.assertGreater(info_shot["target_delta"], 0.007)

    def test_case_a_both_legs_diverge_rejected(self):
        """
        Проверка Кейса А (Обе ноги хаотично разлетелись):
        Оракул улетел вверх на +0.40%, Мишень обвалилась на -0.40%.
        Оба превысили max_static_leg_ratio (0.20%) -> REJECT!
        """
        cfg = {
            "trading_rules": {
                "entry": {
                    "signal_filters": {
                        "static_detector": {
                            "enabled": True,
                            "static_leg": "ANY",
                            "max_static_leg_ratio": 0.0020,
                            "buffer_window_sec": 0.35
                        }
                    }
                }
            }
        }
        det = StaticDetector(cfg)
        pre_spread = 0.006

        det.evaluate_pair_stability("SOL", "BINANCE", "BITGET", 100.0, 100.0, pre_spread, ts_mono=300.0)
        
        # Обе ноги разошлись: Оракул 100.40 (+0.4%), Мишень 99.60 (-0.4%)
        ok, reason, info = det.evaluate_pair_stability(
            "SOL", "BINANCE", "BITGET", 100.40, 99.60, pre_spread, ts_mono=300.1
        )
        self.assertFalse(ok)
        self.assertEqual(info["case"], "CASE_A")
        self.assertIn("CASE_A_REJECTED", reason)

    def test_strict_static_leg_modes(self):
        """
        Проверка жестких режимов static_leg: TARGET и static_leg: ORACLE.
        """
        # 1. Режим строго TARGET: разрешен только Кейс Б, Кейс В блокируется
        cfg_target = {
            "trading_rules": {
                "entry": {
                    "signal_filters": {
                        "static_detector": {
                            "enabled": True,
                            "static_leg": "TARGET",
                            "max_static_leg_ratio": 0.0020,
                            "buffer_window_sec": 0.35
                        }
                    }
                }
            }
        }
        det_tgt = StaticDetector(cfg_target)
        det_tgt.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, 0.006, ts_mono=10.0)

        # Кейс Б -> PASS
        ok_b, _, info_b = det_tgt.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.80, 100.0, 0.006, ts_mono=10.1)
        self.assertTrue(ok_b)
        self.assertEqual(info_b["case"], "CASE_B")

        # Сброс и проверка Кейса В -> REJECT
        det_tgt_c = StaticDetector(cfg_target)
        det_tgt_c.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, 0.006, ts_mono=20.0)
        ok_c, r_c, info_c = det_tgt_c.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 99.20, 0.006, ts_mono=20.1)
        self.assertFalse(ok_c)
        self.assertIn("CASE_C_REJECTED", r_c)

        # 2. Режим строго ORACLE: разрешен только Кейс В, Кейс Б блокируется
        cfg_oracle = {
            "trading_rules": {
                "entry": {
                    "signal_filters": {
                        "static_detector": {
                            "enabled": True,
                            "static_leg": "ORACLE",
                            "max_static_leg_ratio": 0.0020,
                            "buffer_window_sec": 0.35
                        }
                    }
                }
            }
        }
        det_orc = StaticDetector(cfg_oracle)
        det_orc.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, 0.006, ts_mono=30.0)

        # Кейс Б -> REJECT
        ok_b2, r_b2, _ = det_orc.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.80, 100.0, 0.006, ts_mono=30.1)
        self.assertFalse(ok_b2)
        self.assertIn("CASE_B_NOT_ALLOWED", r_b2)

        # Кейс В -> PASS
        det_orc_c = StaticDetector(cfg_oracle)
        det_orc_c.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, 0.006, ts_mono=40.0)
        ok_c2, _, info_c2 = det_orc_c.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 99.20, 0.006, ts_mono=40.1)
        self.assertTrue(ok_c2)
        self.assertEqual(info_c2["case"], "CASE_C")

if __name__ == '__main__':
    unittest.main()


