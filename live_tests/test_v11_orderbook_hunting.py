# ============================================================
# FILE: live_tests/test_v11_orderbook_hunting.py
# ROLE: Unit tests for v11 Orderbook Hunting, Strict Case B, and Synthetic Exit.
# ============================================================

import unittest
from CORE.math_core import StaticDetector, OrderbookHunter
from CORE.trading_engine import TradingEngine


class TestV11OrderbookHunting(unittest.TestCase):
    def setUp(self):
        self.cfg = {
            "exchanges": {
                "BINANCE": {
                    "trading_risks": {"taker_fee": 0.0005, "volatility_discount_entry": 0.5, "volatility_discount_exit": 0.85},
                    "entry": {"signal_filters": {"static_detector": {"enabled": True, "static_leg": "TARGET", "max_static_leg_ratio": 0.0020, "buffer_window_sec": 0.35}, "spread_entry_base": 0.006, "synthetic_exit": {"enabled": True, "check_slippage": True, "max_slippage_ratio": 0.30, "hard_max_slippage": 0.008}}},
                    "exit": {"target_exit": {"exit_order_type": "LIMIT_IOC", "stop_loss_ratio": 0.0050, "min_spread_entry": 0.0030, "orderbook_hunting": {"base_scenario": {"enabled": True, "target_rate": 0.80, "shift_demotion": 0.20, "min_target_rate": 0.40, "shift_ttl_sec": 3.0}, "breakeven_stage": {"enabled": True, "ttl_sec": 7.0, "min_net_profit_ratio": 0.0004, "wait_sec": 2.0}, "extrime_close": {"enabled": True, "retry_ttl_sec": 0.3, "max_retries": 10, "bid_to_ask_orientation": 0.0, "increase_fraction": 0.05}, "oracle_reversal_stop": {"enabled": True, "max_oracle_adverse_ratio": 0.0040}}}}
                },
                "BITGET": {
                    "trading_risks": {"taker_fee": 0.0006, "volatility_discount_entry": 0.5, "volatility_discount_exit": 0.85},
                    "entry": {"signal_filters": {"static_detector": {"enabled": True, "static_leg": "TARGET", "max_static_leg_ratio": 0.0020, "buffer_window_sec": 0.35}, "spread_entry_base": 0.006, "synthetic_exit": {"enabled": True, "check_slippage": True, "max_slippage_ratio": 0.30, "hard_max_slippage": 0.008}}},
                    "exit": {"target_exit": {"exit_order_type": "LIMIT_IOC", "stop_loss_ratio": 0.0050, "min_spread_entry": 0.0030, "orderbook_hunting": {"base_scenario": {"enabled": True, "target_rate": 0.80, "shift_demotion": 0.20, "min_target_rate": 0.40, "shift_ttl_sec": 3.0}, "breakeven_stage": {"enabled": True, "ttl_sec": 7.0, "min_net_profit_ratio": 0.0004, "wait_sec": 2.0}, "extrime_close": {"enabled": True, "retry_ttl_sec": 0.3, "max_retries": 10, "bid_to_ask_orientation": 0.0, "increase_fraction": 0.05}, "oracle_reversal_stop": {"enabled": True, "max_oracle_adverse_ratio": 0.0040}}}}
                }
            },
            "trading_rules": {
                "entry": {"signal_filters": {"static_detector": {"enabled": True, "static_leg": "TARGET", "max_static_leg_ratio": 0.0020, "buffer_window_sec": 0.35}, "spread_entry_base": 0.006, "synthetic_exit": {"enabled": True, "check_slippage": True, "max_slippage_ratio": 0.30, "hard_max_slippage": 0.008}}},
                "exit": {"target_exit": {"exit_order_type": "LIMIT_IOC", "stop_loss_ratio": 0.0050, "min_spread_entry": 0.0030, "orderbook_hunting": {"base_scenario": {"enabled": True, "target_rate": 0.80, "shift_demotion": 0.20, "min_target_rate": 0.40, "shift_ttl_sec": 3.0}, "breakeven_stage": {"enabled": True, "ttl_sec": 7.0, "min_net_profit_ratio": 0.0004, "wait_sec": 2.0}, "extrime_close": {"enabled": True, "retry_ttl_sec": 0.3, "max_retries": 10, "bid_to_ask_orientation": 0.0, "increase_fraction": 0.05}, "oracle_reversal_stop": {"enabled": True, "max_oracle_adverse_ratio": 0.0040}}}}
            }
        }
        self.engine = TradingEngine(self.cfg, {0: "BINANCE", 1: "BITGET"})

    # --- 1. Strict Case B vs Case C Rejection Tests ---
    def test_strict_case_b_accepted_case_c_rejected(self):
        """Кейс Б принимается, Кейс В категорически отвергается."""
        det = StaticDetector(self.cfg)
        det.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, 0.005, ts_mono=10.0)

        # Case B: Oracle fires +0.8%, Target stands still -> ACCEPT
        ok_b, reason_b, info_b = det.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.80, 100.0, 0.005, ts_mono=10.1)
        self.assertTrue(ok_b)
        self.assertEqual(info_b["case"], "CASE_B")

        # Reset & Case C: Target drops -0.8%, Oracle stands still -> REJECT toxic dump
        det_c = StaticDetector(self.cfg)
        det_c.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 100.0, 0.005, ts_mono=20.0)
        ok_c, reason_c, info_c = det_c.evaluate_pair_stability("BTC", "BINANCE", "BITGET", 100.0, 99.20, 0.005, ts_mono=20.1)
        self.assertFalse(ok_c)
        self.assertIn("CASE_C_REJECTED", reason_c)

    # --- 3. OrderbookHunter Math Tests ---
    def test_orderbook_hunter_virtual_tp(self):
        """Проверка расчета Virtual TP для Long и Short."""
        # Long: entry 100, target 101 (+1%), rate 0.8 -> virtual_tp = 100.80
        tp_long = OrderbookHunter.calc_virtual_tp(100.0, 101.0, 0.80, "LONG")
        self.assertAlmostEqual(tp_long, 100.80, places=4)

        # Short: entry 100, target 99 (-1%), rate 0.8 -> virtual_tp = 99.20
        tp_short = OrderbookHunter.calc_virtual_tp(100.0, 99.0, 0.80, "SHORT")
        self.assertAlmostEqual(tp_short, 99.20, places=4)

    def test_orderbook_hunter_find_liquidity(self):
        """Поиск уровня с максимальным объемом у/выше Virtual TP."""
        bids = [
            [101.0, 2.0],
            [100.9, 10.0],
            [100.8, 50.0],  # Max vol >= 100.8
            [100.7, 100.0]
        ]
        ideal_price = OrderbookHunter.find_liquidity_target(bids, virtual_tp=100.8, side="LONG", min_vol=5.0)
        self.assertEqual(ideal_price, 100.8)

    def test_orderbook_hunter_breakeven_price(self):
        """Расчет цены безубытка с комиссиями."""
        # Entry = 100, fee = 0.06% (0.0006), min_profit = 0.04% (0.0004) -> markup = 0.16% (0.0016)
        be_long = OrderbookHunter.calc_breakeven_price(100.0, 0.0006, min_net_profit_ratio=0.0004, side="LONG")
        self.assertAlmostEqual(be_long, 100.16, places=4)

        be_short = OrderbookHunter.calc_breakeven_price(100.0, 0.0006, min_net_profit_ratio=0.0004, side="SHORT")
        self.assertAlmostEqual(be_short, 99.84, places=4)

    def test_orderbook_hunter_extrime_price(self):
        """Прогрессивный расчет цены Extrime Close со смещением вглубь стакана."""
        best_bid = 100.00
        best_ask = 100.02
        # retry 0: mid = 100.01, shift = 0 -> 100.01
        p0 = OrderbookHunter.calc_extrime_price(best_bid, best_ask, "LONG", retry_count=0, orientation=0.0, increase_fraction=0.05)
        self.assertAlmostEqual(p0, 100.01, places=4)

        # retry 5: mid = 100.01, spread = 0.02, shift = 0.02 * 0.05 * 5 = 0.005 -> 100.005
        p5 = OrderbookHunter.calc_extrime_price(best_bid, best_ask, "LONG", retry_count=5, orientation=0.0, increase_fraction=0.05)
        self.assertAlmostEqual(p5, 100.005, places=4)

    # --- 4. TradingEngine Exit Evaluation Cycle Tests ---
    def test_exit_hunting_base_take_profit(self):
        """Шаг 1: Исполнение Take-Profit через хантинг стакана по LIMIT_IOC."""
        target_book = {
            "bids": [[100.85, 100.0], [100.80, 50.0], [100.75, 20.0]],
            "asks": [[100.86, 100.0]]
        }
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=100.0,
            qty=1.0,
            side="LONG",
            duration_sec=1.0,
            actual_net_spread_entry=0.010
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "TAKE_PROFIT")
        self.assertEqual(res["order_type"], "LIMIT_IOC")
        self.assertAlmostEqual(res["exit_price"], 100.85, places=2)

    def test_exit_hunting_breakeven_stage(self):
        """Шаг 2: По истечении 7с переход в Breakeven Stage по LIMIT_IOC."""
        target_book = {
            "bids": [[100.18, 100.0], [100.16, 50.0], [100.14, 20.0]],
            "asks": [[100.19, 100.0]]
        }
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=100.0,
            qty=1.0,
            side="LONG",
            duration_sec=7.5,  # >= be_ttl 7.0s and < 9.0s
            actual_net_spread_entry=0.010
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "BREAKEVEN")
        self.assertEqual(res["order_type"], "LIMIT_IOC")
        self.assertAlmostEqual(res["exit_price"], 100.18, places=2)

    def test_exit_hunting_extrime_close(self):
        """Шаг 3: По истечении окна ожидания БУ (>9с) переход в Extrime Close по LIMIT_IOC."""
        target_book = {
            "bids": [[100.00, 100.0]],
            "asks": [[100.02, 100.0]]
        }
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=100.0,
            qty=1.0,
            side="LONG",
            duration_sec=9.5,  # > 7.0 + 2.0s
            actual_net_spread_entry=0.010,
            retry_count=1
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "EXTRIME_CLOSE")
        self.assertEqual(res["order_type"], "LIMIT_IOC")
        self.assertGreater(res["exit_price"], 0.0)

    def test_exit_oracle_reversal_stop(self):
        """Шаг 4: Разворот Оракула против позы вызывает аварийный ORACLE_REVERSAL_STOP по MARKET."""
        target_book = {"bids": [[100.00, 100]], "asks": [[100.02, 100]]}
        # Oracle dumped from 101 to 99.5 (-0.5% against Long)
        oracle_book = {"bids": [[99.50, 100]], "asks": [[99.52, 100]]}
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=100.0,
            qty=1.0,
            side="LONG",
            duration_sec=2.0,
            actual_net_spread_entry=0.010,
            oracle_book=oracle_book,
            oracle_ex="BINANCE"
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "ORACLE_REVERSAL_STOP")
        self.assertEqual(res["order_type"], "MARKET")

    def test_evaluate_exit_hunting_alias(self):
        """Проверка работы явного метода-алиаса evaluate_exit_hunting."""
        target_book = {
            "bids": [[100.85, 100.0], [100.80, 50.0], [100.75, 20.0]],
            "asks": [[100.86, 100.0]]
        }
        ok, res = self.engine.evaluate_exit_hunting(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=100.0,
            qty=1.0,
            side="LONG",
            duration_sec=1.0,
            actual_net_spread_entry=0.010
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "TAKE_PROFIT")
        self.assertEqual(res["order_type"], "LIMIT_IOC")
        self.assertAlmostEqual(res["exit_price"], 100.85, places=2)


if __name__ == "__main__":
    unittest.main()

