import unittest
import numpy as np
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from CORE.math_core import calc_vwap_usd_jit, calc_vwap_qty_jit, pre_calculate_orderbook, OrderbookUtils

class TestMathCore(unittest.TestCase):
    
    def test_calc_vwap_usd_jit(self):
        book = np.array([
            [100.0, 10.0],
            [110.0, 10.0],
        ], dtype=np.float64)
        
        vwap = calc_vwap_usd_jit(book, 500.0, 1.0)
        self.assertEqual(vwap, 100.0)
        
        vwap = calc_vwap_usd_jit(book, 1550.0, 1.0)
        self.assertAlmostEqual(vwap, 103.33333333333333)

        vwap = calc_vwap_usd_jit(book, 5000.0, 1.0)
        self.assertEqual(vwap, 0.0)

    def test_calc_vwap_usd_jit_with_volatility_discount(self):
        book = np.array([
            [100.0, 10.0],
        ], dtype=np.float64)
        
        vwap = calc_vwap_usd_jit(book, 600.0, 0.5)
        self.assertEqual(vwap, 0.0)
        
        vwap = calc_vwap_usd_jit(book, 200.0, 0.5)
        self.assertEqual(vwap, 100.0)

    def test_pre_calculate_orderbook(self):
        prices = np.array([
            [100.0, 99.0],    # BINANCE: ask 100, bid 99
            [102.0, 101.5],   # BITGET: ask 102, bid 101.5
            [105.0, 104.0]    # KUCOIN: ask 105, bid 104
        ], dtype=np.float64)
        
        active_routes = np.array([
            [0, 1], # BINANCE -> BITGET: buy BINANCE(ask 100), sell BITGET(bid 101.5). Spread: (101.5-100)/101.5 * 100 = 1.4778%
            [1, 0], # BITGET -> BINANCE: buy BITGET(ask 102), sell BINANCE(bid 99). Spread: (99-102)/99 * 100 = -3.03%
            [0, 2]  # BINANCE -> KUCOIN: buy BINANCE(ask 100), sell KUCOIN(bid 104). Spread: (104-100)/104 * 100 = 3.846%
        ], dtype=np.int32)
        
        res = pre_calculate_orderbook(prices, active_routes, top_n=2)
        
        self.assertEqual(res[0][0], 0.0) # BINANCE
        self.assertEqual(res[0][1], 2.0) # KUCOIN
        self.assertAlmostEqual(res[0][2], 3.846153846153846)
        
        self.assertEqual(res[1][0], 0.0)
        self.assertEqual(res[1][1], 1.0)
        self.assertAlmostEqual(res[1][2], 1.4778325123152708)

if __name__ == '__main__':
    unittest.main()
