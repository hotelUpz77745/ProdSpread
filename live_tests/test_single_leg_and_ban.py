# ============================================================
# FILE: live_tests/test_single_leg_and_ban.py
# ROLE: Verification of Hard Stop-Loss, Severe Loss Permanent Ban, and Consecutive Losses.
# ============================================================

import asyncio
import json
import os
import sys
import copy
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from CORE.position_fsm import PositionFSM, PositionState
from CORE.executor_process import ExecutorProcess


class MockExchangeOrder:
    def __init__(self, name: str, fill_size: float = 0.0, book_bid: float = 1.0, book_ask: float = 1.0):
        self.name = name
        self.fill_size = fill_size
        self.book_bid = book_bid
        self.book_ask = book_ask
        self.placed_orders = []
        self.positions = {}

    async def get_position_rest(self, symbol: str, position_side: str = None):
        return {"size": self.positions.get(position_side, self.fill_size)}

    async def get_book_ticker(self, symbol: str):
        return {"bid": self.book_bid, "ask": self.book_ask}

    def get_last_close_price(self, symbol: str):
        return self.book_bid

    async def place_order(self, symbol, side, size_usd, price, order_type="LIMIT_IOC", position_side=None, exact_qty=None, reduce_only=False, is_full_unwind=False):
        self.placed_orders.append({
            "symbol": symbol, "side": side, "usd": size_usd, "price": price,
            "type": order_type, "position_side": position_side, "qty": exact_qty
        })
        if reduce_only:
            # Emulate position closed
            self.positions[position_side] = 0.0
            self.fill_size = 0.0
        return {"status": "FILLED", "orderId": 12345}


class TestSingleLegAndBan(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cfg.json")
        with open(cfg_path, "r", encoding="utf-8") as f:
            self.cfg = json.load(f)

    async def test_circuit_breaker_hard_stop_loss(self):
        """If price moves against single naked leg by >= max_chase_loss_ratio (0.50%), market exit triggers immediately."""
        cfg = copy.deepcopy(self.cfg)
        cfg["trading_rules"]["exit"]["single_leg_exit"]["max_chase_loss_ratio"] = 0.0050
        
        # We hold LONG on Binance at entry_price = 1.000.
        # But market bids drop to 0.994 (-0.60% loss > 0.50%).
        binance = MockExchangeOrder("BINANCE", fill_size=100.0, book_bid=0.994, book_ask=0.995)
        binance.positions["LONG"] = 100.0
        kucoin = MockExchangeOrder("KUCOIN", fill_size=0.0)

        orders = {"BINANCE": binance, "KUCOIN": kucoin}
        ban_calls = []
        def mock_ban(sym, reason="", duration_sec=None):
            ban_calls.append((sym, reason, duration_sec))

        engine_res = {
            "long_avg_price": 1.000,
            "short_avg_price": 1.008,
            "long_qty": 100.0,
            "short_qty": 100.0
        }

        fsm = PositionFSM(
            sym="TESTCOIN",
            route="BINANCE_KUCOIN",
            long_ex="BINANCE",
            short_ex="KUCOIN",
            engine_res=engine_res,
            cfg=cfg,
            orders=orders,
            coin_to_native={},
            pm=None,
            writer=None,
            ban_coin_cb=mock_ban
        )

        # Trigger single leg exposure directly
        await fsm._run_single_leg_exposure(100.0, 0.0, 1.000, 0.0)

        # Check that emergency unwind (MARKET reduce_only) was sent immediately due to circuit breaker
        market_orders = [o for o in binance.placed_orders if o["type"] == "MARKET" and o["position_side"] == "LONG"]
        self.assertTrue(len(market_orders) > 0, "Hard Stop-Loss circuit breaker did not trigger MARKET unwind")
        self.assertEqual(fsm.state, PositionState.ABORTED)

    async def test_severe_loss_permanent_ban(self):
        """Single leg loss >= perm_ban_loss_pct (0.75%) triggers lifetime permanent ban (duration_sec=None)."""
        cfg = copy.deepcopy(self.cfg)
        cfg["trading_rules"]["ban_rules"]["perm_ban_loss_pct"] = 0.0075

        # We hold LONG at 1.000, market drops to 0.985 (-1.5% loss >= 0.75%)
        binance = MockExchangeOrder("BINANCE", fill_size=100.0, book_bid=0.985, book_ask=0.986)
        binance.positions["LONG"] = 100.0
        kucoin = MockExchangeOrder("KUCOIN", fill_size=0.0)

        ban_calls = []
        def mock_ban(sym, reason="", duration_sec=None):
            ban_calls.append((sym, reason, duration_sec))

        engine_res = {"long_avg_price": 1.000, "short_avg_price": 1.008, "long_qty": 100.0, "short_qty": 100.0}

        fsm = PositionFSM(
            sym="TOXICCOIN",
            route="BINANCE_KUCOIN",
            long_ex="BINANCE",
            short_ex="KUCOIN",
            engine_res=engine_res,
            cfg=cfg,
            orders={"BINANCE": binance, "KUCOIN": kucoin},
            coin_to_native={},
            pm=None,
            writer=None,
            ban_coin_cb=mock_ban
        )

        await fsm._run_single_leg_exposure(100.0, 0.0, 1.000, 0.0)

        self.assertTrue(len(ban_calls) > 0)
        sym, reason, duration_sec = ban_calls[-1]
        self.assertEqual(sym, "TOXICCOIN")
        self.assertIsNone(duration_sec, f"Expected permanent ban (duration_sec=None), got {duration_sec}")
        self.assertIn("Severe Single Leg Loss", reason)

    async def test_consecutive_losses_permanent_ban_in_executor(self):
        """Two consecutive losses on a coin trigger permanent ban."""
        cfg = copy.deepcopy(self.cfg)
        cfg["trading_rules"]["ban_rules"]["max_consecutive_losses"] = 2
        executor = ExecutorProcess(port=9999, cfg=cfg)

        # 1st loss (small loss, -0.20%)
        executor.ban_coin("BADCOIN", reason="Single Leg Loss (-0.05$)", duration_sec=1800)
        self.assertIsNotNone(executor.banned_symbols["BADCOIN"])
        self.assertEqual(executor.consecutive_loss_counts["BADCOIN"], 1)

        # 2nd loss (small loss, -0.20%)
        executor.ban_coin("BADCOIN", reason="Single Leg Loss (-0.05$)", duration_sec=1800)
        # Reached 2 consecutive losses -> permanent ban!
        self.assertIsNone(executor.banned_symbols["BADCOIN"])
        self.assertEqual(executor.consecutive_loss_counts["BADCOIN"], 2)

    async def test_profit_resets_consecutive_losses(self):
        """A profitable trade resets consecutive losses counter and skips quarantine."""
        cfg = copy.deepcopy(self.cfg)
        executor = ExecutorProcess(port=9999, cfg=cfg)

        # 1st loss
        executor.ban_coin("GOODCOIN", reason="Loss trade, Net: -0.02$", duration_sec=3600)
        self.assertEqual(executor.consecutive_loss_counts["GOODCOIN"], 1)

        # Profit trade (duration_sec=0)
        executor.ban_coin("GOODCOIN", reason="Single Leg Profit (+0.05$)", duration_sec=0)
        self.assertNotIn("GOODCOIN", executor.consecutive_loss_counts)
        # Should not be banned
        self.assertNotIn("GOODCOIN", executor.banned_symbols)


if __name__ == "__main__":
    unittest.main()
