# ============================================================
# FILE: live_tests/test_single_leg_and_ban.py
# ROLE: Verification of Hard Stop-Loss, Severe Loss Permanent Ban, and Consecutive Losses (v9).
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
from CORE.trading_engine import TradingEngine


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

    async def get_exact_position_guarded(self, symbol: str, position_side: str = None):
        return {"size": self.positions.get(position_side, self.fill_size), "price": self.book_bid}

    def get_executed_position(self, symbol: str, position_side: str = None):
        return {"size": self.positions.get(position_side, self.fill_size), "price": self.book_bid}

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
            self.positions[position_side] = 0.0
            self.fill_size = 0.0
        return {"status": "FILLED", "orderId": 12345}


class TestSingleLegAndBan(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cfg.json")
        with open(cfg_path, "r", encoding="utf-8") as f:
            self.cfg = json.load(f)

    async def test_circuit_breaker_hard_stop_loss(self):
        cfg = copy.deepcopy(self.cfg)
        if "exchanges" in cfg and "BITGET" in cfg["exchanges"] and "exit" in cfg["exchanges"]["BITGET"]:
            cfg["exchanges"]["BITGET"]["exit"]["target_exit"]["stop_loss_ratio"] = 0.0050
            cfg["exchanges"]["BITGET"]["exit"]["target_exit"].pop("stop_loss_pct", None)
        elif "trading_rules" in cfg and "exit" in cfg["trading_rules"]:
            cfg["trading_rules"]["exit"]["target_exit"]["stop_loss_ratio"] = 0.0050
            cfg["trading_rules"]["exit"]["target_exit"].pop("stop_loss_pct", None)
        
        engine = TradingEngine(cfg, {0: "BINANCE", 1: "KUCOIN", 2: "OKX", 3: "BITGET"})
        
        # We hold LONG on Bitget at entry_price = 1.000.
        # Market bids drop to 0.992 (-0.80% gross, minus fees = ~ -0.92% net < -0.50%).
        target_book = {
            "bids": [[0.992, 1000.0]],
            "asks": [[0.993, 1000.0]]
        }
        
        is_exit, exit_res = engine.evaluate_exit_v9(
            target_book=target_book,
            target_ex="BITGET",
            entry_price=1.000,
            qty=100.0,
            side="LONG",
            duration_sec=5.0,
            actual_net_spread_entry=0.01
        )
        
        self.assertTrue(is_exit)
        self.assertEqual(exit_res["reason"], "STOP_LOSS")

    async def test_severe_loss_permanent_ban(self):
        """Single leg loss >= perm_ban_loss_pct (0.75%) triggers lifetime permanent ban."""
        cfg = copy.deepcopy(self.cfg)
        executor = ExecutorProcess(port=9999, cfg=cfg)

        # Net yield = -0.80% (exceeds perm_ban_loss_pct 0.75%)
        net_yield = -0.0080
        net_usd = -0.20
        ban_rules = cfg.get("ban_rules") or cfg.get("trading_rules", {}).get("ban_rules", {})
        perm_ban_ratio = float(ban_rules["perm_ban_loss_ratio"]) if "perm_ban_loss_ratio" in ban_rules else float(ban_rules["perm_ban_loss_pct"])
        
        if abs(net_yield) >= perm_ban_ratio:
            executor.ban_coin("TOXICCOIN", reason=f"Severe loss trade, Net: {net_usd:+.4f}$ ({net_yield*100:+.2f}%)", duration_sec=None)
            
        self.assertIn("TOXICCOIN", executor.banned_symbols)
        self.assertIsNone(executor.banned_symbols["TOXICCOIN"])

    async def test_consecutive_losses_permanent_ban_in_executor(self):
        """Two consecutive losses on a coin trigger permanent ban."""
        cfg = copy.deepcopy(self.cfg)
        if "ban_rules" in cfg:
            cfg["ban_rules"]["max_consecutive_losses"] = 2
        if "trading_rules" in cfg and "ban_rules" in cfg["trading_rules"]:
            cfg["trading_rules"]["ban_rules"]["max_consecutive_losses"] = 2
        executor = ExecutorProcess(port=9999, cfg=cfg)

        # 1st loss (small loss, -0.20%)
        executor.ban_coin("BADCOIN", reason="Loss trade (-0.05$)", duration_sec=1800)
        self.assertIsNotNone(executor.banned_symbols["BADCOIN"])
        self.assertEqual(executor.consecutive_loss_counts["BADCOIN"], 1)

        # 2nd loss (small loss, -0.20%)
        executor.ban_coin("BADCOIN", reason="Loss trade (-0.05$)", duration_sec=1800)
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
