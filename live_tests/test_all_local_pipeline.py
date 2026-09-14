# ============================================================
# FILE: live_tests/test_all_local_pipeline.py
# ROLE: Комплексный локальный запуск тестов всех веток и компонентов пайплайна v9
#       - Без сетевых рисков, без реальных ордеров, 100% offline
# ============================================================

import asyncio
import os
import sys
import unittest
from unittest.mock import MagicMock, AsyncMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from CORE.trading_engine import TradingEngine
from CORE.position_fsm import PositionFSM, PositionState
from CORE.position_manager import PositionManager
from CORE.executor_process import ExecutorProcess
from CORE.ipc_socket import async_write_msg, async_read_msg
from API.orders import round_by_step, InsufficientMarginError
from decimal import ROUND_FLOOR

def get_base_cfg():
    return {
        "QUOTE": "USDT",
        "MAIN_LOOP_DELAY": 0.0,
        "exchange_roles": {
            "BINANCE_KUCOIN": {"oracle": "BINANCE", "target": "KUCOIN"},
            "BINANCE_BITGET": {"oracle": "BINANCE", "target": "BITGET"}
        },
        "margin_settings": {
            "BINANCE": {"margin_type": "ISOLATED", "leverage": 11},
            "KUCOIN": {"margin_type": "CROSS", "leverage": 20},
            "BITGET": {"margin_type": "crossed", "leverage": 20}
        },
        "network_settings": {
            "rest_keepalive_interval_sec": 0,
            "idle_warmup_threshold_sec": 0
        },
        "trading_risks": {
            "binance": {"trade_size_usd": 25.0, "taker_fee": 0.0005, "limit_slip_ratio": 0.002, "volatility_discount_entry": 0.4, "volatility_discount_exit": 0.85, "max_positions": 1},
            "kucoin": {"trade_size_usd": 25.0, "taker_fee": 0.0006, "limit_slip_ratio": 0.002, "volatility_discount_entry": 0.4, "volatility_discount_exit": 0.85, "max_positions": 1},
            "bitget": {"trade_size_usd": 25.0, "taker_fee": 0.0006, "limit_slip_ratio": 0.002, "volatility_discount_entry": 0.4, "volatility_discount_exit": 0.85, "max_positions": 1},
            "okx": {"trade_size_usd": 0.0, "taker_fee": 0.0006, "limit_slip_ratio": 0.002, "volatility_discount_entry": 0.4, "volatility_discount_exit": 0.85, "max_positions": 1}
        },
        "trading_rules": {
            "entry": {
                "order_execution_type": "TARGET_LIMIT_IOC",
                "static_detector": {
                    "enabled": True,
                    "static_leg": "TARGET",
                    "max_static_leg_ratio": 0.0020,
                    "buffer_window_sec": 0.25
                },
                "target_entry_logic": {
                    "entry_slip_ratio": 0.0020,
                    "fill_confirm_timeout_sec": {"BINANCE_KUCOIN": 0.5, "BINANCE_BITGET": 0.5},
                    "fill_confirm_poll_interval_sec": 0.005,
                    "entry_api_timeout_sec": 2.0
                },
                "signal_filters": {
                    "spread_entry_pre": [0.004, 0.03],
                    "spread_entry_base": 0.004,
                    "min_top_depth_usd": 50.0,
                    "orderbook_imbalance": {"enabled": True, "depth_levels": 5, "max_adverse_imbalance": 0.55}
                }
            },
            "exit": {
                "max_desync_ms": 150,
                "close_confirm_timeout_sec": 1.0,
                "target_exit": {
                    "ttl_sec": 60.0,
                    "exit_order_type": "LIMIT_IOC",
                    "exit_slip_ratio": 0.001,
                    "ioc_chase_timeout_sec": 0.2,
                    "stop_loss_ratio": 0.015,
                    "decay_map": [
                        {"step": 1, "after_sec": 0, "target_spread": 0.005},
                        {"step": 2, "after_sec": 10, "target_spread": 0.002},
                        {"step": 3, "after_sec": 30, "target_spread": 0.0005}
                    ]
                }
            },
            "emergency_unwind": {
                "max_attempts": 2,
                "ws_verify_timeout_sec": 0.2,
                "retry_pause_sec": 0.01
            },
            "ban_rules": {
                "is_active": True,
                "max_consecutive_losses": 3,
                "perm_ban_loss_ratio": 0.0075,
                "quarantine_sec": {
                    "entry_error": 60,
                    "zero_fill": 10,
                    "loss_trade": 300
                }
            }
        }
    }


class TestPipelineTradingEngine(unittest.TestCase):
    def setUp(self):
        self.cfg = get_base_cfg()
        self.engine = TradingEngine(self.cfg, {0: "BINANCE", 1: "KUCOIN", 2: "OKX", 3: "BITGET"})

    def test_branch_entry_long(self):
        """Ветка LONG: Oracle (Binance) выше аска Target (KuCoin) -> покупаем на Target."""
        oracle_book = {"bids": [[102.0, 1000]], "asks": [[102.02, 1000]]} # mid = 102.01
        target_book = {"bids": [[100.0, 1000]], "asks": [[100.05, 1000]]} # vwap_ask ~ 100.05
        ok, res = self.engine.evaluate_entry_v9("BTC", oracle_book, target_book, "BINANCE", "KUCOIN", 25.0)
        self.assertTrue(ok)
        self.assertEqual(res["side"], "LONG")
        self.assertEqual(res["target_ex"], "KUCOIN")
        self.assertEqual(res["oracle_ex"], "BINANCE")
        self.assertGreater(res["net_spread"], 0.004)

    def test_branch_entry_short(self):
        """Ветка SHORT: Oracle (Binance) ниже бида Target (KuCoin) -> шортим на Target."""
        oracle_book = {"bids": [[98.0, 1000]], "asks": [[98.02, 1000]]}   # mid = 98.01
        target_book = {"bids": [[100.0, 1000]], "asks": [[100.05, 1000]]} # vwap_bid ~ 100.00
        ok, res = self.engine.evaluate_entry_v9("BTC", oracle_book, target_book, "BINANCE", "KUCOIN", 25.0)
        self.assertTrue(ok)
        self.assertEqual(res["side"], "SHORT")
        self.assertEqual(res["target_ex"], "KUCOIN")
        self.assertGreater(res["net_spread"], 0.004)

    def test_branch_entry_low_spread(self):
        """Ветка отклонения по низкому спреду."""
        oracle_book = {"bids": [[100.0, 1000]], "asks": [[100.02, 1000]]}
        target_book = {"bids": [[100.0, 1000]], "asks": [[100.02, 1000]]}
        ok, res = self.engine.evaluate_entry_v9("BTC", oracle_book, target_book, "BINANCE", "KUCOIN", 25.0)
        self.assertFalse(ok)
        self.assertIn("LOW_SPREAD", res.get("reason", ""))

    def test_branch_entry_high_spread(self):
        """Ветка отклонения по аномально высокому спреду (> spread_entry_max 3%)."""
        oracle_book = {"bids": [[110.0, 1000]], "asks": [[110.02, 1000]]}
        target_book = {"bids": [[100.0, 1000]], "asks": [[100.05, 1000]]}
        ok, res = self.engine.evaluate_entry_v9("BTC", oracle_book, target_book, "BINANCE", "KUCOIN", 25.0)
        self.assertFalse(ok)
        self.assertIn("HIGH_SPREAD", res.get("reason", ""))

    def test_branch_entry_adverse_obi(self):
        """Ветка фильтра дисбаланса стакана (OBI)."""
        oracle_book = {"bids": [[102.0, 1000]], "asks": [[102.02, 1000]]}
        target_book = {"bids": [[100.0, 10]], "asks": [[100.05, 10000]]}
        ok, res = self.engine.evaluate_entry_v9("BTC", oracle_book, target_book, "BINANCE", "KUCOIN", 25.0)
        self.assertFalse(ok)
        self.assertIn("ADVERSE_OBI_LONG", res.get("reason", ""))

    def test_branch_exit_take_profit(self):
        """Ветка выхода по Take-Profit."""
        target_book = {"bids": [[101.5, 1000]], "asks": [[101.6, 1000]]}
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book, target_ex="KUCOIN", entry_price=100.0, qty=1.0,
            side="LONG", duration_sec=5.0, actual_net_spread_entry=0.01
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "TAKE_PROFIT")
        self.assertGreater(res["net_pnl_pct"], 0.005)

    def test_branch_exit_stop_loss(self):
        """Ветка выхода по Stop-Loss (рынок провалился > 1.5%)."""
        target_book = {"bids": [[98.0, 1000]], "asks": [[98.1, 1000]]}
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book, target_ex="KUCOIN", entry_price=100.0, qty=1.0,
            side="LONG", duration_sec=5.0, actual_net_spread_entry=0.01
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "STOP_LOSS")

    def test_branch_exit_ttl_expired(self):
        """Ветка принудительного выхода по тайм-ауту удержания (TTL >= 60s)."""
        target_book = {"bids": [[100.0, 1000]], "asks": [[100.1, 1000]]}
        ok, res = self.engine.evaluate_exit_v9(
            target_book=target_book, target_ex="KUCOIN", entry_price=100.0, qty=1.0,
            side="LONG", duration_sec=65.0, actual_net_spread_entry=0.01
        )
        self.assertTrue(ok)
        self.assertEqual(res["reason"], "TTL_EXPIRED")


class TestPipelinePositionManager(unittest.TestCase):
    def setUp(self):
        self.cfg = get_base_cfg()
        self.pm = PositionManager(
            self.cfg,
            exchanges=["BINANCE", "KUCOIN", "BITGET"],
            route_names=["BINANCE_KUCOIN", "BINANCE_BITGET"],
            active_symbols=["XRPUSDT", "BTCUSDT"],
            state_file=None
        )

    def test_oracle_exclusion_and_capacity(self):
        """Гарантия отсутствия взаимной блокировки (Oracle Deadlock)."""
        self.assertTrue(self.pm.can_enter("BINANCE", "KUCOIN", "XRPUSDT"))
        self.pm.lock_for_entry("BINANCE", "KUCOIN", "XRPUSDT", {})
        self.pm.confirm_entry("BINANCE", "KUCOIN", "XRPUSDT", {"side": "LONG", "entry_price": 1.0, "qty": 10.0}, 100.0)

        # Binance (Oracle) не инкрементируется
        self.assertEqual(self.pm.exchange_state["BINANCE"]["current"], 0)
        self.assertEqual(self.pm.exchange_state["BINANCE"]["pending"], 0)
        # KuCoin (Target) занят (1/1)
        self.assertEqual(self.pm.exchange_state["KUCOIN"]["current"], 1)

        # Маршрут BINANCE_BITGET ОБЯЗАН быть доступен для BTCUSDT!
        self.assertTrue(self.pm.can_enter("BINANCE", "BITGET", "BTCUSDT"))

        # Повторный вход на KuCoin заблокирован
        self.assertFalse(self.pm.can_enter("BINANCE", "KUCOIN", "BTCUSDT"))

    def test_rollback_entry(self):
        """Проверка отката при ошибке входа."""
        self.pm.lock_for_entry("BINANCE", "BITGET", "XRPUSDT", {})
        self.assertEqual(self.pm.exchange_state["BITGET"]["pending"], 1)
        self.pm.rollback_entry("BINANCE", "BITGET", "XRPUSDT")
        self.assertEqual(self.pm.exchange_state["BITGET"]["pending"], 0)
        self.assertTrue(self.pm.can_enter("BINANCE", "BITGET", "XRPUSDT"))

    def test_exit_cycle(self):
        """Проверка цикла закрытия и освобождения слота."""
        self.pm.lock_for_entry("BINANCE", "BITGET", "XRPUSDT", {})
        self.pm.confirm_entry("BINANCE", "BITGET", "XRPUSDT", {"side": "LONG", "entry_price": 1.0, "qty": 10.0}, 100.0)
        self.assertEqual(self.pm.exchange_state["BITGET"]["current"], 1)

        self.pm.lock_for_exit("BINANCE_BITGET", "XRPUSDT")
        self.pm.confirm_exit("BINANCE_BITGET", "XRPUSDT")
        self.assertEqual(self.pm.exchange_state["BITGET"]["current"], 0)
        self.assertTrue(self.pm.can_enter("BINANCE", "BITGET", "XRPUSDT"))


class TestPipelinePositionFSM(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cfg = get_base_cfg()
        self.pm = PositionManager(
            self.cfg, ["BINANCE", "KUCOIN", "BITGET"], ["BINANCE_KUCOIN"], ["XRPUSDT"], state_file=None
        )

    def _make_mock_orders(self, fill_size=10.0, fill_price=1.35):
        mock_adapter = MagicMock()
        mock_adapter.place_order = AsyncMock(return_value={"code": "200000", "orderId": "123"})
        mock_adapter.cancel_all_orders = AsyncMock()
        mock_adapter.get_executed_position = MagicMock(return_value={"size": fill_size, "price": fill_price})
        mock_adapter.get_exact_position_guarded = AsyncMock(return_value={"size": fill_size, "price": fill_price, "status": "ok"})
        mock_adapter.get_last_close_price = MagicMock(return_value=fill_price)
        mock_adapter.subscribe_position_update = MagicMock(return_value=asyncio.Event())
        mock_adapter.unsubscribe_position_update = MagicMock()
        return {"KUCOIN": mock_adapter, "BINANCE": mock_adapter}

    async def test_fsm_successful_lifecycle(self):
        """Полный успешный жизненный цикл FSM: Вход -> Active -> Выход -> Settled."""
        orders = self._make_mock_orders(fill_size=10.0, fill_price=1.35)
        engine_res = {"side": "LONG", "entry_price": 1.35, "qty": 10.0, "net_spread": 0.008}

        settled_data = []
        def on_settle(sym, route, target_ex, oracle_ex, side, entry_price, exit_price, actual_usd, reason):
            settled_data.append({"sym": sym, "reason": reason, "exit_price": exit_price})

        fsm = PositionFSM(
            sym="XRPUSDT", route="BINANCE_KUCOIN", target_ex="KUCOIN", oracle_ex="BINANCE",
            side="LONG", engine_res=engine_res, cfg=self.cfg, orders=orders,
            pm=self.pm, on_settle_cb=on_settle
        )

        self.pm.lock_for_entry("BINANCE", "KUCOIN", "XRPUSDT", engine_res)
        ok_open = await fsm.run_open()
        self.assertTrue(ok_open)
        self.assertEqual(fsm.state, PositionState.ACTIVE)
        self.assertEqual(self.pm.exchange_state["KUCOIN"]["current"], 1)

        orders["KUCOIN"].get_executed_position.side_effect = [
            {"size": 10.0, "price": 1.35},  # при старте run_close
            {"size": 0.0, "price": 0.0}     # при ожидании подтверждения закрытия
        ]
        orders["KUCOIN"].get_exact_position_guarded.return_value = {"size": 0.0, "price": 0.0, "status": "ok"}

        self.pm.lock_for_exit("BINANCE_KUCOIN", "XRPUSDT")
        ok_close = await fsm.run_close({"exit_price": 1.365}, reason="TAKE_PROFIT")
        self.assertTrue(ok_close)
        self.assertEqual(fsm.state, PositionState.SETTLED)
        self.assertEqual(self.pm.exchange_state["KUCOIN"]["current"], 0)
        self.assertEqual(len(settled_data), 1)
        self.assertEqual(settled_data[0]["reason"], "TAKE_PROFIT")

    async def test_fsm_zero_fill_quarantine(self):
        """Ветка Zero-Fill: нулевое исполнение отправляет монету в карантин."""
        orders = self._make_mock_orders(fill_size=0.0, fill_price=0.0)
        engine_res = {"side": "LONG", "entry_price": 1.35, "qty": 10.0, "net_spread": 0.008}

        banned = []
        fsm = PositionFSM(
            sym="XRPUSDT", route="BINANCE_KUCOIN", target_ex="KUCOIN", oracle_ex="BINANCE",
            side="LONG", engine_res=engine_res, cfg=self.cfg, orders=orders,
            pm=self.pm, ban_coin_cb=lambda s, reason, duration_sec: banned.append((s, reason, duration_sec))
        )

        self.pm.lock_for_entry("BINANCE", "KUCOIN", "XRPUSDT", engine_res)
        ok_open = await fsm.run_open()
        self.assertFalse(ok_open)
        self.assertEqual(fsm.state, PositionState.ABORTED)
        self.assertEqual(len(banned), 1)
        self.assertIn("Zero Fill", banned[0][1])

    async def test_fsm_insufficient_margin(self):
        """Ветка Insufficient Margin: корректный откат без бана монеты."""
        orders = self._make_mock_orders()
        orders["KUCOIN"].place_order.side_effect = InsufficientMarginError("Balance too low")
        engine_res = {"side": "LONG", "entry_price": 1.35, "qty": 10.0, "net_spread": 0.008}

        banned = []
        fsm = PositionFSM(
            sym="XRPUSDT", route="BINANCE_KUCOIN", target_ex="KUCOIN", oracle_ex="BINANCE",
            side="LONG", engine_res=engine_res, cfg=self.cfg, orders=orders,
            pm=self.pm, ban_coin_cb=lambda s, reason, duration_sec: banned.append((s, reason))
        )

        self.pm.lock_for_entry("BINANCE", "KUCOIN", "XRPUSDT", engine_res)
        ok_open = await fsm.run_open()
        self.assertFalse(ok_open)
        self.assertEqual(fsm.state, PositionState.ABORTED)
        self.assertEqual(len(banned), 0)

    async def test_fsm_flat_position_instant_close(self):
        """Ветка уже закрытой позы (Flat Position): мгновенное завершение без ошибок."""
        orders = self._make_mock_orders(fill_size=0.0, fill_price=0.0)
        fsm = PositionFSM(
            sym="XRPUSDT", route="BINANCE_KUCOIN", target_ex="KUCOIN", oracle_ex="BINANCE",
            side="LONG", engine_res={}, cfg=self.cfg, orders=orders, pm=self.pm
        )
        fsm.state = PositionState.ACTIVE
        self.pm.positions["BINANCE_KUCOIN"]["XRPUSDT"]["current_position"] = True
        self.pm.positions["BINANCE_KUCOIN"]["XRPUSDT"]["pending_action"] = "CLOSE"

        ok = await fsm.run_close({}, reason="TAKE_PROFIT")
        self.assertTrue(ok)
        self.assertEqual(fsm.state, PositionState.SETTLED)


class TestPipelineIPCSocket(unittest.IsolatedAsyncioTestCase):
    async def test_roundtrip_ipc_framing(self):
        """Проверка надежности сериализации и упаковки сообщений IPC."""
        writer = MagicMock()
        written_chunks = []
        writer.write = MagicMock(side_effect=lambda b: written_chunks.append(b))
        writer.drain = AsyncMock()

        payload = {
            "route": "BINANCE_KUCOIN",
            "sym": "XRPUSDT",
            "exec_res": {"side": "LONG", "entry_price": 1.348, "qty": 18.5}
        }
        await async_write_msg(writer, "POS_OPENED", payload)

        full_buf = b"".join(written_chunks)
        import struct
        header_len = struct.unpack("!I", full_buf[:4])[0]
        data_chunk = full_buf[4:4+header_len]

        reader = AsyncMock()
        reader.readexactly.side_effect = [full_buf[:4], data_chunk]

        msg_type, unpacked_payload = await async_read_msg(reader)
        self.assertEqual(msg_type, "POS_OPENED")
        self.assertEqual(unpacked_payload["sym"], "XRPUSDT")
        self.assertEqual(unpacked_payload["exec_res"]["entry_price"], 1.348)


class TestPipelineMathCore(unittest.TestCase):
    def test_round_by_step_flooring(self):
        """Проверка квантования шагов цены и лота с защитой от переполнения."""
        res = round_by_step(1.29999, "0.1", rounding=ROUND_FLOOR)
        self.assertEqual(res, "1.2")

        res_price = round_by_step(1.34858, "0.0001", rounding=ROUND_FLOOR)
        self.assertEqual(res_price, "1.3485")


class TestDiscoveryWhitelist(unittest.IsolatedAsyncioTestCase):
    async def test_whitelist_filtering(self):
        """Проверка работы SYMBOLS_GLOBAL_WHITELIST (базовые тикеры и суффиксы USDT)."""
        from API.discovery import DiscoveryManager
        from unittest.mock import AsyncMock
        
        dm = DiscoveryManager(quote="USDT", whitelist=["BTC", "ETHUSDT"])
        for ex in dm.apis:
            dm.apis[ex].get_volumes = AsyncMock(return_value={"BTC": 10000000.0, "ETH": 10000000.0, "SOL": 10000000.0})
            
        await dm.build_topology()
        self.assertIn("BTC", dm.active_pairs_map)
        self.assertIn("ETH", dm.active_pairs_map)
        self.assertNotIn("SOL", dm.active_pairs_map)
        
        # Verify ws_routes only contain whitelisted symbols
        for ex, routes in dm.ws_routes.items():
            for r in routes:
                self.assertTrue("BTC" in r or "ETH" in r or "XBT" in r)
                self.assertFalse("SOL" in r)
                
        await dm.aclose()


if __name__ == "__main__":
    unittest.main()
