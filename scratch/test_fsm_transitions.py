# ============================================================
# FILE: scratch/test_fsm_transitions.py
# ROLE: Тестирование переходов состояний PositionFSM и защиты от сбоев
# ============================================================
import asyncio
import sys
import os

# Добавляем родительскую папку в sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from CORE.position_fsm import PositionFSM, PositionState


class MockOrder:
    def __init__(self, name: str, fill_size: float = 100.0, fail_rest: bool = False):
        self.name = name
        self.fill_size = fill_size
        self.fail_rest = fail_rest
        self.orders_placed = []
        self.cancelled = False

    def check_order_size(self, symbol, size_usd, price):
        pass

    async def place_order(self, symbol, side, size_usd, price, order_type="LIMIT", position_side=None, time_in_force=None):
        self.orders_placed.append({"side": side, "size_usd": size_usd, "order_type": order_type})
        return {"code": "00000", "msg": "ok"}

    async def cancel_all_orders(self, symbol):
        self.cancelled = True

    def get_executed_position(self, symbol, side):
        return {"size": self.fill_size, "price": 1.0}

    async def get_exact_position_guarded(self, symbol, side, max_retries=3, retry_delay=0.001):
        if self.fail_rest:
            # Имитация моргания REST: возврат WS страховки
            return {"size": self.fill_size, "price": 1.0, "status": "fallback_ws"}
        return {"size": self.fill_size, "price": 1.0, "status": "ok"}


class MockPM:
    def __init__(self):
        self.confirmed_entries = []
        self.confirmed_exits = []
        self.locked_exits = []

    def confirm_entry(self, long_ex, short_ex, sym, exec_res, open_time):
        self.confirmed_entries.append((sym, long_ex, short_ex))

    def lock_for_exit(self, route, sym):
        self.locked_exits.append((route, sym))

    def confirm_exit(self, route, sym):
        self.confirmed_exits.append((route, sym))


async def test_successful_hedged_entry():
    print("--- TEST 1: Полный налив (100% / 100%) -> ACTIVE_HEDGED ---")
    cfg = {
        "trading_rules": {"entry": {"order_execution_type": "LIMIT_GTC", "min_fill_rate": 0.5}},
        "trading_risks": {"binance": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}, "kucoin": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}},
        "EXECUTION_PAUSE": 0.005
    }
    orders = {
        "BINANCE": MockOrder("BINANCE", fill_size=100.0),
        "KUCOIN": MockOrder("KUCOIN", fill_size=100.0)
    }
    engine_res = {"vwap_spread": 0.01, "long_avg_price": 1.0, "short_avg_price": 1.01, "long_qty": 100.0, "short_qty": 100.0}
    pm = MockPM()

    fsm = PositionFSM(
        sym="TEST", route="BINANCE_KUCOIN", long_ex="BINANCE", short_ex="KUCOIN",
        engine_res=engine_res, cfg=cfg, orders=orders, coin_to_native={},
        pm=pm, writer=None, ban_coin_cb=lambda *args, **kw: None
    )

    success = await fsm.run_open()
    await asyncio.sleep(0.01)
    assert success is True, "Ожидался успешный вход"
    assert fsm.state == PositionState.ACTIVE_HEDGED, f"Ожидался ACTIVE_HEDGED, получен {fsm.state}"
    assert len(pm.confirmed_entries) == 1, "confirm_entry не был вызван"
    assert orders["BINANCE"].cancelled is True, "cancel_all_orders не был вызван для GTC"
    print("✅ TEST 1 PASSED: FSM корректно перешел в ACTIVE_HEDGED\n")


async def test_aborted_zero_fill():
    print("--- TEST 2: Нулевой налив (0% / 0%) -> ABORTED ---")
    cfg = {
        "trading_rules": {"entry": {"order_execution_type": "LIMIT_GTC", "min_fill_rate": 0.5}},
        "trading_risks": {"binance": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}, "kucoin": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}},
        "EXECUTION_PAUSE": 0.005
    }
    orders = {
        "BINANCE": MockOrder("BINANCE", fill_size=0.0),
        "KUCOIN": MockOrder("KUCOIN", fill_size=0.0)
    }
    engine_res = {"vwap_spread": 0.01, "long_avg_price": 1.0, "short_avg_price": 1.01, "long_qty": 100.0, "short_qty": 100.0}
    pm = MockPM()

    fsm = PositionFSM(
        sym="TEST", route="BINANCE_KUCOIN", long_ex="BINANCE", short_ex="KUCOIN",
        engine_res=engine_res, cfg=cfg, orders=orders, coin_to_native={},
        pm=pm, writer=None, ban_coin_cb=lambda *args, **kw: None
    )

    success = await fsm.run_open()
    assert success is False, "Ожидался неуспешный вход"
    assert fsm.state == PositionState.ABORTED, f"Ожидался ABORTED, получен {fsm.state}"
    assert len(pm.confirmed_entries) == 0, "confirm_entry не должен вызываться при 0.0"
    print("✅ TEST 2 PASSED: FSM корректно перешел в ABORTED без лишних действий\n")


async def test_emergency_unwind_desync():
    print("--- TEST 3: Рассинхрон ног (L:100%, S:0%) -> EMERGENCY_UNWIND -> SETTLED ---")
    cfg = {
        "trading_rules": {"entry": {"order_execution_type": "LIMIT_GTC", "min_fill_rate": 0.5}},
        "trading_risks": {"binance": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}, "kucoin": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}},
        "EXECUTION_PAUSE": 0.005
    }
    binance_order = MockOrder("BINANCE", fill_size=100.0)
    kucoin_order = MockOrder("KUCOIN", fill_size=0.0)
    orders = {"BINANCE": binance_order, "KUCOIN": kucoin_order}
    engine_res = {"vwap_spread": 0.01, "long_avg_price": 1.0, "short_avg_price": 1.01, "long_qty": 100.0, "short_qty": 100.0}
    pm = MockPM()

    settled_calls = []
    async def mock_settle(*args):
        settled_calls.append(args)

    fsm = PositionFSM(
        sym="TEST", route="BINANCE_KUCOIN", long_ex="BINANCE", short_ex="KUCOIN",
        engine_res=engine_res, cfg=cfg, orders=orders, coin_to_native={},
        pm=pm, writer=None, ban_coin_cb=lambda *args, **kw: None, on_settle_cb=mock_settle
    )

    # При закрытии (SELL) имитируем, что сброс обнулил ногу
    orig_place = binance_order.place_order
    async def mock_close_place(*args, **kwargs):
        side = kwargs.get("side") or (args[1] if len(args) > 1 else "BUY")
        if side == "SELL":
            binance_order.fill_size = 0.0
        return await orig_place(*args, **kwargs)
    binance_order.place_order = mock_close_place

    success = await fsm.run_open()
    assert success is False, "Ожидался неуспешный вход из-за рассинхрона"
    assert fsm.state == PositionState.SETTLED, f"Ожидался SETTLED после unwind, получен {fsm.state}"
    assert len(pm.confirmed_exits) == 1, "confirm_exit должен быть вызван"
    print("✅ TEST 3 PASSED: FSM обнаружил рассинхрон и гарантированно сбросил ногу в ноль\n")


async def test_rest_glitch_protection():
    print("--- TEST 4: Моргание REST (network glitch) -> WS Guard Fallback ---")
    cfg = {
        "trading_rules": {"entry": {"order_execution_type": "LIMIT_GTC", "min_fill_rate": 0.5}},
        "trading_risks": {"binance": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}, "kucoin": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}},
        "EXECUTION_PAUSE": 0.005
    }
    # Binance с упавшим REST
    binance_order = MockOrder("BINANCE", fill_size=100.0, fail_rest=True)
    kucoin_order = MockOrder("KUCOIN", fill_size=100.0, fail_rest=False)
    orders = {"BINANCE": binance_order, "KUCOIN": kucoin_order}
    engine_res = {"vwap_spread": 0.01, "long_avg_price": 1.0, "short_avg_price": 1.01, "long_qty": 100.0, "short_qty": 100.0}
    pm = MockPM()

    fsm = PositionFSM(
        sym="TEST", route="BINANCE_KUCOIN", long_ex="BINANCE", short_ex="KUCOIN",
        engine_res=engine_res, cfg=cfg, orders=orders, coin_to_native={},
        pm=pm, writer=None, ban_coin_cb=lambda *args, **kw: None
    )

    success = await fsm.run_open()
    assert success is True, "Ожидался успешный вход благодаря WS fallback"
    assert fsm.state == PositionState.ACTIVE_HEDGED, f"Ожидался ACTIVE_HEDGED, получен {fsm.state}"
    print("✅ TEST 4 PASSED: Защита от моргания REST сработала, позиция не была потеряна\n")


async def test_normal_close():
    print("--- TEST 5: Плановое закрытие (run_close) -> SETTLED ---")
    cfg = {
        "trading_rules": {"entry": {"order_execution_type": "LIMIT_GTC", "min_fill_rate": 0.5}},
        "trading_risks": {"binance": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}, "kucoin": {"limit_allow_distance": 1.002, "trade_size_usd": 50.0}},
        "EXECUTION_PAUSE": 0.005
    }
    binance_order = MockOrder("BINANCE", fill_size=100.0)
    kucoin_order = MockOrder("KUCOIN", fill_size=100.0)
    orders = {"BINANCE": binance_order, "KUCOIN": kucoin_order}
    engine_res = {"vwap_spread": 0.01, "long_avg_price": 1.0, "short_avg_price": 1.01, "long_qty": 100.0, "short_qty": 100.0}
    pm = MockPM()

    settled_calls = []
    async def mock_settle(*args):
        settled_calls.append(args)

    fsm = PositionFSM(
        sym="TEST", route="BINANCE_KUCOIN", long_ex="BINANCE", short_ex="KUCOIN",
        engine_res=engine_res, cfg=cfg, orders=orders, coin_to_native={},
        pm=pm, writer=None, ban_coin_cb=lambda *args, **kw: None, on_settle_cb=mock_settle
    )

    # 1. Открытие
    await fsm.run_open()
    assert fsm.state == PositionState.ACTIVE_HEDGED

    # При закрытии имитируем, что ордера обнулили позы
    async def mock_close_orders(*args, **kwargs):
        binance_order.fill_size = 0.0
        kucoin_order.fill_size = 0.0
        return {"code": "00000", "msg": "ok"}
    binance_order.place_order = mock_close_orders
    kucoin_order.place_order = mock_close_orders

    # 2. Закрытие
    exit_res = {"long_close_price": 1.005, "short_close_price": 1.003, "reason": "PROFIT_DECAY"}
    close_success = await fsm.run_close(exit_res, reason="PROFIT_DECAY")
    assert close_success is True
    assert fsm.state == PositionState.SETTLED
    assert len(pm.confirmed_exits) == 1
    print("✅ TEST 5 PASSED: Плановое закрытие run_close успешно завершено с подтвержденным 0.0\n")


async def main():
    print("============================================================")
    print("ЗАПУСК ТЕСТОВ СТЕЙТ-МАШИНЫ (FSM) И ЗАЩИТЫ ОТ СБОЕВ")
    print("============================================================\n")
    await test_successful_hedged_entry()
    await test_aborted_zero_fill()
    await test_emergency_unwind_desync()
    await test_rest_glitch_protection()
    await test_normal_close()
    print("============================================================")
    print("🎉 ВСЕ ТЕСТЫ FSM ПРОЙДЕНЫ УСПЕШНО!")
    print("============================================================")


if __name__ == "__main__":
    asyncio.run(main())
