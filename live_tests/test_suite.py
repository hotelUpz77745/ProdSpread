# ============================================================
# FILE: test_suite.py
# ROLE: Комплексный модульный тест всех компонентов ProdSpread:
#       - Квантование и точность цен/лотов (round_by_step)
#       - Генерация ордеров BinanceOrder (GTC, параметры, подпись)
#       - Генерация ордеров KucoinOrder (GTC, multiplier, лоты, подпись)
#       - Жизненный цикл PositionManager (lock, confirm, rollback, лимиты)
#       - Логика аварийного выхода при низком fill_rate (lock_for_exit)
#       - Каскад фоллбэков цен закрытия в execute_close
# ============================================================

import sys
import os
import time
import json
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Мокируем отсутствующие в минимальном окружении библиотеки
class DummyPytz:
    @staticmethod
    def timezone(name):
        return timezone.utc if 'timezone' in globals() else None

class DummyAiohttp:
    class ClientSession:
        pass
    class WSMsgType:
        TEXT = 1
        CLOSED = 2
        ERROR = 3

sys.modules.setdefault('aiohttp', DummyAiohttp())
sys.modules.setdefault('dotenv', MagicMock())
sys.modules.setdefault('pytz', DummyPytz())
sys.modules.setdefault('pandas', MagicMock())
sys.modules.setdefault('matplotlib', MagicMock())
sys.modules.setdefault('matplotlib.pyplot', MagicMock())
sys.modules.setdefault('numpy', MagicMock())
sys.modules.setdefault('numba', MagicMock())

from consts import load_config
from API.orders import round_by_step, BinanceOrder, KucoinOrder
from CORE.position_manager import PositionManager
from CORE.executor_process import ExecutorProcess

passed_count = 0
failed_count = 0

def run_test(name, func):
    global passed_count, failed_count
    try:
        if asyncio.iscoroutinefunction(func):
            asyncio.run(func())
        else:
            func()
        print(f"  [PASS] {name}")
        passed_count += 1
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        import traceback
        traceback.print_exc()
        failed_count += 1

# ==========================================
# 1. ТЕСТЫ КВАНТОВАНИЯ (round_by_step)
# ==========================================
def test_quantization_various_steps():
    # Обычные шаги
    assert round_by_step(12.3456, "0.01") == "12.35"
    assert round_by_step(12.341, "0.01") == "12.34"
    assert round_by_step(100.4, "1") == "100"
    assert round_by_step(100.6, "1") == "101"
    
    # Мелкие шаги (MEME / щиткоины)
    assert round_by_step(0.000012345, "0.000001") == "0.000012"
    assert round_by_step(0.00001289, "0.000001") == "0.000013"
    
    # Шаг 0.5
    assert round_by_step(10.2, "0.5") == "10.0"
    assert round_by_step(10.3, "0.5") == "10.5"
    assert round_by_step(10.8, "0.5") == "11.0"

# ==========================================
# 2. ТЕСТЫ BINANCE ORDER (GTC, Параметры)
# ==========================================
async def test_binance_gtc_limit_order():
    session = AsyncMock()
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"orderId": 12345, "status": "NEW"})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=mock_resp)
    
    bo = BinanceOrder(api_key="test_key", api_secret="test_secret", session=session)
    bo.symbol_info = [{
        "symbol": "DOGEUSDT",
        "filters": [
            {"filterType": "LOT_SIZE", "stepSize": "1"},
            {"filterType": "PRICE_FILTER", "tickSize": "0.0001"}
        ]
    }]
    
    # Размещаем лимитный ордер
    await bo.place_order(
        symbol="DOGEUSDT",
        side="BUY",
        size_usd=50.0,
        price=0.20,
        order_type="LIMIT",
        position_side="LONG"
    )
    
    # Проверяем URL запроса
    assert session.post.called
    call_url = session.post.call_args[0][0]
    
    assert "type=LIMIT" in call_url
    assert "timeInForce=GTC" in call_url, "Ордер должен иметь timeInForce=GTC (не IOC)!"
    assert "side=BUY" in call_url
    assert "positionSide=LONG" in call_url
    assert "quantity=250" in call_url
    assert "price=0.2000" in call_url

# ==========================================
# 3. ТЕСТЫ KUCOIN ORDER (GTC, Multiplier, Лоты)
# ==========================================
async def test_kucoin_gtc_limit_order():
    session = AsyncMock()
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"code": "200000", "data": {"orderId": "kc_123"}})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=mock_resp)
    
    margin_settings = {"leverage": 20, "margin_type": "CROSS"}
    ko = KucoinOrder(
        api_key="kc_key",
        api_secret="kc_sec",
        api_passphrase="kc_pass",
        session=session,
        position_stream=None,
        margin_settings=margin_settings
    )
    
    # symbol multiplier = 10 (1 контракт = 10 DOGE)
    ko.symbol_info = [{
        "symbol": "DOGEUSDTM",
        "lotSize": 1,
        "tickSize": 0.0001,
        "multiplier": 10.0
    }]
    
    await ko.place_order(
        symbol="DOGEUSDTM",
        side="SELL",
        size_usd=50.0,
        price=0.20,
        order_type="LIMIT",
        position_side="SHORT"
    )
    
    assert session.post.called
    call_data = json.loads(session.post.call_args[1]["data"])
    
    assert call_data["type"] == "limit"
    assert call_data["timeInForce"] == "GTC", "Kucoin ордер должен иметь timeInForce=GTC!"
    assert call_data["side"] == "sell"
    assert call_data["positionSide"] == "SHORT"
    assert call_data["leverage"] == "20"
    assert call_data["marginMode"] == "CROSS"
    # 50 USD / 0.20 = 250 DOGE / multiplier(10) = 25 контрактов (лотов)
    assert call_data["size"] == "25"
    assert call_data["price"] == "0.2000"

# ==========================================
# 4. ТЕСТЫ POSITION MANAGER
# ==========================================
def test_position_manager_full_lifecycle():
    cfg = load_config()
    exchanges = ["BINANCE", "KUCOIN"]
    routes = ["BINANCE_KUCOIN"]
    active_symbols = ["DOGE", "XRP"]
    
    pm = PositionManager(cfg, exchanges, routes, active_symbols)
    
    # 1. Свободное открытие
    assert pm.can_enter("BINANCE", "KUCOIN", "DOGE") is True
    
    # 2. Блокировка на вход
    pm.lock_for_entry("BINANCE", "KUCOIN", "DOGE", {"long_price": 0.20, "short_price": 0.205})
    assert pm.positions["BINANCE_KUCOIN"]["DOGE"]["pending_action"] == "OPEN"
    assert pm.can_enter("BINANCE", "KUCOIN", "DOGE") is False, "Нельзя входить повторно, пока идет вход"
    
    # 3. Подтверждение входа
    exec_res = {
        "long_ex": "BINANCE",
        "short_ex": "KUCOIN",
        "actual_long_price": 0.20,
        "actual_short_price": 0.205,
        "long_executed_volume_rate": 1.0,
        "short_executed_volume_rate": 1.0
    }
    pm.confirm_entry("BINANCE", "KUCOIN", "DOGE", exec_res, time.time())
    pos = pm.positions["BINANCE_KUCOIN"]["DOGE"]
    assert pos["current_position"] is True
    assert pos["pending_action"] is None
    assert pos["details"]["entry_long_price"] == 0.20
    
    # 4. Блокировка на выход (lock_for_exit)
    pm.lock_for_exit("BINANCE_KUCOIN", "DOGE")
    assert pm.positions["BINANCE_KUCOIN"]["DOGE"]["pending_action"] == "CLOSE"
    
    # 5. Подтверждение выхода
    pm.confirm_exit("BINANCE_KUCOIN", "DOGE")
    assert pm.positions["BINANCE_KUCOIN"]["DOGE"]["current_position"] is False
    assert pm.positions["BINANCE_KUCOIN"]["DOGE"]["pending_action"] is None
    assert pm.can_enter("BINANCE", "KUCOIN", "DOGE") is True, "После выхода связка снова свободна"

# ==========================================
# 5. ТЕСТ АВАРИЙНОГО ЗАКРЫТИЯ (EXECUTE_CLOSE)
# ==========================================
async def test_emergency_execute_close_price_fallback():
    cfg = load_config()
    executor = ExecutorProcess(port=9999, cfg=cfg)
    
    # Мокируем PositionManager
    executor.pm = PositionManager(cfg, ["BINANCE", "KUCOIN"], ["BINANCE_KUCOIN"], ["DOGE"])
    
    # Настраиваем позицию с данными входа
    exec_res = {
        "long_ex": "BINANCE",
        "short_ex": "KUCOIN",
        "entry_long_price": 0.20,
        "entry_short_price": 0.205,
        "actual_long_price": 0.20,
        "actual_short_price": 0.205,
        "long_executed_volume_rate": 1.0,
        "short_executed_volume_rate": 0.0
    }
    executor.pm.confirm_entry("BINANCE", "KUCOIN", "DOGE", exec_res, time.time())
    executor.pm.lock_for_exit("BINANCE_KUCOIN", "DOGE")
    
    # Мокируем ордера
    mock_b_order = AsyncMock()
    mock_b_order.get_exact_position = AsyncMock(return_value={"size": 250.0, "price": 0.20})
    mock_b_order.place_order = AsyncMock(return_value={"orderId": 999})
    
    mock_k_order = AsyncMock()
    mock_k_order.get_exact_position = AsyncMock(return_value={"size": 0.0, "price": 0.0})
    
    executor.orders = {
        "BINANCE": mock_b_order,
        "KUCOIN": mock_k_order
    }
    executor.coin_to_native = {"DOGE": {"BINANCE": "DOGEUSDT", "KUCOIN": "DOGEUSDTM"}}
    
    # Вызываем аварийный execute_close БЕЗ exit_res (как при LOW_FILL_RATE)
    close_payload = {
        "route": "BINANCE_KUCOIN",
        "sym": "DOGE",
        "reason": "LOW_FILL_RATE",
        "data": exec_res
    }
    
    await executor.execute_close(close_payload)
    
    # Проверяем, что place_order был вызван с валидной ценой (> 0) и не упал!
    assert mock_b_order.place_order.called, "place_order должен быть вызван для закрытия открытой ноги!"
    call_args = mock_b_order.place_order.call_args
    sym, side, size_usd, price_limit = call_args[0]
    
    assert sym == "DOGEUSDT"
    assert side == "SELL"
    assert size_usd > 0
    assert price_limit > 0, f"price_limit ({price_limit}) обязан быть > 0!"
    print(f"      [Детали аварийного закрытия]: {side} {sym}, объем {size_usd:.2f}$, цена лимита {price_limit:.5f}")

# ==========================================
# 6. ТЕСТ ОБРАТНОГО НАПРАВЛЕНИЯ (LONG KUCOIN / SHORT BINANCE)
# ==========================================
async def test_reverse_direction_orders():
    session = AsyncMock()
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"orderId": 12345, "code": "200000", "data": {"orderId": "kc_rev"}})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=mock_resp)
    
    # 1. Binance SHORT
    bo = BinanceOrder(api_key="test_key", api_secret="test_secret", session=session)
    bo.symbol_info = [{"symbol": "DOGEUSDT", "filters": [{"filterType": "LOT_SIZE", "stepSize": "1"}, {"filterType": "PRICE_FILTER", "tickSize": "0.0001"}]}]
    await bo.place_order("DOGEUSDT", "SELL", 50.0, 0.20, order_type="LIMIT", position_side="SHORT")
    b_url = session.post.call_args[0][0]
    assert "side=SELL" in b_url
    assert "positionSide=SHORT" in b_url
    
    # 2. Kucoin LONG
    ko = KucoinOrder("kc_key", "kc_sec", "kc_pass", session, None, {"leverage": 20, "margin_type": "CROSS"})
    ko.symbol_info = [{"symbol": "DOGEUSDTM", "lotSize": 1, "tickSize": 0.0001, "multiplier": 10.0}]
    await ko.place_order("DOGEUSDTM", "BUY", 50.0, 0.20, order_type="LIMIT", position_side="LONG")
    k_data = json.loads(session.post.call_args[1]["data"])
    assert k_data["side"] == "buy"
    assert k_data["positionSide"] == "LONG"
    assert k_data["size"] == "25"

# ==========================================
# 7. ТЕСТ ПРЕДПОЛЕТНОЙ ПРОВЕРКИ (CHECK_ORDER_SIZE)
# ==========================================
def test_check_order_size_validation():
    bo = BinanceOrder(api_key="k", api_secret="s", session=None)
    bo.symbol_info = [{
        "symbol": "DOGEUSDT",
        "filters": [
            {"filterType": "LOT_SIZE", "minQty": "10", "stepSize": "10"}
        ]
    }]
    # Слишком маленький размер (0.5$ при цене 1.0 и шаге 10 -> округление в 0 лотов)
    try:
        bo.check_order_size("DOGEUSDT", 0.5, 1.0)
        assert False, "Должна вылететь ошибка размера ордера (округление в 0)"
    except ValueError as e:
        assert "Calculated order size is 0" in str(e)
        
    # Валидный размер (50$)
    bo.check_order_size("DOGEUSDT", 50.0, 0.20)

# ==========================================
# 8. ТЕСТ ПОЛНОГО СЦЕНАРИЯ EXECUTE_OPEN С НИЗКИМ FILL_RATE (РАССИНХРОН)
# ==========================================
async def test_execute_open_low_fill_rate_recovery():
    cfg = load_config()
    executor = ExecutorProcess(port=9999, cfg=cfg)
    executor.pm = PositionManager(cfg, ["BINANCE", "KUCOIN"], ["BINANCE_KUCOIN"], ["DOGE"])
    
    # Мокируем ордера: Binance налился 100%, Kucoin 0%
    mock_b = AsyncMock()
    mock_b.check_order_size = MagicMock()
    mock_b.place_order = AsyncMock(return_value={"orderId": 111})
    mock_b.get_executed_position = MagicMock(return_value={"size": 250.0, "price": 0.20})
    mock_b.get_exact_position = AsyncMock(return_value={"size": 250.0, "price": 0.20})
    
    mock_k = AsyncMock()
    mock_k.check_order_size = MagicMock()
    mock_k.place_order = AsyncMock(return_value={"orderId": 222})
    mock_k.get_executed_position = MagicMock(return_value={"size": 0.0, "price": 0.0})
    mock_k.get_exact_position = AsyncMock(return_value={"size": 0.0, "price": 0.0})
    
    executor.orders = {"BINANCE": mock_b, "KUCOIN": mock_k}
    executor.coin_to_native = {"DOGE": {"BINANCE": "DOGEUSDT", "KUCOIN": "DOGEUSDTM"}}
    
    # Вход
    executor.pm.lock_for_entry("BINANCE", "KUCOIN", "DOGE", {"long_price": 0.20, "short_price": 0.205})
    open_payload = {
        "sym": "DOGE",
        "route": "BINANCE_KUCOIN",
        "long_ex": "BINANCE",
        "short_ex": "KUCOIN",
        "engine_res": {
            "long_price": 0.20,
            "short_price": 0.205,
            "long_avg_price": 0.20,
            "short_avg_price": 0.205,
            "long_qty": 250.0,
            "short_qty": 250.0,
            "vwap_spread": 0.015
        }
    }
    
    # Запускаем execute_open: Kucoin 0% -> должен сработать экстренный выход
    await executor.execute_open(open_payload)
    
    # Проверяем, что был вызван place_order на закрытие зависшей ноги Binance!
    assert mock_b.place_order.call_count >= 2, "Должен быть вызван и ордер входа, и ордер закрытия!"
    close_call = mock_b.place_order.call_args_list[-1]
    assert close_call[0][1] == "SELL", "Должен быть SELL ордер для закрытия лонга"
    print("      [Экстренный выход при S=0%]: Успешно сброшена нога Binance без ошибок!")

# ==========================================
# ЗАПУСК ВСЕХ ТЕСТОВ
# ==========================================
if __name__ == "__main__":
    print("\n==================================================")
    print("🚀 ЗАПУСК ТЕСТОВОГО НАБОРА ProdSpread")
    print("==================================================")
    
    print("\n[1/8] Тестирование квантования (round_by_step)...")
    run_test("Точность шагов цены и лота", test_quantization_various_steps)
    
    print("\n[2/8] Тестирование BinanceOrder (GTC лимитки)...")
    run_test("BinanceOrder GTC timeInForce и параметры", test_binance_gtc_limit_order)
    
    print("\n[3/8] Тестирование KucoinOrder (GTC лимитки и multiplier)...")
    run_test("KucoinOrder GTC timeInForce и расчет лотов", test_kucoin_gtc_limit_order)
    
    print("\n[4/8] Тестирование обратного направления (LONG Kucoin / SHORT Binance)...")
    run_test("Параметры обратных ордеров (позиции и стороны)", test_reverse_direction_orders)
    
    print("\n[5/8] Тестирование предполетной проверки ордеров...")
    run_test("Валидация minNotional и minQty в check_order_size", test_check_order_size_validation)
    
    print("\n[6/8] Тестирование PositionManager...")
    run_test("Полный цикл PositionManager (вход, lock_for_exit, выход)", test_position_manager_full_lifecycle)
    
    print("\n[7/8] Тестирование аварийного сброса execute_close...")
    run_test("Фоллбэк цены в execute_close при аварийном выходе", test_emergency_execute_close_price_fallback)
    
    print("\n[8/8] Тестирование полного сценария execute_open при рассинхроне...")
    run_test("Автоматический сброс ноги при 0% на второй бирже", test_execute_open_low_fill_rate_recovery)
    
    print("\n==================================================")
    print(f"ИТОГИ: Пройдено: {passed_count} | Ошибок: {failed_count}")
    print("==================================================")
    
    if failed_count > 0:
        sys.exit(1)
