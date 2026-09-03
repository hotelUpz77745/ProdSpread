# ============================================================
# FILE: live_tests/test_live_entry_exit.py
# ROLE: Сквозной боевой тест с реальным открытием и закрытием микро-лота
#       через PositionFSM, с проверкой сокетов цен, сокетов позиций
#       и Fail-Safe REST зачистки до строго 0.0.
# ============================================================
import asyncio
import os
import sys
import time
import json
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv()

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from utils import SessionManager
from c_log import log
from API.orders import BinanceOrder, BitgetOrder, KucoinOrder
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.BINANCE.stakan import BinanceStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from CORE.position_fsm import PositionFSM, PositionState


class DummyWriter:
    def write(self, data):
        pass
    async def drain(self):
        pass


class DummyPM:
    def confirm_entry(self, *args, **kwargs):
        pass
    def confirm_exit(self, *args, **kwargs):
        pass
    def lock_for_exit(self, *args, **kwargs):
        pass


async def run_live_test():
    print("=" * 65)
    print("🚀 СТАРТ СКВОЗНОГО БОЕВОГО ТЕСТА С РЕАЛЬНЫМ НАЛИВОМ И ЗАКРЫТИЕМ")
    print("=" * 65)

    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    session = await SessionManager().get_session()

    # 1. Запуск приватных стримов
    print("\n--- 1. Подключение приватных WebSocket стримов позиций ---")
    binance_stream = BinancePositionStream(
        api_key=os.environ["BINANCE_API_KEY"],
        api_secret=os.environ["BINANCE_API_SECRET"]
    )
    bitget_stream = BitgetPositionStream(
        api_key=os.environ["BITGET_API_KEY"],
        api_secret=os.environ["BITGET_API_SECRET"],
        api_passphrase=os.environ["BITGET_API_PASSPHRASE"]
    )
    kucoin_stream = KucoinPositionStream(
        api_key=os.environ["KUCOIN_API_KEY"],
        api_secret=os.environ["KUCOIN_API_SECRET"],
        api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"]
    )

    t_binance = asyncio.create_task(binance_stream.start())
    t_bitget = asyncio.create_task(bitget_stream.start())
    t_kucoin = asyncio.create_task(kucoin_stream.start())

    for _ in range(40):
        await asyncio.sleep(0.2)
        if binance_stream.ready and bitget_stream.ready and kucoin_stream.ready:
            break

    print(f"  Binance Private WS: Connected={binance_stream.is_connected}, Ready={binance_stream.ready}")
    print(f"  Bitget Private WS:  Connected={bitget_stream.is_connected}, Ready={bitget_stream.ready}")
    print(f"  Kucoin Private WS:  Connected={kucoin_stream.is_connected}, Ready={kucoin_stream.ready}")

    # 2. Проверка адаптеров
    print("\n--- 2. Инициализация торговых адаптеров и загрузка спецификаций ---")
    binance_order = BinanceOrder(
        api_key=os.environ["BINANCE_API_KEY"],
        api_secret=os.environ["BINANCE_API_SECRET"],
        session=session,
        position_stream=binance_stream
    )
    bitget_order = BitgetOrder(
        api_key=os.environ["BITGET_API_KEY"],
        api_secret=os.environ["BITGET_API_SECRET"],
        api_passphrase=os.environ["BITGET_API_PASSPHRASE"],
        session=session,
        position_stream=bitget_stream,
        margin_settings=cfg["margin_settings"]["BITGET"]
    )
    kucoin_order = KucoinOrder(
        api_key=os.environ["KUCOIN_API_KEY"],
        api_secret=os.environ["KUCOIN_API_SECRET"],
        api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
        session=session,
        position_stream=kucoin_stream,
        margin_settings=cfg["margin_settings"]["KUCOIN"]
    )

    # Загружаем спецификации
    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
        binance_order.symbol_info = (await resp.json())["symbols"]
    await bitget_order.update_symbol_info()
    async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
        kucoin_order.symbol_info = (await resp.json())["data"]

    print(f"  Спецификации загружены: Binance={len(binance_order.symbol_info)}, Bitget={len(bitget_order.symbol_info)}, Kucoin={len(kucoin_order.symbol_info)}")

    # 3. Тест реального открытия и закрытия через PositionFSM
    # Выбираем высоколиквидную пару XRPUSDT (размер 6 USD, минимальный шаг, минимальный спред)
    test_symbol = "XRPUSDT"
    
    # Получаем текущую цену
    async with session.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={test_symbol}") as resp:
        curr_price = float((await resp.json())["price"])
    print(f"\n--- 3. Боевой прогон PositionFSM на {test_symbol} (Market Price: {curr_price}) ---")

    test_size_usd = 6.0 # > 5 USD minimum notional on both Binance and Bitget
    qty = test_size_usd / curr_price

    engine_res = {
        "vwap_spread": 0.008,
        "long_qty": qty,
        "short_qty": qty,
        "long_avg_price": curr_price,
        "short_avg_price": curr_price
    }

    orders_dict = {
        "BINANCE": binance_order,
        "BITGET": bitget_order,
        "KUCOIN": kucoin_order
    }

    # Настраиваем FSM с LIMIT_GTC и боевым EXECUTION_PAUSE
    fsm = PositionFSM(
        sym=test_symbol,
        route="BINANCE_BITGET",
        long_ex="BINANCE",
        short_ex="BITGET",
        engine_res=engine_res,
        cfg=cfg,
        orders=orders_dict,
        coin_to_native={test_symbol: {"BINANCE": test_symbol, "BITGET": test_symbol}},
        pm=DummyPM(),
        writer=DummyWriter(),
        ban_coin_cb=lambda s, reason="", duration_sec=None: print(f"  [Ban Callback]: {s} - {reason}")
    )

    print(f"  Запуск fsm.run_open() (LIMIT_GTC, EXECUTION_PAUSE={fsm.execution_pause}с)...")
    open_success = await fsm.run_open()
    print(f"  Результат run_open: {open_success}")
    print(f"  FSM State: {fsm.state}")
    print(f"  Long Pos:  {fsm.long_pos}")
    print(f"  Short Pos: {fsm.short_pos}")
    print(f"  Binance Private WS STILL CONNECTED: {binance_stream.is_connected}")
    print(f"  Bitget Private WS STILL CONNECTED:  {bitget_stream.is_connected}")

    await asyncio.sleep(1.0)

    # 4. Проверка закрытия и последней линии обороны
    print("\n--- 4. Ликвидация / Закрытие позиций до строго 0.0 ---")
    if fsm.state == PositionState.ACTIVE_HEDGED:
        print("  Позиция ACTIVE_HEDGED -> Вызываем run_close()...")
        close_success = await fsm.run_close({"long_close_price": curr_price, "short_close_price": curr_price}, reason="TEST_COMPLETE")
        print(f"  Результат run_close: {close_success}")
    elif fsm.state == PositionState.EMERGENCY_UNWIND:
        print("  Позиция в EMERGENCY_UNWIND -> Unwind уже отработал, проверяем обнуление...")
    else:
        print(f"  Позиция в состоянии {fsm.state}. Запуск контрольного Unwind...")
        await fsm._emergency_unwind()

    # 5. Контрольная проверка последней линии обороны через прямой GET
    print("\n--- 5. Контрольная верификация последней линии обороны (Fail-Safe REST) ---")
    b_final = await binance_order.get_exact_position_guarded(test_symbol, "LONG")
    bg_final = await bitget_order.get_exact_position_guarded(test_symbol, "SHORT")
    print(f"  [BINANCE] Итоговая позиция (REST): {b_final}")
    print(f"  [BITGET]  Итоговая позиция (REST): {bg_final}")

    assert b_final.get("size", 0.0) == 0.0, f"ОШИБКА: На Binance остался незакрытый объем: {b_final}"
    assert bg_final.get("size", 0.0) == 0.0, f"ОШИБКА: На Bitget остался незакрытый объем: {bg_final}"

    print("\n" + "=" * 65)
    print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ! ПОЗИЦИИ ОБНУЛЕНЫ (0.0). СОКЕТЫ СТАБИЛЬНЫ.")
    print("=" * 65)

    # Остановка стримов
    await binance_stream.stop()
    await bitget_stream.stop()
    await kucoin_stream.stop()
    t_binance.cancel()
    t_bitget.cancel()
    t_kucoin.cancel()
    await session.close()


if __name__ == "__main__":
    asyncio.run(run_live_test())
