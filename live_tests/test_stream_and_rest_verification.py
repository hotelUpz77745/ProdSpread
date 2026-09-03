# ============================================================
# FILE: live_tests/test_stream_and_rest_verification.py
# ROLE: Точечный боевой тест перехвата налива сокетами vs точечные GET-запросы:
#       Этап 1: Лимитки +-7% в небо -> 2с ожидания -> парсинг сокетов -> 1с ожидания -> параллельные GET-запросы.
#       Этап 2: Маркеты на 6$ -> 2с ожидания -> парсинг сокетов -> 1с ожидания -> параллельные GET-запросы -> закрытие -> верификация 0.0.
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


async def main():
    print("=" * 70)
    print("🔬 НАЧАЛО БОЕВОЙ ПРОВЕРКИ СТРИМОВ ПОЗИЦИЙ И ТОЧЕЧНЫХ GET-ЗАПРОСОВ")
    print("=" * 70)

    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    session = await SessionManager().get_session()

    # 1. Запуск стримов
    print("\n--- [ШАГ 1] Запуск и подключение приватных WebSocket стримов ---")
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

    print(f"  Binance Stream Ready: {binance_stream.ready}")
    print(f"  Bitget Stream Ready:  {bitget_stream.ready}")
    print(f"  Kucoin Stream Ready:  {kucoin_stream.ready}")

    # 2. Инициализация адаптеров
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

    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
        binance_order.symbol_info = (await resp.json())["symbols"]
    await bitget_order.update_symbol_info()
    async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
        kucoin_order.symbol_info = (await resp.json())["data"]

    test_sym_binance = "XRPUSDT"
    test_sym_bitget = "XRPUSDT"
    test_sym_kucoin = "XRPUSDTM"

    async with session.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={test_sym_binance}") as resp:
        mkt_price = float((await resp.json())["price"])

    print(f"\nТестовый инструмент: XRPUSDT (текущая цена ~{mkt_price:.4f})")

    # ============================================================
    # ЭТАП 1: Лимитки +-7% в небо
    # ============================================================
    print("\n" + "=" * 50)
    print("▶️ ЭТАП 1: ТЕСТ С ЛИМИТКАМИ (+-7% В НЕБО, БЕЗ НАЛИВА)")
    print("=" * 50)

    p_buy_limit = round(mkt_price * 0.93, 4)
    p_sell_limit = round(mkt_price * 1.07, 4)
    test_size_usd = 16.0 # 16 USD satisfies minimum notional on Binance, Bitget, and Kucoin (1 contract = 10 XRP = ~14.6 USD)

    print(f"  Выставляем лимитки: BUY @ {p_buy_limit}, SELL @ {p_sell_limit}...")
    place_tasks = [
        binance_order.place_order(test_sym_binance, "BUY", test_size_usd, p_buy_limit, position_side="LONG", time_in_force="GTC"),
        bitget_order.place_order(test_sym_bitget, "SELL", test_size_usd, p_sell_limit, position_side="SHORT", time_in_force="GTC"),
        kucoin_order.place_order(test_sym_kucoin, "BUY", test_size_usd, p_buy_limit, position_side="LONG", time_in_force="GTC")
    ]
    place_res = await asyncio.gather(*place_tasks, return_exceptions=True)
    print(f"  Ответы бирж на выставление лимиток: {[type(r).__name__ if isinstance(r, Exception) else 'OK' for r in place_res]}")

    print("  Ожидание 2.0 секунды (реакция стакана / стримов)...")
    await asyncio.sleep(2.0)

    print("\n  [ШАГ 1.1] Парсинг данных из детерминатора позиций (ТОЛЬКО WEBSOCKET, БЕЗ GET!):")
    ws_b = binance_order.get_executed_position(test_sym_binance, "LONG")
    ws_bg = bitget_order.get_executed_position(test_sym_bitget, "SHORT")
    ws_k = kucoin_order.get_executed_position(test_sym_kucoin, "LONG")
    print(f"    Binance WS: {ws_b}")
    print(f"    Bitget  WS: {ws_bg}")
    print(f"    Kucoin  WS: {ws_k}")

    print("\n  Ожидание еще 1.0 секунду перед контрольным GET-запросом...")
    await asyncio.sleep(1.0)

    print("\n  [ШАГ 1.2] Параллельные точечные REST GET-запросы посимвольно:")
    t_start = time.perf_counter()
    rest_tasks = [
        binance_order.get_exact_position_guarded(test_sym_binance, "LONG"),
        bitget_order.get_exact_position_guarded(test_sym_bitget, "SHORT"),
        kucoin_order.get_exact_position_guarded(test_sym_kucoin, "LONG")
    ]
    rest_res = await asyncio.gather(*rest_tasks, return_exceptions=True)
    t_rtt = (time.perf_counter() - t_start) * 1000
    print(f"    Параллельный опрос занял: {t_rtt:.1f} мс")
    print(f"    Binance REST: {rest_res[0]}")
    print(f"    Bitget  REST: {rest_res[1]}")
    print(f"    Kucoin  REST: {rest_res[2]}")

    print("\n  [ШАГ 1.3] Сверка WS vs REST:")
    print(f"    Binance: WS size={ws_b.get('size')} | REST size={rest_res[0].get('size')}")
    print(f"    Bitget:  WS size={ws_bg.get('size')} | REST size={rest_res[1].get('size')}")
    print(f"    Kucoin:  WS size={ws_k.get('size')} | REST size={rest_res[2].get('size')}")

    print("\n  Отмена тестовых лимиток...")
    await asyncio.gather(
        binance_order.cancel_all_orders(test_sym_binance),
        bitget_order.cancel_all_orders(test_sym_bitget),
        kucoin_order.cancel_all_orders(test_sym_kucoin),
        return_exceptions=True
    )
    print("  Лимитки отменены.")

    # ============================================================
    # ЭТАП 2: Реальный налив маркетами на 6$
    # ============================================================
    print("\n" + "=" * 50)
    print("▶️ ЭТАП 2: ТЕСТ С РЕАЛЬНЫМ НАЛИВОМ МАРКЕТАМИ (ПО 6$)")
    print("=" * 50)

    print(f"  Открываем маркет-ордера на 6$ по Binance (BUY), Bitget (SELL), Kucoin (BUY)...")
    mkt_open_tasks = [
        binance_order.place_order(test_sym_binance, "BUY", test_size_usd, mkt_price, order_type="MARKET", position_side="LONG"),
        bitget_order.place_order(test_sym_bitget, "SELL", test_size_usd, mkt_price, order_type="MARKET", position_side="SHORT"),
        kucoin_order.place_order(test_sym_kucoin, "BUY", test_size_usd, mkt_price, order_type="MARKET", position_side="LONG")
    ]
    open_results = await asyncio.gather(*mkt_open_tasks, return_exceptions=True)
    print(f"  Ответы бирж на вход по маркету: {[type(r).__name__ if isinstance(r, Exception) else 'FILLED/OK' for r in open_results]}")

    print("  Ожидание 2.0 секунды для перехвата событий налива сокетами...")
    await asyncio.sleep(2.0)

    print("\n  [ШАГ 2.1] Парсинг данных из детерминатора позиций (ТОЛЬКО WEBSOCKET!):")
    ws_fill_b = binance_order.get_executed_position(test_sym_binance, "LONG")
    ws_fill_bg = bitget_order.get_executed_position(test_sym_bitget, "SHORT")
    ws_fill_k = kucoin_order.get_executed_position(test_sym_kucoin, "LONG")
    print(f"    Binance WS Fill: size={ws_fill_b.get('size')}, price={ws_fill_b.get('price')}")
    print(f"    Bitget  WS Fill: size={ws_fill_bg.get('size')}, price={ws_fill_bg.get('price')}")
    print(f"    Kucoin  WS Fill: size={ws_fill_k.get('size')}, price={ws_fill_k.get('price')}")

    print("\n  Ожидание 1.0 секунду перед контрольным GET-запросом...")
    await asyncio.sleep(1.0)

    print("\n  [ШАГ 2.2] Параллельные точечные REST GET-запросы посимвольно:")
    t_start = time.perf_counter()
    rest_fill_tasks = [
        binance_order.get_exact_position_guarded(test_sym_binance, "LONG"),
        bitget_order.get_exact_position_guarded(test_sym_bitget, "SHORT"),
        kucoin_order.get_exact_position_guarded(test_sym_kucoin, "LONG")
    ]
    rest_fill_res = await asyncio.gather(*rest_fill_tasks, return_exceptions=True)
    t_rtt2 = (time.perf_counter() - t_start) * 1000
    print(f"    Параллельный опрос занял: {t_rtt2:.1f} мс")
    print(f"    Binance REST: {rest_fill_res[0]}")
    print(f"    Bitget  REST: {rest_fill_res[1]}")
    print(f"    Kucoin  REST: {rest_fill_res[2]}")

    print("\n  [ШАГ 2.3] Сверка налива: Сокет vs Точечный REST GET:")
    print(f"    Binance: WS size={ws_fill_b.get('size')}, price={ws_fill_b.get('price')}  |  REST size={rest_fill_res[0].get('size')}, price={rest_fill_res[0].get('price')}")
    print(f"    Bitget:  WS size={ws_fill_bg.get('size')}, price={ws_fill_bg.get('price')}  |  REST size={rest_fill_res[1].get('size')}, price={rest_fill_res[1].get('price')}")
    print(f"    Kucoin:  WS size={ws_fill_k.get('size')}, price={ws_fill_k.get('price')}  |  REST size={rest_fill_res[2].get('size')}, price={rest_fill_res[2].get('price')}")

    # ============================================================
    # ЭТАП 3: Полная ликвидация / сброс позиций маркетом
    # ============================================================
    print("\n" + "=" * 50)
    print("▶️ ЭТАП 3: ЗАКРЫТИЕ ПОЗИЦИЙ МАРКЕТОМ И ВЕРИФИКАЦИЯ ОБНУЛЕНИЯ ДО 0.0")
    print("=" * 50)

    close_tasks = []
    # Binance был BUY LONG -> закрываем SELL LONG
    qty_b = rest_fill_res[0].get("size", 0.0) or ws_fill_b.get("size", 0.0)
    if qty_b > 0:
        close_tasks.append(binance_order.place_order(test_sym_binance, "SELL", qty_b * mkt_price, mkt_price, order_type="MARKET", position_side="LONG"))
    
    # Bitget был SELL SHORT -> закрываем BUY SHORT
    qty_bg = rest_fill_res[1].get("size", 0.0) or ws_fill_bg.get("size", 0.0)
    if qty_bg > 0:
        close_tasks.append(bitget_order.place_order(test_sym_bitget, "BUY", qty_bg * mkt_price, mkt_price, order_type="MARKET", position_side="SHORT"))

    # Kucoin был BUY LONG -> закрываем SELL LONG
    qty_k = rest_fill_res[2].get("size", 0.0) or ws_fill_k.get("size", 0.0)
    if qty_k > 0:
        close_tasks.append(kucoin_order.place_order(test_sym_kucoin, "SELL", qty_k * mkt_price, mkt_price, order_type="MARKET", position_side="LONG"))

    print(f"  Закрываем позиции маркетом: Binance={qty_b}, Bitget={qty_bg}, Kucoin={qty_k}...")
    close_res = await asyncio.gather(*close_tasks, return_exceptions=True)
    print(f"  Ответы на закрытие: {[type(r).__name__ if isinstance(r, Exception) else 'CLOSED' for r in close_res]}")

    print("  Ожидание 2.0 секунды для обработки событий обнуления...")
    await asyncio.sleep(2.0)

    # Контрольный REST GET на проверку строгого 0.0
    final_tasks = [
        binance_order.get_exact_position_guarded(test_sym_binance, "LONG"),
        bitget_order.get_exact_position_guarded(test_sym_bitget, "SHORT"),
        kucoin_order.get_exact_position_guarded(test_sym_kucoin, "LONG")
    ]
    final_res = await asyncio.gather(*final_tasks, return_exceptions=True)

    print("\n  [ФИНАЛЬНЫЙ РЕЗУЛЬТАТ] Точечные REST GET-запросы на остаток позиций:")
    print(f"    Binance Final: {final_res[0]}")
    print(f"    Bitget  Final: {final_res[1]}")
    print(f"    Kucoin  Final: {final_res[2]}")

    b_zero = final_res[0].get("size", 0.0) == 0.0
    bg_zero = final_res[1].get("size", 0.0) == 0.0
    k_zero = final_res[2].get("size", 0.0) == 0.0

    print("\n" + "=" * 70)
    if b_zero and bg_zero and k_zero:
        print("✅ ВСЕ ТРИ БИРЖИ СТРОГО ОБНУЛЕНЫ (0.0). СОКЕТЫ И ТОЧЕЧНЫЕ REST GET РАБОТАЮТ ИДЕАЛЬНО!")
    else:
        print(f"❌ ВНИМАНИЕ: ОСТАТОК НЕ ОБНУЛЕН! B:{b_zero}, BG:{bg_zero}, K:{k_zero}")
    print("=" * 70)

    # Остановка стримов
    await binance_stream.stop()
    await bitget_stream.stop()
    await kucoin_stream.stop()
    t_binance.cancel()
    t_bitget.cancel()
    t_kucoin.cancel()
    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
