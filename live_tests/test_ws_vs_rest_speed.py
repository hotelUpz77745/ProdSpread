# ============================================================
# FILE: live_tests/test_ws_vs_rest_speed.py
# ROLE: Сравнение скорости исполнения MARKET ордеров (WS Streams vs Прямой REST) на Binance, Bitget, Kucoin ($6 USD)
# ============================================================
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

import asyncio
import time
import json
from dotenv import load_dotenv
from c_log import log
from utils import SessionManager
from API.orders import BinanceOrder, BitgetOrder, KucoinOrder
from API.BINANCE.ws_trade_binance import BinanceWsTrader
from API.BITGET.ws_trade_bitget import BitgetWsTrader
from API.KUCOIN.ws_trade_kucoin import KucoinWsTrader

import uuid

load_dotenv()

TEST_COIN = "ADA"
ORDER_SIZE_USD = 6.0  # Требование пользователя: ровно 6 долларов

async def run_benchmark():
    print("=" * 80)
    print(f"🚀 СТАРТ БЕНЧМАРКА СКОРОСТИ ИСПОЛНЕНИЯ ОРДЕРОВ: WS STREAMS vs DIRECT REST")
    print(f"Объем сделок: ${ORDER_SIZE_USD:.2f} USD | Инструмент: {TEST_COIN}USDT")
    print("=" * 80)

    session = await SessionManager().get_session()

    # 1. Инициализация адаптеров
    binance = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session
    )
    bitget = BitgetOrder(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
        margin_settings={"margin_type": "cross"},
        session=session
    )
    kucoin = KucoinOrder(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
        session=session,
        position_stream=None,
        margin_settings={"margin_type": "cross", "leverage": 20}
    )

    print("\n⏳ Загрузка спецификаций инструментов...")
    await bitget.update_symbol_info()
    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as r:
        binance.symbol_info = (await r.json()).get("symbols", [])
    async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as r:
        kucoin.symbol_info = (await r.json()).get("data", [])

    # Получаем актуальную цену
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=ADAUSDT") as r:
        ref_price = float((await r.json())["price"])
    print(f"Актуальная референсная цена {TEST_COIN}USDT: {ref_price:.4f} USDT")

    # 2. Инициализация и прогрев WebSocket стримов
    print("\n🔌 Подключение реактивных торговых WebSocket-стримов...")
    binance_ws = BinanceWsTrader(binance.api_key, binance.api_secret, session=session)
    bitget_ws = BitgetWsTrader(bitget.api_key, bitget.api_secret, bitget.api_passphrase, margin_settings={"margin_type": "crossed"}, session=session)
    kucoin_ws = KucoinWsTrader(kucoin.api_key, kucoin.api_secret, kucoin.api_passphrase, margin_settings={"margin_type": "CROSS", "leverage": 20}, session=session)

    await asyncio.gather(
        binance_ws.start(),
        bitget_ws.start(),
        kucoin_ws.start()
    )
    await asyncio.sleep(1.0)  # Даем сокетам войти в рабочее горячее состояние

    results = []

    # =========================================================================
    # БЕНЧМАРК 1: BINANCE
    # =========================================================================
    print("\n" + "=" * 50)
    print("📍 [1/3] ТЕСТИРОВАНИЕ BINANCE (ADAUSDT)")
    print("=" * 50)

    # 1.1 REST Open
    t0 = time.perf_counter()
    res_b_rest_open = await binance.session.post(
        f"https://fapi.binance.com/fapi/v1/order?symbol=ADAUSDT&side=BUY&positionSide=LONG&type=MARKET&quantity={int(ORDER_SIZE_USD/ref_price)}&timestamp={int(time.time()*1000)}&signature={binance._generate_signature(f'symbol=ADAUSDT&side=BUY&positionSide=LONG&type=MARKET&quantity={int(ORDER_SIZE_USD/ref_price)}&timestamp={int(time.time()*1000)}')}",
        headers={"X-MBX-APIKEY": binance.api_key}
    )
    b_rest_open_data = await res_b_rest_open.json()
    rtt_b_rest_open = (time.perf_counter() - t0) * 1000
    print(f"  REST Open:  {rtt_b_rest_open:.2f} ms | OrderId: {b_rest_open_data.get('orderId')}")

    await asyncio.sleep(0.3)

    # 1.2 REST Close
    pos_b = await binance.get_exact_position_guarded("ADAUSDT", "LONG")
    qty_to_close = str(int(pos_b.get("size", int(ORDER_SIZE_USD/ref_price))))
    t0 = time.perf_counter()
    res_b_rest_close = await binance.session.post(
        f"https://fapi.binance.com/fapi/v1/order?symbol=ADAUSDT&side=SELL&positionSide=LONG&type=MARKET&quantity={qty_to_close}&timestamp={int(time.time()*1000)}&signature={binance._generate_signature(f'symbol=ADAUSDT&side=SELL&positionSide=LONG&type=MARKET&quantity={qty_to_close}&timestamp={int(time.time()*1000)}')}",
        headers={"X-MBX-APIKEY": binance.api_key}
    )
    b_rest_close_data = await res_b_rest_close.json()
    rtt_b_rest_close = (time.perf_counter() - t0) * 1000
    print(f"  REST Close: {rtt_b_rest_close:.2f} ms | OrderId: {b_rest_close_data.get('orderId')}")

    await asyncio.sleep(0.5)

    # 1.3 WS Open
    t0 = time.perf_counter()
    b_ws_open_data = await binance_ws.place_order("ADAUSDT", "BUY", str(int(ORDER_SIZE_USD/ref_price)), position_side="LONG")
    rtt_b_ws_open = (time.perf_counter() - t0) * 1000
    print(f"  WS Open:    {rtt_b_ws_open:.2f} ms | OrderId: {b_ws_open_data.get('orderId')}")

    await asyncio.sleep(0.3)

    # 1.4 WS Close
    pos_b = await binance.get_exact_position_guarded("ADAUSDT", "LONG")
    qty_to_close = str(int(pos_b.get("size", int(ORDER_SIZE_USD/ref_price))))
    t0 = time.perf_counter()
    b_ws_close_data = await binance_ws.place_order("ADAUSDT", "SELL", qty_to_close, position_side="LONG")
    rtt_b_ws_close = (time.perf_counter() - t0) * 1000
    print(f"  WS Close:   {rtt_b_ws_close:.2f} ms | OrderId: {b_ws_close_data.get('orderId')}")

    # Проверка обнуления
    await asyncio.sleep(0.5)
    pos_final_b = await binance.get_exact_position_guarded("ADAUSDT", "LONG")
    print(f"  Финальная позиция Binance: {pos_final_b.get('size')} (Ожидается: 0.0)")

    results.append({
        "exchange": "BINANCE",
        "action": "OPEN",
        "rest_ms": rtt_b_rest_open,
        "ws_ms": rtt_b_ws_open
    })
    results.append({
        "exchange": "BINANCE",
        "action": "CLOSE",
        "rest_ms": rtt_b_rest_close,
        "ws_ms": rtt_b_ws_close
    })

    # =========================================================================
    # БЕНЧМАРК 2: BITGET
    # =========================================================================
    print("\n" + "=" * 50)
    print("📍 [2/3] ТЕСТИРОВАНИЕ BITGET (ADAUSDT)")
    print("=" * 50)

    qty_bg = str(int(ORDER_SIZE_USD / ref_price))

    # 2.1 REST Open
    now = str(int(time.time() * 1000))
    body_bg_open = json.dumps({
        "symbol": "ADAUSDT",
        "productType": "USDT-FUTURES",
        "marginMode": "crossed",
        "marginCoin": "USDT",
        "size": qty_bg,
        "side": "buy",
        "tradeSide": "open",
        "orderType": "market",
        "clientOid": str(uuid.uuid4()).replace("-", "")[:30]
    })
    t0 = time.perf_counter()
    async with session.post(
        "https://api.bitget.com/api/v2/mix/order/place-order",
        headers={
            "ACCESS-KEY": bitget.api_key,
            "ACCESS-SIGN": bitget._generate_signature(now, "POST", "/api/v2/mix/order/place-order", body_bg_open),
            "ACCESS-TIMESTAMP": now,
            "ACCESS-PASSPHRASE": bitget.api_passphrase,
            "Content-Type": "application/json"
        },
        data=body_bg_open
    ) as r:
        bg_rest_open_data = await r.json()
    rtt_bg_rest_open = (time.perf_counter() - t0) * 1000
    print(f"  REST Open:  {rtt_bg_rest_open:.2f} ms | Code: {bg_rest_open_data.get('code')}")

    await asyncio.sleep(0.3)

    # 2.2 REST Close
    t0 = time.perf_counter()
    bg_rest_close_data = await bitget._close_position("ADAUSDT", "long")
    rtt_bg_rest_close = (time.perf_counter() - t0) * 1000
    print(f"  REST Close: {rtt_bg_rest_close:.2f} ms | Code: {bg_rest_close_data.get('code')}")

    await asyncio.sleep(0.5)

    # 2.3 WS Open
    t0 = time.perf_counter()
    bg_ws_open_data = await bitget_ws.place_order("ADAUSDT", "buy", qty_bg, trade_side="open")
    rtt_bg_ws_open = (time.perf_counter() - t0) * 1000
    print(f"  WS Open:    {rtt_bg_ws_open:.2f} ms | Event: {bg_ws_open_data.get('event', bg_ws_open_data.get('code'))}")

    await asyncio.sleep(0.3)

    # 2.4 WS Close (через эндпоинт закрытия)
    t0 = time.perf_counter()
    bg_ws_close_data = await bitget._close_position("ADAUSDT", "long")
    rtt_bg_ws_close = (time.perf_counter() - t0) * 1000
    print(f"  WS Close:   {rtt_bg_ws_close:.2f} ms | Code: {bg_ws_close_data.get('code')}")

    # Проверка обнуления
    await asyncio.sleep(0.5)
    pos_final_bg = await bitget.get_exact_position_guarded("ADAUSDT", "LONG")
    print(f"  Финальная позиция Bitget: {pos_final_bg.get('size')} (Ожидается: 0.0)")

    results.append({
        "exchange": "BITGET",
        "action": "OPEN",
        "rest_ms": rtt_bg_rest_open,
        "ws_ms": rtt_bg_ws_open
    })
    results.append({
        "exchange": "BITGET",
        "action": "CLOSE",
        "rest_ms": rtt_bg_rest_close,
        "ws_ms": rtt_bg_ws_close
    })

    # =========================================================================
    # БЕНЧМАРК 3: KUCOIN
    # =========================================================================
    print("\n" + "=" * 50)
    print("📍 [3/3] ТЕСТИРОВАНИЕ KUCOIN (ADAUSDTM)")
    print("=" * 50)

    # 1 лот ADAUSDTM на Kucoin = 10 ADA (~$2.10). 3 лота = 30 ADA = ~$6.30 USD
    qty_k = "3"

    # 3.1 REST Open
    t0 = time.perf_counter()
    k_rest_open_data = await kucoin._place_order_rest("ADAUSDTM", "buy", qty_k, "LONG") if hasattr(kucoin, "_place_order_rest") else await kucoin_ws._place_order_rest("ADAUSDTM", "buy", qty_k, "LONG", "20", "CROSS")
    rtt_k_rest_open = (time.perf_counter() - t0) * 1000
    print(f"  REST Open:  {rtt_k_rest_open:.2f} ms | Code: {k_rest_open_data.get('code')}")

    await asyncio.sleep(0.3)

    # 3.2 REST Close
    t0 = time.perf_counter()
    k_rest_close_data = await kucoin_ws._place_order_rest("ADAUSDTM", "sell", qty_k, "LONG", "20", "CROSS")
    rtt_k_rest_close = (time.perf_counter() - t0) * 1000
    print(f"  REST Close: {rtt_k_rest_close:.2f} ms | Code: {k_rest_close_data.get('code')}")

    await asyncio.sleep(0.5)

    # 3.3 WS/Hot Transport Open
    t0 = time.perf_counter()
    k_ws_open_data = await kucoin_ws.place_order("ADAUSDTM", "buy", qty_k, position_side="LONG")
    rtt_k_ws_open = (time.perf_counter() - t0) * 1000
    print(f"  WS/Hot Open:  {rtt_k_ws_open:.2f} ms | Code: {k_ws_open_data.get('code')}")

    await asyncio.sleep(0.3)

    # 3.4 WS/Hot Transport Close
    t0 = time.perf_counter()
    k_ws_close_data = await kucoin_ws.place_order("ADAUSDTM", "sell", qty_k, position_side="LONG")
    rtt_k_ws_close = (time.perf_counter() - t0) * 1000
    print(f"  WS/Hot Close: {rtt_k_ws_close:.2f} ms | Code: {k_ws_close_data.get('code')}")

    # Проверка обнуления
    await asyncio.sleep(0.5)
    pos_final_k = await kucoin.get_exact_position_guarded("ADAUSDTM", "LONG")
    print(f"  Финальная позиция Kucoin: {pos_final_k.get('size')} (Ожидается: 0.0)")

    results.append({
        "exchange": "KUCOIN",
        "action": "OPEN",
        "rest_ms": rtt_k_rest_open,
        "ws_ms": rtt_k_ws_open
    })
    results.append({
        "exchange": "KUCOIN",
        "action": "CLOSE",
        "rest_ms": rtt_k_rest_close,
        "ws_ms": rtt_k_ws_close
    })

    # =========================================================================
    # ИТОГОВЫЙ СВОДНЫЙ ОТЧЕТ
    # =========================================================================
    print("\n" + "=" * 80)
    print("📊 ИТОГОВЫЙ ОТЧЕТ: СРАВНЕНИЕ СКОРОСТИ WS STREAM vs DIRECT REST")
    print("=" * 80)
    header = f"{'Биржа':<12} | {'Операция':<8} | {'REST Latency':<14} | {'WS Latency':<14} | {'Дельта (мс)':<12} | {'Ускорение':<10}"
    print(header)
    print("-" * len(header))

    for r in results:
        diff = r["rest_ms"] - r["ws_ms"]
        pct = (diff / r["rest_ms"]) * 100 if r["rest_ms"] > 0 else 0
        speedup_str = f"+{pct:.1f}%" if diff > 0 else f"{pct:.1f}%"
        diff_str = f"-{diff:.1f} ms" if diff > 0 else f"+{abs(diff):.1f} ms"
        print(f"{r['exchange']:<12} | {r['action']:<8} | {r['rest_ms']:>8.2f} ms    | {r['ws_ms']:>8.2f} ms    | {diff_str:>10} | {speedup_str:>10}")

    print("-" * len(header))
    print(f"✅ Финальный контроль балансов: Binance={pos_final_b.get('size')}, Bitget={pos_final_bg.get('size')}, Kucoin={pos_final_k.get('size')}")
    print("=" * 80)

    # Закрываем сессии
    await binance_ws.close()
    await bitget_ws.close()
    await kucoin_ws.close()
    await session.close()

if __name__ == "__main__":
    asyncio.run(run_benchmark())
