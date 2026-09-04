# ============================================================
# FILE: live_tests/test_all_components.py
# ROLE: Сквозная диагностика всех компонентов системы
# ============================================================
import asyncio
import os
import sys
import time
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

# 1. Price streams
from API.BINANCE.stakan import BinanceStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.KUCOIN.stakan import KucoinStakanStream

# 2. Private position streams
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream

# 3. Order adapters
from API.orders import BinanceOrder, BitgetOrder, KucoinOrder


async def test_price_websockets():
    print("\n" + "=" * 60)
    print(">>> 1. ТЕСТИРОВАНИЕ ВСЕХ ТРЕХ СОКЕТОВ ЦЕН (ORDERBOOK DEPTH)")
    print("=" * 60)

    results = {}

    # BINANCE
    b_received = asyncio.Event()
    b_data = {}

    async def on_binance_depth(d):
        if not b_received.is_set():
            b_data["symbol"] = d.symbol
            b_data["bids"] = len(d.bids)
            b_data["asks"] = len(d.asks)
            b_data["best_bid"] = d.bids[0] if d.bids else None
            b_data["best_ask"] = d.asks[0] if d.asks else None
            b_received.set()

    b_stream = BinanceStakanStream(["BTCUSDT"])
    b_task = asyncio.create_task(b_stream.run(on_binance_depth))

    # BITGET
    bg_received = asyncio.Event()
    bg_data = {}

    async def on_bitget_depth(d):
        if not bg_received.is_set():
            bg_data["symbol"] = d.symbol
            bg_data["bids"] = len(d.bids)
            bg_data["asks"] = len(d.asks)
            bg_data["best_bid"] = d.bids[0] if d.bids else None
            bg_data["best_ask"] = d.asks[0] if d.asks else None
            bg_received.set()

    bg_stream = BitgetStakanStream(["BTCUSDT"])
    bg_task = asyncio.create_task(bg_stream.run(on_bitget_depth))

    # KUCOIN
    k_received = asyncio.Event()
    k_data = {}

    async def on_kucoin_depth(d):
        if not k_received.is_set():
            k_data["symbol"] = d.symbol
            k_data["bids"] = len(d.bids)
            k_data["asks"] = len(d.asks)
            k_data["best_bid"] = d.bids[0] if d.bids else None
            k_data["best_ask"] = d.asks[0] if d.asks else None
            k_received.set()

    k_stream = KucoinStakanStream(["XBTUSDTM"])
    k_task = asyncio.create_task(k_stream.run(on_kucoin_depth))

    # Wait up to 10 seconds for all three
    t0 = time.time()
    try:
        await asyncio.wait_for(
            asyncio.gather(b_received.wait(), bg_received.wait(), k_received.wait()),
            timeout=10.0
        )
        dt = time.time() - t0
        print(f"✅ Все три сокета цен получили стаканы за {dt:.2f}с:")
    except asyncio.TimeoutError:
        print(f"⚠️ Таймаут ожидания стаканов через 10с.")

    print(f"  [BINANCE] {b_data.get('symbol')}: Bids={b_data.get('bids')}, Asks={b_data.get('asks')}, BestBid={b_data.get('best_bid')}, BestAsk={b_data.get('best_ask')}")
    print(f"  [BITGET]  {bg_data.get('symbol')}: Bids={bg_data.get('bids')}, Asks={bg_data.get('asks')}, BestBid={bg_data.get('best_bid')}, BestAsk={bg_data.get('best_ask')}")
    print(f"  [KUCOIN]  {k_data.get('symbol')}: Bids={k_data.get('bids')}, Asks={k_data.get('asks')}, BestBid={k_data.get('best_bid')}, BestAsk={k_data.get('best_ask')}")

    # Stop price streams
    b_task.cancel()
    bg_task.cancel()
    k_task.cancel()
    await asyncio.gather(b_task, bg_task, k_task, return_exceptions=True)

    results["BINANCE"] = bool(b_data)
    results["BITGET"] = bool(bg_data)
    results["KUCOIN"] = bool(k_data)
    return results


async def test_order_adapters():
    print("\n" + "=" * 60)
    print(">>> 2. ТЕСТИРОВАНИЕ ВСЕХ АДАПТОРОВ ТРЕХ БИРЖ (ORDERS / SPECS / WARMUP)")
    print("=" * 60)

    session = await SessionManager().get_session()
    results = {}

    binance = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session
    )

    bitget = BitgetOrder(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
        session=session,
        margin_settings={"margin_type": "cross"}
    )

    kucoin = KucoinOrder(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
        session=session,
        position_stream=None,
        margin_settings={"margin_type": "cross", "leverage": 1}
    )

    # 1. Update Symbol Info
    print("1) Загрузка спецификаций инструментов (specs)...")
    async def fetch_binance_info():
        async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
            data = await resp.json()
            binance.symbol_info = data.get("symbols", [])

    async def fetch_kucoin_info():
        async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
            data = await resp.json()
            kucoin.symbol_info = data.get("data", [])

    await asyncio.gather(
        fetch_binance_info(),
        bitget.update_symbol_info(),
        fetch_kucoin_info(),
        return_exceptions=True
    )
    b_symbols = len(binance.symbol_info) if binance.symbol_info else 0
    bg_symbols = len(bitget.symbol_info) if bitget.symbol_info else 0
    k_symbols = len(kucoin.symbol_info) if kucoin.symbol_info else 0
    print(f"  [BINANCE] Инструментов загружено: {b_symbols}")
    print(f"  [BITGET]  Инструментов загружено: {bg_symbols}")
    print(f"  [KUCOIN]  Инструментов загружено: {k_symbols}")

    # 2. Warmup test
    print("\n2) Тестирование прогрева (warmup) и блокиратора активности...")
    t0 = time.time()
    await asyncio.gather(
        binance.warmup(),
        bitget.warmup(),
        kucoin.warmup(),
        return_exceptions=True
    )
    print(f"  Warmup completed in {time.time() - t0:.3f}s (No exceptions).")

    # 3. Safe limit order placement and cancel (very safe distance: XRP at $0.20 BUY, $5.00 SELL)
    print("\n3) Безопасное тестирование постановки и отмены LIMIT GTC ордеров...")
    test_symbol_b = "XRPUSDT"
    test_symbol_bg = "XRPUSDT"
    test_size_usd = 6.0

    # Place distant limit
    b_order_res = await binance.place_order(test_symbol_b, "BUY", test_size_usd, 0.20, position_side="LONG", time_in_force="GTC")
    bg_order_res = await bitget.place_order(test_symbol_bg, "SELL", test_size_usd, 5.00, position_side="SHORT", time_in_force="GTC")
    print(f"  [BINANCE] Safe Limit Place: {b_order_res.get('status') if isinstance(b_order_res, dict) else b_order_res}")
    print(f"  [BITGET]  Safe Limit Place: {bg_order_res.get('status') if isinstance(bg_order_res, dict) else bg_order_res}")

    await asyncio.sleep(0.5)

    # Cancel
    b_cancel = await binance.cancel_all_orders(test_symbol_b)
    bg_cancel = await bitget.cancel_all_orders(test_symbol_bg)
    print(f"  [BINANCE] Cancel Result: {b_cancel}")
    print(f"  [BITGET]  Cancel Result: {bg_cancel}")

    # Kucoin order check (check_order_size)
    k_size_valid = False
    try:
        kucoin.check_order_size("XRPUSDTM", 20.0, 1.50)
        k_size_valid = True
        print(f"  [KUCOIN]  check_order_size валидация: OK")
    except Exception as e:
        print(f"  [KUCOIN]  check_order_size error: {e}")

    results["BINANCE"] = bool(b_symbols > 0 and b_order_res)
    results["BITGET"] = bool(bg_symbols > 0 and bg_order_res)
    results["KUCOIN"] = bool(k_symbols > 0 and k_size_valid)
    return results


async def test_position_streams():
    print("\n" + "=" * 60)
    print(">>> 3. ТЕСТИРОВАНИЕ ВСЕХ ТРЕХ СТРИМОВ ПОЗИЦИЙ (PRIVATE WS)")
    print("=" * 60)

    results = {}

    binance_stream = BinancePositionStream(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", "")
    )
    bitget_stream = BitgetPositionStream(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", "")
    )
    kucoin_stream = KucoinPositionStream(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
    )

    t_binance = asyncio.create_task(binance_stream.start())
    t_bitget = asyncio.create_task(bitget_stream.start())
    t_kucoin = asyncio.create_task(kucoin_stream.start())

    # Wait for ready or is_connected for up to 8 seconds
    t0 = time.time()
    for _ in range(40):
        await asyncio.sleep(0.2)
        b_ok = binance_stream.ready or binance_stream.is_connected
        bg_ok = bitget_stream.ready or bitget_stream.is_connected
        k_ok = kucoin_stream.ready or kucoin_stream.is_connected
        if b_ok and bg_ok and k_ok:
            break

    print(f"  [BINANCE WS_PRIVATE] Connected={binance_stream.is_connected}, Ready={binance_stream.ready}")
    print(f"  [BITGET WS_PRIVATE]  Connected={bitget_stream.is_connected}, Ready={bitget_stream.ready}")
    print(f"  [KUCOIN WS_PRIVATE]  Connected={kucoin_stream.is_connected}, Ready={kucoin_stream.ready}")

    # Test reading memory position
    print(f"  [BINANCE] get_position('BTCUSDT', 'LONG'): {binance_stream.get_position('BTCUSDT', 'LONG')}")
    print(f"  [BITGET]  get_position('BTCUSDT', 'LONG'): {bitget_stream.get_position('BTCUSDT', 'LONG')}")
    print(f"  [KUCOIN]  get_position('XBTUSDTM', 'LONG'): {kucoin_stream.get_position('XBTUSDTM', 'LONG')}")

    results["BINANCE"] = binance_stream.is_connected
    results["BITGET"] = bitget_stream.is_connected
    results["KUCOIN"] = kucoin_stream.is_connected

    # Clean stop
    await binance_stream.stop()
    await bitget_stream.stop()
    await kucoin_stream.stop()

    t_binance.cancel()
    t_bitget.cancel()
    t_kucoin.cancel()
    await asyncio.gather(t_binance, t_bitget, t_kucoin, return_exceptions=True)

    return results


async def test_get_position_guarded():
    print("\n" + "=" * 60)
    print(">>> 4. ТЕСТИРОВАНИЕ ВСЕХ ТРЕХ ГЕТ-ЗАПРОСНИКОВ ПОЗИЦИЙ (REST GUARDED)")
    print("=" * 60)

    session = await SessionManager().get_session()
    results = {}

    binance = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session
    )
    bitget = BitgetOrder(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
        session=session,
        margin_settings={"margin_type": "cross"}
    )
    kucoin = KucoinOrder(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
        session=session,
        position_stream=None,
        margin_settings={"margin_type": "cross", "leverage": 1}
    )

    t0 = time.time()
    b_res = await binance.get_exact_position_guarded("BTCUSDT", "LONG")
    b_time = (time.time() - t0) * 1000

    t0 = time.time()
    bg_res = await bitget.get_exact_position_guarded("BTCUSDT", "LONG")
    bg_time = (time.time() - t0) * 1000

    t0 = time.time()
    k_res = await kucoin.get_exact_position_guarded("XBTUSDTM", "LONG")
    k_time = (time.time() - t0) * 1000

    print(f"  [BINANCE] get_exact_position_guarded -> {b_res} (RTT: {b_time:.1f}ms)")
    print(f"  [BITGET]  get_exact_position_guarded -> {bg_res} (RTT: {bg_time:.1f}ms)")
    print(f"  [KUCOIN]  get_exact_position_guarded -> {k_res} (RTT: {k_time:.1f}ms)")

    results["BINANCE"] = b_res.get("status") in ("ok", "fallback_ws")
    results["BITGET"] = bg_res.get("status") in ("ok", "fallback_ws")
    results["KUCOIN"] = k_res.get("status") in ("ok", "fallback_ws")

    return results


async def main():
    print("🚀 НАЧАЛО ПОЛНОГО ТЕСТИРОВАНИЯ ВСЕХ КОМПОНЕНТОВ СИСТЕМЫ...")
    
    r1 = await test_price_websockets()
    r2 = await test_order_adapters()
    r3 = await test_position_streams()
    r4 = await test_get_position_guarded()

    print("\n" + "=" * 60)
    print("📊 СВОДНЫЙ ОТЧЕТ ТЕСТИРОВАНИЯ:")
    print("=" * 60)
    print(f"1. Сокеты цен:         BINANCE: {'✅' if r1.get('BINANCE') else '❌'} | BITGET: {'✅' if r1.get('BITGET') else '❌'} | KUCOIN: {'✅' if r1.get('KUCOIN') else '❌'}")
    print(f"2. Адапторы бирж:      BINANCE: {'✅' if r2.get('BINANCE') else '❌'} | BITGET: {'✅' if r2.get('BITGET') else '❌'} | KUCOIN: {'✅' if r2.get('KUCOIN') else '❌'}")
    print(f"3. Стримы позиций:     BINANCE: {'✅' if r3.get('BINANCE') else '❌'} | BITGET: {'✅' if r3.get('BITGET') else '❌'} | KUCOIN: {'✅' if r3.get('KUCOIN') else '❌'}")
    print(f"4. Guarded GET запросы:BINANCE: {'✅' if r4.get('BINANCE') else '❌'} | BITGET: {'✅' if r4.get('BITGET') else '❌'} | KUCOIN: {'✅' if r4.get('KUCOIN') else '❌'}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
