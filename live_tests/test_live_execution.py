# ============================================================
# FILE: test_live_execution.py
# ROLE: Тестирование живого цикла исполнения ордеров (6$) на
#       Binance и Kucoin: выставление GTC лимитки, пауза,
#       отмена остатка, трекинг позиции через WS/REST, и закрытие.
# ============================================================
import asyncio
import os
import sys
import time
import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from consts import load_config
from utils import SessionManager
from API.orders import BinanceOrder, KucoinOrder
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from c_log import log

async def run_test(test_coin: str = "DOGE", test_size_usd: float = 6.0, execution_pause: float = 0.3):
    cfg = load_config()
    session = await SessionManager().get_session()
    
    binance_key = os.environ.get("BINANCE_API_KEY", "")
    binance_sec = os.environ.get("BINANCE_API_SECRET", "")
    
    kucoin_key = os.environ.get("KUCOIN_API_KEY", "")
    kucoin_sec = os.environ.get("KUCOIN_API_SECRET", "")
    kucoin_pass = os.environ.get("KUCOIN_API_PASSPHRASE", "")
    
    print(f"=== ТЕСТ ИСПОЛНЕНИЯ: {test_coin} | Объем: {test_size_usd}$ | Пауза: {execution_pause}с ===")
    
    # 1. Запуск WS потоков
    b_stream = BinancePositionStream(binance_key, binance_sec)
    k_stream = KucoinPositionStream(kucoin_key, kucoin_sec, kucoin_pass)
    
    if binance_key:
        asyncio.create_task(b_stream.start())
    if kucoin_key:
        asyncio.create_task(k_stream.start())
        
    # 2. Инициализация ордеров
    b_order = BinanceOrder(binance_key, binance_sec, session, b_stream)
    k_order = KucoinOrder(kucoin_key, kucoin_sec, kucoin_pass, session, k_stream, cfg["margin_settings"]["KUCOIN"])
    
    b_order.start()
    k_order.start()
    
    print("⏳ Ожидание подключения вебсокетов и загрузки specs (3 сек)...")
    await asyncio.sleep(3.0)
    
    native_binance = f"{test_coin}USDT"
    native_kucoin = f"{test_coin}USDTM"
    
    # 3. Проверка текущих позиций
    b_pos_before = await b_order.get_exact_position(native_binance, "LONG")
    k_pos_before = await k_order.get_exact_position(native_kucoin, "SHORT")
    print(f"📊 До входа: Binance LONG = {b_pos_before}, Kucoin SHORT = {k_pos_before}")
    
    # 4. Получение актуальных цен тикеров
    async with session.get(f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={native_binance}") as resp:
        b_ticker = await resp.json()
        b_ask = float(b_ticker["askPrice"])
        
    async with session.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={native_kucoin}") as resp:
        k_ticker = await resp.json()
        k_bid = float(k_ticker["data"]["bestBidPrice"])
        
    print(f"💵 Цены рынка: Binance Ask = {b_ask} | Kucoin Bid = {k_bid}")
    
    # Расчет лимитных цен со скидкой/надбавкой
    b_dist = float(cfg["trading_risks"]["binance"]["limit_allow_distance"])
    k_dist = float(cfg["trading_risks"]["kucoin"]["limit_allow_distance"])
    
    price_long_limit = b_ask * b_dist
    price_short_limit = k_bid / k_dist
    
    print(f"🎯 Лимитные цены ордеров: Buy @ {price_long_limit:.5f} | Sell @ {price_short_limit:.5f}")
    
    # 5. Одновременная отправка ордеров
    t0 = time.time()
    tasks = [
        b_order.place_order(native_binance, "BUY", test_size_usd, price_long_limit, position_side="LONG"),
        k_order.place_order(native_kucoin, "SELL", test_size_usd, price_short_limit, position_side="SHORT")
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    t_sent = time.time() - t0
    print(f"⚡ Ордера отправлены за {t_sent*1000:.1f} мс. Результаты: {results}")
    
    # 6. Выдержка паузы
    print(f"⏳ Выдержка паузы {execution_pause}с в стакане...")
    await asyncio.sleep(execution_pause)
    
    # 7. (Отмена удалена, так как используется IOC)
    
    # 8. Проверка налитого объема через 1 сек
    await asyncio.sleep(1.0)
    b_pos_after = await b_order.get_exact_position(native_binance, "LONG")
    k_pos_after = await k_order.get_exact_position(native_kucoin, "SHORT")
    
    print(f"📈 Налитая позиция: Binance LONG = {b_pos_after} | Kucoin SHORT = {k_pos_after}")
    
    # 9. Закрытие открытых позиций
    close_tasks = []
    if b_pos_after.get("size", 0.0) > 0:
        b_close_size_usd = b_pos_after["size"] * (b_ask / b_dist)
        print(f"🔻 Закрываем Binance LONG {b_pos_after['size']} по рынку/лимиту...")
        close_tasks.append(b_order.place_order(native_binance, "SELL", b_close_size_usd, b_ask / b_dist, position_side="LONG"))
        
    if k_pos_after.get("size", 0.0) > 0:
        k_close_size_usd = k_pos_after["size"] * (k_bid * k_dist)
        print(f"🔻 Закрываем Kucoin SHORT {k_pos_after['size']} по рынку/лимиту...")
        close_tasks.append(k_order.place_order(native_kucoin, "BUY", k_close_size_usd, k_bid * k_dist, position_side="SHORT"))
        
    if close_tasks:
        close_res = await asyncio.gather(*close_tasks, return_exceptions=True)
        print(f"🏁 Закрывающие ордера отправлены: {close_res}")
        await asyncio.sleep(1.0)
        
    b_pos_final = await b_order.get_exact_position(native_binance, "LONG")
    k_pos_final = await k_order.get_exact_position(native_kucoin, "SHORT")
    print(f"✅ Финальный остаток позиций (должен быть 0.0): Binance = {b_pos_final}, Kucoin = {k_pos_final}")
    print("=== ТЕСТ ЗАВЕРШЕН ===")

if __name__ == "__main__":
    asyncio.run(run_test("DOGE", 6.0, 0.3))
