# ============================================================
# FILE: test_pipeline.py
# ROLE: Скрипт тестирования адаптеров (Binance, Bitget, Kucoin) и логики FSM (открытие, отмена, экстренный выход)
# ============================================================

import asyncio
import os
import time
from dotenv import load_dotenv
from c_log import log

load_dotenv()

from utils import SessionManager
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder

async def test_adapters():
    session = await SessionManager().get_session()
    
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

    # 1. Update specs
    await bitget.update_symbol_info()
    
    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
        data = await resp.json()
        binance.symbol_info = data.get("symbols", [])
        
    async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
        data = await resp.json()
        kucoin.symbol_info = data.get("data", [])

    test_symbol = "XRPUSDT"
    test_size_usd = 20.0
    
    # We must fetch actual price to pass limits
    import aiohttp
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=XRPUSDT") as resp:
        data = await resp.json()
        test_price = float(data["price"])

    price_long_limit = test_price * 0.95 # Safe limit below market
    price_short_limit = test_price * 1.05 # Safe limit above market

    print(f"--- Тестирование входа (LIMIT GTC, EXECUTION_PAUSE=0.3) ---")
    
    tasks = []
    tasks.append(binance.place_order(test_symbol, "BUY", test_size_usd, price_long_limit, position_side="LONG", time_in_force="GTC"))
    tasks.append(bitget.place_order(test_symbol, "SELL", test_size_usd, price_short_limit, position_side="SHORT", time_in_force="GTC"))
    # Kucoin usually uses XRPUSDTM for futures, but orders.py might handle suffix
    # Let's test Kucoin as BUY just for testing limit
    tasks.append(kucoin.place_order(f"{test_symbol}M", "BUY", test_size_usd, price_long_limit, position_side="LONG", time_in_force="GTC"))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        print(f"Leg {i} Place Result: {res}")

    print("Ожидание 0.3с (как в EXECUTION_PAUSE)...")
    await asyncio.sleep(0.3)

    print("--- Тестирование CANCELLING ---")
    cancel_tasks = [
        binance.cancel_all_orders(test_symbol),
        bitget.cancel_all_orders(test_symbol),
        kucoin.cancel_all_orders(f"{test_symbol}M")
    ]
    cancel_results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
    for i, res in enumerate(cancel_results):
        print(f"Leg {i} Cancel Result: {res}")

    print("--- Тестирование EMERGENCY UNWIND (Market close) ---")
    unwind_tasks = [
        binance.place_order(test_symbol, "SELL", test_size_usd, test_price, order_type="MARKET", position_side="LONG"),
        bitget.place_order(test_symbol, "BUY", test_size_usd, test_price, order_type="MARKET", position_side="SHORT"),
        kucoin.place_order(f"{test_symbol}M", "SELL", test_size_usd, test_price, order_type="MARKET", position_side="LONG")
    ]
    unwind_results = await asyncio.gather(*unwind_tasks, return_exceptions=True)
    for i, res in enumerate(unwind_results):
        print(f"Leg {i} Unwind Result: {res}")

    await session.close()
    print("Тест завершен.")

if __name__ == "__main__":
    asyncio.run(test_adapters())
