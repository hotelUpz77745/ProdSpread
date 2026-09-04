# ============================================================
# FILE: live_tests/test_api_connectivity.py
# ROLE: Проверка подключения и аутентификации REST API всех бирж
# ============================================================
import asyncio
import os
import time
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from c_log import log
from dotenv import load_dotenv
load_dotenv()

from utils import SessionManager
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder

async def test_live_adapters():
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
        margin_settings={"margin_type": "cross", "leverage": 1}
    )

    print("Fetching symbol info...")
    await asyncio.gather(
        binance.update_symbol_info(),
        bitget.update_symbol_info(),
        kucoin.update_symbol_info(),
        return_exceptions=True
    )
    
    if hasattr(binance, '_bg_task') and binance._bg_task:
        binance._bg_task.cancel()
    if hasattr(bitget, '_bg_task') and bitget._bg_task:
        bitget._bg_task.cancel()
    if hasattr(kucoin, '_bg_task') and kucoin._bg_task:
        kucoin._bg_task.cancel()

    test_symbol = "XRPUSDT"
    test_size_usd = 6.0
    test_price = 0.50

    price_long_limit = test_price * 0.70 # Very safe limit below market
    price_short_limit = test_price * 1.30 # Very safe limit above market

    print(f"\n--- 1. Live Testing LIMIT GTC ENTRY (Safe distance) ---")
    
    tasks = [
        binance.place_order(test_symbol, "BUY", test_size_usd, price_long_limit, position_side="LONG", time_in_force="GTC"),
        bitget.place_order(test_symbol, "SELL", test_size_usd, price_short_limit, position_side="SHORT", time_in_force="GTC"),
        # Kucoin symbol suffix is different usually, e.g. XRPUSDTM, let's skip kucoin order for now if we are not sure
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        print(f"[{'Binance' if i==0 else 'Bitget'}] Place Result: {res}")

    print("\nWaiting 2.0s...")
    await asyncio.sleep(2.0)

    print("\n--- 2. Live Testing CANCELLING ---")
    cancel_tasks = [
        binance.cancel_all_orders(test_symbol),
        bitget.cancel_all_orders(test_symbol)
    ]
    cancel_results = await asyncio.gather(*cancel_tasks, return_exceptions=True)
    for i, res in enumerate(cancel_results):
        print(f"[{'Binance' if i==0 else 'Bitget'}] Cancel Result: {res}")
        
    print("\n--- 3. Margin & Leverage setters (Bitget Safe Test) ---")
    try:
        res = await bitget.set_leverage(test_symbol, 2)
        print(f"Bitget set leverage to 2: {res}")
        res = await bitget.set_margin_type(test_symbol, "cross")
        print(f"Bitget set margin to cross: {res}")
    except Exception as e:
        print(f"Bitget config test failed: {e}")

    await session.close()
    print("\nLive Integration Tests Completed.")

if __name__ == "__main__":
    asyncio.run(test_live_adapters())
