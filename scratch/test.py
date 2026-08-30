import asyncio
import json
from dotenv import load_dotenv
import os
import aiohttp
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder
from c_log import log

async def main():
    load_dotenv()
    
    with open("cfg.json", "r") as f:
        cfg = json.load(f)
        
    session = aiohttp.ClientSession()
    
    binance = BinanceOrder(
        api_key=os.getenv("BINANCE_API_KEY"),
        api_secret=os.getenv("BINANCE_API_SECRET"),
        session=session
    )
    
    kucoin = KucoinOrder(
        api_key=os.getenv("KUCOIN_API_KEY"),
        api_secret=os.getenv("KUCOIN_API_SECRET"),
        api_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"),
        session=session,
        position_stream=None,
        margin_settings={"margin_type": "CROSS", "leverage": 10}
    )
    
    binance.start()
    kucoin.start()
    
    print("Ждем загрузки спецификаций (2 сек)...")
    await asyncio.sleep(2)
    
    # 1. Сначала настроим маржу
    print("Настраиваем маржу на Binance (BTRUSDT)...")
    await binance.set_margin_type("BTRUSDT", "CROSSED")
    await binance.set_leverage("BTRUSDT", 10)
    print("Настраиваем маржу на Kucoin (BTRUSDTM)...")
    await kucoin.set_margin_type("BTRUSDTM", "CROSS", 10)
    await kucoin.set_leverage("BTRUSDTM", 10, "CROSS")
    
    print("\n--- Открываем тестовый LONG на Binance (объем 10$) ---")
    try:
        await binance.place_order("BTRUSDT", "BUY", 10.0, 0.21, order_type="MARKET", position_side="LONG")
        print("Binance LONG order submitted.")
    except Exception as e:
        print(f"Binance LONG error: {e}")
        
    print("\n--- Открываем тестовый SHORT на Kucoin (объем 10$) ---")
    try:
        await kucoin.place_order("BTRUSDTM", "SELL", 10.0, 0.21, order_type="MARKET", position_side="SHORT")
        print("Kucoin SHORT order submitted.")
    except Exception as e:
        print(f"Kucoin SHORT error: {e}")
        
    print("\nЖдем 3 секунды перед закрытием...")
    await asyncio.sleep(3)
        
    print("\n--- Закрываем тестовый LONG на Binance ---")
    try:
        await binance.place_order("BTRUSDT", "SELL", 10.0, 0.21, order_type="MARKET", position_side="LONG")
        print("Binance Close LONG order submitted.")
    except Exception as e:
        print(f"Binance Close LONG error: {e}")
        
    print("\n--- Закрываем тестовый SHORT на Kucoin ---")
    try:
        await kucoin.place_order("BTRUSDTM", "BUY", 10.0, 0.21, order_type="MARKET", position_side="SHORT")
        print("Kucoin Close SHORT order submitted.")
    except Exception as e:
        print(f"Kucoin Close SHORT error: {e}")
        
    await session.close()
    print("\nТест завершен.")

if __name__ == "__main__":
    asyncio.run(main())
