import asyncio
import os
import aiohttp
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder

async def main():
    load_dotenv()
    
    binance_api = os.getenv("BINANCE_API_KEY")
    binance_sec = os.getenv("BINANCE_API_SECRET")
    
    kucoin_api = os.getenv("KUCOIN_API_KEY")
    kucoin_sec = os.getenv("KUCOIN_API_SECRET")
    kucoin_pass = os.getenv("KUCOIN_API_PASSPHRASE")
    
    async with aiohttp.ClientSession() as session:
        print("Initializing orders...")
        binance = BinanceOrder(binance_api, binance_sec, session)
        kucoin = KucoinOrder(kucoin_api, kucoin_sec, kucoin_pass, session)
        
        # Wait a bit for the background fetch specs task to complete
        print("Waiting for specs to load (3 seconds)...")
        await asyncio.sleep(3)
        
        print("Binance specs loaded:", binance.symbol_info is not None)
        print("Kucoin specs loaded:", kucoin.symbol_info is not None)
        
        # Test warmup
        print("\n--- Testing Warmup ---")
        await binance.warmup()
        await kucoin.warmup()
        print("Warmup finished.")
        
        symbol = "XRPUSDT"
        kucoin_symbol = "XRPUSDTM"
        size_usd = 6.0
        
        # Fetch current price
        print(f"\n--- Fetching current price for {symbol} ---")
        async with session.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}") as resp:
            data = await resp.json()
            price = float(data["price"])
            print(f"Current price: {price}")
        
        print(f"\n--- Testing Binance MARKET OPEN (6$) ---")
        try:
            res = await binance.place_order(symbol, "BUY", size_usd, price=price, order_type="MARKET", position_side="LONG")
            print("Binance Open Result:", res)
        except Exception as e:
            print("Binance Open Error:", e)
            
        print(f"\n--- Testing Kucoin MARKET OPEN (6$) ---")
        try:
            res = await kucoin.place_order(kucoin_symbol, "BUY", size_usd, price=price, order_type="MARKET")
            print("Kucoin Open Result:", res)
        except Exception as e:
            print("Kucoin Open Error:", e)
            
        print("\nWaiting 2 seconds before closing...")
        await asyncio.sleep(2)
        
        print(f"\n--- Testing Binance MARKET CLOSE (6$) ---")
        try:
            res = await binance.place_order(symbol, "SELL", size_usd, price=price, order_type="MARKET", position_side="LONG")
            print("Binance Close Result:", res)
        except Exception as e:
            print("Binance Close Error:", e)
            
        print(f"\n--- Testing Kucoin MARKET CLOSE (6$) ---")
        try:
            res = await kucoin.place_order(kucoin_symbol, "SELL", size_usd, price=price, order_type="MARKET")
            print("Kucoin Close Result:", res)
        except Exception as e:
            print("Kucoin Close Error:", e)
            
        print("\n--- Testing Cancel All Orders ---")
        await binance.cancel_all_orders(symbol)
        await kucoin.cancel_all_orders(kucoin_symbol)
        
        print("\nTest finished.")
        
        # Need to cancel background tasks cleanly for the script to exit
        binance._bg_task.cancel()
        binance._keepalive_task.cancel()
        kucoin._bg_task.cancel()
        kucoin._keepalive_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
