import asyncio
import os
import aiohttp
from dotenv import load_dotenv

from API.orders import BitgetOrder
from API.BITGET.ws_private_bitget import BitgetPositionStream

async def main():
    load_dotenv()
    api_key = os.environ.get("BITGET_API_KEY", "")
    api_secret = os.environ.get("BITGET_API_SECRET", "")
    api_passphrase = os.environ.get("BITGET_API_PASSPHRASE", "")
    
    if not api_key or api_key == "dummy":
        print("Please set real BITGET_API_KEY, BITGET_API_SECRET, and BITGET_API_PASSPHRASE in .env")
        return
        
    print("Testing Bitget integration...")
    
    async with aiohttp.ClientSession() as session:
        # Test WS
        stream = BitgetPositionStream(api_key, api_secret, api_passphrase)
        ws_task = asyncio.create_task(stream.run())
        
        await asyncio.sleep(3)
        if not stream.is_connected:
            print("Failed to connect to Bitget WS")
            ws_task.cancel()
            return
            
        print("WS Connected and Authenticated!")
        
        # Test REST
        order = BitgetOrder(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            margin_settings={"margin_type": "crossed", "leverage": 10},
            session=session
        )
        
        print("Fetching symbol info...")
        await order.update_symbol_info()
        print(f"Loaded {len(order.symbol_info)} symbols.")
        
        symbol = "XRPUSDT" # Assuming XRPUSDT is cheap enough to test with a small order, or adjust as needed
        size_usd = 6.0
        
        # We need a reference price. Let's fetch it from public API for testing
        async with session.get(f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol}&productType=USDT-FUTURES") as resp:
            data = await resp.json()
            if isinstance(data.get('data'), list):
                price = float(data['data'][0]['lastPr'])
            else:
                price = float(data['data']['lastPr'])
            
        print(f"Current price of {symbol} is {price}")
        
        print(f"Placing a test IOC order for {size_usd} USD...")
        
        try:
            order.check_order_size(symbol, size_usd, price)
            result = await order.place_order(symbol, "buy", size_usd, price, "LIMIT")
            print("Order Placement Result:", result)
        except Exception as e:
            print("Order Placement Error:", e)
            
        await asyncio.sleep(5)
        print("WS Positions:", stream.positions.get(symbol))
        
        await stream.stop()
        ws_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
