import asyncio
import json
import os
import sys
import aiohttp
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from c_log import log

async def main():
    load_dotenv()
    
    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
        
    session = aiohttp.ClientSession()
    
    # 1. Setup streams
    binance_ws = BinancePositionStream(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", "")
    )
    kucoin_ws = KucoinPositionStream(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
    )
    
    # Start WS in background
    asyncio.create_task(binance_ws.start())
    asyncio.create_task(kucoin_ws.start())
    
    binance_order = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session,
        position_stream=binance_ws
    )
    
    kucoin_order = KucoinOrder(
        api_key=os.environ["KUCOIN_API_KEY"],
        api_secret=os.environ["KUCOIN_API_SECRET"],
        api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
        session=session,
        position_stream=kucoin_ws,
        margin_settings=cfg["margin_settings"]["KUCOIN"]
    )
    
    binance_order.start()
    kucoin_order.start()
    
    print("=== 1. Waiting for specs & WS connections (3 sec) ===")
    await asyncio.sleep(3)
    
    print(f"Binance WS ready: {binance_ws.ready}")
    print(f"Kucoin WS ready: {kucoin_ws.ready}")
    
    # Check Active Positions via REST
    print("\n=== 2. Checking Active Positions via REST ===")
    bin_pos = await binance_order.get_active_positions()
    kuc_pos = await kucoin_order.get_active_positions()
    print(f"Binance active positions (REST): {bin_pos}")
    print(f"Kucoin active positions (REST): {kuc_pos}")
    
    # Check Active Positions via WS
    print("\n=== 3. Checking Positions in WS Cache ===")
    print(f"Binance WS positions map: {binance_ws.positions}")
    print(f"Kucoin WS positions map: {kucoin_ws.positions}")
    
    await session.close()
    await binance_ws.stop()
    await kucoin_ws.stop()

if __name__ == "__main__":
    asyncio.run(main())
