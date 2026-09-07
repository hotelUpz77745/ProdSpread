import asyncio
import json
import os
import time
import aiohttp
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import BitgetOrder

async def main():
    load_dotenv()
    
    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
        
    symbol = "XRPUSDT"
    
    async with aiohttp.ClientSession() as session:
        bitget = BitgetOrder(
            api_key=os.getenv("BITGET_API_KEY"),
            api_secret=os.getenv("BITGET_API_SECRET"),
            api_passphrase=os.getenv("BITGET_API_PASSPHRASE"),
            margin_settings=cfg["margin_settings"]["BITGET"],
            session=session,
            position_stream=None
        )
        
        endpoint = "/api/v2/mix/order/place-order"
        
        print("OPENING WITHOUT TRADESIDE...")
        now = str(int(time.time() * 1000))
        body = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": "10.0",
            "side": "buy",
            "posSide": "long",
            "orderType": "market",
            "clientOid": str(int(time.time()*1000))
        }
        body_str = json.dumps(body)
        sig = bitget._generate_signature(now, "POST", endpoint, body_str)
        headers = {
            'ACCESS-KEY': bitget.api_key,
            'ACCESS-SIGN': sig,
            'ACCESS-TIMESTAMP': now,
            'ACCESS-PASSPHRASE': bitget.api_passphrase,
            'Content-Type': 'application/json'
        }
        async with session.post(f"https://api.bitget.com{endpoint}", headers=headers, data=body_str) as resp:
            print(await resp.json())
            
        await asyncio.sleep(2)
        
        print("\nTRIMMING WITH HOLDSIDE...")
        now3 = str(int(time.time() * 1000))
        body3 = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": "4.0",
            "side": "sell",
            "holdSide": "long",
            "orderType": "market",
            "clientOid": str(int(time.time()*1000))
        }
        body3_str = json.dumps(body3)
        sig3 = bitget._generate_signature(now3, "POST", endpoint, body3_str)
        headers3 = {
            'ACCESS-KEY': bitget.api_key,
            'ACCESS-SIGN': sig3,
            'ACCESS-TIMESTAMP': now3,
            'ACCESS-PASSPHRASE': bitget.api_passphrase,
            'Content-Type': 'application/json'
        }
        async with session.post(f"https://api.bitget.com{endpoint}", headers=headers3, data=body3_str) as resp:
            print(await resp.json())
            
        await asyncio.sleep(2)
        
        print("\nEMERGENCY CLOSE...")
        await bitget._close_position(symbol, "long")

if __name__ == "__main__":
    asyncio.run(main())
