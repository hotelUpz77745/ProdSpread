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
        now = str(int(time.time() * 1000))
        body = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": "10.0",
            "side": "buy",
            "tradeSide": "open",
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
        print("OPENING LONG 10 XRP...")
        async with session.post(f"https://api.bitget.com{endpoint}", headers=headers, data=body_str) as resp:
            print(await resp.json())
            
        await asyncio.sleep(2)
        
        # Test payloads
        payloads = [
            # 1. side=sell, tradeSide=close, no posSide
            {"symbol": symbol, "productType": "USDT-FUTURES", "marginMode": "crossed", "marginCoin": "USDT", "size": "1.0", "side": "sell", "tradeSide": "close", "orderType": "market"},
            # 2. side=sell, tradeSide=close, reduceOnly=True
            {"symbol": symbol, "productType": "USDT-FUTURES", "marginMode": "crossed", "marginCoin": "USDT", "size": "1.0", "side": "sell", "tradeSide": "close", "reduceOnly": True, "orderType": "market"},
            # 3. side=sell, tradeSide=close, reduceOnly="true"
            {"symbol": symbol, "productType": "USDT-FUTURES", "marginMode": "crossed", "marginCoin": "USDT", "size": "1.0", "side": "sell", "tradeSide": "close", "reduceOnly": "true", "orderType": "market"},
            # 4. side=sell, no tradeSide, posSide=long
            {"symbol": symbol, "productType": "USDT-FUTURES", "marginMode": "crossed", "marginCoin": "USDT", "size": "1.0", "side": "sell", "posSide": "long", "orderType": "market"},
            # 5. side=sell, no tradeSide, posSide=long, reduceOnly=True
            {"symbol": symbol, "productType": "USDT-FUTURES", "marginMode": "crossed", "marginCoin": "USDT", "size": "1.0", "side": "sell", "posSide": "long", "reduceOnly": True, "orderType": "market"}
        ]
        
        for p in payloads:
            print(f"\nTESTING PAYLOAD: {json.dumps(p)}")
            p["clientOid"] = str(int(time.time()*1000))
            now_p = str(int(time.time() * 1000))
            p_str = json.dumps(p)
            sig_p = bitget._generate_signature(now_p, "POST", endpoint, p_str)
            h_p = {
                'ACCESS-KEY': bitget.api_key,
                'ACCESS-SIGN': sig_p,
                'ACCESS-TIMESTAMP': now_p,
                'ACCESS-PASSPHRASE': bitget.api_passphrase,
                'Content-Type': 'application/json'
            }
            async with session.post(f"https://api.bitget.com{endpoint}", headers=h_p, data=p_str) as resp:
                print(await resp.json())
            await asyncio.sleep(2)
        
        print("\nEMERGENCY CLOSE...")
        await bitget._close_position(symbol, "long")

if __name__ == "__main__":
    asyncio.run(main())
