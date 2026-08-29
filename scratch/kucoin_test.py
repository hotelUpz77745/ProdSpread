import asyncio
import os
import sys
import aiohttp
import time
import base64
import hmac
import hashlib
import json
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import KucoinOrder

async def test():
    load_dotenv()
    
    async with aiohttp.ClientSession() as session:
        api_key = os.getenv("KUCOIN_API_KEY")
        api_secret = os.getenv("KUCOIN_API_SECRET")
        api_passphrase = os.getenv("KUCOIN_API_PASSPHRASE")
        
        kucoin = KucoinOrder(api_key, api_secret, api_passphrase, session, None, margin_settings={})
        symbol = "XRPUSDTM"
        
        body = {"symbol": symbol, "leverage": "11"}
        body_str = json.dumps(body)
        now = str(int(time.time() * 1000))
        
        endpoints = [
            "/api/v2/changeIsolatedUserLeverage",
            "/api/v2/position/changeIsolatedUserLeverage",
            "/api/v1/position/changeIsolatedUserLeverage",
            "/api/v2/position/changeLeverage",
            "/api/v1/changeIsolatedUserLeverage"
        ]
        
        for ep in endpoints:
            str_to_sign = now + "POST" + ep + body_str
            signature = kucoin._generate_signature(str_to_sign)
            
            passphrase_hmac = hmac.new(kucoin.api_secret.encode('utf-8'), kucoin.api_passphrase.encode('utf-8'), hashlib.sha256)
            encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
            
            headers = {
                'KC-API-KEY': kucoin.api_key,
                'KC-API-SIGN': signature,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': encrypted_passphrase,
                'KC-API-KEY-VERSION': '2',
                'Content-Type': 'application/json'
            }
            
            url = f"https://api-futures.kucoin.com{ep}"
            print(f"Trying {url}...")
            async with session.post(url, headers=headers, data=body_str) as resp:
                data = await resp.json()
                print(f"Response: {data}")

if __name__ == "__main__":
    asyncio.run(test())
