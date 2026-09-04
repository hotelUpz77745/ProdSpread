import asyncio
import time
import json
import base64
import hmac
import hashlib
import aiohttp
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

def generate_signature(api_secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    message = timestamp + method.upper() + request_path + body
    mac = hmac.new(bytes(api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    return base64.b64encode(mac.digest()).decode('utf-8')

async def main():
    api_key = os.environ.get("BITGET_API_KEY")
    api_secret = os.environ.get("BITGET_API_SECRET")
    api_passphrase = os.environ.get("BITGET_API_PASSPHRASE")

    if not api_key:
        print("API keys not found in .env")
        return

    async with aiohttp.ClientSession() as session:
        print("--- Testing Warmup endpoint: /api/v2/mix/order/place-order ---")
        timestamp = str(int(time.time() * 1000))
        body_dict = {
            "symbol": "INVALIDUSDT",
            "productType": "usdt-futures",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": "1",
            "price": "1",
            "side": "buy",
            "tradeSide": "open",
            "orderType": "limit",
            "force": "ioc"
        }
        body = json.dumps(body_dict)
        signature = generate_signature(api_secret, timestamp, "POST", "/api/v2/mix/order/place-order", body)
        headers = {
            "ACCESS-KEY": api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": api_passphrase,
            "Content-Type": "application/json"
        }
        
        async with session.post("https://api.bitget.com/api/v2/mix/order/place-order", headers=headers, data=body) as resp:
            print(f"Status: {resp.status}")
            print("Headers:")
            for k, v in resp.headers.items():
                print(f"  {k}: {v}")
            data = await resp.text()
            print(f"Response: {data}")

if __name__ == "__main__":
    asyncio.run(main())
