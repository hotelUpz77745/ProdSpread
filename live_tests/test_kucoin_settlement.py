# ============================================================
# FILE: live_tests/test_kucoin_settlement.py
# ROLE: Тестирование клиринга и получения сделок Kucoin
# ============================================================
import asyncio
import os
import sys
import time
import json
import base64
import hmac
import hashlib
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

async def main():
    kucoin_key = os.environ.get("KUCOIN_API_KEY")
    kucoin_secret = os.environ.get("KUCOIN_API_SECRET")
    kucoin_passphrase = os.environ.get("KUCOIN_API_PASSPHRASE")

    session = aiohttp.ClientSession()
    endpoint = "/api/v1/history-positions"
    query_str = "?pageSize=10"
    now = str(int(time.time() * 1000))
    str_to_sign = now + "GET" + endpoint + query_str
    sig = base64.b64encode(hmac.new(kucoin_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    passphrase_hmac = hmac.new(kucoin_secret.encode('utf-8'), kucoin_passphrase.encode('utf-8'), hashlib.sha256)
    encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')

    headers = {
        'KC-API-KEY': kucoin_key,
        'KC-API-SIGN': sig,
        'KC-API-TIMESTAMP': now,
        'KC-API-PASSPHRASE': encrypted_passphrase,
        'KC-API-KEY-VERSION': '2'
    }
    url = f"https://api-futures.kucoin.com{endpoint}{query_str}"
    print(f"Requesting: {url}")
    async with session.get(url, headers=headers) as resp:
        print(f"Status: {resp.status}")
        data = await resp.json()
        print(json.dumps(data, indent=2))
        
    await session.close()

if __name__ == "__main__":
    asyncio.run(main())
