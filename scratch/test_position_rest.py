import asyncio
import os
import sys
import aiohttp
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import BinanceOrder, KucoinOrder

async def main():
    load_dotenv()
    session = aiohttp.ClientSession()
    
    binance = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session
    )
    kucoin = KucoinOrder(
        api_key=os.environ["KUCOIN_API_KEY"],
        api_secret=os.environ["KUCOIN_API_SECRET"],
        api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
        session=session,
        position_stream=None,
        margin_settings={"margin_type": "CROSS", "leverage": 10}
    )
    
    # Test Binance single position
    sym_bin = "BTCUSDT"
    ts = int(asyncio.get_event_loop().time() * 1000)
    print("Testing Binance position check...")
    pos_b = await binance.get_active_positions()
    print("Binance active positions:", pos_b)
    
    print("Testing Kucoin position check...")
    pos_k = await kucoin.get_active_positions()
    print("Kucoin active positions:", pos_k)
    
    # Test Kucoin single symbol endpoint
    endpoint = "/api/v1/position?symbol=BTCUSDTM"
    now = str(int(asyncio.get_event_loop().time() * 1000))
    import time
    now = str(int(time.time() * 1000))
    str_to_sign = now + "GET" + endpoint
    sig = kucoin._generate_signature(str_to_sign)
    import hmac, hashlib, base64
    passphrase_hmac = hmac.new(kucoin.api_secret.encode('utf-8'), kucoin.api_passphrase.encode('utf-8'), hashlib.sha256)
    encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
    headers = {
        'KC-API-KEY': kucoin.api_key,
        'KC-API-SIGN': sig,
        'KC-API-TIMESTAMP': now,
        'KC-API-PASSPHRASE': encrypted_passphrase,
        'KC-API-KEY-VERSION': '2'
    }
    url = f"https://api-futures.kucoin.com{endpoint}"
    async with session.get(url, headers=headers) as resp:
        data = await resp.json()
        print("Kucoin single symbol BTCUSDTM response:", data)

    await session.close()

if __name__ == "__main__":
    asyncio.run(main())
