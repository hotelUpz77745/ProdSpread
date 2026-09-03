# Debug Bitget close: print exact request body
import asyncio, aiohttp, os, json, time, sys, hmac, hashlib, base64, uuid
from dotenv import load_dotenv
load_dotenv()
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

async def main():
    session = aiohttp.ClientSession()
    
    api_key = os.environ["BITGET_API_KEY"]
    api_secret = os.environ["BITGET_API_SECRET"]
    api_passphrase = os.environ["BITGET_API_PASSPHRASE"]
    
    def sign(timestamp, method, path, body=""):
        message = timestamp + method.upper() + path + body
        mac = hmac.new(bytes(api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode('utf-8')
    
    # First check position
    now = str(int(time.time() * 1000))
    ep = "/api/v2/mix/position/single-position?productType=USDT-FUTURES&symbol=4USDT&marginCoin=USDT"
    sig = sign(now, "GET", ep)
    headers = {
        'ACCESS-KEY': api_key, 'ACCESS-SIGN': sig,
        'ACCESS-TIMESTAMP': now, 'ACCESS-PASSPHRASE': api_passphrase
    }
    async with session.get(f"https://api.bitget.com{ep}", headers=headers) as resp:
        pos_data = await resp.json()
        print(f"Position RAW response:\n{json.dumps(pos_data, indent=2)}")
    
    # Now try to close with explicit holdSide
    endpoint = "/api/v2/mix/order/place-order"
    now = str(int(time.time() * 1000))
    
    body = {
        "symbol": "4USDT",
        "productType": "USDT-FUTURES",
        "marginMode": "crossed",
        "marginCoin": "USDT",
        "size": "372",
        "side": "buy",
        "tradeSide": "close",
        "holdSide": "short",
        "orderType": "market",
        "clientOid": str(uuid.uuid4())
    }
    body_str = json.dumps(body)
    print(f"\nClose request body:\n{body_str}")
    
    sig = sign(now, "POST", endpoint, body_str)
    headers = {
        'ACCESS-KEY': api_key, 'ACCESS-SIGN': sig,
        'ACCESS-TIMESTAMP': now, 'ACCESS-PASSPHRASE': api_passphrase,
        'Content-Type': 'application/json'
    }
    
    async with session.post(f"https://api.bitget.com{endpoint}", headers=headers, data=body_str) as resp:
        data = await resp.json()
        print(f"\nClose response:\n{json.dumps(data, indent=2)}")
    
    # Also try the flash-close endpoint
    endpoint2 = "/api/v2/mix/order/close-positions"
    now2 = str(int(time.time() * 1000))
    body2 = {
        "symbol": "4USDT",
        "productType": "USDT-FUTURES",
        "holdSide": "short"
    }
    body2_str = json.dumps(body2)
    print(f"\nFlash-close request body:\n{body2_str}")
    
    sig2 = sign(now2, "POST", endpoint2, body2_str)
    headers2 = {
        'ACCESS-KEY': api_key, 'ACCESS-SIGN': sig2,
        'ACCESS-TIMESTAMP': now2, 'ACCESS-PASSPHRASE': api_passphrase,
        'Content-Type': 'application/json'
    }
    
    async with session.post(f"https://api.bitget.com{endpoint2}", headers=headers2, data=body2_str) as resp2:
        data2 = await resp2.json()
        print(f"\nFlash-close response:\n{json.dumps(data2, indent=2)}")
    
    await asyncio.sleep(1)
    
    # Verify
    now3 = str(int(time.time() * 1000))
    sig3 = sign(now3, "GET", ep)
    headers3 = {
        'ACCESS-KEY': api_key, 'ACCESS-SIGN': sig3,
        'ACCESS-TIMESTAMP': now3, 'ACCESS-PASSPHRASE': api_passphrase
    }
    async with session.get(f"https://api.bitget.com{ep}", headers=headers3) as resp3:
        pos_after = await resp3.json()
        print(f"\nPosition after close:\n{json.dumps(pos_after, indent=2)}")
    
    await session.close()

asyncio.run(main())
