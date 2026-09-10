import asyncio
import os
import aiohttp
import time
import hmac
import hashlib
import base64
import json
import uuid
from dotenv import load_dotenv

load_dotenv()

async def ws_test():
    async with aiohttp.ClientSession() as session:
        api_key = os.environ['KUCOIN_API_KEY']
        api_secret = os.environ['KUCOIN_API_SECRET']
        api_passphrase = os.environ['KUCOIN_API_PASSPHRASE']
        
        now = str(int(time.time() * 1000))
        str_to_sign = now + 'POST' + '/api/v1/bullet-private'
        sig = base64.b64encode(hmac.new(api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
        passphrase_hmac = hmac.new(api_secret.encode('utf-8'), api_passphrase.encode('utf-8'), hashlib.sha256)
        enc_pass = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
        headers = {'KC-API-KEY': api_key, 'KC-API-SIGN': sig, 'KC-API-TIMESTAMP': now, 'KC-API-PASSPHRASE': enc_pass, 'KC-API-KEY-VERSION': '2'}
        
        async with session.post('https://api-futures.kucoin.com/api/v1/bullet-private', headers=headers) as r:
            token_data = (await r.json())['data']
            endpoint = token_data['instanceServers'][0]['endpoint']
            token = token_data['token']
            ws_url = f"{endpoint}?token={token}"
            
        ws = await session.ws_connect(ws_url)
        print('Connected WS!')
        
        # Subscribe to all position topics
        await ws.send_json({'id': 1, 'type': 'subscribe', 'topic': '/contract/position:ARBUSDTM', 'privateChannel': True, 'response': True})
        await ws.send_json({'id': 2, 'type': 'subscribe', 'topic': '/contract/position:all', 'privateChannel': True, 'response': True})
        await ws.send_json({'id': 3, 'type': 'subscribe', 'topic': '/contract/position:*', 'privateChannel': True, 'response': True})
        await ws.send_json({'id': 4, 'type': 'subscribe', 'topic': '/contractAccount/wallet', 'privateChannel': True, 'response': True})
        
        async def listen():
            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    print('\n[WS RECEIVED EVENT]:', msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
                    
        task = asyncio.create_task(listen())
        await asyncio.sleep(2)
        
        def sign(endpoint, method='GET', body_str=''):
            now = str(int(time.time() * 1000))
            str_to_sign = now + method + endpoint + body_str
            sig = base64.b64encode(hmac.new(api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
            passphrase_hmac = hmac.new(api_secret.encode('utf-8'), api_passphrase.encode('utf-8'), hashlib.sha256)
            enc_pass = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
            return {'KC-API-KEY': api_key, 'KC-API-SIGN': sig, 'KC-API-TIMESTAMP': now, 'KC-API-PASSPHRASE': enc_pass, 'KC-API-KEY-VERSION': '2', 'Content-Type': 'application/json'}
            
        print('\n--- OPENING SHORT 10 ARBUSDTM ---')
        order_body = {'clientOid': str(uuid.uuid4()), 'symbol': 'ARBUSDTM', 'side': 'sell', 'type': 'market', 'size': '10', 'leverage': '10', 'marginMode': 'CROSS', 'positionSide': 'SHORT'}
        b_str = json.dumps(order_body)
        h = sign('/api/v1/orders', 'POST', b_str)
        async with session.post('https://api-futures.kucoin.com/api/v1/orders', headers=h, data=b_str) as r:
            print('ORDER SENT:', await r.json())
            
        await asyncio.sleep(2)
        
        print('\n--- CLOSING SHORT 10 ARBUSDTM ---')
        close_body = {'clientOid': str(uuid.uuid4()), 'symbol': 'ARBUSDTM', 'side': 'buy', 'type': 'market', 'size': '10', 'leverage': '10', 'marginMode': 'CROSS', 'positionSide': 'SHORT', 'reduceOnly': True}
        b_str = json.dumps(close_body)
        h = sign('/api/v1/orders', 'POST', b_str)
        async with session.post('https://api-futures.kucoin.com/api/v1/orders', headers=h, data=b_str) as r:
            print('CLOSE SENT:', await r.json())
            
        await asyncio.sleep(2)
        task.cancel()
        await ws.close()

if __name__ == '__main__':
    asyncio.run(ws_test())
