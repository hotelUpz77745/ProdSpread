import asyncio
import aiohttp
import json

async def test():
    async with aiohttp.ClientSession() as s:
        async with s.post('https://api-futures.kucoin.com/api/v1/bullet-public') as r:
            td = (await r.json())['data']
            ws_url = f"{td['instanceServers'][0]['endpoint']}?token={td['token']}"
        ws = await s.ws_connect(ws_url)
        await ws.send_json({'id': 1, 'type': 'subscribe', 'topic': '/contractMarket/level2Depth5:FLOCKUSDTM', 'response': True})
        await ws.send_json({'id': 2, 'type': 'subscribe', 'topic': '/contractMarket/level2Depth5:XBTUSDTM', 'response': True})
        await ws.send_json({'id': 3, 'type': 'subscribe', 'topic': '/contractMarket/level2Depth5:ARBUSDTM', 'response': True})
        
        seen = 0
        while seen < 3:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                d = json.loads(msg.data)
                if d.get('data') and 'bids' in d.get('data', {}):
                    print('\nTOPIC:', d.get('topic'))
                    print('  BIDS (top 2):', d['data']['bids'][:2])
                    print('  ASKS (top 2):', d['data']['asks'][:2])
                    seen += 1
        await ws.close()

if __name__ == '__main__':
    asyncio.run(test())
