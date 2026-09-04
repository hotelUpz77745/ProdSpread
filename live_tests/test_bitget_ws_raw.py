# ============================================================
# FILE: live_tests/test_bitget_ws_raw.py
# ROLE: Тестирование приватных и публичных вебсокетов Bitget
# ============================================================
import asyncio
import os
import sys
import time
import json
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.orders import BitgetOrder

class DebugBitgetPositionStream(BitgetPositionStream):
    async def run(self):
        self.session = aiohttp.ClientSession()
        while not self._external_stop and not self.stop_flag():
            ping_task = None
            try:
                self.websocket = await self.session.ws_connect(self.ws_url, heartbeat=None)
                self.is_connected = True
                
                now = str(int(time.time()))
                sign = self._generate_signature(now)
                await self.websocket.send_json({
                    "op": "login",
                    "args": [{"apiKey": self.api_key, "passphrase": self.api_passphrase, "timestamp": now, "sign": sign}]
                })
                auth_resp = await self.websocket.receive_json()
                
                await self.websocket.send_json({
                    "op": "subscribe",
                    "args": [{"instType": "USDT-FUTURES", "channel": "positions", "instId": "default"}]
                })
                
                self.ready = True
                
                async for msg in self.websocket:
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data in ("pong", "ping"): continue
                        try:
                            data = json.loads(msg.data)
                        except:
                            continue
                            
                        # DEBUG PRINT
                        print(f"[RAW WS MSG]: {json.dumps(data)}")
                        
                        if data.get("action") in ("snapshot", "update"):
                            channel = data.get("arg", {}).get("channel")
                            if channel == "positions":
                                for p in data.get("data", []):
                                    sym = p.get("instId", "").replace("_UMCBL", "").strip().upper()
                                    if not sym: continue
                                    size = float(p.get("total", 0.0))
                                    raw_side = (p.get("holdSide") or p.get("posSide") or "").upper()
                                    price = float(p.get("openPriceAvg") or p.get("averageOpenPrice") or p.get("breakEvenPrice") or 0.0)
                                    
                                    if sym not in self.positions:
                                        self.positions[sym] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}
                                    if raw_side in ("LONG", "SHORT"):
                                        self.positions[sym][raw_side] = {"size": size, "price": price}
            except Exception as e:
                pass
            finally:
                self.is_connected = False
                self.ready = False
                await asyncio.sleep(2)

async def main():
    session = aiohttp.ClientSession()
    
    stream = DebugBitgetPositionStream(
        os.environ.get("BITGET_API_KEY", ""), 
        os.environ.get("BITGET_API_SECRET", ""), 
        os.environ.get("BITGET_API_PASSPHRASE", "")
    )
    
    order = BitgetOrder(
        os.environ.get("BITGET_API_KEY", ""), 
        os.environ.get("BITGET_API_SECRET", ""), 
        os.environ.get("BITGET_API_PASSPHRASE", ""), 
        {"margin_type": "cross"}, 
        session, 
        stream
    )
    
    asyncio.create_task(stream.start())
    
    while not stream.ready:
        await asyncio.sleep(0.1)
        
    await order.update_symbol_info()
    
    sym = "XRPUSDT"
    price = 0.5
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=XRPUSDT") as resp:
        data = await resp.json()
        price = float(data["price"])
        
    print(f"\n--- Открываем LONG на XRPUSDT по цене {price} ---")
    await order.place_order(sym, "BUY", 10.0, price, "MARKET", "LONG")
    
    await asyncio.sleep(2.0)
    
    print(f"\n--- Закрываем LONG ---")
    await order.place_order(sym, "SELL", 10.0, price, "MARKET", "LONG")
    
    await asyncio.sleep(2.0)
    
    stream._external_stop = True
    await session.close()

if __name__ == "__main__":
    asyncio.run(main())
