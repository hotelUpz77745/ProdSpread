# ============================================================
# FILE: API/BITGET/ws_private_bitget.py
# ROLE: Приватный WebSocket стрим позиций и ордеров Bitget USDT-M Futures.
# ============================================================

import asyncio
import aiohttp
import json
import time
import hmac
import base64
import contextlib
from typing import Optional, Callable, Dict, Any
from c_log import log

class BitgetPositionStream:
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, stop_flag: Optional[Callable[[], bool]] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.stop_flag = stop_flag or (lambda: False)
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.websocket: Optional[aiohttp.ClientWebSocketResponse] = None
        
        self.ws_url = "wss://ws.bitget.com/v2/ws/private"
        self.ready = False
        self.is_connected = False
        self._external_stop = False
        self.positions: Dict[str, Dict[str, Dict[str, float]]] = {}

    async def stop(self):
        self._external_stop = True
        self.ready = False
        if self.websocket:
            await self.websocket.close()
        if self.session:
            await self.session.close()

    async def start(self):
        await self.run()
            
    def _generate_signature(self, timestamp: str) -> str:
        message = timestamp + "GET" + "/user/verify"
        mac = hmac.new(bytes(self.api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode('utf-8')

    async def _ws_ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        while not self._external_stop and not self.stop_flag():
            await asyncio.sleep(25.0)
            if self._external_stop or self.stop_flag():
                break
            with contextlib.suppress(Exception):
                await ws.send_str("ping")

    def get_position(self, symbol: str, side: str) -> dict:
        sym = symbol.replace("_UMCBL", "").strip().upper()
        if not sym or sym not in self.positions:
            return {"size": 0.0, "price": 0.0}
        return self.positions[sym].get(side.upper(), {"size": 0.0, "price": 0.0})

    async def run(self):
        self.session = aiohttp.ClientSession()
        while not self._external_stop and not self.stop_flag():
            ping_task = None
            try:
                self.websocket = await self.session.ws_connect(self.ws_url, heartbeat=None)
                self.is_connected = True
                
                # Auth
                now = str(int(time.time()))
                sign = self._generate_signature(now)
                login_msg = {
                    "op": "login",
                    "args": [{
                        "apiKey": self.api_key,
                        "passphrase": self.api_passphrase,
                        "timestamp": now,
                        "sign": sign
                    }]
                }
                await self.websocket.send_json(login_msg)
                
                # Wait for login success
                auth_resp = await self.websocket.receive_json()
                if auth_resp.get("event") != "login" or auth_resp.get("code") != 0:
                    log(f"[BITGET WS_PRIVATE] Auth failed: {auth_resp}", level="ERROR")
                    await asyncio.sleep(5)
                    continue
                    
                log("[BITGET WS_PRIVATE] Connected and Authenticated", level="INFO")
                
                # Subscribe to positions and orders
                sub_msg = {
                    "op": "subscribe",
                    "args": [
                        {"instType": "USDT-FUTURES", "channel": "positions", "instId": "default"},
                        {"instType": "USDT-FUTURES", "channel": "orders", "instId": "default"}
                    ]
                }
                await self.websocket.send_json(sub_msg)
                
                self.ready = True
                ping_task = asyncio.create_task(self._ws_ping_loop(self.websocket))
                
                async for msg in self.websocket:
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data in ("pong", "ping"):
                            continue
                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue
                            
                        if data.get("action") in ("snapshot", "update"):
                            channel = data.get("arg", {}).get("channel")
                            if channel == "positions":
                                for p in data.get("data", []):
                                    sym = p.get("instId", "").replace("_UMCBL", "").strip().upper()
                                    if not sym:
                                        continue
                                    size = float(p.get("total", 0.0))
                                    raw_side = (p.get("holdSide") or p.get("posSide") or "").upper()
                                    price = float(p.get("openPriceAvg") or p.get("averageOpenPrice") or p.get("breakEvenPrice") or 0.0)
                                    
                                    if sym not in self.positions:
                                        self.positions[sym] = {
                                            "LONG": {"size": 0.0, "price": 0.0},
                                            "SHORT": {"size": 0.0, "price": 0.0}
                                        }
                                    if raw_side in ("LONG", "SHORT"):
                                        self.positions[sym][raw_side] = {"size": size, "price": price}
                                        
            except Exception as e:
                log(f"[BITGET WS_PRIVATE] Error: {e}", level="ERROR")
            finally:
                if ping_task:
                    ping_task.cancel()
                    with contextlib.suppress(Exception):
                        await ping_task
                self.is_connected = False
                self.ready = False
                await asyncio.sleep(2)
