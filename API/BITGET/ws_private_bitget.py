import asyncio
import aiohttp
import json
import time
import hmac
import base64
import hashlib
from typing import Optional, Callable, Dict, Any
from CORE.utils import log

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
        self.positions: Dict[str, Dict[str, Any]] = {}

    async def stop(self):
        self._external_stop = True
        self.ready = False
        if self.websocket:
            await self.websocket.close()
        if self.session:
            await self.session.close()
            
    def _generate_signature(self, timestamp: str) -> str:
        message = timestamp + "GET" + "/user/verify"
        mac = hmac.new(bytes(self.api_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode('utf-8')

    async def run(self):
        self.session = aiohttp.ClientSession()
        while not self._external_stop and not self.stop_flag():
            try:
                self.websocket = await self.session.ws_connect(self.ws_url, heartbeat=30.0)
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
                if auth_resp.get("event") != "login":
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
                
                async for msg in self.websocket:
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                        
                    data = json.loads(msg.data)
                    if data.get("action") == "snapshot" or data.get("action") == "update":
                        if data.get("arg", {}).get("channel") == "positions":
                            for p in data.get("data", []):
                                sym = p.get("instId", "").replace("_UMCBL", "")
                                size = float(p.get("total", 0))
                                side = p.get("holdSide", "")
                                if sym not in self.positions:
                                    self.positions[sym] = {"long": {"size": 0.0}, "short": {"size": 0.0}}
                                if side == "long":
                                    self.positions[sym]["long"]["size"] = size
                                elif side == "short":
                                    self.positions[sym]["short"]["size"] = size
                                    
            except Exception as e:
                log(f"[BITGET WS_PRIVATE] Error: {e}", level="ERROR")
            finally:
                self.is_connected = False
                self.ready = False
                await asyncio.sleep(2)
