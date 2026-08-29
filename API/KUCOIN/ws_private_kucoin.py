# ==============================================================================
# Path: API/KUCOIN/ws_private_kucoin.py
# Role: Connection to User Data Stream for Kucoin Futures and Position parsing
# ==============================================================================

import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
import base64
from typing import Optional, Callable, Dict, Any
from c_log import log

class KucoinPositionStream:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        stop_flag: Optional[Callable[[], bool]] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.stop_flag = stop_flag or (lambda: False)

        self.session: Optional[aiohttp.ClientSession] = None
        self.websocket: Optional[aiohttp.ClientWebSocketResponse] = None

        self.ws_url: Optional[str] = None
        self.ping_interval = 18000
        self.token: Optional[str] = None
        self.ready = False
        self.is_connected = False
        self._external_stop = False
        
        # Track positions by symbol
        self.positions: Dict[str, Dict[str, Any]] = {}

    def stop(self):
        self._external_stop = True
        self.ready = False
        log("KucoinPositionStream: stop requested", level="INFO")

    async def _create_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession()

    def _generate_signature(self, endpoint: str, method: str, str_to_sign: str) -> str:
        h = hmac.new(self.api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(h.digest()).decode('utf-8')

    async def _get_ws_token(self) -> bool:
        # Simplified token fetching - in reality, requires proper auth headers for bullet-private
        try:
            now = str(int(time.time() * 1000))
            str_to_sign = now + 'POST' + '/api/v1/bullet-private'
            signature = self._generate_signature('/api/v1/bullet-private', 'POST', str_to_sign)
            
            headers = {
                'KC-API-KEY': self.api_key,
                'KC-API-SIGN': signature,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': self.api_passphrase,
                'KC-API-KEY-VERSION': '2'
            }
            
            async with self.session.post('https://api-futures.kucoin.com/api/v1/bullet-private', headers=headers) as resp:
                data = await resp.json()
                if data.get('code') == '200000':
                    token_data = data['data']['token']
                    endpoint = data['data']['instanceServers'][0]['endpoint']
                    self.ping_interval = data['data']['instanceServers'][0]['pingInterval']
                    self.ws_url = f"{endpoint}?token={token_data}"
                    return True
                else:
                    log(f"[KUCOIN WS_PRIVATE] Failed to get token: {data}", level="ERROR")
                    return False
        except Exception as e:
            log(f"[KUCOIN WS_PRIVATE] Token fetch error: {e}", level="ERROR")
            return False

    async def _connect(self) -> bool:
        try:
            self.websocket = await self.session.ws_connect(self.ws_url)
            self.is_connected = True
            log(f"[KUCOIN WS_PRIVATE] Connected successfully.", level="INFO")
            
            # Subscribe to position topic
            sub_msg = {
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": "/contract/position:*",
                "privateChannel": True,
                "response": True
            }
            await self.websocket.send_json(sub_msg)
            
            return True
        except Exception as e:
            log(f"[KUCOIN WS_PRIVATE] Connect failed: {e}", level="ERROR")
            return False

    async def _disconnect(self):
        self.is_connected = False
        self.ready = False
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass
            self.websocket = None
        log("KucoinPositionStream: WS disconnected", level="INFO")

    async def _ping_loop(self):
        while self.is_connected and not self._external_stop:
            await asyncio.sleep(self.ping_interval / 1000 * 0.8)
            if self.websocket:
                try:
                    await self.websocket.send_json({
                        "id": int(time.time() * 1000),
                        "type": "ping"
                    })
                except Exception:
                    pass

    async def _handle_messages(self):
        asyncio.create_task(self._ping_loop())
        while not self._external_stop and not self.stop_flag():
            try:
                msg = await asyncio.wait_for(self.websocket.receive(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("ws_closed")

            if msg.type != aiohttp.WSMsgType.TEXT:
                continue

            try:
                data = json.loads(msg.data)
            except Exception:
                continue

            topic = data.get("topic", "")
            if topic.startswith("/contract/position:"):
                subj = data.get("subject")
                if subj == "position.change":
                    pdata = data.get("data", {})
                    symbol = pdata.get("symbol")
                    pos_amt = float(pdata.get("currentQty", 0))
                    ep_raw = float(pdata.get("avgEntryPrice", 0))
                    
                    if symbol:
                        if symbol not in self.positions:
                            self.positions[symbol] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}
                        
                        side = "LONG" if pos_amt > 0 else "SHORT"
                        self.positions[symbol][side] = {
                            "size": abs(pos_amt),
                            "price": ep_raw
                        }

    async def start(self):
        self._external_stop = False
        try:
            while not self._external_stop and not self.stop_flag():
                self.session = await self._create_session()
                try:
                    if not await self._get_ws_token():
                        await asyncio.sleep(5.0)
                        continue

                    if not await self._connect():
                        raise RuntimeError("ws_connect_failed")

                    self.ready = True
                    await self._handle_messages()

                except asyncio.CancelledError:
                    raise
                except RuntimeError as e:
                    if str(e) == "ws_closed":
                        log(f"[KUCOIN WS_PRIVATE] Stream disconnected, reconnecting...", level="WARNING")
                except Exception as e:
                    log(f"[KUCOIN WS_PRIVATE] cycle failed: {e}", level="ERROR")
                finally:
                    self.ready = False
                    await self._disconnect()
                    if self.session:
                        try:
                            await self.session.close()
                        except Exception:
                            pass
                        self.session = None

                    if not self._external_stop:
                        await asyncio.sleep(1.0)
        finally:
            self.ready = False

    def get_position(self, symbol: str, side: str) -> dict:
        sym = symbol.strip().upper()
        if not sym or sym not in self.positions:
            return {"size": 0.0, "price": 0.0}
        return self.positions[sym].get(side.upper(), {"size": 0.0, "price": 0.0})
