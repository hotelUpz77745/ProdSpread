# ==============================================================================
# FILE: API/KUCOIN/ws_private_kucoin.py
# ROLE: Connection to User Data Stream for Kucoin Futures and Position parsing
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
        self.last_close_prices: Dict[str, float] = {}

    def get_last_close_price(self, symbol: str) -> float:
        sym = symbol.strip().upper()
        return self.last_close_prices.get(sym, 0.0)

    async def stop(self):
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
            
            passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
            encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
            
            headers = {
                'KC-API-KEY': self.api_key,
                'KC-API-SIGN': signature,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': encrypted_passphrase,
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
            
            # 1. Subscribe to tradeOrders (global private fills stream)
            sub_msg = {
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": "/contractMarket/tradeOrders",
                "privateChannel": True,
                "response": True
            }
            await self.websocket.send_json(sub_msg)

            # 2. Subscribe to positionAll (real-time position state and zero-balance events)
            sub_pos = {
                "id": int(time.time() * 1000) + 1,
                "type": "subscribe",
                "topic": "/contract/positionAll",
                "privateChannel": True,
                "response": True
            }
            await self.websocket.send_json(sub_pos)
            
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
        last_msg_time = time.time()
        while not self._external_stop and not self.stop_flag():
            try:
                msg = await asyncio.wait_for(self.websocket.receive(), timeout=5.0)
                last_msg_time = time.time()
            except asyncio.TimeoutError:
                if time.time() - last_msg_time > 40.0:
                    log("[KUCOIN WS_PRIVATE] Watchdog timeout: no messages for 40s. Reconnecting.", level="WARNING")
                    raise RuntimeError("ws_closed")
                continue

            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("ws_closed")

            if msg.type != aiohttp.WSMsgType.TEXT:
                continue

            try:
                data = json.loads(msg.data)
            except Exception:
                continue

            msg_type = data.get("type")
            if msg_type == "error":
                log(f"[KUCOIN WS_PRIVATE] Error from server: {data.get('data')}", level="ERROR")
                continue

            topic = data.get("topic", "")
            if topic == "/contractMarket/tradeOrders":
                pdata = data.get("data", {})
                symbol = pdata.get("symbol")
                status = pdata.get("status")
                filled_size = float(pdata.get("filledSize", 0))
                match_price = float(pdata.get("matchPrice", 0)) or float(pdata.get("price", 0))
                pos_side = (pdata.get("positionSide") or ("LONG" if pdata.get("side") == "buy" else "SHORT")).upper()
                
                if symbol:
                    if match_price > 0:
                        self.last_close_prices[symbol] = match_price
                    if symbol not in self.positions:
                        self.positions[symbol] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}
                    if status in ("match", "done", "filled") and filled_size > 0:
                        curr_p = self.positions[symbol][pos_side]
                        if curr_p.get("size", 0.0) == 0.0:
                            self.positions[symbol][pos_side] = {
                                "size": filled_size,
                                "price": match_price if match_price > 0 else curr_p.get("price", 0.0)
                            }

            elif topic.startswith("/contract/position"):
                pdata = data.get("data", {})
                symbol = pdata.get("symbol")
                pos_amt = float(pdata.get("currentQty", 0))
                ep_raw = float(pdata.get("avgEntryPrice", 0))
                pos_side = (pdata.get("positionSide") or pdata.get("posSide") or "").upper()
                
                if symbol:
                    if symbol not in self.positions:
                        self.positions[symbol] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}
                    
                    if pos_amt == 0.0:
                        self.positions[symbol]["LONG"] = {"size": 0.0, "price": 0.0}
                        self.positions[symbol]["SHORT"] = {"size": 0.0, "price": 0.0}
                    elif pos_side == "LONG":
                        self.positions[symbol]["LONG"] = {"size": abs(float(pos_amt)), "price": ep_raw}
                        self.positions[symbol]["SHORT"] = {"size": 0.0, "price": 0.0}
                    elif pos_side == "SHORT":
                        self.positions[symbol]["SHORT"] = {"size": abs(float(pos_amt)), "price": ep_raw}
                        self.positions[symbol]["LONG"] = {"size": 0.0, "price": 0.0}
                    else:
                        if pos_amt > 0:
                            self.positions[symbol]["LONG"] = {"size": float(pos_amt), "price": ep_raw}
                            self.positions[symbol]["SHORT"] = {"size": 0.0, "price": 0.0}
                        elif pos_amt < 0:
                            self.positions[symbol]["SHORT"] = {"size": abs(float(pos_amt)), "price": ep_raw}
                            self.positions[symbol]["LONG"] = {"size": 0.0, "price": 0.0}
                        else:
                            self.positions[symbol]["LONG"] = {"size": 0.0, "price": 0.0}
                            self.positions[symbol]["SHORT"] = {"size": 0.0, "price": 0.0}

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
