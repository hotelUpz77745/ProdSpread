# ============================================================
# FILE: API/KUCOIN/ws_trade_kucoin.py
# ROLE: Реактивный торговый клиент для Kucoin (Pro WS API v1 / UTA с устойчивым fall-through)
# ============================================================

import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
import base64
import uuid
import urllib.parse
from typing import Optional, Dict, Any
from c_log import log

class KucoinWsTrader:
    """
    Высокоскоростной реактивный торговый клиент для Kucoin Futures.
    Поддерживает:
    1) Pro WebSocket (wss://wsapi.kucoin.com/v1/private) с сигнатурной аутентификацией и авто-пингом.
    2) Бесшовный fallback на горячую persistent HTTP сессию, если аккаунт классический фьючерсный (Classic Futures).
    """
    WS_BASE_URL = "wss://wsapi.kucoin.com/v1/private"
    REST_URL = "https://api-futures.kucoin.com/api/v1/orders"

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, margin_settings: Optional[dict] = None, session: Optional[aiohttp.ClientSession] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.margin_settings = margin_settings or {"leverage": 20, "margin_type": "CROSS"}
        self.session = session
        self._own_session = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._running = False
        self._ws_ready_event = asyncio.Event()
        self._worker_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self.ws_active = False

    async def start(self):
        if self._running:
            return
        self._running = True
        if self.session is None or self.session.closed:
            # Persistent connection pool with keep-alive
            connector = aiohttp.TCPConnector(limit=100, keepalive_timeout=60.0)
            self.session = aiohttp.ClientSession(connector=connector)
            self._own_session = True
        self._worker_task = asyncio.create_task(self._connection_loop())
        try:
            await asyncio.wait_for(self._ws_ready_event.wait(), timeout=4.0)
            log("[KucoinWsTrader] Pro WebSocket стрим подключен и активен.", level="INFO")
        except asyncio.TimeoutError:
            log("[KucoinWsTrader] WS в процессе инициализации, включен горячий REST keep-alive транспорт.", level="INFO")

    async def close(self):
        self._running = False
        self._ws_ready_event.clear()
        self.ws_active = False
        if self._ping_task:
            self._ping_task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._worker_task:
            self._worker_task.cancel()
        if self._own_session and self.session and not self.session.closed:
            await self.session.close()

    def _build_ws_url(self) -> str:
        now = str(int(time.time() * 1000))
        pre_hash = self.api_key + now
        sign = base64.b64encode(hmac.new(self.api_secret.encode('utf-8'), pre_hash.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')

        passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
        encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')

        params = {
            "apikey": self.api_key,
            "timestamp": now,
            "sign": sign,
            "passphrase": encrypted_passphrase
        }
        return f"{self.WS_BASE_URL}?{urllib.parse.urlencode(params)}"

    async def _connection_loop(self):
        backoff = 1.0
        while self._running:
            try:
                self._ws_ready_event.clear()
                self.ws_active = False
                ws_url = self._build_ws_url()
                log("[KucoinWsTrader] Подключение к Pro WS...", level="DEBUG")
                async with self.session.ws_connect(ws_url, heartbeat=18.0, max_msg_size=10*1024*1024) as ws:
                    self._ws = ws
                    self.ws_active = True
                    self._ws_ready_event.set()
                    backoff = 1.0

                    if self._ping_task:
                        self._ping_task.cancel()
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                req_id = data.get("id")
                                if req_id and req_id in self._pending_requests:
                                    fut = self._pending_requests.pop(req_id)
                                    if not fut.done():
                                        fut.set_result(data)
                            except Exception as parse_err:
                                log(f"[KucoinWsTrader] Ошибка парсинга: {parse_err}", level="WARNING")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                # В случае, если аккаунт Classic Futures не имеет доступа к Pro WS URL, остаемся на сверхбыстром HTTP keep-alive
                self.ws_active = False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 10.0)

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        while self._running and not ws.closed:
            try:
                await asyncio.sleep(18.0)
                if not ws.closed:
                    await ws.ping()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def _generate_rest_sig(self, str_to_sign: str) -> str:
        h = hmac.new(self.api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(h.digest()).decode('utf-8')

    async def place_order(self, symbol: str, side: str, qty_str: str, position_side: Optional[str] = None, timeout: float = 3.0) -> dict:
        """
        Отправляет MARKET ордер через реактивный Pro WS или горячий persistent REST транспорт.
        """
        req_id = str(uuid.uuid4())
        leverage = str(self.margin_settings.get("leverage", 20))
        margin_mode = str(self.margin_settings.get("margin_type", "CROSS")).upper()

        # Попытка отправки через Pro WS (если поддерживается аккаунтом)
        if getattr(self, "_ws_uta_supported", True) and self.ws_active and self._ws and not self._ws.closed:
            payload = {
                "id": req_id,
                "op": "uta.order",
                "args": {
                    "symbol": symbol,
                    "side": side.lower(),
                    "type": "market",
                    "size": qty_str,
                    "tradeType": "FUTURES",
                    "leverage": leverage,
                    "marginMode": margin_mode
                }
            }
            if position_side:
                payload["args"]["positionSide"] = position_side.upper()
                if (side.lower() == "buy" and position_side.upper() == "SHORT") or \
                   (side.lower() == "sell" and position_side.upper() == "LONG"):
                    payload["args"]["reduceOnly"] = True

            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._pending_requests[req_id] = fut

            try:
                await self._ws.send_json(payload)
                res = await asyncio.wait_for(fut, timeout=min(timeout, 0.4))
                code = res.get("code")
                if code in (200, 200000, "200000"):
                    return res
                self._ws_uta_supported = False
            except Exception:
                self._ws_uta_supported = False
            finally:
                self._pending_requests.pop(req_id, None)

        # Реактивный fallback через горячий keep-alive REST сеанс
        return await self._place_order_rest(symbol, side, qty_str, position_side, leverage, margin_mode)

    async def _place_order_rest(self, symbol: str, side: str, qty_str: str, position_side: Optional[str], leverage: str, margin_mode: str) -> dict:
        endpoint = "/api/v1/orders"
        now = str(int(time.time() * 1000))
        body = {
            "clientOid": str(uuid.uuid4()),
            "symbol": symbol,
            "side": side.lower(),
            "size": qty_str,
            "type": "market",
            "leverage": leverage,
            "marginMode": margin_mode
        }
        if position_side:
            body["positionSide"] = position_side.upper()
            if (side.lower() == "buy" and position_side.upper() == "SHORT") or \
               (side.lower() == "sell" and position_side.upper() == "LONG"):
                body["reduceOnly"] = True

        body_str = json.dumps(body)
        str_to_sign = now + "POST" + endpoint + body_str
        signature = self._generate_rest_sig(str_to_sign)

        passphrase_hmac = hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256)
        encrypted_passphrase = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')

        headers = {
            'KC-API-KEY': self.api_key,
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': now,
            'KC-API-PASSPHRASE': encrypted_passphrase,
            'KC-API-KEY-VERSION': '2',
            'Content-Type': 'application/json'
        }

        url = f"https://api-futures.kucoin.com{endpoint}"
        async with self.session.post(url, headers=headers, data=body_str) as resp:
            data = await resp.json()
            if data.get('code') != '200000':
                msg = data.get("msg", str(data))
                raise Exception(f"Kucoin API Error: {msg}")
            return data
