# ============================================================
# FILE: API/BITGET/ws_trade_bitget.py
# ROLE: Реактивный WebSocket торговый клиент для Bitget Futures V2 WS-API
# ============================================================

import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
import base64
import uuid
from typing import Optional, Dict, Any
from c_log import log

class BitgetWsTrader:
    """
    Высокоскоростной реактивный WebSocket клиент для размещения и отмены ордеров
    на Bitget USDT-M Futures через wss://ws.bitget.com/v2/ws/private.
    Поддерживает авторизованную горячую сессию через периодические пинги.
    """
    WS_URL = "wss://ws.bitget.com/v2/ws/private"

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, margin_settings: Optional[dict] = None, session: Optional[aiohttp.ClientSession] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.margin_settings = margin_settings or {"margin_type": "crossed"}
        self.session = session
        self._own_session = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._running = False
        self._logged_in_event = asyncio.Event()
        self._worker_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._running:
            return
        self._running = True
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            self._own_session = True
        self._worker_task = asyncio.create_task(self._connection_loop())
        try:
            await asyncio.wait_for(self._logged_in_event.wait(), timeout=6.0)
            log("[BitgetWsTrader] WebSocket торговый стрим успешно авторизован и готов.", level="INFO")
        except asyncio.TimeoutError:
            log("[BitgetWsTrader] WebSocket стрим авторизуется в фоновом режиме...", level="WARNING")

    async def close(self):
        self._running = False
        self._logged_in_event.clear()
        if self._ping_task:
            self._ping_task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._worker_task:
            self._worker_task.cancel()
        if self._own_session and self.session and not self.session.closed:
            await self.session.close()

    def _generate_signature(self, timestamp: str) -> str:
        message = timestamp + "GET" + "/user/verify"
        mac = hmac.new(bytes(self.api_secret, encoding='utf-8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode('utf-8')

    async def _connection_loop(self):
        backoff = 0.5
        while self._running:
            try:
                self._logged_in_event.clear()
                log(f"[BitgetWsTrader] Подключение к {self.WS_URL}...", level="DEBUG")
                async with self.session.ws_connect(self.WS_URL, heartbeat=None, max_msg_size=10*1024*1024) as ws:
                    self._ws = ws

                    # 1. Отправляем логин
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
                    await ws.send_json(login_msg)

                    # Запускаем фоновый пингер
                    if self._ping_task:
                        self._ping_task.cancel()
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))

                    backoff = 0.5
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw = msg.data.strip()
                            if raw == "pong":
                                continue
                            try:
                                data = json.loads(raw)
                                event = data.get("event")
                                code = data.get("code")

                                # Проверка успешного логина
                                if event == "login":
                                    if code == 0 or code == "0":
                                        self._logged_in_event.set()
                                        log("[BitgetWsTrader] Успешная авторизация в сокете.", level="DEBUG")
                                    else:
                                        log(f"[BitgetWsTrader] Ошибка авторизации: {data}", level="ERROR")
                                    continue

                                # Обработка ответа на trade операцию
                                op = data.get("op")
                                event = data.get("event")
                                req_id = data.get("id")

                                # Проверка по data (list или dict)
                                d = data.get("data")
                                if isinstance(d, list) and len(d) > 0:
                                    req_id = req_id or d[0].get("clientOid") or d[0].get("id")
                                elif isinstance(d, dict):
                                    req_id = req_id or d.get("clientOid") or d.get("id")

                                # Проверка по args (list)
                                args = data.get("args")
                                if isinstance(args, list) and len(args) > 0:
                                    req_id = req_id or args[0].get("id")

                                if req_id and req_id in self._pending_requests:
                                    fut = self._pending_requests.pop(req_id)
                                    if not fut.done():
                                        fut.set_result(data)
                                elif self._pending_requests:
                                    if event in ("trade", "order", "place-order") or op in ("trade", "order") or "data" in data or code in (0, "0", "00000"):
                                        for pending_id, fut in list(self._pending_requests.items()):
                                            if not fut.done():
                                                fut.set_result(data)
                                                self._pending_requests.pop(pending_id, None)
                                                break
                            except Exception as parse_err:
                                log(f"[BitgetWsTrader] Ошибка парсинга сообщения: {parse_err}", level="WARNING")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log(f"[BitgetWsTrader] Сокет закрылся: {msg.type}", level="WARNING")
                            break
            except asyncio.CancelledError:
                break
            except Exception as conn_err:
                log(f"[BitgetWsTrader] Ошибка сокета: {conn_err}. Реконнект через {backoff:.1f}с...", level="WARNING")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 5.0)

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Bitget требует отправку строки 'ping' каждые 20-25 секунд."""
        while self._running and not ws.closed:
            try:
                await asyncio.sleep(20.0)
                if not ws.closed:
                    await ws.send_str("ping")
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def place_order(self, symbol: str, side: str, qty_str: str, trade_side: str = "open", timeout: float = 3.0) -> dict:
        """
        Отправляет MARKET ордер через реактивный WebSocket.
        trade_side: 'open' или 'close'
        """
        if not self._logged_in_event.is_set() or not self._ws or self._ws.closed:
            try:
                await asyncio.wait_for(self._logged_in_event.wait(), timeout=1.5)
            except asyncio.TimeoutError:
                raise ConnectionError("[BitgetWsTrader] WebSocket стрим не авторизован")

        req_id = str(uuid.uuid4()).replace("-", "")[:30]
        # Bitget instId: strip _UMCBL if present
        inst_id = symbol.replace("_UMCBL", "").strip().upper()
        margin_mode = str(self.margin_settings.get("margin_type", "crossed")).lower()

        payload = {
            "op": "trade",
            "args": [
                {
                    "channel": "place-order",
                    "instType": "USDT-FUTURES",
                    "instId": inst_id,
                    "id": req_id,
                    "params": {
                        "orderType": "market",
                        "side": side.lower(),
                        "size": qty_str,
                        "tradeSide": trade_side.lower(),
                        "marginMode": margin_mode,
                        "marginCoin": "USDT",
                        "clientOid": req_id
                    }
                }
            ]
        }

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_requests[req_id] = fut

        try:
            await self._ws.send_json(payload)
            res = await asyncio.wait_for(fut, timeout=timeout)
            code = res.get("code")
            if code is not None and str(code) not in ("0", "00000"):
                msg = res.get("msg", str(res))
                raise Exception(f"Bitget WS Trade Error ({code}): {msg}")
            return res
        finally:
            self._pending_requests.pop(req_id, None)

    async def cancel_all_orders(self, symbol: str, timeout: float = 3.0) -> dict:
        """Отменяет все ордера по символу через WS-API."""
        if not self._logged_in_event.is_set() or not self._ws or self._ws.closed:
            return {}

        req_id = str(uuid.uuid4()).replace("-", "")[:30]
        inst_id = symbol.replace("_UMCBL", "").strip().upper()
        payload = {
            "op": "trade",
            "args": [
                {
                    "channel": "cancel-order",
                    "instType": "USDT-FUTURES",
                    "instId": inst_id,
                    "id": req_id,
                    "params": {
                        "instId": inst_id
                    }
                }
            ]
        }

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_requests[req_id] = fut

        try:
            await self._ws.send_json(payload)
            return await asyncio.wait_for(fut, timeout=timeout)
        except Exception as e:
            log(f"[BitgetWsTrader] cancel_all_orders WS error: {e}", level="WARNING")
            return {}
        finally:
            self._pending_requests.pop(req_id, None)
