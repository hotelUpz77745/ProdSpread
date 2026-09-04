# ============================================================
# FILE: API/BINANCE/ws_trade_binance.py
# ROLE: Реактивный WebSocket торговый клиент для Binance Futures WS-API (v1)
# ============================================================

import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
import uuid
from typing import Optional, Dict, Any
from urllib.parse import urlencode
from c_log import log

class BinanceWsTrader:
    """
    Высокоскоростной реактивный WebSocket клиент для размещения и отмены ордеров
    на Binance USDS-M Futures через wss://ws-fapi.binance.com/ws-fapi/v1.
    Постоянно поддерживает горячее соединение через WebSocket ping/heartbeat.
    """
    WS_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"

    def __init__(self, api_key: str, api_secret: str, session: Optional[aiohttp.ClientSession] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = session
        self._own_session = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._running = False
        self._connected_event = asyncio.Event()
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
            await asyncio.wait_for(self._connected_event.wait(), timeout=5.0)
            log("[BinanceWsTrader] WebSocket торговый стрим успешно подключен и готов.", level="INFO")
        except asyncio.TimeoutError:
            log("[BinanceWsTrader] WebSocket стрим подключается в фоновом режиме...", level="WARNING")

    async def close(self):
        self._running = False
        self._connected_event.clear()
        if self._ping_task:
            self._ping_task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._worker_task:
            self._worker_task.cancel()
        if self._own_session and self.session and not self.session.closed:
            await self.session.close()

    def _generate_signature(self, params: dict) -> str:
        # Сортируем параметры по алфавиту для формирования строки подписи
        sorted_keys = sorted(params.keys())
        query_str = "&".join(f"{k}={params[k]}" for k in sorted_keys)
        return hmac.new(self.api_secret.encode('utf-8'), query_str.encode('utf-8'), hashlib.sha256).hexdigest()

    async def _connection_loop(self):
        backoff = 0.5
        while self._running:
            try:
                self._connected_event.clear()
                log(f"[BinanceWsTrader] Подключение к {self.WS_URL}...", level="DEBUG")
                async with self.session.ws_connect(self.WS_URL, heartbeat=15.0, max_msg_size=10*1024*1024) as ws:
                    self._ws = ws
                    self._connected_event.set()
                    backoff = 0.5
                    log("[BinanceWsTrader] Соединение установлено.", level="DEBUG")

                    # Запускаем фоновый пингер для поддержания постоянной горячей сессии
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
                                log(f"[BinanceWsTrader] Ошибка парсинга сообщения: {parse_err}", level="WARNING")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log(f"[BinanceWsTrader] Сокет закрылся: {msg.type}", level="WARNING")
                            break
            except asyncio.CancelledError:
                break
            except Exception as conn_err:
                log(f"[BinanceWsTrader] Ошибка сокета: {conn_err}. Реконнект через {backoff:.1f}с...", level="WARNING")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 5.0)

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse):
        """Отправляет ping-фрейм каждые 15 секунд для поддержания постоянной горячей сессии."""
        while self._running and not ws.closed:
            try:
                await asyncio.sleep(15.0)
                if not ws.closed:
                    await ws.ping()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def place_order(self, symbol: str, side: str, qty_str: str, position_side: Optional[str] = None, timeout: float = 3.0) -> dict:
        """
        Отправляет MARKET ордер через горячий реактивный WebSocket.
        Возвращает ответ биржи с информацией об ордере.
        """
        if not self._connected_event.is_set() or not self._ws or self._ws.closed:
            # Ожидание готовности сокета с коротким таймаутом
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                raise ConnectionError("[BinanceWsTrader] WebSocket стрим не подключен")

        req_id = str(uuid.uuid4())
        timestamp = int(time.time() * 1000)

        params = {
            "apiKey": self.api_key,
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "quantity": qty_str,
            "timestamp": timestamp
        }
        if position_side:
            params["positionSide"] = position_side.upper()

        signature = self._generate_signature(params)
        params["signature"] = signature

        payload = {
            "id": req_id,
            "method": "order.place",
            "params": params
        }

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_requests[req_id] = fut

        try:
            await self._ws.send_json(payload)
            res = await asyncio.wait_for(fut, timeout=timeout)
            status = res.get("status")
            if status != 200:
                err_data = res.get("error", res)
                raise Exception(f"Binance WS API Error: {err_data}")
            return res.get("result", res)
        finally:
            self._pending_requests.pop(req_id, None)

    async def cancel_all_orders(self, symbol: str, timeout: float = 3.0) -> dict:
        """Отменяет открытые ордера по символу через WS-API."""
        if not self._connected_event.is_set() or not self._ws or self._ws.closed:
            return {}

        req_id = str(uuid.uuid4())
        timestamp = int(time.time() * 1000)
        params = {
            "apiKey": self.api_key,
            "symbol": symbol.upper(),
            "timestamp": timestamp
        }
        signature = self._generate_signature(params)
        params["signature"] = signature

        payload = {
            "id": req_id,
            "method": "order.cancelAll",
            "params": params
        }

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_requests[req_id] = fut

        try:
            await self._ws.send_json(payload)
            return await asyncio.wait_for(fut, timeout=timeout)
        except Exception as e:
            log(f"[BinanceWsTrader] cancel_all_orders WS error: {e}", level="WARNING")
            return {}
        finally:
            self._pending_requests.pop(req_id, None)
