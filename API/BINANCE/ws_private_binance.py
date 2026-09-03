# ============================================================
# FILE: API/BINANCE/ws_private_binance.py
# ROLE: Connection to User Data Stream for Binance Futures and Position parsing
# ============================================================

import asyncio
import aiohttp
import re
import json
from typing import Optional, Callable, Set, Dict, Any
from c_log import log

# Permitted symbols regex
_SYMBOL_REGEX = re.compile(r"^[A-Z0-9]+$")

def normalize_symbol(raw: str) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    sym = raw.strip().upper()
    if not sym:
        return None
    for ch in sym:
        if "А" <= ch <= "Я" or "а" <= ch <= "я":
            return None
    if not _SYMBOL_REGEX.match(sym):
        return None
    return sym

class BinanceListenKeyManager:
    KEEPALIVE_INTERVAL = 25 * 60  # Binance recommends < 30 mins

    def __init__(self, api_key: str, session: aiohttp.ClientSession):
        self.api_key = api_key
        self.session = session
        self.listen_key: Optional[str] = None
        self._task: Optional[asyncio.Task] = None

    async def create(self) -> str:
        async with self.session.post(
            "https://fapi.binance.com/fapi/v1/listenKey",
            headers={"X-MBX-APIKEY": self.api_key},
        ) as r:
            data = await r.json()
            if "listenKey" not in data:
                error_msg = data.get('msg', str(data))
                log(f"[BINANCE] API-Key rejected: {error_msg}", level="ERROR")
                raise ValueError(f"Binance API Error: {error_msg}")
            self.listen_key = data["listenKey"]

        log(f"[BINANCE] listenKey created & activated: {self.listen_key[:5]}...", level="INFO")
        return self.listen_key

    async def _keepalive_loop(self):
        while True:
            await asyncio.sleep(self.KEEPALIVE_INTERVAL)
            try:
                await self.session.put(
                    "https://fapi.binance.com/fapi/v1/listenKey",
                    headers={"X-MBX-APIKEY": self.api_key},
                )
            except Exception as e:
                log(f"[BINANCE] listenKey keepalive failed: {e}", level="WARNING")

    def start_keepalive(self):
        if not self._task:
            self._task = asyncio.create_task(self._keepalive_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

class BinancePositionStream:
    def __init__(
        self,
        api_key: str,
        api_secret: str = "",
        stop_flag: Optional[Callable[[], bool]] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.stop_flag = stop_flag or (lambda: False)

        self.session: Optional[aiohttp.ClientSession] = None
        self.websocket: Optional[aiohttp.ClientWebSocketResponse] = None
        self.listen_mgr: Optional[BinanceListenKeyManager] = None

        self.ws_url: Optional[str] = None
        self.ready = False
        self.is_connected = False
        self._external_stop = False
        
        # Track positions by symbol
        self.positions: Dict[str, Dict[str, Any]] = {}

    async def stop(self):
        self._external_stop = True
        self.ready = False
        log("BinancePositionStream: stop requested", level="INFO")

    async def _create_session(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=None)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        return aiohttp.ClientSession(timeout=timeout, trust_env=False, headers=headers)

    async def _connect(self) -> bool:
        try:
            self.websocket = await self.session.ws_connect(
                self.ws_url,
                autoping=True,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Origin": "https://fapi.binance.com"
                },
                heartbeat=15.0
            )
            self.is_connected = True
            log(f"[BINANCE WS_PRIVATE] Connected successfully.", level="INFO")
            return True
        except Exception as e:
            log(f"[BINANCE WS_PRIVATE] Connect failed: {e}", level="ERROR")
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

        if self.listen_mgr:
            await self.listen_mgr.stop()
            self.listen_mgr = None

        log("BinancePositionStream: WS disconnected", level="INFO")

    async def _handle_order_update(self, data: dict):
        o = data.get("o", {})
        raw_symbol = o.get("s", "")
        symbol = normalize_symbol(raw_symbol)
        if not symbol:
            return

        pos_side_raw = (o.get("ps") or "").upper()
        order_side = str(o.get("S", "")).upper()
        if pos_side_raw == "BOTH" or not pos_side_raw:
            pos_side_raw = "LONG" if order_side == "BUY" else "SHORT"

        order_status = o.get("X", "")
        cum_qty = float(o.get("z", 0.0))
        avg_price = float(o.get("ap", 0.0)) or float(o.get("L", 0.0)) or float(o.get("p", 0.0))
        is_reduce = o.get("R") is True

        if symbol not in self.positions:
            self.positions[symbol] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}

        if order_status in ("FILLED", "PARTIALLY_FILLED"):
            if order_status == "FILLED" and is_reduce:
                self.positions[symbol][pos_side_raw] = {"size": 0.0, "price": 0.0}
            elif cum_qty > 0:
                self.positions[symbol][pos_side_raw] = {
                    "size": cum_qty,
                    "price": avg_price
                }

    async def _handle_account_update(self, data: dict):
        acc = data.get("a", {})
        positions = acc.get("P", [])
        
        for p in positions:
            raw_symbol = p.get("s", "")
            symbol = normalize_symbol(raw_symbol)
            if not symbol:
                continue

            pos_side_raw = (p.get("ps") or "").upper()
            pos_amt = float(p.get("pa", 0.0))
            ep_raw = float(p.get("ep") or p.get("bep") or 0.0)

            if symbol not in self.positions:
                self.positions[symbol] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}

            if pos_side_raw == "BOTH":
                if pos_amt > 0:
                    self.positions[symbol]["LONG"] = {"size": abs(pos_amt), "price": ep_raw}
                elif pos_amt < 0:
                    self.positions[symbol]["SHORT"] = {"size": abs(pos_amt), "price": ep_raw}
                else:
                    self.positions[symbol]["LONG"] = {"size": 0.0, "price": 0.0}
                    self.positions[symbol]["SHORT"] = {"size": 0.0, "price": 0.0}
            else:
                self.positions[symbol][pos_side_raw] = {
                    "size": abs(pos_amt),
                    "price": ep_raw
                }

    async def _handle_messages(self):
        while not self._external_stop and not self.stop_flag():
            try:
                msg = await asyncio.wait_for(
                    self.websocket.receive(),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                continue

            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                log(f"[BINANCE WS_PRIVATE] WebSocket closed by server: type={msg.type}, extra={getattr(msg, 'extra', None)}, data={getattr(msg, 'data', None)}", level="WARNING")
                raise RuntimeError("ws_closed")

            if msg.type != aiohttp.WSMsgType.TEXT:
                continue

            try:
                data = json.loads(msg.data)
            except Exception:
                continue

            etype = data.get("e")
            if not etype:
                continue

            if etype == "ACCOUNT_UPDATE":
                await self._handle_account_update(data)
            elif etype == "ORDER_TRADE_UPDATE":
                await self._handle_order_update(data)

    async def start(self):
        self._external_stop = False
        try:
            while not self._external_stop and not self.stop_flag():
                self.session = await self._create_session()
                try:
                    self.listen_mgr = BinanceListenKeyManager(
                        api_key=self.api_key,
                        session=self.session,
                    )
                    listen_key = await self.listen_mgr.create()
                    self.listen_mgr.start_keepalive()

                    self.ws_url = f"wss://fstream.binance.com/private/ws/{listen_key}"

                    if not await self._connect():
                        raise RuntimeError("ws_connect_failed")

                    self.ready = True
                    await self._handle_messages()

                except asyncio.CancelledError:
                    raise
                except RuntimeError as e:
                    if str(e) == "ws_closed":
                        log(f"[BINANCE WS_PRIVATE] Stream disconnected, reconnecting...", level="WARNING")
                except Exception as e:
                    log(f"[BINANCE WS_PRIVATE] cycle failed: {e}", level="ERROR")
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
        sym = normalize_symbol(symbol)
        if not sym or sym not in self.positions:
            return {"size": 0.0, "price": 0.0}
        return self.positions[sym].get(side.upper(), {"size": 0.0, "price": 0.0})
