# ============================================================
# FILE: API/GATE/stakan.py
# ROLE: GATE OrderBook Stream via WebSocket
# ============================================================

from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable, Iterable, List, Optional

import aiohttp
from API.BINANCE.stakan import DepthTop


class GateStakanStream:
    """GATE OrderBook Stream.
    
    Topic: futures.order_book
    WS URL: wss://fx-ws.gateio.ws/v4/ws/usdt
    """

    WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    def __init__(
        self,
        symbols: Iterable[str],
        chunk_size: int = 20,
        ping_sec: float = 15.0,
    ):
        self._symbols = list(set(sym.upper() for sym in symbols))
        self._chunk_size = max(1, chunk_size)
        self._ping_sec = max(5.0, ping_sec)
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: List[asyncio.Task] = []
        self._run_lock = asyncio.Lock()
        
    def _chunks(self) -> List[List[str]]:
        return [
            self._symbols[i : i + self._chunk_size]
            for i in range(0, len(self._symbols), self._chunk_size)
        ]

    async def aclose(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._session = None

    async def run(self, on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
        async with self._run_lock:
            if self._tasks:
                raise RuntimeError("Stream already running")

            from utils import SessionManager
            self._session = await SessionManager().get_session()
            
            try:
                for chunk in self._chunks():
                    t = asyncio.create_task(self._run_ws(chunk, on_depth))
                    self._tasks.append(t)
                
                if self._tasks:
                    await asyncio.gather(*self._tasks)
            finally:
                await self.aclose()

    async def _run_ws(self, symbols: List[str], on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
        if not self._session:
            return
            
        # For Gate, we can subscribe to "level": "5" or "10" and interval "100ms"
        # We need top 5 or 10 bids/asks.
        # Actually Gate has top level orderbook update: channel: "futures.order_book", event: "subscribe", payload: ["BTC_USDT", "5", "100ms"]
        payload = []
        for sym in symbols:
            payload.append(sym)
            payload.append("5")
            payload.append("100ms")
            
        # The API format allows subscribing multiple symbols? Actually payload: ["BTC_USDT", "5", "100ms", "ETH_USDT", "5", "100ms"] ? 
        # Wait, Gate API docs state: payload: [ "BTC_USDT", "5", "0" ] for top 5 without delay.
        # Let's subscribe individually or in bulk. Bulk is just array of args.
        
        # Let's send a subscribe request per symbol to be safe, or just one big array.
        req_id = int(time.time())
        reqs = []
        for sym in symbols:
            reqs.append({
                "time": int(time.time()),
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": [sym]
            })
        
        while True:
            ws_conn = None
            ping_task = None
            try:
                from contextlib import nullcontext
                async with nullcontext():
                    ws_conn = await self._session.ws_connect(self.WS_URL, autoping=False, max_msg_size=0)
                    
                    async def pinger():
                        while True:
                            await asyncio.sleep(self._ping_sec)
                            if ws_conn and not ws_conn.closed:
                                await ws_conn.send_json({"time": int(time.time()), "channel": "futures.ping"})
                    
                    ping_task = asyncio.create_task(pinger())
                    for req in reqs:
                        await ws_conn.send_json(req)
                    
                    async for msg in ws_conn:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            
                            channel = data.get("channel", "")
                            event = data.get("event", "")
                            
                            if channel == "futures.book_ticker" and event == "update":
                                result = data.get("result", {})
                                sym = result.get("s", "")
                                if not sym: continue
                                    
                                best_bid_p = float(result.get("b", 0))
                                best_bid_s = float(result.get("B", 0))
                                best_ask_p = float(result.get("a", 0))
                                best_ask_s = float(result.get("A", 0))
                                
                                ts = int(result.get("t", time.time() * 1000))
                                
                                if best_bid_p > 0 and best_ask_p > 0:
                                    dt = DepthTop(
                                        symbol=sym,
                                        bids=[(best_bid_p, best_bid_s)],
                                        asks=[(best_ask_p, best_ask_s)],
                                        event_time_ms=ts,
                                    )
                                    await on_depth(dt)
                                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"GATE WS Error: {e}")
                await asyncio.sleep(2)
            finally:
                if ping_task:
                    ping_task.cancel()
                if ws_conn and not ws_conn.closed:
                    await ws_conn.close()


# ----------------------------
# SELF TEST
# ----------------------------
if __name__ == "__main__":
    async def _test():
        count = 0
        async def on_depth(d: DepthTop):
            nonlocal count
            count += 1
            if count % 10 == 0:
                print(d)

        stream = GateStakanStream(["BTC_USDT", "ETH_USDT"])
        task = asyncio.create_task(stream.run(on_depth))
        await asyncio.sleep(5)
        await stream.aclose()
        await task
        print("Done")

    asyncio.run(_test())
