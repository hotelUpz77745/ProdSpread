# ============================================================
# FILE: API/BYBIT/stakan.py
# ROLE: BYBIT OrderBook Stream via WebSocket
# ============================================================

from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable, Iterable, List, Optional

import aiohttp
from API.BINANCE.stakan import DepthTop


class BybitStakanStream:
    """BYBIT OrderBook Stream.
    
    Topic: orderbook.1.{symbol} (top 1 level pushed every 10ms as snapshot)
    WS URL: wss://stream.bybit.com/v5/public/linear
    """

    WS_URL = "wss://stream.bybit.com/v5/public/linear"

    def __init__(
        self,
        symbols: Iterable[str],
        chunk_size: int = 10,
        ping_sec: float = 20.0,
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
            
        args = [f"orderbook.1.{sym}" for sym in symbols]
        req = {
            "op": "subscribe",
            "args": args
        }
        
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
                                await ws_conn.send_json({"op": "ping"})
                    
                    ping_task = asyncio.create_task(pinger())
                    await ws_conn.send_json(req)
                    
                    async for msg in ws_conn:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            
                            topic = data.get("topic", "")
                            if topic.startswith("orderbook.1.") and "data" in data:
                                sym = topic.replace("orderbook.1.", "")
                                payload = data["data"]
                                
                                bids = [(float(p[0]), float(p[1])) for p in payload.get("b", [])]
                                asks = [(float(p[0]), float(p[1])) for p in payload.get("a", [])]
                                ts = int(data.get("ts", time.time() * 1000))
                                
                                if bids or asks:
                                    dt = DepthTop(
                                        symbol=sym,
                                        bids=bids,
                                        asks=asks,
                                        event_time_ms=ts,
                                    )
                                    await on_depth(dt)
                                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"BYBIT WS Error: {e}")
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

        stream = BybitStakanStream(["BTCUSDT", "ETHUSDT"])
        task = asyncio.create_task(stream.run(on_depth))
        await asyncio.sleep(5)
        await stream.aclose()
        await task
        print("Done")

    asyncio.run(_test())
