#!/usr/bin/env python3
import asyncio
import os
import sys
import time
import json
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder

# --- DEBUG BINANCE ---
class DebugBinancePositionStream(BinancePositionStream):
    async def _handle_account_update(self, data):
        print(f"[BINANCE RAW WS] ACCOUNT_UPDATE: {json.dumps(data)}")
        await super()._handle_account_update(data)
        
    async def _handle_order_update(self, data):
        print(f"[BINANCE RAW WS] ORDER_TRADE_UPDATE: {json.dumps(data)}")
        await super()._handle_order_update(data)

# --- DEBUG KUCOIN ---
class DebugKucoinPositionStream(KucoinPositionStream):
    async def _handle_messages(self):
        asyncio.create_task(self._ping_loop())
        last_msg_time = time.time()
        while not self._external_stop and not self.stop_flag():
            try:
                msg = await asyncio.wait_for(self.websocket.receive(), timeout=5.0)
                last_msg_time = time.time()
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
                
            subject = data.get("subject", "")
            if subject in ("position.change", "orderChange"):
                print(f"[KUCOIN RAW WS] {subject}: {json.dumps(data)}")

            if data.get("type") == "message" and data.get("subject") == "position.change":
                d = data.get("data", {})
                symbol = d.get("symbol", "")
                if symbol:
                    pos_amt = float(d.get("currentQty", 0.0))
                    ep_raw = float(d.get("avgEntryPrice", 0.0))
                    pos_side = d.get("posSide", "")
                    
                    if symbol not in self.positions:
                        self.positions[symbol] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}
                    
                    if pos_side == "LONG":
                        self.positions[symbol]["LONG"] = {"size": abs(float(pos_amt)), "price": ep_raw if pos_amt != 0 else 0.0}
                    elif pos_side == "SHORT":
                        self.positions[symbol]["SHORT"] = {"size": abs(float(pos_amt)), "price": ep_raw if pos_amt != 0 else 0.0}
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

# --- DEBUG BITGET ---
class DebugBitgetPositionStream(BitgetPositionStream):
    async def run(self):
        self.session = aiohttp.ClientSession()
        while not self._external_stop and not self.stop_flag():
            ping_task = None
            try:
                self.websocket = await self.session.ws_connect(self.ws_url, heartbeat=None)
                self.is_connected = True
                
                now = str(int(time.time()))
                sign = self._generate_signature(now)
                await self.websocket.send_json({
                    "op": "login",
                    "args": [{"apiKey": self.api_key, "passphrase": self.api_passphrase, "timestamp": now, "sign": sign}]
                })
                auth_resp = await self.websocket.receive_json()
                
                await self.websocket.send_json({
                    "op": "subscribe",
                    "args": [{"instType": "USDT-FUTURES", "channel": "positions", "instId": "default"}]
                })
                
                self.ready = True
                
                async for msg in self.websocket:
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data in ("pong", "ping"): continue
                        try:
                            data = json.loads(msg.data)
                        except:
                            continue
                            
                        action = data.get("action")
                        if action in ("snapshot", "update"):
                            print(f"[BITGET RAW WS] {action}: {json.dumps(data)}")
                            channel = data.get("arg", {}).get("channel")
                            if channel == "positions":
                                incoming_syms = set()
                                for p in data.get("data", []):
                                    sym = p.get("instId", "").replace("_UMCBL", "").strip().upper()
                                    if not sym: continue
                                    raw_side = (p.get("holdSide") or p.get("posSide") or "").upper()
                                    incoming_syms.add((sym, raw_side))
                                    
                                    size = float(p.get("total", 0.0))
                                    price = float(p.get("openPriceAvg") or p.get("averageOpenPrice") or p.get("breakEvenPrice") or 0.0)
                                    
                                    if sym not in self.positions:
                                        self.positions[sym] = {"LONG": {"size": 0.0, "price": 0.0}, "SHORT": {"size": 0.0, "price": 0.0}}
                                    if raw_side in ("LONG", "SHORT"):
                                        self.positions[sym][raw_side] = {"size": size, "price": price}
                                        
                                if data.get("action") == "snapshot":
                                    for sym in list(self.positions.keys()):
                                        for side in ("LONG", "SHORT"):
                                            if (sym, side) not in incoming_syms:
                                                self.positions[sym][side] = {"size": 0.0, "price": 0.0}
            except Exception as e:
                pass
            finally:
                self.is_connected = False
                self.ready = False
                await asyncio.sleep(2)

async def get_xrp_price(session):
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=XRPUSDT") as resp:
        data = await resp.json()
        return float(data["price"])

async def poll_ws_size(stream, symbol, side, expected, timeout=3.0):
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        size = stream.get_position(symbol, side).get("size", 0.0)
        if expected(size): return (time.perf_counter() - start) * 1000, size
        await asyncio.sleep(0.005)
    return -1, stream.get_position(symbol, side).get("size", 0.0)

async def main():
    print("=" * 60)
    print("🚀 СОВМЕСТНЫЙ ТЕСТ: РЕАЛЬНЫЕ СДЕЛКИ И ДАМП WS (ALL EXCHANGES)")
    print("=" * 60)
    
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cfg.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
        
    session = aiohttp.ClientSession()
    
    b_stream = DebugBinancePositionStream(os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""))
    k_stream = DebugKucoinPositionStream(os.environ.get("KUCOIN_API_KEY", ""), os.environ.get("KUCOIN_API_SECRET", ""), os.environ.get("KUCOIN_API_PASSPHRASE", ""))
    bg_stream = DebugBitgetPositionStream(os.environ.get("BITGET_API_KEY", ""), os.environ.get("BITGET_API_SECRET", ""), os.environ.get("BITGET_API_PASSPHRASE", ""))
    
    b_order = BinanceOrder(os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""), session, b_stream)
    k_order = KucoinOrder(os.environ.get("KUCOIN_API_KEY", ""), os.environ.get("KUCOIN_API_SECRET", ""), os.environ.get("KUCOIN_API_PASSPHRASE", ""), session, k_stream, cfg["margin_settings"]["KUCOIN"])
    bg_order = BitgetOrder(os.environ.get("BITGET_API_KEY", ""), os.environ.get("BITGET_API_SECRET", ""), os.environ.get("BITGET_API_PASSPHRASE", ""), cfg["margin_settings"]["BITGET"], session, bg_stream)
    
    tasks = [asyncio.create_task(b_stream.start()), asyncio.create_task(k_stream.start()), asyncio.create_task(bg_stream.start())]
    
    while not (b_stream.ready and k_stream.ready and bg_stream.ready):
        await asyncio.sleep(0.1)
        
    await k_order.update_symbol_info()
    await bg_order.update_symbol_info()
    
    price = await get_xrp_price(session)
    size_usd = 6.0
    print(f"[*] Открываем XRP на {size_usd} USD (~цена {price})...")
    
    sym_b, sym_k, sym_bg = "XRPUSDT", "XRPUSDTM", "XRPUSDT"
    
    print("\n--- ОТКРЫТИЕ ПОЗИЦИЙ ---")
    async def open_pos(name, order, stream, sym):
        print(f"[{name}] BUY MARKET")
        start = time.perf_counter()
        try:
            await order.place_order(sym, "BUY", size_usd, price, "MARKET", "LONG")
        except Exception as e:
            print(f"[{name}] ERROR: {e}")
            return
        ms = (time.perf_counter() - start) * 1000
        print(f"[{name}] REST Executed in {ms:.1f}ms")
        delay, s = await poll_ws_size(stream, sym, "LONG", lambda s: s > 0, 2.0)
        print(f"[{name}] WS Update delay: {delay:.1f}ms (size: {s})")
        
    await asyncio.gather(open_pos("BINANCE", b_order, b_stream, sym_b), open_pos("KUCOIN", k_order, k_stream, sym_k), open_pos("BITGET", bg_order, bg_stream, sym_bg))
    
    await asyncio.sleep(3.0)
    
    print("\n--- ЗАКРЫТИЕ ПОЗИЦИЙ ---")
    async def close_pos(name, order, stream, sym):
        print(f"[{name}] SELL MARKET")
        start = time.perf_counter()
        try:
            await order.place_order(sym, "SELL", size_usd, price, "MARKET", "LONG")
        except Exception as e:
            print(f"[{name}] ERROR: {e}")
            return
        ms = (time.perf_counter() - start) * 1000
        print(f"[{name}] REST Executed in {ms:.1f}ms")
        delay, s = await poll_ws_size(stream, sym, "LONG", lambda s: s == 0, 2.0)
        print(f"[{name}] WS Update delay: {delay:.1f}ms (size: {s})")
        if delay == -1 and name == "BITGET":
            await order._close_position(sym, "long")
            
    await asyncio.gather(close_pos("BINANCE", b_order, b_stream, sym_b), close_pos("KUCOIN", k_order, k_stream, sym_k), close_pos("BITGET", bg_order, bg_stream, sym_bg))
    
    b_stream._external_stop = k_stream._external_stop = bg_stream._external_stop = True
    for t in tasks: t.cancel()
    await session.close()
    print("\n[V] ТЕСТ ЗАВЕРШЕН.")

if __name__ == "__main__":
    asyncio.run(main())
