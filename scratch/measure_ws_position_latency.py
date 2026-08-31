import asyncio
import json
import os
import sys
import aiohttp
import time
import uuid
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream

async def benchmark_latencies():
    load_dotenv()
    session = aiohttp.ClientSession()
    
    # 1. WS Streams
    binance_ws = BinancePositionStream(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", "")
    )
    kucoin_ws = KucoinPositionStream(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
    )
    
    asyncio.create_task(binance_ws.start())
    asyncio.create_task(kucoin_ws.start())
    
    binance_order = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session,
        position_stream=binance_ws
    )
    kucoin_order = KucoinOrder(
        api_key=os.environ["KUCOIN_API_KEY"],
        api_secret=os.environ["KUCOIN_API_SECRET"],
        api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
        session=session,
        position_stream=kucoin_ws,
        margin_settings={"margin_type": "CROSS", "leverage": 10}
    )
    
    binance_order.start()
    kucoin_order.start()
    
    print("Waiting 3s for warmup & connections...")
    await asyncio.sleep(3)
    
    sym_bin = "ARBUSDT"
    sym_kuc = "ARBUSDTM"
    
    # Track WS arrival timestamps
    ws_events = {
        "binance_open": None,
        "kucoin_open": None,
        "binance_close": None,
        "kucoin_close": None
    }
    
    # Hook into ws_private handlers to measure exact arrival time
    orig_bin_handler = binance_ws._handle_account_update
    async def hooked_bin_handler(data):
        t_arr = time.time()
        for p in data.get("a", {}).get("P", []):
            if "ARB" in p.get("s", ""):
                amt = float(p.get("pa", 0))
                if amt != 0 and ws_events["binance_open"] is None:
                    ws_events["binance_open"] = t_arr
                elif amt == 0 and ws_events["binance_close"] is None:
                    ws_events["binance_close"] = t_arr
        await orig_bin_handler(data)
    binance_ws._handle_account_update = hooked_bin_handler
    
    orig_kuc_positions = kucoin_ws.positions
    # We poll / hook Kucoin ws
    
    print("\n--- MEASURING OPEN ORDER LATENCY ---")
    t0_send = time.time()
    
    task_b = binance_order.place_order(sym_bin, "BUY", 6.0, 0.10, order_type="MARKET", position_side="LONG")
    task_k = kucoin_order.place_order(sym_kuc, "SELL", 6.0, 0.08, order_type="MARKET", position_side="SHORT")
    
    res_b, res_k = await asyncio.gather(task_b, task_k)
    t_http_done = time.time()
    
    print(f"HTTP POST RTT: {(t_http_done - t0_send)*1000:.1f} ms")
    
    # Poll WS stream every 5ms to record exact time it appeared in memory
    t_bin_ws_seen = None
    t_kuc_ws_seen = None
    
    for _ in range(400): # 2 seconds
        now = time.time()
        if t_bin_ws_seen is None and binance_ws.get_position(sym_bin, "LONG")["size"] > 0:
            t_bin_ws_seen = now
        if t_kuc_ws_seen is None and kucoin_ws.get_position(sym_kuc, "SHORT")["size"] > 0:
            t_kuc_ws_seen = now
        if t_bin_ws_seen and t_kuc_ws_seen:
            break
        await asyncio.sleep(0.005)
        
    print(f"\n[LATENCY RESULTS - OPEN]:")
    if t_bin_ws_seen:
        print(f"  BINANCE WS position received in: {(t_bin_ws_seen - t0_send)*1000:.1f} ms from order send ({(t_bin_ws_seen - t_http_done)*1000:.1f} ms after HTTP resp)")
    else:
        print("  BINANCE WS not seen within 2s")
        
    if t_kuc_ws_seen:
        print(f"  KUCOIN WS position received in:  {(t_kuc_ws_seen - t0_send)*1000:.1f} ms from order send ({(t_kuc_ws_seen - t_http_done)*1000:.1f} ms after HTTP resp)")
    else:
        print("  KUCOIN WS not seen within 2s (will check REST)")

    # CLEANUP CLOSE
    print("\n--- MEASURING CLOSE ORDER LATENCY ---")
    pos_b = await binance_order.get_exact_position(sym_bin, "LONG")
    pos_k = await kucoin_order.get_exact_position(sym_kuc, "SHORT")
    
    t0_close = time.time()
    task_cb = binance_order.place_order(sym_bin, "SELL", pos_b["size"] * 0.08, 0.08, order_type="MARKET", position_side="LONG")
    task_ck = kucoin_order.place_order(sym_kuc, "BUY", pos_k["size"] * 0.10, 0.10, order_type="MARKET", position_side="SHORT")
    await asyncio.gather(task_cb, task_ck)
    t_http_close_done = time.time()
    
    t_bin_zero_seen = None
    t_kuc_zero_seen = None
    for _ in range(400):
        now = time.time()
        if t_bin_zero_seen is None and binance_ws.get_position(sym_bin, "LONG")["size"] == 0:
            t_bin_zero_seen = now
        if t_kuc_zero_seen is None and kucoin_ws.get_position(sym_kuc, "SHORT")["size"] == 0:
            t_kuc_zero_seen = now
        if t_bin_zero_seen and t_kuc_zero_seen:
            break
        await asyncio.sleep(0.005)
        
    print(f"\n[LATENCY RESULTS - CLOSE]:")
    if t_bin_zero_seen:
        print(f"  BINANCE WS zero position received in: {(t_bin_zero_seen - t0_close)*1000:.1f} ms from close send")
    if t_kuc_zero_seen:
        print(f"  KUCOIN WS zero position received in:  {(t_kuc_zero_seen - t0_close)*1000:.1f} ms from close send")
        
    # Verify 0 positions
    await asyncio.sleep(1)
    all_b = await binance_order.get_active_positions()
    all_k = await kucoin_order.get_active_positions()
    print(f"\nFinal check: Binance = {all_b}, Kucoin = {all_k}")
    
    await session.close()
    await binance_ws.stop()
    await kucoin_ws.stop()

if __name__ == "__main__":
    asyncio.run(benchmark_latencies())
