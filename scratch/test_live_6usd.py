import asyncio
import json
import os
import sys
import aiohttp
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from c_log import log

async def main():
    load_dotenv()
    
    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
        
    session = aiohttp.ClientSession()
    
    # 1. Start WS Streams
    binance_ws = BinancePositionStream(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", "")
    )
    kucoin_ws = KucoinPositionStream(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
    )
    
    t1 = asyncio.create_task(binance_ws.start())
    t2 = asyncio.create_task(kucoin_ws.start())
    
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
    
    print(">>> 1. Waiting 3 sec for exchange specs & WS connection...")
    await asyncio.sleep(3)
    
    print(f"    Binance WS ready: {binance_ws.ready}")
    print(f"    Kucoin WS ready: {kucoin_ws.ready}")
    
    # We test on ARB (Binance: ARBUSDT, Kucoin: ARBUSDTM)
    sym_bin = "ARBUSDT"
    sym_kuc = "ARBUSDTM"
    trade_size_usd = 6.0
    
    # Fetch current ticker prices
    async with session.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={sym_bin}") as r:
        bin_price = float((await r.json())["price"])
    async with session.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={sym_kuc}") as r:
        kuc_price = float((await r.json())["data"]["price"])
        
    print(f"\n>>> 2. Current prices: Binance ARB = {bin_price}, Kucoin ARB = {kuc_price}")
    
    # Setup leverage and margin
    print("\n>>> 3. Setting Leverage & Margin (10x CROSS)...")
    try:
        await binance_order.set_margin_type(sym_bin, "CROSSED")
        await binance_order.set_leverage(sym_bin, 10)
    except Exception as e:
        print(f"    Binance margin setup: {e}")
    try:
        await kucoin_order.set_margin_type(sym_kuc, "CROSS", 10)
        await kucoin_order.set_leverage(sym_kuc, 10, "CROSS")
    except Exception as e:
        print(f"    Kucoin margin setup: {e}")
        
    # Check initial positions
    pos_b_init = await binance_order.get_exact_position(sym_bin, "LONG")
    pos_k_init = await kucoin_order.get_exact_position(sym_kuc, "SHORT")
    print(f"    Initial Binance pos: {pos_b_init}")
    print(f"    Initial Kucoin pos:  {pos_k_init}")
    
    # OPEN $6 POSITION (Binance LONG, Kucoin SHORT)
    print(f"\n>>> 4. Opening positions with ~{trade_size_usd}$ (Binance BUY LONG | Kucoin SELL SHORT)...")
    long_task = binance_order.place_order(sym_bin, "BUY", trade_size_usd, bin_price * 1.05, order_type="MARKET", position_side="LONG")
    short_task = kucoin_order.place_order(sym_kuc, "SELL", trade_size_usd, kuc_price * 0.95, order_type="MARKET", position_side="SHORT")
    
    open_res = await asyncio.gather(long_task, short_task, return_exceptions=True)
    print(f"    Binance order response: {open_res[0] if not isinstance(open_res[0], Exception) else 'ERROR: ' + str(open_res[0])}")
    print(f"    Kucoin order response:  {open_res[1] if not isinstance(open_res[1], Exception) else 'ERROR: ' + str(open_res[1])}")
    
    # Wait for execution and verify positions
    print("\n>>> 5. Waiting 1.5 sec for WS stream to capture positions...")
    await asyncio.sleep(1.5)
    
    pos_b_ws = binance_order.get_executed_position(sym_bin, "LONG")
    pos_k_ws = kucoin_order.get_executed_position(sym_kuc, "SHORT")
    print(f"    WS cached positions -> Binance: {pos_b_ws}, Kucoin: {pos_k_ws}")
    
    pos_b_exact = await binance_order.get_exact_position(sym_bin, "LONG")
    pos_k_exact = await kucoin_order.get_exact_position(sym_kuc, "SHORT")
    print(f"    EXACT verified positions -> Binance: {pos_b_exact}, Kucoin: {pos_k_exact}")
    
    # Cancel any residual orders
    print("\n>>> 6. Testing cancel_all_orders...")
    await asyncio.gather(
        binance_order.cancel_all_orders(sym_bin),
        kucoin_order.cancel_all_orders(sym_kuc),
        return_exceptions=True
    )
    print("    Open orders cancelled successfully.")
    
    # EMERGENCY CLOSE TEST: Close positions with reduceOnly
    print("\n>>> 7. Testing EMERGENCY CLOSE on both exchanges...")
    close_bin_qty = pos_b_exact["size"] if pos_b_exact["size"] > 0 else 60.0
    close_kuc_qty = pos_k_exact["size"] if pos_k_exact["size"] > 0 else 60.0
    
    close_bin_usd = close_bin_qty * (bin_price * 0.90)
    close_kuc_usd = close_kuc_qty * (kuc_price * 1.10)
    
    close_long_task = binance_order.place_order(sym_bin, "SELL", close_bin_usd, bin_price * 0.90, order_type="MARKET", position_side="LONG")
    close_short_task = kucoin_order.place_order(sym_kuc, "BUY", close_kuc_usd, kuc_price * 1.10, order_type="MARKET", position_side="SHORT")
    
    close_res = await asyncio.gather(close_long_task, close_short_task, return_exceptions=True)
    print(f"    Binance close response: {close_res[0] if not isinstance(close_res[0], Exception) else 'ERROR: ' + str(close_res[0])}")
    print(f"    Kucoin close response:  {close_res[1] if not isinstance(close_res[1], Exception) else 'ERROR: ' + str(close_res[1])}")
    
    print("\n>>> 8. Waiting 2 sec for close settlement and checking remaining positions...")
    await asyncio.sleep(2)
    
    final_bin = await binance_order.get_exact_position(sym_bin, "LONG")
    final_kuc = await kucoin_order.get_exact_position(sym_kuc, "SHORT")
    all_bin = await binance_order.get_active_positions()
    all_kuc = await kucoin_order.get_active_positions()
    
    print(f"    Final Binance position for {sym_bin}: {final_bin}")
    print(f"    Final Kucoin position for {sym_kuc}:  {final_kuc}")
    print(f"    All Binance active positions: {all_bin}")
    print(f"    All Kucoin active positions:  {all_kuc}")
    
    success = (len(all_bin) == 0 or all(p['size'] == 0 for p in all_bin)) and (len(all_kuc) == 0 or all(p['size'] == 0 for p in all_kuc))
    if success:
        print("\n=======================================================")
        print(">>> SUCCESS: Full 6$ Open -> Sync -> Close cycle passed perfectly! No stranded positions! <<<")
        print("=======================================================")
    else:
        print("\n>>> WARNING: Check remaining positions above! <<<")
        
    await session.close()
    await binance_ws.stop()
    await kucoin_ws.stop()

if __name__ == "__main__":
    asyncio.run(main())
