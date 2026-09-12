import asyncio
import json
import os
import sys
import aiohttp
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder, BitgetOrder
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from c_log import log

async def main():
    load_dotenv()
    
    session = aiohttp.ClientSession()
    
    binance_ws = BinancePositionStream(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", "")
    )
    kucoin_ws = KucoinPositionStream(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
    )
    bitget_ws = BitgetPositionStream(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", "")
    )
    
    t1 = asyncio.create_task(binance_ws.start())
    t2 = asyncio.create_task(kucoin_ws.start())
    t3 = asyncio.create_task(bitget_ws.start())
    
    binance_order = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session,
        position_stream=binance_ws
    )
    kucoin_order = KucoinOrder(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
        session=session,
        position_stream=kucoin_ws,
        margin_settings={"margin_type": "CROSS", "leverage": 10}
    )
    bitget_order = BitgetOrder(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
        session=session,
        position_stream=bitget_ws,
        margin_settings={"margin_type": "crossed", "leverage": 10}
    )
    
    binance_order.start()
    kucoin_order.start()
    bitget_order.start()
    
    print(">>> 1. Waiting 3 sec for exchange specs & WS connection...")
    await asyncio.sleep(3)
    
    sym_bin = "XRPUSDT"
    sym_kuc = "XRPUSDTM"
    sym_bit = "XRPUSDT"
    trade_size_usd = 6.0
    
    async with session.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={sym_bin}") as r:
        bin_price = float((await r.json())["price"])
    async with session.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={sym_kuc}") as r:
        kuc_price = float((await r.json())["data"]["price"])
    async with session.get(f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={sym_bit}&productType=USDT-FUTURES") as r:
        bit_data = await r.json()
        bit_price = float(bit_data["data"][0]["lastPr"])
        
    print(f"\n>>> 2. Current prices: Binance = {bin_price}, Kucoin = {kuc_price}, Bitget = {bit_price}")
    
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
    try:
        await bitget_order.set_margin_type(sym_bit, "crossed")
        await bitget_order.set_leverage(sym_bit, 10, "crossed")
    except Exception as e:
        print(f"    Bitget margin setup: {e}")
        
    print(f"\n>>> 4. Opening positions with ~{trade_size_usd}$ (Binance LONG | Kucoin SHORT | Bitget SHORT)...")
    tasks = [
        binance_order.place_order(sym_bin, "BUY", trade_size_usd, bin_price * 1.05, order_type="MARKET", position_side="LONG"),
        kucoin_order.place_order(sym_kuc, "SELL", trade_size_usd, kuc_price * 0.95, order_type="MARKET", position_side="SHORT"),
        bitget_order.place_order(sym_bit, "SELL", trade_size_usd, bit_price * 0.95, order_type="MARKET", position_side="SHORT")
    ]
    
    open_res = await asyncio.gather(*tasks, return_exceptions=True)
    print(f"    Binance response: {open_res[0] if not isinstance(open_res[0], Exception) else 'ERROR: ' + str(open_res[0])}")
    print(f"    Kucoin response:  {open_res[1] if not isinstance(open_res[1], Exception) else 'ERROR: ' + str(open_res[1])}")
    print(f"    Bitget response:  {open_res[2] if not isinstance(open_res[2], Exception) else 'ERROR: ' + str(open_res[2])}")
    
    print("\n>>> 5. Waiting 2 sec for execution...")
    await asyncio.sleep(2.0)
    
    pos_b_exact = await binance_order.get_exact_position(sym_bin, "LONG")
    pos_k_exact = await kucoin_order.get_exact_position(sym_kuc, "SHORT")
    pos_bit_exact = await bitget_order.get_exact_position(sym_bit, "SHORT")
    print(f"    EXACT verified positions -> Binance: {pos_b_exact}")
    print(f"    EXACT verified positions -> Kucoin:  {pos_k_exact}")
    print(f"    EXACT verified positions -> Bitget:  {pos_bit_exact}")
    
    print("\n>>> 6. Closing positions (EMERGENCY MARKET)...")
    c_tasks = []
    if pos_b_exact["size"] > 0:
        c_tasks.append(binance_order.place_order(sym_bin, "SELL", pos_b_exact["size"] * bin_price * 0.90, bin_price * 0.90, order_type="MARKET", position_side="LONG"))
    if pos_k_exact["size"] > 0:
        c_tasks.append(kucoin_order.place_order(sym_kuc, "BUY", pos_k_exact["size"] * kuc_price * 1.10, kuc_price * 1.10, order_type="MARKET", position_side="SHORT"))
    if pos_bit_exact["size"] > 0:
        c_tasks.append(bitget_order.place_order(sym_bit, "BUY", pos_bit_exact["size"] * bit_price * 1.10, bit_price * 1.10, order_type="MARKET", position_side="SHORT"))
        
    c_res = await asyncio.gather(*c_tasks, return_exceptions=True)
    print(f"    Close responses: {c_res}")
    
    print("\n>>> 7. Waiting 2 sec for close settlement...")
    await asyncio.sleep(2.0)
    
    f_bin = await binance_order.get_exact_position(sym_bin, "LONG")
    f_kuc = await kucoin_order.get_exact_position(sym_kuc, "SHORT")
    f_bit = await bitget_order.get_exact_position(sym_bit, "SHORT")
    
    print(f"    Final Binance: {f_bin}")
    print(f"    Final Kucoin:  {f_kuc}")
    print(f"    Final Bitget:  {f_bit}")
    
    await session.close()
    await binance_ws.stop()
    await kucoin_ws.stop()
    await bitget_ws.stop()

if __name__ == "__main__":
    asyncio.run(main())
