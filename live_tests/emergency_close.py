# ============================================================
# FILE: live_tests/emergency_close.py
# ROLE: Diagnostic script to immediately emergency close exposed positions.
# ============================================================
import asyncio
import os
import time
import aiohttp
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import BinanceOrder

async def close_position():
    load_dotenv()
    
    async with aiohttp.ClientSession() as session:
        binance = BinanceOrder(
            api_key=os.getenv("BINANCE_API_KEY"),
            api_secret=os.getenv("BINANCE_API_SECRET"),
            session=session
        )
        
        timestamp = int(time.time() * 1000)
        qs = f"symbol=XRPUSDT&timestamp={timestamp}"
        sig = binance._generate_signature(qs)
        
        async with session.get(f"https://fapi.binance.com/fapi/v2/positionRisk?{qs}&signature={sig}", headers={"X-MBX-APIKEY": binance.api_key}) as resp:
            data = await resp.json()
            print("Current positions:", data)
            
            if isinstance(data, list):
                for pos in data:
                    amt = float(pos.get("positionAmt", 0))
                    side = pos.get("positionSide")
                    if amt != 0:
                        print(f"Found open position: {amt} {side}")
                        try:
                            trade_side = "SELL" if amt > 0 else "BUY"
                            qty = abs(amt)
                            
                            close_qs = f"symbol=XRPUSDT&side={trade_side}&positionSide={side}&type=MARKET&quantity={qty}&timestamp={int(time.time()*1000)}"
                            close_sig = binance._generate_signature(close_qs)
                            
                            async with session.post(f"https://fapi.binance.com/fapi/v1/order?{close_qs}&signature={close_sig}", headers={"X-MBX-APIKEY": binance.api_key}) as c_resp:
                                c_data = await c_resp.json()
                                print("Close response:", c_data)
                        except Exception as e:
                            print(f"Error closing: {e}")

if __name__ == "__main__":
    asyncio.run(close_position())
