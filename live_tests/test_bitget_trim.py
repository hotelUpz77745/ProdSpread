# ============================================================
# FILE: live_tests/test_bitget_trim.py
# ROLE: Bitget asymmetric position trimming test.
# ============================================================
import asyncio
import json
import os
import time
import aiohttp
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import BitgetOrder

async def main():
    load_dotenv()
    
    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
        
    symbol = "XRPUSDT"
    
    # We will buy 10 XRP initially, and then try to sell 4 XRP as a partial trim
    size_usd = 15.0 # roughly 10 XRP
    
    # Mock spec for Bitget XRPUSDT
    mock_bitget = [{"symbol": "XRPUSDT", "sizeMultiplier": 1.0, "volumePlace": 1, "pricePlace": 4}]
    
    async with aiohttp.ClientSession() as session:
        bitget = BitgetOrder(
            api_key=os.getenv("BITGET_API_KEY"),
            api_secret=os.getenv("BITGET_API_SECRET"),
            api_passphrase=os.getenv("BITGET_API_PASSPHRASE"),
            margin_settings=cfg["margin_settings"]["BITGET"],
            session=session,
            position_stream=None
        )
        bitget.symbol_info = mock_bitget
        
        # Get current price
        endpoint = f"/api/v2/mix/market/ticker?symbol={symbol}&productType=USDT-FUTURES"
        async with session.get(f"https://api.bitget.com{endpoint}") as resp:
            data = await resp.json()
            if data.get('data') and len(data['data']) > 0:
                current_price = float(data['data'][0]['lastPr'])
            else:
                print("Failed to get price from Bitget")
                return
                
        print(f"Текущая цена {symbol} на Bitget: {current_price}")
        
        try:
            print("\n=== STEP 1: Открываем LONG позицию (10 XRP) ===")
            qty_open = 10.0
            open_usd = qty_open * current_price
            
            # Открываем MARKET ордером, цена передается для расчетов размера
            res_open = await bitget.place_order(symbol, "BUY", open_usd, current_price, order_type="MARKET", position_side="LONG")
            print(f"Open Response: {res_open}")
            
            await asyncio.sleep(2)
            
            print("\n=== STEP 2: Частичная подрезка (Trim 4 XRP) ===")
            qty_trim = 4.0
            trim_usd = qty_trim * current_price
            
            # reduce_only=True должно направить нас на place-order с tradeSide="close", а не в _close_position
            res_trim = await bitget.place_order(symbol, "SELL", trim_usd, current_price, order_type="MARKET", position_side="LONG", reduce_only=True)
            print(f"Trim Response: {res_trim}")
            
            await asyncio.sleep(2)
            
        except Exception as e:
            print(f"Ошибка в процессе теста: {e}")
            
        finally:
            print("\n=== FINALLY: Закрываем все хвосты ===")
            try:
                close_res = await bitget._close_position(symbol, "long")
                print(f"Emergency Close Response: {close_res}")
            except Exception as e:
                print(f"Ошибка финального закрытия: {e}")

if __name__ == "__main__":
    asyncio.run(main())
