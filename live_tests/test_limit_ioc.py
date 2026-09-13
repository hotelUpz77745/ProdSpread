# ============================================================
# FILE: live_tests/test_limit_ioc.py
# ROLE: LIMIT_IOC order placement and execution test across exchanges.
# ============================================================
import asyncio
import json
import os
import time
import aiohttp
from dotenv import load_dotenv

# Подключаем API из проекта
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder

async def main():
    load_dotenv()
    
    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
        
    symbol = "XRPUSDT"
    size_usd = 6.0
    
    # Mock specs to avoid starting background loops
    mock_binance = [{"symbol": "XRPUSDT", "filters": [{"filterType": "LOT_SIZE", "stepSize": "1"}, {"filterType": "PRICE_FILTER", "tickSize": "0.0001"}]}]
    mock_kucoin = [{"symbol": "XRPUSDTM", "lotSize": 1, "tickSize": 0.0001, "multiplier": 1.0, "maxLeverage": 20}]
    mock_bitget = [{"symbol": "XRPUSDT", "sizeMultiplier": 1.0, "volumePlace": 1, "pricePlace": 4}]
    
    async with aiohttp.ClientSession() as session:
        print("=== Инициализация коннекторов ===")
        
        binance = BinanceOrder(
            api_key=os.getenv("BINANCE_API_KEY"),
            api_secret=os.getenv("BINANCE_API_SECRET"),
            session=session
        )
        binance.symbol_info = mock_binance
        
        kucoin = KucoinOrder(
            api_key=os.getenv("KUCOIN_API_KEY"),
            api_secret=os.getenv("KUCOIN_API_SECRET"),
            api_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"),
            margin_settings=cfg["margin_settings"]["KUCOIN"],
            session=session,
            position_stream=None
        )
        kucoin.symbol_info = mock_kucoin
        
        bitget = BitgetOrder(
            api_key=os.getenv("BITGET_API_KEY"),
            api_secret=os.getenv("BITGET_API_SECRET"),
            api_passphrase=os.getenv("BITGET_API_PASSPHRASE"),
            margin_settings=cfg["margin_settings"]["BITGET"],
            session=session,
            position_stream=None
        )
        bitget.symbol_info = mock_bitget
        
        # Запрашиваем текущую цену на Binance
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=XRPUSDT") as resp:
            data = await resp.json()
            current_price = float(data["price"])
            
        print(f"\nТекущая рыночная цена XRPUSDT: {current_price}")
        
        # Для гарантированного исполнения LIMIT_IOC (BUY) цена должна быть ВЫШЕ рынка
        # Берем цену на 2% выше текущей
        aggressive_price = current_price * 1.02
        
        print(f"Агрессивная тестовая цена BUY для {symbol}: {aggressive_price:.4f} (+2% к рынку, ожидаем мгновенный налив)")
        
        try:
            print("\n-> BINANCE (EXECUTING AGGRESSIVE LIMIT_IOC)")
            b_res = await binance.place_order(symbol, "BUY", size_usd, aggressive_price, order_type="LIMIT_IOC", position_side="LONG")
            print(f"Binance Response: {b_res}")
            
            # Ждем немного и запрашиваем статус
            await asyncio.sleep(1)
            order_id = b_res.get("orderId")
            
            # Делаем ручной REST GET для ордера
            timestamp = int(time.time() * 1000)
            qs = f"symbol={symbol}&orderId={order_id}&timestamp={timestamp}"
            sig = binance._generate_signature(qs)
            
            async with session.get(f"https://fapi.binance.com/fapi/v1/order?{qs}&signature={sig}", headers={"X-MBX-APIKEY": binance.api_key}) as resp:
                check_data = await resp.json()
                print(f"Binance Order Check: {check_data}")
                
                executed_qty = float(check_data.get("executedQty", 0.0))
                if executed_qty > 0:
                    print(f"SUCCESS: Order filled! Executed Qty: {executed_qty} XRP")
                    print("Closing position via MARKET order...")
                    close_res = await binance.place_order(symbol, "SELL", size_usd, aggressive_price, order_type="MARKET", position_side="LONG")
                    print(f"Close Response: {close_res}")
                else:
                    print("FAILURE: Order did not fill (executedQty = 0)")
                    
        except Exception as e:
            print(f"Binance ERROR: {e}")

        # ============================================================
        # KUCOIN TEST
        # ============================================================
        print("\n" + "=" * 50)
        print("-> KUCOIN (TESTING AGGRESSIVE LIMIT_IOC)")
        print("=" * 50)
        ku_sym = "XRPUSDTM"
        
        try:
            # Fetch active contracts to get real specs for XRPUSDTM
            async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
                data = await resp.json()
                if data.get("code") == "200000":
                    kucoin.symbol_info = data.get("data", [])
            
            # Fetch Kucoin ticker
            async with session.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={ku_sym}") as resp:
                data = await resp.json()
                ku_ticker = data.get("data", {})
                ku_price = float(ku_ticker.get("price", 0.0))
                
            print(f"Текущая рыночная цена {ku_sym} на KuCoin: {ku_price}")
            if ku_price <= 0:
                print("ERROR: Kucoin ticker returned 0 price.")
                return
                
            ku_aggressive_price = ku_price * 1.02
            print(f"Агрессивная тестовая цена BUY для KuCoin: {ku_aggressive_price:.4f} (+2% к рынку)")
            
            # Switch margin mode to CROSS (as leverage_setter does)
            await kucoin.set_margin_type(ku_sym, "CROSS", leverage=20)
            
            # Place LIMIT_IOC on KuCoin (XRP contract is 10 XRP = ~$13.5, so we need >= $15)
            ku_size_usd = 16.0
            k_res = await kucoin.place_order(ku_sym, "BUY", ku_size_usd, ku_aggressive_price, order_type="LIMIT_IOC", position_side="LONG")
            print(f"KuCoin Place Response: {k_res}")
            
            await asyncio.sleep(1.0)
            
            # Verify fill via get_position_rest
            pos = await kucoin.get_position_rest(ku_sym, "LONG")
            print(f"KuCoin Position Check: {pos}")
            
            pos_size = float(pos.get("size", 0.0))
            if pos_size > 0:
                print(f"SUCCESS: KuCoin Order filled! Position size: {pos_size}")
                print("Closing KuCoin position via MARKET order...")
                close_k = await kucoin.place_order(ku_sym, "SELL", ku_size_usd, ku_aggressive_price, order_type="MARKET", position_side="LONG", reduce_only=True)
                print(f"KuCoin Close Response: {close_k}")
                await asyncio.sleep(1.0)
                pos_after = await kucoin.get_position_rest(ku_sym, "LONG")
                print(f"KuCoin Position after close: {pos_after}")
            else:
                print("FAILURE: KuCoin Order did not fill (Position size = 0).")
                
        except Exception as e:
            print(f"KuCoin ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
