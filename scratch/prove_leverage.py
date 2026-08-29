import asyncio
import os
import sys
import aiohttp
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import KucoinOrder

async def test_real_order():
    load_dotenv()
    
    async with aiohttp.ClientSession() as session:
        api_key = os.getenv("KUCOIN_API_KEY")
        api_secret = os.getenv("KUCOIN_API_SECRET")
        api_passphrase = os.getenv("KUCOIN_API_PASSPHRASE")
        
        kucoin = KucoinOrder(
            api_key, api_secret, api_passphrase, session, None, 
            margin_settings={"leverage": 11, "margin_type": "ISOLATED"}
        )
        
        print("Загружаем спеки биржи...")
        for _ in range(30):
            if kucoin.symbol_info:
                break
            await asyncio.sleep(0.5)
        else:
            print("Спеки не загрузились за 15 сек!")
            return
        
        symbol = "XRPUSDTM"
        
        # Узнаем tickSize и minPrice для XRPUSDTM
        spec = next((s for s in kucoin.symbol_info if s.get('symbol') == symbol), None)
        if spec:
            print(f"Specs: tickSize={spec.get('tickSize')}, multiplier={spec.get('multiplier')}, lotSize={spec.get('lotSize')}, minOrderQty={spec.get('minOrderQty')}")
        
        print(f"\n1. Переключаем на ISOLATED с плечом 11...")
        await kucoin.set_margin_type(symbol, "ISOLATED", leverage=11)
        print("OK")
        
        print(f"\n2. Выставляем ордер на {symbol} с size_usd=20, price=1.0...")
        try:
            resp = await kucoin.place_order(
                symbol=symbol,
                side="buy",
                order_type="limit",
                size_usd=20,
                price=1.0,   # XRP ~2.5$, цена 1.0$ не исполнится
                position_side=""
            )
            print("Ордер выставлен!")
            print(f"Зайдите в Kucoin -> Open Orders -> {symbol}. Ордер должен быть с плечом 11x!")
            
            print("Ордер провисит 30 секунд...")
            for i in range(30, 0, -1):
                print(f"  {i}...", end="\r")
                await asyncio.sleep(1)
                
            print("\nОтменяем...")
            await kucoin.cancel_all_orders(symbol)
            print("Готово.")
        except Exception as e:
            print(f"Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(test_real_order())
