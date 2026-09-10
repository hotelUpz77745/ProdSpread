# ============================================================
# FILE: test_limit_gtc_mechanism.py
# ROLE: Тестирование механизма LIMIT_GTC (заброс лимитки, пауза, отмена) на Binance, KuCoin и Bitget.
# ============================================================

import asyncio
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()

from c_log import log
from utils import SessionManager
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder

class LimitGtcTester:
    def __init__(self):
        with open("cfg.json", "r", encoding="utf-8") as f:
            self.cfg = json.load(f)
            
        self.session = None
        self.binance_order = None
        self.kucoin_order = None
        self.bitget_order = None

    async def init(self):
        self.session = await SessionManager().get_session()
        
        self.binance_order = BinanceOrder(
            api_key=os.environ["BINANCE_API_KEY"],
            api_secret=os.environ["BINANCE_API_SECRET"],
            session=self.session
        )
        
        self.kucoin_order = KucoinOrder(
            api_key=os.environ["KUCOIN_API_KEY"],
            api_secret=os.environ["KUCOIN_API_SECRET"],
            api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
            margin_settings=self.cfg["margin_settings"]["KUCOIN"],
            session=self.session,
            position_stream=None
        )
        
        self.bitget_order = BitgetOrder(
            api_key=os.environ["BITGET_API_KEY"],
            api_secret=os.environ["BITGET_API_SECRET"],
            api_passphrase=os.environ["BITGET_API_PASSPHRASE"],
            margin_settings=self.cfg["margin_settings"]["BITGET"],
            session=self.session
        )
        
        self.binance_order.start()
        self.kucoin_order.start()
        await self.bitget_order.start()
        
        for _ in range(50):
            if self.binance_order.symbol_info and self.kucoin_order.symbol_info and self.bitget_order.symbol_info:
                break
            await asyncio.sleep(0.1)

    async def test_binance(self) -> bool:
        print("\n" + "="*50)
        print(">>> ТЕСТИРОВАНИЕ LIMIT_GTC НА BINANCE")
        symbol = "XRPUSDT"
        
        # Получаем актуальный тикер
        url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
        async with self.session.get(url) as resp:
            data = await resp.json()
            bid_price = float(data["bidPrice"])
            
        # Ставим лимитку на 3% ниже лучшего бида (чтобы не налило, но прошло PERCENT_PRICE фильтр)
        safe_price = round(bid_price * 0.97, 4)
        size_usd = 6.0  # Минимальный размер ордера > 5$
        
        print(f"[Binance] Best Bid: {bid_price}, Safe Limit Price: {safe_price} (-3%), Size: {size_usd}$")
        
        try:
            # 1. Заброс лимитки GTC
            order_res = await self.binance_order.place_order(
                symbol=symbol,
                side="BUY",
                size_usd=size_usd,
                price=safe_price,
                order_type="LIMIT",
                position_side="LONG",
                time_in_force="GTC"
            )
            order_id = order_res.get("orderId")
            print(f"[Binance] ✅ Лимитка успешно выставлена! OrderId: {order_id}, Status: {order_res.get('status')}")
            
            # 2. Пауза
            pause_sec = float(self.cfg["EXECUTION_PAUSE"])
            print(f"[Binance] Пауза {pause_sec} сек...")
            await asyncio.sleep(pause_sec)
            
            # 3. Отмена всех ордеров
            await self.binance_order.cancel_all_orders(symbol)
            print(f"[Binance] ✅ Вызвана отмена всех открытых ордеров по {symbol}")
            
            # 4. Проверка, что ордеров не осталось
            check_url = f"https://fapi.binance.com/fapi/v1/openOrders?symbol={symbol}&timestamp={int(time.time()*1000)}"
            sig = self.binance_order._generate_signature(f"symbol={symbol}&timestamp={int(time.time()*1000)}")
            # Проверим через API
            headers = {"X-MBX-APIKEY": self.binance_order.api_key}
            ts = int(time.time()*1000)
            qs = f"symbol={symbol}&timestamp={ts}"
            sig = self.binance_order._generate_signature(qs)
            async with self.session.get(f"https://fapi.binance.com/fapi/v1/openOrders?{qs}&signature={sig}", headers=headers) as resp:
                open_orders = await resp.json()
                print(f"[Binance] Оставшихся открытых ордеров: {len(open_orders)}")
                assert len(open_orders) == 0, f"Остались открытые ордера: {open_orders}"
                print("[Binance] 🏆 ТЕСТ BINANCE LIMIT_GTC УСПЕШНО ПРОЙДЕН!")
                return True
        except Exception as e:
            print(f"[Binance] 🚨 ОШИБКА: {e}")
            await self.binance_order.cancel_all_orders(symbol)
            return False

    async def test_kucoin(self) -> bool:
        print("\n" + "="*50)
        print(">>> ТЕСТИРОВАНИЕ LIMIT_GTC НА KUCOIN")
        symbol = "XRPUSDTM"
        
        # Получаем актуальный тикер
        url = f"https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"
        async with self.session.get(url) as resp:
            res_data = await resp.json()
            bid_price = float(res_data["data"]["bestBidPrice"])
            
        safe_price = round(bid_price * 0.97, 4)
        # На KuCoin 1 контракт XRP = 10 XRP. При цене ~1.4$ 1 контракт ~ 14$. Ставим 15$
        size_usd = 15.0
        
        print(f"[Kucoin] Best Bid: {bid_price}, Safe Limit Price: {safe_price} (-3%), Size: {size_usd}$")
        
        try:
            # 1. Заброс лимитки GTC
            order_res = await self.kucoin_order.place_order(
                symbol=symbol,
                side="BUY",
                size_usd=size_usd,
                price=safe_price,
                order_type="LIMIT",
                position_side="LONG",
                time_in_force="GTC"
            )
            order_id = order_res.get("data", {}).get("orderId")
            print(f"[Kucoin] ✅ Лимитка успешно выставлена! OrderId: {order_id}")
            
            # 2. Пауза
            pause_sec = float(self.cfg["EXECUTION_PAUSE"])
            print(f"[Kucoin] Пауза {pause_sec} сек...")
            await asyncio.sleep(pause_sec)
            
            # 3. Отмена всех ордеров
            await self.kucoin_order.cancel_all_orders(symbol)
            print(f"[Kucoin] ✅ Вызвана отмена всех открытых ордеров по {symbol}")
            
            # 4. Проверка отсутствия открытых ордеров
            await asyncio.sleep(0.5)
            # Запрос открытых ордеров
            endpoint = f"/api/v1/orders?symbol={symbol}&status=active"
            now = str(int(time.time() * 1000))
            sig = self.kucoin_order._generate_signature(now + "GET" + endpoint)
            import hmac, hashlib, base64
            passphrase_hmac = hmac.new(self.kucoin_order.api_secret.encode('utf-8'), self.kucoin_order.api_passphrase.encode('utf-8'), hashlib.sha256)
            enc_pass = base64.b64encode(passphrase_hmac.digest()).decode('utf-8')
            headers = {
                'KC-API-KEY': self.kucoin_order.api_key,
                'KC-API-SIGN': sig,
                'KC-API-TIMESTAMP': now,
                'KC-API-PASSPHRASE': enc_pass,
                'KC-API-KEY-VERSION': '2'
            }
            async with self.session.get(f"https://api-futures.kucoin.com{endpoint}", headers=headers) as resp:
                data = await resp.json()
                open_items = data.get("data", {}).get("items", [])
                print(f"[Kucoin] Оставшихся открытых ордеров: {len(open_items)}")
                assert len(open_items) == 0, f"Остались открытые ордера: {open_items}"
                print("[Kucoin] 🏆 ТЕСТ KUCOIN LIMIT_GTC УСПЕШНО ПРОЙДЕН!")
                return True
        except Exception as e:
            print(f"[Kucoin] 🚨 ОШИБКА: {e}")
            await self.kucoin_order.cancel_all_orders(symbol)
            return False

    async def test_bitget(self) -> bool:
        print("\n" + "="*50)
        print(">>> ТЕСТИРОВАНИЕ LIMIT_GTC НА BITGET")
        symbol = "XRPUSDT"
        
        # Получаем актуальный тикер
        url = f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol}&productType=USDT-FUTURES"
        async with self.session.get(url) as resp:
            res_data = await resp.json()
            bid_price = float(res_data["data"][0]["bidPr"])
            
        safe_price = round(bid_price * 0.97, 4)
        size_usd = 6.0
        
        print(f"[Bitget] Best Bid: {bid_price}, Safe Limit Price: {safe_price} (-3%), Size: {size_usd}$")
        
        try:
            # 1. Заброс лимитки GTC
            order_res = await self.bitget_order.place_order(
                symbol=symbol,
                side="BUY",
                size_usd=size_usd,
                price=safe_price,
                order_type="LIMIT",
                position_side="LONG",
                time_in_force="GTC"
            )
            order_id = order_res.get("data", {}).get("orderId")
            print(f"[Bitget] ✅ Лимитка успешно выставлена! OrderId: {order_id}")
            
            # 2. Пауза
            pause_sec = float(self.cfg["EXECUTION_PAUSE"])
            print(f"[Bitget] Пауза {pause_sec} сек...")
            await asyncio.sleep(pause_sec)
            
            # 3. Отмена всех ордеров
            await self.bitget_order.cancel_all_orders(symbol)
            print(f"[Bitget] ✅ Вызвана отмена всех открытых ордеров по {symbol}")
            
            # 4. Проверка отсутствия открытых ордеров
            await asyncio.sleep(0.5)
            endpoint = f"/api/v2/mix/order/orders-pending?symbol={symbol}&productType=USDT-FUTURES"
            now = str(int(time.time() * 1000))
            sig = self.bitget_order._generate_signature(now, "GET", endpoint)
            headers = {
                'ACCESS-KEY': self.bitget_order.api_key,
                'ACCESS-SIGN': sig,
                'ACCESS-TIMESTAMP': now,
                'ACCESS-PASSPHRASE': self.bitget_order.api_passphrase
            }
            async with self.session.get(f"https://api.bitget.com{endpoint}", headers=headers) as resp:
                data = await resp.json()
                open_items = data.get("data", {}).get("entrustedList", []) or []
                print(f"[Bitget] Оставшихся открытых ордеров: {len(open_items)}")
                assert len(open_items) == 0, f"Остались открытые ордера: {open_items}"
                print("[Bitget] 🏆 ТЕСТ BITGET LIMIT_GTC УСПЕШНО ПРОЙДЕН!")
                return True
        except Exception as e:
            print(f"[Bitget] 🚨 ОШИБКА: {e}")
            await self.bitget_order.cancel_all_orders(symbol)
            return False

async def main():
    tester = LimitGtcTester()
    await tester.init()
    
    b_ok = await tester.test_binance()
    k_ok = await tester.test_kucoin()
    bg_ok = await tester.test_bitget()
    
    print("\n" + "="*50)
    print("ИТОГИ ТЕСТИРОВАНИЯ РЕЖИМА LIMIT_GTC:")
    print(f"  Binance: {'[PASS]' if b_ok else '[FAIL]'}")
    print(f"  KuCoin:  {'[PASS]' if k_ok else '[FAIL]'}")
    print(f"  Bitget:  {'[PASS]' if bg_ok else '[FAIL]'}")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
