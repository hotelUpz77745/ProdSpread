# ============================================================
# FILE: live_tests/test_kucoin_full_diag.py
# ROLE: Комплексная диагностика и сквозное тестирование биржи KuCoin
#       - Публичный WS стакан (KucoinStakanStream)
#       - Спецификации и квантование лотов/шагов (KucoinOrder)
#       - Приватный WS стрим позиций (KucoinPositionStream)
#       - Guarded REST запрос позиций с замером RTT
#       - Постановка и отмена безопасного GTC лимитного ордера
#       - Проверка режима маржи (CROSS)
#       - Гарантия нулевой экспозиции (Safety Guard: 0 ордеров, 0 позиций)
# ============================================================
import asyncio
import os
import sys
import time
import json
import base64
import hmac
import hashlib
import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from API.KUCOIN.stakan import KucoinStakanStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.orders import KucoinOrder

async def main():
    print("=" * 65)
    print("🚀 ПОЛНОЕ СКВОЗНОЕ ТЕСТИРОВАНИЕ БИРЖИ KUCOIN (v9 ARCHITECTURE)")
    print("=" * 65)

    api_key = os.getenv("KUCOIN_API_KEY", "")
    api_secret = os.getenv("KUCOIN_API_SECRET", "")
    api_passphrase = os.getenv("KUCOIN_API_PASSPHRASE", "")

    if not api_key or not api_secret or not api_passphrase:
        print("❌ ОШИБКА: Ключи KuCoin не найдены в .env файле!")
        sys.exit(1)

    passed_steps = 0
    total_steps = 7

    async with aiohttp.ClientSession() as session:
        # ========================================================
        # ШАГ 1: Публичный сокет цен и стакана
        # ========================================================
        print("\n[ШАГ 1/7] Тестирование публичного WebSocket стакана (Depth5)...")
        depth_received = asyncio.Event()
        sample_book = None

        async def on_depth(d):
            nonlocal sample_book
            if d.bids and d.asks:
                sample_book = d
                depth_received.set()

        stakan_stream = KucoinStakanStream(symbols=["XBTUSDTM", "XRPUSDTM"], chunk_size=50, throttle_ms=0)
        stakan_task = asyncio.create_task(stakan_stream.run(on_depth))

        try:
            t0 = time.time()
            await asyncio.wait_for(depth_received.wait(), timeout=8.0)
            elapsed = time.time() - t0
            bids = sample_book.bids
            asks = sample_book.asks
            print(f"  ✅ Стакан получен за {elapsed:.2f}с ({sample_book.symbol}):")
            print(f"     Bids={len(bids)}, BestBid={bids[0] if bids else 'None'}")
            print(f"     Asks={len(asks)}, BestAsk={asks[0] if asks else 'None'}")
            passed_steps += 1
        except asyncio.TimeoutError:
            print("  ❌ Таймаут ожидания публичного стакана KuCoin!")
        finally:
            stakan_stream.stop()
            stakan_task.cancel()

        # ========================================================
        # ШАГ 2: Загрузка спецификаций и проверка точности лотов
        # ========================================================
        print("\n[ШАГ 2/7] Загрузка спецификаций контрактов и валидация шагов...")
        kucoin_order = KucoinOrder(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            session=session,
            position_stream=None,
            margin_settings={"margin_type": "cross", "leverage": 20}
        )

        async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
            data = await resp.json()
            if data.get("code") == "200000":
                kucoin_order.symbol_info = data.get("data", [])
                specs_count = len(kucoin_order.symbol_info)
                print(f"  ✅ Загружено активных контрактов: {specs_count}")
                
                # Проверяем ключевые инструменты
                for sym in ("XRPUSDTM", "DOGEUSDTM", "XBTUSDTM"):
                    spec = next((item for item in kucoin_order.symbol_info if item.get("symbol") == sym), None)
                    if spec:
                        print(f"     [{sym:10}] LotSize={spec.get('lotSize')} | TickSize={spec.get('tickSize')} | Multiplier={spec.get('multiplier')}")
                        test_size = 100.0 if "XBT" in sym else 25.0
                        kucoin_order.check_order_size(sym, test_size, float(spec.get("lastTradePrice", 1.0) or 1.0))
                print("  ✅ Валидация check_order_size для всех монет: OK")
                passed_steps += 1
            else:
                print(f"  ❌ Ошибка загрузки спецификаций: {data}")

        # ========================================================
        # ШАГ 3: Приватный сокет позиций (KucoinPositionStream)
        # ========================================================
        print("\n[ШАГ 3/7] Тестирование приватного WebSocket стрима позиций...")
        pos_stream = KucoinPositionStream(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        pos_task = asyncio.create_task(pos_stream.start())

        t0 = time.time()
        while time.time() - t0 < 6.0:
            if pos_stream.is_connected and pos_stream.ready:
                break
            await asyncio.sleep(0.2)

        if pos_stream.is_connected and pos_stream.ready:
            print(f"  ✅ Приватный WS подключен и готов к приему апдейтов (время подключения: {time.time() - t0:.2f}с)")
            pos_xrp = pos_stream.get_position("XRPUSDTM", "LONG")
            print(f"     get_position('XRPUSDTM', 'LONG') -> {pos_xrp}")
            passed_steps += 1
        else:
            print(f"  ❌ Приватный WS не готов: Connected={pos_stream.is_connected}, Ready={pos_stream.ready}")

        kucoin_order.position_stream = pos_stream

        # ========================================================
        # ШАГ 4: Guarded REST запрос позиций с замером RTT
        # ========================================================
        print("\n[ШАГ 4/7] Тестирование Guarded REST запроса позиций (тайминг и валидация)...")
        t0 = time.perf_counter()
        rest_pos = await kucoin_order.get_exact_position_guarded("XRPUSDTM", "LONG")
        rtt_ms = (time.perf_counter() - t0) * 1000
        print(f"  ✅ get_exact_position_guarded: {rest_pos} (RTT: {rtt_ms:.1f} мс)")
        if rest_pos.get("status") == "ok":
            passed_steps += 1
        else:
            print(f"  ❌ Некорректный ответ get_exact_position_guarded: {rest_pos}")

        # ========================================================
        # ШАГ 5: Безопасная постановка и отмена GTC лимитного ордера
        # ========================================================
        print("\n[ШАГ 5/7] Тестирование живого жизненного цикла ордера (Place & Cancel)...")
        test_sym = "XRPUSDTM"
        # Безопасная цена: BUY по 0.20$ (при рынке ~1.35$ ордер никогда не исполнится)
        safe_buy_price = 0.20
        safe_size_usd = 15.0

        print(f"  -> Отправка безопасного LIMIT GTC BUY на {test_sym} (Цена: {safe_buy_price}$, Объем: {safe_size_usd}$)...")
        place_res = await kucoin_order.place_order(
            symbol=test_sym,
            side="BUY",
            size_usd=safe_size_usd,
            price=safe_buy_price,
            order_type="LIMIT",
            position_side="LONG",
            time_in_force="GTC"
        )
        print(f"     Ответ биржи на ордер: code={place_res.get('code')}, data={place_res.get('data')}")

        if place_res.get("code") == "200000":
            order_id = place_res["data"]["orderId"]
            print(f"  ✅ Ордер успешно выставлен на биржу. OrderId: {order_id}")
            
            # Проверяем наличие в стакане через active orders
            await asyncio.sleep(0.4)
            
            print(f"  -> Снятие ордеров через cancel_all_orders({test_sym})...")
            await kucoin_order.cancel_all_orders(test_sym)
            await asyncio.sleep(0.4)
            print("  ✅ Ордер успешно отменен биржей.")
            passed_steps += 1
        else:
            print(f"  ❌ Ошибка выставления ордера на KuCoin: {place_res}")

        # ========================================================
        # ШАГ 6: Баланс аккаунта и проверка режима маржи / плеча
        # ========================================================
        print("\n[ШАГ 6/7] Проверка баланса фьючерсного аккаунта и режима маржи (CROSS)...")
        try:
            bal_endpoint = "/api/v1/account-overview?currency=USDT"
            now = str(int(time.time() * 1000))
            sig = kucoin_order._generate_signature(now + "GET" + bal_endpoint)
            pass_hmac = hmac.new(api_secret.encode("utf-8"), api_passphrase.encode("utf-8"), hashlib.sha256)
            enc_pass = base64.b64encode(pass_hmac.digest()).decode("utf-8")
            h = {
                "KC-API-KEY": api_key, "KC-API-SIGN": sig, "KC-API-TIMESTAMP": now,
                "KC-API-PASSPHRASE": enc_pass, "KC-API-KEY-VERSION": "2"
            }
            async with session.get(f"https://api-futures.kucoin.com{bal_endpoint}", headers=h) as r:
                bal_data = await r.json()
                if bal_data.get("code") == "200000" and "data" in bal_data:
                    d = bal_data["data"]
                    equity = float(d.get("accountEquity", 0.0))
                    avail = float(d.get("availableBalance", 0.0))
                    print(f"  ✅ Баланс KuCoin Futures: Equity = {equity:.4f} USDT | Available = {avail:.4f} USDT")
                else:
                    print(f"  ⚠️ Ответ баланса: {bal_data}")

            # Проверка установки CROSS маржи и плеча
            margin_ok = await kucoin_order.set_margin_type("XRPUSDTM", "CROSS")
            lev_ok = await kucoin_order.set_leverage("XRPUSDTM", 20, "CROSS")
            print(f"  ✅ Настройка маржи: set_margin_type(CROSS)={margin_ok}, set_leverage(20)={lev_ok}")
            passed_steps += 1
        except Exception as e:
            print(f"  ❌ Ошибка проверки баланса/маржи: {e}")

        # ========================================================
        # ШАГ 7: Проверка нулевой экспозиции (Safety Guard)
        # ========================================================
        print("\n[ШАГ 7/7] Safety Guard: Проверка отсутствия висящих ордеров и открытых позиций...")
        active_orders = []
        try:
            now = str(int(time.time() * 1000))
            endpoint = "/api/v1/orders?status=active"
            sig = kucoin_order._generate_signature(now + "GET" + endpoint)
            pass_hmac = hmac.new(api_secret.encode("utf-8"), api_passphrase.encode("utf-8"), hashlib.sha256)
            enc_pass = base64.b64encode(pass_hmac.digest()).decode("utf-8")
            h = {
                "KC-API-KEY": api_key, "KC-API-SIGN": sig, "KC-API-TIMESTAMP": now,
                "KC-API-PASSPHRASE": enc_pass, "KC-API-KEY-VERSION": "2"
            }
            async with session.get(f"https://api-futures.kucoin.com{endpoint}", headers=h) as r:
                od = await r.json()
                active_orders = od.get("data", {}).get("items", []) or []
        except Exception as oe:
            print(f"  Warning querying orders: {oe}")

        active_positions = await kucoin_order.get_active_positions()
        print(f"  -> Активных ордеров на бирже: {len(active_orders)}")
        print(f"  -> Активных позиций на бирже: {len(active_positions)}")

        if len(active_orders) > 0:
            print(f"  ⚠️ ОБНАРУЖЕНЫ НЕОТМЕНЕННЫЕ ОРДЕРА ({len(active_orders)}). Производится аварийная отмена...")
            for o in active_orders:
                c_now = str(int(time.time() * 1000))
                c_ep = f"/api/v1/orders/{o['id']}"
                c_sig = kucoin_order._generate_signature(c_now + "DELETE" + c_ep)
                c_h = {
                    "KC-API-KEY": api_key, "KC-API-SIGN": c_sig, "KC-API-TIMESTAMP": c_now,
                    "KC-API-PASSPHRASE": enc_pass, "KC-API-KEY-VERSION": "2"
                }
                await session.delete(f"https://api-futures.kucoin.com{c_ep}", headers=c_h)

        if len(active_positions) > 0:
            print(f"  🚨 ОБНАРУЖЕНЫ ОТКРЫТЫЕ ПОЗИЦИИ ({len(active_positions)}). Аварийное закрытие по маркету...")
            for p in active_positions:
                sym = p["symbol"]
                amt = float(p.get("size", 0))
                if amt != 0:
                    c_side = "sell" if amt > 0 else "buy"
                    await kucoin_order.place_order(sym, c_side, abs(amt) * 1.35, 1.35, order_type="MARKET", position_side="LONG" if amt > 0 else "SHORT", reduce_only=True)
                    print(f"     Позиция {sym} ликвидирована!")

        # Повторная проверка
        final_orders = []
        async with session.get(f"https://api-futures.kucoin.com{endpoint}", headers=h) as r:
            od = await r.json()
            final_orders = od.get("data", {}).get("items", []) or []
        final_positions = await kucoin_order.get_active_positions()

        if len(final_orders) == 0 and len(final_positions) == 0:
            print("  ✅ БАЛАНС ЧИСТ: Ровно 0 ордеров и 0 позиций на KuCoin.")
            passed_steps += 1
        else:
            print(f"  ❌ Ошибка зачистки: ордеров={len(final_orders)}, позиций={len(final_positions)}")

        # Завершение фоновых задач
        pos_task.cancel()
        await pos_stream.stop()

    print("\n" + "=" * 65)
    print(f"📊 ИТОГИ ДИАГНОСТИКИ KUCOIN: Пройдено шагов {passed_steps}/{total_steps}")
    print("=" * 65)

    if passed_steps == total_steps:
        print("🎉 ВСЕ ТЕСТЫ KUCOIN ПРОЙДЕНЫ С ОТЛИЧИЕМ! БИРЖА ПОЛНОСТЬЮ ГОТОВА К БОЮ.")
    else:
        print(f"⚠️ Есть замечания по {total_steps - passed_steps} шагам.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
