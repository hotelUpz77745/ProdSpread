#!/usr/bin/env python3
"""
ЖИВОЙ ТЕСТ: ОТКРЫТИЕ ПОЗИЦИЙ И ЗАМЕР ЗАДЕРЖКИ WS
==================================================
Скрипт:
1. Поднимает WS стримы 3 бирж
2. Открывает реальную позицию на 10 USDT (рыночным ордером) по XRP
3. Замеряет, через сколько миллисекунд WS-стрим покажет эту позицию
4. Закрывает позицию и замеряет задержку WS на закрытие
"""

import asyncio
import os
import sys
import time
import json
import aiohttp
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder


async def wait_ready(name: str, stream, timeout: float = 10.0):
    start = time.time()
    while time.time() - start < timeout:
        if stream.ready:
            return True
        await asyncio.sleep(0.05)
    return False


async def get_xrp_price(session):
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=XRPUSDT") as resp:
        data = await resp.json()
        return float(data["price"])


async def poll_ws_for_size(name, stream, symbol, side, expected_size_condition, timeout=3.0):
    """
    Поллит локальный кэш WS стрима каждые 5мс и возвращает задержку в мс, 
    когда условие выполнится, либо -1 по таймауту.
    """
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        # Для Kucoin/Bitget нужно обращаться к positions dict, как это делает get_executed_position
        pos = stream.get_position(symbol, side)
        size = pos.get("size", 0.0)
        
        if expected_size_condition(size):
            return (time.perf_counter() - start) * 1000, size
        await asyncio.sleep(0.005)
    
    return -1, stream.get_position(symbol, side).get("size", 0.0)


async def main():
    print("=" * 60)
    print("🚀 НАЧАЛО ЖИВОГО ТЕСТА ОТКРЫТИЯ/ЗАКРЫТИЯ ПОЗИЦИЙ")
    print("=" * 60)
    
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cfg.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    
    session = aiohttp.ClientSession()
    
    # 1. Стримы
    b_stream = BinancePositionStream(os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""))
    k_stream = KucoinPositionStream(os.environ.get("KUCOIN_API_KEY", ""), os.environ.get("KUCOIN_API_SECRET", ""), os.environ.get("KUCOIN_API_PASSPHRASE", ""))
    bg_stream = BitgetPositionStream(os.environ.get("BITGET_API_KEY", ""), os.environ.get("BITGET_API_SECRET", ""), os.environ.get("BITGET_API_PASSPHRASE", ""))
    
    # 2. Адаптеры ордеров
    b_order = BinanceOrder(os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""), session, b_stream)
    k_order = KucoinOrder(os.environ.get("KUCOIN_API_KEY", ""), os.environ.get("KUCOIN_API_SECRET", ""), os.environ.get("KUCOIN_API_PASSPHRASE", ""), session, k_stream, cfg["margin_settings"]["KUCOIN"])
    bg_order = BitgetOrder(os.environ.get("BITGET_API_KEY", ""), os.environ.get("BITGET_API_SECRET", ""), os.environ.get("BITGET_API_PASSPHRASE", ""), cfg["margin_settings"]["BITGET"], session, bg_stream)
    
    # 3. Стартуем стримы
    print("[*] Запуск WS стримов...")
    tasks = [
        asyncio.create_task(b_stream.start()),
        asyncio.create_task(k_stream.start()),
        asyncio.create_task(bg_stream.start())
    ]
    
    b_ok = await wait_ready("BINANCE", b_stream)
    k_ok = await wait_ready("KUCOIN", k_stream)
    bg_ok = await wait_ready("BITGET", bg_stream)
    
    if not (b_ok and k_ok and bg_ok):
        print("❌ Не удалось подключить все стримы. Выход.")
        return
        
    
    # 4. Берем цену
    price = await get_xrp_price(session)
    size_usd = 10.0 # 10 баксов на каждую биржу
    print(f"[*] Цена XRP: {price}. Будем открывать на {size_usd} USD.")
    
    sym_b = "XRPUSDT"
    sym_k = "XRPUSDTM"
    sym_bg = "XRPUSDT"
    
    # --- ОТКРЫТИЕ ---
    print("\n" + "-"*40)
    print("▶ ФАЗА 1: ОТКРЫТИЕ ПОЗИЦИЙ (MARKET)")
    print("-"*40)
    
    async def open_and_measure(name, order_adapter, stream, symbol, side="LONG"):
        print(f"  [{name}] Отправка ордера BUY MARKET {size_usd} USD...")
        
        # Отправляем ордер и сразу начинаем слушать WS
        start_req = time.perf_counter()
        
        try:
            await order_adapter.place_order(
                symbol=symbol, side="BUY", size_usd=size_usd, price=price, 
                order_type="MARKET", position_side="LONG"
            )
        except Exception as e:
            print(f"  ❌ [{name}] Ошибка отправки ордера: {e}")
            return
            
        req_time = (time.perf_counter() - start_req) * 1000
        print(f"  [{name}] Ордер отправлен (REST занял {req_time:.1f}ms). Ждём пуш от WS...")
        
        # Замеряем сколько времени после отправки REST-запроса потребуется WS чтобы обновить кэш
        delay, ws_size = await poll_ws_for_size(name, stream, symbol, "LONG", lambda s: s > 0, timeout=2.0)
        
        if delay != -1:
            print(f"  ✅ [{name}] WS обновился! Задержка: {delay:.1f}ms (size: {ws_size})")
        else:
            print(f"  ❌ [{name}] WS ТАК И НЕ ОБНОВИЛСЯ ЗА 2 СЕКУНДЫ! (size: {ws_size})")
            
            # Если не обновился, проверим REST
            rest_pos = await order_adapter.get_position_rest(symbol, "LONG")
            print(f"      [REST Check]: {rest_pos}")

    await asyncio.gather(
        open_and_measure("BINANCE", b_order, b_stream, sym_b),
        open_and_measure("KUCOIN", k_order, k_stream, sym_k),
        open_and_measure("BITGET", bg_order, bg_stream, sym_bg)
    )
    
    # Пауза
    print("\n[*] Пауза 3 сек перед закрытием...")
    await asyncio.sleep(3.0)
    
    # --- ЗАКРЫТИЕ ---
    print("\n" + "-"*40)
    print("▶ ФАЗА 2: ЗАКРЫТИЕ ПОЗИЦИЙ (MARKET)")
    print("-"*40)
    
    async def close_and_measure(name, order_adapter, stream, symbol, side="LONG"):
        print(f"  [{name}] Отправка ордера SELL MARKET (CLOSE)...")
        start_req = time.perf_counter()
        
        try:
            # Для Kucoin нужно передать closeOrder=True (в адаптере это position_side="LONG" и side="SELL")
            # Для Bitget адаптер сам вызовет _close_position если is_close
            await order_adapter.place_order(
                symbol=symbol, side="SELL", size_usd=size_usd, price=price, 
                order_type="MARKET", position_side="LONG"
            )
        except Exception as e:
            print(f"  ❌ [{name}] Ошибка закрытия: {e}")
            return
            
        req_time = (time.perf_counter() - start_req) * 1000
        print(f"  [{name}] Close-ордер отправлен (REST {req_time:.1f}ms). Ждём пуш от WS...")
        
        # Ждём пока size не станет 0
        delay, ws_size = await poll_ws_for_size(name, stream, symbol, "LONG", lambda s: s == 0, timeout=2.0)
        
        if delay != -1:
            print(f"  ✅ [{name}] WS обновился (закрыто)! Задержка: {delay:.1f}ms")
        else:
            print(f"  ❌ [{name}] WS ТАК И НЕ УВИДЕЛ ЗАКРЫТИЕ ЗА 2 СЕК! (size: {ws_size})")
            rest_pos = await order_adapter.get_position_rest(symbol, "LONG")
            print(f"      [REST Check]: {rest_pos}")
            
            # Emergency Unwind just in case
            if name == "BITGET":
                await order_adapter._close_position(symbol, "long")

    await asyncio.gather(
        close_and_measure("BINANCE", b_order, b_stream, sym_b),
        close_and_measure("KUCOIN", k_order, k_stream, sym_k),
        close_and_measure("BITGET", bg_order, bg_stream, sym_bg)
    )

    # Cleanup
    b_stream._external_stop = True
    k_stream._external_stop = True
    bg_stream._external_stop = True
    for t in tasks:
        t.cancel()
    await session.close()
    print("\n[V] ТЕСТ ЗАВЕРШЕН.")

if __name__ == "__main__":
    asyncio.run(main())
