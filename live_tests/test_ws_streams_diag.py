#!/usr/bin/env python3
"""
ДИАГНОСТИКА WS СТРИМОВ ПОЗИЦИЙ
================================
Тест поднимает приватные WS стримы всех бирж и:
1. Проверяет что стримы коннектятся и помечаются ready
2. Ждёт 5с и дампит содержимое positions dict для каждого стрима
3. Если есть открытые позиции на биржах — покажет видит ли их WS
4. Параллельно дёргает REST для сравнения (что REST видит vs что WS видит)

Запуск на сервере:
    cd /opt/ProdSpread && python live_tests/test_ws_streams_diag.py
"""

import asyncio
import os
import sys
import time
import json
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.BITGET.ws_private_bitget import BitgetPositionStream
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder


async def wait_ready(name: str, stream, timeout: float = 10.0):
    """Ждём ready=True на стриме."""
    start = time.time()
    while time.time() - start < timeout:
        if stream.ready:
            elapsed = (time.time() - start) * 1000
            print(f"  ✅ [{name}] WS stream READY за {elapsed:.0f}ms")
            return True
        await asyncio.sleep(0.05)
    print(f"  ❌ [{name}] WS stream НЕ READY за {timeout}с! is_connected={stream.is_connected}")
    return False


async def dump_stream_positions(name: str, stream):
    """Дампит внутренний positions dict стрима."""
    positions = getattr(stream, 'positions', {})
    if not positions:
        print(f"  [{name}] positions dict ПУСТ (ни одного символа)")
    else:
        print(f"  [{name}] positions dict ({len(positions)} символов):")
        for sym, sides in positions.items():
            l = sides.get("LONG", {})
            s = sides.get("SHORT", {})
            if l.get("size", 0) > 0 or s.get("size", 0) > 0:
                print(f"    {sym}: LONG size={l.get('size',0)} price={l.get('price',0)} | SHORT size={s.get('size',0)} price={s.get('price',0)}")


async def test_adapter_bridge(name: str, order_adapter, test_symbols: list):
    """Проверяет что get_executed_position() возвращает данные через position_stream."""
    print(f"\n  [{name}] Проверка get_executed_position() через адаптер:")
    for sym in test_symbols:
        for side in ("LONG", "SHORT"):
            pos = order_adapter.get_executed_position(sym, side)
            if pos.get("size", 0) > 0:
                print(f"    ✅ {sym}/{side}: size={pos['size']} price={pos.get('price',0)}")
            else:
                print(f"    ⬜ {sym}/{side}: size=0 (не видит)")


async def test_rest_vs_ws(name: str, order_adapter, test_symbols: list):
    """Сравнивает REST и WS для одних и тех же символов."""
    print(f"\n  [{name}] REST vs WS сравнение:")
    for sym in test_symbols:
        for side in ("LONG", "SHORT"):
            ws_pos = order_adapter.get_executed_position(sym, side)
            try:
                rest_pos = await order_adapter.get_position_rest(sym, side)
            except Exception as e:
                rest_pos = {"size": 0.0, "price": 0.0, "status": f"error: {e}"}
            
            ws_size = ws_pos.get("size", 0.0)
            rest_size = rest_pos.get("size", 0.0)
            
            if ws_size > 0 or rest_size > 0:
                match = "✅" if abs(ws_size - rest_size) < 0.001 else "❌ РАССИНХРОН!"
                print(f"    {match} {sym}/{side}: WS={ws_size} REST={rest_size}")


async def main():
    print("=" * 70)
    print("ДИАГНОСТИКА ПРИВАТНЫХ WS СТРИМОВ ПОЗИЦИЙ")
    print("=" * 70)
    
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cfg.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    
    session = aiohttp.ClientSession()
    
    # ---- Создаём стримы (как в executor_process.py) ----
    binance_stream = BinancePositionStream(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", "")
    )
    kucoin_stream = KucoinPositionStream(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", "")
    )
    bitget_stream = BitgetPositionStream(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", "")
    )
    
    # ---- Создаём адаптеры ордеров (как в executor_process.py) ----
    binance_order = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session,
        position_stream=binance_stream
    )
    kucoin_order = KucoinOrder(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
        session=session,
        position_stream=kucoin_stream,
        margin_settings=cfg["margin_settings"]["KUCOIN"]
    )
    bitget_order = BitgetOrder(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
        margin_settings=cfg["margin_settings"]["BITGET"],
        session=session,
        position_stream=bitget_stream
    )
    
    
    # ---- Запускаем WS стримы ----
    print("\n[1] Запуск WS стримов...")
    stream_tasks = []
    if os.environ.get("BINANCE_API_KEY"):
        stream_tasks.append(asyncio.create_task(binance_stream.start()))
    if os.environ.get("KUCOIN_API_KEY"):
        stream_tasks.append(asyncio.create_task(kucoin_stream.start()))
    if os.environ.get("BITGET_API_KEY"):
        stream_tasks.append(asyncio.create_task(bitget_stream.start()))
    
    # ---- Ждём ready ----
    print("\n[2] Ожидание ready на стримах...")
    b_ok = await wait_ready("BINANCE", binance_stream) if os.environ.get("BINANCE_API_KEY") else False
    k_ok = await wait_ready("KUCOIN", kucoin_stream) if os.environ.get("KUCOIN_API_KEY") else False
    bg_ok = await wait_ready("BITGET", bitget_stream) if os.environ.get("BITGET_API_KEY") else False
    
    # ---- Даём стримам ещё 2с получить snapshot ----
    print("\n[3] Пауза 2с для получения snapshot с бирж...")
    await asyncio.sleep(2.0)
    
    # ---- Дамп позиций из WS ----
    print("\n[4] Содержимое positions dict из WS стримов:")
    if b_ok:
        await dump_stream_positions("BINANCE", binance_stream)
    if k_ok:
        await dump_stream_positions("KUCOIN", kucoin_stream)
    if bg_ok:
        await dump_stream_positions("BITGET", bitget_stream)
    
    # ---- Тест символов через адаптеры ----
    binance_test_syms = ["BTCUSDT", "ETHUSDT", "MAGMAUSDT", "MUBARAKUSDT"]
    kucoin_test_syms = ["XBTUSDTM", "ETHUSDTM", "MAGMAUSDTM", "MUBARAKUSDTM"]
    bitget_test_syms = ["BTCUSDT", "ETHUSDT", "MAGMAUSDT", "MUBARAKUSDT"]
    
    print("\n[5] Проверка get_executed_position() через адаптеры ордеров:")
    if b_ok:
        await test_adapter_bridge("BINANCE", binance_order, binance_test_syms)
    if k_ok:
        await test_adapter_bridge("KUCOIN", kucoin_order, kucoin_test_syms)
    if bg_ok:
        await test_adapter_bridge("BITGET", bitget_order, bitget_test_syms)
    
    # ---- REST vs WS сравнение ----
    print("\n[6] REST vs WS сравнение (ищем рассинхроны):")
    if b_ok:
        await test_rest_vs_ws("BINANCE", binance_order, binance_test_syms)
    if k_ok:
        await test_rest_vs_ws("KUCOIN", kucoin_order, kucoin_test_syms)
    if bg_ok:
        await test_rest_vs_ws("BITGET", bitget_order, bitget_test_syms)
    
    # ---- Мониторинг: ждём ещё 10с ----
    print("\n[7] Мониторинг WS (10с)...")
    for _ in range(5):
        await asyncio.sleep(2.0)
        for name, stream, ok in [("BIN", binance_stream, b_ok), ("KUC", kucoin_stream, k_ok), ("BIT", bitget_stream, bg_ok)]:
            if not ok:
                continue
            positions = getattr(stream, 'positions', {})
            active = {sym: sides for sym, sides in positions.items() 
                      if sides.get("LONG", {}).get("size", 0) > 0 or sides.get("SHORT", {}).get("size", 0) > 0}
            if active:
                print(f"    [{name}] Активные позиции: {json.dumps(active, default=str)}")
    
    print("\n" + "=" * 70)
    print("ДИАГНОСТИКА ЗАВЕРШЕНА")
    print("=" * 70)
    
    # Cleanup
    binance_stream._external_stop = True
    kucoin_stream._external_stop = True
    bitget_stream._external_stop = True
    for t in stream_tasks:
        t.cancel()
    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
