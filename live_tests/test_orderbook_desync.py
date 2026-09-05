# ============================================================
# FILE: live_tests/test_orderbook_desync.py
# ROLE: Измерение реальной задержки и рассинхрона получения стаканов (max_desync_ms)
#       между биржами (Binance, Kucoin, Bitget) в боевом WebSocket-рантайме
# ============================================================
"""
БЕНЧМАРК РАССИНХРОНА СТАКАНОВ (ORDERBOOK DESYNC BENCHMARK)
=========================================================
Скрипт подключается к публичным WebSocket-стримам стаканов (L2 Depth)
активных бирж и замеряет точную статистику рассинхрона (diff_ms):
  diff_ms = abs(arrival_ts[ex1] - arrival_ts[ex2]) * 1000.0

Вычисляет:
  - Средний рассинхрон (Mean)
  - Медиана (P50)
  - 90-й перцентиль (P90)
  - 95-й перцентиль (P95)
  - 99-й перцентиль (P99)
  - Максимум (Max)
  - Процент тиков, проходящих фильтры 75ms, 100ms, 125ms, 150ms, 200ms

Запуск на боевом сервере в Токио:
  cd /opt/ProdSpread && python live_tests/test_orderbook_desync.py
"""

import asyncio
import os
import sys
import time
import json
import numpy as np
from collections import defaultdict
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

from API.discovery import DiscoveryManager
from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.OKX.stakan import OkxStakanStream

DURATION_SECONDS = 30  # Длительность замера (сек)


async def main():
    print("=" * 80)
    print("🔬 ЗАМЕР ЛАТЕНЦИИ РАССИНХРОНА СТАКАНОВ (DESYNC BENCHMARK)")
    print(f"   Длительность теста: {DURATION_SECONDS} сек | Замер diff_ms между биржами")
    print("=" * 80)

    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cfg.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    active_routes_cfg = cfg.get("active_routes", {})
    configured_entry_desync = cfg.get("trading_rules", {}).get("entry", {}).get("max_desync_ms", 125)
    print(f"Текущий max_desync_ms в cfg.json: {configured_entry_desync} мс")

    # 1. Топология инструментов
    print("\n[1/4] Построение топологии инструментов (Discovery)...")
    discovery = DiscoveryManager(quote=cfg.get("QUOTE", "USDT"))
    await discovery.build_topology()

    active_routes = [r for r, is_active in active_routes_cfg.items() if is_active]
    print(f"Активные связки: {active_routes}")
    print(f"Общих торгуемых монет: {len(discovery.active_pairs_map)}")

    if not discovery.active_pairs_map:
        print("❌ Нет доступных общих монет для активных связок. Проверьте volume_filters.")
        await discovery.aclose()
        return

    # 2. Инициализация публичных стримов стаканов
    print("\n[2/4] Запуск публичных WebSocket-стримов стаканов...")
    stream_classes = {
        "BINANCE": BinanceStakanStream,
        "KUCOIN": KucoinStakanStream,
        "OKX": OkxStakanStream,
        "BITGET": BitgetStakanStream
    }

    streams = {}
    tasks = []
    
    # Хранилище времен прибытия: arrival_ts[ex][sym] = monotonic_time
    arrival_ts: Dict[str, Dict[str, float]] = defaultdict(dict)
    event_ts: Dict[str, Dict[str, int]] = defaultdict(dict)

    def make_handler(exchange_name: str):
        async def on_depth(d):
            sym = getattr(d, "symbol", "")
            base_coin = sym
            # Обратный маппинг в generic coin
            for coin, mapping in discovery.coin_to_native.items():
                if mapping.get(exchange_name) == sym or coin == sym:
                    base_coin = coin
                    break
            now_mono = time.monotonic()
            arrival_ts[exchange_name][base_coin] = now_mono
            event_ts[exchange_name][base_coin] = getattr(d, "event_time_ms", int(time.time() * 1000))
        return on_depth

    for ex, syms in discovery.ws_routes.items():
        if syms and ex in stream_classes:
            stream = stream_classes[ex](syms)
            streams[ex] = stream
            handler = make_handler(ex)
            tasks.append(asyncio.create_task(stream.run(handler)))
            print(f"  ✅ {ex}: запущен стрим ({len(syms)} тикеров)")

    # 3. Прогрев сокетов (первые данные)
    print("\n[3/4] Прогрев сокетов (5 сек ожидания первых пакетов)...")
    await asyncio.sleep(5.0)

    # 4. Основной сбор статистики
    print(f"\n[4/4] Идет замер рассинхрона ({DURATION_SECONDS} сек)...")
    print("Нажмите Ctrl+C для досрочного завершения и вывода отчета.\n")

    # Сбор замеров по связкам: samples[route] = list of diff_ms
    samples: Dict[str, List[float]] = defaultdict(list)
    server_samples: Dict[str, List[float]] = defaultdict(list)

    start_time = time.monotonic()
    last_print_time = start_time

    try:
        while time.monotonic() - start_time < DURATION_SECONDS:
            now_mono = time.monotonic()
            
            for sym in discovery.active_pairs_map:
                for route in active_routes:
                    parts = route.split("_")
                    if len(parts) != 2:
                        continue
                    ex1, ex2 = parts[0], parts[1]

                    ts1 = arrival_ts[ex1].get(sym, 0.0)
                    ts2 = arrival_ts[ex2].get(sym, 0.0)

                    # Учитываем только если оба стакана живые (обновлены не позже 5с назад)
                    if ts1 > 0 and ts2 > 0 and (now_mono - ts1) <= 5.0 and (now_mono - ts2) <= 5.0:
                        local_diff_ms = abs(ts1 - ts2) * 1000.0
                        samples[route].append(local_diff_ms)

                        ev1 = event_ts[ex1].get(sym, 0)
                        ev2 = event_ts[ex2].get(sym, 0)
                        if ev1 > 0 and ev2 > 0:
                            srv_diff_ms = abs(ev1 - ev2)
                            server_samples[route].append(srv_diff_ms)

            # Периодический мини-статус каждые 5 сек
            if now_mono - last_print_time >= 5.0:
                elapsed = int(now_mono - start_time)
                line_parts = []
                for r in active_routes:
                    cnt = len(samples[r])
                    avg = np.mean(samples[r][-200:]) if cnt > 0 else 0.0
                    line_parts.append(f"{r}: {avg:.1f}ms ({cnt} проб)")
                print(f"  [{elapsed:2d}s/{DURATION_SECONDS}s] " + " | ".join(line_parts))
                last_print_time = now_mono

            await asyncio.sleep(0.05)  # 50 ms loop (аналог MAIN_LOOP_DELAY)

    except KeyboardInterrupt:
        print("\n⛔ Замер остановлен пользователем (Ctrl+C). Формирование отчета...")
    finally:
        for t in tasks:
            t.cancel()
        for s in streams.values():
            await s.aclose()
        await discovery.aclose()

    # 5. Итоговый отчет
    print("\n" + "=" * 85)
    print("📊 ИТОГОВЫЙ ОТЧЕТ РАССИНХРОНА СТАКАНОВ (DESYNC REPORT)")
    print("=" * 85)

    thresholds = [50, 75, 100, 125, 150, 200]

    for route in active_routes:
        data = samples.get(route, [])
        if not data:
            print(f"\n❌ [{route}]: нет данных для анализа (проверьте подключение).")
            continue

        arr = np.array(data)
        count = len(arr)
        p50 = np.percentile(arr, 50)
        p75 = np.percentile(arr, 75)
        p90 = np.percentile(arr, 90)
        p95 = np.percentile(arr, 95)
        p99 = np.percentile(arr, 99)
        mean_val = np.mean(arr)
        min_val = np.min(arr)
        max_val = np.max(arr)

        print(f"\nСВЯЗКА: 🚀 {route} (Выборка: {count:,} замеров)")
        print("-" * 85)
        print(f"  Метрика латентности (Local Packet Arrival Desync):")
        print(f"    Min:    {min_val:6.2f} мс")
        print(f"    Mean:   {mean_val:6.2f} мс  (Средний рассинхрон)")
        print(f"    Median: {p50:6.2f} мс  (P50)")
        print(f"    P90:    {p90:6.2f} мс  (90% тиков быстрее этого)")
        print(f"    P95:    {p95:6.2f} мс  (95% тиков быстрее этого)")
        print(f"    P99:    {p99:6.2f} мс  (99% тиков быстрее этого)")
        print(f"    Max:    {max_val:6.2f} мс")
        print()
        print("  Процент сигналов, проходящих фильтр при разных порогах max_desync_ms:")
        for th in thresholds:
            pass_pct = (np.sum(arr <= th) / count) * 100.0
            marker = "👈 [ТЕКУЩИЙ В CFG]" if th == configured_entry_desync else ""
            print(f"    <= {th:3d} мс: {pass_pct:6.2f}% сигналов проходят {marker}")

        rec_threshold = int(p95)
        print(f"\n  💡 Рекомендация для {route}:")
        if p95 <= 125:
            print(f"     ✅ Порог {configured_entry_desync} мс комфортен (пропускает >95% валидных тиков).")
        else:
            print(f"     ⚠️ P95 составляет {p95:.0f} мс. Рекомендуемое безопасное значение max_desync_ms: {rec_threshold} мс.")

    print("\n" + "=" * 85)


if __name__ == "__main__":
    asyncio.run(main())
