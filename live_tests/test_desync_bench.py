# ============================================================
# FILE: live_tests/test_desync_bench.py
# ROLE: Измерение реального рассинхрона стаканов (max_desync_ms)
#       в точности так, как его вычисляет боевой бот в main.py
# ============================================================
"""
БЕНЧМАРК РАССИНХРОНА СТАКАНОВ (100% АНАЛОГ main.py)
==================================================
В боевом боте (main.py):
  1. При получении каждого кадра стакана по WS:
       self.ts[exchange_name][base_coin] = time.monotonic()
  2. В вычислительном цикле перед выстрелом:
       diff_ms = abs(self.ts[long_ex][sym] - self.ts[short_ex][sym]) * 1000.0
       if diff_ms > self.entry_desync_limit:  # (max_desync_ms)
           continue  # отсекаем сигнал как рассинхронизированный

Этот скрипт запускает точно такие же стримы L2 стаканов,
регистрирует self.ts через time.monotonic(), крутит цикл с шагом
MAIN_LOOP_DELAY и замеряет эмпирическое распределение diff_ms
на реальных боевых парах.

Запуск на сервере:
    python live_tests/test_desync_bench.py
    (или python test_desync_bench.py)
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
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from API.discovery import DiscoveryManager
from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.OKX.stakan import OkxStakanStream

DURATION_SECONDS = 45  # Длительность замера (сек)


async def main():
    print("=" * 85)
    print("🔬 БЕНЧМАРК РАССИНХРОНА СТАКАНОВ (Точная копия логики main.py)")
    print(f"   Замер diff_ms = abs(self.ts[ex1] - self.ts[ex2]) * 1000.0 на боевых сокетах")
    print("=" * 85)

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg_path = os.path.join(root_dir, "cfg.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    active_routes_cfg = cfg.get("active_routes", {})
    entry_desync_limit = cfg.get("trading_rules", {}).get("entry", {}).get("max_desync_ms", 125)
    main_loop_delay = cfg.get("MAIN_LOOP_DELAY", 0.0)

    print(f"Конфигурация:")
    print(f"  • Текущий max_desync_ms в cfg.json: {entry_desync_limit} мс")
    print(f"  • MAIN_LOOP_DELAY:                  {main_loop_delay} с")

    # 1. Построение топологии инструментов (как в main.py)
    print("\n[1/4] Построение топологии инструментов через Discovery...")
    discovery = DiscoveryManager(quote=cfg.get("QUOTE", "USDT"))
    await discovery.build_topology()

    active_routes = [r for r, is_active in active_routes_cfg.items() if is_active]
    print(f"  Активные связки: {active_routes}")
    print(f"  Общих монет в пуле: {len(discovery.active_pairs_map)}")

    if not discovery.active_pairs_map:
        print("❌ Нет общих монет для активных связок. Завершение.")
        await discovery.aclose()
        return

    # 2. Инициализация публичных сокетов стаканов (как в main.py)
    print("\n[2/4] Запуск публичных WebSocket-стримов стаканов...")
    stream_classes = {
        "BINANCE": BinanceStakanStream,
        "KUCOIN": KucoinStakanStream,
        "OKX": OkxStakanStream,
        "BITGET": BitgetStakanStream
    }

    streams = {}
    tasks = []
    
    # self.ts[exchange_name][base_coin] = time.monotonic() (1-в-1 как в main.py)
    ts: Dict[str, Dict[str, float]] = defaultdict(dict)
    books: Dict[str, Dict[str, dict]] = defaultdict(dict)

    def make_depth_handler(exchange_name: str):
        async def on_depth(d):
            base_coin = None
            for coin, mapping in discovery.coin_to_native.items():
                if mapping.get(exchange_name) == d.symbol or coin == d.symbol:
                    base_coin = coin
                    break
            if base_coin and getattr(d, 'bids', None) and getattr(d, 'asks', None):
                # Фиксация времени прибытия пакета ровно как в main.py строка 114
                ts[exchange_name][base_coin] = time.monotonic()
                books[exchange_name][base_coin] = {"bids": d.bids, "asks": d.asks}
        return on_depth

    for ex, syms in discovery.ws_routes.items():
        if syms and ex in stream_classes:
            stream = stream_classes[ex](syms)
            streams[ex] = stream
            handler = make_depth_handler(ex)
            tasks.append(asyncio.create_task(stream.run(handler)))
            print(f"  ✅ {ex}: запущен стрим ({len(syms)} тикеров)")

    # 3. Прогрев (ожидание первых пакетов)
    print("\n[3/4] Прогрев сокетов (5 сек)...")
    await asyncio.sleep(5.0)

    # 4. Вычислительный цикл замера (1-в-1 логика main.py)
    print(f"\n[4/4] Сбор реальной статистики рассинхрона ({DURATION_SECONDS} сек)...")
    print("Нажмите Ctrl+C для досрочного завершения и вывода отчета.\n")

    # Сбор замеров: samples[route] = [diff_ms, ...]
    samples: Dict[str, List[float]] = defaultdict(list)
    # Помонетный сбор для выявления проблемных монет
    coin_samples: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))

    start_time = time.monotonic()
    last_print_time = start_time
    total_iterations = 0

    try:
        while time.monotonic() - start_time < DURATION_SECONDS:
            now_mono = time.monotonic()
            total_iterations += 1

            for sym in discovery.active_pairs_map:
                for route in active_routes:
                    parts = route.split("_")
                    if len(parts) != 2:
                        continue
                    long_ex, short_ex = parts[0], parts[1]

                    # Проверка наличия меток времени (как в main.py строка 377)
                    if sym not in ts[long_ex] or sym not in ts[short_ex]:
                        continue

                    t_long = ts[long_ex][sym]
                    t_short = ts[short_ex][sym]

                    # Проверка на свежесть данных (не старше 5 сек, как в main.py строка 347)
                    if (now_mono - t_long) <= 5.0 and (now_mono - t_short) <= 5.0:
                        # РОVНО ТОТ САМЫЙ РАСЧЕТ ИЗ main.py (строка 380):
                        diff_ms = abs(t_long - t_short) * 1000.0
                        samples[route].append(diff_ms)
                        coin_samples[route][sym].append(diff_ms)

            # Промежуточный прогресс каждые 5 сек
            if now_mono - last_print_time >= 5.0:
                elapsed_s = int(now_mono - start_time)
                parts_str = []
                for r in active_routes:
                    cnt = len(samples[r])
                    if cnt > 0:
                        recent = samples[r][-500:]
                        parts_str.append(f"{r}: avg={np.mean(recent):.1f}ms, p95={np.percentile(recent, 95):.1f}ms ({cnt:,} проб)")
                print(f"  [{elapsed_s:2d}s/{DURATION_SECONDS}s] " + " | ".join(parts_str))
                last_print_time = now_mono

            if main_loop_delay > 0:
                await asyncio.sleep(main_loop_delay)
            else:
                await asyncio.sleep(0.005)  # 5 мс минимальный квант для бенчмарка

    except KeyboardInterrupt:
        print("\n⛔ Замер остановлен пользователем (Ctrl+C). Формирование отчета...")
    finally:
        for t in tasks:
            t.cancel()
        for s in streams.values():
            try:
                await s.aclose()
            except Exception:
                pass
        await discovery.aclose()

    # 5. Итоговый отчет
    print("\n" + "=" * 92)
    print("📊 ИТОГОВЫЙ ОТЧЕТ РАССИНХРОНА СТАКАНОВ (IDENTICAL TO main.py diff_ms)")
    print("=" * 92)

    thresholds = [50, 75, 100, 125, 150, 200]

    for route in active_routes:
        data = samples.get(route, [])
        if not data:
            print(f"\n❌ [{route}]: нет замеров (проверьте подключение).")
            continue

        arr = np.array(data)
        count = len(arr)
        min_v = np.min(arr)
        mean_v = np.mean(arr)
        p50 = np.percentile(arr, 50)
        p75 = np.percentile(arr, 75)
        p90 = np.percentile(arr, 90)
        p95 = np.percentile(arr, 95)
        p99 = np.percentile(arr, 99)
        max_v = np.max(arr)

        print(f"\nСВЯЗКА: 🚀 {route} (Выборка: {count:,} проверок)")
        print("-" * 92)
        print(f"  Метрики задержки (diff_ms = abs(ts_ex1 - ts_ex2) * 1000.0):")
        print(f"    MIN:    {min_v:6.2f} мс")
        print(f"    AVG:    {mean_v:6.2f} мс  (Средний рассинхрон)")
        print(f"    P50:    {p50:6.2f} мс  (Медиана: 50% всех проверок)")
        print(f"    P75:    {p75:6.2f} мс")
        print(f"    P90:    {p90:6.2f} мс")
        print(f"    P95:    {p95:6.2f} мс  (95% всех проверок быстрее этого порога!)")
        print(f"    P99:    {p99:6.2f} мс")
        print(f"    MAX:    {max_v:6.2f} мс")
        print()
        if isinstance(entry_desync_limit, dict):
            pair_limit = entry_desync_limit.get(route, entry_desync_limit.get("default", 150))
        else:
            pair_limit = entry_desync_limit

        print(f"  Проходимость торговых сигналов при разных значениях max_desync_ms (порог для {route}: {pair_limit} мс):")
        for th in thresholds:
            pass_pct = (np.sum(arr <= th) / count) * 100.0
            marker = f"👈 [ТЕКУЩЕЕ В CFG ДЛЯ {route}]" if th == pair_limit else ""
            print(f"    <= {th:3d} мс: {pass_pct:6.2f}% сигналов будет пропущено в торговлю {marker}")

        rec_val = max(75, int(np.ceil(p95 / 5.0) * 5))
        print(f"\n  💡 ВЫВОД И РЕКОМЕНДАЦИЯ ДЛЯ {route}:")
        pass_at_current = (np.sum(arr <= pair_limit) / count) * 100.0
        if p95 <= pair_limit:
            print(f"     ✅ Порог {pair_limit} мс идеален: пропускает {pass_at_current:.1f}% сигналов и срезает рассинхроны.")
        else:
            print(f"     ⚠️ При пороге {pair_limit} мс пропускается {pass_at_current:.1f}% сигналов (P95={p95:.1f} мс).")
            print(f"     👉 Рекомендуемое оптимальное значение: max_desync_ms = {rec_val} мс.")

    print("\n" + "=" * 92)


if __name__ == "__main__":
    asyncio.run(main())
