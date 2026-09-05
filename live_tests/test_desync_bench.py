# ============================================================
# FILE: live_tests/test_desync_bench.py
# ROLE: Измерение реальных дельт рассинхрона стаканов (max_desync_ms)
#       между биржами на живых публичных WS-стримах
#
# Запуск:  python live_tests/test_desync_bench.py
# Выход:   logs/desync_bench_<timestamp>.json + сводная таблица
# ============================================================

import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

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

from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.OKX.stakan import OkxStakanStream
from API.BITGET.stakan import BitgetStakanStream


# ── Параметры теста ───────────────────────────────────────────
WARMUP_SEC = 10     # Прогрев сокетов перед началом замера
MEASURE_SEC = 60    # Длительность замера (1 минута)
TOTAL_SEC = WARMUP_SEC + MEASURE_SEC

TEST_COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP",
    "DOGE", "ADA", "AVAX", "DOT", "LINK",
]

EXCHANGES = ["BINANCE", "KUCOIN", "BITGET"]


def to_native(coin: str, exchange: str, quote: str = "USDT") -> str:
    if exchange == "KUCOIN":
        alias = {"BTC": "XBT"}.get(coin, coin)
        return f"{alias}{quote}M"
    elif exchange == "OKX":
        return f"{coin}-{quote}-SWAP"
    else:
        return f"{coin}{quote}"


NATIVE_TO_COIN: Dict[str, Dict[str, str]] = {}
for _coin in TEST_COINS:
    for _ex in EXCHANGES:
        _native = to_native(_coin, _ex)
        NATIVE_TO_COIN.setdefault(_ex, {})[_native.upper()] = _coin

# Локальное время прибытия пакета (именно оно проверяется в main.py через time.monotonic())
last_local_ts: Dict[str, Dict[str, Optional[float]]] = {
    coin: {ex: None for ex in EXCHANGES}
    for coin in TEST_COINS
}

# Серверное время биржи (event_time_ms)
last_event_ms: Dict[str, Dict[str, Optional[int]]] = {
    coin: {ex: None for ex in EXCHANGES}
    for coin in TEST_COINS
}

local_deltas: Dict[str, List[float]] = defaultdict(list)
server_deltas: Dict[str, List[float]] = defaultdict(list)

start_time: float = 0.0
warmup_done: bool = False


def record_event(exchange: str, coin: str, event_ms: int, local_mono: float) -> None:
    global warmup_done
    now = time.time()

    if not warmup_done:
        if now - start_time < WARMUP_SEC:
            last_local_ts[coin][exchange] = local_mono
            last_event_ms[coin][exchange] = event_ms
            return
        else:
            warmup_done = True
            print(f"[{elapsed():>4.0f}s] 🔥 Прогрев завершён — начинаем измерение ({MEASURE_SEC}с)...")

    prev_other_local = last_local_ts[coin]
    last_local_ts[coin][exchange] = local_mono
    last_event_ms[coin][exchange] = event_ms

    for other_ex in EXCHANGES:
        if other_ex == exchange:
            continue
        other_local = prev_other_local.get(other_ex)
        if other_local is not None:
            # Замер в миллисекундах (как в main.py: abs(ts1 - ts2) * 1000.0)
            diff_local_ms = abs(local_mono - other_local) * 1000.0
            pair = f"{min(exchange, other_ex)}_{max(exchange, other_ex)}"
            local_deltas[pair].append(diff_local_ms)

            other_srv = last_event_ms[coin].get(other_ex)
            if other_srv is not None and event_ms > 0 and other_srv > 0:
                diff_srv_ms = abs(event_ms - other_srv)
                server_deltas[pair].append(float(diff_srv_ms))


def elapsed() -> float:
    return time.time() - start_time


def make_handler(exchange: str):
    async def on_depth(d) -> None:
        coin = NATIVE_TO_COIN.get(exchange, {}).get(d.symbol.upper())
        if coin:
            local_mono = time.monotonic()
            ev_ms = getattr(d, "event_time_ms", 0)
            record_event(exchange, coin, ev_ms, local_mono)
    return on_depth


def percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100.0
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def build_report() -> dict:
    report = {}
    for pair, vals in sorted(local_deltas.items()):
        if not vals:
            continue
        arr = sorted(vals)
        n = len(arr)
        p50 = percentile(arr, 50)
        p75 = percentile(arr, 75)
        p90 = percentile(arr, 90)
        p95 = percentile(arr, 95)
        p99 = percentile(arr, 99)
        report[pair] = {
            "samples": n,
            "min_ms": round(min(arr), 2),
            "avg_ms": round(sum(arr) / n, 2),
            "p50_ms": round(p50, 2),
            "p75_ms": round(p75, 2),
            "p90_ms": round(p90, 2),
            "p95_ms": round(p95, 2),
            "p99_ms": round(p99, 2),
            "max_ms": round(max(arr), 2),
            "pass_75ms": round(sum(1 for x in arr if x <= 75) / n * 100, 1),
            "pass_100ms": round(sum(1 for x in arr if x <= 100) / n * 100, 1),
            "pass_125ms": round(sum(1 for x in arr if x <= 125) / n * 100, 1),
            "pass_150ms": round(sum(1 for x in arr if x <= 150) / n * 100, 1),
            "pass_200ms": round(sum(1 for x in arr if x <= 200) / n * 100, 1),
        }
    return report


def print_table(report: dict) -> None:
    print("\n" + "=" * 92)
    print("📊 РЕЗУЛЬТАТЫ ЗАМЕРА РАССИНХРОНА СТАКАНОВ (LOCAL PACKET ARRIVAL DESYNC)")
    print("=" * 92)
    print(f"{'СВЯЗКА':<20} {'ПРОБ':>7} {'MIN':>6} {'AVG':>6} {'P50':>6} {'P90':>6} {'P95':>6} {'P99':>6} {'MAX':>7}")
    print("-" * 92)
    for pair, s in sorted(report.items(), key=lambda x: x[1]["avg_ms"]):
        print(f"{pair:<20} {s['samples']:>7,d} "
              f"{s['min_ms']:>6.1f} {s['avg_ms']:>6.1f} {s['p50_ms']:>6.1f} "
              f"{s['p90_ms']:>6.1f} {s['p95_ms']:>6.1f} {s['p99_ms']:>6.1f} {s['max_ms']:>7.1f}  мс")
    print("=" * 92)

    print("\n📈 ПРОЦЕНТ СИГНАЛОВ, ПРОХОДЯЩИХ ФИЛЬТР ПРИ РАЗНЫХ ЗНАЧЕНИЯХ max_desync_ms:")
    print("-" * 92)
    print(f"{'СВЯЗКА':<20} {'<=75ms':>10} {'<=100ms':>10} {'<=125ms (CFG)':>14} {'<=150ms':>10} {'<=200ms':>10}")
    print("-" * 92)
    for pair, s in sorted(report.items()):
        print(f"{pair:<20} {s['pass_75ms']:>9.1f}% {s['pass_100ms']:>9.1f}% "
              f"{s['pass_125ms']:>13.1f}% {s['pass_150ms']:>9.1f}% {s['pass_200ms']:>9.1f}%")
    print("=" * 92)

    print("\n💡 РЕКОМЕНДАЦИИ ДЛЯ cfg.json (на основе 95-го перцентиля P95):")
    for pair, s in sorted(report.items()):
        rec = max(75, int(s['p95_ms'] * 1.15))
        status = "✅ 125ms отлично подходит" if s['p95_ms'] <= 125 else f"⚠️ P95={s['p95_ms']:.0f}ms, поднимите до {rec}ms"
        print(f"  • {pair}: {status} (пропускает {s['pass_125ms']:.1f}% тиков при 125ms)")
    print()


async def main():
    global start_time
    os.makedirs("logs", exist_ok=True)

    print("=" * 80)
    print("🔬 ЗАМЕР РАССИНХРОНА СТАКАНОВ (test_desync_bench.py)")
    print(f"   Биржи: {EXCHANGES} | Монеты: {TEST_COINS}")
    print(f"   Прогрев: {WARMUP_SEC}с | Замер: {MEASURE_SEC}с | Итого: {TOTAL_SEC}с")
    print("=" * 80)

    symbols_per_ex = {
        ex: [to_native(coin, ex) for coin in TEST_COINS]
        for ex in EXCHANGES
    }

    stream_map = {
        "BINANCE": BinanceStakanStream(symbols_per_ex["BINANCE"]),
        "KUCOIN": KucoinStakanStream(symbols_per_ex["KUCOIN"]),
        "BITGET": BitgetStakanStream(symbols_per_ex["BITGET"])
    }

    start_time = time.time()
    tasks = []
    for ex, stream in stream_map.items():
        handler = make_handler(ex)
        tasks.append(asyncio.create_task(stream.run(handler)))
        print(f"  ✅ Запущен WebSocket стрим {ex} ({len(symbols_per_ex[ex])} пар)")

    print(f"\n⏳ Ожидание прогрева {WARMUP_SEC}с...\n")

    async def progress_ticker():
        while True:
            await asyncio.sleep(10)
            t = elapsed()
            total_samples = sum(len(v) for v in local_deltas.values())
            status = "ПРОГРЕВ" if not warmup_done else "ИЗМЕРЕНИЕ"
            print(f"  [{t:>4.0f}s | {status}] Накоплено замеров: {total_samples:,}")

    prog_task = asyncio.create_task(progress_ticker())
    try:
        await asyncio.sleep(TOTAL_SEC)
    except KeyboardInterrupt:
        print("\n⛔ Замер остановлен пользователем по Ctrl+C.")
    finally:
        prog_task.cancel()
        for stream in stream_map.values():
            try:
                await stream.aclose()
            except Exception:
                pass
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    report = build_report()
    total_samples = sum(s["samples"] for s in report.values())
    print(f"\nТест завершён. Всего замеров: {total_samples:,}")
    print_table(report)

    ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("logs", f"desync_bench_{ts_str}.json")
    meta = {
        "timestamp_utc": ts_str,
        "test_coins": TEST_COINS,
        "warmup_sec": WARMUP_SEC,
        "measure_sec": MEASURE_SEC,
        "total_samples": total_samples,
        "pairs": report,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"📁 Подробный отчет сохранён: {out_path}\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗамер прерван.")
