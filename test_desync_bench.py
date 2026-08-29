# ============================================================
# FILE: test_desync_bench.py
# ROLE: Измерение реальных дельт event_time_ms между всеми 21
#       парами бирж на живых WS-стримах.
#
# Запуск:  python test_desync_bench.py
# Выход:   logs/desync_bench_<timestamp>.json  +  таблица в консоль
# ============================================================

import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from API.BINANCE.stakan  import BinanceStakanStream
from API.KUCOIN.stakan   import KucoinStakanStream
from API.OKX.stakan      import OkxStakanStream
from API.BITGET.stakan   import BitgetStakanStream
from API.PHEMEX.stakan   import PhemexStakanStream
from API.BYBIT.stakan    import BybitStakanStream
from API.GATE.stakan     import GateStakanStream

# ── параметры теста ───────────────────────────────────────────
WARMUP_SEC  = 30
TOTAL_SEC   = 3 * 60
MEASURE_SEC = TOTAL_SEC - WARMUP_SEC

TEST_COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP",
    "DOGE", "ADA", "AVAX", "DOT", "LINK",
]

EXCHANGES = ["BINANCE", "KUCOIN", "OKX", "BITGET", "PHEMEX", "BYBIT", "GATE"]

def to_native(coin: str, exchange: str, quote: str = "USDT") -> str:
    if exchange == "KUCOIN":
        alias = {"BTC": "XBT"}.get(coin, coin)
        return f"{alias}{quote}M"
    elif exchange == "OKX":
        return f"{coin}-{quote}-SWAP"
    elif exchange == "GATE":
        return f"{coin}_{quote}"
    else:
        return f"{coin}{quote}"

NATIVE_TO_COIN: Dict[str, Dict[str, str]] = {}
for _coin in TEST_COINS:
    for _ex in EXCHANGES:
        _native = to_native(_coin, _ex)
        NATIVE_TO_COIN.setdefault(_ex, {})[_native.upper()] = _coin

last_event_ms: Dict[str, Dict[str, Optional[int]]] = {
    coin: {ex: None for ex in EXCHANGES}
    for coin in TEST_COINS
}

deltas: Dict[str, List[float]] = defaultdict(list)

start_time: float = 0.0
warmup_done: bool = False


def record_event(exchange: str, coin: str, event_ms: int) -> None:
    global warmup_done
    now = time.time()

    if not warmup_done:
        if now - start_time < WARMUP_SEC:
            last_event_ms[coin][exchange] = event_ms
            return
        else:
            warmup_done = True
            print(f"[{elapsed():>5.0f}s] Прогрев завершён — начинаем измерение ({MEASURE_SEC}с)")

    last_event_ms[coin][exchange] = event_ms

    for other_ex in EXCHANGES:
        if other_ex == exchange:
            continue
        other_ms = last_event_ms[coin].get(other_ex)
        if other_ms is None:
            continue
        diff = abs(event_ms - other_ms)
        pair = f"{min(exchange, other_ex)}_{max(exchange, other_ex)}"
        deltas[pair].append(float(diff))


def elapsed() -> float:
    return time.time() - start_time


def make_handler(exchange: str):
    async def on_depth(d) -> None:
        coin = NATIVE_TO_COIN.get(exchange, {}).get(d.symbol.upper())
        if coin and d.event_time_ms:
            record_event(exchange, coin, d.event_time_ms)
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
    for pair, vals in sorted(deltas.items()):
        if not vals:
            continue
        report[pair] = {
            "samples": len(vals),
            "min_ms":  round(min(vals), 2),
            "avg_ms":  round(sum(vals) / len(vals), 2),
            "p95_ms":  round(percentile(vals, 95), 2),
            "p99_ms":  round(percentile(vals, 99), 2),
            "max_ms":  round(max(vals), 2),
        }
    return report


def print_table(report: dict) -> None:
    print()
    print("=" * 76)
    print(f"{'ПАРА':<28} {'N':>6} {'MIN':>7} {'AVG':>7} {'P95':>7} {'P99':>7} {'MAX':>7}")
    print("-" * 76)
    for pair, s in sorted(report.items(), key=lambda x: x[1]["avg_ms"]):
        print(f"{pair:<28} {s['samples']:>6} "
              f"{s['min_ms']:>6.1f} {s['avg_ms']:>6.1f} "
              f"{s['p95_ms']:>6.1f} {s['p99_ms']:>6.1f} {s['max_ms']:>6.1f}  мс")
    print("=" * 76)
    print()


async def main():
    global start_time
    os.makedirs("logs", exist_ok=True)

    print(f"[BENCH] Монеты: {TEST_COINS}")
    print(f"[BENCH] Прогрев: {WARMUP_SEC}с | Измерение: {MEASURE_SEC}с | Итого: {TOTAL_SEC}с\n")

    symbols_per_ex = {
        ex: [to_native(coin, ex) for coin in TEST_COINS]
        for ex in EXCHANGES
    }

    stream_map = {
        "BINANCE": BinanceStakanStream(symbols_per_ex["BINANCE"]),
        "KUCOIN":  KucoinStakanStream(symbols_per_ex["KUCOIN"]),
        "OKX":     OkxStakanStream(symbols_per_ex["OKX"]),
        "BITGET":  BitgetStakanStream(symbols_per_ex["BITGET"]),
        "PHEMEX":  PhemexStakanStream(symbols_per_ex["PHEMEX"]),
        "BYBIT":   BybitStakanStream(symbols_per_ex["BYBIT"]),
        "GATE":    GateStakanStream(symbols_per_ex["GATE"]),
    }

    start_time = time.time()
    tasks = []
    for ex, stream in stream_map.items():
        handler = make_handler(ex)
        tasks.append(asyncio.create_task(stream.run(handler)))
        print(f"[BENCH] Запущен {ex}")

    print(f"\n[BENCH] Ждём {WARMUP_SEC}с прогрева...\n")

    async def progress_ticker():
        while True:
            await asyncio.sleep(15)
            t = elapsed()
            total_samples = sum(len(v) for v in deltas.values())
            status = "ПРОГРЕВ" if not warmup_done else "ИЗМЕРЕНИЕ"
            print(f"[{t:>5.0f}s | {status}] дельт накоплено: {total_samples:,}")

    prog_task = asyncio.create_task(progress_ticker())
    await asyncio.sleep(TOTAL_SEC)

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
    print(f"\n[BENCH] Тест завершён. Всего дельт: {total_samples:,}")
    print_table(report)

    ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("logs", f"desync_bench_{ts_str}.json")
    meta = {
        "timestamp_utc": ts_str,
        "test_coins":    TEST_COINS,
        "warmup_sec":    WARMUP_SEC,
        "measure_sec":   MEASURE_SEC,
        "total_samples": total_samples,
        "pairs":         report,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print(f"[BENCH] Результат сохранён: {out_path}\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[BENCH] Прерван. Промежуточные результаты:")
        report = build_report()
        if report:
            print_table(report)
        else:
            print("  Данных нет.")
