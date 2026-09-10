# ============================================================
# FILE: live_tests/test_desync_bench.py
# ROLE: Measure real orderbook desync (max_desync_ms)
#       identically to main.py production calculation
# ============================================================
"""
ORDERBOOK DESYNC BENCHMARK (100% ANALOG TO main.py)
===================================================
In production bot (main.py):
  1. On each orderbook WS frame arrival:
       self.ts[exchange_name][base_coin] = time.monotonic()
  2. In calculation loop before firing:
       diff_ms = abs(self.ts[long_ex][sym] - self.ts[short_ex][sym]) * 1000.0
       if diff_ms > self.entry_desync_limit:  # (max_desync_ms)
           continue  # drop desynchronized signal

This script runs identical L2 book streams,
records self.ts via time.monotonic(), loops with MAIN_LOOP_DELAY,
and measures the empirical distribution of diff_ms across live pairs.

Execution:
    python live_tests/test_desync_bench.py
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

from API.discovery import DiscoveryManager
from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.BITGET.stakan import BitgetStakanStream
from API.OKX.stakan import OkxStakanStream

DURATION_SECONDS = 45  # Benchmark duration (seconds)


async def main():
    print("=" * 85)
    print("ORDERBOOK DESYNC BENCHMARK (Identical to main.py logic)")
    print(f"   Measuring diff_ms = abs(self.ts[ex1] - self.ts[ex2]) * 1000.0 on live sockets")
    print("=" * 85)

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg_path = os.path.join(root_dir, "cfg.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    active_routes_cfg = cfg.get("active_routes", {})
    entry_desync_limit = cfg.get("trading_rules", {}).get("entry", {}).get("max_desync_ms", 125)
    main_loop_delay = cfg.get("MAIN_LOOP_DELAY", 0.0)

    print("Configuration:")
    print(f"  * Current max_desync_ms in cfg.json: {entry_desync_limit} ms")
    print(f"  * MAIN_LOOP_DELAY:                  {main_loop_delay} s")

    # 1. Build instrument topology (identical to main.py)
    print("\n[1/4] Building instrument topology via Discovery...")
    discovery = DiscoveryManager(quote=cfg.get("QUOTE", "USDT"))
    await discovery.build_topology()

    active_routes = [r for r, is_active in active_routes_cfg.items() if is_active]
    print(f"  Active routes: {active_routes}")
    print(f"  Common symbols in pool: {len(discovery.active_pairs_map)}")

    if not discovery.active_pairs_map:
        print("No common symbols for active routes. Exiting.")
        await discovery.aclose()
        return

    # 2. Launch public orderbook streams (identical to main.py)
    print("\n[2/4] Starting public orderbook WebSocket streams...")
    stream_classes = {
        "BINANCE": BinanceStakanStream,
        "KUCOIN": KucoinStakanStream,
        "OKX": OkxStakanStream,
        "BITGET": BitgetStakanStream
    }

    streams = {}
    tasks = []
    
    # self.ts[exchange_name][base_coin] = time.monotonic() (1-to-1 as in main.py)
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
                # Timestamp capture identical to main.py
                ts[exchange_name][base_coin] = time.monotonic()
                books[exchange_name][base_coin] = {"bids": d.bids, "asks": d.asks}
        return on_depth

    for ex, syms in discovery.ws_routes.items():
        if syms and ex in stream_classes:
            stream = stream_classes[ex](syms)
            streams[ex] = stream
            handler = make_depth_handler(ex)
            tasks.append(asyncio.create_task(stream.run(handler)))
            print(f"  {ex}: stream started ({len(syms)} tickers)")

    # 3. Warm up (await initial frames)
    print("\n[3/4] Warming up sockets (5s)...")
    await asyncio.sleep(5.0)

    # 4. Measurement calculation loop (1-to-1 main.py logic)
    print(f"\n[4/4] Collecting real desync statistics ({DURATION_SECONDS}s)...")
    print("Press Ctrl+C to terminate early and print report.\n")

    # Sample storage: samples[route] = [diff_ms, ...]
    samples: Dict[str, List[float]] = defaultdict(list)
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

                    # Check timestamps exist (as in main.py)
                    if sym not in ts[long_ex] or sym not in ts[short_ex]:
                        continue

                    t_long = ts[long_ex][sym]
                    t_short = ts[short_ex][sym]

                    # Freshness check (< 5.0s, as in main.py)
                    if (now_mono - t_long) <= 5.0 and (now_mono - t_short) <= 5.0:
                        # Calculation identical to main.py:
                        diff_ms = abs(t_long - t_short) * 1000.0
                        samples[route].append(diff_ms)
                        coin_samples[route][sym].append(diff_ms)

            # Progress every 5s
            if now_mono - last_print_time >= 5.0:
                elapsed_s = int(now_mono - start_time)
                parts_str = []
                for r in active_routes:
                    cnt = len(samples[r])
                    if cnt > 0:
                        recent = samples[r][-500:]
                        parts_str.append(f"{r}: avg={np.mean(recent):.1f}ms, p95={np.percentile(recent, 95):.1f}ms ({cnt:,} samples)")
                print(f"  [{elapsed_s:2d}s/{DURATION_SECONDS}s] " + " | ".join(parts_str))
                last_print_time = now_mono

            if main_loop_delay > 0:
                await asyncio.sleep(main_loop_delay)
            else:
                await asyncio.sleep(0.005)  # 5ms minimum quantum for benchmark

    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user (Ctrl+C). Generating report...")
    finally:
        for t in tasks:
            t.cancel()
        for s in streams.values():
            try:
                await s.aclose()
            except Exception:
                pass
        await discovery.aclose()

    # 5. Final Report
    print("\n" + "=" * 92)
    print("FINAL ORDERBOOK DESYNC REPORT (IDENTICAL TO main.py diff_ms)")
    print("=" * 92)

    thresholds = [50, 75, 100, 125, 150, 200]

    for route in active_routes:
        data = samples.get(route, [])
        if not data:
            print(f"\n[{route}]: no samples (verify connection).")
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

        print(f"\nROUTE: {route} (Sample size: {count:,} checks)")
        print("-" * 92)
        print(f"  Latency metrics (diff_ms = abs(ts_ex1 - ts_ex2) * 1000.0):")
        print(f"    MIN:    {min_v:6.2f} ms")
        print(f"    AVG:    {mean_v:6.2f} ms  (Average desync)")
        print(f"    P50:    {p50:6.2f} ms  (Median: 50% of all checks)")
        print(f"    P75:    {p75:6.2f} ms")
        print(f"    P90:    {p90:6.2f} ms")
        print(f"    P95:    {p95:6.2f} ms  (95% of checks are faster than this threshold)")
        print(f"    P99:    {p99:6.2f} ms")
        print(f"    MAX:    {max_v:6.2f} ms")
        print()
        if isinstance(entry_desync_limit, dict):
            pair_limit = entry_desync_limit.get(route, entry_desync_limit.get("default", 150))
        else:
            pair_limit = entry_desync_limit

        print(f"  Signal pass rate across max_desync_ms thresholds (current threshold for {route}: {pair_limit} ms):")
        for th in thresholds:
            pass_pct = (np.sum(arr <= th) / count) * 100.0
            marker = f"<- [CURRENT CFG FOR {route}]" if th == pair_limit else ""
            print(f"    <= {th:3d} ms: {pass_pct:6.2f}% of signals pass to execution {marker}")

        rec_val = max(75, int(np.ceil(p95 / 5.0) * 5))
        print(f"\n  RECOMMENDATION FOR {route}:")
        pass_at_current = (np.sum(arr <= pair_limit) / count) * 100.0
        if p95 <= pair_limit:
            print(f"     Threshold {pair_limit} ms is optimal: passes {pass_at_current:.1f}% signals and cuts desyncs.")
        else:
            print(f"     At threshold {pair_limit} ms, {pass_at_current:.1f}% signals pass (P95={p95:.1f} ms).")
            print(f"     Recommended optimal setting: max_desync_ms = {rec_val} ms.")

    print("\n" + "=" * 92)


if __name__ == "__main__":
    asyncio.run(main())
