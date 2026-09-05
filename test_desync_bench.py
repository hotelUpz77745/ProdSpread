# ============================================================
# FILE: test_desync_bench.py (Root Launcher)
# ROLE: Измерение реального рассинхрона стаканов (max_desync_ms)
#       в точности так, как его вычисляет боевой бот в main.py
# ============================================================
import asyncio
from live_tests.test_desync_bench import main

if __name__ == "__main__":
    asyncio.run(main())
