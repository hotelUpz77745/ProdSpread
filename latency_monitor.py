# ============================================================
# FILE: latency_monitor.py
# ROLE: Exchange latency and ping monitoring.
# ============================================================
import asyncio
import aiohttp
import time
import statistics

async def measure_latency(session: aiohttp.ClientSession, exchange_name: str, url: str, num_pings: int = 10):
    latencies = []
    print(f"[{exchange_name}] Latency benchmark start ({num_pings} pings) to {url}")
    
    for i in range(num_pings):
        start_time = time.perf_counter()
        try:
            # 5 second timeout per request
            async with session.get(url, timeout=5) as response:
                await response.read()  # Await full response payload
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                end_time = time.perf_counter()
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                print(f"[{exchange_name}] Ping {i+1}: {latency_ms:.2f} ms")
        except Exception as e:
            print(f"[{exchange_name}] Error on ping {i+1}: {e}")
            
        await asyncio.sleep(0.3)  # Pause between requests to avoid rate limits
        
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        median_latency = statistics.median(latencies)
        print(f"[{exchange_name}] === SUMMARY ===")
        print(f"[{exchange_name}] Min:    {min_latency:.2f} ms")
        print(f"[{exchange_name}] Max:    {max_latency:.2f} ms")
        print(f"[{exchange_name}] Avg:    {avg_latency:.2f} ms")
        print(f"[{exchange_name}] Median: {median_latency:.2f} ms\n")
    else:
        print(f"[{exchange_name}] SUMMARY: No successful pings.\n")

async def main():
    # Futures/spot API endpoints for latency testing
    urls = {
        "BINANCE": "https://fapi.binance.com/fapi/v1/ping",
        "KUCOIN": "https://api-futures.kucoin.com/api/v1/timestamp",
        "OKX": "https://www.okx.com/api/v5/public/time",
        "BITGET": "https://api.bitget.com/api/v2/public/time"
    }
    
    async with aiohttp.ClientSession() as session:
        print("Starting concurrent latency benchmark (REST API)...\n")
        
        tasks = [
            measure_latency(session, ex_name, url, 10)
            for ex_name, url in urls.items()
        ]
        
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Benchmark interrupted by user.")
