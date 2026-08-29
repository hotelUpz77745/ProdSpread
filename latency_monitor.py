# ============================================================
# FILE: latency_monitor.py
# ROLE: Мониторинг задержек (пингов) до бирж.
# ============================================================
import asyncio
import aiohttp
import time
import statistics

async def measure_latency(session: aiohttp.ClientSession, exchange_name: str, url: str, num_pings: int = 10):
    latencies = []
    print(f"[{exchange_name}] Начало замера ({num_pings} пингов) к {url}")
    
    for i in range(num_pings):
        start_time = time.perf_counter()
        try:
            # timeout в 5 секунд на каждый запрос
            async with session.get(url, timeout=5) as response:
                await response.read()  # Ожидаем полную загрузку ответа
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                end_time = time.perf_counter()
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                print(f"[{exchange_name}] Пинг {i+1}: {latency_ms:.2f} мс")
        except Exception as e:
            print(f"[{exchange_name}] Ошибка при пинге {i+1}: {e}")
            
        await asyncio.sleep(0.3)  # Небольшая пауза между запросами, чтобы нас не забанили
        
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        median_latency = statistics.median(latencies)
        print(f"[{exchange_name}] === ИТОГ ===")
        print(f"[{exchange_name}] Мин:    {min_latency:.2f} мс")
        print(f"[{exchange_name}] Макс:   {max_latency:.2f} мс")
        print(f"[{exchange_name}] Средн:  {avg_latency:.2f} мс")
        print(f"[{exchange_name}] Медиана:{median_latency:.2f} мс\n")
    else:
        print(f"[{exchange_name}] ИТОГ: Нет успешных пингов.\n")

async def main():
    # Эндпоинты фьючерсных/спотовых API для тестирования задержки
    urls = {
        "BINANCE": "https://fapi.binance.com/fapi/v1/ping",
        "KUCOIN": "https://api-futures.kucoin.com/api/v1/timestamp",
        "OKX": "https://www.okx.com/api/v5/public/time",
        "BITGET": "https://api.bitget.com/api/v2/public/time",
        "PHEMEX": "https://api.phemex.com/public/time",
        "BYBIT": "https://api.bybit.com/v5/market/time",
        "GATE": "https://api.gateio.ws/api/v4/spot/time"
    }
    
    async with aiohttp.ClientSession() as session:
        print("Запуск параллельного замера латенции (REST API)...\n")
        
        tasks = [
            measure_latency(session, ex_name, url, 10)
            for ex_name, url in urls.items()
        ]
        
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Замер прерван пользователем.")
