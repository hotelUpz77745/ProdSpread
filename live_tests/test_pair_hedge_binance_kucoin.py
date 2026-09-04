# ============================================================
# FILE: live_tests/test_pair_hedge_binance_kucoin.py
# ROLE: Тестовый замер связки BINANCE-KUCOIN:
#       1. Запуск стаканов и приватных стримов
#       2. Замер спреда перед входом (по стакану)
#       3. Вход хеджированными рыночными ордерами
#       4. Замер времени матчинга двух ног (латенси исполнения)
#       5. Замер спреда после входа (на основании данных стрима позиций)
#       6. Пауза 2-3 секунды
#       7. Замер спреда перед выходом (по стакану)
#       8. Выход хеджированными рыночными ордерами
#       9. Замер времени матчинга двух ног при выходе
#      10. Замер спреда после выхода (по ценам закрытия из стримов)
#      11. Подробный лог в консоль и файл logs/test_hedge_binance_kucoin.log
# ============================================================
import asyncio
import os
import sys
import time
import json
import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from API.BINANCE.stakan import BinanceStakanStream
from API.KUCOIN.stakan import KucoinStakanStream
from API.BINANCE.ws_private_binance import BinancePositionStream
from API.KUCOIN.ws_private_kucoin import KucoinPositionStream
from API.orders import BinanceOrder, KucoinOrder

LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "test_hedge_binance_kucoin.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log_msg(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

async def main():
    log_msg("=" * 75)
    log_msg("🚀 СТАРТ ТЕСТОВОГО ПРОГОНА ХЕДЖ-СВЯЗКИ: BINANCE <-> KUCOIN")
    log_msg("=" * 75)

    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    session = aiohttp.ClientSession()

    TEST_COIN = "ADA"
    SYM_BINANCE = "ADAUSDT"
    SYM_KUCOIN = "ADAUSDTM"
    ORDER_SIZE_USD = 6.0  # Минимально допустимый размер (> 5 USD)

    # 1. Приватные стримы позиций
    b_stream = BinancePositionStream(os.environ["BINANCE_API_KEY"], os.environ["BINANCE_API_SECRET"])
    k_stream = KucoinPositionStream(os.environ["KUCOIN_API_KEY"], os.environ["KUCOIN_API_SECRET"], os.environ["KUCOIN_API_PASSPHRASE"])

    t_b_stream = asyncio.create_task(b_stream.start())
    t_k_stream = asyncio.create_task(k_stream.start())

    # 2. Адаптеры ордеров
    net_cfg = cfg.get("network_settings", {})
    b_order = BinanceOrder(os.environ["BINANCE_API_KEY"], os.environ["BINANCE_API_SECRET"], session, b_stream, network_settings=net_cfg)
    k_order = KucoinOrder(os.environ["KUCOIN_API_KEY"], os.environ["KUCOIN_API_SECRET"], os.environ["KUCOIN_API_PASSPHRASE"], session, k_stream, cfg["margin_settings"]["KUCOIN"], network_settings=net_cfg)

    # Загружаем спецификации
    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as r:
        b_order.symbol_info = (await r.json()).get("symbols", [])
    async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as r:
        k_order.symbol_info = (await r.json()).get("data", [])

    log_msg("[1/5] Прогрев и ожидание готовности WebSocket стримов...")
    for _ in range(50):
        if b_stream.ready and k_stream.ready:
            break
        await asyncio.sleep(0.1)

    log_msg(f"  Binance Stream Ready: {b_stream.ready}, Kucoin Stream Ready: {k_stream.ready}")

    # Прогреваем REST-сессии
    await asyncio.gather(b_order.warmup(), k_order.warmup())
    log_msg("  REST-сессии (TCP/TLS keepalive) прогреты.")

    # 3. Публичные стаканы
    books = {"BINANCE": {}, "KUCOIN": {}}
    def make_handler(ex):
        def cb(d):
            mult = 10.0 if ex == "KUCOIN" else 1.0
            books[ex] = {
                "bids": [(p, q * mult) for p, q in d.bids] if d.bids else [],
                "asks": [(p, q * mult) for p, q in d.asks] if d.asks else []
            }
        return cb

    b_stakan = BinanceStakanStream([SYM_BINANCE])
    k_stakan = KucoinStakanStream([SYM_KUCOIN])
    t_bs = asyncio.create_task(b_stakan.run(make_handler("BINANCE")))
    t_ks = asyncio.create_task(k_stakan.run(make_handler("KUCOIN")))

    log_msg("[2/5] Ожидание поступления стаканов...")
    for _ in range(50):
        if books["BINANCE"].get("asks") and books["KUCOIN"].get("bids"):
            break
        await asyncio.sleep(0.1)

    b_ask0 = books["BINANCE"]["asks"][0][0]
    k_bid0 = books["KUCOIN"]["bids"][0][0]
    pre_spread_gross = (k_bid0 - b_ask0) / b_ask0
    taker_fee_b = float(cfg["trading_risks"]["binance"]["taker_fee"])
    taker_fee_k = float(cfg["trading_risks"]["kucoin"]["taker_fee"])
    entry_comm = taker_fee_b + taker_fee_k
    pre_spread_net = pre_spread_gross - entry_comm

    log_msg(f"📊 [СПРЕД ПЕРЕД ВХОДОМ]:")
    log_msg(f"   Binance Ask: {b_ask0:.5f} | Kucoin Bid: {k_bid0:.5f}")
    log_msg(f"   Gross Spread: {pre_spread_gross*100:+.4f}% | Net Spread: {pre_spread_net*100:+.4f}%")

    # 4. Вход хеджированными ордерами: LONG Binance, SHORT Kucoin
    log_msg("\n[3/5] ⚡ ОТПРАВКА ОРДЕРОВ ВХОДА (LONG BINANCE | SHORT KUCOIN)...")
    t_open_start = time.perf_counter()

    task_b_open = asyncio.create_task(b_order.place_order(SYM_BINANCE, "BUY", ORDER_SIZE_USD, b_ask0, order_type="MARKET", position_side="LONG"))
    task_k_open = asyncio.create_task(k_order.place_order(SYM_KUCOIN, "SELL", ORDER_SIZE_USD, k_bid0, order_type="MARKET", position_side="SHORT"))

    res_b_open, res_k_open = await asyncio.gather(task_b_open, task_k_open, return_exceptions=True)
    t_open_end = time.perf_counter()
    total_entry_rtt_ms = (t_open_end - t_open_start) * 1000.0

    log_msg(f"   Binance Open Response: {res_b_open if not isinstance(res_b_open, Exception) else f'ERROR: {res_b_open}'}")
    log_msg(f"   Kucoin Open Response:  {res_k_open if not isinstance(res_k_open, Exception) else f'ERROR: {res_k_open}'}")
    log_msg(f"⏱  [ЛАТЕНСИ ВХОДА ОБЕИХ НОГ]: {total_entry_rtt_ms:.2f} мс")

    # Опрос стрима позиций на подтверждение налива
    t_poll_start = time.perf_counter()
    p_long_entry = 0.0
    p_short_entry = 0.0
    size_long = 0.0
    size_short = 0.0

    for _ in range(100):
        pos_b = b_order.get_executed_position(SYM_BINANCE, "LONG")
        pos_k = k_order.get_executed_position(SYM_KUCOIN, "SHORT")
        if pos_b.get("size", 0.0) > 0 and pos_k.get("size", 0.0) > 0:
            size_long = pos_b["size"]
            size_short = pos_k["size"]
            p_long_entry = pos_b.get("price", 0.0) or b_ask0
            p_short_entry = pos_k.get("price", 0.0) or k_bid0
            break
        await asyncio.sleep(0.005)

    t_poll_ms = (time.perf_counter() - t_poll_start) * 1000.0
    log_msg(f"⏱  [ПОДТВЕРЖДЕНИЕ НАЛИВА ПО WS]: {t_poll_ms:.2f} мс")
    log_msg(f"   Налито: Binance Long = {size_long} шт @ {p_long_entry:.5f}, Kucoin Short = {size_short} шт @ {p_short_entry:.5f}")

    if p_long_entry > 0 and p_short_entry > 0:
        actual_entry_gross = (p_short_entry - p_long_entry) / p_long_entry
        actual_entry_net = actual_entry_gross - entry_comm
        log_msg(f"📊 [ФАКТИЧЕСКИЙ СПРЕД ВХОДА (ПО СТРИМАМ)]: Gross: {actual_entry_gross*100:+.4f}% | Net: {actual_entry_net*100:+.4f}%")

    log_msg("\n⏳ Выдержка позиции 3.0 секунды...")
    await asyncio.sleep(3.0)

    # 5. Замер перед выходом (по текущему стакану)
    b_bid_exit = books["BINANCE"]["bids"][0][0] if books["BINANCE"].get("bids") else b_ask0
    k_ask_exit = books["KUCOIN"]["asks"][0][0] if books["KUCOIN"].get("asks") else k_bid0
    pre_exit_gross = (k_ask_exit - b_bid_exit) / b_bid_exit
    log_msg(f"📊 [СПРЕД ПЕРЕД ВЫХОДОМ (ПО СТАКАНУ)]:")
    log_msg(f"   Binance Bid: {b_bid_exit:.5f} | Kucoin Ask: {k_ask_exit:.5f}")
    log_msg(f"   Gross Exit Spread: {pre_exit_gross*100:+.4f}%")

    # 6. Выход хеджированными ордерами: SELL Binance, BUY Kucoin
    log_msg("\n[4/5] ⚡ ОТПРАВКА ОРДЕРОВ ЗАКРЫТИЯ (SELL BINANCE | BUY KUCOIN)...")
    t_close_start = time.perf_counter()

    size_b_usd = size_long * b_bid_exit if size_long > 0 else ORDER_SIZE_USD
    size_k_usd = size_short * k_ask_exit if size_short > 0 else ORDER_SIZE_USD

    task_b_close = asyncio.create_task(b_order.place_order(SYM_BINANCE, "SELL", size_b_usd, b_bid_exit, order_type="MARKET", position_side="LONG"))
    task_k_close = asyncio.create_task(k_order.place_order(SYM_KUCOIN, "BUY", size_k_usd, k_ask_exit, order_type="MARKET", position_side="SHORT"))

    res_b_close, res_k_close = await asyncio.gather(task_b_close, task_k_close, return_exceptions=True)
    t_close_end = time.perf_counter()
    total_exit_rtt_ms = (t_close_end - t_close_start) * 1000.0

    log_msg(f"   Binance Close Response: {res_b_close if not isinstance(res_b_close, Exception) else f'ERROR: {res_b_close}'}")
    log_msg(f"   Kucoin Close Response:  {res_k_close if not isinstance(res_k_close, Exception) else f'ERROR: {res_k_close}'}")
    log_msg(f"⏱  [ЛАТЕНСИ ВЫХОДА ОБЕИХ НОГ]: {total_exit_rtt_ms:.2f} мс")

    # Ожидание обнуления по WS
    t_zero_start = time.perf_counter()
    close_p_b = 0.0
    close_p_k = 0.0
    for _ in range(100):
        pos_b = b_order.get_executed_position(SYM_BINANCE, "LONG")
        pos_k = k_order.get_executed_position(SYM_KUCOIN, "SHORT")
        if hasattr(b_order, "get_last_close_price"):
            close_p_b = b_order.get_last_close_price(SYM_BINANCE)
        if hasattr(k_order, "get_last_close_price"):
            close_p_k = k_order.get_last_close_price(SYM_KUCOIN)
        if pos_b.get("size", 0.0) == 0.0 and pos_k.get("size", 0.0) == 0.0:
            break
        await asyncio.sleep(0.005)

    t_zero_ms = (time.perf_counter() - t_zero_start) * 1000.0
    log_msg(f"⏱  [ПОДТВЕРЖДЕНИЕ ОБНУЛЕНИЯ ПО WS (0.0)]: {t_zero_ms:.2f} мс")

    if not close_p_b:
        close_p_b = b_bid_exit
    if not close_p_k:
        close_p_k = k_ask_exit

    log_msg(f"📊 [ФАКТИЧЕСКИЕ ЦЕНЫ ЗАКРЫТИЯ (ПО СТРИМАМ)]:")
    log_msg(f"   Binance Long Close: {close_p_b:.5f} | Kucoin Short Close: {close_p_k:.5f}")

    pnl_b = (close_p_b - p_long_entry) / p_long_entry if p_long_entry > 0 else 0.0
    pnl_k = (p_short_entry - close_p_k) / p_short_entry if p_short_entry > 0 else 0.0
    total_fee = entry_comm * 2.0
    net_pnl = pnl_b + pnl_k - total_fee

    log_msg(f"\n[5/5] 🏁 ФИНАЛЬНЫЙ РЕЗУЛЬТАТ СДЕЛКИ:")
    log_msg(f"   PnL Binance (Long):  {pnl_b*100:+.4f}%")
    log_msg(f"   PnL Kucoin (Short):  {pnl_k*100:+.4f}%")
    log_msg(f"   Суммарная комиссия: -{total_fee*100:.4f}%")
    log_msg(f"   Итоговый чистый PnL: {net_pnl*100:+.4f}%")

    # Контрольная проверка через REST GET
    rest_b = await b_order.get_exact_position_guarded(SYM_BINANCE, "LONG")
    rest_k = await k_order.get_exact_position_guarded(SYM_KUCOIN, "SHORT")
    log_msg(f"   Остаток на бирже (REST): Binance={rest_b.get('size')}, Kucoin={rest_k.get('size')}")

    # Завершение
    await b_stakan.aclose()
    await k_stakan.aclose()
    await b_stream.stop()
    await k_stream.stop()
    t_bs.cancel()
    t_ks.cancel()
    t_b_stream.cancel()
    t_k_stream.cancel()
    await session.close()
    log_msg("=" * 75)
    log_msg("✅ ТЕСТ ЗАВЕРШЕН!")
    log_msg("=" * 75)

if __name__ == "__main__":
    asyncio.run(main())
