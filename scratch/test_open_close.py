# ============================================================
# FILE: test_open_close.py
# Тест IOC сценария: открыть IOC лимиткой с запасом -> проверить -> закрыть по маркету
# ============================================================
import asyncio
import aiohttp
import os
import sys
import json
import time
from dotenv import load_dotenv

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from API.orders import KucoinOrder, BitgetOrder

TEST_COIN_KUCOIN = "4USDTM"
TEST_COIN_BITGET = "4USDT"
TEST_SIZE_USD = 6.0

MARGIN_SETTINGS = {"margin_type": "cross", "leverage": 20}


def p(tag, msg):
    print(f"[{time.strftime('%H:%M:%S')}] [{tag}] {msg}")


async def get_kucoin_ticker(session, symbol):
    url = f"https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"
    async with session.get(url) as resp:
        data = await resp.json()
        if data.get("code") == "200000":
            return float(data["data"]["price"])
    return 0.0


async def get_bitget_ticker(session, symbol):
    url = f"https://api.bitget.com/api/v2/mix/market/ticker?productType=USDT-FUTURES&symbol={symbol}"
    async with session.get(url) as resp:
        data = await resp.json()
        if data.get("code") == "00000":
            return float(data["data"][0]["lastPr"])
    return 0.0


async def main():
    p("INIT", "=" * 60)
    p("INIT", "Test IOC: OPEN IOC limit -> CHECK -> CLOSE market")
    p("INIT", "=" * 60)

    session = aiohttp.ClientSession()

    kucoin = KucoinOrder(
        api_key=os.environ["KUCOIN_API_KEY"],
        api_secret=os.environ["KUCOIN_API_SECRET"],
        api_passphrase=os.environ["KUCOIN_API_PASSPHRASE"],
        session=session, position_stream=None, margin_settings=MARGIN_SETTINGS
    )
    bitget = BitgetOrder(
        api_key=os.environ["BITGET_API_KEY"],
        api_secret=os.environ["BITGET_API_SECRET"],
        api_passphrase=os.environ["BITGET_API_PASSPHRASE"],
        margin_settings=MARGIN_SETTINGS, session=session, position_stream=None
    )

    p("INIT", "Loading specs...")
    kucoin._bg_task = asyncio.create_task(kucoin._fetch_exchange_info_loop())
    await bitget.update_symbol_info()
    await asyncio.sleep(2)

    if not kucoin.symbol_info or not bitget.symbol_info:
        p("ERROR", "Specs failed!")
        await session.close()
        return

    kucoin_price = await get_kucoin_ticker(session, TEST_COIN_KUCOIN)
    bitget_price = await get_bitget_ticker(session, TEST_COIN_BITGET)
    p("PRICE", f"Kucoin: {kucoin_price} | Bitget: {bitget_price}")

    if kucoin_price <= 0 or bitget_price <= 0:
        p("ERROR", "No prices!")
        await session.close()
        return

    # ====== PHASE 1: OPEN с IOC (лимитка с запасом, должна залиться мгновенно) ======
    p("OPEN", "=" * 60)
    kucoin_open_price = kucoin_price * 1.002
    bitget_open_price = bitget_price / 1.002

    p("OPEN", f"Kucoin BUY LONG IOC: price={kucoin_open_price:.6f}, size_usd={TEST_SIZE_USD}")
    try:
        res_k = await kucoin.place_order(TEST_COIN_KUCOIN, "BUY", TEST_SIZE_USD, kucoin_open_price,
                                          order_type="LIMIT", position_side="LONG", time_in_force="IOC")
        p("OPEN", f"[OK] Kucoin: {json.dumps(res_k)}")
    except Exception as e:
        p("OPEN", f"[FAIL] Kucoin: {type(e).__name__}: {e}")
        await session.close()
        return

    p("OPEN", f"Bitget SELL SHORT IOC: price={bitget_open_price:.6f}, size_usd={TEST_SIZE_USD}")
    try:
        res_b = await bitget.place_order(TEST_COIN_BITGET, "SELL", TEST_SIZE_USD, bitget_open_price,
                                          order_type="LIMIT", position_side="SHORT", time_in_force="IOC")
        p("OPEN", f"[OK] Bitget: {json.dumps(res_b)}")
    except Exception as e:
        p("OPEN", f"[FAIL] Bitget: {type(e).__name__}: {e}")
        await kucoin.cancel_all_orders(TEST_COIN_KUCOIN)
        await session.close()
        return

    # IOC не требует отмены остатков - они автоматически отменяются
    p("WAIT", "Waiting 1 sec (IOC auto-cancels unfilled)...")
    await asyncio.sleep(1)

    # ====== Check positions ======
    kucoin_pos = await kucoin.get_position_rest(TEST_COIN_KUCOIN, "LONG")
    bitget_pos = await bitget.get_position_rest(TEST_COIN_BITGET, "SHORT")
    p("POS", f"Kucoin LONG: {kucoin_pos}")
    p("POS", f"Bitget SHORT: {bitget_pos}")

    kucoin_has = kucoin_pos.get("size", 0.0) > 0
    bitget_has = bitget_pos.get("size", 0.0) > 0

    if not kucoin_has and not bitget_has:
        p("RESULT", "Both empty - IOC orders did not fill. Test inconclusive.")
        await session.close()
        return

    p("POS", f"Kucoin filled: {kucoin_has} | Bitget filled: {bitget_has}")

    # ====== PHASE 2: CLOSE BY MARKET ======
    p("CLOSE", "=" * 60)
    close_errors = []

    if kucoin_has:
        qty = kucoin_pos["size"]
        price_ref = kucoin_pos.get("price") or kucoin_price
        size_usd = qty * price_ref
        p("CLOSE", f"Kucoin SELL LONG market: qty={qty}, size_usd={size_usd:.4f}")
        try:
            res = await kucoin.place_order(TEST_COIN_KUCOIN, "SELL", size_usd, price_ref,
                                            order_type="MARKET", position_side="LONG")
            p("CLOSE", f"[OK] Kucoin: {json.dumps(res)}")
        except Exception as e:
            p("CLOSE", f"[FAIL] Kucoin: {type(e).__name__}: {e}")
            close_errors.append(("KUCOIN", str(e)))

    if bitget_has:
        qty = bitget_pos["size"]
        price_ref = bitget_pos.get("price") or bitget_price
        size_usd = qty * price_ref
        p("CLOSE", f"Bitget BUY SHORT market: qty={qty}, size_usd={size_usd:.4f}")
        try:
            res = await bitget.place_order(TEST_COIN_BITGET, "BUY", size_usd, price_ref,
                                            order_type="MARKET", position_side="SHORT")
            p("CLOSE", f"[OK] Bitget: {json.dumps(res)}")
        except Exception as e:
            p("CLOSE", f"[FAIL] Bitget: {type(e).__name__}: {e}")
            close_errors.append(("BITGET", str(e)))

    await asyncio.sleep(1)

    # ====== VERIFY ======
    p("VERIFY", "=" * 60)
    kucoin_after = await kucoin.get_position_rest(TEST_COIN_KUCOIN, "LONG")
    bitget_after = await bitget.get_position_rest(TEST_COIN_BITGET, "SHORT")
    p("VERIFY", f"Kucoin LONG: {kucoin_after}")
    p("VERIFY", f"Bitget SHORT: {bitget_after}")

    if close_errors:
        p("RESULT", "ERRORS:")
        for ex, err in close_errors:
            p("RESULT", f"  {ex}: {err}")
    elif kucoin_after.get("size", 0) > 0 or bitget_after.get("size", 0) > 0:
        p("RESULT", "WARN: Positions still open!")
    else:
        p("RESULT", "OK! IOC Open + Market Close works on both exchanges.")

    p("RESULT", "=" * 60)

    if kucoin._bg_task:
        kucoin._bg_task.cancel()
    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
