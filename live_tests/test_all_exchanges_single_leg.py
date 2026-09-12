# ============================================================
# FILE: live_tests/test_all_exchanges_single_leg.py
# ROLE: Comprehensive live verification of PositionFSM single_leg_exit across Binance, Kucoin, and Bitget.
# ============================================================

import asyncio
import os
import aiohttp
import json
import time
from dotenv import load_dotenv
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder
from CORE.position_fsm import PositionFSM, PositionState

async def test_exchange(ex_name: str, symbol: str, open_qty: float, cfg: dict, session: aiohttp.ClientSession):
    print(f"\n{'='*25} TESTING {ex_name} ({symbol}) {'='*25}")
    
    if ex_name == "BINANCE":
        adapter = BinanceOrder(
            api_key=os.getenv("BINANCE_API_KEY"),
            api_secret=os.getenv("BINANCE_API_SECRET"),
            session=session
        )
        adapter.start()
        for _ in range(50):
            if adapter.symbol_info:
                break
            await asyncio.sleep(0.1)
    elif ex_name == "KUCOIN":
        adapter = KucoinOrder(
            api_key=os.getenv("KUCOIN_API_KEY"),
            api_secret=os.getenv("KUCOIN_API_SECRET"),
            api_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"),
            session=session,
            position_stream=None,
            margin_settings=cfg["margin_settings"]["KUCOIN"]
        )
        adapter.start()
        for _ in range(50):
            if adapter.symbol_info:
                break
            await asyncio.sleep(0.1)
    elif ex_name == "BITGET":
        adapter = BitgetOrder(
            api_key=os.getenv("BITGET_API_KEY"),
            api_secret=os.getenv("BITGET_API_SECRET"),
            api_passphrase=os.getenv("BITGET_API_PASSPHRASE"),
            margin_settings=cfg["margin_settings"]["BITGET"],
            session=session
        )
        await adapter.update_symbol_info()
    else:
        raise ValueError(f"Unknown exchange: {ex_name}")

    orders = {ex_name: adapter}

    # 1. Fetch live book ticker
    bt = await adapter.get_book_ticker(symbol)
    print(f"[{ex_name}] Live Book Ticker: Bid={bt['bid']}, Ask={bt['ask']}")
    assert bt['bid'] > 0 and bt['ask'] > 0, f"Invalid ticker on {ex_name}"

    # 2. Open Long
    open_price = bt['ask']
    usd_open = open_qty * open_price
    print(f"[{ex_name}] 1. Opening simulated hanging leg: BUY {open_qty} {symbol} @ {open_price} ({usd_open:.2f} USD)...")
    res_open = await adapter.place_order(symbol, "BUY", usd_open, open_price, order_type="MARKET", position_side="LONG")
    print(f"[{ex_name}] Open Response:", res_open)
    await asyncio.sleep(1.0)

    # 3. Confirm position
    pos = await adapter.get_position_rest(symbol, "LONG")
    actual_size = pos.get("size", 0.0)
    actual_price = pos.get("price", open_price)
    print(f"[{ex_name}] 2. Position confirmed: size={actual_size}, entry_price={actual_price}")
    assert actual_size > 0, f"Position was not opened on {ex_name}"

    # 4. Run PositionFSM single leg exposure
    print(f"[{ex_name}] 3. Running PositionFSM._run_single_leg_exposure (immediate_market=False)...")
    banned_calls = []
    def mock_ban(sym, reason="", duration_sec=None):
        banned_calls.append((sym, reason, duration_sec))
        print(f"[{ex_name} MOCK BAN] Coin {sym} quarantined for {duration_sec}s. Reason: {reason}")

    other_ex = "BINANCE" if ex_name != "BINANCE" else "BITGET"
    fsm = PositionFSM(
        sym="XRP",
        route=f"{ex_name}_{other_ex}",
        long_ex=ex_name,
        short_ex=other_ex,
        engine_res={"long_avg_price": actual_price, "short_avg_price": actual_price},
        cfg=cfg,
        orders=orders,
        coin_to_native={"XRP": {ex_name: symbol, other_ex: "XRPUSDT"}},
        pm=None,
        writer=None,
        ban_coin_cb=mock_ban
    )

    t0 = time.time()
    await fsm._run_single_leg_exposure(l_qty=actual_size, s_qty=0.0, l_price=actual_price, s_price=0.0)
    elapsed = time.time() - t0
    print(f"[{ex_name}] _run_single_leg_exposure completed in {elapsed:.2f}s. State: {fsm.state}")

    # 5. Verify position zeroed
    final_pos = await adapter.get_position_rest(symbol, "LONG")
    print(f"[{ex_name}] 4. Final Position Check: {final_pos}")
    assert final_pos.get("size", 0.0) == 0.0, f"Position on {ex_name} still open: {final_pos}"
    print(f">>> {ex_name} PASSED: Cleanly closed via limit chasing! <<<")

async def main():
    load_dotenv()
    with open("cfg.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg["trading_rules"]["exit"]["single_leg_exit"]["immediate_market"] = False

    async with aiohttp.ClientSession() as session:
        # Test BITGET
        await test_exchange("BITGET", "XRPUSDT", 5.0, cfg, session)
        await asyncio.sleep(1.0)
        # Test BINANCE
        await test_exchange("BINANCE", "XRPUSDT", 5.0, cfg, session)
        await asyncio.sleep(1.0)
        # Test KUCOIN
        await test_exchange("KUCOIN", "XRPUSDTM", 10.0, cfg, session)

    print("\n" + "="*70)
    print("ALL 3 EXCHANGES (BITGET, BINANCE, KUCOIN) FULLY VERIFIED AND PASSED!")
    print("="*70)

if __name__ == "__main__":
    asyncio.run(main())
