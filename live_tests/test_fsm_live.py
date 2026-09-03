import asyncio
import os
import time
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()
from c_log import log

from utils import SessionManager
from API.orders import BinanceOrder, KucoinOrder, BitgetOrder
from CORE.position_fsm import PositionFSM, PositionState

class DummyWriter:
    def write(self, data):
        pass
    async def drain(self):
        pass

class DummyPM:
    def confirm_entry(self, *args, **kwargs):
        pass
    def confirm_exit(self, *args, **kwargs):
        pass
    def emergency_unwind_trigger(self, *args, **kwargs):
        log(f"PM Emergency Trigger: {args}", level="ERROR")

async def test_fsm_micro_lot():
    session = await SessionManager().get_session()
    
    binance = BinanceOrder(
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_API_SECRET", ""),
        session=session
    )

    bitget = BitgetOrder(
        api_key=os.environ.get("BITGET_API_KEY", ""),
        api_secret=os.environ.get("BITGET_API_SECRET", ""),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE", ""),
        session=session,
        margin_settings={"margin_type": "cross"}
    )
    
    kucoin = KucoinOrder(
        api_key=os.environ.get("KUCOIN_API_KEY", ""),
        api_secret=os.environ.get("KUCOIN_API_SECRET", ""),
        api_passphrase=os.environ.get("KUCOIN_API_PASSPHRASE", ""),
        session=session,
        position_stream=None,
        margin_settings={"margin_type": "cross", "leverage": 1}
    )

    print("Fetching specs for LIVE execution...")
    await bitget.update_symbol_info()

    import aiohttp
    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
        data = await resp.json()
        binance.symbol_info = data.get("symbols", [])
        
    async with session.get("https://api-futures.kucoin.com/api/v1/contracts/active") as resp:
        data = await resp.json()
        kucoin.symbol_info = data.get("data", [])
        
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=XRPUSDT") as resp:
        data = await resp.json()
        test_price = float(data["price"])

    print(f"XRPUSDT Market Price: {test_price}")

    cfg = {
        "EXECUTION_PAUSE": 0.5,
        "trading_rules": {
            "entry": {
                "order_execution_type": "IOC",
                "min_fill_rate": 0.5,
            }
        },
        "trading_risks": {
            "binance": {"limit_allow_distance": 1.002},
            "bitget": {"limit_allow_distance": 1.002}
        }
    }
    
    orders = {
        "BINANCE": binance,
        "BITGET": bitget,
        "KUCOIN": kucoin
    }

    engine_res = {
        "size_usd": 20.0, # Enough volume for Kucoin
        "long_avg_price": test_price,
        "short_avg_price": test_price,
        "long_qty": 20.0 / test_price,
        "short_qty": 20.0 / test_price
    }

    fsm = PositionFSM(
        sym="XRPUSDT",
        route="BINANCE_BITGET",
        long_ex="BINANCE",
        short_ex="BITGET",
        engine_res=engine_res,
        cfg=cfg,
        orders=orders,
        coin_to_native={"XRP": "XRP"},
        pm=DummyPM(),
        writer=DummyWriter(),
        ban_coin_cb=lambda s: print(f"Banned {s}")
    )

    print("\n--- RUNNING LIVE MICRO-LOT THROUGH FSM ---")
    print("WARNING: This WILL execute real market orders!")
    
    # We will use MARKET orders just to ensure fill. Since FSM uses LIMIT IOC by default, we'll override it if needed, or let FSM use its default execution logic
    # Actually FSM uses limit order if order_execution_type is LIMIT, else IOC.
    
    # To execute a real test, let's call fsm.run_open()
    res = await fsm.run_open()
    print(f"\nFSM run_open Result: {res}")
    print(f"FSM State: {fsm.state}")
    
    print("\nWaiting 2 seconds...")
    await asyncio.sleep(2.0)
    
    if fsm.state == PositionState.ACTIVE_HEDGED or fsm.state == PositionState.RESTING_BOOK:
        print("\n--- FORCING EMERGENCY UNWIND (Closing Position) ---")
        await fsm._emergency_unwind()
        print(f"FSM State after Unwind: {fsm.state}")
    else:
        print("\nPosition was not opened or already settled. No need to unwind.")

    await session.close()
    print("\nLive Integration FSM Test Completed.")

if __name__ == "__main__":
    asyncio.run(test_fsm_micro_lot())
