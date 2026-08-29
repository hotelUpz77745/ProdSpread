import asyncio
import os
import sys
import aiohttp
from dotenv import load_dotenv

# Ensure we can import from CORE
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from API.orders import BinanceOrder, KucoinOrder
from CORE.leverage_setter import LeverageSetter
from CORE.utils import log

class DummyBot:
    def __init__(self, session):
        self.cfg = {
            "setup_margin_leverage": True,
            "margin_settings": {
                "BINANCE": {
                    "leverage": 11,
                    "margin_type": "ISOLATED"
                },
                "KUCOIN": {
                    "leverage": 11,
                    "margin_type": "ISOLATED"
                }
            }
        }
        
        self.orders = {
            "BINANCE": BinanceOrder(
                api_key=os.getenv("BINANCE_API_KEY"), 
                api_secret=os.getenv("BINANCE_API_SECRET"), 
                session=session,
                position_stream=None
            ),
            "KUCOIN": KucoinOrder(
                api_key=os.getenv("KUCOIN_API_KEY"), 
                api_secret=os.getenv("KUCOIN_API_SECRET"), 
                api_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"), 
                session=session,
                position_stream=None,
                margin_settings=self.cfg["margin_settings"]["KUCOIN"]
            )
        }
        
        class DummyDiscovery:
            active_pairs_map = {
                "XRPUSDT": {
                    "BINANCE": "XRPUSDT",
                    "KUCOIN": "XRPUSDTM"
                }
            }
        
        self.discovery = DummyDiscovery()

async def run_test():
    load_dotenv()
    
    async with aiohttp.ClientSession() as session:
        bot = DummyBot(session)
        
        # Test the leverage setter
        setter = LeverageSetter(bot)
        
        if os.path.exists(setter.cache_path):
            os.remove(setter.cache_path)
            setter._cache = {}
            log("Cache cleared.", level="INFO")
            
        await setter.setup()
        
        log("First run completed.", level="INFO")
        
        # Run again to test caching
        log("Running again to test caching...", level="INFO")
        await setter.setup()

if __name__ == "__main__":
    asyncio.run(run_test())
