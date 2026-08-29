# ============================================================
# FILE: API/discovery.py
# ROLE: Orchestrates REST API volume fetching and route discovery.
# ============================================================

import asyncio
from typing import Dict, List, Set, Tuple
from itertools import combinations
from consts import VOLUME_FILTERS, ACTIVE_ROUTES

from API.BINANCE.symbol import BinanceSymbols
from API.KUCOIN.symbol import KucoinSymbols
from API.OKX.symbol import OkxSymbols
from API.BITGET.symbol import BitgetSymbols

from utils import UnifiedLogger

logger = UnifiedLogger("DISCOVERY")

_REVERSE_ALIASES = {"BTC": "XBT"}  # Kucoin specific

def to_native(coin: str, exchange: str, quote: str = "USDT") -> str:
    """Converts a normalized base coin to the exchange's native WS ticker format."""
    if exchange == "KUCOIN":
        raw = _REVERSE_ALIASES.get(coin, coin)
        return f"{raw}{quote}M"
    elif exchange == "OKX":
        return f"{coin}-{quote}-SWAP"
    else:  # BINANCE, BITGET
        return f"{coin}{quote}"

class DiscoveryManager:
    def __init__(self, timeout_sec: float = 20.0, quote: str = "USDT"):
        self.quote = quote
        self.apis = {
            "BINANCE": BinanceSymbols(timeout_sec=timeout_sec),
            "KUCOIN": KucoinSymbols(timeout_sec=timeout_sec),
            "OKX": OkxSymbols(timeout_sec=timeout_sec),
            "BITGET": BitgetSymbols(timeout_sec=timeout_sec)
        }
        
        self.ws_routes: Dict[str, List[str]] = {}           
        self.active_pairs_map: Dict[str, Set[str]] = {}    
        self.coin_to_native: Dict[str, Dict[str, str]] = {}      

    async def aclose(self):
        for name, api in self.apis.items():
            try:
                await api.aclose()
            except Exception as e:
                logger.error(f"Error closing {name} REST client: {e}")

    async def build_topology(self, banned_symbols: dict = None) -> None:
        """
        1. REST Aggregation
        2. Volume Filter
        3. Inverted Index (active_pairs_map)
        4. Route Validation
        5. Routing (ws_routes & coin_to_native)
        """
        # Step 1 & 2: Fetch and filter
        tasks = []
        names = []
        
        # Only fetch exchanges that are part of at least one active route
        needed_exchanges = set()
        for route, is_active in ACTIVE_ROUTES.items():
            if is_active:
                parts = route.split("_")
                if len(parts) == 2:
                    needed_exchanges.update(parts)
                    
        for name, api in self.apis.items():
            if name in needed_exchanges:
                names.append(name)
                tasks.append(api.get_volumes(quote=self.quote))
            else:
                logger.info(f"Skipping {name} (no active routes)")
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_sets = {}
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch volumes for {name}: {result}")
                all_sets[name] = set()
                continue
            
            min_vol = VOLUME_FILTERS.get(name, 0.0)
            valid_coins = {c for c, v in result.items() if v >= min_vol}
            all_sets[name] = valid_coins
            logger.info(f"{name}: {len(result)} total, {len(valid_coins)} passed volume filter >= {min_vol}")

        # Step 3: Inverted Index
        raw_pairs_map = {}
        for ex_name, filtered_set in all_sets.items():
            for coin in filtered_set:
                raw_pairs_map.setdefault(coin, set()).add(ex_name)
                
        # Step 4: Route Validation
        self.active_pairs_map = {}
        for coin, exchanges in raw_pairs_map.items():
            if banned_symbols and coin in banned_symbols:
                continue
                
            valid_coin_exchanges = set()
            # Generate all pairs this coin appears on
            for ex1, ex2 in combinations(sorted(list(exchanges)), 2):
                route1 = f"{ex1}_{ex2}"
                route2 = f"{ex2}_{ex1}"
                
                if ACTIVE_ROUTES.get(route1) or ACTIVE_ROUTES.get(route2):
                    valid_coin_exchanges.add(ex1)
                    valid_coin_exchanges.add(ex2)
                    
            if valid_coin_exchanges:
                self.active_pairs_map[coin] = valid_coin_exchanges
                
        # Step 5: Routing (ws_routes & coin_to_native)
        self.ws_routes = {ex: [] for ex in needed_exchanges}
        self.coin_to_native = {}
        
        for coin, valid_exchanges in self.active_pairs_map.items():
            self.coin_to_native[coin] = {}
            for ex in valid_exchanges:
                native_ticker = to_native(coin, ex, self.quote)
                self.ws_routes[ex].append(native_ticker)
                self.coin_to_native[coin][ex] = native_ticker

        logger.info(f"Topology built: {len(self.active_pairs_map)} valid coins across {len(needed_exchanges)} exchanges.")


# ----------------------------
# SELF TEST
# ----------------------------
if __name__ == "__main__":
    async def _test():
        from utils import SessionManager
        dm = DiscoveryManager()
        await dm.build_topology()
        
        print("\n=== WS ROUTES ===")
        for ex, routes in dm.ws_routes.items():
            if routes:
                print(f"{ex}: {len(routes)} coins (e.g. {routes[:3]})")
            
        print(f"\nTotal active coins: {len(dm.active_pairs_map)}")
        
        await dm.aclose()
        await SessionManager().close_all()
        
    asyncio.run(_test())
