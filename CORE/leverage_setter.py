# ============================================================
# FILE: CORE/leverage_setter.py
# ROLE: Bulk leverage and margin type configuration with caching
# ============================================================
import asyncio
import os
import json
from typing import List, Dict, Any, Set
from CORE.utils import log

class LeverageSetter:
    """
    Class for bulk leverage and margin mode configuration for common symbols.
    Caches successful results to CACHE/leverage_cache.json to avoid
    exchange rate limits on restarts.
    """
    def __init__(self, cfg, orders, coin_to_native):
        self.cfg = cfg
        self.orders = orders
        self.coin_to_native = coin_to_native
        self.cache_path = os.path.join("CACHE", "leverage_cache.json")
        self._cache = self._load_cache()
        
    def _load_cache(self) -> Dict:
        if not os.path.exists("CACHE"):
            os.makedirs("CACHE")
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log(f"[LeverageSetter] Error loading cache: {e}", level="WARNING")
        return {}

    def _save_cache(self) -> None:
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=4)
        except Exception as e:
            log(f"[LeverageSetter] Error saving cache: {e}", level="ERROR")

    async def setup(self):
        if not self.cfg["setup_margin_leverage"]:
            log("[LeverageSetter] Leverage/margin setup disabled in config.", level="INFO")
            return

        log("[LeverageSetter] Starting margin and leverage configuration...", level="INFO")
        
        # Collect unique symbols for each active exchange
        symbols_per_exchange: Dict[str, Set[str]] = {
            "BINANCE": set(),
            "KUCOIN": set(),
            "OKX": set(),
            "BITGET": set()
        }
        
        for generic_sym, ex_map in self.coin_to_native.items():
            for ex, native_sym in ex_map.items():
                if self.orders.get(ex) and self.orders[ex].api_key:
                    symbols_per_exchange[ex].add(native_sym)
                    
        new_settings_applied = False
        
        # Dispatch requests per exchange
        for ex_name, symbols in symbols_per_exchange.items():
            if not symbols:
                continue
                
            ex_settings = self.cfg["margin_settings"][ex_name]
            target_leverage = ex_settings["leverage"]
            target_margin = ex_settings["margin_type"]
            order_adapter = self.orders[ex_name]
                
            # Initialize cache for exchange if absent
            if ex_name not in self._cache:
                self._cache[ex_name] = {}
                
            tasks = []
            for sym in symbols:
                cached_data = self._cache[ex_name].get(sym, {})
                
                # Skip if cache already matches target settings
                if cached_data.get("leverage") == target_leverage and cached_data.get("margin_type") == target_margin:
                    continue
                    
                # Add task
                tasks.append(self._apply_settings(order_adapter, ex_name, sym, target_leverage, target_margin))
                
            if tasks:
                log(f"[LeverageSetter] [{ex_name}] Configuring {len(tasks)} symbols (lev: {target_leverage}, type: {target_margin})...", level="INFO")
                # Run in batches to respect rate limits
                batch_size = 10
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i+batch_size]
                    results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, tuple) and result[0]: # (success, symbol)
                            sym = result[1]
                            self._cache[ex_name][sym] = {
                                "leverage": target_leverage,
                                "margin_type": target_margin
                            }
                            new_settings_applied = True
                    await asyncio.sleep(0.5) # Pause between batches
                    
        if new_settings_applied:
            self._save_cache()
            log("[LeverageSetter] New settings saved to cache.", level="INFO")
            
    async def _apply_settings(self, adapter, ex_name: str, sym: str, leverage: int, margin_type: str):
        try:
            res_margin = await adapter.set_margin_type(sym, margin_type, leverage=leverage)
            res_lev = await adapter.set_leverage(sym, leverage, margin_type=margin_type)
            
            if not res_margin or not res_lev:
                return False, sym
            return True, sym
        except Exception as e:
            err = str(e).lower()
            if "no need to change" in err or "margin type cannot be changed" in err:
                return True, sym
            log(f"[LeverageSetter] [{ex_name}] Error configuring {sym}: {e}", level="WARNING")
            return False, sym
