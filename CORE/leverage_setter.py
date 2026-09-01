import asyncio
import os
import json
from typing import List, Dict, Any, Set
from CORE.utils import log

class LeverageSetter:
    """
    Класс для массовой установки плеча и типа маржи для общих символов.
    Кеширует успешные результаты в CACHE/leverage_cache.json для избежания
    лимитов API бирж при перезапусках.
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
                log(f"[LeverageSetter] Ошибка загрузки кэша: {e}", level="WARNING")
        return {}

    def _save_cache(self) -> None:
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=4)
        except Exception as e:
            log(f"[LeverageSetter] Ошибка сохранения кэша: {e}", level="ERROR")

    async def setup(self):
        if not self.cfg["setup_margin_leverage"]:
            log("[LeverageSetter] Настройка плеча/маржи отключена в конфиге.", level="INFO")
            return

        log("[LeverageSetter] Запуск настройки маржи и плечей...", level="INFO")
        
        # Собираем уникальные символы для каждой активной биржи
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
        
        # Отправляем запросы по каждой бирже
        for ex_name, symbols in symbols_per_exchange.items():
            if not symbols:
                continue
                
            ex_settings = self.cfg["margin_settings"][ex_name]
            target_leverage = ex_settings["leverage"]
            target_margin = ex_settings["margin_type"]
            order_adapter = self.orders[ex_name]
                
            # Инициализируем кэш для биржи если нет
            if ex_name not in self._cache:
                self._cache[ex_name] = {}
                
            tasks = []
            for sym in symbols:
                cached_data = self._cache[ex_name].get(sym, {})
                
                # Если в кэше уже есть настройки и они совпадают с таргетом - пропускаем
                if cached_data.get("leverage") == target_leverage and cached_data.get("margin_type") == target_margin:
                    continue
                    
                # Добавляем таску
                tasks.append(self._apply_settings(order_adapter, ex_name, sym, target_leverage, target_margin))
                
            if tasks:
                log(f"[LeverageSetter] [{ex_name}] Настройка {len(tasks)} новых символов (lev: {target_leverage}, type: {target_margin})...", level="INFO")
                # Запускаем батчами, чтобы не словить рейт-лимиты
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
                    await asyncio.sleep(0.5) # Пауза между батчами
                    
        if new_settings_applied:
            self._save_cache()
            log("[LeverageSetter] Новые настройки успешно сохранены в кэш.", level="INFO")
            
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
            log(f"[LeverageSetter] [{ex_name}] Ошибка настройки {sym}: {e}", level="WARNING")
            return False, sym
