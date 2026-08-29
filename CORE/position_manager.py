# ============================================================
# FILE: CORE/position_manager.py
# ROLE: Менеджер позиций, управление семафорами и блокировками связок.
# ============================================================
# ============================================================
import json
import os

class PositionManager:
    STATE_FILE = "active_positions.json"
    def __init__(self, cfg: dict, exchanges: list, route_names: list, active_symbols: list):
        self.cfg = cfg
        self.exchanges = exchanges
        self.route_names = route_names
        
        # Лимиты из конфига (default 1 если не указано)
        self.max_pos = {}
        for ex in self.exchanges:
            risk_cfg = self.cfg.get("trading_risks", {}).get(ex.lower(), {})
            self.max_pos[ex] = risk_cfg.get("max_positions", 1)
            
        # Состояние по биржам: сколько активно и сколько в ожидании (pending)
        self.exchange_state = {ex: {"current": 0, "pending": 0} for ex in self.exchanges}
        
        # Состояние по связкам: locked == True означает, что на эту связку нельзя входить
        self.route_state = {route: {"is_locked": False} for route in self.route_names}
        
        # Состояние позиций: route -> symbol -> state
        self.positions = {route: {} for route in self.route_names}
        for route in self.route_names:
            for sym in active_symbols:
                self.positions[route][sym] = {"current_position": False, "pending_action": None, "details": {}}
                
        self._load_state()

    def _load_state(self):
        if not os.path.exists(self.STATE_FILE):
            return
            
        try:
            with open(self.STATE_FILE, "r", encoding="utf-8") as f:
                saved_positions = json.load(f)
                
            for route, sym_map in saved_positions.items():
                if route not in self.positions:
                    continue
                for sym, state in sym_map.items():
                    if sym in self.positions[route] and state.get("current_position"):
                        # Защита от старых/кривых стейтов - сбрасываем pending_action, если был крэш
                        state["pending_action"] = None
                        self.positions[route][sym] = state
                        
                        long_ex = state.get("details", {}).get("long_ex")
                        short_ex = state.get("details", {}).get("short_ex")
                        if long_ex and short_ex:
                            self.exchange_state[long_ex]["current"] += 1
                            self.exchange_state[short_ex]["current"] += 1
                            
            self._update_locks()
        except Exception as e:
            print(f"Error loading positions state: {e}")

    def _save_state(self):
        try:
            with open(self.STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.positions, f, indent=4)
        except Exception as e:
            print(f"Error saving positions state: {e}")

    def _update_locks(self):
        """
        Пересчитывает состояние замков для всех связок на основе занятости бирж.
        Связка блокируется, если любая из её бирж достигла лимита max_positions (current + pending >= max).
        """
        for route in self.route_names:
            ex1, ex2 = route.split('_')
            
            ex1_used = self.exchange_state[ex1]["current"] + self.exchange_state[ex1]["pending"]
            ex2_used = self.exchange_state[ex2]["current"] + self.exchange_state[ex2]["pending"]
            
            if ex1_used >= self.max_pos[ex1] or ex2_used >= self.max_pos[ex2]:
                self.route_state[route]["is_locked"] = True
            else:
                self.route_state[route]["is_locked"] = False
                
    def can_enter(self, long_ex: str, short_ex: str, sym: str) -> bool:
        route = f"{long_ex}_{short_ex}"
        if route not in self.route_state:
            return False
            
        # Проверяем, не заблокирована ли связка
        if self.route_state[route]["is_locked"]:
            return False
            
        # Проверяем, нет ли уже открытой позиции по этой монете на этой связке
        state = self.positions[route].get(sym)
        if not state:
            return False
            
        if state["current_position"] or state["pending_action"] is not None:
            return False
            
        return True
        
    def lock_for_entry(self, long_ex: str, short_ex: str, sym: str, engine_res: dict):
        route = f"{long_ex}_{short_ex}"
        self.positions[route][sym]["pending_action"] = "OPEN"
        self.positions[route][sym]["details"] = {"engine_res": engine_res}
        
        self.exchange_state[long_ex]["pending"] += 1
        self.exchange_state[short_ex]["pending"] += 1
        
        self._update_locks()
        
    def confirm_entry(self, long_ex: str, short_ex: str, sym: str, exec_res: dict, open_time: float):
        route = f"{long_ex}_{short_ex}"
        state = self.positions[route][sym]
        
        if state["pending_action"] != "OPEN":
            return
            
        state["current_position"] = True
        state["pending_action"] = None
        state["details"].update({
            "long_ex": long_ex,
            "short_ex": short_ex,
            "entry_long_price": exec_res["actual_long_price"],
            "entry_short_price": exec_res["actual_short_price"],
            "long_executed_volume_rate": exec_res.get("long_executed_volume_rate", 1.0),
            "short_executed_volume_rate": exec_res.get("short_executed_volume_rate", 1.0),
            "open_time": open_time
        })
        
        self.exchange_state[long_ex]["pending"] -= 1
        self.exchange_state[short_ex]["pending"] -= 1
        
        self.exchange_state[long_ex]["current"] += 1
        self.exchange_state[short_ex]["current"] += 1
        
        self._update_locks()
        self._save_state()
        
    def rollback_entry(self, long_ex: str, short_ex: str, sym: str):
        route = f"{long_ex}_{short_ex}"
        state = self.positions[route][sym]
        
        if state["pending_action"] == "OPEN":
            state["pending_action"] = None
            state["details"] = {}
            
            self.exchange_state[long_ex]["pending"] -= 1
            self.exchange_state[short_ex]["pending"] -= 1
            
            self._update_locks()

    def lock_for_exit(self, route: str, sym: str):
        self.positions[route][sym]["pending_action"] = "CLOSE"
        
    def confirm_exit(self, route: str, sym: str):
        state = self.positions[route][sym]
        if state["pending_action"] != "CLOSE":
            return
            
        long_ex = state["details"]["long_ex"]
        short_ex = state["details"]["short_ex"]
        
        state["current_position"] = False
        state["pending_action"] = None
        state["details"] = {}
        
        self.exchange_state[long_ex]["current"] -= 1
        self.exchange_state[short_ex]["current"] -= 1
        
        self._update_locks()
        self._save_state()
        
    def rollback_exit(self, route: str, sym: str):
        state = self.positions[route][sym]
        if state["pending_action"] == "CLOSE":
            state["pending_action"] = None

    def get_open_positions(self):
        """
        Возвращает список всех позиций (route, sym, state), 
        которые сейчас открыты и не в процессе выхода.
        """
        res = []
        for route, sym_map in self.positions.items():
            for sym, state in sym_map.items():
                if state["current_position"] and state["pending_action"] is None:
                    res.append((route, sym, state))
        return res
