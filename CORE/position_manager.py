# ============================================================
# FILE: CORE/position_manager.py
# ROLE: Position manager, semaphore and route lock orchestration.
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
        
        # Limits from config (default 1 if unspecified)
        self.max_pos = {}
        for ex in self.exchanges:
            risk_cfg = self.cfg["trading_risks"][ex.lower()]
            self.max_pos[ex] = risk_cfg["max_positions"]
            
        # State per exchange: active and pending counts
        self.exchange_state = {ex: {"current": 0, "pending": 0} for ex in self.exchanges}
        
        # State per route: locked == True means route entry is barred
        self.route_state = {route: {"is_locked": False} for route in self.route_names}
        
        # Position state: route -> symbol -> state
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
                        # Protection against stale states - reset pending_action if crashed
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
        Recalculates lock states for all routes based on exchange utilization.
        A route is locked if either exchange reached max_positions limit (current + pending >= max).
        """
        for route in self.route_names:
            ex1, ex2 = route.split('_')
            
            ex1_used = self.exchange_state[ex1]["current"] + self.exchange_state[ex1]["pending"]
            ex2_used = self.exchange_state[ex2]["current"] + self.exchange_state[ex2]["pending"]
            
            if ex1_used >= self.max_pos[ex1] or ex2_used >= self.max_pos[ex2]:
                self.route_state[route]["is_locked"] = True
            else:
                self.route_state[route]["is_locked"] = False
                
    def _normalize_route(self, long_ex: str, short_ex: str) -> str:
        """Returns the canonical route name from route_state, checking both directions."""
        route = f"{long_ex}_{short_ex}"
        if route in self.route_state:
            return route
        rev = f"{short_ex}_{long_ex}"
        if rev in self.route_state:
            return rev
        return route  # fallback to original (will fail downstream checks)

    def can_enter(self, long_ex: str, short_ex: str, sym: str) -> bool:
        route = self._normalize_route(long_ex, short_ex)
        if route not in self.route_state:
            return False
            
        # Check if route is locked
        if self.route_state[route]["is_locked"]:
            return False
            
        # Strict exchange limit check: current + pending must not exceed max_positions
        ex1_used = self.exchange_state[long_ex]["current"] + self.exchange_state[long_ex]["pending"]
        ex2_used = self.exchange_state[short_ex]["current"] + self.exchange_state[short_ex]["pending"]
        if ex1_used >= self.max_pos[long_ex] or ex2_used >= self.max_pos[short_ex]:
            return False

        # Global symbol check across ALL routes:
        # coin must not be open on any route, and have no pending actions
        for r in self.route_names:
            st = self.positions[r].get(sym)
            if st and (st["current_position"] or st["pending_action"] is not None):
                return False
                
        return True
        
    def lock_for_entry(self, long_ex: str, short_ex: str, sym: str, engine_res: dict):
        route = self._normalize_route(long_ex, short_ex)
        self.positions[route][sym]["pending_action"] = "OPEN"
        self.positions[route][sym]["details"] = {"engine_res": engine_res}
        
        self.exchange_state[long_ex]["pending"] += 1
        self.exchange_state[short_ex]["pending"] += 1
        
        self._update_locks()
        
    def confirm_entry(self, long_ex: str, short_ex: str, sym: str, exec_res: dict, open_time: float):
        route = self._normalize_route(long_ex, short_ex)
        state = self.positions[route][sym]
        
        if state["pending_action"] != "OPEN":
            return
            
        state["current_position"] = True
        state["pending_action"] = None
        state["details"].update({
            "long_ex": long_ex,
            "short_ex": short_ex,
            "entry_long_price": exec_res.get("actual_long_price", 0.0) or exec_res.get("entry_long_price", 0.0),
            "entry_short_price": exec_res.get("actual_short_price", 0.0) or exec_res.get("entry_short_price", 0.0),
            "long_executed_volume_rate": exec_res.get("long_executed_volume_rate", 1.0),
            "short_executed_volume_rate": exec_res.get("short_executed_volume_rate", 1.0),
            "actual_gross_spread": exec_res.get("actual_gross_spread"),
            "actual_net_spread": exec_res.get("actual_net_spread"),
            "use_extreme_decay": exec_res.get("use_extreme_decay", False),
            "open_time": open_time
        })
        
        self.exchange_state[long_ex]["pending"] -= 1
        self.exchange_state[short_ex]["pending"] -= 1
        
        self.exchange_state[long_ex]["current"] += 1
        self.exchange_state[short_ex]["current"] += 1
        
        self._update_locks()
        self._save_state()
        
    def rollback_entry(self, long_ex: str, short_ex: str, sym: str):
        route = self._normalize_route(long_ex, short_ex)
        state = self.positions[route][sym]
        
        if state["pending_action"] == "OPEN":
            state["pending_action"] = None
            state["details"] = {}
            
            self.exchange_state[long_ex]["pending"] = max(0, self.exchange_state[long_ex]["pending"] - 1)
            self.exchange_state[short_ex]["pending"] = max(0, self.exchange_state[short_ex]["pending"] - 1)
            
            self._update_locks()
            self._save_state()

    def lock_for_exit(self, route: str, sym: str):
        self.positions[route][sym]["pending_action"] = "CLOSE"
        
    def confirm_exit(self, route: str, sym: str):
        state = self.positions[route].get(sym)
        if not state:
            return
            
        # If position was still in OPEN stage (emergency leg unwind during entry)
        if state["pending_action"] == "OPEN":
            long_ex, short_ex = route.split('_')
            self.rollback_entry(long_ex, short_ex, sym)
            return

        if state["pending_action"] != "CLOSE":
            return
            
        long_ex = state["details"].get("long_ex")
        short_ex = state["details"].get("short_ex")
        if not long_ex or not short_ex:
            long_ex, short_ex = route.split('_')
        
        state["current_position"] = False
        state["pending_action"] = None
        state["details"] = {}
        
        self.exchange_state[long_ex]["current"] = max(0, self.exchange_state[long_ex]["current"] - 1)
        self.exchange_state[short_ex]["current"] = max(0, self.exchange_state[short_ex]["current"] - 1)
        
        self._update_locks()
        self._save_state()
        
    def rollback_exit(self, route: str, sym: str):
        state = self.positions[route][sym]
        if state["pending_action"] == "CLOSE":
            state["pending_action"] = None

    def get_open_positions(self):
        """
        Returns list of all positions (route, sym, state)
        currently active and not in exit process.
        """
        res = []
        for route, sym_map in self.positions.items():
            for sym, state in sym_map.items():
                if state["current_position"] and state["pending_action"] is None:
                    res.append((route, sym, state))
        return res
