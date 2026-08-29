# ============================================================
# FILE: utils.py
# ROLE: Вспомогательные утилиты, асинхронные таймеры.
# ============================================================
from datetime import datetime
import pytz
import time
from c_log import log
from consts import MAX_RECONNECT_ATTEMPTS, TIME_ZONE
from CORE.math_core import is_stale_jit


class Utils:
    def __init__(self):  
        self.tz_location = pytz.timezone(TIME_ZONE)

    @staticmethod
    def is_stale(binance_ts: float, kucoin_ts: float, timeout: float = 5.0) -> bool:
        """Checks if the orderbook data timestamps are stale (optimized via JIT)."""
        return is_stale_jit(float(binance_ts or 0.0), float(kucoin_ts or 0.0), time.time(), timeout)

    @staticmethod
    def check_phantom_spread(b_local: float, k_local: float, max_desync_ms: float) -> bool:
        """
        Protection against phantom spreads: verifies the local arrival time of websocket packets.
        Returns True if desync is within the allowed limit, otherwise False.
        """
        if b_local <= 0 or k_local <= 0:
            return False
        diff_ms = abs(b_local - k_local) * 1000.0
        return diff_ms <= max_desync_ms

    def get_date_time_now(self) -> str:
        now = datetime.now(self.tz_location)
        return now.strftime("%Y-%m-%d %H:%M:%S")
        
    @staticmethod
    def has_open_position(response: dict, symbol: str, side: str) -> bool:
        try:           
            if not response.get("success"):
                return False

            for pos in response.get("positions", []):
                if pos["symbol"] == symbol and pos["side"] == side.upper():
                    return True
        except Exception as e:
            log(f"Error parsing positions: {e}", level="ERROR")
        return False
