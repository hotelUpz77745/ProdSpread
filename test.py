from API.discovery import DiscoveryManager
from CORE.trading_engine import TradingEngine
from CORE.position_manager import PositionManager
from CORE.math_core import OrderbookUtils
import json

engine = TradingEngine(json.load(open("cfg.json")))
book = {
    "bids": [[0.1246, 10000.0]],
    "asks": [[0.1255, 10000.0]]
}
book2 = {
    "bids": [[0.1250, 10000.0]],
    "asks": [[0.12551, 10000.0]]
}
# entry size usd = 100
long_qty = 100.0 / 0.1246
short_qty = 100.0 / 0.12551

is_exit, exit_res = engine.evaluate_exit(
    book, book2, "binance", "bybit",
    long_qty, short_qty,
    0.1246, 0.12551,
    642.0, True, 1.0, 1.0
)
print("Evaluate exit:", is_exit)
print("Exit res:", exit_res)
