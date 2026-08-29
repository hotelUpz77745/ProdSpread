# ============================================================
# FILE: consts.py
# ROLE: Глобальные константы, загрузка конфигурации и переменных окружения.
# ============================================================
import os
import json

def load_config():
    cfg_path = os.path.join(os.path.dirname(__file__), 'cfg.json')
    with open(cfg_path, 'r', encoding='utf-8') as f:
        return json.load(f)

cfg = load_config()


# General settings
SYMBOLS_GLOBAL_WHITELIST = cfg["SYMBOLS_GLOBAL_WHITELIST"]
QUOTE = cfg["QUOTE"]
MAIN_LOOP_DELAY = cfg["MAIN_LOOP_DELAY"]
EXECUTION_PAUSE = cfg["EXECUTION_PAUSE"]
MAX_RECONNECT_ATTEMPTS = 5
# Strategy settings
TRADING_RULES = cfg["trading_rules"]
TRADING_RISKS_CFG = cfg["trading_risks"]
# Logging settings
LOGGING_CFG = cfg["logging"]
LOG_DEBUG = LOGGING_CFG["debug"]
LOG_INFO = LOGGING_CFG["info"]
LOG_WARNING = LOGGING_CFG["warning"]
LOG_ERROR = LOGGING_CFG["error"]
MAX_LOG_LINES = LOGGING_CFG["max_log_lines"]
LOG_TO_CONSOLE = LOGGING_CFG["log_to_console"]
LOG_TO_FILE = LOGGING_CFG["log_to_file"]
TIME_ZONE = LOGGING_CFG["TIME_ZONE"]

VOLUME_FILTERS = cfg.get("volume_filters", {})
ACTIVE_ROUTES = cfg.get("active_routes", {})
