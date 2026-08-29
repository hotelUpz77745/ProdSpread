# ============================================================
# FILE: c_log.py
# ROLE: Система логирования (кастомный логгер).
# ============================================================

from __future__ import annotations

import inspect
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from functools import wraps
from logging.handlers import RotatingFileHandler
from pprint import pformat 
from typing import Any, Optional

import pytz

from consts import (
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARNING,
    LOG_ERROR,
    LOG_TO_CONSOLE,
    LOG_TO_FILE,
    MAX_LOG_LINES,
    TIME_ZONE,
)


# ============================================================
# TIME
# ============================================================

def log_time() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# HELPERS
# ============================================================

def estimate_average_line_length(path: str, sample: int = 200) -> int:
    if not os.path.exists(path):
        return 300
    try:
        lines = []
        with open(path, "r", encoding="utf-8") as f:
            for _ in range(sample):
                line = next(f, None)
                if line is None:
                    break
                lines.append(len(line))
        return sum(lines) // len(lines) if lines else 300
    except Exception:
        return 300


def calc_max_bytes(avg_len: int, lines: int) -> int:
    return avg_len * lines


# ============================================================
# HANDLERS
# ============================================================

class UnlockedRotatingFileHandler(RotatingFileHandler):
    """
    Кастомный RotatingFileHandler, который не держит файл постоянно открытым.
    Каждая запись (emit) открывает, пишет и закрывает файл.
    Это снимает жесткую блокировку Windows, позволяя удалять/переименовывать логи.
    """

    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay=True)
        self.stream = None  # Принудительно отключаем постоянный стрим

    def _open(self):
        return None

    def emit(self, record):
        try:
            if self.shouldRollover(record):
                self.doRollover()

            msg = self.format(record)
            os.makedirs(os.path.dirname(self.baseFilename), exist_ok=True)
            with open(self.baseFilename, self.mode, encoding=self.encoding) as f:
                f.write(msg + self.terminator)
        except Exception:
            self.handleError(record)

    def shouldRollover(self, record):
        if self.maxBytes > 0:
            msg = "%s\n" % self.format(record)
            try:
                size = os.path.getsize(self.baseFilename)
            except OSError:
                size = 0
            if size + len(msg.encode(self.encoding or 'utf-8')) >= self.maxBytes:
                return 1
        return 0


# ============================================================
# UNIFIED LOGGER
# ============================================================

class UnifiedLogger:
    """
    Универсальный логгер:
    - logging + RotatingFileHandler
    - decorator для методов
    - совместим с async / sync
    """

    def __init__(
        self,
        name: str,
        log_dir: str = "./logs",
        max_lines: int = MAX_LOG_LINES,
        context: Optional[dict] = None,
    ):
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{name}.log")

        avg_len = estimate_average_line_length(log_path)
        max_bytes = calc_max_bytes(avg_len, max_lines)

        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # Не всплывать в root

        # Handler добавляем ТОЛЬКО если его ещё нет
        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(context)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            if LOG_TO_FILE:
                # Общий лог-файл для всех модулей
                all_log_path = os.path.join(log_dir, "all.log")
                all_handler = UnlockedRotatingFileHandler(
                    all_log_path,
                    maxBytes=max_bytes * 5,  # В 5 раз больше, так как он общий
                    backupCount=1,
                    encoding="utf-8",
                )
                all_handler.setFormatter(formatter)
                logger.addHandler(all_handler)

            if LOG_TO_CONSOLE:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)

        self._logger = logging.LoggerAdapter(
            logger,
            extra={"context": context or name},
        )
        self._last_log_times = {}

    def _should_throttle(self, msg: str, throttle_sec: int, throttle_key: Optional[str] = None) -> bool:
        if throttle_sec <= 0:
            return False
        current_time = time.time()
        key = throttle_key if throttle_key is not None else msg
        last_time = self._last_log_times.get(key, 0)
        if current_time - last_time < throttle_sec:
            return True
        self._last_log_times[key] = current_time
        return False

    def debug(self, msg: str, *args, throttle_sec: int = 0, throttle_key: Optional[str] = None, **kwargs):
        if LOG_DEBUG and not self._should_throttle(msg, throttle_sec, throttle_key):
            self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, throttle_sec: int = 0, throttle_key: Optional[str] = None, **kwargs):
        if LOG_INFO and not self._should_throttle(msg, throttle_sec, throttle_key):
            self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, throttle_sec: int = 0, throttle_key: Optional[str] = None, **kwargs):
        if LOG_WARNING and not self._should_throttle(msg, throttle_sec, throttle_key):
            self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, throttle_sec: int = 0, throttle_key: Optional[str] = None, **kwargs):
        if LOG_ERROR and not self._should_throttle(msg, throttle_sec, throttle_key):
            self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, throttle_sec: int = 0, throttle_key: Optional[str] = None, exc: Exception = None, **kwargs):
        if LOG_ERROR and not self._should_throttle(msg, throttle_sec, throttle_key):
            self._logger.exception(msg, *args, **kwargs)

    # ======================================================
    # DECORATOR
    # ======================================================

    def total_exception_decor(self, func, context: Optional[Any] = None):
        """
        Ловит ВСЕ исключения, логирует контекст,
        НЕ крашит приложение.
        """
        if getattr(func, "_is_wrapped", False):
            return func

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as ex:
                self._log_exception(func, ex, args, kwargs, context)
                return None

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                self._log_exception(func, ex, args, kwargs, context)
                return None

        wrapper = (
            async_wrapper
            if inspect.iscoroutinefunction(func)
            else sync_wrapper
        )
        wrapper._is_wrapped = True
        return wrapper

    def _log_exception(self, func, ex, args, kwargs, context: Optional[Any] = None):
        extra = {}
        if context is not None:
            extra["context"] = context

        self._logger.error(
            f"[EXCEPTION] {func.__qualname__} -> {ex}\n"
            f"Args:\n{pformat({'args': args, 'kwargs': kwargs})}\n"
            f"Stack:\n{traceback.format_exc()}",
            extra=extra or None,
        )

    # ======================================================
    # MASS WRAP
    # ======================================================

    def wrap_object_methods(self, obj: Any, context: Optional[Any] = None):
        for cls in obj.__class__.mro():
            if cls is object:
                continue

            for name, attr in cls.__dict__.items():
                if name.startswith("_"):
                    continue

                if name.startswith("__"):
                    continue

                if not callable(attr):
                    continue

                try:
                    original = getattr(obj, name)
                    if getattr(original, "_is_wrapped", False):
                        continue

                    wrapped = self.total_exception_decor(original, context)
                    setattr(obj, name, wrapped)
                except Exception:
                    continue

from consts import SYMBOLS_GLOBAL_WHITELIST
import concurrent.futures

# Глобальный экземпляр
_global_logger = UnifiedLogger("SYSTEM")
_log_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

def log(msg: str, level: str = "DEBUG", *args, throttle_sec: int = 0, throttle_key: Optional[str] = None, exc: Exception = None, is_signal: bool = False, **kwargs):
    def _do_log():
        lvl = level.upper()
        if lvl == "INFO":
            _global_logger.info(msg, *args, throttle_sec=throttle_sec, throttle_key=throttle_key, **kwargs)
        elif lvl == "WARNING":
            _global_logger.warning(msg, *args, throttle_sec=throttle_sec, throttle_key=throttle_key, **kwargs)
        elif lvl == "ERROR":
            if exc:
                _global_logger.exception(msg, *args, throttle_sec=throttle_sec, throttle_key=throttle_key, exc=exc, **kwargs)
            else:
                _global_logger.error(msg, *args, throttle_sec=throttle_sec, throttle_key=throttle_key, **kwargs)
        else:
            _global_logger.debug(msg, *args, throttle_sec=throttle_sec, throttle_key=throttle_key, **kwargs)
    
        if is_signal:
            try:
                timestamp = datetime.now(timezone.utc).astimezone(pytz.timezone(TIME_ZONE)).strftime("%Y-%m-%d %H:%M:%S")
                sym = SYMBOLS_GLOBAL_WHITELIST[0] if SYMBOLS_GLOBAL_WHITELIST else "GLOBAL"
                with open(os.path.join("logs", f"signals_{sym}.log"), "a", encoding="utf-8") as f:
                    f.write(f"{timestamp} | INFO | {sym} | {msg}\n")
            except Exception:
                pass

    _log_executor.submit(_do_log)
