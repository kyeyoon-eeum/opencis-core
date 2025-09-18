"""
Compatibility shim: expose `logger` with the old API but backed by fast_logger.
"""

import logging
import os
from typing import Any

from .fast_logger import (
    install_fast_logger,
    set_level as _fast_set_level,
    reopen as _fast_reopen,
    flush as _fast_flush,
    stop as _fast_stop,
)

# Install fast logger on import (stdout by default)
_default_level = os.environ.get("OPENCIS_LOG_LEVEL", "INFO")
install_fast_logger(level=_default_level, path=None, flush_interval_ms=20)

# Add TRACE level compatibility (DEBUG-5)
TRACE = logging.DEBUG - 5
if not hasattr(logging, "TRACE"):
    logging.addLevelName(TRACE, "TRACE")
    setattr(logging, "TRACE", TRACE)


class LoggerShim:
    def __init__(self) -> None:
        self._logger = logging.getLogger("opencis")

    # Core methods
    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.critical(msg, *args, **kwargs)

    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.log(level, msg, *args, **kwargs)

    def isEnabledFor(self, level: int) -> bool:
        return self._logger.isEnabledFor(level)

    # Back-compat API
    def add_log_level(self, level_name: str, level_num: int) -> None:
        method_name = level_name.lower()

        def _log(self_ref: logging.Logger, message, *a, **k):
            self_ref.log(level_num, message, *a, **k)

        logging.addLevelName(level_num, level_name)
        setattr(logging, level_name, level_num)
        setattr(logging.getLoggerClass(), method_name, _log)
        setattr(logging, method_name, _log)

    def set_stdout_levels(
        self,
        loglevel: str = "INFO",
        show_timestamp: bool = False,
        show_loglevel: bool = False,
        show_linenumber: bool = False,
    ) -> None:
        _fast_set_level(loglevel)

    def create_log_file(
        self,
        filename: str,
        loglevel: str = "DEBUG",
        show_timestamp: bool = False,
        show_loglevel: bool = False,
        show_linenumber: bool = False,
    ) -> None:
        install_fast_logger(level=loglevel, path=filename)
        _fast_reopen()

    def hexdump(self, loglevel: str, data: bytes, *args: Any, **kwargs: Any) -> None:
        lvl = getattr(logging, loglevel.upper(), logging.INFO)
        addr = 0
        num_lines = (len(data) // 0x10) + 1
        for _ in range(num_lines):
            d = data[addr : addr + 0x10]
            if not d:
                return
            data_ascii = "".join([chr(b) if (32 < b < 128) else "." for b in d])
            data_bytes = " ".join(f"{i:02x}" for i in d)
            line = f"{addr:08x}:  {data_bytes:47}  |{data_ascii:16}|"
            self._logger.log(lvl, line, *args, **kwargs)
            addr += 0x10

    # Expose utility controls
    def set_level(self, level: str | int) -> None:
        _fast_set_level(level)

    def flush(self) -> None:
        _fast_flush()

    def stop(self) -> None:
        _fast_stop()


logger = LoggerShim()
