"""Logger compatibility layer.

The project declares loguru as a runtime dependency. This fallback keeps tests
and basic commands importable in a partially prepared environment.
"""

from __future__ import annotations

import logging as std_logging
from typing import Any

try:
    from loguru import logger as logger
except ModuleNotFoundError:

    class _FallbackLogger:
        def __init__(self) -> None:
            self._logger = std_logging.getLogger("qdc")
            if not self._logger.handlers:
                handler = std_logging.StreamHandler()
                handler.setFormatter(std_logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
                self._logger.addHandler(handler)
            self._logger.setLevel(std_logging.INFO)

        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._logger.info(self._format(message, *args), **kwargs)

        def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
            self._logger.exception(self._format(message, *args), **kwargs)

        def add(self, *args: Any, **kwargs: Any) -> None:
            return None

        def remove(self, *args: Any, **kwargs: Any) -> None:
            return None

        def _format(self, message: str, *args: Any) -> str:
            if not args:
                return message
            try:
                return message.format(*args)
            except Exception:
                return message

    logger = _FallbackLogger()
