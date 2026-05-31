"""Small Baostock wrapper limited to the APIs allowed by the architecture."""

from __future__ import annotations

import contextlib
import socket
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import baostock as bs
import baostock.common.contants as baostock_constants
import baostock.common.context as baostock_context
import baostock.util.socketutil as baostock_socketutil
import pandas as pd
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.pipeline.common import FULL_HISTORY_START_DATE
from src.utils.logging import logger
from src.utils.run_context import pipeline_log_values


class BaostockError(RuntimeError):
    """Raised when Baostock returns a non-zero error code."""


class BaostockTimeoutError(BaostockError):
    """Raised when a Baostock socket operation exceeds the configured timeout."""


class BaostockConnectionError(BaostockError):
    """Raised when the Baostock socket is closed or returns malformed data."""


_BAOSTOCK_MESSAGE_END = b"<![CDATA[]]>\n"
_DEFAULT_SAFE_SEND_TIMEOUT_SECONDS = 60.0


def _safe_baostock_send_msg(msg: str, timeout_seconds: float | None = None) -> str:
    """Send a Baostock protocol message without spinning forever on closed sockets."""

    default_socket = getattr(baostock_context, "default_socket", None)
    if default_socket is None:
        raise BaostockConnectionError("Baostock socket is not connected")

    timeout = _resolve_safe_send_timeout(timeout_seconds)
    _set_baostock_socket_timeout(default_socket, timeout)
    deadline = time.monotonic() + timeout
    try:
        payload = f"{msg}\n".encode()
        if hasattr(default_socket, "sendall"):
            default_socket.sendall(payload)
        else:
            default_socket.send(payload)

        received = b""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BaostockTimeoutError(f"Baostock socket receive timed out after {timeout:g}s")
            _set_baostock_socket_timeout(default_socket, remaining)
            chunk = default_socket.recv(8192)
            if chunk == b"":
                raise BaostockConnectionError("Baostock socket closed connection during receive")
            received += chunk
            if received.endswith(_BAOSTOCK_MESSAGE_END):
                return _decode_baostock_response(received)
    except BaostockError:
        raise
    except TimeoutError as exc:
        raise BaostockTimeoutError(f"Baostock socket receive timed out after {timeout:g}s") from exc
    except OSError as exc:
        raise BaostockConnectionError(f"Baostock socket operation failed: {exc}") from exc


def _resolve_safe_send_timeout(timeout_seconds: float | None) -> float:
    if timeout_seconds is not None:
        return max(float(timeout_seconds), 0.001)
    default_timeout = socket.getdefaulttimeout()
    if default_timeout is not None:
        return max(float(default_timeout), 0.001)
    return _DEFAULT_SAFE_SEND_TIMEOUT_SECONDS


def _set_baostock_socket_timeout(default_socket: Any, timeout_seconds: float) -> None:
    if hasattr(default_socket, "settimeout"):
        default_socket.settimeout(max(float(timeout_seconds), 0.001))


def _decode_baostock_response(received: bytes) -> str:
    try:
        head_bytes = received[0 : baostock_constants.MESSAGE_HEADER_LENGTH]
        head_str = bytes.decode(head_bytes)
        head_arr = head_str.split(baostock_constants.MESSAGE_SPLIT)
        if len(head_arr) >= 3 and head_arr[1] in baostock_constants.COMPRESSED_MESSAGE_TYPE_TUPLE:
            head_inner_length = int(head_arr[2])
            body = received[
                baostock_constants.MESSAGE_HEADER_LENGTH : baostock_constants.MESSAGE_HEADER_LENGTH + head_inner_length
            ]
            return head_str + bytes.decode(zlib.decompress(body))
        return bytes.decode(received)
    except Exception as exc:
        raise BaostockConnectionError("Baostock socket returned malformed response") from exc


def _install_safe_baostock_send_msg() -> None:
    baostock_socketutil.send_msg = _safe_baostock_send_msg


_install_safe_baostock_send_msg()


def _ensure_success(result: Any, action: str) -> None:
    error_code = getattr(result, "error_code", "0")
    error_msg = getattr(result, "error_msg", "")
    if str(error_code) != "0":
        raise BaostockError(f"{action} failed: {error_code} {error_msg}")


def _checked_result(result: Any, action: str) -> Any:
    _ensure_success(result, action)
    return result


def _result_to_dataframe(result: Any, action: str, max_rows_per_result: int) -> pd.DataFrame:
    _ensure_success(result, action)
    rows: list[list[str]] = []
    fields = list(getattr(result, "fields", []))
    while result.next():
        if len(rows) >= max_rows_per_result:
            raise BaostockError(f"{action} exceeded max_rows_per_result={max_rows_per_result}")
        row = result.get_row_data()
        if len(row) != len(fields):
            raise BaostockError(f"{action} returned {len(row)} columns; expected {len(fields)}")
        rows.append(row)
    _ensure_result_reached_terminal_page(result, action)
    _ensure_success(result, action)
    return pd.DataFrame(rows, columns=fields)


def _ensure_result_reached_terminal_page(result: Any, action: str) -> None:
    try:
        data_len = len(getattr(result, "data", []))
        per_page_count = int(getattr(result, "per_page_count", 0) or 0)
        cur_row_num = int(getattr(result, "cur_row_num", 0) or 0)
    except (TypeError, ValueError):
        return
    if per_page_count > 0 and data_len >= per_page_count and cur_row_num >= data_len:
        raise BaostockError(f"{action} stopped after a full page without an explicit terminal page")


@dataclass
class BaostockClient:
    """Context-managed Baostock client returning pandas DataFrames."""

    max_attempts: int = 3
    timeout_seconds: float = 60
    max_rows_per_result: int = 200000
    logged_in: bool = False

    def __enter__(self) -> BaostockClient:
        self.login()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.logout()

    def login(self) -> None:
        if self.logged_in:
            return
        self._call_with_retries("baostock login", lambda: _checked_result(bs.login(), "baostock login"))
        self.logged_in = True
        self._apply_default_socket_timeout()
        run_id, pid, thread = pipeline_log_values()
        logger.info("Baostock login succeeded run_id={} pid={} thread={}", run_id, pid, thread)

    def logout(self) -> None:
        if not self.logged_in:
            return
        bs.logout()
        self.logged_in = False
        run_id, pid, thread = pipeline_log_values()
        logger.info("Baostock logout completed run_id={} pid={} thread={}", run_id, pid, thread)

    def _ensure_logged_in(self) -> None:
        if not self.logged_in:
            self.login()

    def _call_with_retries(self, action: str, operation: Callable[[], Any]) -> Any:
        for attempt in Retrying(
            retry=retry_if_exception_type(BaostockError),
            stop=stop_after_attempt(max(int(self.max_attempts), 1)),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            reraise=True,
        ):
            with attempt:
                if action != "baostock login":
                    self._ensure_logged_in()
                return self._call_once(action, operation)
        raise BaostockError(f"{action} failed without a captured error")

    def _call_once(self, action: str, operation: Callable[[], Any]) -> Any:
        run_id, pid, thread = pipeline_log_values()
        logger.info("Baostock API started run_id={} pid={} thread={} action={}", run_id, pid, thread, action)
        previous_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(float(self.timeout_seconds))
        try:
            self._apply_default_socket_timeout()
            result = operation()
            self._apply_default_socket_timeout()
            run_id, pid, thread = pipeline_log_values()
            logger.info("Baostock API completed run_id={} pid={} thread={} action={}", run_id, pid, thread, action)
            return result
        except BaostockTimeoutError:
            self._invalidate_session()
            raise
        except BaostockConnectionError:
            self._invalidate_session()
            raise
        except TimeoutError as exc:
            self._invalidate_session()
            raise BaostockTimeoutError(f"{action} timed out after {self.timeout_seconds:g}s") from exc
        except OSError as exc:
            self._invalidate_session()
            raise BaostockConnectionError(f"{action} socket failed: {exc}") from exc
        finally:
            socket.setdefaulttimeout(previous_timeout)

    def _apply_default_socket_timeout(self) -> None:
        default_socket = getattr(baostock_context, "default_socket", None)
        if default_socket is not None and hasattr(default_socket, "settimeout"):
            default_socket.settimeout(float(self.timeout_seconds))

    def _invalidate_session(self) -> None:
        default_socket = getattr(baostock_context, "default_socket", None)
        if default_socket is not None and hasattr(default_socket, "close"):
            with contextlib.suppress(Exception):
                default_socket.close()
        baostock_context.default_socket = None
        self.logged_in = False

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjust_flag: str = "3",
    ) -> pd.DataFrame:
        self._ensure_logged_in()
        action = f"query_history_k_data_plus code={code}"
        return self._call_with_retries(
            action,
            lambda: _result_to_dataframe(
                bs.query_history_k_data_plus(
                    code=code,
                    fields=fields,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                    adjustflag=adjust_flag,
                ),
                "query_history_k_data_plus",
                self.max_rows_per_result,
            ),
        )

    def query_baostock_cn_stock_adjustment_factor(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        self._ensure_logged_in()
        action = f"query_adjust_factor code={code}"
        return self._call_with_retries(
            action,
            lambda: _result_to_dataframe(
                bs.query_adjust_factor(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                ),
                "query_adjust_factor",
                self.max_rows_per_result,
            ),
        )

    def query_baostock_cn_stock_basic(self, code: str | None = None, code_name: str | None = None) -> pd.DataFrame:
        self._ensure_logged_in()
        kwargs: dict[str, str] = {}
        if code:
            kwargs["code"] = code
        if code_name:
            kwargs["code_name"] = code_name
        action = "query_stock_basic"
        return self._call_with_retries(
            action,
            lambda: _result_to_dataframe(
                bs.query_stock_basic(**kwargs),
                "query_stock_basic",
                self.max_rows_per_result,
            ),
        )

    def query_trade_dates(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._ensure_logged_in()
        kwargs: dict[str, str] = {"start_date": start_date or FULL_HISTORY_START_DATE}
        if end_date is not None:
            kwargs["end_date"] = end_date
        action = "query_trade_dates"
        return self._call_with_retries(
            action,
            lambda: _result_to_dataframe(
                bs.query_trade_dates(**kwargs),
                "query_trade_dates",
                self.max_rows_per_result,
            ),
        )
