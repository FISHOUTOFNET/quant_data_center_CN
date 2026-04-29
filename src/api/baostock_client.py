"""Small Baostock wrapper limited to the APIs allowed by the architecture."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import baostock as bs
import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.pipeline.common import FULL_HISTORY_START_DATE
from src.utils.logging import logger


class BaostockError(RuntimeError):
    """Raised when Baostock returns a non-zero error code."""


def _ensure_success(result: Any, action: str) -> None:
    error_code = getattr(result, "error_code", "0")
    error_msg = getattr(result, "error_msg", "")
    if str(error_code) != "0":
        raise BaostockError(f"{action} failed: {error_code} {error_msg}")


def _result_to_dataframe(result: Any, action: str) -> pd.DataFrame:
    _ensure_success(result, action)
    rows: list[list[str]] = []
    fields = list(getattr(result, "fields", []))
    while result.next():
        rows.append(result.get_row_data())
    _ensure_success(result, action)
    return pd.DataFrame(rows, columns=fields)


@dataclass
class BaostockClient:
    """Context-managed Baostock client returning pandas DataFrames."""

    max_attempts: int = 3
    logged_in: bool = False

    def __enter__(self) -> "BaostockClient":
        self.login()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.logout()

    def login(self) -> None:
        if self.logged_in:
            return
        result = bs.login()
        _ensure_success(result, "baostock login")
        self.logged_in = True
        logger.info("Baostock login succeeded")

    def logout(self) -> None:
        if not self.logged_in:
            return
        bs.logout()
        self.logged_in = False
        logger.info("Baostock logout completed")

    def _ensure_logged_in(self) -> None:
        if not self.logged_in:
            self.login()

    @retry(
        retry=retry_if_exception_type(BaostockError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = "3",
    ) -> pd.DataFrame:
        self._ensure_logged_in()
        result = bs.query_history_k_data_plus(
            code=code,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )
        return _result_to_dataframe(result, "query_history_k_data_plus")

    @retry(
        retry=retry_if_exception_type(BaostockError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def query_stock_basic(self, code: str | None = None, code_name: str | None = None) -> pd.DataFrame:
        self._ensure_logged_in()
        kwargs: dict[str, str] = {}
        if code:
            kwargs["code"] = code
        if code_name:
            kwargs["code_name"] = code_name
        result = bs.query_stock_basic(**kwargs)
        return _result_to_dataframe(result, "query_stock_basic")

    @retry(
        retry=retry_if_exception_type(BaostockError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
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
        result = bs.query_trade_dates(**kwargs)
        return _result_to_dataframe(result, "query_trade_dates")
