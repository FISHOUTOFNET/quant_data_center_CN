"""Baostock implementation of the provider-neutral market data interface."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from src.api.baostock_client import BaostockClient
from src.api.market_data import DailyKRequest
from src.utils.config_mgr import ConfigManager


class BaostockProvider:
    """MarketDataProvider adapter backed by BaostockClient."""

    name = "baostock"

    def __init__(
        self,
        config: ConfigManager,
        client_factory: Callable[..., BaostockClient] = BaostockClient,
    ) -> None:
        self._config = config
        self._client_factory = client_factory
        self._client: BaostockClient | None = None

    def __enter__(self) -> "BaostockProvider":
        client = self._client_factory(max_attempts=int(self._config.get("pipeline.max_retries", 3)))
        self._client = client.__enter__()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._client is not None:
            self._client.__exit__(exc_type, exc, tb)
            self._client = None

    def query_trade_dates(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._require_client().query_trade_dates(start_date=start_date, end_date=end_date)

    def query_stock_basic(
        self,
        code: str | None = None,
        code_name: str | None = None,
    ) -> pd.DataFrame:
        return self._require_client().query_stock_basic(code=code, code_name=code_name)

    def query_daily_k(self, request: DailyKRequest) -> pd.DataFrame:
        return self._require_client().query_history_k_data_plus(
            code=request.code,
            fields=request.fields,
            start_date=request.start_date,
            end_date=request.end_date,
            frequency=request.frequency,
            adjustflag=self._config.adjustflag_for_dataset(request.dataset),
        )

    def _require_client(self) -> BaostockClient:
        if self._client is None:
            raise RuntimeError("BaostockProvider must be used as a context manager")
        return self._client
