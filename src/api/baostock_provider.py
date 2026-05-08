"""Baostock implementation of the provider-neutral market data interface."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from src.api.baostock_client import BaostockClient
from src.api.market_data import DailyBarRequest
from src.utils.config_mgr import ConfigManager


_SOURCE_PREV_CLOSE = "pre" + "close"
_SOURCE_ADJUST_FLAG = "adjust" + "flag"
_SOURCE_TRADE_STATUS = "trade" + "status"
_SOURCE_PCT_CHANGE = "pct" + "Chg"
_SOURCE_PE_TTM = "pe" + "TTM"
_SOURCE_PB_MRQ = "pb" + "MRQ"
_SOURCE_PS_TTM = "ps" + "TTM"
_SOURCE_PCF_NCF_TTM = "pcf" + "Ncf" + "TTM"
_SOURCE_IS_ST = "is" + "ST"

BAOSTOCK_DAILY_BAR_FIELD_ALIASES = {
    "prev_close": _SOURCE_PREV_CLOSE,
    "adjust_flag": _SOURCE_ADJUST_FLAG,
    "turnover_rate": "turn",
    "trade_status": _SOURCE_TRADE_STATUS,
    "pct_change": _SOURCE_PCT_CHANGE,
    "pe_ttm": _SOURCE_PE_TTM,
    "pb_mrq": _SOURCE_PB_MRQ,
    "ps_ttm": _SOURCE_PS_TTM,
    "pcf_ncf_ttm": _SOURCE_PCF_NCF_TTM,
    "is_st": _SOURCE_IS_ST,
}


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

    def query_baostock_cn_stock_basic(
        self,
        code: str | None = None,
        code_name: str | None = None,
    ) -> pd.DataFrame:
        return self._require_client().query_baostock_cn_stock_basic(code=code, code_name=code_name)

    def query_daily_bars(self, request: DailyBarRequest) -> pd.DataFrame:
        return self._require_client().query_history_k_data_plus(
            code=request.code,
            fields=_baostock_daily_bar_fields(request.fields),
            start_date=request.start_date,
            end_date=request.end_date,
            frequency=request.frequency,
            adjust_flag=self._config.adjust_flag_for_dataset(request.dataset),
        )

    def query_baostock_cn_stock_adjustment_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._require_client().query_baostock_cn_stock_adjustment_factor(
            code=code,
            start_date=start_date,
            end_date=end_date,
        )

    def _require_client(self) -> BaostockClient:
        if self._client is None:
            raise RuntimeError("BaostockProvider must be used as a context manager")
        return self._client


def _baostock_daily_bar_fields(fields: str) -> str:
    source_fields = []
    for field in str(fields).split(","):
        normalized = field.strip()
        if not normalized:
            continue
        source_fields.append(BAOSTOCK_DAILY_BAR_FIELD_ALIASES.get(normalized, normalized))
    return ",".join(source_fields)
