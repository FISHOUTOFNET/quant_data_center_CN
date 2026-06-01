"""Compatibility facade for dataset-specific AkShare ingestion."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.api.akshare.adapters.capital_structure_em import CapitalStructureEmAdapter
from src.api.akshare.adapters.daily_bar import DailyBarAdapter
from src.api.akshare.adapters.delist_sh import DelistShAdapter
from src.api.akshare.adapters.delist_sz import DelistSzAdapter
from src.api.akshare.adapters.report_disclosure import ReportDisclosureAdapter
from src.api.akshare.adapters.spot_quote_eastmoney import SpotQuoteEastmoneyAdapter
from src.api.akshare.adapters.spot_quote_sina import SpotQuoteSinaAdapter
from src.api.akshare.adapters.valuation_eastmoney import ValuationEastmoneyAdapter
from src.api.akshare.adapters.yysj_em import YysjEmAdapter
from src.api.akshare.errors import (
    AkShareCircuitOpen,
    AkShareEmptyDataError,
    AkShareError,
    AkShareNetworkError,
    AkShareSchemaDriftError,
)
from src.api.akshare.models import AkShareResponse
from src.api.akshare.normalization import date_iso
from src.api.akshare.runtime import AkShareRuntime
from src.api.akshare.symbols import normalize_akshare_code
from src.utils.config_mgr import ConfigManager


class AkShareClient:
    """Dataset-specific AkShare facade backed by endpoint adapters."""

    def __init__(
        self,
        config: ConfigManager | None = None,
        ak_module: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._now = now or datetime.now
        self._runtime = AkShareRuntime(
            config=config,
            ak_module=ak_module,
            sleep=sleep,
            random_uniform=random_uniform,
            now=self._now,
        )

    def close(self) -> None:
        self._runtime.close()

    def fetch_stock_valuation(self, code: str) -> AkShareResponse:
        adapter = ValuationEastmoneyAdapter(normalize_akshare_code(code))
        return self._fetch_adapter(adapter)

    def fetch_akshare_cn_stock_delist_sh(
        self,
        symbol: str = "全部",
        snapshot_date: str | date | None = None,
    ) -> AkShareResponse:
        now = self._now()
        adapter = DelistShAdapter(
            symbol=symbol,
            snapshot_date=date_iso(snapshot_date, now.date().isoformat()),
            fetched_at=now,
        )
        return self._fetch_adapter(adapter)

    def fetch_akshare_cn_stock_delist_sz(
        self,
        symbol: str = "终止上市公司",
        snapshot_date: str | date | None = None,
    ) -> AkShareResponse:
        now = self._now()
        adapter = DelistSzAdapter(
            symbol=symbol,
            snapshot_date=date_iso(snapshot_date, now.date().isoformat()),
            fetched_at=now,
        )
        return self._fetch_adapter(adapter)

    def fetch_spot_quote_eastmoney(self, trade_date: str | date | None = None) -> AkShareResponse:
        now = self._now()
        adapter = SpotQuoteEastmoneyAdapter(
            trade_date=date_iso(trade_date, now.date().isoformat()),
            fetched_at=now,
        )
        return self._fetch_adapter(adapter)

    def fetch_spot_quote_sina(
        self,
        trade_date: str | date | None = None,
        fallback_reason: str = "",
    ) -> AkShareResponse:
        now = self._now()
        adapter = SpotQuoteSinaAdapter(
            trade_date=date_iso(trade_date, now.date().isoformat()),
            fallback_reason=fallback_reason,
            fetched_at=now,
        )
        return self._fetch_adapter(adapter)

    def fetch_report_disclosure(self, market: str = "沪深京", period: str | None = None) -> AkShareResponse:
        if period is None:
            raise ValueError("stock_report_disclosure requires period")
        adapter = ReportDisclosureAdapter(
            market=market,
            period=period,
            fetched_at=self._now(),
        )
        return self._fetch_adapter(adapter)

    def fetch_yysj_em(self, symbol: str = "沪深A股", period: str | None = None) -> AkShareResponse:
        if period is None:
            raise ValueError("stock_yysj_em requires period")
        adapter = YysjEmAdapter(
            symbol=symbol,
            period=period,
            fetched_at=self._now(),
        )
        return self._fetch_adapter(adapter)

    def fetch_daily_bars(
        self,
        symbol: str,
        start_date: str | date,
        end_date: str | date,
        adjustment: str,
    ) -> AkShareResponse:
        adapter = DailyBarAdapter(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjustment=adjustment,
            fetched_at=self._now(),
        )
        return self._fetch_adapter(adapter)

    def fetch_capital_structure(self, code: str) -> AkShareResponse:
        adapter = CapitalStructureEmAdapter(symbol=code, fetched_at=self._now())
        return self._fetch_adapter(adapter)

    def _fetch_adapter(self, adapter: Any) -> AkShareResponse:
        return self._runtime.fetch(
            endpoint=adapter.endpoint,
            params=adapter.params,
            caller=lambda: adapter.call(self._runtime.ak()),
            normalizer=adapter.normalize,
        )


def dataframe_hash(df: pd.DataFrame) -> str:
    payload = df.to_json(orient="split", date_format="iso", force_ascii=False, default_handler=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = [
    "AkShareCircuitOpen",
    "AkShareClient",
    "AkShareEmptyDataError",
    "AkShareError",
    "AkShareNetworkError",
    "AkShareResponse",
    "AkShareSchemaDriftError",
    "dataframe_hash",
    "normalize_akshare_code",
]
