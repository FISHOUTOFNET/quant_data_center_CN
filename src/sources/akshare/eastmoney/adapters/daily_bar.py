"""Adapter for AkShare stock_zh_a_hist."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.sources.akshare.core.normalization import (
    akshare_date,
    normalize_adjustment,
    select_required_columns,
    standardize_columns,
    to_numeric,
)
from src.sources.akshare.core.symbols import clean_source_symbol, normalize_akshare_code, normalize_source_code
from src.storage.schema import AKSHARE_DAILY_BAR_SCHEMA, field_names

AKSHARE_DAILY_BAR_FIELD_ALIASES = {
    "date": ("日期", "date"),
    "source_symbol": ("股票代码", "代码", "source_symbol"),
    "open": ("开盘", "open"),
    "close": ("收盘", "close"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "amplitude": ("振幅", "amplitude"),
    "pct_change": ("涨跌幅", "pct_change", "pct_chg"),
    "price_change": ("涨跌额", "price_change", "change_amount"),
    "turnover_rate": ("换手率", "turnover_rate"),
}


@dataclass(frozen=True)
class DailyBarAdapter:
    symbol: str
    start_date: str | date
    end_date: str | date
    adjustment: str
    fetched_at: datetime

    endpoint: str = "stock_zh_a_hist"
    stock_code: str = field(init=False)
    normalized_adjustment: str = field(init=False)
    request_start: str = field(init=False)
    request_end: str = field(init=False)
    ak_adjustment: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stock_code", normalize_akshare_code(self.symbol))
        object.__setattr__(self, "normalized_adjustment", normalize_adjustment(self.adjustment))
        object.__setattr__(self, "request_start", akshare_date(self.start_date))
        object.__setattr__(self, "request_end", akshare_date(self.end_date))
        ak_adjustment = "" if self.normalized_adjustment == "unadjusted" else self.normalized_adjustment
        object.__setattr__(self, "ak_adjustment", ak_adjustment)

    @property
    def params(self) -> dict[str, object]:
        return {
            "symbol": self.stock_code,
            "code": self.stock_code,
            "period": "daily",
            "start_date": self.request_start,
            "end_date": self.request_end,
            "adjustment": self.normalized_adjustment,
        }

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_zh_a_hist(
            symbol=self.stock_code,
            period="daily",
            start_date=self.request_start,
            end_date=self.request_end,
            adjust=self.ak_adjustment,
        )

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        columns = field_names(AKSHARE_DAILY_BAR_SCHEMA)
        if source_df.empty:
            return pd.DataFrame(columns=columns)
        selected = select_required_columns(source_df, AKSHARE_DAILY_BAR_FIELD_ALIASES, self.endpoint)
        selected["source_symbol"] = selected["source_symbol"].map(clean_source_symbol)
        selected.loc[selected["source_symbol"].astype("string").str.strip() == "", "source_symbol"] = self.stock_code
        selected["code"] = selected["source_symbol"].map(normalize_source_code)
        selected.loc[selected["code"].astype("string").str.strip() == "", "code"] = self.stock_code
        for column in [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "amplitude",
            "pct_change",
            "price_change",
            "turnover_rate",
        ]:
            selected[column] = to_numeric(selected[column])
        selected["volume"] = (selected["volume"] * 100).round().astype("Int64")
        selected["adjustment"] = self.normalized_adjustment
        selected["source_endpoint"] = self.endpoint
        selected["quality_status"] = "daily_bar_confirmed"
        selected["fetched_at"] = self.fetched_at
        return selected[columns].reset_index(drop=True)
