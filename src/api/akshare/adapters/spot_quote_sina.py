"""Adapter for AkShare stock_zh_a_spot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.api.akshare.errors import AkShareEmptyDataError
from src.api.akshare.normalization import select_required_columns, standardize_columns, to_numeric
from src.api.akshare.symbols import clean_source_symbol, normalize_source_code
from src.storage.schema import AKSHARE_SPOT_QUOTE_SINA_SCHEMA, field_names

_SOURCE_PREV_CLOSE = "pre" + "close"

AKSHARE_SPOT_QUOTE_SINA_FIELD_ALIASES = {
    "source_symbol": ("代码", "股票代码", "source_symbol"),
    "name": ("名称", "股票简称", "name"),
    "last_price": ("最新价", "最新价格", "last_price", "latest_price"),
    "price_change": ("涨跌额", "price_change", "change_amount"),
    "pct_change": ("涨跌幅", "pct_change", "pct_chg"),
    "bid": ("买入", "竞买价", "bid"),
    "ask": ("卖出", "竞卖价", "ask"),
    "prev_close": ("昨收", "prev_close", _SOURCE_PREV_CLOSE),
    "open": ("今开", "开盘", "open"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "source_timestamp": ("时间戳", "时间", "source_timestamp"),
}


@dataclass(frozen=True)
class SpotQuoteSinaAdapter:
    trade_date: str
    fallback_reason: str
    fetched_at: datetime

    endpoint: str = "stock_zh_a_spot"

    @property
    def params(self) -> dict[str, object]:
        return {"trade_date": self.trade_date, "fallback_reason": self.fallback_reason}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_zh_a_spot()

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        if source_df.empty:
            raise AkShareEmptyDataError("stock_zh_a_spot returned empty data")
        selected = select_required_columns(source_df, AKSHARE_SPOT_QUOTE_SINA_FIELD_ALIASES, "stock_zh_a_spot")
        selected["trade_date"] = self.trade_date
        selected["source_symbol"] = selected["source_symbol"].map(clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(normalize_source_code)
        for column in [
            "last_price",
            "price_change",
            "pct_change",
            "bid",
            "ask",
            "prev_close",
            "open",
            "high",
            "low",
            "volume",
            "amount",
        ]:
            selected[column] = to_numeric(selected[column])
        selected["source_timestamp"] = selected["source_timestamp"].astype("string")
        selected["source_endpoint"] = self.endpoint
        selected["is_fallback"] = True
        selected["fallback_reason"] = self.fallback_reason
        selected["fetched_at"] = self.fetched_at
        return selected[field_names(AKSHARE_SPOT_QUOTE_SINA_SCHEMA)].reset_index(drop=True)
