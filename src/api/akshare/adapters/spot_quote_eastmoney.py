"""Adapter for AkShare stock_zh_a_spot_em."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.api.akshare.errors import AkShareEmptyDataError
from src.api.akshare.normalization import select_required_columns, standardize_columns, to_numeric
from src.api.akshare.symbols import clean_source_symbol, normalize_source_code
from src.storage.schema import AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA, field_names

_SOURCE_PREV_CLOSE = "pre" + "close"

AKSHARE_SPOT_QUOTE_EASTMONEY_FIELD_ALIASES = {
    "source_symbol": ("代码", "股票代码", "source_symbol"),
    "name": ("名称", "股票简称", "name"),
    "last_price": ("最新价", "最新价格", "last_price", "latest_price"),
    "price_change": ("涨跌额", "price_change", "change_amount"),
    "pct_change": ("涨跌幅", "pct_change", "pct_chg"),
    "open": ("今开", "开盘", "open"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "prev_close": ("昨收", "prev_close", _SOURCE_PREV_CLOSE),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "turnover_rate": ("换手率", "turnover_rate"),
    "amplitude": ("振幅", "amplitude"),
    "pe_dynamic": ("市盈率-动态", "动态市盈率", "pe_dynamic"),
    "pb": ("市净率", "pb"),
    "total_market_cap": ("总市值", "total_market_cap"),
    "float_market_cap": ("流通市值", "float_market_cap"),
}


@dataclass(frozen=True)
class SpotQuoteEastmoneyAdapter:
    trade_date: str
    fetched_at: datetime

    endpoint: str = "stock_zh_a_spot_em"

    @property
    def params(self) -> dict[str, object]:
        return {"trade_date": self.trade_date}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_zh_a_spot_em()

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        if source_df.empty:
            raise AkShareEmptyDataError("akshare_cn_stock_spot_quote_eastmoney returned empty data")
        selected = select_required_columns(
            source_df,
            AKSHARE_SPOT_QUOTE_EASTMONEY_FIELD_ALIASES,
            "akshare_cn_stock_spot_quote_eastmoney",
        )
        selected["trade_date"] = self.trade_date
        selected["source_symbol"] = selected["source_symbol"].map(clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(normalize_source_code)
        for column in [
            "last_price",
            "price_change",
            "pct_change",
            "open",
            "high",
            "low",
            "prev_close",
            "volume",
            "amount",
            "turnover_rate",
            "amplitude",
            "pe_dynamic",
            "pb",
            "total_market_cap",
            "float_market_cap",
        ]:
            selected[column] = to_numeric(selected[column])
        selected["volume"] = selected["volume"] * 100
        selected["source_endpoint"] = self.endpoint
        selected["fetched_at"] = self.fetched_at
        return selected[field_names(AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA)].reset_index(drop=True)
