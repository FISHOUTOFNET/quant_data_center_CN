"""Adapter for AkShare stock_value_em."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.sources.akshare.core.normalization import select_required_columns, standardize_columns
from src.storage.schema import AKSHARE_VALUATION_EASTMONEY_SCHEMA, field_names

STOCK_VALUATION_FIELD_ALIASES = {
    "date": ("数据日期", "date"),
    "close": ("当日收盘价", "close"),
    "pct_change": ("当日涨跌幅", "pct_change", "pct_chg"),
    "total_market_cap": ("总市值", "total_market_cap"),
    "float_market_cap": ("流通市值", "float_market_cap"),
    "total_shares": ("总股本", "total_shares"),
    "float_shares": ("流通股本", "float_shares"),
    "pe_ttm": ("PE(TTM)", "pe_ttm"),
    "pe_static": ("PE(静)", "pe_static"),
    "pb": ("市净率", "pb"),
    "peg": ("PEG值", "peg"),
    "pcf": ("市现率", "pcf"),
    "ps": ("市销率", "ps"),
}


@dataclass(frozen=True)
class ValuationEastmoneyAdapter:
    symbol: str

    endpoint: str = "stock_value_em"

    @property
    def params(self) -> dict[str, object]:
        return {"symbol": self.symbol}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_value_em(symbol=self.symbol)

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        if source_df.empty:
            return pd.DataFrame(columns=field_names(AKSHARE_VALUATION_EASTMONEY_SCHEMA))
        selected = select_required_columns(
            source_df, STOCK_VALUATION_FIELD_ALIASES, "akshare_cn_stock_valuation_eastmoney"
        )
        selected.insert(1, "code", self.symbol)
        return selected[field_names(AKSHARE_VALUATION_EASTMONEY_SCHEMA)].reset_index(drop=True)
