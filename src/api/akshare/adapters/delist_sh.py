"""Adapter for AkShare stock_info_sh_delist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.api.akshare.normalization import select_required_columns, standardize_columns
from src.api.akshare.symbols import clean_source_symbol, normalize_source_code
from src.storage.schema import AKSHARE_DELIST_SH_SCHEMA, field_names

AKSHARE_DELIST_SH_FIELD_ALIASES = {
    "source_symbol": ("公司代码", "证券代码", "代码", "source_symbol"),
    "name": ("公司简称", "证券简称", "名称", "name"),
    "list_date": ("上市日期", "list_date"),
    "delist_date": ("暂停上市日期", "终止上市日期", "delist_date"),
}


@dataclass(frozen=True)
class DelistShAdapter:
    symbol: str
    snapshot_date: str
    fetched_at: datetime

    endpoint: str = "stock_info_sh_delist"

    @property
    def params(self) -> dict[str, object]:
        return {"symbol": self.symbol, "snapshot_date": self.snapshot_date}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_info_sh_delist(symbol=self.symbol)

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        columns = field_names(AKSHARE_DELIST_SH_SCHEMA)
        if source_df.empty:
            return pd.DataFrame(columns=columns)
        selected = select_required_columns(source_df, AKSHARE_DELIST_SH_FIELD_ALIASES, "akshare_cn_stock_delist_sh")
        selected["snapshot_date"] = self.snapshot_date
        selected["exchange"] = "sh"
        selected["market"] = self.symbol
        selected["source_symbol"] = selected["source_symbol"].map(clean_source_symbol)
        selected["code"] = selected["source_symbol"].map(normalize_source_code)
        selected["source_endpoint"] = self.endpoint
        selected["fetched_at"] = self.fetched_at
        return selected[columns].reset_index(drop=True)
