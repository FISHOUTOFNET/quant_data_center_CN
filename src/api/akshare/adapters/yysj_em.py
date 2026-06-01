"""Adapter for AkShare stock_yysj_em."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.api.akshare.adapters.report_disclosure import report_period_end_date
from src.api.akshare.normalization import select_required_columns, standardize_columns
from src.api.akshare.symbols import normalize_source_code
from src.storage.schema import AKSHARE_YYSJ_EM_SCHEMA, field_names

YYSJ_EM_FIELD_ALIASES = {
    "code": ("股票代码", "code"),
    "name": ("股票简称", "name"),
    "first_scheduled_date": ("首次预约时间", "first_scheduled_date"),
    "first_changed_date": ("一次变更日期", "first_changed_date"),
    "second_changed_date": ("二次变更日期", "second_changed_date"),
    "third_changed_date": ("三次变更日期", "third_changed_date"),
    "actual_disclosure_date": ("实际披露时间", "actual_disclosure_date"),
}


@dataclass(frozen=True)
class YysjEmAdapter:
    symbol: str
    period: str
    fetched_at: datetime

    endpoint: str = "stock_yysj_em"

    @property
    def date(self) -> str:
        return report_period_end_date(self.period).strftime("%Y%m%d")

    @property
    def params(self) -> dict[str, object]:
        return {"symbol": self.symbol, "date": self.date}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_yysj_em(symbol=self.symbol, date=self.date)

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        if source_df.empty:
            return pd.DataFrame(columns=field_names(AKSHARE_YYSJ_EM_SCHEMA))
        selected = select_required_columns(
            source_df,
            YYSJ_EM_FIELD_ALIASES,
            "akshare_cn_stock_yysj_em",
        )
        selected["code"] = selected["code"].map(normalize_source_code)
        for column in [
            "first_scheduled_date",
            "first_changed_date",
            "second_changed_date",
            "third_changed_date",
            "actual_disclosure_date",
        ]:
            selected[column] = pd.to_datetime(selected[column].replace("", pd.NA), errors="coerce").dt.date
        selected.insert(0, "report_period", self.period)
        selected.insert(1, "period_end_date", report_period_end_date(self.period))
        selected.insert(2, "symbol", self.symbol)
        selected["source_endpoint"] = self.endpoint
        selected["fetched_at"] = self.fetched_at
        return selected[field_names(AKSHARE_YYSJ_EM_SCHEMA)].reset_index(drop=True)
