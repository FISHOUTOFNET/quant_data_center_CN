"""Adapter for AkShare stock_report_disclosure."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.api.akshare.normalization import select_required_columns, standardize_columns
from src.api.akshare.symbols import normalize_source_code
from src.storage.schema import AKSHARE_REPORT_DISCLOSURE_SCHEMA, field_names

REPORT_DISCLOSURE_FIELD_ALIASES = {
    "code": ("股票代码", "code"),
    "name": ("股票简称", "name"),
    "first_scheduled_date": ("首次预约", "first_scheduled_date"),
    "first_changed_date": ("初次变更", "first_changed_date"),
    "second_changed_date": ("二次变更", "second_changed_date"),
    "third_changed_date": ("三次变更", "third_changed_date"),
    "actual_disclosure_date": ("实际披露", "actual_disclosure_date"),
}

REPORT_PERIOD_END_DATES = {
    "一季": "03-31",
    "半年报": "06-30",
    "三季": "09-30",
    "年报": "12-31",
}


@dataclass(frozen=True)
class ReportDisclosureAdapter:
    market: str
    period: str
    fetched_at: datetime

    endpoint: str = "stock_report_disclosure"

    @property
    def params(self) -> dict[str, object]:
        return {"market": self.market, "period": self.period}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_report_disclosure(market=self.market, period=self.period)

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        if source_df.empty:
            return pd.DataFrame(columns=field_names(AKSHARE_REPORT_DISCLOSURE_SCHEMA))
        selected = select_required_columns(
            source_df,
            REPORT_DISCLOSURE_FIELD_ALIASES,
            "akshare_cn_stock_report_disclosure",
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
        selected.insert(2, "market", self.market)
        selected["source_endpoint"] = self.endpoint
        selected["fetched_at"] = self.fetched_at
        return selected[field_names(AKSHARE_REPORT_DISCLOSURE_SCHEMA)].reset_index(drop=True)


def report_period_end_date(period: str) -> date:
    value = str(period).strip()
    year = value[:4]
    suffix = value[4:]
    if not year.isdigit() or suffix not in REPORT_PERIOD_END_DATES:
        raise ValueError(f"Unsupported report disclosure period: {period}")
    return datetime.strptime(f"{year}-{REPORT_PERIOD_END_DATES[suffix]}", "%Y-%m-%d").date()
