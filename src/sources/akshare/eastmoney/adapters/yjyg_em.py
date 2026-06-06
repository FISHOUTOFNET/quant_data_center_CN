"""Adapter for AkShare stock_yjyg_em."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from src.sources.akshare.cninfo.adapters.report_disclosure import report_period_end_date
from src.sources.akshare.core.normalization import select_required_columns, standardize_columns, to_numeric
from src.sources.akshare.core.symbols import normalize_source_code
from src.storage.schema import AKSHARE_YJYG_EM_SCHEMA, field_names

YJYG_EM_FIELD_ALIASES = {
    "code": ("股票代码", "code"),
    "name": ("股票简称", "name"),
    "forecast_indicator": ("预测指标", "forecast_indicator"),
    "performance_change": ("业绩变动", "performance_change"),
    "forecast_value": ("预测数值", "预测数值(元)", "预测数值（元）", "forecast_value"),  # noqa: RUF001
    "performance_change_pct": (
        "业绩变动幅度",
        "业绩变动幅度(%)",
        "业绩变动幅度（%）",  # noqa: RUF001
        "performance_change_pct",
    ),
    "performance_change_reason": ("业绩变动原因", "performance_change_reason"),
    "forecast_type": ("预告类型", "forecast_type"),
    "prior_period_value": (
        "上年同期值",
        "上年同期值(元)",
        "上年同期值（元）",  # noqa: RUF001
        "prior_period_value",
    ),
    "announcement_date": ("公告日期", "announcement_date"),
}

TEXT_COLUMNS = (
    "name",
    "forecast_indicator",
    "performance_change",
    "performance_change_reason",
    "forecast_type",
)


@dataclass(frozen=True)
class YjygEmAdapter:
    period: str
    fetched_at: datetime

    endpoint: str = "stock_yjyg_em"

    @property
    def date(self) -> str:
        return report_period_end_date(self.period).strftime("%Y%m%d")

    @property
    def params(self) -> dict[str, object]:
        return {"date": self.date}

    def call(self, ak_module: Any) -> object:
        try:
            return ak_module.stock_yjyg_em(date=self.date)
        except ValueError as exc:
            if _is_akshare_empty_history_error(exc):
                return pd.DataFrame()
            raise

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        if source_df.empty:
            return pd.DataFrame(columns=field_names(AKSHARE_YJYG_EM_SCHEMA))
        selected = select_required_columns(
            source_df,
            YJYG_EM_FIELD_ALIASES,
            "akshare_cn_stock_yjyg_em",
        )
        selected["code"] = selected["code"].map(normalize_source_code)
        selected["announcement_date"] = pd.to_datetime(
            selected["announcement_date"].replace("", pd.NA),
            errors="coerce",
        ).dt.date
        for column in ["forecast_value", "performance_change_pct", "prior_period_value"]:
            selected[column] = to_numeric(selected[column])
        for column in TEXT_COLUMNS:
            selected[column] = selected[column].astype("string")
        selected.insert(0, "report_period", self.period)
        selected.insert(1, "period_end_date", report_period_end_date(self.period))
        selected["source_endpoint"] = self.endpoint
        selected["fetched_at"] = self.fetched_at
        return selected[field_names(AKSHARE_YJYG_EM_SCHEMA)].drop_duplicates(ignore_index=True).reset_index(drop=True)


def _is_akshare_empty_history_error(exc: ValueError) -> bool:
    message = str(exc).lower()
    return any(token in message for token in ("length mismatch", "empty", "no data", "无数据", "not found"))
