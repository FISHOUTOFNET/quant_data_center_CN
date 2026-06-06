"""Adapter for AkShare stock_financial_report_sina."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from src.sources.akshare.core.normalization import standardize_columns, to_numeric
from src.sources.akshare.core.symbols import normalize_akshare_code
from src.storage.schema import AKSHARE_FINANCIAL_REPORT_SINA_SCHEMA, field_names

REPORT_TYPE_TO_SYMBOL = {
    "balance_sheet": "资产负债表",
    "income_statement": "利润表",
    "cash_flow_statement": "现金流量表",
}
REPORT_SYMBOL_TO_TYPE = {value: key for key, value in REPORT_TYPE_TO_SYMBOL.items()}
METADATA_COLUMNS = {
    "报告日",
    "数据源",
    "是否审计",
    "公告日期",
    "币种",
    "类型",
    "更新日期",
}


@dataclass(frozen=True)
class FinancialReportSinaAdapter:
    symbol: str
    report_type: str
    fetched_at: datetime

    endpoint: str = "stock_financial_report_sina"
    stock_code: str = field(init=False)
    source_symbol: str = field(init=False)
    akshare_symbol: str = field(init=False)

    def __post_init__(self) -> None:
        stock_code = normalize_akshare_code(self.symbol)
        normalized_report_type = _normalize_report_type(self.report_type)
        object.__setattr__(self, "stock_code", stock_code)
        object.__setattr__(self, "source_symbol", _sina_source_symbol(stock_code))
        object.__setattr__(self, "akshare_symbol", REPORT_TYPE_TO_SYMBOL[normalized_report_type])
        object.__setattr__(self, "report_type", normalized_report_type)

    @property
    def params(self) -> dict[str, object]:
        return {"stock": self.source_symbol, "symbol": self.akshare_symbol, "code": self.stock_code}

    def call(self, ak_module: Any) -> object:
        return ak_module.stock_financial_report_sina(stock=self.source_symbol, symbol=self.akshare_symbol)

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        columns = field_names(AKSHARE_FINANCIAL_REPORT_SINA_SCHEMA)
        if source_df.empty:
            return pd.DataFrame(columns=columns)
        if "报告日" not in source_df.columns:
            raise ValueError(f"{self.endpoint} missing required field: 报告日; actual={list(source_df.columns)}")

        rows: list[dict[str, object]] = []
        item_columns = [column for column in source_df.columns if column not in METADATA_COLUMNS]
        numeric_item_columns = {column: to_numeric(source_df[column]) for column in item_columns}
        for row_index, (_, source_row) in enumerate(source_df.iterrows()):
            report_date = _parse_report_date(source_row.get("报告日"))
            metadata = {
                "code": self.stock_code,
                "source_symbol": self.source_symbol,
                "report_type": self.report_type,
                "report_date": report_date,
                "period_end_date": report_date,
                "data_source": _string_value(source_row.get("数据源")),
                "is_audit": _string_value(source_row.get("是否审计")),
                "publish_date": _date_or_none(source_row.get("公告日期")),
                "currency": _string_value(source_row.get("币种")),
                "report_kind": _string_value(source_row.get("类型")),
                "source_update_time": _timestamp_or_none(source_row.get("更新日期")),
                "source_endpoint": self.endpoint,
                "fetched_at": self.fetched_at,
            }
            for item_name in item_columns:
                item_value = source_row.get(item_name)
                numeric_value = pd.NA if pd.isna(item_value) else numeric_item_columns[item_name].iat[row_index]
                rows.append(
                    {
                        **metadata,
                        "item_name": str(item_name).strip(),
                        "item_value": numeric_value,
                        "item_value_text": "" if pd.isna(item_value) else str(item_value),
                    }
                )
        return pd.DataFrame(rows, columns=columns).reset_index(drop=True)


def _normalize_report_type(report_type: str) -> str:
    value = str(report_type).strip()
    if value in REPORT_TYPE_TO_SYMBOL:
        return value
    if value in REPORT_SYMBOL_TO_TYPE:
        return REPORT_SYMBOL_TO_TYPE[value]
    raise ValueError(f"Unsupported stock_financial_report_sina report_type: {report_type}")


def _sina_source_symbol(code: str) -> str:
    if code.startswith(("4", "8")) or code.startswith("920"):
        return f"bj{code}"
    if code.startswith(("5", "6", "9")):
        return f"sh{code}"
    return f"sz{code}"


def _parse_report_date(value: object) -> object:
    text = str(value).strip()
    if pd.isna(text):
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _date_or_none(value: object) -> object:
    text = str(value).strip()
    if pd.isna(text) or text == "":
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _timestamp_or_none(value: object) -> object:
    text = str(value).strip()
    if pd.isna(text) or text == "":
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.floor("ms")


def _string_value(value: object) -> str:
    text = str(value).strip()
    if pd.isna(text):
        return ""
    return text
