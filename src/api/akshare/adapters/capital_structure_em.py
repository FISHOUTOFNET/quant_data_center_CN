"""Adapter for AkShare stock_zh_a_gbjg_em."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from src.api.akshare.normalization import select_required_columns, standardize_columns, to_numeric
from src.api.akshare.symbols import normalize_akshare_code
from src.storage.schema import AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA, field_names

CAPITAL_STRUCTURE_FIELD_ALIASES = {
    "change_date": ("变更日期", "change_date"),
    "total_shares": ("总股本", "total_shares"),
    "restricted_shares": ("流通受限股份", "restricted_shares"),
    "other_domestic_restricted_shares": ("其他内资持股(受限)", "other_domestic_restricted_shares"),
    "domestic_legal_person_restricted_shares": ("境内法人持股(受限)", "domestic_legal_person_restricted_shares"),
    "domestic_natural_person_restricted_shares": ("境内自然人持股(受限)", "domestic_natural_person_restricted_shares"),
    "circulated_shares": ("已流通股份", "circulated_shares"),
    "listed_a_shares": ("已上市流通A股", "listed_a_shares"),
    "change_reason": ("变动原因", "change_reason"),
}

CAPITAL_STRUCTURE_FIELD_ALIASES.update(
    {
        "change_date": (*CAPITAL_STRUCTURE_FIELD_ALIASES["change_date"], "END_DATE"),
        "total_shares": (*CAPITAL_STRUCTURE_FIELD_ALIASES["total_shares"], "TOTAL_SHARES"),
        "restricted_shares": (*CAPITAL_STRUCTURE_FIELD_ALIASES["restricted_shares"], "LIMITED_A_SHARES"),
        "other_domestic_restricted_shares": (
            *CAPITAL_STRUCTURE_FIELD_ALIASES["other_domestic_restricted_shares"],
            "LIMITED_OTHARS",
        ),
        "domestic_legal_person_restricted_shares": (
            *CAPITAL_STRUCTURE_FIELD_ALIASES["domestic_legal_person_restricted_shares"],
            "LIMITED_DOMESTIC_NOSTATE",
        ),
        "domestic_natural_person_restricted_shares": (
            *CAPITAL_STRUCTURE_FIELD_ALIASES["domestic_natural_person_restricted_shares"],
            "LIMITED_DOMESTIC_NATURAL",
        ),
        "circulated_shares": (*CAPITAL_STRUCTURE_FIELD_ALIASES["circulated_shares"], "FREE_SHARES"),
        "listed_a_shares": (*CAPITAL_STRUCTURE_FIELD_ALIASES["listed_a_shares"], "LISTED_A_SHARES"),
        "change_reason": (*CAPITAL_STRUCTURE_FIELD_ALIASES["change_reason"], "CHANGE_REASON"),
    }
)

EASTMONEY_CAPITAL_STRUCTURE_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
EASTMONEY_CAPITAL_STRUCTURE_COLUMNS = (
    "SECUCODE,SECURITY_CODE,END_DATE,TOTAL_SHARES,LIMITED_SHARES,LIMITED_OTHARS,"
    "LIMITED_DOMESTIC_NATURAL,LIMITED_STATE_LEGAL,LIMITED_OVERSEAS_NOSTATE,LIMITED_OVERSEAS_NATURAL,"
    "UNLIMITED_SHARES,LISTED_A_SHARES,B_FREE_SHARE,H_FREE_SHARE,FREE_SHARES,LIMITED_A_SHARES,"
    "NON_FREE_SHARES,LIMITED_B_SHARES,OTHER_FREE_SHARES,LIMITED_STATE_SHARES,"
    "LIMITED_DOMESTIC_NOSTATE,LOCK_SHARES,LIMITED_FOREIGN_SHARES,LIMITED_H_SHARES,"
    "SPONSOR_SHARES,STATE_SPONSOR_SHARES,SPONSOR_SOCIAL_SHARES,RAISE_SHARES,"
    "RAISE_STATE_SHARES,RAISE_DOMESTIC_SHARES,RAISE_OVERSEAS_SHARES,CHANGE_REASON"
)
EASTMONEY_PAGE_SIZE = 500
EASTMONEY_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class CapitalStructureEmAdapter:
    symbol: str
    fetched_at: datetime

    endpoint: str = "stock_zh_a_gbjg_em"
    stock_code: str = field(init=False)
    akshare_symbol: str = field(init=False)

    def __post_init__(self) -> None:
        stock_code = normalize_akshare_code(self.symbol)
        object.__setattr__(self, "stock_code", stock_code)
        object.__setattr__(self, "akshare_symbol", _eastmoney_symbol(stock_code))

    @property
    def params(self) -> dict[str, object]:
        return {"symbol": self.akshare_symbol, "code": self.stock_code}

    def call(self, ak_module: Any) -> object:
        if _use_injected_akshare_method(ak_module):
            return ak_module.stock_zh_a_gbjg_em(symbol=self.akshare_symbol)
        return _fetch_eastmoney_capital_structure(self.akshare_symbol)

    def normalize(self, source_df: pd.DataFrame) -> pd.DataFrame:
        source_df = standardize_columns(source_df)
        columns = field_names(AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA)
        if source_df.empty:
            return pd.DataFrame(columns=columns)
        selected = select_required_columns(source_df, CAPITAL_STRUCTURE_FIELD_ALIASES, self.endpoint)
        selected["change_date"] = pd.to_datetime(selected["change_date"], errors="coerce").dt.date
        selected.insert(1, "code", self.stock_code)
        selected.insert(2, "source_symbol", self.akshare_symbol)
        for column in [
            "total_shares",
            "restricted_shares",
            "other_domestic_restricted_shares",
            "domestic_legal_person_restricted_shares",
            "domestic_natural_person_restricted_shares",
            "circulated_shares",
            "listed_a_shares",
        ]:
            selected[column] = to_numeric(selected[column])
        selected["change_reason"] = selected["change_reason"].astype("string").fillna("")
        selected["source_endpoint"] = self.endpoint
        selected["fetched_at"] = self.fetched_at
        return selected[columns].sort_values(["code", "change_date", "change_reason"]).reset_index(drop=True)


def _eastmoney_symbol(code: str) -> str:
    suffix = "SH" if code.startswith(("5", "6", "9")) else "SZ"
    return f"{code}.{suffix}"


def _use_injected_akshare_method(ak_module: Any) -> bool:
    return getattr(ak_module, "__name__", None) != "akshare" and hasattr(ak_module, "stock_zh_a_gbjg_em")


def _fetch_eastmoney_capital_structure(symbol: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    page_number = 1
    pages = 1
    while page_number <= pages:
        response = requests.get(
            EASTMONEY_CAPITAL_STRUCTURE_URL,
            params=_eastmoney_params(symbol, page_number),
            timeout=EASTMONEY_TIMEOUT_SECONDS,
        )
        raise_for_status = getattr(response, "raise_for_status", None)
        if raise_for_status is not None:
            raise_for_status()
        payload = response.json()
        result = payload.get("result") or {}
        rows.extend(result.get("data") or [])
        pages = max(int(result.get("pages") or 1), 1)
        page_number += 1
    return pd.DataFrame(rows)


def _eastmoney_params(symbol: str, page_number: int) -> dict[str, str]:
    return {
        "reportName": "RPT_F10_EH_EQUITY",
        "columns": EASTMONEY_CAPITAL_STRUCTURE_COLUMNS,
        "quoteColumns": "",
        "filter": f'(SECUCODE="{symbol}")',
        "pageNumber": str(page_number),
        "pageSize": str(EASTMONEY_PAGE_SIZE),
        "sortTypes": "-1",
        "sortColumns": "END_DATE",
        "source": "HSF10",
        "client": "PC",
        "v": "047483522105257925",
    }
