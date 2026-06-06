"""Build the partitioned canonical stock valuation dataset."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.sources.derived.common import (
    read_partition_or_empty,
    refresh_derived_registry,
    safe_remove_derived_dataset_dir,
)
from src.sources.derived.security_master import build_security_master
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore

AKSHARE_VALUATION_DATASET = "akshare_cn_stock_valuation_eastmoney"
BAOSTOCK_PERCENTILE_DATASET = "baostock_cn_stock_valuation_percentile"
PERCENTILE_MAPPINGS = {
    "pe_ttm_percentile_1y": "pe_ttm_percentile_1y",
    "pe_ttm_percentile_3y": "pe_ttm_percentile_3y",
    "pe_ttm_percentile_5y": "pe_ttm_percentile_5y",
    "pe_ttm_percentile_10y": "pe_ttm_percentile_10y",
    "pe_ttm_percentile_all_history": "pe_ttm_percentile_all_history",
    "pb_mrq_percentile_1y": "pb_percentile_1y",
    "pb_mrq_percentile_3y": "pb_percentile_3y",
    "pb_mrq_percentile_5y": "pb_percentile_5y",
    "pb_mrq_percentile_10y": "pb_percentile_10y",
    "pb_mrq_percentile_all_history": "pb_percentile_all_history",
    "ps_ttm_percentile_1y": "ps_percentile_1y",
    "ps_ttm_percentile_3y": "ps_percentile_3y",
    "ps_ttm_percentile_5y": "ps_percentile_5y",
    "ps_ttm_percentile_10y": "ps_percentile_10y",
    "ps_ttm_percentile_all_history": "ps_percentile_all_history",
    "pcf_ncf_ttm_percentile_1y": "pcf_percentile_1y",
    "pcf_ncf_ttm_percentile_3y": "pcf_percentile_3y",
    "pcf_ncf_ttm_percentile_5y": "pcf_percentile_5y",
    "pcf_ncf_ttm_percentile_10y": "pcf_percentile_10y",
    "pcf_ncf_ttm_percentile_all_history": "pcf_percentile_all_history",
}


def build_cn_stock_valuation(
    *,
    root: Path | None = None,
    build_views: bool = True,
    refresh_registry: bool = True,
    now: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    store = ParquetStore(root=root)
    store.ensure_layout()
    master = _read_or_build_master(store, now)
    safe_remove_derived_dataset_dir(store, "cn_stock_valuation")

    updated_at = (now or datetime.now)()
    rows = 0
    partitions = 0
    for _, security in master.iterrows():
        security_id = _clean_string(security.get("security_id"))
        if not security_id:
            continue
        security_df = _materialize_security_valuation(store, security, updated_at)
        if security_df.empty:
            continue
        result = store.write_dataset(
            "cn_stock_valuation",
            security_df,
            partition={"security_id": security_id},
            mode="replace",
        )
        rows += result.row_count
        partitions += result.updated_partitions

    if refresh_registry:
        refresh_derived_registry(store, ["cn_stock_valuation"])
    if build_views:
        DuckDBStore(root=store.root).build_views()
    return {
        "dataset": "cn_stock_valuation",
        "status": "success",
        "rows": rows,
        "partitions": partitions,
    }


def _read_or_build_master(store: ParquetStore, now: Callable[[], datetime] | None) -> pd.DataFrame:
    master = store.read_dataset("cn_security_master")
    if not store.dataset_exists("cn_security_master") or master.empty:
        build_security_master(root=store.root, build_views=False, refresh_registry=False, now=now)
        master = store.read_dataset("cn_security_master")
    return master


def _materialize_security_valuation(
    store: ParquetStore,
    security: pd.Series,
    updated_at: datetime,
) -> pd.DataFrame:
    akshare_code = _clean_string(security.get("akshare_code"))
    baostock_code = _clean_string(security.get("baostock_code"))
    akshare = (
        read_partition_or_empty(store, AKSHARE_VALUATION_DATASET, akshare_code) if akshare_code else pd.DataFrame()
    )
    baostock = (
        read_partition_or_empty(store, BAOSTOCK_PERCENTILE_DATASET, baostock_code) if baostock_code else pd.DataFrame()
    )
    if akshare.empty and baostock.empty:
        return pd.DataFrame()

    akshare_by_date = _rows_by_date(akshare)
    baostock_by_date = _rows_by_date(baostock)
    dates = sorted(set(akshare_by_date) | set(baostock_by_date))
    return pd.DataFrame(
        [
            _valuation_row(
                date_value,
                security,
                akshare_by_date.get(date_value),
                baostock_by_date.get(date_value),
                updated_at,
            )
            for date_value in dates
        ]
    )


def _valuation_row(
    date_value: date,
    security: pd.Series,
    akshare: pd.Series | None,
    baostock: pd.Series | None,
    updated_at: datetime,
) -> dict[str, object]:
    row: dict[str, object] = {
        "date": date_value,
        "security_id": security.get("security_id"),
        "code": security.get("code"),
        "exchange": security.get("exchange"),
        "name": security.get("name"),
        "close": _value(akshare, "close"),
        "total_market_cap": _value(akshare, "total_market_cap"),
        "float_market_cap": _value(akshare, "float_market_cap"),
        "total_shares": _value(akshare, "total_shares"),
        "float_shares": _value(akshare, "float_shares"),
        "pe_ttm": _first_non_null(_value(akshare, "pe_ttm"), _value(baostock, "pe_ttm")),
        "pe_static": _value(akshare, "pe_static"),
        "pb": _first_non_null(_value(akshare, "pb"), _value(baostock, "pb_mrq")),
        "ps": _first_non_null(_value(akshare, "ps"), _value(baostock, "ps_ttm")),
        "pcf": _first_non_null(_value(akshare, "pcf"), _value(baostock, "pcf_ncf_ttm")),
        "source_dataset": _source_dataset(akshare is not None, baostock is not None),
        "updated_at": updated_at,
    }
    for source_column, target_column in PERCENTILE_MAPPINGS.items():
        row[target_column] = _value(baostock, source_column)
    return row


def _source_dataset(has_akshare: bool, has_baostock: bool) -> str:
    if has_akshare and has_baostock:
        return f"{AKSHARE_VALUATION_DATASET}+{BAOSTOCK_PERCENTILE_DATASET}"
    if has_akshare:
        return AKSHARE_VALUATION_DATASET
    return BAOSTOCK_PERCENTILE_DATASET


def _rows_by_date(df: pd.DataFrame) -> dict[date, pd.Series]:
    rows: dict[date, pd.Series] = {}
    if df.empty:
        return rows
    for _, row in df.iterrows():
        date_value = _date_value(row.get("date"))
        if date_value is not None:
            rows[date_value] = row
    return rows


def _value(row: pd.Series | None, column: str) -> object:
    if row is None or column not in row:
        return None
    value = row.get(column)
    if value is None or pd.isna(cast(Any, value)):
        return None
    return value


def _first_non_null(*values: object) -> object:
    for value in values:
        if value is not None and not pd.isna(cast(Any, value)):
            return value
    return None


def _date_value(value: object) -> date | None:
    if value is None or pd.isna(cast(Any, value)):
        return None
    parsed = pd.to_datetime(cast(Any, value), errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _clean_string(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()
