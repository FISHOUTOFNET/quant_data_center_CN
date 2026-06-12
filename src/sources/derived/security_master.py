"""Build the canonical China security master dataset."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.sources.derived.common import read_latest_or_empty, refresh_derived_registry
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger


def build_security_master(
    *,
    root: Path | None = None,
    security_ids: tuple[str, ...] | None = None,
    changed_since: datetime | None = None,
    build_views: bool = True,
    refresh_registry: bool = True,
    now: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    del security_ids, changed_since
    store = ParquetStore(root=root)
    store.ensure_layout()
    updated_at = (now or datetime.now)()
    qlib_symbols = _local_qlib_symbols(store)
    records: dict[str, dict[str, Any]] = {}

    spot = read_latest_or_empty(store, "akshare_cn_stock_spot_quote_eastmoney")
    if spot.empty:
        spot = read_latest_or_empty(store, "akshare_cn_stock_spot_quote_sina")
    _merge_spot(records, spot, qlib_symbols)
    _merge_delist(records, read_latest_or_empty(store, "akshare_cn_stock_delist_sh"), "SH", qlib_symbols)
    _merge_delist(records, read_latest_or_empty(store, "akshare_cn_stock_delist_sz"), "SZ", qlib_symbols)
    _merge_baostock_basic(records, store.read_dataset("baostock_cn_stock_basic"), qlib_symbols)
    _merge_qlib(records, qlib_symbols)

    rows = [_finalize_record(record, updated_at, qlib_symbols) for record in records.values()]
    df = pd.DataFrame(rows).sort_values(["exchange", "code"]).reset_index(drop=True) if rows else pd.DataFrame()
    result = store.write_dataset("cn_security_master", df, mode="replace")

    if refresh_registry:
        refresh_derived_registry(store, ["cn_security_master"])
    if build_views:
        DuckDBStore(root=store.root).build_views()

    loaded = store.read_dataset("cn_security_master")
    active = int(loaded["is_active"].fillna(False).astype(bool).sum()) if "is_active" in loaded else 0
    delisted = int((loaded.get("listing_status", pd.Series(dtype="string")).astype("string") == "delisted").sum())
    return {
        "dataset": "cn_security_master",
        "status": "success",
        "rows": result.row_count,
        "active": active,
        "delisted": delisted,
        "path": str(result.primary_path),
    }


def _local_qlib_symbols(store: ParquetStore) -> set[str]:
    membership = read_latest_or_empty(store, "qlib_cn_instrument_membership")
    if not membership.empty and "qlib_symbol" in membership:
        return {
            symbol
            for symbol in membership["qlib_symbol"].dropna().astype(str).str.strip().str.lower()
            if _parse_qlib_symbol(symbol) is not None
        }
    return {
        symbol.strip().lower()
        for symbol in store.list_dataset_partitions("qlib_cn_stock_features_day")
        if _parse_qlib_symbol(symbol) is not None
    }


def _merge_spot(records: dict[str, dict[str, Any]], df: pd.DataFrame, qlib_symbols: set[str]) -> None:
    if df.empty:
        return
    for _, row in df.iterrows():
        code = _six_digit(row.get("code"))
        if code is None:
            continue
        exchange = _exchange_from_akshare_code(code)
        if exchange is None:
            logger.warning("Unknown AkShare exchange for code={}, skipping security master row", code)
            continue
        record = _record(records, exchange, code, qlib_symbols)
        record["sources"].add("spot")
        record["spot_name"] = _clean_string(row.get("name"))
        record["spot_date"] = _to_date(row.get("trade_date"))


def _merge_delist(
    records: dict[str, dict[str, Any]],
    df: pd.DataFrame,
    default_exchange: str,
    qlib_symbols: set[str],
) -> None:
    if df.empty:
        return
    for _, row in df.iterrows():
        code = _six_digit(row.get("code"))
        if code is None:
            continue
        exchange = _normalize_exchange(row.get("exchange")) or default_exchange
        record = _record(records, exchange, code, qlib_symbols)
        record["sources"].add("delist")
        record["delist_name"] = _clean_string(row.get("name"))
        record["list_date"] = _to_date(row.get("list_date"))
        record["akshare_delist_date"] = _to_date(row.get("delist_date"))
        record["delist_snapshot_date"] = _to_date(row.get("snapshot_date"))


def _merge_baostock_basic(records: dict[str, dict[str, Any]], df: pd.DataFrame, qlib_symbols: set[str]) -> None:
    if df.empty:
        return
    for _, row in df.iterrows():
        parsed = _parse_baostock_code(row.get("code"))
        if parsed is None:
            logger.warning("Unknown Baostock code format for security master: {}", row.get("code"))
            continue
        exchange, code = parsed
        record = _record(records, exchange, code, qlib_symbols)
        record["sources"].add("baostock")
        record["baostock_name"] = _clean_string(row.get("name"))
        record["ipo_date"] = _to_date(row.get("ipo_date"))
        record["baostock_delist_date"] = _to_date(row.get("delist_date"))
        record["security_type"] = _clean_string(row.get("security_type"))
        record["baostock_listing_status"] = _clean_string(row.get("listing_status")).lower()


def _merge_qlib(records: dict[str, dict[str, Any]], qlib_symbols: set[str]) -> None:
    for symbol in sorted(qlib_symbols):
        parsed = _parse_qlib_symbol(symbol)
        if parsed is None:
            continue
        exchange, code = parsed
        record = _record(records, exchange, code, qlib_symbols)
        record["sources"].add("qlib")


def _record(
    records: dict[str, dict[str, Any]],
    exchange: str,
    code: str,
    qlib_symbols: set[str],
) -> dict[str, Any]:
    security_id = f"{exchange}.{code}"
    if security_id not in records:
        records[security_id] = {
            "security_id": security_id,
            "code": code,
            "exchange": exchange,
            "sources": set(),
            "board": _board(exchange, code),
            "baostock_code": f"{exchange.lower()}.{code}" if exchange in {"SH", "SZ"} else "",
            "akshare_code": code,
            "qlib_symbol": _qlib_symbol(exchange, code, qlib_symbols),
        }
    return records[security_id]


def _finalize_record(record: dict[str, Any], updated_at: datetime, qlib_symbols: set[str]) -> dict[str, Any]:
    sources = set(record["sources"])
    has_spot = "spot" in sources
    has_delist = "delist" in sources
    has_baostock = "baostock" in sources
    has_qlib_only = sources == {"qlib"}
    baostock_delist_date = record.get("baostock_delist_date")

    if has_delist:
        is_active: bool | None = False
        listing_status = "delisted"
    elif has_spot:
        is_active = True
        listing_status = "active"
    elif has_baostock and baostock_delist_date is not None:
        is_active = False
        listing_status = "delisted"
    elif has_baostock:
        is_active, listing_status = _status_from_baostock(record.get("baostock_listing_status"))
    elif has_qlib_only:
        is_active = False
        listing_status = "unknown"
    else:
        is_active = None
        listing_status = "unknown"

    name = ""
    if has_spot and listing_status == "active":
        name = record.get("spot_name", "")
    if not name:
        name = record.get("delist_name", "") or record.get("baostock_name", "")

    exchange = str(record["exchange"])
    code = str(record["code"])
    return {
        "security_id": record["security_id"],
        "code": code,
        "exchange": exchange,
        "name": name,
        "security_type": record.get("security_type", ""),
        "board": record.get("board", "unknown"),
        "baostock_code": record.get("baostock_code", ""),
        "akshare_code": record.get("akshare_code", ""),
        "qlib_symbol": _qlib_symbol(exchange, code, qlib_symbols),
        "ipo_date": record.get("ipo_date") or record.get("list_date"),
        "delist_date": record.get("akshare_delist_date") or baostock_delist_date,
        "listing_status": listing_status,
        "is_active": is_active,
        "source_priority": _source_priority(sources),
        "latest_source_date": _max_date(record.get("spot_date"), record.get("delist_snapshot_date")),
        "updated_at": updated_at,
    }


def _source_priority(sources: set[str]) -> str:
    if len(sources) > 1:
        return "mixed"
    if "spot" in sources:
        return "akshare_spot"
    if "delist" in sources:
        return "akshare_delist"
    if "baostock" in sources:
        return "baostock_basic"
    if "qlib" in sources:
        return "qlib_only"
    return "unknown"


def _status_from_baostock(status: object) -> tuple[bool | None, str]:
    normalized = _clean_string(status).lower()
    if normalized in {"1", "active", "listed", "normal"}:
        return True, "active"
    if normalized in {"0", "delisted", "terminated", "inactive"}:
        return False, "delisted"
    return None, "unknown"


def _exchange_from_akshare_code(code: str) -> str | None:
    if code.startswith("6"):
        return "SH"
    if code.startswith(("0", "2", "3")):
        return "SZ"
    if code.startswith(("4", "8", "9")):
        return "BJ"
    return None


def _normalize_exchange(value: object) -> str | None:
    normalized = _clean_string(value).upper()
    if normalized in {"SH", "SSE"}:
        return "SH"
    if normalized in {"SZ", "SZSE"}:
        return "SZ"
    if normalized in {"BJ", "BSE"}:
        return "BJ"
    return None


def _board(exchange: str, code: str) -> str:
    if exchange == "SH" and code.startswith("688"):
        return "star"
    if exchange == "SH" and code.startswith("60"):
        return "main"
    if exchange == "SZ" and code.startswith("00"):
        return "main"
    if exchange == "SZ" and code.startswith("30"):
        return "chinext"
    if exchange == "BJ":
        return "bse"
    return "unknown"


def _qlib_symbol(exchange: str, code: str, qlib_symbols: set[str]) -> str:
    symbol = f"{exchange.lower()}{code}"
    if exchange in {"SH", "SZ"}:
        return symbol
    if exchange == "BJ" and symbol in qlib_symbols:
        return symbol
    return ""


def _parse_baostock_code(value: object) -> tuple[str, str] | None:
    text = _clean_string(value).lower()
    if len(text) != 9 or text[2] != ".":
        return None
    prefix = text[:2]
    code = _six_digit(text[3:])
    if code is None or prefix not in {"sh", "sz"}:
        return None
    return prefix.upper(), code


def _parse_qlib_symbol(value: object) -> tuple[str, str] | None:
    text = _clean_string(value).lower()
    if len(text) != 8:
        return None
    prefix = text[:2]
    code = _six_digit(text[2:])
    if code is None or prefix not in {"sh", "sz", "bj"}:
        return None
    return prefix.upper(), code


def _six_digit(value: object) -> str | None:
    text = _clean_string(value)
    return text if len(text) == 6 and text.isdigit() else None


def _clean_string(value: object) -> str:
    if value is None or pd.isna(cast(Any, value)):
        return ""
    return str(value).strip()


def _to_date(value: object) -> date | None:
    if value is None or pd.isna(cast(Any, value)):
        return None
    parsed = pd.to_datetime(cast(Any, value), errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _max_date(*values: object) -> date | None:
    dates = [value for value in values if isinstance(value, date)]
    return max(dates) if dates else None
