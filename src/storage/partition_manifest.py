"""Partition-level Parquet write ledger hashing utilities."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

from src.storage.schema import DATASET_PARTITION_MANIFEST_SCHEMA, field_names

SEMANTIC_EXCLUDED_COLUMNS = {
    "fetched_at",
    "updated_at",
    "created_at",
    "ingested_at",
    "downloaded_at",
    "collected_at",
    "run_id",
    "writer_pid",
    "writer_thread",
}
DATE_RANGE_COLUMNS = (
    "date",
    "trade_date",
    "calendar_date",
    "snapshot_date",
    "period_end_date",
    "change_date",
    "dividend_operate_date",
    "report_date",
    "publish_date",
)
MASTER_HASH_COLUMNS = (
    "security_id",
    "code",
    "exchange",
    "name",
    "security_type",
    "baostock_code",
    "akshare_code",
    "qlib_symbol",
    "ipo_date",
    "delist_date",
    "listing_status",
    "is_active",
)


def schema_hash(schema: pa.Schema) -> str:
    payload = [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }
        for field in schema
    ]
    return _sha256_json(payload)


def dataframe_content_hash(df: pd.DataFrame, schema: pa.Schema) -> str:
    table = _table_for_schema(df, schema)
    return _sha256_bytes(_arrow_ipc_bytes(table))


def dataframe_semantic_hash(df: pd.DataFrame, schema: pa.Schema, dataset_id: str) -> str:
    del dataset_id
    semantic_schema = pa.schema([field for field in schema if field.name not in SEMANTIC_EXCLUDED_COLUMNS])
    table = _table_for_schema(df, semantic_schema)
    return _sha256_bytes(_arrow_ipc_bytes(table))


def master_row_hash(row: Any) -> str:
    payload = {column: _stable_value(_row_value(row, column)) for column in MASTER_HASH_COLUMNS}
    return _sha256_json(payload)


def source_signature(source_manifest_rows: pd.DataFrame | list[dict[str, object]], master_hash: str) -> str:
    frame = pd.DataFrame(source_manifest_rows)
    rows: list[dict[str, str]] = []
    if not frame.empty:
        for _, row in frame.iterrows():
            rows.append(
                {
                    "dataset": _clean_string(row.get("dataset")),
                    "partition_column": _clean_string(row.get("partition_column")),
                    "partition_value": _clean_string(row.get("partition_value")),
                    "semantic_hash": _clean_string(row.get("semantic_hash")),
                }
            )
    rows.sort(
        key=lambda item: (
            item["dataset"],
            item["partition_column"],
            item["partition_value"],
            item["semantic_hash"],
        )
    )
    return _sha256_json({"master_row_hash": master_hash, "sources": rows})


def date_range(df: pd.DataFrame) -> tuple[str, str]:
    for column in DATE_RANGE_COLUMNS:
        if column not in df.columns:
            continue
        values = pd.to_datetime(df[column], errors="coerce")
        values = values.loc[values.notna()]
        if values.empty:
            continue
        return values.min().strftime("%Y-%m-%d"), values.max().strftime("%Y-%m-%d")
    return "", ""


def dataset_partition_manifest_row(
    *,
    dataset: str,
    partition_column: str,
    partition_value: str,
    output_path: Path,
    root: Path,
    df: pd.DataFrame,
    schema: pa.Schema,
    source_signature_value: str = "",
    master_row_hash_value: str = "",
    run_id: str = "",
    writer_pid: int | None = None,
    writer_thread: str = "",
    updated_at: datetime | None = None,
) -> dict[str, object]:
    stat = output_path.stat()
    min_date, max_date = date_range(df)
    return {
        "dataset": dataset,
        "partition_column": partition_column,
        "partition_value": partition_value,
        "output_path": _relative_output_path(output_path, root),
        "row_count": len(df),
        "min_date": min_date,
        "max_date": max_date,
        "content_hash": dataframe_content_hash(df, schema),
        "semantic_hash": dataframe_semantic_hash(df, schema, dataset),
        "schema_hash": schema_hash(schema),
        "source_signature": source_signature_value,
        "master_row_hash": master_row_hash_value,
        "file_size_bytes": stat.st_size,
        "file_mtime": pd.Timestamp(datetime.fromtimestamp(stat.st_mtime)).floor("ms").to_pydatetime(),
        "run_id": run_id,
        "writer_pid": writer_pid,
        "writer_thread": writer_thread,
        "updated_at": updated_at or datetime.now(),
    }


def empty_manifest_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=field_names(DATASET_PARTITION_MANIFEST_SCHEMA))


def _table_for_schema(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    cleaned = _clean_dataframe_for_schema(df, schema)
    return pa.Table.from_pandas(cleaned, schema=schema, preserve_index=False)


def _clean_dataframe_for_schema(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    cleaned = pd.DataFrame(index=df.index)
    for field in schema:
        series = df[field.name] if field.name in df.columns else pd.Series(pd.NA, index=df.index, name=field.name)
        cleaned[field.name] = _coerce_series(series, field.type)
    return cleaned.reset_index(drop=True)


def _coerce_series(series: pd.Series, arrow_type: pa.DataType) -> pd.Series:
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        dates = pd.to_datetime(_replace_null_like(series), errors="coerce")
        return dates.dt.date.where(dates.notna(), None)
    if pa.types.is_timestamp(arrow_type):
        return pd.to_datetime(_replace_null_like(series), errors="coerce").dt.floor("ms")
    if pa.types.is_integer(arrow_type):
        return pd.to_numeric(_replace_null_like(series), errors="coerce").astype("Int64")
    if pa.types.is_floating(arrow_type):
        return pd.to_numeric(_replace_null_like(series), errors="coerce")
    if pa.types.is_boolean(arrow_type):
        if pd.api.types.is_bool_dtype(series.dtype):
            return series.astype("boolean")
        normalized = _replace_null_like(series).astype("string").str.strip().str.lower()
        return normalized.map(
            {
                "true": True,
                "t": True,
                "1": True,
                "yes": True,
                "false": False,
                "f": False,
                "0": False,
                "no": False,
            }
        ).astype("boolean")
    if pa.types.is_string(arrow_type):
        return series.astype("string")
    return series


def _replace_null_like(series: pd.Series) -> pd.Series:
    if series.empty or (
        not pd.api.types.is_object_dtype(series.dtype) and not pd.api.types.is_string_dtype(series.dtype)
    ):
        return series
    return series.mask(series.isin(["", "None", "none", "NaN", "nan"]), pd.NA)


def _arrow_ipc_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_json(payload: object) -> str:
    return _sha256_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _stable_value(value: object) -> object:
    if value is None:
        return None
    try:
        if pd.isna(cast(Any, value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.floor("ms").isoformat()
    if isinstance(value, datetime):
        return pd.Timestamp(value).floor("ms").isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return _stable_value(cast(Any, value).item())
        except (AttributeError, ValueError):
            pass
    return value


def _row_value(row: Any, column: str) -> object:
    if isinstance(row, dict):
        return row.get(column)
    if hasattr(row, "get"):
        return row.get(column)
    return getattr(row, column, None)


def _clean_string(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(cast(Any, value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _relative_output_path(path: Path, root: Path) -> str:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return resolved_path.as_posix()
