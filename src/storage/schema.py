"""Authoritative PyArrow schemas used by Parquet and DuckDB layers."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa


DAILY_K_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("code", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("preclose", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("amount", pa.float64()),
        pa.field("adjustflag", pa.string()),
        pa.field("turn", pa.float64()),
        pa.field("tradestatus", pa.string()),
        pa.field("pctChg", pa.float64()),
        pa.field("peTTM", pa.float64()),
        pa.field("pbMRQ", pa.float64()),
        pa.field("psTTM", pa.float64()),
        pa.field("pcfNcfTTM", pa.float64()),
        pa.field("isST", pa.string()),
    ]
)

STOCK_BASIC_SCHEMA = pa.schema(
    [
        pa.field("code", pa.string()),
        pa.field("code_name", pa.string()),
        pa.field("ipoDate", pa.date32()),
        pa.field("outDate", pa.date32()),
        pa.field("type", pa.string()),
        pa.field("status", pa.string()),
    ]
)

CALENDAR_SCHEMA = pa.schema(
    [
        pa.field("calendar_date", pa.date32()),
        pa.field("is_trading_day", pa.string()),
    ]
)

ADJUST_FACTOR_SCHEMA = pa.schema(
    [
        pa.field("code", pa.string()),
        pa.field("dividOperateDate", pa.date32()),
        pa.field("foreAdjustFactor", pa.float64()),
        pa.field("backAdjustFactor", pa.float64()),
        pa.field("adjustFactor", pa.float64()),
    ]
)

UPDATE_RUNS_SCHEMA = pa.schema(
    [
        pa.field("task_id", pa.string()),
        pa.field("dataset", pa.string()),
        pa.field("code", pa.string()),
        pa.field("status", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
        pa.field("start_time", pa.timestamp("ms")),
        pa.field("end_time", pa.timestamp("ms")),
        pa.field("row_count", pa.int64()),
        pa.field("error_stack", pa.string()),
    ]
)

UPDATE_STATUS_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string()),
        pa.field("code", pa.string()),
        pa.field("last_success_date", pa.date32()),
        pa.field("row_count", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("updated_at", pa.timestamp("ms")),
        pa.field("error_stack", pa.string()),
    ]
)

PIPELINE_CHECKPOINTS_SCHEMA = pa.schema(
    [
        pa.field("pipeline", pa.string()),
        pa.field("dataset", pa.string()),
        pa.field("code", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
        pa.field("status", pa.string()),
        pa.field("row_count", pa.int64()),
        pa.field("output_path", pa.string()),
        pa.field("updated_at", pa.timestamp("ms")),
        pa.field("error_stack", pa.string()),
    ]
)

METADATA_SCHEMAS: Mapping[str, pa.Schema] = {
    "update_runs": UPDATE_RUNS_SCHEMA,
    "update_status": UPDATE_STATUS_SCHEMA,
    "pipeline_checkpoints": PIPELINE_CHECKPOINTS_SCHEMA,
}

DATASET_SCHEMAS: Mapping[str, pa.Schema] = {
    "daily_k_none": DAILY_K_SCHEMA,
    "daily_k_qfq": DAILY_K_SCHEMA,
    "daily_k_hfq": DAILY_K_SCHEMA,
    "stock_basic": STOCK_BASIC_SCHEMA,
    "calendar": CALENDAR_SCHEMA,
    "adjust_factor": ADJUST_FACTOR_SCHEMA,
    **METADATA_SCHEMAS,
}


def schema_for_dataset(dataset: str) -> pa.Schema:
    """Return the schema for a known dataset."""

    try:
        return DATASET_SCHEMAS[dataset]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset schema: {dataset}") from exc


def field_names(schema: pa.Schema) -> list[str]:
    return [field.name for field in schema]
