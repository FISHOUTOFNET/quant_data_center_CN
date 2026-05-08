"""Shared helpers for the independent AkShare A-share pipelines."""

from __future__ import annotations

import json
import os
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import AkShareError, AkShareResponse, dataframe_hash
from src.pipeline.common import checkpoint_row
from src.quality.validators import ValidationError
from src.storage.parquet_store import ParquetStore


PIPELINE_UPDATE_AKSHARE_DELIST = "update_akshare_delist"
PIPELINE_UPDATE_AKSHARE_SPOT = "update_akshare_spot"
PIPELINE_UPDATE_AKSHARE_DAILY_BAR = "update_akshare_daily_bar"


def write_raw_response(root: Path, response: AkShareResponse, started_at: datetime) -> Path:
    directory = root / "data" / "raw" / "akshare" / response.endpoint / started_at.strftime("%Y%m%d")
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{started_at.strftime('%H%M%S%f')}_{response.data_hash[:12]}_{uuid.uuid4().hex[:8]}.parquet"
    tmp_path = destination.with_name(f"{destination.stem}.tmp{destination.suffix}")
    response.raw_df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, destination)
    return destination


def append_response_manifest(
    store: ParquetStore,
    pipeline: str,
    dataset: str,
    code: str,
    response: AkShareResponse,
    raw_path: Path | None,
    status: str,
    error_type: str,
    error_message: str,
    started_at: datetime,
    ended_at: datetime,
) -> None:
    append_manifest_row(
        store,
        {
            "pipeline": pipeline,
            "dataset": dataset,
            "endpoint": response.endpoint,
            "code": code,
            "params": response.params,
            "akshare_version": response.akshare_version,
            "row_count": response.row_count,
            "data_hash": response.data_hash,
            "raw_path": str(raw_path or ""),
            "status": status,
            "error_type": error_type,
            "error_message": error_message,
            "started_at": started_at.isoformat(timespec="milliseconds"),
            "ended_at": ended_at.isoformat(timespec="milliseconds"),
        },
    )


def append_failed_manifest(
    store: ParquetStore,
    pipeline: str,
    dataset: str,
    endpoint: str,
    code: str,
    params: dict[str, object],
    client: Any,
    error_type: str,
    error_message: str,
    started_at: datetime,
    ended_at: datetime,
) -> None:
    append_manifest_row(
        store,
        {
            "pipeline": pipeline,
            "dataset": dataset,
            "endpoint": endpoint,
            "code": code,
            "params": params,
            "akshare_version": client_akshare_version(client),
            "row_count": 0,
            "data_hash": "",
            "raw_path": "",
            "status": "failed",
            "error_type": error_type,
            "error_message": error_message,
            "started_at": started_at.isoformat(timespec="milliseconds"),
            "ended_at": ended_at.isoformat(timespec="milliseconds"),
        },
    )


def append_manifest_row(store: ParquetStore, row: dict[str, object]) -> None:
    path = store.akshare_manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def client_akshare_version(client: Any) -> str:
    value = getattr(client, "akshare_version", None)
    if value is not None:
        return str(value() if callable(value) else value)
    private_value = getattr(client, "_akshare_version", None)
    if private_value is not None:
        return str(private_value() if callable(private_value) else private_value)
    return "unknown"


def ensure_response(result: object, endpoint: str, params: dict[str, object], client: Any) -> AkShareResponse:
    if isinstance(result, AkShareResponse):
        return result
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"{endpoint} client returned unsupported result: {type(result)!r}")
    return AkShareResponse(
        endpoint=endpoint,
        params=params,
        akshare_version=client_akshare_version(client),
        raw_df=result.copy(),
        data=result.copy(),
        data_hash=dataframe_hash(result),
    )


def run_row(
    dataset: str,
    code: str,
    status: str,
    start_date: str,
    end_date: str,
    start_time: datetime,
    end_time: datetime,
    row_count: int,
    error_stack: str,
) -> dict[str, object]:
    return {
        "task_id": str(uuid.uuid4()),
        "dataset": dataset,
        "code": code,
        "status": status,
        "start_date": start_date,
        "end_date": end_date,
        "start_time": start_time,
        "end_time": end_time,
        "row_count": row_count,
        "error_stack": error_stack,
    }


def status_row(
    dataset: str,
    code: str,
    last_success_date: str | None,
    row_count: int,
    status: str,
    error_stack: str,
) -> dict[str, object]:
    return {
        "dataset": dataset,
        "code": code,
        "last_success_date": last_success_date,
        "row_count": row_count,
        "status": status,
        "updated_at": datetime.now(),
        "error_stack": error_stack,
    }


def success_metadata(
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    started_at: datetime,
    ended_at: datetime,
    row_count: int,
    output_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    return (
        run_row(dataset, code, "success", start_date, end_date, started_at, ended_at, row_count, ""),
        status_row(dataset, code, end_date, row_count, "success", ""),
        checkpoint_row(pipeline, dataset, code, start_date, end_date, "success", row_count, output_path),
    )


def failed_metadata(
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    started_at: datetime,
    ended_at: datetime,
    error_stack: str,
    output_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    return (
        run_row(dataset, code, "failed", start_date, end_date, started_at, ended_at, 0, error_stack),
        status_row(dataset, code, None, 0, "failed", error_stack),
        checkpoint_row(pipeline, dataset, code, start_date, end_date, "failed", 0, output_path, error_stack),
    )


def persist_metadata(
    store: ParquetStore,
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]],
) -> list[dict[str, object]]:
    if not metadata:
        return []
    run_rows = [item[0] for item in metadata]
    status_rows = [item[1] for item in metadata]
    checkpoint_rows = [item[2] for item in metadata]
    store.persist_update_metadata(run_rows, status_rows, checkpoint_rows)
    return run_rows


def error_type(exc: Exception) -> str:
    if isinstance(exc, AkShareError):
        return exc.error_type
    if isinstance(exc, (ValidationError, ValueError)):
        return "schema_drift"
    return "unknown"


def error_stack(exc: Exception) -> str:
    stack = traceback.format_exc()
    return stack if stack and stack != "NoneType: None\n" else f"{type(exc).__name__}: {exc}"

