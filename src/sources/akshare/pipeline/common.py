"""Shared helpers for the independent AkShare A-share pipelines."""

from __future__ import annotations

import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.lifecycle import LifecycleTaskRef, failure_rows, success_rows
from src.sources.akshare.client import AkShareResponse
from src.storage.parquet_store import ParquetStore

PIPELINE_UPDATE_AKSHARE_DELIST = "update_akshare_delist"
PIPELINE_UPDATE_AKSHARE_SPOT = "update_akshare_spot"
PIPELINE_UPDATE_AKSHARE_DAILY_BAR = "update_akshare_daily_bar"


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
        data=result.copy(),
    )


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
    rows = success_rows(
        LifecycleTaskRef(pipeline, dataset, code, start_date, end_date, output_path),
        started_at=started_at,
        ended_at=ended_at,
        row_count=row_count,
    )
    assert rows.status_row is not None
    assert rows.checkpoint_row is not None
    return rows.run_row, rows.status_row, rows.checkpoint_row


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
    rows = failure_rows(
        LifecycleTaskRef(pipeline, dataset, code, start_date, end_date, output_path),
        started_at=started_at,
        ended_at=ended_at,
        error_stack=error_stack,
    )
    assert rows.status_row is not None
    assert rows.checkpoint_row is not None
    return rows.run_row, rows.status_row, rows.checkpoint_row


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


def error_stack(exc: Exception) -> str:
    stack = traceback.format_exc()
    return stack if stack and stack != "NoneType: None\n" else f"{type(exc).__name__}: {exc}"
