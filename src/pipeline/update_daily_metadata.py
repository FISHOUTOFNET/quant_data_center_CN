"""Metadata write helpers for the daily update pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.pipeline.common import PIPELINE_UPDATE_DAILY
from src.pipeline.lifecycle import (
    LifecycleRows,
    LifecycleTaskRef,
    PipelineMetadataBatch,
    failure_rows,
    success_rows,
)
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger


def _persist_lifecycle_rows(
    store: ParquetStore,
    rows: LifecycleRows,
) -> None:
    store.persist_update_metadata(
        [rows.run_row],
        [rows.status_row] if rows.status_row is not None else [],
        [rows.checkpoint_row] if rows.checkpoint_row is not None else [],
    )


def _write_daily_success(
    store: ParquetStore,
    metadata_batch: PipelineMetadataBatch | None,
    dataset: str,
    code: str,
    run_start_date: str,
    end_date: str,
    start_time: datetime,
    df: pd.DataFrame,
    checkpoint_start_date: str,
) -> dict[str, object]:
    output_path = store.write_dataset(dataset, df, {"code": code}).primary_path
    row_count = len(df)
    rows = success_rows(
        LifecycleTaskRef(
            PIPELINE_UPDATE_DAILY,
            dataset,
            code,
            run_start_date,
            end_date,
            output_path,
            checkpoint_start_date=checkpoint_start_date,
        ),
        started_at=start_time,
        ended_at=datetime.now(),
        row_count=row_count,
    )
    if metadata_batch is None:
        _persist_lifecycle_rows(store, rows)
    else:
        metadata_batch.add(run_row=rows.run_row, status_row=rows.status_row, checkpoint=rows.checkpoint_row)
    return rows.run_row


def _write_daily_failure(
    store: ParquetStore,
    metadata_batch: PipelineMetadataBatch | None,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    start_time: datetime,
    output_path: Path,
    error_stack: str,
) -> dict[str, object]:
    rows = failure_rows(
        LifecycleTaskRef(PIPELINE_UPDATE_DAILY, dataset, code, start_date, end_date, output_path),
        started_at=start_time,
        ended_at=datetime.now(),
        error_stack=error_stack,
    )
    try:
        if metadata_batch is None:
            _persist_lifecycle_rows(store, rows)
        else:
            metadata_batch.add(run_row=rows.run_row, status_row=rows.status_row, checkpoint=rows.checkpoint_row)
    except Exception:
        logger.exception("Failed to persist daily update failure for {} {}", dataset, code)
    return rows.run_row
