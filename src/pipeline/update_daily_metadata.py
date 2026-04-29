"""Metadata write helpers for the daily update pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.pipeline.common import PIPELINE_UPDATE_DAILY, checkpoint_row, write_checkpoint
from src.pipeline.services import PipelineMetadataBatch
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger


def _persist_run_status(
    store: ParquetStore,
    run_row: dict[str, object],
    status_row: dict[str, object] | None = None,
) -> None:
    store.append_update_runs(pd.DataFrame([run_row]))
    if status_row is not None:
        store.upsert_update_status(pd.DataFrame([status_row]))


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
    output_path = store.write_daily_k(dataset, code, df)
    row_count = len(df)
    run_row = _run_row(
        dataset,
        code,
        "success",
        run_start_date,
        end_date,
        start_time,
        datetime.now(),
        row_count,
        "",
    )
    status_row = _status_row(dataset, code, end_date, row_count, "success", "")
    checkpoint = checkpoint_row(
        PIPELINE_UPDATE_DAILY,
        dataset,
        code,
        checkpoint_start_date,
        end_date,
        "success",
        row_count,
        output_path,
    )
    if metadata_batch is None:
        _persist_run_status(store, run_row, status_row)
        write_checkpoint(
            store,
            PIPELINE_UPDATE_DAILY,
            dataset,
            code,
            checkpoint_start_date,
            end_date,
            "success",
            row_count,
            output_path,
        )
    else:
        metadata_batch.add(run_row=run_row, status_row=status_row, checkpoint=checkpoint)
    return run_row


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
    run_row = _run_row(
        dataset,
        code,
        "failed",
        start_date,
        end_date,
        start_time,
        datetime.now(),
        0,
        error_stack,
    )
    status_row = _status_row(dataset, code, None, 0, "failed", error_stack)
    checkpoint = checkpoint_row(
        PIPELINE_UPDATE_DAILY,
        dataset,
        code,
        start_date,
        end_date,
        "failed",
        0,
        output_path,
        error_stack,
    )
    try:
        if metadata_batch is None:
            _persist_run_status(store, run_row, status_row)
            write_checkpoint(
                store,
                PIPELINE_UPDATE_DAILY,
                dataset,
                code,
                start_date,
                end_date,
                "failed",
                0,
                output_path,
                error_stack,
            )
        else:
            metadata_batch.add(run_row=run_row, status_row=status_row, checkpoint=checkpoint)
    except Exception:
        logger.exception("Failed to persist daily update failure for {} {}", dataset, code)
    return run_row


def _add_success_run(
    records: list[dict[str, object]],
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    row_count: int,
) -> dict[str, object]:
    row = _success_run_row(dataset, code, start_date, end_date, row_count)
    records.append(row)
    return row


def _add_skipped_run(
    records: list[dict[str, object]],
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    reason: str,
) -> dict[str, object]:
    row = _skipped_run_row(dataset, code, start_date, end_date, reason)
    records.append(row)
    return row


def _success_run_row(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    row_count: int,
) -> dict[str, object]:
    now = datetime.now()
    return _run_row(dataset, code, "success", start_date, end_date, now, now, row_count, "")


def _skipped_run_row(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    reason: str,
) -> dict[str, object]:
    now = datetime.now()
    return _run_row(dataset, code, f"skipped_{reason}", start_date, end_date, now, now, 0, reason)


def _run_row(
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
    row = {
        "task_id": str(uuid4()),
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
    return row


def _status_row(
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
