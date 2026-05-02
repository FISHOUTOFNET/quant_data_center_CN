"""AkShare crawler dataset update pipeline."""

from __future__ import annotations

import json
import os
import traceback
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import (
    AkShareClient,
    AkShareEmptyDataError,
    AkShareError,
    AkShareResponse,
    dataframe_hash,
    report_period_to_akshare_quarter,
)
from src.pipeline.akshare_tasks import AkShareTask, plan_akshare_tasks
from src.pipeline.common import PipelineCheckpointLookup, checkpoint_row, should_skip_checkpoint
from src.pipeline.services import PipelineMetadataBatch
from src.quality.validators import ValidationError
from src.storage.dataset_catalog import STOCK_INSTITUTE_HOLD_DATASET, STOCK_VALUE_EM_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


PIPELINE_UPDATE_AKSHARE = "update_akshare"


def update_akshare(
    dataset: str = "all",
    mode: str = "partial",
    start_quarter: str | None = None,
    end_quarter: str | None = None,
    code: tuple[str, ...] | list[str] | str | None = None,
    include_inactive: bool = False,
    max_tasks: int | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager, pd.DataFrame], Any] | None = None,
) -> list[dict[str, object]]:
    """Update AkShare crawler datasets without going through MarketDataProvider."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    tasks = plan_akshare_tasks(
        config=config,
        store=store,
        dataset=dataset,
        mode=mode,
        start_quarter=start_quarter,
        end_quarter=end_quarter,
        code=code,
        include_inactive=include_inactive,
        max_tasks=max_tasks,
    )
    checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
    stock_basic_df = store.read_stock_basic()
    ak_client = client or (
        client_factory(config, stock_basic_df)
        if client_factory is not None
        else AkShareClient(config=config, stock_basic_df=stock_basic_df)
    )
    metadata_batch = PipelineMetadataBatch(
        store,
        int(config.get("pipeline.metadata_flush_size", 200)),
        count_by="run",
    )
    run_records: list[dict[str, object]] = []

    for task in tasks:
        if should_skip_checkpoint(
            store,
            PIPELINE_UPDATE_AKSHARE,
            task.dataset,
            task.key,
            task.start_date,
            task.end_date,
            task.output_path,
            resume,
            force,
            checkpoint_lookup,
        ):
            row = _run_row(
                task.dataset,
                task.key,
                "skipped_checkpoint",
                task.start_date,
                task.end_date,
                datetime.now(),
                datetime.now(),
                0,
                "checkpoint",
            )
            metadata_batch.add(run_row=row)
            run_records.append(row)
            continue

        row = _execute_task(store, ak_client, task, run_records)
        metadata_batch.add(
            run_row=row["run_row"],
            status_row=row.get("status_row"),
            checkpoint=row.get("checkpoint_row"),
        )

    metadata_batch.flush()
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views()
    return run_records


def _execute_task(
    store: ParquetStore,
    client: Any,
    task: AkShareTask,
    run_records: list[dict[str, object]],
) -> dict[str, dict[str, object] | None]:
    start_time = datetime.now()
    response: AkShareResponse | None = None
    raw_path: Path | None = None
    try:
        response = _fetch_task(client, task)
        raw_path = _write_raw_response(store.root, response, start_time)
        output_path, row_count, last_success_date = _write_task_data(store, task, response.data)
        end_time = datetime.now()
        _append_manifest(store, task, response, raw_path, "success", "", "", start_time, end_time)
        run_row = _run_row(
            task.dataset,
            task.key,
            "success",
            task.start_date,
            task.end_date,
            start_time,
            end_time,
            row_count,
            "",
        )
        status_row = _status_row(task.dataset, task.key, last_success_date, row_count, "success", "")
        checkpoint = checkpoint_row(
            PIPELINE_UPDATE_AKSHARE,
            task.dataset,
            task.key,
            task.start_date,
            task.end_date,
            "success",
            row_count,
            output_path,
        )
        run_records.append(run_row)
        return {"run_row": run_row, "status_row": status_row, "checkpoint_row": checkpoint}
    except Exception as exc:
        end_time = datetime.now()
        error_stack = traceback.format_exc()
        error_type = _error_type(exc)
        error_message = str(exc)
        logger.exception("AkShare task failed dataset={} key={}", task.dataset, task.key)
        if response is not None:
            if raw_path is None:
                raw_path = _write_raw_response(store.root, response, start_time)
            _append_manifest(store, task, response, raw_path, "failed", error_type, error_message, start_time, end_time)
        else:
            _append_failed_manifest(store, client, task, error_type, error_message, start_time, end_time)
        run_row = _run_row(
            task.dataset,
            task.key,
            "failed",
            task.start_date,
            task.end_date,
            start_time,
            end_time,
            0,
            error_stack,
        )
        status_row = _status_row(task.dataset, task.key, None, 0, "failed", error_stack)
        checkpoint = checkpoint_row(
            PIPELINE_UPDATE_AKSHARE,
            task.dataset,
            task.key,
            task.start_date,
            task.end_date,
            "failed",
            0,
            task.output_path,
            error_stack,
        )
        run_records.append(run_row)
        return {"run_row": run_row, "status_row": status_row, "checkpoint_row": checkpoint}


def _fetch_task(client: Any, task: AkShareTask) -> AkShareResponse:
    if task.dataset == STOCK_INSTITUTE_HOLD_DATASET.name:
        if task.report_period is None:
            raise ValueError("stock_institute_hold task missing report_period")
        result = (
            client.fetch_stock_institute_hold(task.report_period)
            if hasattr(client, "fetch_stock_institute_hold")
            else client.query_stock_institute_hold(task.report_period)
        )
        return _ensure_response(
            result,
            endpoint=STOCK_INSTITUTE_HOLD_DATASET.name,
            params={"symbol": report_period_to_akshare_quarter(task.report_period)},
            client=client,
        )

    if task.dataset == STOCK_VALUE_EM_DATASET.name:
        if task.code is None:
            raise ValueError("stock_value_em task missing code")
        result = (
            client.fetch_stock_value(task.code)
            if hasattr(client, "fetch_stock_value")
            else client.query_stock_value(task.code)
        )
        return _ensure_response(
            result,
            endpoint=STOCK_VALUE_EM_DATASET.name,
            params={"symbol": task.code},
            client=client,
        )

    raise ValueError(f"Unsupported AkShare task dataset: {task.dataset}")


def _ensure_response(result: object, endpoint: str, params: dict[str, object], client: Any) -> AkShareResponse:
    if isinstance(result, AkShareResponse):
        return result
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"{endpoint} client returned unsupported result: {type(result)!r}")
    return AkShareResponse(
        endpoint=endpoint,
        params=params,
        akshare_version=str(getattr(client, "akshare_version", "unknown")),
        raw_df=result.copy(),
        data=result.copy(),
        data_hash=dataframe_hash(result),
    )


def _write_task_data(store: ParquetStore, task: AkShareTask, df: pd.DataFrame) -> tuple[Path, int, str | None]:
    if task.dataset == STOCK_INSTITUTE_HOLD_DATASET.name:
        if task.report_period is None:
            raise ValueError("stock_institute_hold task missing report_period")
        if df.empty:
            raise AkShareEmptyDataError(f"stock_institute_hold returned empty data for {task.report_period}")
        output_path = store.write_stock_institute_hold(task.report_period, df)
        return output_path, len(df), task.end_date

    if task.dataset == STOCK_VALUE_EM_DATASET.name:
        if task.code is None:
            raise ValueError("stock_value_em task missing code")
        if df.empty and task.active:
            raise AkShareEmptyDataError(f"stock_value_em returned empty data for active code {task.code}")
        if _stock_value_em_unchanged(store, task.code, df):
            return task.output_path, len(df), _max_date_iso(df) or task.end_date
        output_path = store.write_stock_value_em(task.code, df)
        return output_path, len(df), _max_date_iso(df) or task.end_date

    raise ValueError(f"Unsupported AkShare task dataset: {task.dataset}")


def _stock_value_em_unchanged(store: ParquetStore, code: str, df: pd.DataFrame) -> bool:
    if not store.stock_value_em_path(code).exists():
        return False
    cleaned = store.clean_dataframe_for_schema(df, STOCK_VALUE_EM_DATASET.schema)
    if not cleaned.empty:
        cleaned = cleaned.sort_values(["code", "date"]).reset_index(drop=True)
    STOCK_VALUE_EM_DATASET.validator(cleaned)
    existing = store.read_stock_value_em(code)
    existing = store.clean_dataframe_for_schema(existing, STOCK_VALUE_EM_DATASET.schema)
    if not existing.empty:
        existing = existing.sort_values(["code", "date"]).reset_index(drop=True)
    return dataframe_hash(existing) == dataframe_hash(cleaned) and _max_date_iso(existing) == _max_date_iso(cleaned)


def _write_raw_response(root: Path, response: AkShareResponse, started_at: datetime) -> Path:
    directory = root / "data" / "raw" / "akshare" / response.endpoint / started_at.strftime("%Y%m%d")
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{started_at.strftime('%H%M%S%f')}_{response.data_hash[:12]}_{uuid.uuid4().hex[:8]}.parquet"
    tmp_path = destination.with_name(f"{destination.stem}.tmp{destination.suffix}")
    response.raw_df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, destination)
    return destination


def _append_manifest(
    store: ParquetStore,
    task: AkShareTask,
    response: AkShareResponse,
    raw_path: Path | None,
    status: str,
    error_type: str,
    error_message: str,
    started_at: datetime,
    ended_at: datetime,
) -> None:
    _append_manifest_row(
        store,
        {
            "pipeline": PIPELINE_UPDATE_AKSHARE,
            "dataset": task.dataset,
            "endpoint": response.endpoint,
            "code": task.key,
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


def _append_failed_manifest(
    store: ParquetStore,
    client: Any,
    task: AkShareTask,
    error_type: str,
    error_message: str,
    started_at: datetime,
    ended_at: datetime,
) -> None:
    _append_manifest_row(
        store,
        {
            "pipeline": PIPELINE_UPDATE_AKSHARE,
            "dataset": task.dataset,
            "endpoint": task.dataset,
            "code": task.key,
            "params": _task_params(task),
            "akshare_version": str(getattr(client, "akshare_version", "unknown")),
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


def _append_manifest_row(store: ParquetStore, row: dict[str, object]) -> None:
    path = store.akshare_manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _task_params(task: AkShareTask) -> dict[str, object]:
    if task.dataset == STOCK_INSTITUTE_HOLD_DATASET.name and task.report_period is not None:
        return {"symbol": report_period_to_akshare_quarter(task.report_period)}
    if task.dataset == STOCK_VALUE_EM_DATASET.name and task.code is not None:
        return {"symbol": task.code}
    return {}


def _error_type(exc: Exception) -> str:
    if isinstance(exc, AkShareError):
        return exc.error_type
    if isinstance(exc, (ValidationError, ValueError)):
        return "schema_drift"
    return "unknown"


def _max_date_iso(df: pd.DataFrame) -> str | None:
    if df.empty or "date" not in df.columns:
        return None
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.dropna().empty:
        return None
    return dates.max().date().isoformat()


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
