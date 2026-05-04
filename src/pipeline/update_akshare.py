"""AkShare crawler dataset update pipeline."""

from __future__ import annotations

import json
import os
import traceback
import uuid
from collections import deque
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import (
    AkShareClient,
    AkShareCircuitOpen,
    AkShareEmptyDataError,
    AkShareError,
    AkShareNetworkError,
    AkShareResponse,
    dataframe_hash,
)
from src.pipeline.akshare_tasks import AkShareTask, plan_akshare_tasks
from src.pipeline.common import PipelineCheckpointLookup, checkpoint_row, should_skip_checkpoint
from src.pipeline.services import PipelineMetadataBatch
from src.quality.validators import ValidationError
from src.storage.dataset_catalog import STOCK_VALUE_EM_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


PIPELINE_UPDATE_AKSHARE = "update_akshare"


def _get_latest_calendar_date(store: ParquetStore) -> str | None:
    calendar_df = store.read_calendar()
    if calendar_df.empty or "calendar_date" not in calendar_df.columns:
        return None
    dates = calendar_df["calendar_date"]
    if dates.empty:
        return None
    return str(dates.max())


def _prefilter_stock_value_em_tasks(
    tasks: list[AkShareTask],
    store: ParquetStore,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[AkShareTask]:
    if checkpoint_lookup is None or not tasks:
        return list(tasks)

    latest_calendar_date = _get_latest_calendar_date(store)
    if latest_calendar_date is None:
        logger.warning("Calendar is empty, cannot prefilter stock_value_em tasks")
        return list(tasks)

    remaining_tasks: list[AkShareTask] = []
    skipped_count = 0
    calendar_warning_shown = False

    for task in tasks:
        if task.end_date is None:
            remaining_tasks.append(task)
            continue

        if task.end_date >= latest_calendar_date:
            if not task.output_path.exists():
                remaining_tasks.append(task)
                continue
            if task.end_date > latest_calendar_date and not calendar_warning_shown:
                logger.warning(
                    "Local calendar is outdated (latest: {}). "
                    "Some stock_value_em data has dates beyond calendar. "
                    "Run 'python -m src.cli update-daily --include-calendar' to update calendar.",
                    latest_calendar_date,
                )
                calendar_warning_shown = True
            skipped_count += 1
            continue

        remaining_tasks.append(task)

    if skipped_count:
        skipped_ratio = skipped_count / len(tasks) * 100
        logger.info(
            "Checkpoint prefilter skipped {}/{} stock_value_em tasks ({:.1f}%); processing {} tasks",
            skipped_count,
            len(tasks),
            skipped_ratio,
            len(remaining_tasks),
        )
    return remaining_tasks


def update_akshare(
    dataset: str = "all",
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    include_inactive: bool = False,
    max_tasks: int | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    workers: int | None = None,
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
    resolved_workers = _resolve_akshare_workers(config, workers)
    metadata_batch = PipelineMetadataBatch(
        store,
        int(config.get("pipeline.metadata_flush_size", 200)),
        count_by="run",
    )
    run_records: list[dict[str, object]] = []
    concurrent_stock_value_tasks: list[AkShareTask] = []

    stock_value_em_tasks = [t for t in tasks if t.dataset == STOCK_VALUE_EM_DATASET.name]
    other_tasks = [t for t in tasks if t.dataset != STOCK_VALUE_EM_DATASET.name]
    stock_value_em_tasks = _prefilter_stock_value_em_tasks(stock_value_em_tasks, store, checkpoint_lookup)
    tasks = other_tasks + stock_value_em_tasks

    def flush_concurrent_stock_value_tasks() -> None:
        if not concurrent_stock_value_tasks:
            return
        _execute_stock_value_tasks_concurrently(
            store,
            ak_client,
            list(concurrent_stock_value_tasks),
            metadata_batch,
            run_records,
            resolved_workers,
        )
        concurrent_stock_value_tasks.clear()

    for task in tasks:
        if task.dataset != STOCK_VALUE_EM_DATASET.name and should_skip_checkpoint(
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

        if task.dataset == STOCK_VALUE_EM_DATASET.name and resolved_workers > 1:
            concurrent_stock_value_tasks.append(task)
            continue

        flush_concurrent_stock_value_tasks()
        row = _execute_task(store, ak_client, task, run_records)
        metadata_batch.add(
            run_row=row["run_row"],
            status_row=row.get("status_row"),
            checkpoint=row.get("checkpoint_row"),
        )

    flush_concurrent_stock_value_tasks()
    metadata_batch.flush()
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views()
    return run_records


@dataclass(frozen=True)
class _TaskFetchResult:
    task: AkShareTask
    started_at: datetime
    ended_at: datetime
    response: AkShareResponse | None = None
    error: Exception | None = None
    error_stack: str = ""


class _AdaptiveConcurrencyController:
    """Conservative fetch concurrency control for crawler-style AkShare endpoints."""

    def __init__(
        self,
        max_workers: int,
        window_size: int = 20,
        failure_rate_threshold: float = 0.15,
        recovery_successes: int = 50,
        consecutive_failure_threshold: int = 3,
    ) -> None:
        self.max_workers = max(int(max_workers), 1)
        self.target_workers = self.max_workers
        self._window_size = max(int(window_size), 1)
        self._failure_rate_threshold = float(failure_rate_threshold)
        self._recovery_successes = max(int(recovery_successes), 1)
        self._consecutive_failure_threshold = max(int(consecutive_failure_threshold), 1)
        self._recent_successes: deque[bool] = deque(maxlen=self._window_size)
        self._consecutive_successes = 0
        self._consecutive_failures = 0

    def record_fetch_result(self, success: bool) -> None:
        self._recent_successes.append(success)
        if success:
            self._consecutive_successes += 1
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1
            self._consecutive_successes = 0

        if not success and self._consecutive_failures >= self._consecutive_failure_threshold:
            self._decrease()
            self._consecutive_failures = 0
            self._recent_successes.clear()
            return

        if len(self._recent_successes) == self._window_size:
            failures = sum(1 for item in self._recent_successes if not item)
            if failures / self._window_size > self._failure_rate_threshold:
                self._decrease()
                self._recent_successes.clear()
                return

        if success and self._consecutive_successes >= self._recovery_successes:
            self._increase()
            self._consecutive_successes = 0
            self._recent_successes.clear()

    def _decrease(self) -> None:
        self._consecutive_successes = 0
        if self.target_workers > 1:
            self.target_workers -= 1
            logger.warning("AkShare stock_value_em fetch concurrency reduced to {}", self.target_workers)

    def _increase(self) -> None:
        if self.target_workers < self.max_workers:
            self.target_workers += 1
            logger.info("AkShare stock_value_em fetch concurrency restored to {}", self.target_workers)


def _resolve_akshare_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("api.akshare.workers", 3)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid AkShare workers value: {raw_workers!r}") from exc


def _execute_stock_value_tasks_concurrently(
    store: ParquetStore,
    client: Any,
    tasks: list[AkShareTask],
    metadata_batch: PipelineMetadataBatch,
    run_records: list[dict[str, object]],
    workers: int,
) -> None:
    if not tasks:
        return

    controller = _AdaptiveConcurrencyController(workers)
    pending: set[Future[_TaskFetchResult]] = set()
    future_tasks: dict[Future[_TaskFetchResult], AkShareTask] = {}
    task_index = 0
    stop_submitting = False

    def submit_until_target(executor: ThreadPoolExecutor) -> None:
        nonlocal task_index
        while (
            not stop_submitting
            and task_index < len(tasks)
            and len(pending) < controller.target_workers
        ):
            task = tasks[task_index]
            task_index += 1
            future = executor.submit(_fetch_task_result, client, task)
            pending.add(future)
            future_tasks[future] = task

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="update-akshare-fetch") as executor:
        submit_until_target(executor)
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                task = future_tasks.pop(future)
                try:
                    result = future.result()
                except Exception as exc:
                    result = _TaskFetchResult(
                        task=task,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                        error=exc,
                        error_stack=traceback.format_exc(),
                    )
                row = _record_fetched_task_result(store, client, result, run_records)
                metadata_batch.add(
                    run_row=row["run_row"],
                    status_row=row.get("status_row"),
                    checkpoint=row.get("checkpoint_row"),
                )
                fetch_success = result.error is None
                controller.record_fetch_result(fetch_success)
                if isinstance(result.error, AkShareCircuitOpen):
                    stop_submitting = True
                    logger.warning(
                        "AkShare stock_value_em circuit opened; stopping new submissions after {} attempted tasks",
                        task_index,
                    )
            submit_until_target(executor)


def _fetch_task_result(client: Any, task: AkShareTask) -> _TaskFetchResult:
    started_at = datetime.now()
    try:
        response = _fetch_task(client, task)
        return _TaskFetchResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            response=response,
        )
    except Exception as exc:
        return _TaskFetchResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            error=exc,
            error_stack=traceback.format_exc(),
        )


def _execute_task(
    store: ParquetStore,
    client: Any,
    task: AkShareTask,
    run_records: list[dict[str, object]],
) -> dict[str, dict[str, object] | None]:
    return _record_fetched_task_result(store, client, _fetch_task_result(client, task), run_records)


def _record_fetched_task_result(
    store: ParquetStore,
    client: Any,
    result: _TaskFetchResult,
    run_records: list[dict[str, object]],
) -> dict[str, dict[str, object] | None]:
    task = result.task
    response = result.response
    raw_path: Path | None = None
    if result.error is not None:
        error_type = _error_type(result.error)
        error_message = str(result.error)
        error_stack = result.error_stack
        logger.error("AkShare task failed dataset={} key={}: {}", task.dataset, task.key, error_message)
        _append_failed_manifest(store, client, task, error_type, error_message, result.started_at, result.ended_at)
        run_row = _run_row(
            task.dataset,
            task.key,
            "failed",
            task.start_date,
            task.end_date,
            result.started_at,
            result.ended_at,
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

    try:
        if response is None:
            raise AkShareNetworkError(f"{task.dataset} returned no response")
        raw_path = _write_raw_response(store.root, response, result.started_at)
        output_path, row_count, last_success_date = _write_task_data(store, task, response.data)
        end_time = datetime.now()
        _append_manifest(store, task, response, raw_path, "success", "", "", result.started_at, end_time)
        run_row = _run_row(
            task.dataset,
            task.key,
            "success",
            task.start_date,
            task.end_date,
            result.started_at,
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
                raw_path = _write_raw_response(store.root, response, result.started_at)
            _append_manifest(
                store,
                task,
                response,
                raw_path,
                "failed",
                error_type,
                error_message,
                result.started_at,
                end_time,
            )
        else:
            _append_failed_manifest(store, client, task, error_type, error_message, result.started_at, end_time)
        run_row = _run_row(
            task.dataset,
            task.key,
            "failed",
            task.start_date,
            task.end_date,
            result.started_at,
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
    if task.dataset == STOCK_VALUE_EM_DATASET.name:
        if task.code is None:
            raise ValueError("stock_value_em task missing code")
        if df.empty and task.active:
            raise AkShareEmptyDataError(f"stock_value_em returned empty data for active code {task.code}")
        if _stock_value_em_unchanged(store, task.code, df):
            logger.info(
                "AkShare stock_value_em unchanged code={} rows={} path={}",
                task.code,
                len(df),
                task.output_path,
            )
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
