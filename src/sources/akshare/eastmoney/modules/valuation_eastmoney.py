"""AkShare valuation Eastmoney update module."""

from __future__ import annotations

import traceback
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common import PipelineCheckpointLookup
from src.pipeline.lifecycle import LifecycleTaskRef
from src.sources.akshare.client import (
    AkShareEmptyDataError,
    AkShareNetworkError,
    AkShareResponse,
    dataframe_hash,
    normalize_akshare_code,
)
from src.sources.akshare.pipeline.execution import (
    AkShareExecutionContext,
    AkShareUpdateRequest,
    ConcurrencyPolicy,
    FetchResult,
)
from src.sources.akshare.pipeline.universe import (
    latest_active_akshare_valuation_codes,
    resolve_akshare_valuation_universe_codes,
)
from src.storage.dataset_catalog import AKSHARE_VALUATION_EASTMONEY_DATASET
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger

PIPELINE_UPDATE_AKSHARE = "update_akshare"


@dataclass(frozen=True)
class ValuationTask:
    dataset: str
    key: str
    start_date: str | None
    end_date: str | None
    output_path: Path
    code: str | None = None
    active: bool = False


def plan_valuation_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    include_inactive: bool = False,
    max_tasks: int | None = None,
) -> list[ValuationTask]:
    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported AkShare valuation update mode: {mode}")

    active_codes = latest_active_akshare_valuation_codes(store)
    tasks = _valuation_tasks(config, store, mode, code, include_inactive, active_codes)
    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def _valuation_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    code: tuple[str, ...] | list[str] | str | None,
    include_inactive: bool,
    active_codes: set[str],
) -> list[ValuationTask]:
    if isinstance(code, str):
        codes = [normalize_akshare_code(code)]
    elif code:
        codes = [normalize_akshare_code(item) for item in code]
    else:
        active_only = bool(config.get("datasets.akshare_cn_stock_valuation_eastmoney.active_only", True))
        codes = resolve_akshare_valuation_universe_codes(
            store,
            include_delisted=mode == "full" or include_inactive or not active_only,
            context="akshare_cn_stock_valuation_eastmoney",
        )

    codes = list(dict.fromkeys(item for item in codes if item))
    if not codes:
        raise ValueError("No AkShare stock codes found for akshare_cn_stock_valuation_eastmoney")

    tasks: list[ValuationTask] = []
    for stock_code in codes:
        output_path = store.dataset_path(AKSHARE_VALUATION_EASTMONEY_DATASET.name, {"code": stock_code})
        start_date, end_date = _valuation_date_range(store, stock_code)
        tasks.append(
            ValuationTask(
                dataset=AKSHARE_VALUATION_EASTMONEY_DATASET.name,
                key=stock_code,
                code=stock_code,
                start_date=start_date,
                end_date=end_date,
                output_path=output_path,
                active=stock_code in active_codes if active_codes else code is not None,
            )
        )
    return tasks


def _valuation_date_range(store: ParquetStore, code: str) -> tuple[str | None, str | None]:
    path = store.dataset_path(AKSHARE_VALUATION_EASTMONEY_DATASET.name, {"code": code})
    if not path.exists():
        return None, None
    df = store.read_dataset(AKSHARE_VALUATION_EASTMONEY_DATASET.name, {"code": code})
    if df.empty or "date" not in df.columns:
        return None, None
    dates = df["date"]
    if dates.empty:
        return None, None
    return str(dates.min()), str(dates.max())


def _latest_calendar_date(store: ParquetStore) -> str | None:
    baostock_cn_trading_calendar_df = store.read_dataset("baostock_cn_trading_calendar")
    if baostock_cn_trading_calendar_df.empty or "calendar_date" not in baostock_cn_trading_calendar_df.columns:
        return None
    dates = baostock_cn_trading_calendar_df["calendar_date"]
    if dates.empty:
        return None
    return str(dates.max())


def prefilter_valuation_tasks(
    tasks: list[ValuationTask],
    store: ParquetStore,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[ValuationTask]:
    if checkpoint_lookup is None or not tasks:
        return list(tasks)

    latest_calendar_date = _latest_calendar_date(store)
    if latest_calendar_date is None:
        logger.warning("Calendar is empty, cannot prefilter akshare_cn_stock_valuation_eastmoney tasks")
        return list(tasks)

    remaining_tasks: list[ValuationTask] = []
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
                    "Local baostock_cn_trading_calendar is outdated (latest: {}). "
                    "Some akshare_cn_stock_valuation_eastmoney data has dates beyond baostock_cn_trading_calendar. "
                    "Run 'python -m src.cli update-baostock-daily --dataset baostock_cn_trading_calendar' "
                    "to update baostock_cn_trading_calendar.",
                    latest_calendar_date,
                )
                calendar_warning_shown = True
            skipped_count += 1
            continue

        remaining_tasks.append(task)

    if skipped_count:
        skipped_ratio = skipped_count / len(tasks) * 100
        logger.info(
            "Checkpoint prefilter skipped {}/{} akshare_cn_stock_valuation_eastmoney tasks ({:.1f}%); "
            "processing {} tasks",
            skipped_count,
            len(tasks),
            skipped_ratio,
            len(remaining_tasks),
        )
    return remaining_tasks


class AdaptiveConcurrencyController:
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
            logger.warning(
                "AkShare akshare_cn_stock_valuation_eastmoney fetch concurrency reduced to {}", self.target_workers
            )

    def _increase(self) -> None:
        if self.target_workers < self.max_workers:
            self.target_workers += 1
            logger.info(
                "AkShare akshare_cn_stock_valuation_eastmoney fetch concurrency restored to {}", self.target_workers
            )


class ValuationEastmoneyModule:
    target = "valuation"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[ValuationTask]:
        return plan_valuation_tasks(
            config=context.config,
            store=context.store,
            mode=request.mode,
            code=request.code,
            include_inactive=request.include_inactive,
            max_tasks=request.max_tasks,
        )

    def prefilter(self, tasks: list[ValuationTask], context: AkShareExecutionContext) -> list[ValuationTask]:
        return prefilter_valuation_tasks(tasks, context.store, context.checkpoint_lookup)

    def fetch(self, task: ValuationTask, context: AkShareExecutionContext) -> FetchResult:
        started_at = datetime.now()
        try:
            response = _fetch_task(context.client, task)
            return FetchResult(task=task, started_at=started_at, ended_at=datetime.now(), response=response)
        except Exception as exc:
            return FetchResult(
                task=task,
                started_at=started_at,
                ended_at=datetime.now(),
                error=exc,
                error_stack=traceback.format_exc(),
            )

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]:
        task = result.task
        if result.error is not None:
            logger.error("AkShare task failed dataset={} key={}: {}", task.dataset, task.key, str(result.error))
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=result.ended_at,
                error_stack=result.error_stack,
            )
            return [rows.run_row]

        try:
            if result.response is None:
                raise AkShareNetworkError(f"{task.dataset} returned no response")
            output_path, row_count, last_success_date = _write_task_data(context.store, task, result.response.data)
            rows = context.lifecycle.record_success(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                row_count=row_count,
                output_path=output_path,
                last_success_date=last_success_date,
            )
            return [rows.run_row]
        except Exception:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=traceback.format_exc(),
            )
            logger.exception("AkShare task failed dataset={} key={}", task.dataset, task.key)
            return [rows.run_row]

    def record_skip(
        self,
        task: ValuationTask,
        context: AkShareExecutionContext,
        status: str = "skipped_checkpoint",
        reason: str = "checkpoint",
    ) -> list[dict[str, object]]:
        now = datetime.now()
        rows = context.lifecycle.record_skipped(
            _task_ref(task),
            status=status,
            started_at=now,
            ended_at=now,
            reason=reason,
        )
        return [rows.run_row]

    def progress_row(self, task: ValuationTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.key, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        workers = _resolve_workers(context.config, request.workers)
        if workers <= 1:
            return ConcurrencyPolicy(workers=1)
        return ConcurrencyPolicy(
            workers=workers,
            adaptive_controller=AdaptiveConcurrencyController(workers),
            stop_on_circuit_open=True,
        )

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare update started dataset={} mode={} force={} workers={} planned_tasks={} processing_tasks={}",
            "akshare_cn_stock_valuation_eastmoney",
            request.mode,
            request.force,
            workers,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: ValuationTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare update progress {}/{} code={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            row.get("code", task.key),
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )

    def log_circuit_open(self, attempted_tasks: int) -> None:
        logger.warning(
            "AkShare akshare_cn_stock_valuation_eastmoney circuit opened; stopping new submissions after {} "
            "attempted tasks",
            attempted_tasks,
        )


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("api.akshare.workers", 3)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid AkShare workers value: {raw_workers!r}") from exc


def _task_ref(task: ValuationTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE,
        task.dataset,
        task.key,
        cast(str, task.start_date),
        cast(str, task.end_date),
        task.output_path,
    )


def _fetch_task(client: Any, task: ValuationTask) -> AkShareResponse:
    if task.code is None:
        raise ValueError("akshare_cn_stock_valuation_eastmoney task missing code")
    result = client.fetch_stock_valuation(task.code)
    return _ensure_response(
        result,
        endpoint=AKSHARE_VALUATION_EASTMONEY_DATASET.name,
        params={"symbol": task.code},
        client=client,
    )


def _ensure_response(result: object, endpoint: str, params: dict[str, object], client: Any) -> AkShareResponse:
    if isinstance(result, AkShareResponse):
        return result
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"{endpoint} client returned unsupported result: {type(result)!r}")
    return AkShareResponse(
        endpoint=endpoint,
        params=params,
        akshare_version=str(getattr(client, "akshare_version", "unknown")),
        data=result.copy(),
    )


def _write_task_data(store: ParquetStore, task: ValuationTask, df: pd.DataFrame) -> tuple[Path, int, str | None]:
    if task.code is None:
        raise ValueError("akshare_cn_stock_valuation_eastmoney task missing code")
    if df.empty and task.active:
        raise AkShareEmptyDataError(
            f"akshare_cn_stock_valuation_eastmoney returned empty data for active code {task.code}"
        )
    if _valuation_unchanged(store, task.code, df):
        logger.info(
            "AkShare akshare_cn_stock_valuation_eastmoney unchanged code={} rows={} path={}",
            task.code,
            len(df),
            task.output_path,
        )
        return task.output_path, len(df), _last_success_date(df)
    output_path = store.write_dataset(task.dataset, df, {"code": task.code}).primary_path
    return output_path, len(df), _last_success_date(df)


def _valuation_unchanged(store: ParquetStore, code: str, df: pd.DataFrame) -> bool:
    path = store.dataset_path(AKSHARE_VALUATION_EASTMONEY_DATASET.name, {"code": code})
    if not path.exists():
        return False
    existing = store.read_dataset(AKSHARE_VALUATION_EASTMONEY_DATASET.name, {"code": code})
    return dataframe_hash(existing) == dataframe_hash(df)


def _last_success_date(df: pd.DataFrame) -> str | None:
    if df.empty or "date" not in df.columns:
        return None
    dates = df["date"]
    if dates.empty:
        return None
    return str(dates.max())
