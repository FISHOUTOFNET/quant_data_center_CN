"""AkShare capital structure update module."""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.sources.akshare.client import (
    AkShareEmptyDataError,
    AkShareNetworkError,
    AkShareResponse,
    dataframe_hash,
    normalize_akshare_code,
)
from src.sources.akshare.pipeline.execution import AkShareExecutionContext, AkShareUpdateRequest, ConcurrencyPolicy, FetchResult
from src.sources.akshare.pipeline.universe import latest_active_akshare_codes, resolve_akshare_universe_codes
from src.pipeline.common import PipelineCheckpointLookup
from src.pipeline.lifecycle import LifecycleTaskRef
from src.storage.dataset_catalog import AKSHARE_CAPITAL_STRUCTURE_EM_DATASET
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger

PIPELINE_UPDATE_AKSHARE = "update_akshare"
CAPITAL_STRUCTURE_START_DATE = "1900-01-01"
CAPITAL_STRUCTURE_END_DATE = "2100-01-01"


@dataclass(frozen=True)
class CapitalStructureTask:
    dataset: str
    key: str
    start_date: str
    end_date: str
    output_path: Path
    code: str
    active: bool = False


def plan_capital_structure_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    include_inactive: bool = False,
    max_tasks: int | None = None,
) -> list[CapitalStructureTask]:
    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported AkShare capital_structure update mode: {mode}")
    active_codes = latest_active_akshare_codes(store)
    if isinstance(code, str):
        codes = [normalize_akshare_code(code)]
    elif code:
        codes = [normalize_akshare_code(item) for item in code]
    else:
        codes = resolve_akshare_universe_codes(
            store,
            include_delisted=mode == "full" or include_inactive,
            context="akshare_cn_stock_capital_structure_em",
        )
    codes = list(dict.fromkeys(item for item in codes if item))
    if not codes:
        raise ValueError("No AkShare stock codes found for akshare_cn_stock_capital_structure_em")
    tasks = [
        CapitalStructureTask(
            dataset=AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.name,
            key=stock_code,
            code=stock_code,
            start_date=CAPITAL_STRUCTURE_START_DATE,
            end_date=CAPITAL_STRUCTURE_END_DATE,
            output_path=store.dataset_path(AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.name, {"code": stock_code}),
            active=stock_code in active_codes if active_codes else code is not None,
        )
        for stock_code in codes
    ]
    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def prefilter_capital_structure_tasks(
    tasks: list[CapitalStructureTask],
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[CapitalStructureTask]:
    if checkpoint_lookup is None:
        return list(tasks)
    remaining: list[CapitalStructureTask] = []
    for task in tasks:
        if task.output_path.exists() and checkpoint_lookup.pipeline_checkpoint_succeeded(
            PIPELINE_UPDATE_AKSHARE,
            task.dataset,
            task.key,
            task.start_date,
            task.end_date,
            task.output_path,
        ):
            continue
        remaining.append(task)
    return remaining


class CapitalStructureEmModule:
    target = "capital_structure"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[CapitalStructureTask]:
        return plan_capital_structure_tasks(
            config=context.config,
            store=context.store,
            mode=request.mode,
            code=request.code,
            include_inactive=request.include_inactive,
            max_tasks=request.max_tasks,
        )

    def prefilter(
        self, tasks: list[CapitalStructureTask], context: AkShareExecutionContext
    ) -> list[CapitalStructureTask]:
        return prefilter_capital_structure_tasks(tasks, context.checkpoint_lookup)

    def fetch(self, task: CapitalStructureTask, context: AkShareExecutionContext) -> FetchResult:
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
            logger.error("AkShare capital structure task failed code={}: {}", task.code, result.error)
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
            logger.exception("AkShare capital structure write failed code={}", task.code)
            return [rows.run_row]

    def record_skip(
        self,
        task: CapitalStructureTask,
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

    def progress_row(self, task: CapitalStructureTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.key, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(
            workers=_resolve_workers(context.config, request.workers),
            thread_name_prefix="update-akshare-capital-structure",
            stop_on_circuit_open=True,
        )

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare capital structure update started mode={} force={} workers={} planned_tasks={} processing_tasks={}",
            request.mode,
            request.force,
            workers,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: CapitalStructureTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare capital structure progress {}/{} code={} status={} rows={}",
            progress.processed,
            progress.total,
            task.code,
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare capital structure update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("api.akshare.workers", 3)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid AkShare capital_structure workers value: {raw_workers!r}") from exc


def _task_ref(task: CapitalStructureTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE,
        task.dataset,
        task.key,
        task.start_date,
        task.end_date,
        task.output_path,
    )


def _fetch_task(client: Any, task: CapitalStructureTask) -> AkShareResponse:
    result = client.fetch_capital_structure(task.code)
    if isinstance(result, AkShareResponse):
        return result
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"{task.dataset} client returned unsupported result: {type(result)!r}")
    return AkShareResponse(
        endpoint="stock_zh_a_gbjg_em",
        params={"symbol": task.code},
        akshare_version=str(getattr(client, "akshare_version", "unknown")),
        data=result.copy(),
    )


def _write_task_data(store: ParquetStore, task: CapitalStructureTask, df: pd.DataFrame) -> tuple[Path, int, str | None]:
    if df.empty and task.active:
        raise AkShareEmptyDataError(f"{task.dataset} returned empty data for active code {task.code}")
    if _capital_structure_unchanged(store, task.code, df):
        logger.info(
            "AkShare capital structure unchanged code={} rows={} path={}",
            task.code,
            len(df),
            task.output_path,
        )
        return task.output_path, len(df), _last_success_date(df)
    output_path = store.write_dataset(task.dataset, df, {"code": task.code}).primary_path
    return output_path, len(df), _last_success_date(df)


def _capital_structure_unchanged(store: ParquetStore, code: str, df: pd.DataFrame) -> bool:
    path = store.dataset_path(AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.name, {"code": code})
    if not path.exists():
        return False
    existing = store.read_dataset(AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.name, {"code": code})
    return dataframe_hash(existing) == dataframe_hash(df)


def _last_success_date(df: pd.DataFrame) -> str | None:
    if df.empty or "change_date" not in df.columns:
        return None
    dates = df["change_date"]
    if dates.empty:
        return None
    return str(dates.max())
