"""AkShare daily bar update module."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.sources.akshare.core.normalization import date_iso
from src.sources.akshare.client import AkShareCircuitOpen
from src.sources.akshare.pipeline.execution import AkShareExecutionContext, AkShareUpdateRequest, ConcurrencyPolicy, FetchResult
from src.sources.akshare.pipeline.common import PIPELINE_UPDATE_AKSHARE_DAILY_BAR, error_stack
from src.sources.akshare.pipeline.universe import resolve_akshare_universe_codes
from src.pipeline.common import PipelineCheckpointLookup, default_candidate_date, latest_trading_day_on_or_before
from src.pipeline.lifecycle import LifecycleTaskRef
from src.storage.dataset_catalog import (
    akshare_daily_bar_adjustments,
    akshare_daily_bar_dataset_id,
    normalize_adjustment,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


@dataclass(frozen=True)
class DailyBarTask:
    code: str
    adjustment: str
    dataset: str
    start_date: str
    end_date: str
    output_path: Path
    api_start_date: str
    api_end_date: str
    write_mode: str


def plan_daily_bar_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    adjustment: str = "unadjusted",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | date | None = None,
    end: str | date | None = None,
    max_tasks: int | None = None,
) -> list[DailyBarTask]:
    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in {"full", "incremental"}:
        raise ValueError(f"Unsupported AkShare daily bar mode: {mode}")
    api_end_date = _resolve_daily_bar_end_date(config, store, end)
    if normalized_mode == "full":
        api_start_date = date_iso(
            start, str(config.get("datasets.akshare_cn_stock_daily_bar.full_start", "1990-01-01"))
        )
    else:
        if start is None:
            raise ValueError("AkShare daily bar incremental mode requires --start")
        api_start_date = date_iso(start, api_end_date)
    if api_start_date > api_end_date:
        raise ValueError(f"AkShare daily bar start date {api_start_date} is after end date {api_end_date}")

    codes = resolve_akshare_universe_codes(
        store,
        code=code,
        include_delisted=normalized_mode == "full",
        context="AkShare daily bar",
    )
    tasks = []
    for daily_bar_adjustment in _resolve_adjustments(adjustment):
        for stock_code in codes:
            dataset = akshare_daily_bar_dataset_id(daily_bar_adjustment)
            tasks.append(
                DailyBarTask(
                    code=stock_code,
                    adjustment=daily_bar_adjustment,
                    dataset=dataset,
                    start_date=api_start_date,
                    end_date=api_end_date,
                    output_path=store.dataset_path(dataset, {"code": stock_code}),
                    api_start_date=api_start_date,
                    api_end_date=api_end_date,
                    write_mode="replace" if normalized_mode == "full" else "upsert",
                )
            )
    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def _latest_calendar_date_from_duckdb(conn: Any) -> str | None:
    row = conn.execute(
        """
        SELECT max(calendar_date)
        FROM v_baostock_cn_trading_calendar
        WHERE lower(trim(cast(is_trading_day AS varchar))) IN ('1', 'true', 't', 'yes')
        """
    ).fetchone()
    if row is None or row[0] is None:
        return None
    latest_date = pd.to_datetime(row[0], errors="coerce")
    if pd.isna(latest_date):
        return None
    return latest_date.date().isoformat()


def _resolve_daily_bar_end_date(config: ConfigManager, store: ParquetStore, end: str | date | None) -> str:
    candidate = date_iso(end, default_candidate_date(config))
    if end is not None:
        return candidate
    calendar = store.read_dataset("baostock_cn_trading_calendar")
    if calendar.empty:
        return candidate
    return latest_trading_day_on_or_before(calendar, candidate)


def _duckdb_skippable_daily_bar_task_keys(
    duck_store: DuckDBStore,
    tasks: list[DailyBarTask],
) -> set[tuple[str, str]] | None:
    task_keys = sorted({(task.adjustment, task.code) for task in tasks})
    if not task_keys:
        return set()

    with duck_store.connect() as conn:
        latest_calendar_date = _latest_calendar_date_from_duckdb(conn)
        if latest_calendar_date is None:
            return None

        task_frame = pd.DataFrame(task_keys, columns=["adjustment", "code"])
        conn.register("daily_bar_prefilter_tasks", task_frame)
        try:
            skippable: set[tuple[str, str]] = set()
            for raw_adjustment in sorted(task_frame["adjustment"].unique()):
                daily_bar_adjustment = str(raw_adjustment)
                dataset = akshare_daily_bar_dataset_id(daily_bar_adjustment)
                rows = conn.execute(
                    f"""
                    SELECT latest.code
                    FROM (
                        SELECT
                            h.code,
                            h.date AS latest_date,
                            h.source_endpoint,
                            row_number() OVER (PARTITION BY h.code ORDER BY h.date DESC) AS rn
                        FROM v_{dataset} AS h
                        INNER JOIN daily_bar_prefilter_tasks AS t ON t.code = h.code
                        WHERE t.adjustment = ?
                          AND h.date IS NOT NULL
                    ) AS latest
                    WHERE latest.rn = 1
                      AND latest.source_endpoint = 'stock_zh_a_hist'
                      AND latest.latest_date >= CAST(? AS DATE)
                    """,
                    [daily_bar_adjustment, latest_calendar_date],
                ).fetchall()
                skippable.update((daily_bar_adjustment, str(row[0])) for row in rows)
            return skippable
        finally:
            with suppress(Exception):
                conn.unregister("daily_bar_prefilter_tasks")


def prefilter_daily_bar_tasks(
    tasks: list[DailyBarTask],
    store: ParquetStore,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[DailyBarTask]:
    if checkpoint_lookup is None or not tasks:
        return list(tasks)

    duck_store = DuckDBStore(root=store.root)
    duck_store.build_views(cleanup_tmp_files=False)
    skippable_task_keys = _duckdb_skippable_daily_bar_task_keys(duck_store, tasks)
    if skippable_task_keys is None:
        logger.warning("Calendar is empty, cannot prefilter daily bar tasks")
        return list(tasks)

    remaining_tasks: list[DailyBarTask] = []
    skipped_count = 0
    for task in tasks:
        if (task.adjustment, task.code) in skippable_task_keys:
            skipped_count += 1
            continue
        remaining_tasks.append(task)

    if skipped_count:
        skipped_ratio = skipped_count / len(tasks) * 100
        logger.info(
            "Checkpoint prefilter skipped {}/{} daily bar tasks ({:.1f}%); processing {} tasks",
            skipped_count,
            len(tasks),
            skipped_ratio,
            len(remaining_tasks),
        )
    return remaining_tasks


class DailyBarModule:
    target = "daily_bar"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[DailyBarTask]:
        return plan_daily_bar_tasks(
            config=context.config,
            store=context.store,
            mode=request.mode,
            adjustment=request.adjustment or "unadjusted",
            code=request.code,
            start=request.start,
            end=request.end,
            max_tasks=request.max_tasks,
        )

    def prefilter(self, tasks: list[DailyBarTask], context: AkShareExecutionContext) -> list[DailyBarTask]:
        return prefilter_daily_bar_tasks(tasks, context.store, context.checkpoint_lookup)

    def fetch(self, task: DailyBarTask, context: AkShareExecutionContext) -> FetchResult:
        started_at = datetime.now()
        try:
            response = context.client.fetch_daily_bars(
                symbol=task.code,
                start_date=task.api_start_date,
                end_date=task.api_end_date,
                adjustment=task.adjustment,
            )
            return FetchResult(task=task, started_at=started_at, ended_at=datetime.now(), response=response)
        except Exception as exc:
            return FetchResult(
                task=task,
                started_at=started_at,
                ended_at=datetime.now(),
                error=exc,
                error_stack=error_stack(exc),
            )

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]:
        task = result.task
        if result.error is not None:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=result.ended_at,
                error_stack=result.error_stack,
            )
            if isinstance(result.error, AkShareCircuitOpen):
                logger.warning(
                    "AkShare daily bar circuit open code={} adjustment={}: {}",
                    task.code,
                    task.adjustment,
                    result.error,
                )
            else:
                logger.error(
                    "AkShare daily bar task failed code={} adjustment={}: {}",
                    task.code,
                    task.adjustment,
                    result.error,
                )
            return [rows.run_row]

        try:
            if result.response is None:
                raise RuntimeError("stock_zh_a_hist returned no response")
            output_path = context.store.write_dataset(
                task.dataset, result.response.data, {"code": task.code}, mode=task.write_mode
            ).primary_path
            rows = context.lifecycle.record_success(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                row_count=len(result.response.data),
                output_path=output_path,
            )
            return [rows.run_row]
        except Exception as exc:
            stack = error_stack(exc)
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=stack,
            )
            logger.exception("AkShare daily bar write failed code={} adjustment={}", task.code, task.adjustment)
            return [rows.run_row]

    def record_skip(
        self,
        task: DailyBarTask,
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

    def progress_row(self, task: DailyBarTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.code, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(
            workers=_resolve_workers(context.config, request.workers),
            thread_name_prefix="update-akshare-daily-bar",
            stop_on_circuit_open=True,
        )

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare daily bar update started mode={} adjustment={} force={} workers={} planned_tasks={} "
            "processing_tasks={}",
            request.mode,
            request.adjustment or "unadjusted",
            request.force,
            workers,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: DailyBarTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare daily bar progress {}/{} code={} adjustment={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            task.code,
            task.adjustment,
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare daily bar update completed processed={} success={} failed={}",
            progress.processed,
            progress.success,
            progress.failed,
        )

    def log_circuit_open(self, attempted_tasks: int) -> None:
        logger.warning(
            "AkShare daily bar circuit opened; stopping new submissions after {} attempted tasks",
            attempted_tasks,
        )


def _task_ref(task: DailyBarTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE_DAILY_BAR,
        task.dataset,
        task.code,
        task.start_date,
        task.end_date,
        task.output_path,
    )


def _resolve_adjustments(adjustment: str) -> list[str]:
    normalized = str(adjustment).strip().lower()
    if normalized == "all":
        return list(akshare_daily_bar_adjustments())
    normalized = normalize_adjustment(normalized)
    if normalized in akshare_daily_bar_adjustments():
        return [normalized]
    raise ValueError(f"Unsupported AkShare daily bar adjustment: {adjustment}")


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("api.akshare.workers", 3)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid AkShare daily bar workers value: {raw_workers!r}") from exc
