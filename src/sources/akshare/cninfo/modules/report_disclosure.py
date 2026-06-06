"""AkShare report disclosure update module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.pipeline.common import should_skip_checkpoint
from src.pipeline.lifecycle import LifecycleTaskRef
from src.sources.akshare.cninfo.adapters.report_disclosure import report_period_end_date
from src.sources.akshare.pipeline.common import error_stack
from src.sources.akshare.pipeline.execution import (
    AkShareExecutionContext,
    AkShareUpdateRequest,
    ConcurrencyPolicy,
    FetchResult,
)
from src.storage.dataset_catalog import AKSHARE_REPORT_DISCLOSURE_DATASET
from src.utils.logging import logger

PIPELINE_UPDATE_AKSHARE_REPORT_DISCLOSURE = "update_akshare_report_disclosure"
DEFAULT_MARKET = "沪深京"
REPORT_PERIOD_SUFFIXES = ("一季", "半年报", "三季", "年报")


@dataclass(frozen=True)
class ReportDisclosureTask:
    dataset: str
    market: str
    period: str
    period_end_date: str
    output_path: Path
    skipped: bool = False


class ReportDisclosureModule:
    target = "report_disclosure"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[ReportDisclosureTask]:
        today = _request_today(request)
        market = request.market or DEFAULT_MARKET
        periods = _resolve_periods(
            request,
            today,
            context.config.get("datasets.akshare_cn_stock_report_disclosure.full_start_year", 1990),
        )
        if request.max_tasks is not None:
            periods = periods[: max(int(request.max_tasks), 0)]
        tasks: list[ReportDisclosureTask] = []
        for period in periods:
            period_end = report_period_end_date(period).isoformat()
            output_path = context.store.dataset_path(
                AKSHARE_REPORT_DISCLOSURE_DATASET.name,
                {"report_period": period},
            )
            skipped = should_skip_checkpoint(
                context.store,
                PIPELINE_UPDATE_AKSHARE_REPORT_DISCLOSURE,
                AKSHARE_REPORT_DISCLOSURE_DATASET.name,
                market,
                period_end,
                period_end,
                output_path,
                request.resume,
                request.force,
                context.checkpoint_lookup,
            )
            tasks.append(
                ReportDisclosureTask(
                    dataset=AKSHARE_REPORT_DISCLOSURE_DATASET.name,
                    market=market,
                    period=period,
                    period_end_date=period_end,
                    output_path=output_path,
                    skipped=skipped,
                )
            )
        return tasks

    def prefilter(
        self, tasks: list[ReportDisclosureTask], context: AkShareExecutionContext
    ) -> list[ReportDisclosureTask]:
        return list(tasks)

    def fetch(self, task: ReportDisclosureTask, context: AkShareExecutionContext) -> FetchResult:
        now = datetime.now()
        if task.skipped:
            return FetchResult(task=task, started_at=now, ended_at=now, skipped=True)
        try:
            response = context.client.fetch_report_disclosure(market=task.market, period=task.period)
            return FetchResult(task=task, started_at=now, ended_at=datetime.now(), response=response)
        except Exception as exc:
            return FetchResult(
                task=task,
                started_at=now,
                ended_at=datetime.now(),
                error=exc,
                error_stack=error_stack(exc),
            )

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]:
        task = result.task
        if result.skipped:
            return []
        if result.error is not None:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=result.ended_at,
                error_stack=result.error_stack,
            )
            return [rows.run_row]
        try:
            if result.response is None:
                raise RuntimeError("stock_report_disclosure returned no response")
            output_path = context.store.write_dataset(
                task.dataset,
                result.response.data,
                {"report_period": task.period},
            ).primary_path
            rows = context.lifecycle.record_success(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                row_count=len(result.response.data),
                output_path=output_path,
                last_success_date=task.period_end_date,
            )
            return [rows.run_row]
        except Exception as exc:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=error_stack(exc),
            )
            return [rows.run_row]

    def record_skip(
        self,
        task: ReportDisclosureTask,
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

    def progress_row(self, task: ReportDisclosureTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.market, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(workers=1)

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare report disclosure update started market={} mode={} force={} planned_tasks={} processing_tasks={}",
            request.market or DEFAULT_MARKET,
            request.mode,
            request.force,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: ReportDisclosureTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare report disclosure progress {}/{} market={} period={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            task.market,
            task.period,
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare report disclosure update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )


def _resolve_periods(request: AkShareUpdateRequest, today: date, full_start_year: object) -> list[str]:
    explicit_periods = _normalize_explicit_periods(request.period)
    if explicit_periods:
        return explicit_periods
    if request.mode == "full":
        start_year = int(str(full_start_year))
        return _periods_between(start_year, today)
    if request.mode != "partial":
        raise ValueError(f"Unsupported AkShare report disclosure update mode: {request.mode}")
    return _periods_between(today.year - 2, today)[-4:]


def _normalize_explicit_periods(period: tuple[str, ...] | list[str] | str | None) -> list[str]:
    if period is None:
        return []
    values = [period] if isinstance(period, str) else list(period)
    periods: list[str] = []
    for item in values:
        value = str(item).strip()
        if not value:
            continue
        report_period_end_date(value)
        periods.append(value)
    return list(dict.fromkeys(periods))


def _periods_between(start_year: int, today: date) -> list[str]:
    periods: list[str] = []
    for year in range(start_year, today.year + 1):
        for suffix in REPORT_PERIOD_SUFFIXES:
            period = f"{year}{suffix}"
            if report_period_end_date(period) <= today:
                periods.append(period)
    return periods


def _request_today(request: AkShareUpdateRequest) -> date:
    now = (request.now or datetime.now)()
    if isinstance(now, datetime):
        return now.date()
    return now


def _task_ref(task: ReportDisclosureTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE_REPORT_DISCLOSURE,
        task.dataset,
        task.market,
        task.period_end_date,
        task.period_end_date,
        task.output_path,
    )
