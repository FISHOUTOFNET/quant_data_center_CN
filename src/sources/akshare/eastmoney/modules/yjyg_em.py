"""AkShare Eastmoney earnings forecast update module."""

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
from src.storage.dataset_catalog import AKSHARE_YJYG_EM_DATASET
from src.utils.logging import logger

PIPELINE_UPDATE_AKSHARE_YJYG_EM = "update_akshare_yjyg_em"
YJYG_EM_MIN_PERIOD_END_DATE = date(2003, 6, 30)
QUARTER_ENDS = ((3, 31), (6, 30), (9, 30), (12, 31))


@dataclass(frozen=True)
class YjygEmTask:
    dataset: str
    period: str
    period_end_date: str
    output_path: Path
    skipped: bool = False


class YjygEmModule:
    target = "yjyg_em"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[YjygEmTask]:
        today = _request_today(request)
        full_start_period = str(
            context.config.get(
                "datasets.akshare_cn_stock_yjyg_em.full_start_period",
                "20030630",
            )
        ).strip()
        if not full_start_period:
            raise ValueError("akshare_cn_stock_yjyg_em.full_start_period must not be empty")

        rolling_period_count_raw = context.config.get(
            "datasets.akshare_cn_stock_yjyg_em.rolling_period_count",
            5,
        )
        try:
            rolling_period_count = int(rolling_period_count_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid akshare_cn_stock_yjyg_em rolling_period_count: {rolling_period_count_raw!r}"
            ) from exc
        if rolling_period_count < 1:
            raise ValueError("akshare_cn_stock_yjyg_em.rolling_period_count must be >= 1")

        periods = _resolve_periods(
            request,
            today,
            full_start_period,
            rolling_period_count,
        )
        tasks: list[YjygEmTask] = []
        for period in periods:
            period_end = report_period_end_date(period).isoformat()
            output_path = context.store.dataset_path(
                AKSHARE_YJYG_EM_DATASET.name,
                {"report_period": period},
            )
            skipped = should_skip_checkpoint(
                context.store,
                PIPELINE_UPDATE_AKSHARE_YJYG_EM,
                AKSHARE_YJYG_EM_DATASET.name,
                period,
                period_end,
                period_end,
                output_path,
                request.resume,
                request.force,
                context.checkpoint_lookup,
            )
            tasks.append(
                YjygEmTask(
                    dataset=AKSHARE_YJYG_EM_DATASET.name,
                    period=period,
                    period_end_date=period_end,
                    output_path=output_path,
                    skipped=skipped,
                )
            )
        if request.max_tasks is not None:
            tasks = tasks[: max(int(request.max_tasks), 0)]
        return tasks

    def prefilter(self, tasks: list[YjygEmTask], context: AkShareExecutionContext) -> list[YjygEmTask]:
        return list(tasks)

    def fetch(self, task: YjygEmTask, context: AkShareExecutionContext) -> FetchResult:
        now = datetime.now()
        if task.skipped:
            return FetchResult(task=task, started_at=now, ended_at=now, skipped=True)
        try:
            response = context.client.fetch_yjyg_em(period=task.period)
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
                raise RuntimeError("stock_yjyg_em returned no response")
            output_path = context.store.write_dataset(
                task.dataset,
                result.response.data,
                {"report_period": task.period},
                mode="replace",
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
        task: YjygEmTask,
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

    def progress_row(self, task: YjygEmTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.period, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(workers=1)

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare yjyg_em update started mode={} force={} planned_tasks={} processing_tasks={}",
            request.mode,
            request.force,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: YjygEmTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare yjyg_em progress {}/{} period={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            task.period,
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare yjyg_em update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )


def period_end_date_to_report_period(period_end_date: date) -> str:
    if not _is_quarter_end(period_end_date):
        raise ValueError(f"Not a quarter-end date: {period_end_date}")
    suffix = {
        (3, 31): "一季",
        (6, 30): "半年报",
        (9, 30): "三季",
        (12, 31): "年报",
    }[(period_end_date.month, period_end_date.day)]
    return f"{period_end_date.year}{suffix}"


def parse_quarter_end_yyyymmdd(value: str) -> date:
    text = str(value).strip()
    if not text or len(text) != 8 or not text.isdigit():
        raise ValueError(f"Invalid quarter-end date: {value!r}")
    parsed = datetime.strptime(text, "%Y%m%d").date()
    if not _is_quarter_end(parsed):
        raise ValueError(f"Invalid quarter-end date: {value!r}")
    return parsed


def quarter_end_sequence(start: date, end: date) -> list[date]:
    if not _is_quarter_end(start):
        raise ValueError(f"Not a quarter-end date: {start}")
    if not _is_quarter_end(end):
        raise ValueError(f"Not a quarter-end date: {end}")
    if start > end:
        return []
    periods: list[date] = []
    current = start
    while current <= end:
        periods.append(current)
        current = _next_quarter_end(current)
    return periods


def previous_report_period_end(today: date) -> date:
    candidates = [date(today.year, month, day) for month, day in QUARTER_ENDS]
    previous = [candidate for candidate in candidates if candidate <= today]
    if previous:
        return previous[-1]
    return date(today.year - 1, 12, 31)


def rolling_forecast_periods(today: date, count: int = 5) -> list[str]:
    if count < 1:
        raise ValueError("rolling forecast period count must be >= 1")
    current = previous_report_period_end(today)
    periods: list[str] = []
    for _ in range(count):
        periods.append(period_end_date_to_report_period(current))
        current = _next_quarter_end(current)
    return periods


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


def _resolve_periods(
    request: AkShareUpdateRequest,
    today: date,
    full_start_period: str,
    rolling_period_count: int,
) -> list[str]:
    explicit_periods = _normalize_explicit_periods(request.period)
    if explicit_periods:
        return explicit_periods
    if request.mode == "full":
        start = parse_quarter_end_yyyymmdd(full_start_period)
        if start < YJYG_EM_MIN_PERIOD_END_DATE:
            raise ValueError("akshare_cn_stock_yjyg_em.full_start_period must be >= 20030630")
        end = report_period_end_date(rolling_forecast_periods(today, rolling_period_count)[-1])
        return [period_end_date_to_report_period(item) for item in quarter_end_sequence(start, end)]
    if request.mode in {"partial", "incremental"}:
        return rolling_forecast_periods(today, rolling_period_count)
    raise ValueError(f"Unsupported AkShare yjyg_em update mode: {request.mode}")


def _request_today(request: AkShareUpdateRequest) -> date:
    now = (request.now or datetime.now)()
    if isinstance(now, datetime):
        return now.date()
    return now


def _task_ref(task: YjygEmTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE_YJYG_EM,
        task.dataset,
        task.period,
        task.period_end_date,
        task.period_end_date,
        task.output_path,
    )


def _is_quarter_end(value: date) -> bool:
    return (value.month, value.day) in QUARTER_ENDS


def _next_quarter_end(value: date) -> date:
    if not _is_quarter_end(value):
        raise ValueError(f"Not a quarter-end date: {value}")
    sequence = list(QUARTER_ENDS)
    index = sequence.index((value.month, value.day))
    if index == len(sequence) - 1:
        return date(value.year + 1, 3, 31)
    month, day = sequence[index + 1]
    return date(value.year, month, day)
