"""AkShare Sina financial report update module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.lifecycle import LifecycleTaskRef
from src.sources.akshare.client import AkShareResponse, normalize_akshare_code
from src.sources.akshare.cninfo.adapters.report_disclosure import report_period_end_date
from src.sources.akshare.pipeline.common import error_stack
from src.sources.akshare.pipeline.execution import (
    AkShareExecutionContext,
    AkShareUpdateRequest,
    ConcurrencyPolicy,
    FetchResult,
)
from src.sources.akshare.pipeline.universe import resolve_akshare_universe_codes
from src.storage.dataset_catalog import AKSHARE_FINANCIAL_REPORT_SINA_DATASET
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger

PIPELINE_UPDATE_AKSHARE_FINANCIAL_REPORT = "update_akshare_financial_report_sina"
FINANCIAL_REPORT_START_DATE = "1900-01-01"
FINANCIAL_REPORT_END_DATE = "2100-01-01"
PENDING_FILE = Path("data/metadata/akshare_financial_report_pending.parquet")
STATE_FILE = Path("data/metadata/akshare_financial_report_incremental_state.parquet")
PENDING_COLUMNS = [
    "code",
    "report_period",
    "period_end_date",
    "trigger_date",
    "trigger_priority",
    "trigger_source",
    "status",
    "updated_at",
]
DISCLOSURE_DATASETS = (
    ("akshare_cn_stock_report_disclosure", "stock_report_disclosure"),
    ("akshare_cn_stock_yysj_em", "stock_yysj_em"),
)


@dataclass(frozen=True)
class FinancialReportTask:
    dataset: str
    code: str
    start_date: str
    end_date: str
    output_path: Path
    mode: str
    target_periods: tuple[str, ...] = ()
    trigger_rows: tuple[dict[str, object], ...] = ()
    skip_existing: bool = False


class FinancialReportSinaModule:
    target = "financial_report"

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[FinancialReportTask]:
        if request.mode == "full":
            tasks = _plan_full_tasks(context.store, request)
        elif request.mode == "incremental":
            tasks = _plan_incremental_tasks(context.config, context.store, request)
        else:
            raise ValueError(f"Unsupported AkShare financial_report update mode: {request.mode}")
        if request.max_tasks is not None:
            tasks = tasks[: max(int(request.max_tasks), 0)]
        return tasks

    def prefilter(
        self,
        tasks: list[FinancialReportTask],
        context: AkShareExecutionContext,
    ) -> list[FinancialReportTask]:
        remaining: list[FinancialReportTask] = []
        for task in tasks:
            if task.mode != "full":
                remaining.append(task)
                continue
            if task.skip_existing and task.output_path.exists():
                continue
            if (
                context.checkpoint_lookup is not None
                and task.output_path.exists()
                and (
                    context.checkpoint_lookup.pipeline_checkpoint_succeeded(
                        PIPELINE_UPDATE_AKSHARE_FINANCIAL_REPORT,
                        task.dataset,
                        task.code,
                        task.start_date,
                        task.end_date,
                        task.output_path,
                    )
                    or task.output_path.exists()
                )
            ):
                continue
            remaining.append(task)
        return remaining

    def fetch(self, task: FinancialReportTask, context: AkShareExecutionContext) -> FetchResult:
        started_at = datetime.now()
        try:
            response = context.client.fetch_financial_report_sina(task.code)
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
            return [rows.run_row]
        try:
            if result.response is None:
                raise RuntimeError("stock_financial_report_sina returned no response")
            output_path, row_count, last_success_date, missing = _write_task_data(
                context.store,
                task,
                result.response,
            )
            if task.mode == "incremental":
                _update_pending_after_success(context.store.root, task, missing)
            rows = context.lifecycle.record_success(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                row_count=row_count,
                output_path=output_path,
                last_success_date=last_success_date,
            )
            return [rows.run_row]
        except Exception as exc:
            rows = context.lifecycle.record_failure(
                _task_ref(task),
                started_at=result.started_at,
                ended_at=datetime.now(),
                error_stack=error_stack(exc),
            )
            logger.exception("AkShare financial report write failed code={}", task.code)
            return [rows.run_row]

    def record_skip(
        self,
        task: FinancialReportTask,
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

    def progress_row(self, task: FinancialReportTask, rows: list[dict[str, object]]) -> dict[str, object]:
        if rows:
            return rows[-1]
        return {"dataset": task.dataset, "code": task.code, "status": "skipped_checkpoint", "row_count": 0}

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(
            workers=_resolve_workers(context.config, request.workers),
            thread_name_prefix="update-akshare-financial-report",
            stop_on_circuit_open=True,
        )

    def log_started(self, request: AkShareUpdateRequest, planned: int, processing: int, workers: int) -> None:
        logger.info(
            "AkShare financial report update started mode={} force={} workers={} planned_tasks={} processing_tasks={}",
            request.mode,
            request.force,
            workers,
            planned,
            processing,
        )

    def log_progress(self, progress: Any, task: FinancialReportTask, row: dict[str, object]) -> None:
        logger.info(
            "AkShare financial report progress {}/{} code={} dataset={} status={} rows={}",
            progress.processed,
            progress.total,
            task.code,
            row.get("dataset", task.dataset),
            row.get("status", "unknown"),
            row.get("row_count", 0),
        )

    def log_completed(self, progress: Any) -> None:
        logger.info(
            "AkShare financial report update completed processed={} success={} failed={} skipped={}",
            progress.processed,
            progress.success,
            progress.failed,
            progress.skipped,
        )

    def after_completed(self, request: AkShareUpdateRequest, context: AkShareExecutionContext, progress: Any) -> None:
        if request.mode == "incremental" and progress.failed == 0:
            write_financial_report_incremental_state(
                context.store.root,
                _effective_disclosure_date(context.config, request),
            )

    def log_circuit_open(self, attempted_tasks: int) -> None:
        logger.warning(
            "AkShare financial report circuit opened; stopping new submissions after {} attempted tasks",
            attempted_tasks,
        )


def _plan_full_tasks(store: ParquetStore, request: AkShareUpdateRequest) -> list[FinancialReportTask]:
    codes = resolve_akshare_universe_codes(
        store,
        code=request.code,
        include_delisted=True,
        context="akshare_cn_stock_financial_report_sina",
    )
    return [
        FinancialReportTask(
            dataset=AKSHARE_FINANCIAL_REPORT_SINA_DATASET.name,
            code=stock_code,
            start_date=FINANCIAL_REPORT_START_DATE,
            end_date=FINANCIAL_REPORT_END_DATE,
            output_path=store.dataset_path(AKSHARE_FINANCIAL_REPORT_SINA_DATASET.name, {"code": stock_code}),
            mode="full",
            skip_existing=request.resume and not request.force,
        )
        for stock_code in codes
    ]


def _plan_incremental_tasks(
    config: ConfigManager,
    store: ParquetStore,
    request: AkShareUpdateRequest,
) -> list[FinancialReportTask]:
    effective_date = _effective_disclosure_date(config, request)
    trigger_start = _incremental_trigger_start_date(config, store.root, effective_date)
    candidates = _local_disclosure_candidates(store, effective_date, trigger_start)
    pending = read_financial_report_pending(store.root)
    if not pending.empty:
        pending_rows = _active_pending_rows(config, pending, effective_date)
        _write_financial_report_pending(store.root, pending_rows)
        candidates = _merge_candidate_rows([*candidates, *pending_rows])
    if request.code:
        requested = set(_normalize_codes(request.code))
        candidates = [row for row in candidates if row["code"] in requested]
    candidates = _filter_uncovered_candidates(store, candidates)
    by_code: dict[str, list[dict[str, object]]] = {}
    for row in candidates:
        by_code.setdefault(str(row["code"]), []).append(row)
    tasks: list[FinancialReportTask] = []
    for stock_code, rows in sorted(by_code.items()):
        target_periods = tuple(sorted({str(row["report_period"]) for row in rows}))
        period_ends = [str(row["period_end_date"]) for row in rows]
        tasks.append(
            FinancialReportTask(
                dataset=AKSHARE_FINANCIAL_REPORT_SINA_DATASET.name,
                code=stock_code,
                start_date=min(period_ends),
                end_date=max(str(row["trigger_date"]) for row in rows),
                output_path=store.dataset_path(AKSHARE_FINANCIAL_REPORT_SINA_DATASET.name, {"code": stock_code}),
                mode="incremental",
                target_periods=target_periods,
                trigger_rows=tuple(rows),
            )
        )
    return tasks


def _local_disclosure_candidates(
    store: ParquetStore,
    effective_date: date,
    trigger_start: date | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for dataset, source in DISCLOSURE_DATASETS:
        for df in _read_all_report_period_partitions(store, dataset):
            if df.empty:
                continue
            for record in _disclosure_candidate_records(df, effective_date, trigger_start):
                code = normalize_akshare_code(record["code"])
                report_period = str(record["report_period"])
                trigger_date = cast(date, record["trigger_date"])
                rows.append(
                    {
                        "code": code,
                        "report_period": report_period,
                        "period_end_date": report_period_end_date(report_period).isoformat(),
                        "trigger_date": trigger_date.isoformat(),
                        "trigger_priority": record["trigger_priority"],
                        "trigger_source": source,
                        "status": "pending",
                        "updated_at": datetime.now().isoformat(timespec="milliseconds"),
                    }
                )
    return _merge_candidate_rows(rows)


def _disclosure_candidate_records(
    df: pd.DataFrame,
    effective_date: date,
    trigger_start: date | None,
) -> list[dict[str, object]]:
    work = df.copy()
    trigger_date = _datetime_series(work, "actual_disclosure_date")
    priority = pd.Series(3, index=work.index, dtype="int64")

    changed = pd.Series(pd.NaT, index=work.index, dtype="datetime64[ns]")
    for column in ("third_changed_date", "second_changed_date", "first_changed_date"):
        values = _datetime_series(work, column)
        changed = changed.fillna(values)
    changed_mask = trigger_date.isna() & changed.notna()
    trigger_date = trigger_date.mask(changed_mask, changed)
    priority = priority.mask(changed_mask, 2)

    scheduled = _datetime_series(work, "first_scheduled_date")
    scheduled_mask = trigger_date.isna() & scheduled.notna()
    trigger_date = trigger_date.mask(scheduled_mask, scheduled)
    priority = priority.mask(scheduled_mask, 1)

    work["_trigger_date"] = trigger_date
    work["_trigger_priority"] = priority
    mask = work["_trigger_date"].notna() & (work["_trigger_date"] <= pd.Timestamp(effective_date))
    if trigger_start is not None:
        mask &= work["_trigger_date"] >= pd.Timestamp(trigger_start)

    records: list[dict[str, object]] = []
    for _, record in work.loc[mask, ["code", "report_period", "_trigger_date", "_trigger_priority"]].iterrows():
        records.append(
            {
                "code": record["code"],
                "report_period": record["report_period"],
                "trigger_date": record["_trigger_date"].date(),
                "trigger_priority": int(record["_trigger_priority"]),
            }
        )
    return records


def _datetime_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    return pd.to_datetime(df[column], errors="coerce")


def _read_all_report_period_partitions(store: ParquetStore, dataset: str) -> list[pd.DataFrame]:
    dataset_dir = store.parquet_dir / dataset
    if not dataset_dir.exists():
        return []
    frames: list[pd.DataFrame] = []
    for partition in sorted(dataset_dir.glob("report_period=*")):
        if not partition.is_dir() or not (partition / "data.parquet").exists():
            continue
        period = partition.name.split("=", 1)[1]
        frames.append(store.read_dataset(dataset, {"report_period": period}))
    return frames


def _trigger_from_disclosure_row(record: pd.Series) -> tuple[date, int] | None:
    actual = _date_or_none(record.get("actual_disclosure_date"))
    if actual is not None:
        return actual, 3
    for column in ("third_changed_date", "second_changed_date", "first_changed_date"):
        changed = _date_or_none(record.get(column))
        if changed is not None:
            return changed, 2
    scheduled = _date_or_none(record.get("first_scheduled_date"))
    if scheduled is not None:
        return scheduled, 1
    return None


def _merge_candidate_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    selected: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (str(row["code"]), str(row["report_period"]))
        current = selected.get(key)
        if current is None or _candidate_sort_key(row) > _candidate_sort_key(current):
            selected[key] = dict(row)
    return sorted(selected.values(), key=lambda item: (str(item["code"]), str(item["report_period"])))


def _candidate_sort_key(row: dict[str, object]) -> tuple[int, str]:
    return int(str(row.get("trigger_priority") or 0)), str(row.get("trigger_date") or "")


def _write_task_data(
    store: ParquetStore,
    task: FinancialReportTask,
    response: AkShareResponse,
) -> tuple[Path, int, str | None, set[str]]:
    df = response.data
    target_period_ends = {report_period_end_date(period).isoformat() for period in task.target_periods}
    present_period_ends = _present_period_end_dates(df)
    missing = {
        period
        for period in task.target_periods
        if report_period_end_date(period).isoformat() not in present_period_ends
    }
    if not df.empty:
        output_path = store.write_dataset(task.dataset, df, {"code": task.code}).primary_path
    else:
        output_path = task.output_path
        if not output_path.exists():
            store.write_dataset(task.dataset, df, {"code": task.code})
    last_success_date = max(present_period_ends) if present_period_ends else None
    if task.mode == "incremental" and target_period_ends and target_period_ends.isdisjoint(present_period_ends):
        logger.info(
            "AkShare financial report target periods still missing code={} periods={}",
            task.code,
            sorted(task.target_periods),
        )
    return output_path, len(df), last_success_date, missing


def _present_period_end_dates(df: pd.DataFrame) -> set[str]:
    if df.empty or "period_end_date" not in df.columns:
        return set()
    values = pd.to_datetime(df["period_end_date"], errors="coerce").dropna()
    return {item.date().isoformat() for item in values}


def _update_pending_after_success(root: Path, task: FinancialReportTask, missing_periods: set[str]) -> None:
    existing = read_financial_report_pending(root)
    existing_rows = cast(list[dict[str, object]], existing.to_dict("records")) if not existing.empty else []
    task_keys = {(task.code, period) for period in task.target_periods}
    remaining = [
        row
        for row in existing_rows
        if (str(row["code"]), str(row["report_period"])) not in task_keys
        or str(row["report_period"]) in missing_periods
    ]
    for row in task.trigger_rows:
        if str(row["report_period"]) in missing_periods:
            remaining.append(dict(row))
    _write_financial_report_pending(root, _merge_candidate_rows(remaining))


def read_financial_report_pending(root: Path | str | None = None) -> pd.DataFrame:
    base = Path(root or ".").resolve()
    path = base / PENDING_FILE
    if not path.exists():
        return pd.DataFrame(columns=PENDING_COLUMNS)
    df = pd.read_parquet(path)
    for column in PENDING_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    return df[PENDING_COLUMNS].astype("string").reset_index(drop=True)


def _active_pending_rows(config: ConfigManager, pending: pd.DataFrame, effective_date: date) -> list[dict[str, object]]:
    if pending.empty:
        return []
    retention_days = int(config.get("datasets.akshare_cn_stock_financial_report_sina.pending_retention_days", 30))
    oldest_trigger = effective_date - timedelta(days=max(retention_days, 0))
    work = pending.copy()
    trigger_dates = pd.to_datetime(work["trigger_date"], errors="coerce")
    mask = trigger_dates.notna()
    mask &= trigger_dates.dt.date >= oldest_trigger
    mask &= trigger_dates.dt.date <= effective_date
    return work.loc[mask, PENDING_COLUMNS].to_dict("records")


def _write_financial_report_pending(root: Path, rows: list[dict[str, object]]) -> None:
    path = root / PENDING_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=PENDING_COLUMNS)
    if df.empty:
        if path.exists():
            path.unlink()
        return
    df[PENDING_COLUMNS].astype("string").to_parquet(path, index=False)


def _filter_uncovered_candidates(store: ParquetStore, candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    covered_by_code: dict[str, set[str]] = {}
    filtered: list[dict[str, object]] = []
    for row in candidates:
        code = str(row["code"])
        covered = covered_by_code.get(code)
        if covered is None:
            covered = _covered_financial_report_periods(store, code)
            covered_by_code[code] = covered
        if str(row["period_end_date"]) in covered:
            continue
        filtered.append(row)
    return filtered


def _covered_financial_report_periods(store: ParquetStore, code: str) -> set[str]:
    path = store.dataset_path(AKSHARE_FINANCIAL_REPORT_SINA_DATASET.name, {"code": code})
    if not path.exists():
        return set()
    try:
        df = store.read_dataset(AKSHARE_FINANCIAL_REPORT_SINA_DATASET.name, {"code": code})
    except FileNotFoundError:
        return set()
    return _present_period_end_dates(df)


def _incremental_trigger_start_date(config: ConfigManager, root: Path, effective_date: date) -> date:
    lookback_days = int(config.get("datasets.akshare_cn_stock_financial_report_sina.incremental_lookback_days", 14))
    lookback_start = effective_date - timedelta(days=max(lookback_days, 0))
    last_effective = _read_incremental_state(root)
    if last_effective is None:
        return lookback_start
    return min(lookback_start, last_effective)


def _read_incremental_state(root: Path) -> date | None:
    path = root / STATE_FILE
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty or "last_effective_date" not in df.columns:
        return None
    values = pd.to_datetime(df["last_effective_date"], errors="coerce").dropna()
    if values.empty:
        return None
    return values.max().date()


def write_financial_report_incremental_state(root: Path, effective_date: date) -> None:
    path = root / STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "last_effective_date": effective_date.isoformat(),
                "updated_at": datetime.now().isoformat(timespec="milliseconds"),
            }
        ]
    ).to_parquet(path, index=False)


def _effective_disclosure_date(config: ConfigManager, request: AkShareUpdateRequest) -> date:
    now = (request.now or datetime.now)()
    if not isinstance(now, datetime):
        return now
    cutoff = _close_after_time(config)
    effective = now.date()
    if now.time() >= cutoff:
        effective += timedelta(days=1)
    return effective


def _close_after_time(config: ConfigManager) -> time:
    raw = str(config.get("datasets.akshare_cn_stock_financial_report_sina.close_after_time", "18:00"))
    hour, minute = raw.split(":", 1)
    return time(int(hour), int(minute))


def _date_or_none(value: object) -> date | None:
    text = str(value).strip()
    if pd.isna(text) or text == "":
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _normalize_codes(code: tuple[str, ...] | list[str] | str) -> list[str]:
    values = [code] if isinstance(code, str) else list(code)
    return [normalize_akshare_code(item) for item in values]


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = (
        workers
        if workers is not None
        else config.get("datasets.akshare_cn_stock_financial_report_sina.workers", config.get("api.akshare.workers", 3))
    )
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid AkShare financial_report workers value: {raw_workers!r}") from exc


def _task_ref(task: FinancialReportTask) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_AKSHARE_FINANCIAL_REPORT,
        task.dataset,
        task.code,
        task.start_date,
        task.end_date,
        task.output_path,
    )
