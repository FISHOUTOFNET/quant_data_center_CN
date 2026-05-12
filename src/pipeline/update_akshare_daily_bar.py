"""Manual AkShare AkShare daily bar full and incremental pipeline."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.api.akshare_client import AkShareClient, AkShareResponse
from src.pipeline.akshare_common import (
    PIPELINE_UPDATE_AKSHARE_DAILY_BAR,
    error_stack,
    failed_metadata,
    persist_metadata,
    success_metadata,
)
from src.pipeline.akshare_universe import resolve_akshare_universe_codes
from src.pipeline.common import PipelineCheckpointLookup, default_candidate_date
from src.pipeline.dry_run import apply_limit, dry_run_record
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
class AkShareDailyBarTask:
    code: str
    adjustment: str
    dataset: str
    start_date: str
    end_date: str
    output_path: Path
    api_start_date: str
    api_end_date: str


@dataclass(frozen=True)
class _DailyBarFetchResult:
    task: AkShareDailyBarTask
    started_at: datetime
    ended_at: datetime
    response: AkShareResponse | None = None
    error: Exception | None = None
    error_stack: str = ""


def _latest_calendar_date_from_duckdb(conn: Any) -> str | None:
    row = conn.execute("SELECT max(calendar_date) FROM v_baostock_cn_trading_calendar").fetchone()
    if row is None or row[0] is None:
        return None
    latest_date = pd.to_datetime(row[0], errors="coerce")
    if pd.isna(latest_date):
        return None
    return latest_date.date().isoformat()


def _duckdb_skippable_daily_bar_task_keys(
    duck_store: DuckDBStore,
    tasks: list[AkShareDailyBarTask],
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
                view_name = f"v_{dataset}"
                rows = conn.execute(
                    f"""
                    SELECT latest.code
                    FROM (
                        SELECT
                            h.code,
                            h.date AS latest_date,
                            h.source_endpoint,
                            row_number() OVER (PARTITION BY h.code ORDER BY h.date DESC) AS rn
                        FROM {view_name} AS h
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


def _prefilter_daily_bar_tasks(
    tasks: list[AkShareDailyBarTask],
    store: ParquetStore,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[AkShareDailyBarTask]:
    if checkpoint_lookup is None or not tasks:
        return list(tasks)

    duck_store = DuckDBStore(root=store.root)
    duck_store.build_views(cleanup_tmp_files=False)
    skippable_task_keys = _duckdb_skippable_daily_bar_task_keys(duck_store, tasks)
    if skippable_task_keys is None:
        logger.warning("Calendar is empty, cannot prefilter daily bar tasks")
        return list(tasks)

    remaining_tasks: list[AkShareDailyBarTask] = []
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


def update_akshare_daily_bar(
    mode: str,
    adjustment: str = "all",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | date | None = None,
    end: str | date | None = None,
    max_codes: int | None = None,
    max_tasks: int | None = None,
    workers: int | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager], Any] | None = None,
    dry_run: bool = False,
) -> list[dict[str, object]]:
    """Run full initialization or manual incremental repair for AkShare daily bars."""

    mode = str(mode).strip().lower()
    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    tasks = plan_akshare_daily_bar_tasks(
        config=config,
        store=store,
        mode=mode,
        adjustment=adjustment,
        code=code,
        start=start,
        end=end,
        max_codes=max_codes,
        max_tasks=max_tasks,
    )
    if not tasks and not dry_run:
        return []
    if dry_run:
        return [
            dry_run_record(
                task.dataset,
                task.code,
                task.start_date,
                task.end_date,
                task.output_path,
                operation="write_akshare_daily_bar",
                adjustment=task.adjustment,
            )
            for task in tasks
        ]

    store.ensure_layout()
    checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
    ak_client = client or (
        client_factory(config)
        if client_factory is not None
        else AkShareClient(config=config)
    )
    selected_tasks = _prefilter_daily_bar_tasks(tasks, store, checkpoint_lookup)
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]] = []
    records: list[dict[str, object]] = []

    resolved_workers = _resolve_workers(config, workers)
    progress_total = len(selected_tasks)
    progress_processed = 0
    progress_success = 0
    progress_failed = 0
    logger.info(
        "AkShare daily bar update started mode={} adjustment={} force={} workers={} planned_tasks={} processing_tasks={}",
        mode,
        adjustment,
        force,
        resolved_workers,
        len(tasks),
        progress_total,
    )

    def daily_bar_progress_row(
        task: AkShareDailyBarTask,
        row: dict[str, object] | None,
        metadata_start_count: int,
    ) -> dict[str, object]:
        if row is not None:
            return row
        if len(metadata) > metadata_start_count:
            return metadata[-1][0]
        return {
            "dataset": task.dataset,
            "code": task.code,
            "status": "failed",
            "row_count": 0,
        }

    def log_daily_bar_progress(task: AkShareDailyBarTask, row: dict[str, object]) -> None:
        nonlocal progress_processed, progress_success, progress_failed
        progress_processed += 1
        status = str(row.get("status", "unknown"))
        if status == "success":
            progress_success += 1
        elif status == "failed":
            progress_failed += 1
        logger.info(
            "AkShare daily bar progress {}/{} code={} adjustment={} dataset={} status={} rows={}",
            progress_processed,
            progress_total,
            task.code,
            task.adjustment,
            row.get("dataset", task.dataset),
            status,
            row.get("row_count", 0),
        )

    if resolved_workers == 1:
        for task in selected_tasks:
            metadata_start_count = len(metadata)
            result = _fetch_daily_bar_task(ak_client, task)
            row = _record_daily_bar_result(store, result, mode, metadata)
            if row is not None:
                records.append(row)
            log_daily_bar_progress(task, daily_bar_progress_row(task, row, metadata_start_count))
    else:
        with ThreadPoolExecutor(max_workers=resolved_workers, thread_name_prefix="update-akshare-daily-bar") as executor:
            futures: dict[Future[_DailyBarFetchResult], AkShareDailyBarTask] = {
                executor.submit(_fetch_daily_bar_task, ak_client, task): task for task in selected_tasks
            }
            for future in as_completed(futures):
                metadata_start_count = len(metadata)
                try:
                    result = future.result()
                except Exception as exc:
                    task = futures[future]
                    result = _DailyBarFetchResult(
                        task=task,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                        error=exc,
                        error_stack=traceback.format_exc(),
                    )
                row = _record_daily_bar_result(store, result, mode, metadata)
                if row is not None:
                    records.append(row)
                log_daily_bar_progress(result.task, daily_bar_progress_row(result.task, row, metadata_start_count))

    persisted_records = persist_metadata(store, metadata)
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=progress_success > 0)
    logger.info(
        "AkShare daily bar update completed processed={} success={} failed={}",
        progress_processed,
        progress_success,
        progress_failed,
    )
    return persisted_records if persisted_records else records


def plan_akshare_daily_bar_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    adjustment: str = "unadjusted",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | date | None = None,
    end: str | date | None = None,
    max_codes: int | None = None,
    max_tasks: int | None = None,
) -> list[AkShareDailyBarTask]:
    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in {"full", "incremental"}:
        raise ValueError(f"Unsupported AkShare daily bar mode: {mode}")
    api_end_date = _date_iso(end, default_candidate_date(config))
    if normalized_mode == "full":
        api_start_date = _date_iso(start, str(config.get("datasets.akshare_cn_stock_daily_bar.full_start", "1990-01-01")))
    else:
        if start is None:
            raise ValueError("AkShare daily bar incremental mode requires --start")
        api_start_date = _date_iso(start, api_end_date)
    if api_start_date > api_end_date:
        raise ValueError(f"AkShare daily bar start date {api_start_date} is after end date {api_end_date}")

    codes = apply_limit(
        _resolve_daily_bar_codes(store, code, include_delisted=normalized_mode == "full"),
        max_codes,
        "max_codes",
    )
    adjustments = _resolve_adjustments(adjustment)
    tasks = []
    for daily_bar_adjustment in adjustments:
        for stock_code in codes:
            tasks.append(
                AkShareDailyBarTask(
                    code=stock_code,
                    adjustment=daily_bar_adjustment,
                    dataset=akshare_daily_bar_dataset_id(daily_bar_adjustment),
                    start_date=api_start_date,
                    end_date=api_end_date,
                    output_path=store.akshare_daily_bar_path(daily_bar_adjustment, stock_code),
                    api_start_date=api_start_date,
                    api_end_date=api_end_date,
                )
            )
    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def _fetch_daily_bar_task(client: Any, task: AkShareDailyBarTask) -> _DailyBarFetchResult:
    started_at = datetime.now()
    try:
        response = client.fetch_daily_bars(
            symbol=task.code,
            start_date=task.api_start_date,
            end_date=task.api_end_date,
            adjustment=task.adjustment,
        )
        return _DailyBarFetchResult(task=task, started_at=started_at, ended_at=datetime.now(), response=response)
    except Exception as exc:
        return _DailyBarFetchResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            error=exc,
            error_stack=error_stack(exc),
        )


def _record_daily_bar_result(
    store: ParquetStore,
    result: _DailyBarFetchResult,
    mode: str,
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]],
) -> dict[str, object] | None:
    task = result.task
    if result.error is not None:
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_DAILY_BAR,
                task.dataset,
                task.code,
                task.start_date,
                task.end_date,
                result.started_at,
                result.ended_at,
                result.error_stack,
                task.output_path,
            )
        )
        logger.error("AkShare daily bar task failed code={} adjustment={}: {}", task.code, task.adjustment, result.error)
        return None

    response = result.response
    if response is None:
        return _record_daily_bar_result(
            store,
            _DailyBarFetchResult(
                task=task,
                started_at=result.started_at,
                ended_at=datetime.now(),
                error=RuntimeError("stock_zh_a_hist returned no response"),
                error_stack="stock_zh_a_hist returned no response",
            ),
            mode,
            metadata,
        )

    try:
        if mode == "full":
            output_path = store.write_akshare_daily_bars(task.adjustment, task.code, response.data)
        else:
            output_path = store.upsert_akshare_daily_bars(task.adjustment, task.code, response.data)
        ended_at = datetime.now()
        row_metadata = success_metadata(
            PIPELINE_UPDATE_AKSHARE_DAILY_BAR,
            task.dataset,
            task.code,
            task.start_date,
            task.end_date,
            result.started_at,
            ended_at,
            len(response.data),
            output_path,
        )
        metadata.append(row_metadata)
        return row_metadata[0]
    except Exception as exc:
        ended_at = datetime.now()
        stack = error_stack(exc)
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_DAILY_BAR,
                task.dataset,
                task.code,
                task.start_date,
                task.end_date,
                result.started_at,
                ended_at,
                stack,
                task.output_path,
            )
        )
        logger.exception("AkShare daily bar write failed code={} adjustment={}", task.code, task.adjustment)
        return None


def _resolve_daily_bar_codes(
    store: ParquetStore,
    code: tuple[str, ...] | list[str] | str | None,
    include_delisted: bool,
) -> list[str]:
    return resolve_akshare_universe_codes(
        store,
        code=code,
        include_delisted=include_delisted,
        context="AkShare daily bar",
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


def _date_iso(value: str | date | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return pd.to_datetime(value, errors="raise").date().isoformat()

