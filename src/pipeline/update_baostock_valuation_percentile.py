"""Derived Baostock valuation percentile pipeline."""

from __future__ import annotations

import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.analytics.valuation_percentile import compute_valuation_percentiles
from src.pipeline.adjustments import UNADJUSTED_DAILY_DATASET
from src.pipeline.common import checkpoint_row, date_iso, should_skip_checkpoint
from src.pipeline.update_daily_metadata import _run_row, _status_row
from src.storage.dataset_catalog import BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE = "update_baostock_valuation_percentile"


@dataclass(frozen=True)
class _ValuationPercentileTask:
    root: Path
    code: str
    mode: str
    replace_start: str | None
    checkpoint_start: str
    source_end: str
    output_path: Path


@dataclass(frozen=True)
class _ValuationPercentileResult:
    task: _ValuationPercentileTask
    started_at: datetime
    ended_at: datetime
    computed: pd.DataFrame | None = None
    error_stack: str = ""


def update_baostock_valuation_percentile(
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
    workers: int | None = None,
) -> list[dict[str, object]]:
    """Compute local Baostock valuation percentiles from unadjusted daily bars."""

    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported valuation percentile mode: {mode}")

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    dataset = BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.name
    run_records: list[dict[str, object]] = []
    status_rows: list[dict[str, object]] = []
    checkpoint_rows: list[dict[str, object]] = []
    success_count = 0

    codes = _resolve_source_codes(store, code)
    resolved_workers = _resolve_workers(config, workers)
    logger.info(
        "Baostock valuation percentile update started mode={} force={} resume={} workers={} codes={}",
        mode,
        force,
        resume,
        resolved_workers,
        len(codes),
    )

    tasks: list[_ValuationPercentileTask] = []
    for stock_code in codes:
        output_path = store.baostock_cn_stock_valuation_percentile_path(stock_code)
        source_bounds = _source_date_bounds(store, stock_code)
        if source_bounds is None:
            continue
        source_start, source_end = source_bounds
        replace_start = _replace_start(store, stock_code, mode, start)
        checkpoint_start = replace_start or source_start

        if _should_skip(
            store,
            dataset,
            stock_code,
            checkpoint_start,
            source_end,
            output_path,
            resume,
            force,
        ):
            continue

        tasks.append(
            _ValuationPercentileTask(
                root=config.root,
                code=stock_code,
                mode=mode,
                replace_start=replace_start,
                checkpoint_start=checkpoint_start,
                source_end=source_end,
                output_path=output_path,
            )
        )

    def record_result(result: _ValuationPercentileResult) -> None:
        nonlocal success_count
        if _record_valuation_result(
            store,
            result,
            dataset,
            run_records,
            status_rows,
            checkpoint_rows,
        ):
            success_count += 1

    if resolved_workers == 1 or len(tasks) <= 1:
        for task in tasks:
            record_result(_compute_valuation_percentile_task(task))
    else:
        with ProcessPoolExecutor(max_workers=resolved_workers) as executor:
            futures = {executor.submit(_compute_valuation_percentile_task, task): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                except Exception:
                    result = _ValuationPercentileResult(
                        task=task,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                        error_stack=traceback.format_exc(),
                    )
                record_result(result)

    if run_records or status_rows or checkpoint_rows:
        store.persist_update_metadata(run_records, status_rows, checkpoint_rows)
    store.close()

    if build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=success_count > 0)
    logger.info(
        "Baostock valuation percentile update completed records={} success={}",
        len(run_records),
        success_count,
    )
    return run_records


def _compute_valuation_percentile_task(task: _ValuationPercentileTask) -> _ValuationPercentileResult:
    started_at = datetime.now()
    store = ParquetStore(root=task.root)
    try:
        source = store.read_baostock_daily_bars(UNADJUSTED_DAILY_DATASET, task.code)
        if source.empty:
            raise ValueError(f"Source daily bars are missing for {task.code}")
        computed = compute_valuation_percentiles(source, start=task.replace_start)
        return _ValuationPercentileResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            computed=computed,
        )
    except Exception:
        return _ValuationPercentileResult(
            task=task,
            started_at=started_at,
            ended_at=datetime.now(),
            error_stack=traceback.format_exc(),
        )
    finally:
        store.close()


def _record_valuation_result(
    store: ParquetStore,
    result: _ValuationPercentileResult,
    dataset: str,
    run_records: list[dict[str, object]],
    status_rows: list[dict[str, object]],
    checkpoint_rows: list[dict[str, object]],
) -> bool:
    task = result.task
    if result.error_stack or result.computed is None:
        _append_failed_metadata(
            dataset,
            task,
            result.started_at,
            result.ended_at,
            result.error_stack,
            run_records,
            status_rows,
            checkpoint_rows,
        )
        return False

    try:
        if task.mode == "partial" and task.replace_start is not None:
            existing = store.read_baostock_cn_stock_valuation_percentile(task.code)
            final = _replace_from_start(existing, result.computed, task.replace_start)
        else:
            final = result.computed

        if final.empty:
            return False

        path = store.write_baostock_cn_stock_valuation_percentile(task.code, final)
        row_count = len(final)
        run_row = _run_row(
            dataset,
            task.code,
            "success",
            task.checkpoint_start,
            task.source_end,
            result.started_at,
            datetime.now(),
            row_count,
            "",
        )
        status_row = _status_row(dataset, task.code, task.source_end, row_count, "success", "")
        checkpoint = checkpoint_row(
            PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
            dataset,
            task.code,
            task.checkpoint_start,
            task.source_end,
            "success",
            row_count,
            path,
        )
        run_records.append(run_row)
        status_rows.append(status_row)
        checkpoint_rows.append(checkpoint)
        return True
    except Exception:
        _append_failed_metadata(
            dataset,
            task,
            result.started_at,
            datetime.now(),
            traceback.format_exc(),
            run_records,
            status_rows,
            checkpoint_rows,
        )
        return False


def _append_failed_metadata(
    dataset: str,
    task: _ValuationPercentileTask,
    started_at: datetime,
    ended_at: datetime,
    error_stack: str,
    run_records: list[dict[str, object]],
    status_rows: list[dict[str, object]],
    checkpoint_rows: list[dict[str, object]],
) -> None:
    logger.error("Baostock valuation percentile failed for {}", task.code)
    run_row = _run_row(
        dataset,
        task.code,
        "failed",
        task.checkpoint_start,
        task.source_end,
        started_at,
        ended_at,
        0,
        error_stack,
    )
    status_row = _status_row(dataset, task.code, None, 0, "failed", error_stack)
    checkpoint = checkpoint_row(
        PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
        dataset,
        task.code,
        task.checkpoint_start,
        task.source_end,
        "failed",
        0,
        task.output_path,
        error_stack,
    )
    run_records.append(run_row)
    status_rows.append(status_row)
    checkpoint_rows.append(checkpoint)


def _resolve_source_codes(store: ParquetStore, code: tuple[str, ...] | list[str] | str | None) -> list[str]:
    if isinstance(code, str):
        return [code]
    if code:
        return [str(item) for item in code]
    dataset_dir = store.parquet_dir / UNADJUSTED_DAILY_DATASET
    if not dataset_dir.exists():
        return []
    prefix = "code="
    return sorted(
        item.name[len(prefix):]
        for item in dataset_dir.iterdir()
        if item.is_dir() and item.name.startswith(prefix) and (item / "data.parquet").exists()
    )


def _source_date_bounds(store: ParquetStore, code: str) -> tuple[str, str] | None:
    path = store.baostock_daily_bar_path(UNADJUSTED_DAILY_DATASET, code)
    if not path.exists():
        logger.warning("Skip valuation percentile for {} because source daily bars are missing", code)
        return None
    source_dates = pd.to_datetime(pd.read_parquet(path, columns=["date"])["date"], errors="coerce").dropna()
    if source_dates.empty:
        logger.warning("Skip valuation percentile for {} because source daily bar dates are invalid", code)
        return None
    return source_dates.min().date().isoformat(), source_dates.max().date().isoformat()


def _resolve_workers(config: ConfigManager, workers: int | None) -> int:
    raw_workers = workers if workers is not None else config.get("pipeline.baostock_valuation_percentile_workers", 1)
    try:
        return max(int(raw_workers), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Baostock valuation percentile workers value: {raw_workers!r}") from exc


def _replace_start(store: ParquetStore, code: str, mode: str, start: str | None) -> str | None:
    if mode == "full":
        return None
    if start is not None:
        return date_iso(start)
    existing = store.read_baostock_cn_stock_valuation_percentile(code)
    if existing.empty:
        return None
    dates = pd.to_datetime(existing["date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return (dates.max().date() + timedelta(days=1)).isoformat()


def _replace_from_start(existing: pd.DataFrame, fresh: pd.DataFrame, start: str) -> pd.DataFrame:
    if existing.empty:
        return fresh.reset_index(drop=True)
    dates = pd.to_datetime(existing["date"], errors="coerce")
    before = existing.loc[dates < pd.Timestamp(start)].copy()
    combined = pd.concat([before, fresh], ignore_index=True)
    if combined.empty:
        return combined
    combined["_date_key"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return (
        combined.drop_duplicates(["code", "_date_key"], keep="last")
        .drop(columns=["_date_key"])
        .sort_values(["code", "date"])
        .reset_index(drop=True)
    )


def _should_skip(
    store: ParquetStore,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    output_path: Path,
    resume: bool,
    force: bool,
) -> bool:
    return should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
        dataset,
        code,
        start_date,
        end_date,
        output_path,
        resume,
        force,
    )
