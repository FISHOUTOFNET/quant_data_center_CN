"""Baostock valuation percentile derived dataset pipeline."""

from __future__ import annotations

import math
import traceback
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import numpy as np
import pandas as pd

from src.pipeline.common import PipelineCheckpointLookup
from src.pipeline.lifecycle import LifecycleTaskRef, PipelineLifecycle
from src.storage.dataset_catalog import BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.storage.schema import field_names
from src.utils.config_mgr import ConfigError, ConfigManager
from src.utils.logging import logger
from src.utils.run_context import new_pipeline_run_id, pipeline_run_context

BAOSTOCK_DAILY_BAR_UNADJUSTED_DATASET = "baostock_cn_stock_daily_bar_unadjusted"
BAOSTOCK_VALUATION_PERCENTILE_DATASET = BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.name
PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE = "update_baostock_valuation_percentile"
VALUATION_FIELDS = ("pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm")
WINDOWS = {"1y": 1, "3y": 3, "5y": 5, "10y": 10}
ALL_HISTORY_WINDOW = "all_history"


class _FenwickTree:
    def __init__(self, size: int) -> None:
        self._tree = [0] * (size + 1)

    def add(self, index: int, delta: int) -> None:
        current = index + 1
        while current < len(self._tree):
            self._tree[current] += delta
            current += current & -current

    def prefix_sum(self, index: int) -> int:
        if index < 0:
            return 0
        current = min(index + 1, len(self._tree) - 1)
        total = 0
        while current > 0:
            total += self._tree[current]
            current -= current & -current
        return total


@dataclass(frozen=True)
class _Sample:
    date: pd.Timestamp
    value: float
    index: int


@dataclass(frozen=True)
class _ValuationPercentileTask:
    code: str
    output_path: Path
    checkpoint_start: str | None = None
    source_end: str | None = None
    skip_status: str | None = None
    skip_reason: str = ""


class _RollingCounts:
    def __init__(self, values: list[float]) -> None:
        size = max(len(values), 1)
        self._values = values
        self._all = _FenwickTree(size)
        self._positive = _FenwickTree(size)
        self._negative = _FenwickTree(size)
        self._samples: deque[_Sample] = deque()
        self.total_count = 0
        self.positive_count = 0
        self.negative_count = 0

    def add(self, sample_date: pd.Timestamp, value: float, index: int) -> None:
        sample = _Sample(sample_date, value, index)
        self._samples.append(sample)
        self._add_sample(sample, 1)

    def evict_before(self, threshold: pd.Timestamp) -> None:
        while self._samples and self._samples[0].date < threshold:
            sample = self._samples.popleft()
            self._add_sample(sample, -1)

    def percentile(self, value: float, index: int) -> float | None:
        if self.total_count <= 0:
            return None
        if value < 0:
            less_than_current = self._negative.prefix_sum(index - 1)
            negative_greater_equal_current = self.negative_count - less_than_current
            return (self.positive_count + negative_greater_equal_current) / self.total_count * 100.0
        if self.negative_count > 0 and value > 0:
            return self._positive.prefix_sum(index) / self.positive_count * 100.0
        return self._all.prefix_sum(index) / self.total_count * 100.0

    def _add_sample(self, sample: _Sample, delta: int) -> None:
        self._all.add(sample.index, delta)
        self.total_count += delta
        if sample.value > 0:
            self._positive.add(sample.index, delta)
            self.positive_count += delta
        elif sample.value < 0:
            self._negative.add(sample.index, delta)
            self.negative_count += delta


def compute_valuation_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Baostock valuation percentiles for one stock's daily bars."""

    output_columns = field_names(BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.schema)
    if df.empty:
        return pd.DataFrame(columns=output_columns)

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values(["code", "date"]).reset_index(drop=True)
    for field in VALUATION_FIELDS:
        work[field] = pd.to_numeric(work[field], errors="coerce")

    result_columns: dict[str, list[object]] = {
        column: work[column].tolist() for column in ("date", "code", *VALUATION_FIELDS)
    }
    for column in output_columns:
        if column not in result_columns:
            result_columns[column] = [math.nan] * len(work)

    work["_date_only"] = work["date"]

    _compute_all_history_percentiles(result_columns, work)
    for _code, group in work.groupby("code", dropna=False, sort=False):
        for field in VALUATION_FIELDS:
            _compute_field_percentiles(result_columns, group, field)

    result_columns["date"] = pd.Series(result_columns["date"]).dt.date.astype(object).tolist()
    result = pd.DataFrame(result_columns, columns=output_columns)
    return result[output_columns].reset_index(drop=True)


def _compute_all_history_percentiles(result_columns: dict[str, list[object]], work: pd.DataFrame) -> None:
    codes = work["code"]
    for field in VALUATION_FIELDS:
        values = work[field]
        valid = values.notna() & (values != 0)
        positive = values > 0
        negative = values < 0
        output = pd.Series(math.nan, index=work.index, dtype="float64")

        positive_rank = (
            values.where(positive)
            .groupby(codes)
            .expanding()
            .rank(method="max", pct=True)
            .reset_index(level=0, drop=True)
            * 100.0
        )
        output.loc[positive] = positive_rank.loc[positive]

        valid_count = valid.groupby(codes).cumsum()
        positive_count = positive.groupby(codes).cumsum()
        negative_rank_desc = (
            (-values.where(negative))
            .groupby(codes)
            .expanding()
            .rank(method="max", pct=False)
            .reset_index(level=0, drop=True)
        )
        output.loc[negative] = (
            (positive_count.loc[negative] + negative_rank_desc.loc[negative]) / valid_count.loc[negative] * 100.0
        )

        output.loc[~valid] = math.nan
        result_columns[f"{field}_percentile_{ALL_HISTORY_WINDOW}"] = output.tolist()


def update_baostock_valuation_percentile(
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
) -> list[dict[str, object]]:
    run_id = new_pipeline_run_id("baostock_valuation_percentile")
    with pipeline_run_context(run_id):
        return _update_baostock_valuation_percentile_impl(
            mode=mode,
            code=code,
            start=start,
            root=root,
            resume=resume,
            force=force,
            build_views=build_views,
        )


def _compute_field_percentiles(result_columns: dict[str, list[object]], group: pd.DataFrame, field: str) -> None:
    valid = group.loc[group[field].map(_is_valid_valuation), ["date", field]]
    values = sorted(valid[field].astype(float).unique().tolist())
    if not values:
        return

    value_to_index = {value: index for index, value in enumerate(values)}
    first_valid_date = valid["date"].min()
    fixed_windows = {name: _RollingCounts(values) for name in WINDOWS}
    row_positions = group.index.to_list()
    dates = group["_date_only"].to_list()
    field_values = group[field].to_list()
    thresholds = {
        name: [_subtract_years(current_date, years) for current_date in dates] for name, years in WINDOWS.items()
    }
    fixed_window_columns = {name: result_columns[f"{field}_percentile_{name}"] for name in WINDOWS}

    for offset, (row_index, current_date, value) in enumerate(zip(row_positions, dates, field_values, strict=False)):
        for name in WINDOWS:
            fixed_windows[name].evict_before(thresholds[name][offset])

        if not _is_valid_valuation(value):
            continue

        current_value = float(value)
        value_index = value_to_index[current_value]
        for window in fixed_windows.values():
            window.add(current_date, current_value, value_index)

        for name in WINDOWS:
            threshold = thresholds[name][offset]
            if first_valid_date <= threshold:
                fixed_window_columns[name][row_index] = fixed_windows[name].percentile(current_value, value_index)


def _subtract_years(value: pd.Timestamp, years: int) -> pd.Timestamp:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _is_valid_valuation(value: object) -> bool:
    try:
        numeric = float(cast(Any, value))
    except (TypeError, ValueError):
        return False
    return not math.isnan(numeric) and numeric != 0.0


def _compute_append_only_valuation_percentiles(source: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    output_columns = field_names(BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.schema)
    if source.empty or existing.empty:
        return compute_valuation_percentiles(source)

    latest_existing = pd.to_datetime(existing["date"], errors="coerce").max()
    if pd.isna(latest_existing):
        return compute_valuation_percentiles(source)

    work = source.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values(["code", "date"]).reset_index(drop=True)
    for field in VALUATION_FIELDS:
        work[field] = pd.to_numeric(work[field], errors="coerce")
    work["_date_only"] = work["date"].dt.date
    work["_append_output"] = work["date"] > latest_existing

    append_work = work.loc[work["_append_output"]].copy()
    if append_work.empty:
        return pd.DataFrame(columns=output_columns)

    result_columns: dict[str, list[object]] = {
        column: append_work[column].tolist() for column in ("date", "code", *VALUATION_FIELDS)
    }
    for column in output_columns:
        if column not in result_columns:
            result_columns[column] = [math.nan] * len(append_work)

    append_position_by_index = {row_index: offset for offset, row_index in enumerate(append_work.index)}
    for _code, group in work.groupby("code", dropna=False, sort=False):
        group_append_indices = set(group.index[group["_append_output"]])
        if not group_append_indices:
            continue
        for field in VALUATION_FIELDS:
            _compute_append_only_all_history_percentiles(
                result_columns,
                group,
                field,
                append_position_by_index,
            )
            _compute_append_only_fixed_window_percentiles(
                result_columns,
                group,
                field,
                append_position_by_index,
            )

    result_columns["date"] = pd.Series(result_columns["date"]).dt.date.astype(object).tolist()
    result = pd.DataFrame(result_columns, columns=output_columns)
    return result[output_columns].reset_index(drop=True)


def _compute_append_only_all_history_percentiles(
    result_columns: dict[str, list[object]],
    group: pd.DataFrame,
    field: str,
    append_position_by_index: dict[int, int],
) -> None:
    values = group[field].to_numpy(dtype="float64", copy=False)
    dates = group["date"].to_numpy(dtype="datetime64[ns]", copy=False)
    valid = ~np.isnan(values) & (values != 0.0)
    if not valid.any():
        return

    output = result_columns[f"{field}_percentile_{ALL_HISTORY_WINDOW}"]
    append_rows = group.loc[group["_append_output"]]
    for row_index, row in append_rows.iterrows():
        value = row[field]
        if not _is_valid_valuation(value):
            continue
        current_value = float(value)
        current_date = np.datetime64(row["date"])
        sample = values[valid & (dates <= current_date)]
        output[append_position_by_index[row_index]] = _percentile_from_values(sample, current_value)


def _compute_append_only_fixed_window_percentiles(
    result_columns: dict[str, list[object]],
    group: pd.DataFrame,
    field: str,
    append_position_by_index: dict[int, int],
) -> None:
    valid = group.loc[group[field].map(_is_valid_valuation), ["date", field]]
    if valid.empty:
        return

    first_valid_date = valid["date"].min().date()
    values = group[field].to_numpy(dtype="float64", copy=False)
    dates = group["date"].to_numpy(dtype="datetime64[ns]", copy=False)
    valid_mask = ~np.isnan(values) & (values != 0.0)
    fixed_window_columns = {name: result_columns[f"{field}_percentile_{name}"] for name in WINDOWS}

    append_rows = group.loc[group["_append_output"]]
    for row_index, row in append_rows.iterrows():
        value = row[field]
        if not _is_valid_valuation(value):
            continue

        current_value = float(value)
        current_date = row["_date_only"]
        current_timestamp = np.datetime64(row["date"])
        output_position = append_position_by_index[row_index]
        for name, years in WINDOWS.items():
            threshold = _subtract_years(current_date, years)
            if first_valid_date <= threshold:
                threshold_timestamp = np.datetime64(pd.Timestamp(threshold))
                sample = values[valid_mask & (dates >= threshold_timestamp) & (dates <= current_timestamp)]
                fixed_window_columns[name][output_position] = _percentile_from_values(sample, current_value)


def _percentile_from_values(values: np.ndarray, current_value: float) -> float | None:
    total_count = len(values)
    if total_count <= 0:
        return None
    positive_values = values[values > 0.0]
    negative_values = values[values < 0.0]
    positive_count = len(positive_values)
    negative_count = len(negative_values)
    if current_value < 0:
        negative_greater_equal_current = int((negative_values >= current_value).sum())
        return (positive_count + negative_greater_equal_current) / total_count * 100.0
    if negative_count > 0 and current_value > 0:
        return int((positive_values <= current_value).sum()) / positive_count * 100.0
    return int((values <= current_value).sum()) / total_count * 100.0


def _update_baostock_valuation_percentile_impl(
    mode: str,
    code: tuple[str, ...] | list[str] | str | None,
    start: str | None,
    root: Path | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> list[dict[str, object]]:
    if mode not in {"full", "partial"}:
        raise ValueError(f"Unsupported Baostock valuation percentile mode: {mode}")
    if mode == "partial" and start is not None and not force:
        raise ValueError("--start requires --force in partial mode")

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    codes = _resolve_source_codes(store, code)
    checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if resume and not force else None
    tasks = _prefilter_valuation_percentile_tasks(store, codes, mode, start, checkpoint_lookup)
    lifecycle = PipelineLifecycle(
        store,
        flush_size=_metadata_flush_size(config),
        count_by="run",
    )
    records: list[dict[str, object]] = []
    wrote_metadata = False
    progress_total = len(tasks)
    progress_processed = 0
    progress_success = 0
    progress_failed = 0
    progress_skipped = 0

    logger.info(
        "Baostock valuation percentile update started mode={} force={} planned_codes={} processing_codes={}",
        mode,
        force,
        len(codes),
        progress_total,
    )

    def log_progress(row: dict[str, object], stock_code: str) -> None:
        nonlocal progress_processed, progress_success, progress_failed, progress_skipped
        progress_processed += 1
        status = str(row.get("status", "unknown"))
        if status == "success":
            progress_success += 1
        elif status == "failed":
            progress_failed += 1
        elif status.startswith("skipped"):
            progress_skipped += 1
        logger.info(
            "Baostock valuation percentile progress {}/{} code={} status={} rows={}",
            progress_processed,
            progress_total,
            row.get("code", stock_code),
            status,
            row.get("row_count", 0),
        )

    for task in tasks:
        stock_code = task.code
        started_at = datetime.now()
        output_path = task.output_path
        if task.skip_status is not None:
            run_date = _date_or_default(start, date.today().isoformat())
            skipped = lifecycle.record_skipped(
                _task_ref(stock_code, run_date, task.source_end or run_date, output_path),
                status=task.skip_status,
                started_at=started_at,
                ended_at=datetime.now(),
                reason=task.skip_reason,
            )
            wrote_metadata = True
            records.append(skipped.run_row)
            log_progress(skipped.run_row, stock_code)
            continue

        try:
            source = store.read_dataset(BAOSTOCK_DAILY_BAR_UNADJUSTED_DATASET, {"code": stock_code})
            if source.empty:
                raise FileNotFoundError(f"Missing source partition for {stock_code}")
            source_dates = pd.to_datetime(source["date"], errors="coerce")
            source_start = source_dates.min().date().isoformat()
            source_end = task.source_end or source_dates.max().date().isoformat()
            existing = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": stock_code})
            checkpoint_start = task.checkpoint_start or _checkpoint_start_date(mode, start, source_start, existing)

            if _partial_existing_covers_source(mode, start, existing, source_end):
                selected = pd.DataFrame(columns=field_names(BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.schema))
            elif mode == "partial" and start is None and not existing.empty:
                selected = _compute_append_only_valuation_percentiles(source, existing)
            else:
                computed = compute_valuation_percentiles(source)
                selected = _select_rows_for_mode(computed, existing, mode, start)
            if selected.empty:
                skipped = lifecycle.record_skipped(
                    _task_ref(stock_code, checkpoint_start, source_end, output_path),
                    status="skipped_existing",
                    started_at=started_at,
                    ended_at=datetime.now(),
                    reason="",
                )
                records.append(skipped.run_row)
                log_progress(skipped.run_row, stock_code)
                continue

            output = _merge_output_for_mode(existing, selected, mode, start)
            store.write_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, output, {"code": stock_code})
            success = lifecycle.record_success(
                _task_ref(stock_code, _first_date(selected), source_end, output_path),
                started_at=started_at,
                ended_at=datetime.now(),
                row_count=len(selected),
                output_path=output_path,
            )
            wrote_metadata = True
            records.append(success.run_row)
            log_progress(success.run_row, stock_code)
        except Exception as exc:
            stack = traceback.format_exc()
            if not stack or stack == "NoneType: None\n":
                stack = f"{type(exc).__name__}: {exc}"
            failed = lifecycle.record_failure(
                _task_ref(
                    stock_code,
                    _date_or_default(start, date.today().isoformat()),
                    _date_or_default(start, date.today().isoformat()),
                    output_path,
                ),
                started_at=started_at,
                ended_at=datetime.now(),
                error_stack=stack,
            )
            wrote_metadata = True
            records.append(failed.run_row)
            log_progress(failed.run_row, stock_code)

    lifecycle.finish()
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=wrote_metadata)
    logger.info(
        "Baostock valuation percentile update completed processed={} success={} failed={} skipped={}",
        progress_processed,
        progress_success,
        progress_failed,
        progress_skipped,
    )
    return records


def _task_ref(stock_code: str, start_date: str, end_date: str, output_path: Path) -> LifecycleTaskRef:
    return LifecycleTaskRef(
        PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
        BAOSTOCK_VALUATION_PERCENTILE_DATASET,
        stock_code,
        start_date,
        end_date,
        output_path,
    )


def _metadata_flush_size(config: ConfigManager) -> int:
    try:
        return int(config.get("pipeline.metadata_flush_size", 200))
    except ConfigError:
        return 200


def _batch_read_date_ranges(store: ParquetStore, dataset: str) -> dict[str, tuple[str, str]]:
    dataset_dir = store.parquet_dir / dataset
    if not dataset_dir.exists():
        return {}
    pattern = (dataset_dir / "**" / "*.parquet").as_posix()
    try:
        conn = duckdb.connect()
        result = conn.execute(
            f"SELECT code, MIN(date)::VARCHAR AS min_date, MAX(date)::VARCHAR AS max_date "
            f"FROM read_parquet('{pattern}', hive_partitioning=true) "
            f"GROUP BY code"
        ).fetchall()
        conn.close()
        return {row[0]: (row[1], row[2]) for row in result}
    except Exception:
        return {}


def _prefilter_valuation_percentile_tasks(
    store: ParquetStore,
    codes: list[str],
    mode: str,
    start: str | None,
    checkpoint_lookup: PipelineCheckpointLookup | None,
) -> list[_ValuationPercentileTask]:
    if not codes:
        return []

    logger.info("Batch loading date ranges for {} codes...", len(codes))
    source_ranges = _batch_read_date_ranges(store, BAOSTOCK_DAILY_BAR_UNADJUSTED_DATASET)
    existing_ranges = {} if mode == "full" else _batch_read_date_ranges(store, BAOSTOCK_VALUATION_PERCENTILE_DATASET)
    logger.info(
        "Loaded source ranges for {} codes, existing ranges for {} codes", len(source_ranges), len(existing_ranges)
    )

    tasks: list[_ValuationPercentileTask] = []
    skipped_count = 0
    for stock_code in codes:
        output_path = store.dataset_path(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": stock_code})
        source_range = source_ranges.get(stock_code)
        if source_range is None or source_range[0] is None or source_range[1] is None:
            tasks.append(
                _ValuationPercentileTask(
                    code=stock_code,
                    output_path=output_path,
                    skip_status="skipped_missing_source",
                    skip_reason=f"Missing source partition for {stock_code}",
                )
            )
            continue

        source_start, source_end = source_range
        if checkpoint_lookup is None:
            tasks.append(_ValuationPercentileTask(code=stock_code, output_path=output_path, source_end=source_end))
            continue

        try:
            existing_range = existing_ranges.get(stock_code)
            checkpoint_start = _checkpoint_start_date_from_range(mode, start, source_start, existing_range)
            if _partial_existing_covers_source_from_range(mode, start, existing_range, source_end):
                skipped_count += 1
                continue
            if checkpoint_lookup.pipeline_checkpoint_succeeded(
                PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
                BAOSTOCK_VALUATION_PERCENTILE_DATASET,
                stock_code,
                checkpoint_start,
                source_end,
                output_path,
            ):
                skipped_count += 1
                continue

            tasks.append(
                _ValuationPercentileTask(
                    code=stock_code,
                    output_path=output_path,
                    checkpoint_start=checkpoint_start,
                    source_end=source_end,
                )
            )
        except Exception:
            tasks.append(_ValuationPercentileTask(code=stock_code, output_path=output_path))

    if skipped_count:
        skipped_ratio = skipped_count / len(codes) * 100
        logger.info(
            "Checkpoint prefilter skipped {}/{} baostock valuation percentile codes ({:.1f}%); processing {} codes",
            skipped_count,
            len(codes),
            skipped_ratio,
            len(tasks),
        )
    return tasks


def _resolve_source_codes(store: ParquetStore, code: tuple[str, ...] | list[str] | str | None) -> list[str]:
    source_dir = store.parquet_dir / BAOSTOCK_DAILY_BAR_UNADJUSTED_DATASET
    requested = [code] if isinstance(code, str) else list(code or [])
    if requested:
        return requested
    if not source_dir.exists():
        return []
    return sorted(
        path.name.removeprefix("code=")
        for path in source_dir.iterdir()
        if path.is_dir() and path.name.startswith("code=")
    )


def _partial_existing_covers_source_from_range(
    mode: str, start: str | None, existing_range: tuple[str, str] | None, source_end: str
) -> bool:
    if mode != "partial" or start is not None or existing_range is None:
        return False
    return pd.Timestamp(existing_range[1]) >= pd.Timestamp(source_end)


def _checkpoint_start_date_from_range(
    mode: str, start: str | None, source_start: str, existing_range: tuple[str, str] | None
) -> str:
    if mode == "full":
        return source_start
    if start is not None:
        return pd.Timestamp(start).date().isoformat()
    if existing_range is None:
        return source_start
    return existing_range[1]


def _partial_existing_covers_source(mode: str, start: str | None, existing: pd.DataFrame, source_end: str) -> bool:
    if mode != "partial" or start is not None or existing.empty:
        return False
    latest_existing = pd.to_datetime(existing["date"], errors="coerce").max()
    if pd.isna(latest_existing):
        return False
    return pd.Timestamp(latest_existing) >= pd.Timestamp(source_end)


def _checkpoint_start_date(mode: str, start: str | None, source_start: str, existing: pd.DataFrame) -> str:
    if mode == "full":
        return source_start
    if start is not None:
        return pd.Timestamp(start).date().isoformat()
    if existing.empty:
        return source_start
    latest = pd.to_datetime(existing["date"], errors="coerce").max()
    if pd.isna(latest):
        return source_start
    return latest.date().isoformat()


def _select_rows_for_mode(computed: pd.DataFrame, existing: pd.DataFrame, mode: str, start: str | None) -> pd.DataFrame:
    if mode == "full" or (existing.empty and start is None):
        return computed
    dates = pd.to_datetime(computed["date"], errors="coerce")
    if start is not None:
        return computed.loc[dates >= pd.Timestamp(start)].reset_index(drop=True)
    latest_existing = pd.to_datetime(existing["date"], errors="coerce").max()
    if pd.isna(latest_existing):
        return computed
    return computed.loc[dates > latest_existing].reset_index(drop=True)


def _merge_output_for_mode(
    existing: pd.DataFrame, selected: pd.DataFrame, mode: str, start: str | None
) -> pd.DataFrame:
    if mode == "full" or existing.empty:
        return selected.reset_index(drop=True)
    if start is None:
        return pd.concat([existing, selected], ignore_index=True)
    before_start = existing.loc[pd.to_datetime(existing["date"], errors="coerce") < pd.Timestamp(start)]
    return pd.concat([before_start, selected], ignore_index=True)


def _first_date(df: pd.DataFrame) -> str:
    return pd.to_datetime(df["date"], errors="coerce").min().date().isoformat()


def _date_or_default(value: str | None, default: str) -> str:
    if value is None:
        return default
    return pd.Timestamp(value).date().isoformat()
