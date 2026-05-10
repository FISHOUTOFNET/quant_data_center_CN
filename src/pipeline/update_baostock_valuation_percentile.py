"""Derived Baostock valuation percentile pipeline."""

from __future__ import annotations

import traceback
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


def update_baostock_valuation_percentile(
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    start: str | None = None,
    root: Path | None = None,
    build_views: bool = True,
    resume: bool = True,
    force: bool = False,
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
    logger.info(
        "Baostock valuation percentile update started mode={} force={} resume={} codes={}",
        mode,
        force,
        resume,
        len(codes),
    )

    for stock_code in codes:
        start_time = datetime.now()
        output_path = store.baostock_cn_stock_valuation_percentile_path(stock_code)
        try:
            source = store.read_baostock_daily_bars(UNADJUSTED_DAILY_DATASET, stock_code)
            if source.empty:
                logger.warning("Skip valuation percentile for {} because source daily bars are missing", stock_code)
                continue

            source_dates = pd.to_datetime(source["date"], errors="coerce").dropna()
            if source_dates.empty:
                logger.warning("Skip valuation percentile for {} because source daily bar dates are invalid", stock_code)
                continue
            source_start = source_dates.min().date().isoformat()
            source_end = source_dates.max().date().isoformat()
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

            computed = compute_valuation_percentiles(source, start=replace_start)
            if mode == "partial" and replace_start is not None:
                existing = store.read_baostock_cn_stock_valuation_percentile(stock_code)
                final = _replace_from_start(existing, computed, replace_start)
            else:
                final = computed

            if final.empty:
                continue

            path = store.write_baostock_cn_stock_valuation_percentile(stock_code, final)
            row_count = len(final)
            run_row = _run_row(dataset, stock_code, "success", checkpoint_start, source_end, start_time, datetime.now(), row_count, "")
            status_row = _status_row(dataset, stock_code, source_end, row_count, "success", "")
            checkpoint = checkpoint_row(
                PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
                dataset,
                stock_code,
                checkpoint_start,
                source_end,
                "success",
                row_count,
                path,
            )
            run_records.append(run_row)
            status_rows.append(status_row)
            checkpoint_rows.append(checkpoint)
            success_count += 1
        except Exception:
            error_stack = traceback.format_exc()
            logger.exception("Baostock valuation percentile failed for {}", stock_code)
            run_row = _run_row(dataset, stock_code, "failed", start or "", "", start_time, datetime.now(), 0, error_stack)
            status_row = _status_row(dataset, stock_code, None, 0, "failed", error_stack)
            checkpoint = checkpoint_row(
                PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
                dataset,
                stock_code,
                start or "",
                "",
                "failed",
                0,
                output_path,
                error_stack,
            )
            run_records.append(run_row)
            status_rows.append(status_row)
            checkpoint_rows.append(checkpoint)

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
