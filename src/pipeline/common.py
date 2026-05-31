"""Shared pipeline helpers."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src.pipeline.adjustments import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET
from src.storage.dataset_catalog import (
    AKSHARE_VALUATION_EASTMONEY_DATASET,
    daily_bar_dataset_names,
    expand_daily_bar_selection,
    is_daily_bar_dataset,
)
from src.storage.parquet_store import ParquetStore
from src.storage.schema import DAILY_BAR_SCHEMA
from src.utils.config_mgr import ConfigManager

DAILY_BAR_DATASETS = daily_bar_dataset_names()
FULL_HISTORY_START_DATE = "1990-01-01"
PIPELINE_UPDATE_DAILY = "update_daily"
MARKET_DATE_CUTOFF = time(18, 0)


class PipelineCheckpointLookup:
    """In-memory checkpoint index for hot resume checks."""

    def __init__(self, checkpoints: pd.DataFrame) -> None:
        self._pipeline_status: dict[tuple[str, str, str, str, str], str] = {}
        self._date_status: dict[tuple[str, str, str, str], str] = {}
        if checkpoints.empty:
            return

        work = checkpoints.copy()
        work["_start_key"] = pd.to_datetime(work["start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        work["_end_key"] = pd.to_datetime(work["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        work["_updated_key"] = pd.to_datetime(work["updated_at"], errors="coerce")
        work = work.sort_values("_updated_key", na_position="first")

        for _, row in work.iterrows():
            pipeline = str(row["pipeline"])
            dataset = str(row["dataset"])
            code = str(row["code"])
            start_key = row["_start_key"]
            end_key = row["_end_key"]
            status = str(row["status"])
            if pd.notna(start_key) and pd.notna(end_key):
                self._pipeline_status[(pipeline, dataset, code, str(start_key), str(end_key))] = status
            if pd.notna(end_key):
                self._date_status[(pipeline, dataset, code, str(end_key))] = status

    @classmethod
    def from_store(cls, store: ParquetStore) -> PipelineCheckpointLookup:
        return cls(store.read_pipeline_checkpoints())

    def pipeline_checkpoint_succeeded(
        self,
        pipeline: str,
        dataset: str,
        code: str,
        start_date: str,
        end_date: str,
        output_path: str | Path,
    ) -> bool:
        if not Path(output_path).exists():
            return False
        key = (pipeline, dataset, code, start_date, end_date)
        return self._pipeline_status.get(key) == "success"

    def checkpoint_succeeded_for_date(
        self,
        pipeline: str,
        dataset: str,
        code: str,
        end_date: str,
        output_path: str | Path,
    ) -> bool:
        if not Path(output_path).exists():
            return False
        key = (pipeline, dataset, code, end_date)
        return self._date_status.get(key) == "success"


def today_iso() -> str:
    return date.today().isoformat()


def default_candidate_date(config: ConfigManager, now: datetime | None = None) -> str:
    """Return the natural date to resolve through the trading baostock_cn_trading_calendar.

    Data may still change before 18:00 local exchange time, so unattended runs
    before that cutoff target the previous natural date first.
    """

    timezone_name = str(config.get("project.timezone", "Asia/Shanghai"))
    local_zone = ZoneInfo(timezone_name)
    if now is None:
        local_now = datetime.now(local_zone)
    elif now.tzinfo is None:
        local_now = now.replace(tzinfo=local_zone)
    else:
        local_now = now.astimezone(local_zone)

    candidate = local_now.date()
    if local_now.time() < MARKET_DATE_CUTOFF:
        candidate -= timedelta(days=1)
    return candidate.isoformat()


def date_iso(value: str | date | None, default: str | None = None) -> str:
    if value is None:
        if default is None:
            raise ValueError("date value is required")
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="raise")
    return parsed.date().isoformat()


def subtract_days(value: str, days: int) -> str:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    return (parsed - timedelta(days=days)).isoformat()


def baostock_cn_trading_calendar_fetch_start(candidate_date: str, lookback_days: int = 0) -> str:
    baostock_cn_trading_calendar_days = max(int(lookback_days) * 3 + 14, 90)
    return subtract_days(candidate_date, baostock_cn_trading_calendar_days)


def is_trading_day(baostock_cn_trading_calendar_df: pd.DataFrame, value: str | date) -> bool:
    work = _baostock_cn_trading_calendar_with_keys(baostock_cn_trading_calendar_df)
    if work.empty:
        return False
    target = pd.Timestamp(date_iso(value))
    matches = work.loc[work["_calendar_date"] == target, "_is_trading_day"]
    return not matches.empty and bool(matches.iloc[-1])


def baostock_cn_trading_calendar_covers_range(
    baostock_cn_trading_calendar_df: pd.DataFrame, start_date: str | date, end_date: str | date
) -> bool:
    work = _baostock_cn_trading_calendar_with_keys(baostock_cn_trading_calendar_df)
    if work.empty:
        return False
    start_ts = pd.Timestamp(date_iso(start_date))
    end_ts = pd.Timestamp(date_iso(end_date))
    return bool(work["_calendar_date"].min() <= start_ts and work["_calendar_date"].max() >= end_ts)


def latest_trading_day_on_or_before(baostock_cn_trading_calendar_df: pd.DataFrame, value: str | date) -> str:
    work = _baostock_cn_trading_calendar_with_keys(baostock_cn_trading_calendar_df)
    if work.empty:
        raise ValueError("No baostock_cn_trading_calendar rows available to resolve trading day")

    target = pd.Timestamp(date_iso(value))
    matches = work.loc[(work["_calendar_date"] <= target) & work["_is_trading_day"]]
    if matches.empty:
        raise ValueError(
            f"No trading day found on or before {target.date().isoformat()} in baostock_cn_trading_calendar"
        )
    return matches.sort_values("_calendar_date").iloc[-1]["_calendar_date"].date().isoformat()


def first_trading_day_on_or_after(baostock_cn_trading_calendar_df: pd.DataFrame, value: str | date) -> str:
    work = _baostock_cn_trading_calendar_with_keys(baostock_cn_trading_calendar_df)
    if work.empty:
        raise ValueError("No baostock_cn_trading_calendar rows available to resolve trading day")

    target = pd.Timestamp(date_iso(value))
    matches = work.loc[(work["_calendar_date"] >= target) & work["_is_trading_day"]]
    if matches.empty:
        raise ValueError(
            f"No trading day found on or after {target.date().isoformat()} in baostock_cn_trading_calendar"
        )
    return matches.sort_values("_calendar_date").iloc[0]["_calendar_date"].date().isoformat()


def trading_day_lookback_start(
    baostock_cn_trading_calendar_df: pd.DataFrame, end_date: str | date, lookback_days: int
) -> str:
    if lookback_days < 0:
        raise ValueError("lookback_days must be non-negative")

    work = _baostock_cn_trading_calendar_with_keys(baostock_cn_trading_calendar_df)
    if work.empty:
        raise ValueError("No baostock_cn_trading_calendar rows available to resolve trading day lookback")

    resolved_end = latest_trading_day_on_or_before(baostock_cn_trading_calendar_df, end_date)
    end_ts = pd.Timestamp(resolved_end)
    trading_days = work.loc[(work["_calendar_date"] <= end_ts) & work["_is_trading_day"], "_calendar_date"]
    trading_days = trading_days.drop_duplicates().sort_values().reset_index(drop=True)
    if trading_days.empty:
        raise ValueError(f"No trading days found on or before {resolved_end}")

    index = max(len(trading_days) - 1 - int(lookback_days), 0)
    return trading_days.iloc[index].date().isoformat()


def trading_range_bounds(
    baostock_cn_trading_calendar_df: pd.DataFrame, start_date: str | date, end_date: str | date
) -> tuple[str, str]:
    resolved_start = first_trading_day_on_or_after(baostock_cn_trading_calendar_df, start_date)
    resolved_end = latest_trading_day_on_or_before(baostock_cn_trading_calendar_df, end_date)
    if resolved_start > resolved_end:
        raise ValueError(f"No trading days found between {date_iso(start_date)} and {date_iso(end_date)}")
    return resolved_start, resolved_end


def resolve_codes(
    config: ConfigManager,
    store: ParquetStore,
    code: tuple[str, ...] | list[str] | str | None,
    baostock_cn_stock_basic_mode: str,
    security_type: str | None = None,
) -> list[str]:
    if isinstance(code, str):
        return [code]
    if code:
        return [str(item) for item in code]
    codes = baostock_cn_stock_basic_codes(store, baostock_cn_stock_basic_mode, security_type=security_type)
    if not codes:
        raise ValueError("No stock codes found in baostock_cn_stock_basic data")
    return codes


def baostock_cn_stock_basic_codes(store: ParquetStore, mode: str = "all", security_type: str | None = None) -> list[str]:
    df = store.read_dataset("baostock_cn_stock_basic")
    if df.empty:
        return []
    if mode == "all":
        work = df
    elif mode == "active":
        status = df["listing_status"].astype("string").str.strip()
        work = df.loc[status == "1"]
    else:
        raise ValueError(f"Unsupported baostock_cn_stock_basic code mode: {mode}")

    if security_type is not None:
        work = work.loc[work["security_type"].astype("string").str.strip() == security_type]

    codes = work["code"].astype("string").str.strip()
    codes = codes.loc[codes.notna() & (codes != "")]
    return list(dict.fromkeys(codes.astype(str).tolist()))


def _baostock_cn_trading_calendar_with_keys(baostock_cn_trading_calendar_df: pd.DataFrame) -> pd.DataFrame:
    if baostock_cn_trading_calendar_df.empty or "calendar_date" not in baostock_cn_trading_calendar_df.columns:
        return pd.DataFrame(columns=["_calendar_date", "_is_trading_day"])

    work = baostock_cn_trading_calendar_df.copy()
    work["_calendar_date"] = pd.to_datetime(work["calendar_date"], errors="coerce").dt.normalize()
    status = work.get("is_trading_day", pd.Series("", index=work.index))
    work["_is_trading_day"] = status.astype("string").str.strip().str.lower().isin({"1", "true", "t", "yes"})
    return work.loc[work["_calendar_date"].notna()].copy()


def checkpoint_output_path(store: ParquetStore, dataset: str, code: str, end_date: str) -> Path:
    if is_daily_bar_dataset(dataset):
        return store.dataset_path(dataset, {"code": code})
    if dataset == BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET:
        return store.dataset_path(dataset, {"code": code})
    if dataset == "baostock_cn_stock_basic":
        return store.dataset_path(dataset)
    if dataset == "baostock_cn_trading_calendar":
        return store.dataset_path(dataset)
    if dataset == AKSHARE_VALUATION_EASTMONEY_DATASET.name:
        return store.dataset_path(dataset, {"code": code})
    raise ValueError(f"Unsupported checkpoint dataset: {dataset}")


def should_skip_checkpoint(
    store: ParquetStore,
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    output_path: Path,
    resume: bool,
    force: bool,
    checkpoint_lookup: PipelineCheckpointLookup | None = None,
) -> bool:
    if force or not resume:
        return False
    if checkpoint_lookup is not None:
        if checkpoint_lookup.pipeline_checkpoint_succeeded(pipeline, dataset, code, start_date, end_date, output_path):
            return True
        return checkpoint_lookup.checkpoint_succeeded_for_date(pipeline, dataset, code, end_date, output_path)
    if store.pipeline_checkpoint_succeeded(pipeline, dataset, code, start_date, end_date, output_path):
        return True
    return store.checkpoint_succeeded_for_date(pipeline, dataset, code, end_date, output_path)


def checkpoint_row(
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    status: str,
    row_count: int,
    output_path: str | Path,
    error_stack: str = "",
) -> dict[str, object]:
    return {
        "pipeline": pipeline,
        "dataset": dataset,
        "code": code,
        "start_date": start_date,
        "end_date": end_date,
        "status": status,
        "row_count": row_count,
        "output_path": str(output_path),
        "updated_at": datetime.now(),
        "error_stack": error_stack,
    }


def write_checkpoint(
    store: ParquetStore,
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    status: str,
    row_count: int,
    output_path: str | Path,
    error_stack: str = "",
) -> None:
    store.upsert_pipeline_checkpoints(
        pd.DataFrame(
            [
                checkpoint_row(
                    pipeline,
                    dataset,
                    code,
                    start_date,
                    end_date,
                    status,
                    row_count,
                    output_path,
                    error_stack,
                )
            ]
        )
    )


def expand_daily_datasets(dataset: str) -> list[str]:
    return expand_daily_bar_selection(dataset)


def merge_daily_frames(store: ParquetStore, existing: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    existing_clean = store.clean_dataframe_for_schema(existing, DAILY_BAR_SCHEMA)
    fresh_clean = store.clean_dataframe_for_schema(fresh, DAILY_BAR_SCHEMA)
    combined = pd.concat([existing_clean, fresh_clean], ignore_index=True)
    if combined.empty:
        return combined
    combined["_date_key"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    combined = (
        combined.drop_duplicates(["code", "_date_key"], keep="last")
        .drop(columns=["_date_key"])
        .sort_values(["code", "date"])
        .reset_index(drop=True)
    )
    return combined


def daily_frames_differ_on_overlap(
    store: ParquetStore,
    existing: pd.DataFrame,
    fresh: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> bool:
    """Return True when existing lookback rows conflict with freshly fetched rows.

    Rows present only in fresh data are normal for daily updates, so they do not
    count as conflicts. Existing rows missing from the fresh window, or shared
    rows with changed values, do count as conflicts.
    """

    existing_window = _daily_window_by_key(store, existing, start_date, end_date)
    if existing_window.empty:
        return False

    fresh_window = _daily_window_by_key(store, fresh, start_date, end_date)
    if fresh_window.empty:
        return True

    missing_existing_keys = existing_window.index.difference(fresh_window.index)
    if len(missing_existing_keys) > 0:
        return True

    common_index = existing_window.index.intersection(fresh_window.index).sort_values()
    if len(common_index) == 0:
        return False

    compare_columns = [name for name in DAILY_BAR_SCHEMA.names if name not in {"code", "date"}]
    existing_compare = existing_window.loc[common_index, compare_columns].sort_index()
    fresh_compare = fresh_window.loc[common_index, compare_columns].sort_index()
    return not _daily_values_equal(existing_compare, fresh_compare)


def _daily_values_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=True)
    except AssertionError:
        return False
    return True


def _daily_window_by_key(
    store: ParquetStore,
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    clean = store.clean_dataframe_for_schema(df, DAILY_BAR_SCHEMA)
    if clean.empty:
        empty = clean.iloc[0:0].copy()
        empty.index = pd.MultiIndex.from_arrays([[], []], names=["_code_key", "_date_key"])
        return empty

    dates = pd.to_datetime(clean["date"], errors="coerce")
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    window = clean.loc[(dates >= start_ts) & (dates <= end_ts)].copy()
    if window.empty:
        empty = window.iloc[0:0].copy()
        empty.index = pd.MultiIndex.from_arrays([[], []], names=["_code_key", "_date_key"])
        return empty

    window["_code_key"] = window["code"].astype("string").fillna("")
    window["_date_key"] = pd.to_datetime(window["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    return (
        window.drop_duplicates(["_code_key", "_date_key"], keep="last")
        .set_index(["_code_key", "_date_key"])
        .sort_index()
    )


def replace_daily_range(
    store: ParquetStore,
    existing: pd.DataFrame,
    fresh: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    existing_clean = store.clean_dataframe_for_schema(existing, DAILY_BAR_SCHEMA)
    if not existing_clean.empty:
        dates = pd.to_datetime(existing_clean["date"], errors="coerce")
        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        existing_clean = existing_clean.loc[(dates < start_ts) | (dates > end_ts)].reset_index(drop=True)
    return merge_daily_frames(store, existing_clean, fresh)


def project_config(root: Path | None = None) -> ConfigManager:
    return ConfigManager(root)
