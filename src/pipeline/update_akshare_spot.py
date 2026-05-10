"""Daily AkShare A-share spot snapshot pipeline with Sina fallback."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from src.api.akshare_client import AkShareClient
from src.pipeline.akshare_universe import normalize_akshare_code_list
from src.pipeline.akshare_common import (
    PIPELINE_UPDATE_AKSHARE_SPOT,
    append_failed_manifest,
    append_response_manifest,
    error_stack,
    error_type,
    failed_metadata,
    persist_metadata,
    success_metadata,
    write_raw_response,
)
from src.pipeline.common import (
    default_candidate_date,
    is_trading_day,
    latest_trading_day_on_or_before,
    should_skip_checkpoint,
)
from src.storage.dataset_catalog import (
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
    AKSHARE_SPOT_QUOTE_SINA_DATASET,
    akshare_daily_bar_dataset_id,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.storage.schema import field_names, schema_for_dataset
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


def update_akshare_spot(
    end: str | date | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager], Any] | None = None,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, object]]:
    """Fetch akshare_cn_stock_spot_quote_eastmoney and map successful close snapshots into daily_bar_unadjusted."""

    config = ConfigManager(root)
    store = ParquetStore(root=config.root)
    _ensure_spot_quote_close_window(config, now, store)
    store.ensure_layout()
    trade_date = _resolve_trade_date(config, end)
    dataset = AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.name
    output_path = store.akshare_cn_stock_spot_quote_eastmoney_path(trade_date)
    progress_processed = 0
    progress_success = 0
    progress_failed = 0
    progress_skipped = 0
    logger.info(
        "AkShare spot update started trade_date={} force={} resume={}",
        trade_date,
        force,
        resume,
    )

    def log_spot_progress(current: int, total: int, row: dict[str, object]) -> None:
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
            "AkShare spot progress {}/{} code={} dataset={} status={} rows={}",
            current,
            total,
            row.get("code", "*"),
            row.get("dataset", dataset),
            status,
            row.get("row_count", 0),
        )

    if should_skip_checkpoint(
        store,
        PIPELINE_UPDATE_AKSHARE_SPOT,
        dataset,
        "*",
        trade_date,
        trade_date,
        output_path,
        resume,
        force,
    ):
        log_spot_progress(
            1,
            1,
            {"dataset": dataset, "code": "*", "status": "skipped_checkpoint", "row_count": 0},
        )
        store.close()
        logger.info(
            "AkShare spot update completed processed={} success={} failed={} skipped={}",
            progress_processed,
            progress_success,
            progress_failed,
            progress_skipped,
        )
        return []

    ak_client = client or (
        client_factory(config)
        if client_factory is not None
        else AkShareClient(config=config)
    )
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]] = []

    started_at = datetime.now()
    try:
        response = ak_client.fetch_spot_quote_eastmoney(trade_date=trade_date)
    except Exception as exc:
        ended_at = datetime.now()
        stack = error_stack(exc)
        append_failed_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_SPOT,
            dataset,
            "stock_zh_a_spot_em",
            "*",
            {"trade_date": trade_date},
            ak_client,
            error_type(exc),
            str(exc),
            started_at,
            ended_at,
        )
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_SPOT,
                dataset,
                "*",
                trade_date,
                trade_date,
                started_at,
                ended_at,
                stack,
                output_path,
            )
        )
        log_spot_progress(1, 2, metadata[-1][0])
        update_daily_bar_from_spot = bool(config.get("datasets.akshare_cn_stock_spot_quote.update_daily_bar_from_spot", True))
        logger.info("AkShare spot fallback started trade_date={} reason={}", trade_date, str(exc))
        fallback_metadata_start = len(metadata)
        _run_sina_fallback(store, ak_client, trade_date, str(exc), metadata, update_daily_bar_from_spot)
        if len(metadata) > fallback_metadata_start:
            log_spot_progress(2, 2, metadata[fallback_metadata_start][0])
    else:
        raw_path: Path | None = None
        try:
            raw_path = write_raw_response(store.root, response, started_at)
            output_path = store.write_stock_spot_quote_eastmoney(trade_date, response.data)
            daily_bar_rows = _drop_delisted_daily_bar_rows(store, spot_em_to_daily_bar_unadjusted(response.data))
            update_daily_bar_from_spot = bool(config.get("datasets.akshare_cn_stock_spot_quote.update_daily_bar_from_spot", True))
            daily_bar_output_path = (
                _write_spot_daily_bar_rows(store, daily_bar_rows)
                if update_daily_bar_from_spot
                else store.parquet_dir / akshare_daily_bar_dataset_id("unadjusted")
            )
            ended_at = datetime.now()
            append_response_manifest(
                store,
                PIPELINE_UPDATE_AKSHARE_SPOT,
                dataset,
                "*",
                response,
                raw_path,
                "success",
                "",
                "",
                started_at,
                ended_at,
            )
            metadata.append(
                success_metadata(
                    PIPELINE_UPDATE_AKSHARE_SPOT,
                    dataset,
                    "*",
                    trade_date,
                    trade_date,
                    started_at,
                    ended_at,
                    len(response.data),
                    output_path,
                )
            )
            log_spot_progress(1, 1, metadata[-1][0])
            if update_daily_bar_from_spot:
                daily_bar_dataset = akshare_daily_bar_dataset_id("unadjusted")
                metadata.append(
                    success_metadata(
                        PIPELINE_UPDATE_AKSHARE_SPOT,
                        daily_bar_dataset,
                        "*",
                        trade_date,
                        trade_date,
                        started_at,
                        ended_at,
                        len(daily_bar_rows),
                        daily_bar_output_path,
                    )
                )
        except Exception as exc:
            ended_at = datetime.now()
            stack = error_stack(exc)
            append_response_manifest(
                store,
                PIPELINE_UPDATE_AKSHARE_SPOT,
                dataset,
                "*",
                response,
                raw_path,
                "failed",
                error_type(exc),
                str(exc),
                started_at,
                ended_at,
            )
            metadata.append(
                failed_metadata(
                    PIPELINE_UPDATE_AKSHARE_SPOT,
                    dataset,
                    "*",
                    trade_date,
                    trade_date,
                    started_at,
                    ended_at,
                    stack,
                    output_path,
                )
            )
            log_spot_progress(1, 1, metadata[-1][0])

    records = persist_metadata(store, metadata)
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=progress_success > 0)
    logger.info(
        "AkShare spot update completed processed={} success={} failed={} skipped={}",
        progress_processed,
        progress_success,
        progress_failed,
        progress_skipped,
    )
    return records


def spot_em_to_daily_bar_unadjusted(df: pd.DataFrame) -> pd.DataFrame:
    schema = schema_for_dataset(akshare_daily_bar_dataset_id("unadjusted"))
    if df.empty:
        return pd.DataFrame(columns=field_names(schema))
    daily_bar = pd.DataFrame(
        {
            "date": df["trade_date"],
            "code": df["code"],
            "source_symbol": df["source_symbol"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["last_price"],
            "volume": pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64"),
            "amount": df["amount"],
            "amplitude": df["amplitude"],
            "pct_change": df["pct_change"],
            "price_change": df["price_change"],
            "turnover_rate": df["turnover_rate"],
            "adjustment": "unadjusted",
            "source_endpoint": "stock_zh_a_spot_em",
            "quality_status": "spot_quote_close",
            "fetched_at": df["fetched_at"],
        }
    )
    return daily_bar[field_names(schema)].reset_index(drop=True)


def spot_sina_to_daily_bar_unadjusted(df: pd.DataFrame) -> pd.DataFrame:
    schema = schema_for_dataset(akshare_daily_bar_dataset_id("unadjusted"))
    if df.empty:
        return pd.DataFrame(columns=field_names(schema))
    daily_bar = pd.DataFrame(
        {
            "date": df["trade_date"],
            "code": df["code"],
            "source_symbol": df["source_symbol"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["last_price"],
            "volume": pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64"),
            "amount": df["amount"],
            "amplitude": pd.NA,
            "pct_change": df["pct_change"],
            "price_change": df["price_change"],
            "turnover_rate": pd.NA,
            "adjustment": "unadjusted",
            "source_endpoint": "stock_zh_a_spot",
            "quality_status": "spot_quote_close",
            "fetched_at": df["fetched_at"],
        }
    )
    return daily_bar[field_names(schema)].reset_index(drop=True)


def _write_spot_daily_bar_rows(store: ParquetStore, daily_bar_rows: pd.DataFrame) -> Path:
    dataset = akshare_daily_bar_dataset_id("unadjusted")
    dataset_dir = store.parquet_dir / dataset
    if daily_bar_rows.empty:
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return dataset_dir
    
    stats = store.append_akshare_daily_bar_batch("unadjusted", daily_bar_rows, skip_existing=True)
    
    if stats['skipped'] > 0:
        logger.info(
            "Spot daily-bar batch append completed updated={} skipped={}",
            stats['updated'],
            stats['skipped'],
        )
    
    return dataset_dir


def _run_sina_fallback(
    store: ParquetStore,
    client: Any,
    trade_date: str,
    fallback_reason: str,
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]],
    update_daily_bar_from_spot: bool,
) -> None:
    dataset = AKSHARE_SPOT_QUOTE_SINA_DATASET.name
    output_path = store.akshare_cn_stock_spot_quote_sina_path(trade_date)
    started_at = datetime.now()
    try:
        response = client.fetch_spot_quote_sina(
            trade_date=trade_date,
            fallback_reason=fallback_reason,
        )
        raw_path = write_raw_response(store.root, response, started_at)
        output_path = store.write_stock_spot_quote_sina(trade_date, response.data)
        daily_bar_rows = spot_sina_to_daily_bar_unadjusted(response.data)
        daily_bar_output_path = (
            _write_spot_daily_bar_rows(store, daily_bar_rows)
            if update_daily_bar_from_spot
            else store.parquet_dir / akshare_daily_bar_dataset_id("unadjusted")
        )
        ended_at = datetime.now()
        append_response_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_SPOT,
            dataset,
            "*",
            response,
            raw_path,
            "success",
            "",
            "",
            started_at,
            ended_at,
        )
        metadata.append(
            success_metadata(
                PIPELINE_UPDATE_AKSHARE_SPOT,
                dataset,
                "*",
                trade_date,
                trade_date,
                started_at,
                ended_at,
                len(response.data),
                output_path,
            )
        )
        if update_daily_bar_from_spot:
            daily_bar_dataset = akshare_daily_bar_dataset_id("unadjusted")
            metadata.append(
                success_metadata(
                    PIPELINE_UPDATE_AKSHARE_SPOT,
                    daily_bar_dataset,
                    "*",
                    trade_date,
                    trade_date,
                    started_at,
                    ended_at,
                    len(daily_bar_rows),
                    daily_bar_output_path,
                )
            )
    except Exception as exc:
        ended_at = datetime.now()
        stack = error_stack(exc)
        append_failed_manifest(
            store,
            PIPELINE_UPDATE_AKSHARE_SPOT,
            dataset,
            "stock_zh_a_spot",
            "*",
            {"trade_date": trade_date, "fallback_reason": fallback_reason},
            client,
            error_type(exc),
            str(exc),
            started_at,
            ended_at,
        )
        metadata.append(
            failed_metadata(
                PIPELINE_UPDATE_AKSHARE_SPOT,
                dataset,
                "*",
                trade_date,
                trade_date,
                started_at,
                ended_at,
                stack,
                output_path,
            )
        )


def _resolve_trade_date(config: ConfigManager, end: str | date | None) -> str:
    candidate = _date_iso(end, default_candidate_date(config)) if end is not None else default_candidate_date(config)
    store = ParquetStore(root=config.root)
    try:
        baostock_cn_trading_calendar_df = store.read_baostock_cn_trading_calendar()
    except Exception:
        raise RuntimeError(
            "Cannot resolve trade date without baostock_cn_trading_calendar data. "
            "Please run baostock_cn_trading_calendar update first."
        )
    if is_trading_day(baostock_cn_trading_calendar_df, candidate):
        return candidate
    if end is not None and baostock_cn_trading_calendar_df.empty:
        logger.warning(
            "Spot trade_date uses explicit end={} because baostock_cn_trading_calendar is empty",
            candidate,
        )
        return candidate
    try:
        resolved = latest_trading_day_on_or_before(baostock_cn_trading_calendar_df, candidate)
        logger.info(
            "Spot trade_date resolved from {} to {} (non-trading day)",
            candidate,
            resolved,
        )
        return resolved
    except ValueError:
        raise RuntimeError(
            f"No trading day found on or before {candidate}. "
            "Please check baostock_cn_trading_calendar data."
        )


def _ensure_spot_quote_close_window(
    config: ConfigManager,
    now: Callable[[], datetime] | None = None,
    store: ParquetStore | None = None,
) -> None:
    timezone_name = str(config.get("project.timezone", "Asia/Shanghai"))
    local_zone = ZoneInfo(timezone_name)
    current = now() if now is not None else datetime.now(local_zone)
    if current.tzinfo is None:
        local_now = current.replace(tzinfo=local_zone)
    else:
        local_now = current.astimezone(local_zone)
    current_time = local_now.time()
    current_date = local_now.date()

    if current_time >= time(18, 0) or current_time < time(8, 0):
        return

    if store is None:
        store = ParquetStore(root=config.root)

    try:
        baostock_cn_trading_calendar_df = store.read_baostock_cn_trading_calendar()
    except Exception:
        raise RuntimeError(
            "stock_zh_a_spot_em/stock_zh_a_spot cannot verify trading day "
            "without baostock_cn_trading_calendar data. Please run baostock_cn_trading_calendar update first."
        )

    if not is_trading_day(baostock_cn_trading_calendar_df, current_date):
        return

    raise RuntimeError(
        "stock_zh_a_spot_em/stock_zh_a_spot can only write daily bars after 18:00 "
        "and before 08:00 Asia/Shanghai on trading days"
    )


def _drop_delisted_daily_bar_rows(store: ParquetStore, daily_bar_rows: pd.DataFrame) -> pd.DataFrame:
    delist_df = store.read_latest_akshare_cn_stock_delist_sh()
    if daily_bar_rows.empty or delist_df.empty or "code" not in delist_df.columns:
        return daily_bar_rows
    delisted = set(normalize_akshare_code_list(delist_df["code"].dropna().astype(str).tolist()))
    if not delisted:
        return daily_bar_rows
    codes = daily_bar_rows["code"].astype("string").map(lambda value: str(value).strip())
    return daily_bar_rows.loc[~codes.isin(delisted)].reset_index(drop=True)


def _date_iso(value: str | date | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return pd.to_datetime(value, errors="raise").date().isoformat()


