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
from src.pipeline.common import default_candidate_date, should_skip_checkpoint
from src.storage.dataset_catalog import (
    STOCK_ZH_A_SPOT_EM_DATASET,
    STOCK_ZH_A_SPOT_SINA_DATASET,
    stock_zh_a_hist_dataset_name,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.storage.schema import field_names, schema_for_dataset
from src.utils.config_mgr import ConfigManager


def update_akshare_spot(
    end: str | date | None = None,
    root: Path | None = None,
    resume: bool = True,
    force: bool = False,
    build_views: bool = True,
    client: Any | None = None,
    client_factory: Callable[[ConfigManager, pd.DataFrame], Any] | None = None,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, object]]:
    """Fetch stock_zh_a_spot_em and map successful close snapshots into hist_none."""

    config = ConfigManager(root)
    _ensure_spot_close_window(config, now)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    trade_date = _resolve_trade_date(config, end)
    dataset = STOCK_ZH_A_SPOT_EM_DATASET.name
    output_path = store.stock_zh_a_spot_em_path(trade_date)
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
        store.close()
        return []

    stock_basic_df = store.read_stock_basic()
    ak_client = client or (
        client_factory(config, stock_basic_df)
        if client_factory is not None
        else AkShareClient(config=config, stock_basic_df=stock_basic_df)
    )
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]] = []

    started_at = datetime.now()
    try:
        response = ak_client.fetch_stock_zh_a_spot_em(trade_date=trade_date)
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
        update_hist_from_spot = bool(config.get("datasets.stock_zh_a_spot.update_hist_from_spot", True))
        _run_sina_fallback(store, ak_client, trade_date, str(exc), metadata, update_hist_from_spot)
    else:
        raw_path: Path | None = None
        try:
            raw_path = write_raw_response(store.root, response, started_at)
            output_path = store.write_stock_zh_a_spot_em(trade_date, response.data)
            hist_rows = _drop_delisted_hist_rows(store, spot_em_to_hist_none(response.data))
            update_hist_from_spot = bool(config.get("datasets.stock_zh_a_spot.update_hist_from_spot", True))
            hist_output_path = (
                _write_spot_hist_rows(store, hist_rows)
                if update_hist_from_spot
                else store.parquet_dir / stock_zh_a_hist_dataset_name("none")
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
            if update_hist_from_spot:
                hist_dataset = stock_zh_a_hist_dataset_name("none")
                metadata.append(
                    success_metadata(
                        PIPELINE_UPDATE_AKSHARE_SPOT,
                        hist_dataset,
                        "*",
                        trade_date,
                        trade_date,
                        started_at,
                        ended_at,
                        len(hist_rows),
                        hist_output_path,
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

    records = persist_metadata(store, metadata)
    store.close()
    if build_views:
        DuckDBStore(root=config.root).build_views()
    return records


def spot_em_to_hist_none(df: pd.DataFrame) -> pd.DataFrame:
    schema = schema_for_dataset(stock_zh_a_hist_dataset_name("none"))
    if df.empty:
        return pd.DataFrame(columns=field_names(schema))
    hist = pd.DataFrame(
        {
            "date": df["trade_date"],
            "code": df["code"],
            "source_symbol": df["source_symbol"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["latest_price"],
            "volume": pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64"),
            "amount": df["amount"],
            "amplitude": df["amplitude"],
            "pct_chg": df["pct_chg"],
            "change_amount": df["change_amount"],
            "turnover_rate": df["turnover_rate"],
            "adjust": "none",
            "source_endpoint": "stock_zh_a_spot_em",
            "quality_status": "spot_close",
            "fetched_at": df["fetched_at"],
        }
    )
    return hist[field_names(schema)].reset_index(drop=True)


def spot_sina_to_hist_none(df: pd.DataFrame) -> pd.DataFrame:
    schema = schema_for_dataset(stock_zh_a_hist_dataset_name("none"))
    if df.empty:
        return pd.DataFrame(columns=field_names(schema))
    hist = pd.DataFrame(
        {
            "date": df["trade_date"],
            "code": df["code"],
            "source_symbol": df["source_symbol"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["latest_price"],
            "volume": pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64"),
            "amount": df["amount"],
            "amplitude": pd.NA,
            "pct_chg": df["pct_chg"],
            "change_amount": df["change_amount"],
            "turnover_rate": pd.NA,
            "adjust": "none",
            "source_endpoint": "stock_zh_a_spot",
            "quality_status": "spot_close",
            "fetched_at": df["fetched_at"],
        }
    )
    return hist[field_names(schema)].reset_index(drop=True)


def _write_spot_hist_rows(store: ParquetStore, hist_rows: pd.DataFrame) -> Path:
    dataset = stock_zh_a_hist_dataset_name("none")
    dataset_dir = store.parquet_dir / dataset
    if hist_rows.empty:
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return dataset_dir
    for code, group in hist_rows.groupby("code", dropna=False, sort=False):
        if pd.isna(code) or str(code).strip() == "":
            continue
        store.upsert_stock_zh_a_hist("none", str(code), group.reset_index(drop=True))
    return dataset_dir


def _run_sina_fallback(
    store: ParquetStore,
    client: Any,
    trade_date: str,
    fallback_reason: str,
    metadata: list[tuple[dict[str, object], dict[str, object], dict[str, object]]],
    update_hist_from_spot: bool,
) -> None:
    dataset = STOCK_ZH_A_SPOT_SINA_DATASET.name
    output_path = store.stock_zh_a_spot_sina_path(trade_date)
    started_at = datetime.now()
    try:
        response = client.fetch_stock_zh_a_spot_sina(
            trade_date=trade_date,
            fallback_reason=fallback_reason,
        )
        raw_path = write_raw_response(store.root, response, started_at)
        output_path = store.write_stock_zh_a_spot_sina(trade_date, response.data)
        hist_rows = spot_sina_to_hist_none(response.data)
        hist_output_path = (
            _write_spot_hist_rows(store, hist_rows)
            if update_hist_from_spot
            else store.parquet_dir / stock_zh_a_hist_dataset_name("none")
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
        if update_hist_from_spot:
            hist_dataset = stock_zh_a_hist_dataset_name("none")
            metadata.append(
                success_metadata(
                    PIPELINE_UPDATE_AKSHARE_SPOT,
                    hist_dataset,
                    "*",
                    trade_date,
                    trade_date,
                    started_at,
                    ended_at,
                    len(hist_rows),
                    hist_output_path,
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
    if end is not None:
        return _date_iso(end, default_candidate_date(config))
    return default_candidate_date(config)


def _ensure_spot_close_window(config: ConfigManager, now: Callable[[], datetime] | None = None) -> None:
    timezone_name = str(config.get("project.timezone", "Asia/Shanghai"))
    local_zone = ZoneInfo(timezone_name)
    current = now() if now is not None else datetime.now(local_zone)
    if current.tzinfo is None:
        local_now = current.replace(tzinfo=local_zone)
    else:
        local_now = current.astimezone(local_zone)
    current_time = local_now.time()
    if current_time >= time(18, 0) or current_time < time(8, 0):
        return
    raise RuntimeError(
        "stock_zh_a_spot_em/stock_zh_a_spot can only write hist after 18:00 "
        "and before 08:00 Asia/Shanghai"
    )


def _drop_delisted_hist_rows(store: ParquetStore, hist_rows: pd.DataFrame) -> pd.DataFrame:
    delist_df = store.read_latest_stock_info_sh_delist()
    if hist_rows.empty or delist_df.empty or "code" not in delist_df.columns:
        return hist_rows
    delisted = set(normalize_akshare_code_list(delist_df["code"].dropna().astype(str).tolist()))
    if not delisted:
        return hist_rows
    codes = hist_rows["code"].astype("string").map(lambda value: str(value).strip())
    return hist_rows.loc[~codes.isin(delisted)].reset_index(drop=True)


def _date_iso(value: str | date | None, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return pd.to_datetime(value, errors="raise").date().isoformat()
