"""Qlib binary data update and Parquet synchronization."""

from __future__ import annotations

import json
import math
import os
import shutil
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.pipeline.common import date_iso, default_candidate_date, latest_trading_day_on_or_before
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger

QLIB_DOWNLOAD_URL = "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz"
QLIB_RELEASE_API_URL = "https://api.github.com/repos/chenditc/investment_data/releases/latest"
QLIB_ASSET_NAME = "qlib_bin.tar.gz"
QLIB_SOURCE_DIR = Path.home() / ".qlib" / "qlib_data" / "cn_data"
QLIB_SYNC_STATE_FILE = "qlib_sync_state.parquet"
QLIB_FEATURE_FIELDS = ("open", "high", "low", "close", "volume", "amount", "factor", "change", "vwap", "adjclose")


@dataclass(frozen=True)
class QlibRemoteAsset:
    asset_id: str
    etag: str | None
    size: int | None
    download_url: str = QLIB_DOWNLOAD_URL


@dataclass(frozen=True)
class QlibSyncResult:
    status: str
    target_date: str
    source_latest_date: str | None
    project_latest_date: str | None
    downloaded: bool
    synced: bool


@dataclass(frozen=True)
class _QlibFeatureSyncResult:
    qlib_symbol: str
    status: str
    rows: int
    convert_seconds: float
    write_seconds: float


RemoteAssetProvider = Callable[[], QlibRemoteAsset]
DownloadAndExtract = Callable[[Path, QlibRemoteAsset, bool], None]
Deadline = float | None


class QlibSyncTimeoutError(TimeoutError):
    """Raised when a Qlib sync exceeds its configured runtime budget."""


def sync_qlib_data(
    *,
    root: Path | None = None,
    source_dir: Path | None = None,
    target_date: str | date | None = None,
    force_download: bool = False,
    build_views: bool = True,
    max_runtime_seconds: float | None = None,
    workers: int | None = None,
    remote_asset_provider: RemoteAssetProvider | None = None,
    download_and_extract: DownloadAndExtract | None = None,
) -> QlibSyncResult:
    deadline = _deadline_from_max_runtime(max_runtime_seconds)
    config = ConfigManager(root)
    base = config.root
    resolved_workers = _resolve_qlib_workers(config, workers)
    store = ParquetStore(root=base)
    store.ensure_layout()
    source = (source_dir or QLIB_SOURCE_DIR).expanduser().resolve()
    target = _resolve_target_date(config, store, target_date)
    remote_provider = remote_asset_provider or fetch_latest_qlib_asset
    downloader = download_and_extract or download_and_extract_qlib_asset

    source_latest = latest_qlib_calendar_date(source)
    project_latest = latest_project_qlib_date(store)
    _check_deadline(deadline, "check local qlib state")
    if not force_download and _covers_target(source_latest, target) and _covers_target(project_latest, target):
        _record_state(base, target, source_latest, project_latest, "checked_current", None)
        if build_views:
            _check_deadline(deadline, "build qlib views")
            DuckDBStore(root=base).build_views(cleanup_tmp_files=False)
        return QlibSyncResult("checked_current", target, source_latest, project_latest, False, False)

    downloaded = False
    remote_asset: QlibRemoteAsset | None = None
    if force_download or not _covers_target(source_latest, target):
        _check_deadline(deadline, "fetch qlib release metadata")
        remote_asset = remote_provider()
        if not force_download and _same_stale_asset(base, remote_asset, source_latest, target):
            _record_state(base, target, source_latest, project_latest, "upstream_not_ready", remote_asset)
            return QlibSyncResult("upstream_not_ready", target, source_latest, project_latest, False, False)
        if download_and_extract is None:
            download_and_extract_qlib_asset(source, remote_asset, force_download, deadline=deadline)
        else:
            downloader(source, remote_asset, force_download)
            _check_deadline(deadline, "download qlib asset")
        downloaded = True
        source_latest = latest_qlib_calendar_date(source)
        if not _covers_target(source_latest, target):
            _record_state(base, target, source_latest, project_latest, "upstream_not_ready", remote_asset)
            return QlibSyncResult("upstream_not_ready", target, source_latest, project_latest, downloaded, False)

    sync_source_to_parquet(source, store, deadline=deadline, workers=resolved_workers)
    project_latest = latest_project_qlib_date(store)
    status = "synced" if _covers_target(project_latest, target) else "synced_stale"
    _record_state(base, target, source_latest, project_latest, status, remote_asset)
    if build_views:
        _check_deadline(deadline, "build qlib views")
        DuckDBStore(root=base).build_views()
    return QlibSyncResult(status, target, source_latest, project_latest, downloaded, True)


def sync_source_to_parquet(
    source_dir: Path,
    store: ParquetStore,
    *,
    deadline: Deadline = None,
    workers: int | None = None,
) -> None:
    started = time.perf_counter()
    calendar = read_qlib_calendar(source_dir)
    store.write_dataset("qlib_cn_calendar_day", pd.DataFrame({"calendar_date": calendar}))
    store.write_dataset("qlib_cn_instrument_membership", read_qlib_instruments(source_dir))
    logger.info(
        "Qlib calendar and instruments synced elapsed={:.3f}s",
        time.perf_counter() - started,
    )

    dataset_dir = store.parquet_dir / "qlib_cn_stock_features_day"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    source_latest = max(calendar) if calendar else None
    symbol_dirs = [path for path in sorted((source_dir / "features").iterdir()) if path.is_dir()]
    expected_symbols = {normalize_qlib_symbol(path.name) for path in symbol_dirs}
    worker_count = max(int(workers or 1), 1)
    processed = 0
    skipped = 0
    empty = 0
    convert_seconds = 0.0
    write_seconds = 0.0
    feature_started = time.perf_counter()
    thread_state = threading.local()

    def thread_store() -> ParquetStore:
        local_store = getattr(thread_state, "store", None)
        if local_store is None:
            local_store = ParquetStore(root=store.root)
            thread_state.store = local_store
        return local_store

    def sync_one_symbol(symbol_dir: Path) -> _QlibFeatureSyncResult:
        _check_deadline(deadline, "sync qlib feature partitions")
        qlib_symbol = normalize_qlib_symbol(symbol_dir.name)
        if source_latest is not None and _qlib_feature_partition_covers_date(store, qlib_symbol, source_latest):
            return _QlibFeatureSyncResult(qlib_symbol, "skipped", 0, 0.0, 0.0)
        convert_started = time.perf_counter()
        frame = load_qlib_symbol_features(symbol_dir, calendar)
        converted = time.perf_counter() - convert_started
        if frame.empty:
            return _QlibFeatureSyncResult(qlib_symbol, "empty", 0, converted, 0.0)
        write_started = time.perf_counter()
        thread_store().write_dataset(
            "qlib_cn_stock_features_day",
            frame,
            {"qlib_symbol": frame["qlib_symbol"].iloc[0]},
        )
        return _QlibFeatureSyncResult(
            qlib_symbol,
            "processed",
            len(frame),
            converted,
            time.perf_counter() - write_started,
        )

    for result in _iter_qlib_feature_sync_results(symbol_dirs, sync_one_symbol, worker_count, deadline):
        if result.status == "processed":
            processed += 1
        elif result.status == "skipped":
            skipped += 1
        elif result.status == "empty":
            empty += 1
        convert_seconds += result.convert_seconds
        write_seconds += result.write_seconds
        total_seen = processed + skipped + empty
        if total_seen > 0 and total_seen % 200 == 0:
            logger.info(
                "Qlib feature sync progress processed={} skipped={} empty={} latest_source_date={}",
                processed,
                skipped,
                empty,
                source_latest,
            )
    _check_deadline(deadline, "clean stale qlib feature partitions")
    _remove_stale_qlib_feature_partitions(dataset_dir, expected_symbols)
    feature_elapsed = time.perf_counter() - feature_started
    updated = processed + empty
    logger.info(
        "Qlib feature sync completed processed={} skipped={} empty={} elapsed={:.3f}s "
        "convert_total={:.3f}s write_total={:.3f}s avg_updated={:.3f}s workers={}",
        processed,
        skipped,
        empty,
        feature_elapsed,
        convert_seconds,
        write_seconds,
        feature_elapsed / max(updated, 1),
        worker_count,
    )


def is_qlib_update_day(value: date | datetime | None = None) -> bool:
    current = value or datetime.now()
    current_date = current.date() if isinstance(current, datetime) else current
    return current_date.weekday() in {4, 5, 6}


def load_qlib_symbol_features(symbol_dir: Path, calendar: list[date]) -> pd.DataFrame:
    qlib_symbol = normalize_qlib_symbol(symbol_dir.name)
    exchange, code = split_qlib_symbol(qlib_symbol)
    field_values: dict[str, tuple[int, np.ndarray]] = {}
    min_index: int | None = None
    max_index: int | None = None

    for field in QLIB_FEATURE_FIELDS:
        path = symbol_dir / f"{field}.day.bin"
        if not path.exists():
            continue
        values = _read_qlib_values(path)
        if len(values) <= 1:
            continue
        start_index = int(values[0])
        field_start = max(start_index, 0)
        field_end = min(start_index + len(values) - 1, len(calendar))
        if field_start >= field_end:
            continue
        value_start = field_start - start_index + 1
        value_end = field_end - start_index + 1
        field_values[field] = (field_start, values[value_start:value_end].astype("float64", copy=False))
        min_index = field_start if min_index is None else min(min_index, field_start)
        max_index = field_end - 1 if max_index is None else max(max_index, field_end - 1)

    if min_index is None or max_index is None:
        return pd.DataFrame(columns=["date", "qlib_symbol", "exchange", "code", *QLIB_FEATURE_FIELDS])

    row_indexes = np.arange(min_index, max_index + 1)
    result = pd.DataFrame(
        {
            "date": [calendar[index] for index in row_indexes],
            "qlib_symbol": qlib_symbol,
            "exchange": exchange,
            "code": code,
        }
    )
    for field in QLIB_FEATURE_FIELDS:
        if field not in field_values:
            result[field] = None
            continue
        field_start, values = field_values[field]
        column = np.full(len(row_indexes), np.nan, dtype="float64")
        offset = field_start - min_index
        column[offset : offset + len(values)] = values
        column = np.round(column, 6)
        result[field] = pd.Series(column, dtype=object).where(~np.isnan(column), None)
    return result[["date", "qlib_symbol", "exchange", "code", *QLIB_FEATURE_FIELDS]]


def load_qlib_feature_series(path: Path, calendar: list[date]) -> pd.DataFrame:
    values = _read_qlib_values(path)
    if len(values) == 0:
        return pd.DataFrame(columns=["date", "value"])
    start_index = int(values[0])
    rows: list[dict[str, object]] = []
    for offset, raw_value in enumerate(values[1:]):
        calendar_index = start_index + offset
        if calendar_index < 0 or calendar_index >= len(calendar):
            continue
        value = None if math.isnan(raw_value) else round(float(raw_value), 6)
        rows.append({"date": calendar[calendar_index], "value": value})
    return pd.DataFrame(rows, columns=["date", "value"], dtype=object)


def read_qlib_calendar(source_dir: Path) -> list[date]:
    calendar_path = source_dir / "calendars" / "day.txt"
    if not calendar_path.exists():
        raise FileNotFoundError(f"Qlib calendar not found: {calendar_path}")
    return [
        pd.Timestamp(line.strip()).date()
        for line in calendar_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def read_qlib_instruments(source_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    instruments_dir = source_dir / "instruments"
    for path in sorted(instruments_dir.glob("*.txt")):
        universe = path.stem
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = [part.strip() for part in line.split("\t")]
            if len(parts) < 3 or not parts[0]:
                continue
            qlib_symbol = normalize_qlib_symbol(parts[0])
            exchange, code = split_qlib_symbol(qlib_symbol)
            rows.append(
                {
                    "universe": universe,
                    "qlib_symbol": qlib_symbol,
                    "exchange": exchange,
                    "code": code,
                    "start_date": date_iso(parts[1]),
                    "end_date": date_iso(parts[2]),
                }
            )
    return pd.DataFrame(rows, columns=["universe", "qlib_symbol", "exchange", "code", "start_date", "end_date"])


def latest_qlib_calendar_date(source_dir: Path) -> str | None:
    try:
        calendar = read_qlib_calendar(source_dir)
    except FileNotFoundError:
        return None
    return max(calendar).isoformat() if calendar else None


def latest_project_qlib_date(store: ParquetStore) -> str | None:
    df = store.read_dataset("qlib_cn_calendar_day")
    if df.empty:
        return None
    latest = pd.to_datetime(df["calendar_date"], errors="coerce").max()
    if pd.isna(latest):
        return None
    return latest.date().isoformat()


def fetch_latest_qlib_asset() -> QlibRemoteAsset:
    request = urllib.request.Request(QLIB_RELEASE_API_URL, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    for asset in payload.get("assets", []):
        if asset.get("name") == QLIB_ASSET_NAME:
            return QlibRemoteAsset(
                asset_id=str(asset.get("id") or asset.get("node_id") or asset.get("browser_download_url")),
                etag=asset.get("etag"),
                size=int(asset["size"]) if asset.get("size") is not None else None,
                download_url=str(asset.get("browser_download_url") or QLIB_DOWNLOAD_URL),
            )
    return QlibRemoteAsset(asset_id=str(payload.get("tag_name") or payload.get("id") or "latest"), etag=None, size=None)


def download_and_extract_qlib_asset(
    source_dir: Path,
    remote_asset: QlibRemoteAsset,
    force_download: bool,
    *,
    deadline: Deadline = None,
) -> None:
    del force_download
    started = time.perf_counter()
    _check_deadline(deadline, "download qlib asset")
    source_dir.parent.mkdir(parents=True, exist_ok=True)
    archive_path = source_dir.parent / QLIB_ASSET_NAME
    _download_file(remote_asset.download_url, archive_path, deadline=deadline)
    logger.info("Qlib asset downloaded elapsed={:.3f}s path={}", time.perf_counter() - started, archive_path)
    with tempfile.TemporaryDirectory(prefix="qdc_qlib_") as temp_name:
        temp_root = Path(temp_name)
        extract_started = time.perf_counter()
        _check_deadline(deadline, "extract qlib asset")
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(temp_root, filter="data")
        extracted = _find_extracted_qlib_dir(temp_root)
        logger.info("Qlib asset extracted elapsed={:.3f}s", time.perf_counter() - extract_started)
        replacement = source_dir.parent / f"{source_dir.name}.replacement"
        if replacement.exists():
            shutil.rmtree(replacement)
        replace_started = time.perf_counter()
        _check_deadline(deadline, "replace qlib source directory")
        shutil.copytree(extracted, replacement)
        if source_dir.exists():
            shutil.rmtree(source_dir)
        os.replace(replacement, source_dir)
        logger.info(
            "Qlib source directory replaced elapsed={:.3f}s path={}",
            time.perf_counter() - replace_started,
            source_dir,
        )
    archive_path.unlink(missing_ok=True)


def write_qlib_sync_state(root: Path, row: dict[str, object]) -> None:
    metadata_dir = root / "data" / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    path = metadata_dir / QLIB_SYNC_STATE_FILE
    incoming = pd.DataFrame([_state_row(row)])
    if path.exists():
        existing = pd.read_parquet(path)
        incoming = pd.concat([existing, incoming], ignore_index=True)
    incoming.to_parquet(path, index=False)


def read_qlib_sync_state(root: Path) -> pd.DataFrame:
    path = root / "data" / "metadata" / QLIB_SYNC_STATE_FILE
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def normalize_qlib_symbol(value: object) -> str:
    return str(value).strip().lower().replace(".", "")


def split_qlib_symbol(symbol: str) -> tuple[str, str]:
    normalized = normalize_qlib_symbol(symbol)
    if len(normalized) < 3:
        return "", normalized
    return normalized[:2], normalized[2:]


def _read_qlib_bin(path: Path) -> list[float]:
    return [float(item) for item in _read_qlib_bin_array(path)]


def _read_qlib_values(path: Path) -> np.ndarray:
    if path.suffix == ".pkl":
        return np.asarray(pd.read_pickle(path), dtype="float64")
    return _read_qlib_bin_array(path)


def _read_qlib_bin_array(path: Path) -> np.ndarray:
    raw = path.read_bytes()
    if len(raw) % 4 != 0:
        raise ValueError(f"Invalid Qlib binary length for {path}: {len(raw)}")
    return np.frombuffer(raw, dtype="<f4").astype("float64")


def _download_file(url: str, target_path: Path, *, deadline: Deadline = None) -> None:
    _check_deadline(deadline, "download qlib asset")
    existing_size = target_path.stat().st_size if target_path.exists() else 0
    headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=_remaining_timeout(deadline, 60)) as response:
            mode = "ab" if headers and response.status == 206 else "wb"
            with target_path.open(mode) as out:
                while True:
                    _check_deadline(deadline, "download qlib asset")
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
    except urllib.error.HTTPError as exc:
        if exc.code == 416:
            return
        raise


def _find_extracted_qlib_dir(root: Path) -> Path:
    for candidate in [root, *root.rglob("*")]:
        if all((candidate / name).exists() for name in ("calendars", "features", "instruments")):
            return candidate
    raise FileNotFoundError("Extracted archive does not contain calendars/features/instruments")


def _resolve_target_date(config: ConfigManager, store: ParquetStore, target_date: str | date | None) -> str:
    candidate = date_iso(target_date) if target_date is not None else default_candidate_date(config)
    calendar = store.read_dataset("baostock_cn_trading_calendar")
    if calendar.empty:
        return candidate
    return latest_trading_day_on_or_before(calendar, candidate)


def _covers_target(value: str | None, target_date: str) -> bool:
    return value is not None and value >= target_date


def _same_stale_asset(root: Path, remote_asset: QlibRemoteAsset, source_latest: str | None, target_date: str) -> bool:
    if _covers_target(source_latest, target_date):
        return False
    state = read_qlib_sync_state(root)
    if state.empty:
        return False
    last = state.iloc[-1]
    if str(last.get("status")) != "upstream_not_ready":
        return False
    return (
        str(last.get("asset_id")) == remote_asset.asset_id
        and _nullable_str(last.get("asset_etag")) == _nullable_str(remote_asset.etag)
        and _nullable_int(last.get("asset_size")) == _nullable_int(remote_asset.size)
    )


def _qlib_feature_partition_covers_date(store: ParquetStore, qlib_symbol: str, target_date: date) -> bool:
    path = store.dataset_path("qlib_cn_stock_features_day", {"qlib_symbol": qlib_symbol})
    if not path.exists():
        return False
    latest_from_stats = _latest_parquet_date_from_statistics(path)
    if latest_from_stats is not None:
        return latest_from_stats >= target_date
    try:
        df = pd.read_parquet(path, columns=["date"])
    except (FileNotFoundError, OSError, ValueError):
        return False
    if df.empty:
        return False
    latest = pd.to_datetime(df["date"], errors="coerce").max()
    return not pd.isna(latest) and latest.date() >= target_date


def _latest_parquet_date_from_statistics(path: Path) -> date | None:
    try:
        parquet_file = pq.ParquetFile(path)
        date_index = parquet_file.schema_arrow.names.index("date")
    except (FileNotFoundError, OSError, ValueError):
        return None
    latest: date | None = None
    metadata = parquet_file.metadata
    for row_group_index in range(metadata.num_row_groups):
        statistics = metadata.row_group(row_group_index).column(date_index).statistics
        if statistics is None or not statistics.has_min_max or statistics.max is None:
            return None
        value = _coerce_date(statistics.max)
        if value is None:
            return None
        latest = value if latest is None else max(latest, value)
    return latest


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    converted = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(converted):
        return None
    return converted.date()


def _remove_stale_qlib_feature_partitions(dataset_dir: Path, expected_symbols: set[str]) -> None:
    prefix = "qlib_symbol="
    for partition_dir in dataset_dir.glob(f"{prefix}*"):
        if not partition_dir.is_dir():
            continue
        if partition_dir.name.removeprefix(prefix) not in expected_symbols:
            shutil.rmtree(partition_dir)
            logger.info("Removed stale qlib feature partition: {}", partition_dir)


def _iter_qlib_feature_sync_results(
    symbol_dirs: list[Path],
    sync_one_symbol: Callable[[Path], _QlibFeatureSyncResult],
    workers: int,
    deadline: Deadline,
) -> list[_QlibFeatureSyncResult]:
    if workers <= 1:
        return [sync_one_symbol(symbol_dir) for symbol_dir in symbol_dirs]

    results: list[_QlibFeatureSyncResult] = []
    pending: dict[Future[_QlibFeatureSyncResult], Path] = {}
    symbol_iter = iter(symbol_dirs)
    max_in_flight = max(workers * 4, 1)

    def submit_until_full(executor: ThreadPoolExecutor) -> None:
        while len(pending) < max_in_flight:
            try:
                symbol_dir = next(symbol_iter)
            except StopIteration:
                return
            pending[executor.submit(sync_one_symbol, symbol_dir)] = symbol_dir

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="qlib-sync") as executor:
        submit_until_full(executor)
        while pending:
            _check_deadline(deadline, "sync qlib feature partitions")
            done, _ = wait(pending, timeout=_remaining_timeout(deadline, 1.0), return_when=FIRST_COMPLETED)
            if not done:
                continue
            for future in done:
                pending.pop(future)
                results.append(future.result())
            submit_until_full(executor)
    return results


def _resolve_qlib_workers(config: ConfigManager, workers: int | None) -> int:
    if workers is not None:
        return max(int(workers), 1)
    try:
        configured = config.get("pipeline.qlib_sync_workers", None)
        if configured is None:
            configured = config.get("pipeline.background_workers", 4)
    except Exception:
        configured = 4
    return max(int(configured), 1)


def _deadline_from_max_runtime(max_runtime_seconds: float | None) -> Deadline:
    if max_runtime_seconds is None:
        return None
    seconds = max(float(max_runtime_seconds), 0.001)
    return time.monotonic() + seconds


def _remaining_timeout(deadline: Deadline, default_seconds: float) -> float:
    if deadline is None:
        return default_seconds
    return max(min(default_seconds, deadline - time.monotonic()), 0.001)


def _check_deadline(deadline: Deadline, action: str) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise QlibSyncTimeoutError(f"Qlib sync timed out while trying to {action}")


def _record_state(
    root: Path,
    target_date: str,
    source_latest: str | None,
    project_latest: str | None,
    status: str,
    remote_asset: QlibRemoteAsset | None,
) -> None:
    write_qlib_sync_state(
        root,
        {
            "target_date": target_date,
            "source_latest_date": source_latest,
            "project_latest_date": project_latest,
            "status": status,
            "asset_id": remote_asset.asset_id if remote_asset is not None else None,
            "asset_etag": remote_asset.etag if remote_asset is not None else None,
            "asset_size": remote_asset.size if remote_asset is not None else None,
            "updated_at": datetime.now(),
        },
    )
    logger.info(
        "Qlib sync status={} target_date={} source_latest_date={} project_latest_date={}",
        status,
        target_date,
        source_latest,
        project_latest,
    )


def _state_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "target_date": row.get("target_date"),
        "source_latest_date": row.get("source_latest_date"),
        "project_latest_date": row.get("project_latest_date"),
        "status": row.get("status"),
        "asset_id": row.get("asset_id"),
        "asset_etag": row.get("asset_etag"),
        "asset_size": row.get("asset_size"),
        "updated_at": row.get("updated_at") or datetime.now(),
    }


def _nullable_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


def _nullable_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)
