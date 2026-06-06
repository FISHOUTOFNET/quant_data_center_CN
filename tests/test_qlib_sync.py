from __future__ import annotations

import math
import struct
import time
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import src.sources.qlib.sync as qlib_sync_module
from src.sources.qlib.sync import (
    QlibRemoteAsset,
    QlibSyncTimeoutError,
    is_qlib_update_day,
    load_qlib_feature_series,
    load_qlib_symbol_features,
    sync_qlib_data,
    write_qlib_sync_state,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore


def test_load_qlib_feature_series_aligns_start_index_to_calendar(tmp_path: Path) -> None:
    qlib_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-02", "2024-01-03", "2024-01-04"],
        instruments={"all.txt": [("SH600000", "2024-01-03", "2024-01-04")]},
        features={"sh600000": {"close": (1, [8.2, math.nan])}},
    )

    series = load_qlib_feature_series(qlib_dir / "features" / "sh600000" / "close.day.bin", _calendar(qlib_dir))

    assert series.to_dict("records") == [
        {"date": date(2024, 1, 3), "value": 8.2},
        {"date": date(2024, 1, 4), "value": None},
    ]


def test_is_qlib_update_day_accepts_friday_through_sunday_only() -> None:
    assert is_qlib_update_day(date(2026, 5, 22)) is True
    assert is_qlib_update_day(date(2026, 5, 23)) is True
    assert is_qlib_update_day(date(2026, 5, 24)) is True
    assert is_qlib_update_day(date(2026, 5, 25)) is False


def test_load_qlib_symbol_features_aligns_fields_with_different_start_indexes(tmp_path: Path) -> None:
    qlib_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-02", "2024-01-03", "2024-01-04"],
        instruments={"all.txt": [("SH600000", "2024-01-02", "2024-01-04")]},
        features={
            "sh600000": {
                "open": (0, [1.1111114, math.nan]),
                "close": (1, [2.2222226, 3.3333334]),
                "volume": (2, [100.0]),
            }
        },
    )

    frame = load_qlib_symbol_features(qlib_dir / "features" / "sh600000", _calendar(qlib_dir))

    assert list(frame.columns) == [
        "date",
        "qlib_symbol",
        "exchange",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
        "change",
        "vwap",
        "adjclose",
    ]
    assert frame[["date", "qlib_symbol", "exchange", "code", "open", "close", "volume"]].to_dict("records") == [
        {
            "date": date(2024, 1, 2),
            "qlib_symbol": "sh600000",
            "exchange": "sh",
            "code": "600000",
            "open": 1.111111,
            "close": None,
            "volume": None,
        },
        {
            "date": date(2024, 1, 3),
            "qlib_symbol": "sh600000",
            "exchange": "sh",
            "code": "600000",
            "open": None,
            "close": 2.222223,
            "volume": None,
        },
        {
            "date": date(2024, 1, 4),
            "qlib_symbol": "sh600000",
            "exchange": "sh",
            "code": "600000",
            "open": None,
            "close": 3.333333,
            "volume": 100.0,
        },
    ]


def test_sync_qlib_skips_download_when_source_and_project_cover_target(tmp_path: Path) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-04", "2024-01-05"],
        instruments={"all.txt": [("SH600000", "2024-01-04", "2024-01-05")]},
        features={"sh600000": {"close": (0, [8.1, 8.2])}},
    )
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("qlib_cn_calendar_day", pd.DataFrame([{"calendar_date": "2024-01-05"}]))

    def fail_download(*args, **kwargs) -> None:
        raise AssertionError("download should not be called")

    result = sync_qlib_data(
        root=tmp_path,
        source_dir=source_dir,
        target_date="2024-01-05",
        download_and_extract=fail_download,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
    )

    assert result.status == "checked_current"
    assert result.downloaded is False
    assert result.synced is False


def test_sync_qlib_converts_source_when_project_lags(tmp_path: Path) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-04", "2024-01-05"],
        instruments={"all.txt": [("SH600000", "2024-01-04", "2024-01-05")]},
        features={"sh600000": {"close": (0, [8.1, 8.2]), "volume": (0, [100.0, 120.0])}},
    )

    result = sync_qlib_data(
        root=tmp_path,
        source_dir=source_dir,
        target_date="2024-01-05",
        download_and_extract=lambda *args, **kwargs: None,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
    )

    store = ParquetStore(root=tmp_path)
    calendar = store.read_dataset("qlib_cn_calendar_day")
    instruments = store.read_dataset("qlib_cn_instrument_membership")
    features = store.read_dataset("qlib_cn_stock_features_day", {"qlib_symbol": "sh600000"})
    state = pd.read_parquet(tmp_path / "data" / "metadata" / "qlib_sync_state.parquet")

    assert result.status == "synced"
    assert calendar["calendar_date"].max() == date(2024, 1, 5)
    assert instruments[["universe", "qlib_symbol", "exchange", "code"]].to_dict("records") == [
        {"universe": "all", "qlib_symbol": "sh600000", "exchange": "sh", "code": "600000"}
    ]
    assert features[["date", "qlib_symbol", "close", "volume"]].to_dict("records") == [
        {"date": date(2024, 1, 4), "qlib_symbol": "sh600000", "close": 8.1, "volume": 100.0},
        {"date": date(2024, 1, 5), "qlib_symbol": "sh600000", "close": 8.2, "volume": 120.0},
    ]
    assert state.iloc[-1]["status"] == "synced"


def test_sync_qlib_resumes_feature_sync_without_rewriting_current_partitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-04", "2024-01-05"],
        instruments={"all.txt": [("SH600000", "2024-01-04", "2024-01-05"), ("SZ000001", "2024-01-04", "2024-01-05")]},
        features={
            "sh600000": {"close": (0, [8.1, 8.2])},
            "sz000001": {"close": (0, [9.1, 9.2])},
        },
    )
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "qlib_cn_stock_features_day",
        pd.DataFrame(
            [
                {
                    "date": date(2024, 1, 4),
                    "qlib_symbol": "sh600000",
                    "exchange": "sh",
                    "code": "600000",
                    "close": 8.1,
                },
                {
                    "date": date(2024, 1, 5),
                    "qlib_symbol": "sh600000",
                    "exchange": "sh",
                    "code": "600000",
                    "close": 8.2,
                },
            ]
        ),
        {"qlib_symbol": "sh600000"},
    )

    loaded_symbols: list[str] = []
    original_loader = qlib_sync_module.load_qlib_symbol_features

    def counted_loader(symbol_dir: Path, calendar: list[date]) -> pd.DataFrame:
        loaded_symbols.append(symbol_dir.name)
        return original_loader(symbol_dir, calendar)

    monkeypatch.setattr(qlib_sync_module, "load_qlib_symbol_features", counted_loader)

    sync_qlib_data(
        root=tmp_path,
        source_dir=source_dir,
        target_date="2024-01-05",
        download_and_extract=lambda *args, **kwargs: None,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
    )

    assert loaded_symbols == ["sz000001"]


def test_sync_qlib_stops_when_runtime_budget_is_exhausted(tmp_path: Path) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-04", "2024-01-05"],
        instruments={"all.txt": [("SH600000", "2024-01-04", "2024-01-05")]},
        features={"sh600000": {"close": (0, [8.1, 8.2])}},
    )

    with pytest.raises(QlibSyncTimeoutError, match="timed out"):
        sync_qlib_data(
            root=tmp_path,
            source_dir=source_dir,
            target_date="2024-01-05",
            download_and_extract=lambda *args, **kwargs: time.sleep(0.02),
            remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
            build_views=False,
            force_download=True,
            max_runtime_seconds=0.001,
        )


@pytest.mark.slow
def test_sync_qlib_parallel_workers_match_single_worker_output(tmp_path: Path) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-02", "2024-01-03", "2024-01-04"],
        instruments={
            "all.txt": [
                ("SH600000", "2024-01-02", "2024-01-04"),
                ("SZ000001", "2024-01-02", "2024-01-04"),
            ]
        },
        features={
            "sh600000": {"open": (0, [1.0, 1.1, 1.2]), "close": (0, [2.0, 2.1, 2.2])},
            "sz000001": {"open": (0, [3.0, 3.1, 3.2]), "close": (1, [4.1, 4.2])},
        },
    )
    single_root = tmp_path / "single"
    parallel_root = tmp_path / "parallel"

    sync_qlib_data(
        root=single_root,
        source_dir=source_dir,
        target_date="2024-01-04",
        download_and_extract=lambda *args, **kwargs: None,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
        workers=1,
    )
    sync_qlib_data(
        root=parallel_root,
        source_dir=source_dir,
        target_date="2024-01-04",
        download_and_extract=lambda *args, **kwargs: None,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
        workers=2,
    )

    single_store = ParquetStore(root=single_root)
    parallel_store = ParquetStore(root=parallel_root)
    for symbol in ["sh600000", "sz000001"]:
        single = single_store.read_dataset("qlib_cn_stock_features_day", {"qlib_symbol": symbol})
        parallel = parallel_store.read_dataset("qlib_cn_stock_features_day", {"qlib_symbol": symbol})
        pd.testing.assert_frame_equal(single, parallel)


def test_sync_qlib_does_not_redownload_same_stale_asset(tmp_path: Path) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-04"],
        instruments={"all.txt": [("SH600000", "2024-01-04", "2024-01-04")]},
        features={"sh600000": {"close": (0, [8.1])}},
    )
    write_qlib_sync_state(
        tmp_path,
        {
            "target_date": "2024-01-05",
            "source_latest_date": "2024-01-04",
            "project_latest_date": None,
            "status": "upstream_not_ready",
            "asset_id": "asset-1",
            "asset_etag": "etag-1",
            "asset_size": 10,
            "updated_at": datetime(2024, 1, 6, 10, 0),
        },
    )

    def fail_download(*args, **kwargs) -> None:
        raise AssertionError("same stale asset should not be downloaded twice")

    result = sync_qlib_data(
        root=tmp_path,
        source_dir=source_dir,
        target_date="2024-01-05",
        download_and_extract=fail_download,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
    )

    assert result.status == "upstream_not_ready"
    assert result.downloaded is False


def test_qlib_duckdb_views_can_be_created_and_queried(tmp_path: Path) -> None:
    source_dir = _write_qlib_source(
        tmp_path,
        calendar=["2024-01-05"],
        instruments={"all.txt": [("SH600000", "2024-01-05", "2024-01-05")]},
        features={"sh600000": {"close": (0, [8.2])}},
    )
    sync_qlib_data(
        root=tmp_path,
        source_dir=source_dir,
        target_date="2024-01-05",
        download_and_extract=lambda *args, **kwargs: None,
        remote_asset_provider=lambda: QlibRemoteAsset(asset_id="asset-1", etag="etag-1", size=10),
        build_views=False,
    )

    DuckDBStore(root=tmp_path).build_views()

    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        assert conn.execute("select max(calendar_date) from v_qlib_cn_calendar_day").fetchone() == (date(2024, 1, 5),)
        assert conn.execute(
            "select close from v_qlib_cn_stock_features_day where qlib_symbol='sh600000'"
        ).fetchone() == (8.2,)


def _calendar(qlib_dir: Path) -> list[date]:
    return [pd.Timestamp(item).date() for item in (qlib_dir / "calendars" / "day.txt").read_text().splitlines()]


def _write_qlib_source(
    tmp_path: Path,
    *,
    calendar: list[str],
    instruments: dict[str, list[tuple[str, str, str]]],
    features: dict[str, dict[str, tuple[int, list[float]]]],
) -> Path:
    root = tmp_path / "source" / "cn_data"
    (root / "calendars").mkdir(parents=True)
    (root / "instruments").mkdir(parents=True)
    (root / "features").mkdir(parents=True)
    (root / "calendars" / "day.txt").write_text("\n".join(calendar) + "\n", encoding="utf-8")
    for name, rows in instruments.items():
        (root / "instruments" / name).write_text(
            "".join(f"{symbol}\t{start}\t{end}\n" for symbol, start, end in rows),
            encoding="utf-8",
        )
    for symbol, fields in features.items():
        symbol_dir = root / "features" / symbol
        symbol_dir.mkdir()
        for field, (start_index, values) in fields.items():
            payload = struct.pack("<f", float(start_index)) + struct.pack(f"<{len(values)}f", *values)
            (symbol_dir / f"{field}.day.bin").write_bytes(payload)
    return root
