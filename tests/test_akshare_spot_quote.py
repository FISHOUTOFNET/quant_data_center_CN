from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.sources.akshare.pipeline.spot_quote import _upsert_spot_daily_bar_rows, _write_spot_daily_bar_rows
from src.storage.dataset_catalog import akshare_daily_bar_dataset_id
from src.storage.parquet_store import ParquetStore


def test_spot_daily_bar_upsert_skips_existing_partitions_without_rewrite(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    dataset = akshare_daily_bar_dataset_id("unadjusted")
    rows = pd.DataFrame(
        [
            _spot_daily_bar_row("600000", date(2024, 1, 3), close=8.3),
            _spot_daily_bar_row("000001", datetime(2024, 1, 3, 15, 0), close=11.2),
        ]
    )

    result = _upsert_spot_daily_bar_rows(store, dataset, rows, skip_existing=True)

    assert result.row_count == 2
    assert result.updated_partitions == 2
    assert result.skipped_partitions == 0
    assert set(store.list_dataset_partitions(dataset)) == {"000001", "600000"}

    atomic_write_calls: list[Path] = []
    original_atomic_write = ParquetStore.atomic_write

    def counted_atomic_write(self, df, schema, destination):
        atomic_write_calls.append(destination)
        return original_atomic_write(self, df, schema, destination)

    monkeypatch.setattr(ParquetStore, "atomic_write", counted_atomic_write)

    skipped = _upsert_spot_daily_bar_rows(
        store,
        dataset,
        pd.DataFrame(
            [
                _spot_daily_bar_row("600000", "2024-01-03", close=8.3),
                _spot_daily_bar_row("000001", date(2024, 1, 3), close=11.2),
            ]
        ),
        skip_existing=True,
    )

    assert skipped.row_count == 0
    assert skipped.updated_partitions == 0
    assert skipped.skipped_partitions == 2
    assert atomic_write_calls == []
    _assert_no_duplicate_daily_bar_keys(store, dataset, "600000")
    _assert_no_duplicate_daily_bar_keys(store, dataset, "000001")


def test_spot_daily_bar_upsert_updates_only_changed_existing_code_partition(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    dataset = akshare_daily_bar_dataset_id("unadjusted")
    _upsert_spot_daily_bar_rows(
        store,
        dataset,
        pd.DataFrame(
            [
                _spot_daily_bar_row("600000", "2024-01-03", close=8.3),
                _spot_daily_bar_row("000001", "2024-01-03", close=11.2),
            ]
        ),
        skip_existing=True,
    )

    written: list[Path] = []
    original_atomic_write = ParquetStore.atomic_write

    def counted_atomic_write(self, df, schema, destination):
        written.append(destination)
        return original_atomic_write(self, df, schema, destination)

    monkeypatch.setattr(ParquetStore, "atomic_write", counted_atomic_write)

    result = _upsert_spot_daily_bar_rows(
        store,
        dataset,
        pd.DataFrame([_spot_daily_bar_row("600000", datetime(2024, 1, 4, 15, 0), close=8.4)]),
        skip_existing=True,
    )

    assert result.row_count == 1
    assert result.updated_partitions == 1
    assert result.skipped_partitions == 0
    assert written == [store.dataset_path(dataset, {"code": "600000"})]
    assert len(store.read_dataset(dataset, {"code": "600000"})) == 2
    assert len(store.read_dataset(dataset, {"code": "000001"})) == 1
    _assert_no_duplicate_daily_bar_keys(store, dataset, "600000")


def test_spot_daily_bar_upsert_adds_only_new_code_partition(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    dataset = akshare_daily_bar_dataset_id("unadjusted")
    _upsert_spot_daily_bar_rows(
        store,
        dataset,
        pd.DataFrame([_spot_daily_bar_row("600000", "2024-01-03", close=8.3)]),
        skip_existing=True,
    )

    written: list[Path] = []
    original_atomic_write = ParquetStore.atomic_write

    def counted_atomic_write(self, df, schema, destination):
        written.append(destination)
        return original_atomic_write(self, df, schema, destination)

    monkeypatch.setattr(ParquetStore, "atomic_write", counted_atomic_write)

    result = _upsert_spot_daily_bar_rows(
        store,
        dataset,
        pd.DataFrame([_spot_daily_bar_row("300001", date(2024, 1, 3), close=21.5)]),
        skip_existing=True,
    )

    assert result.row_count == 1
    assert result.updated_partitions == 1
    assert result.skipped_partitions == 0
    assert written == [store.dataset_path(dataset, {"code": "300001"})]
    assert set(store.list_dataset_partitions(dataset)) == {"300001", "600000"}
    _assert_no_duplicate_daily_bar_keys(store, dataset, "300001")


def test_write_spot_daily_bar_rows_uses_optimized_upsert_path(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    dataset = akshare_daily_bar_dataset_id("unadjusted")

    dataset_dir = _write_spot_daily_bar_rows(
        store,
        pd.DataFrame(
            [
                _spot_daily_bar_row("600000", "2024-01-03", close=8.3),
                _spot_daily_bar_row("000001", "2024-01-03", close=11.2),
            ]
        ),
    )

    assert dataset_dir == store.parquet_dir / dataset
    assert set(store.list_dataset_partitions(dataset)) == {"000001", "600000"}


def _spot_daily_bar_row(code: str, trade_date: object, *, close: float) -> dict[str, object]:
    return {
        "date": trade_date,
        "code": code,
        "source_symbol": code,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": 120000,
        "amount": 9960.0,
        "amplitude": 3.0,
        "pct_change": 1.2,
        "price_change": 0.1,
        "turnover_rate": 0.12,
        "adjustment": "unadjusted",
        "source_endpoint": "stock_zh_a_spot_em",
        "quality_status": "spot_quote_close",
        "fetched_at": datetime(2024, 1, 3, 16, 0),
    }


def _assert_no_duplicate_daily_bar_keys(store: ParquetStore, dataset: str, code: str) -> None:
    hist = store.read_dataset(dataset, {"code": code})
    keys = hist[["code", "date", "adjustment"]].astype("string")
    assert len(keys.drop_duplicates()) == len(keys)
