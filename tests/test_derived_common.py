from __future__ import annotations

import pandas as pd
import pytest

from src.sources.derived.common import read_partition_or_empty, safe_remove_derived_dataset_dir
from src.storage.parquet_store import ParquetStore


def test_safe_remove_derived_dataset_dir_only_allows_derived(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    derived_dir = tmp_path / "data" / "parquet" / "cn_stock_daily_bar" / "security_id=SH.600000"
    derived_dir.mkdir(parents=True)
    (derived_dir / "data.parquet").write_text("stale", encoding="utf-8")

    safe_remove_derived_dataset_dir(store, "cn_stock_daily_bar")

    assert (tmp_path / "data" / "parquet" / "cn_stock_daily_bar").is_dir()
    assert not derived_dir.exists()
    with pytest.raises(ValueError, match="non-derived"):
        safe_remove_derived_dataset_dir(store, "baostock_cn_stock_daily_bar_qfq")


def test_read_partition_or_empty_returns_schema_frame_for_missing_partition(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    frame = read_partition_or_empty(store, "akshare_cn_stock_daily_bar_unadjusted", "600000")

    assert frame.empty
    assert list(frame.columns) == list(store.empty_dataset_frame("akshare_cn_stock_daily_bar_unadjusted").columns)


def test_read_partition_or_empty_reads_non_partitioned_dataset(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame([{"calendar_date": "2024-01-02", "is_trading_day": "1"}]),
    )

    frame = read_partition_or_empty(store, "baostock_cn_trading_calendar", "ignored")

    assert len(frame) == 1
    assert str(frame.loc[0, "calendar_date"]) == "2024-01-02"
