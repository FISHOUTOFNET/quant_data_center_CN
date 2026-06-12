from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from src.sources.derived.stock_daily_bar import (
    _assign_source_rank,
    _filter_master_to_daily_source_candidates,
    _map_akshare_daily,
    _map_baostock_daily,
    build_cn_stock_daily_bar,
)
from src.storage.parquet_store import ParquetStore
from src.storage.schema import CN_STOCK_DAILY_BAR_SCHEMA

NOW = datetime(2024, 1, 5, 12, 0)
DAILY_SOURCE_DATASETS = {
    "baostock_cn_stock_daily_bar_unadjusted",
    "baostock_cn_stock_daily_bar_qfq",
    "baostock_cn_stock_daily_bar_hfq",
    "akshare_cn_stock_daily_bar_unadjusted",
    "akshare_cn_stock_daily_bar_qfq",
    "akshare_cn_stock_daily_bar_hfq",
}
CN_STOCK_DAILY_BAR_COLUMNS = CN_STOCK_DAILY_BAR_SCHEMA.names


def test_build_cn_stock_daily_bar_materializes_one_security_partition(tmp_path, daily_sample, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})
    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", _akshare_daily(close=99.0), {"code": "600000"})
    calls: list[tuple[str, object]] = []
    original_read_dataset = ParquetStore.read_dataset

    def spy_read_dataset(self: ParquetStore, dataset_id: str, partition=None):
        if dataset_id in DAILY_SOURCE_DATASETS:
            assert partition is not None
            calls.append((dataset_id, partition))
        return original_read_dataset(self, dataset_id, partition)

    monkeypatch.setattr(ParquetStore, "read_dataset", spy_read_dataset)

    result = build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_stock_daily_bar", {"security_id": "SH.600000"})

    assert result["rows"] == 2
    assert result["partitions"] == 1
    assert calls
    assert loaded["security_id"].unique().tolist() == ["SH.600000"]
    assert loaded.loc[loaded["date"] == date(2024, 1, 2), "close"].iloc[0] == 8.2
    assert loaded.loc[loaded["date"] == date(2024, 1, 2), "source_dataset"].iloc[0].startswith("baostock_")
    assert (tmp_path / "data" / "parquet" / "cn_stock_daily_bar" / "security_id=SH.600000" / "data.parquet").exists()


def test_build_cn_stock_daily_bar_builds_missing_master(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("akshare_cn_stock_spot_quote_eastmoney", _spot(), {"trade_date": "2024-01-05"})
    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", _akshare_daily(close=8.3), {"code": "600000"})

    result = build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    assert result["rows"] == 1
    assert store.dataset_exists("cn_security_master")
    assert store.dataset_exists("cn_stock_daily_bar", {"security_id": "SH.600000"})


def test_build_cn_stock_daily_bar_failure_keeps_existing_official_data(tmp_path, daily_sample, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})
    store.write_dataset(
        "cn_stock_daily_bar",
        _canonical_daily_bar(close=1.0, date_value=date(2023, 12, 29)),
        {"security_id": "SH.600000"},
        mode="replace",
    )

    def fail_materialize(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("daily staging build failed")

    monkeypatch.setattr("src.sources.derived.stock_daily_bar._materialize_security_daily_bar", fail_materialize)

    with pytest.raises(RuntimeError, match="daily staging build failed"):
        build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    loaded = store.read_dataset("cn_stock_daily_bar", {"security_id": "SH.600000"})
    assert loaded["close"].tolist() == [1.0]
    assert store.dataset_exists("cn_stock_daily_bar", {"security_id": "SH.600000"})
    assert _temporary_dataset_dirs(tmp_path, ".staging", "cn_stock_daily_bar") == []


def test_build_cn_stock_daily_bar_success_promotes_staging_dataset(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset(
        "cn_stock_daily_bar",
        _canonical_daily_bar(close=1.0, date_value=date(2023, 12, 29)),
        {"security_id": "SH.600000"},
        mode="replace",
    )
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})

    result = build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_stock_daily_bar", {"security_id": "SH.600000"})

    assert result["rows"] == 2
    assert loaded["close"].tolist() == [8.2, 8.3]
    assert date(2023, 12, 29) not in set(loaded["date"])
    assert _temporary_dataset_dirs(tmp_path, ".staging", "cn_stock_daily_bar") == []
    assert _temporary_dataset_dirs(tmp_path, ".backup", "cn_stock_daily_bar") == []


def test_map_baostock_daily_preserves_schema_order_and_missing_optional_fields() -> None:
    source = pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "open": 8.1,
                "high": 8.3,
                "low": 8.0,
                "close": 8.2,
            }
        ]
    )

    mapped = _map_baostock_daily(
        source,
        "baostock_cn_stock_daily_bar_unadjusted",
        "unadjusted",
        _master().iloc[0],
        NOW,
    )

    assert list(mapped.columns) == CN_STOCK_DAILY_BAR_COLUMNS
    assert mapped.loc[0, "security_id"] == "SH.600000"
    assert mapped.loc[0, "code"] == "600000"
    assert mapped.loc[0, "source_dataset"] == "baostock_cn_stock_daily_bar_unadjusted"
    assert mapped.loc[0, "source_endpoint"] == "query_history_k_data_plus"
    assert mapped.loc[0, "quality_status"] == "daily_bar_confirmed"
    assert mapped.loc[0, "close"] == 8.2
    assert pd.isna(mapped.loc[0, "prev_close"])
    assert pd.isna(mapped.loc[0, "volume"])


def test_map_akshare_daily_defaults_blank_source_fields_and_preserves_schema_order() -> None:
    source = pd.DataFrame(
        [
            {"date": date(2024, 1, 2), "close": 8.2, "source_endpoint": "", "quality_status": ""},
            {"date": date(2024, 1, 3), "close": 8.3, "source_endpoint": None, "quality_status": None},
            {"date": date(2024, 1, 4), "close": 8.4, "source_endpoint": pd.NA, "quality_status": pd.NA},
            {"date": date(2024, 1, 5), "close": 8.5, "source_endpoint": float("nan"), "quality_status": float("nan")},
            {"date": date(2024, 1, 6), "close": 8.6, "source_endpoint": "   ", "quality_status": "   "},
        ]
    )

    mapped = _map_akshare_daily(
        source,
        "akshare_cn_stock_daily_bar_unadjusted",
        "unadjusted",
        _master().iloc[0],
        NOW,
    )

    assert list(mapped.columns) == CN_STOCK_DAILY_BAR_COLUMNS
    assert mapped["source_endpoint"].tolist() == ["stock_zh_a_hist"] * 5
    assert mapped["quality_status"].tolist() == ["daily_bar_confirmed"] * 5
    assert mapped["prev_close"].isna().all()
    assert mapped["trade_status"].isna().all()
    assert mapped["is_st"].isna().all()


def test_assign_source_rank_prefers_baostock_then_akshare_quality() -> None:
    combined = pd.DataFrame(
        [
            {"source_dataset": "akshare_cn_stock_daily_bar_unadjusted", "quality_status": "daily_bar_confirmed"},
            {"source_dataset": "akshare_cn_stock_daily_bar_unadjusted", "quality_status": "spot_quote_close"},
            {"source_dataset": "akshare_cn_stock_daily_bar_unadjusted", "quality_status": "partial"},
            {"source_dataset": "baostock_cn_stock_daily_bar_unadjusted", "quality_status": "partial"},
        ]
    )

    assert _assign_source_rank(combined).tolist() == [1, 2, 3, 0]


def test_filter_master_to_daily_source_candidates_handles_blank_codes() -> None:
    master = pd.DataFrame(
        [
            {**_master().iloc[0].to_dict(), "security_id": "SH.600000", "baostock_code": "sh.600000"},
            {**_master().iloc[0].to_dict(), "security_id": "SZ.000001", "baostock_code": "   ", "akshare_code": pd.NA},
            {
                **_master().iloc[0].to_dict(),
                "security_id": "SZ.000002",
                "baostock_code": None,
                "akshare_code": "000002",
            },
        ]
    )
    partition_cache = {
        "baostock_cn_stock_daily_bar_unadjusted": {"sh.600000"},
        "baostock_cn_stock_daily_bar_qfq": set(),
        "baostock_cn_stock_daily_bar_hfq": set(),
        "akshare_cn_stock_daily_bar_unadjusted": set(),
        "akshare_cn_stock_daily_bar_qfq": set(),
        "akshare_cn_stock_daily_bar_hfq": set(),
    }

    filtered = _filter_master_to_daily_source_candidates(master, partition_cache)

    assert filtered["security_id"].tolist() == ["SH.600000"]


def test_build_cn_stock_daily_bar_caches_daily_source_partition_lists(tmp_path, daily_sample, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})
    calls: dict[str, int] = {}
    original_list_dataset_partitions = ParquetStore.list_dataset_partitions

    def spy_list_dataset_partitions(self: ParquetStore, dataset_id: str):
        if dataset_id in DAILY_SOURCE_DATASETS:
            calls[dataset_id] = calls.get(dataset_id, 0) + 1
        return original_list_dataset_partitions(self, dataset_id)

    monkeypatch.setattr(ParquetStore, "list_dataset_partitions", spy_list_dataset_partitions)

    build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    assert calls == dict.fromkeys(DAILY_SOURCE_DATASETS, 1)


def test_build_cn_stock_daily_bar_skips_security_without_daily_source_partitions(
    tmp_path,
    daily_sample,
    monkeypatch,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    master = pd.concat(
        [
            _master(),
            pd.DataFrame(
                [
                    {
                        **_master().iloc[0].to_dict(),
                        "security_id": "SZ.000001",
                        "code": "000001",
                        "exchange": "SZ",
                        "baostock_code": "sz.000001",
                        "akshare_code": "000001",
                        "qlib_symbol": "sz000001",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    store.write_dataset("cn_security_master", master)
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})
    daily_reads: list[dict[str, str]] = []
    original_read_dataset = ParquetStore.read_dataset

    def spy_read_dataset(self: ParquetStore, dataset_id: str, partition=None):
        if dataset_id in DAILY_SOURCE_DATASETS and partition is not None:
            daily_reads.append(dict(partition))
        return original_read_dataset(self, dataset_id, partition)

    monkeypatch.setattr(ParquetStore, "read_dataset", spy_read_dataset)

    result = build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    assert result["rows"] == 2
    assert {"code": "sh.600000"} in daily_reads
    assert {"code": "sz.000001"} not in daily_reads
    assert {"code": "000001"} not in daily_reads
    assert not store.dataset_exists("cn_stock_daily_bar", {"security_id": "SZ.000001"})


def _master() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "SH.600000",
                "code": "600000",
                "exchange": "SH",
                "name": "PF Bank",
                "security_type": "1",
                "board": "main",
                "baostock_code": "sh.600000",
                "akshare_code": "600000",
                "qlib_symbol": "sh600000",
                "ipo_date": date(1999, 11, 10),
                "delist_date": None,
                "listing_status": "active",
                "is_active": True,
                "source_priority": "mixed",
                "latest_source_date": date(2024, 1, 5),
                "updated_at": NOW,
            }
        ]
    )


def _canonical_daily_bar(close: float, date_value: date) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date_value,
                "security_id": "SH.600000",
                "code": "600000",
                "exchange": "SH",
                "name": "PF Bank",
                "adjustment": "unadjusted",
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "prev_close": close,
                "volume": 1000,
                "amount": close * 1000,
                "turnover_rate": 0.1,
                "pct_change": 0.0,
                "trade_status": "1",
                "is_st": False,
                "is_active": True,
                "source_dataset": "old_cn_stock_daily_bar",
                "source_endpoint": "test",
                "quality_status": "daily_bar_confirmed",
                "updated_at": NOW,
            }
        ]
    )


def _temporary_dataset_dirs(root, directory_name: str, dataset_id: str):
    directory = root / "data" / "parquet" / directory_name
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob(f"{dataset_id}.*") if path.is_dir())


def _akshare_daily(close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "600000",
                "source_symbol": "600000",
                "open": 8.1,
                "high": max(close, 8.4),
                "low": 8.0,
                "close": close,
                "volume": 1000,
                "amount": 8200.0,
                "amplitude": 1.0,
                "pct_change": 2.5,
                "price_change": 0.2,
                "turnover_rate": 0.1,
                "adjustment": "unadjusted",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "daily_bar_confirmed",
                "fetched_at": NOW,
            }
        ]
    )


def _spot() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": date(2024, 1, 5),
                "code": "600000",
                "source_symbol": "600000",
                "name": "PF Bank",
                "last_price": 8.3,
                "price_change": 0.1,
                "pct_change": 1.2,
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "prev_close": 8.2,
                "volume": 120000.0,
                "amount": 9960.0,
                "turnover_rate": 0.12,
                "amplitude": 3.0,
                "pe_dynamic": 5.1,
                "pb": 0.71,
                "total_market_cap": 101000000.0,
                "float_market_cap": 81000000.0,
                "source_endpoint": "spot",
                "fetched_at": NOW,
            }
        ]
    )
