from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from src.sources.derived.stock_valuation import build_cn_stock_valuation
from src.storage.parquet_store import ParquetStore

NOW = datetime(2024, 1, 5, 12, 0)
VALUATION_SOURCE_DATASETS = {
    "akshare_cn_stock_valuation_eastmoney",
    "baostock_cn_stock_valuation_percentile",
}


def test_build_cn_stock_valuation_merges_partition_sources(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("akshare_cn_stock_valuation_eastmoney", _akshare_valuation(), {"code": "600000"})
    store.write_dataset(
        "baostock_cn_stock_valuation_percentile", _baostock_percentile(pe_ttm=50.0), {"code": "sh.600000"}
    )
    calls: list[tuple[str, object]] = []
    original_read_dataset = ParquetStore.read_dataset

    def spy_read_dataset(self: ParquetStore, dataset_id: str, partition=None):
        if dataset_id in VALUATION_SOURCE_DATASETS:
            assert partition is not None
            calls.append((dataset_id, partition))
        return original_read_dataset(self, dataset_id, partition)

    monkeypatch.setattr(ParquetStore, "read_dataset", spy_read_dataset)

    result = build_cn_stock_valuation(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_stock_valuation", {"security_id": "SH.600000"})

    assert result["rows"] == 2
    assert result["partitions"] == 1
    assert calls
    assert loaded.loc[0, "pe_ttm"] == 5.0
    assert loaded.loc[0, "pb_percentile_1y"] == 20.0
    assert loaded.loc[0, "source_dataset"] == (
        "akshare_cn_stock_valuation_eastmoney+baostock_cn_stock_valuation_percentile"
    )


def test_build_cn_stock_valuation_outputs_baostock_only_rows(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset(
        "baostock_cn_stock_valuation_percentile", _baostock_percentile(pe_ttm=6.0), {"code": "sh.600000"}
    )

    build_cn_stock_valuation(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_stock_valuation", {"security_id": "SH.600000"})

    assert loaded.loc[0, "pe_ttm"] == 6.0
    assert loaded.loc[0, "source_dataset"] == "baostock_cn_stock_valuation_percentile"


def test_build_cn_stock_valuation_failure_keeps_existing_official_data(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset(
        "cn_stock_valuation",
        _canonical_valuation(close=1.0, date_value=date(2023, 12, 29)),
        {"security_id": "SH.600000"},
        mode="replace",
    )

    def fail_materialize(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("valuation staging build failed")

    monkeypatch.setattr("src.sources.derived.stock_valuation._materialize_security_valuation", fail_materialize)

    with pytest.raises(RuntimeError, match="valuation staging build failed"):
        build_cn_stock_valuation(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    loaded = store.read_dataset("cn_stock_valuation", {"security_id": "SH.600000"})
    assert loaded["close"].tolist() == [1.0]
    assert store.dataset_exists("cn_stock_valuation", {"security_id": "SH.600000"})
    assert _temporary_dataset_dirs(tmp_path, ".staging", "cn_stock_valuation") == []


def test_build_cn_stock_valuation_success_promotes_staging_dataset(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset(
        "cn_stock_valuation",
        _canonical_valuation(close=1.0, date_value=date(2023, 12, 29)),
        {"security_id": "SH.600000"},
        mode="replace",
    )
    store.write_dataset("akshare_cn_stock_valuation_eastmoney", _akshare_valuation(), {"code": "600000"})

    result = build_cn_stock_valuation(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_stock_valuation", {"security_id": "SH.600000"})

    assert result["rows"] == 2
    assert loaded["close"].tolist() == [8.2, 8.3]
    assert date(2023, 12, 29) not in set(loaded["date"])
    assert _temporary_dataset_dirs(tmp_path, ".staging", "cn_stock_valuation") == []
    assert _temporary_dataset_dirs(tmp_path, ".backup", "cn_stock_valuation") == []


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


def _canonical_valuation(close: float, date_value: date) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date_value,
                "security_id": "SH.600000",
                "code": "600000",
                "exchange": "SH",
                "name": "PF Bank",
                "close": close,
                "total_market_cap": 100000000.0,
                "float_market_cap": 80000000.0,
                "total_shares": 12000000.0,
                "float_shares": 10000000.0,
                "pe_ttm": 5.0,
                "pe_static": 5.5,
                "pb": 0.7,
                "ps": 1.2,
                "pcf": 3.0,
                "source_dataset": "old_cn_stock_valuation",
                "updated_at": NOW,
            }
        ]
    )


def _temporary_dataset_dirs(root, directory_name: str, dataset_id: str):
    directory = root / "data" / "parquet" / directory_name
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob(f"{dataset_id}.*") if path.is_dir())


def _akshare_valuation() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "600000",
                "close": 8.2,
                "pct_change": 2.5,
                "total_market_cap": 100000000.0,
                "float_market_cap": 80000000.0,
                "total_shares": 12000000.0,
                "float_shares": 10000000.0,
                "pe_ttm": 5.0,
                "pe_static": 5.5,
                "pb": 0.7,
                "peg": 0.8,
                "pcf": 3.0,
                "ps": 1.2,
            },
            {
                "date": date(2024, 1, 3),
                "code": "600000",
                "close": 8.3,
                "pct_change": 1.2,
                "total_market_cap": 101000000.0,
                "float_market_cap": 81000000.0,
                "total_shares": 12000000.0,
                "float_shares": 10000000.0,
                "pe_ttm": 5.1,
                "pe_static": 5.6,
                "pb": 0.71,
                "peg": 0.81,
                "pcf": 3.1,
                "ps": 1.21,
            },
        ]
    )


def _baostock_percentile(pe_ttm: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "sh.600000",
                "pe_ttm": pe_ttm,
                "pb_mrq": 0.8,
                "ps_ttm": 1.3,
                "pcf_ncf_ttm": 3.3,
                "pe_ttm_percentile_1y": 10.0,
                "pe_ttm_percentile_3y": 11.0,
                "pe_ttm_percentile_5y": 12.0,
                "pe_ttm_percentile_10y": 13.0,
                "pe_ttm_percentile_all_history": 14.0,
                "pb_mrq_percentile_1y": 20.0,
                "pb_mrq_percentile_3y": 21.0,
                "pb_mrq_percentile_5y": 22.0,
                "pb_mrq_percentile_10y": 23.0,
                "pb_mrq_percentile_all_history": 24.0,
                "ps_ttm_percentile_1y": 30.0,
                "ps_ttm_percentile_3y": 31.0,
                "ps_ttm_percentile_5y": 32.0,
                "ps_ttm_percentile_10y": 33.0,
                "ps_ttm_percentile_all_history": 34.0,
                "pcf_ncf_ttm_percentile_1y": 40.0,
                "pcf_ncf_ttm_percentile_3y": 41.0,
                "pcf_ncf_ttm_percentile_5y": 42.0,
                "pcf_ncf_ttm_percentile_10y": 43.0,
                "pcf_ncf_ttm_percentile_all_history": 44.0,
            }
        ]
    )
