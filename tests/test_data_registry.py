from __future__ import annotations

import gc
from datetime import date, datetime

import pandas as pd

import src.storage.data_registry as data_registry_module
from src.storage.data_registry import DataRegistry
from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.parquet_store import ParquetStore


def test_data_registry_generates_catalog_inventory_after_explicit_refresh(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    registry = DataRegistry(root=tmp_path)

    catalog = registry.read_catalog()
    assert len(catalog) == len(DATASET_CATALOG)
    assert any(item["dataset_id"] == "baostock_cn_stock_daily_bar_qfq" for item in catalog)

    path = store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}).primary_path

    assert path.exists()
    assert registry.read_events() == []

    inventory = registry.refresh_inventory(["baostock_cn_stock_daily_bar_qfq"])
    row = inventory.loc[inventory["dataset_id"] == "baostock_cn_stock_daily_bar_qfq"].iloc[0]
    assert row["parquet_file_count"] == 1
    assert row["partition_count"] == 1
    assert row["row_count"] == 2
    assert row["latest_partition"] == "sh.600000"
    assert row["min_date"] == "2024-01-02"
    assert row["max_date"] == "2024-01-03"

    detail = registry.dataset_detail("baostock_cn_stock_daily_bar_qfq")
    assert detail["view_name"] == "v_baostock_cn_stock_daily_bar_qfq"
    assert detail["code_format"] == "baostock_prefixed"
    assert detail["row_count"] == 2


def test_parquet_writes_do_not_update_registry_until_explicit_refresh(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    registry = DataRegistry(root=tmp_path)

    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})

    assert not registry.catalog_path.exists()
    assert not registry.inventory_path.exists()
    assert not registry.events_path.exists()

    registry.write_catalog()
    inventory = registry.refresh_inventory(["baostock_cn_stock_daily_bar_qfq"])

    row = inventory.loc[inventory["dataset_id"] == "baostock_cn_stock_daily_bar_qfq"].iloc[0]
    assert row["parquet_file_count"] == 1
    assert row["partition_count"] == 1
    assert row["row_count"] == 2


def test_metadata_writes_do_not_update_registry_until_explicit_refresh(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    registry = DataRegistry(root=tmp_path)

    store.upsert_dataset_update_status(
        pd.DataFrame(
            [
                {
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
                    "code": "sh.600000",
                    "last_success_date": "2024-01-03",
                    "row_count": 2,
                    "status": "success",
                    "updated_at": datetime(2024, 1, 3, 18, 0),
                    "error_stack": "",
                }
            ]
        )
    )

    assert not registry.catalog_path.exists()
    assert not registry.inventory_path.exists()
    assert not registry.events_path.exists()


def test_data_registry_refreshes_status_from_metadata_rows(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})

    store.upsert_dataset_update_status(
        pd.DataFrame(
            [
                {
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
                    "code": "sh.600000",
                    "last_success_date": "2024-01-03",
                    "row_count": 2,
                    "status": "success",
                    "updated_at": datetime(2024, 1, 3, 18, 0),
                    "error_stack": "",
                }
            ]
        )
    )

    registry = DataRegistry(root=tmp_path)
    registry.refresh_inventory(
        ["baostock_cn_stock_daily_bar_qfq"],
        status_rows=store.read_dataset_update_status(),
    )

    detail = registry.dataset_detail("baostock_cn_stock_daily_bar_qfq")
    assert detail["latest_success_date"] == "2024-01-03"
    assert detail["latest_status"] == "success"


def test_data_registry_lists_partitions(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})

    partitions = DataRegistry(root=tmp_path).dataset_partitions("baostock_cn_stock_daily_bar_qfq")

    assert partitions == [
        {
            "partition_value": "sh.600000",
            "row_count": 2,
            "path": str(
                tmp_path / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600000" / "data.parquet"
            ),
            "mtime": partitions[0]["mtime"],
        }
    ]


def test_data_registry_discovers_derived_datasets_and_security_partitions(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("cn_stock_daily_bar", _cn_daily_bar(), {"security_id": "SH.600000"}, mode="replace")

    registry = DataRegistry(root=tmp_path)
    catalog = registry.write_catalog()
    inventory = registry.refresh_inventory(["cn_security_master", "cn_stock_daily_bar"])
    partitions = registry.dataset_partitions("cn_stock_daily_bar")

    assert any(item["dataset_id"] == "cn_security_master" for item in catalog)
    assert any(item["dataset_id"] == "cn_stock_daily_bar" for item in catalog)
    daily_row = inventory.loc[inventory["dataset_id"] == "cn_stock_daily_bar"].iloc[0]
    assert daily_row["partition_column"] == "security_id"
    assert daily_row["partition_count"] == 1
    assert partitions[0]["partition_value"] == "SH.600000"


def test_data_registry_global_locks_do_not_retain_released_registry_paths(tmp_path) -> None:
    before = len(data_registry_module._LOCKS)
    registries = [DataRegistry(root=tmp_path / f"registry-{index}") for index in range(10)]

    assert len(data_registry_module._LOCKS) >= before + 10

    registries.clear()
    gc.collect()

    assert len(data_registry_module._LOCKS) == before


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
                "updated_at": datetime(2024, 1, 5, 12, 0),
            }
        ]
    )


def _cn_daily_bar() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "security_id": "SH.600000",
                "code": "600000",
                "exchange": "SH",
                "name": "PF Bank",
                "adjustment": "unadjusted",
                "open": 8.1,
                "high": 8.3,
                "low": 8.0,
                "close": 8.2,
                "prev_close": 8.0,
                "volume": 1000.0,
                "amount": 8200.0,
                "turnover_rate": 0.1,
                "pct_change": 2.5,
                "trade_status": "1",
                "is_st": "0",
                "is_active": True,
                "source_dataset": "baostock_cn_stock_daily_bar_unadjusted",
                "source_endpoint": "query_history_k_data_plus",
                "quality_status": "daily_bar_confirmed",
                "updated_at": datetime(2024, 1, 5, 12, 0),
            }
        ]
    )
