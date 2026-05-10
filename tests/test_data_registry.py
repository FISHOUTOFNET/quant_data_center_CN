from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.storage.data_registry import DataRegistry
from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.parquet_store import ParquetStore


def test_data_registry_generates_catalog_inventory_and_write_events(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    registry = DataRegistry(root=tmp_path)

    catalog = registry.read_catalog()
    assert len(catalog) == len(DATASET_CATALOG)
    assert any(item["dataset_id"] == "baostock_cn_stock_daily_bar_qfq" for item in catalog)

    path = store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())

    events = registry.read_events()
    assert events[-1]["dataset_id"] == "baostock_cn_stock_daily_bar_qfq"
    assert events[-1]["code"] == "sh.600000"
    assert events[-1]["row_count"] == 2
    assert events[-1]["output_path"] == str(path.relative_to(tmp_path))

    inventory = registry.read_inventory()
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


def test_data_registry_refreshes_status_from_metadata_rows(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())

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

    detail = DataRegistry(root=tmp_path).dataset_detail("baostock_cn_stock_daily_bar_qfq")
    assert detail["latest_success_date"] == "2024-01-03"
    assert detail["latest_status"] == "success"


def test_data_registry_lists_partitions(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())

    partitions = DataRegistry(root=tmp_path).dataset_partitions("baostock_cn_stock_daily_bar_qfq")

    assert partitions == [
        {
            "partition_value": "sh.600000",
            "row_count": 2,
            "path": str(tmp_path / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600000" / "data.parquet"),
            "mtime": partitions[0]["mtime"],
        }
    ]
