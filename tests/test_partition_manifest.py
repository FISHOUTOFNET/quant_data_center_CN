from __future__ import annotations

import os
from datetime import date

import pandas as pd
import pytest
from click.testing import CliRunner

import src.cli as cli_module
from src.storage.manifest_rebuild import rebuild_partition_manifest
from src.storage.parquet_store import ParquetStore


def test_source_partition_write_generates_manifest(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    path = store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}).primary_path

    manifest = store.read_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq")

    assert len(manifest) == 1
    row = manifest.iloc[0]
    assert row["dataset"] == "baostock_cn_stock_daily_bar_qfq"
    assert row["partition_column"] == "code"
    assert row["partition_value"] == "sh.600000"
    assert row["output_path"] == "data/parquet/baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet"
    assert row["row_count"] == 2
    assert row["min_date"] == "2024-01-02"
    assert row["max_date"] == "2024-01-03"
    assert row["file_size_bytes"] == path.stat().st_size
    assert row["content_hash"]
    assert row["semantic_hash"]
    assert row["schema_hash"]


def test_staging_write_does_not_generate_manifest(tmp_path, daily_sample) -> None:
    staging = tmp_path / "data" / "parquet" / ".staging" / "unit"
    store = ParquetStore(root=tmp_path, parquet_dir=staging)

    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})

    assert store.read_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq").empty


def test_manifest_write_failure_raises_from_dataset_write(tmp_path, daily_sample, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)

    def fail_manifest_write(df: pd.DataFrame) -> None:
        del df
        raise RuntimeError("manifest failed")

    monkeypatch.setattr(store._metadata_store, "upsert_dataset_partition_manifest", fail_manifest_write)

    with pytest.raises(RuntimeError, match="manifest failed"):
        store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})


def test_rebuild_partition_manifest_backfills_existing_parquet(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})
    store.delete_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq", "code", "sh.600000")

    result = rebuild_partition_manifest(store=store, dataset_id="baostock_cn_stock_daily_bar_qfq")

    manifest = store.read_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq")
    assert result.partition_count == 1
    assert result.updated_count == 1
    assert result.skipped_count == 0
    assert manifest.iloc[0]["partition_value"] == "sh.600000"


def test_rebuild_partition_manifest_cli_help_and_execution(tmp_path, daily_sample, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})
    store.delete_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq", "code", "sh.600000")
    monkeypatch.setattr(cli_module.paths, "ROOT", tmp_path)

    help_result = CliRunner().invoke(cli_module.cli, ["--help"])
    result = CliRunner().invoke(
        cli_module.cli,
        ["rebuild-partition-manifest", "--dataset", "baostock_cn_stock_daily_bar_qfq"],
        env={"QDC_DISABLE_FILE_LOG": "1"},
    )

    assert help_result.exit_code == 0
    assert "rebuild-partition-manifest" in help_result.output
    assert result.exit_code == 0
    assert "dataset=baostock_cn_stock_daily_bar_qfq partition_count=1 updated_count=1 skipped_count=0" in result.output
    assert not store.read_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq").empty


def test_semantic_hash_excludes_fetched_at(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    first = _akshare_daily(fetched_at="2024-01-03 12:00:00")
    second = _akshare_daily(fetched_at="2024-01-04 12:00:00")

    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", first, {"code": "600000"})
    first_manifest = store.read_dataset_partition_manifest("akshare_cn_stock_daily_bar_unadjusted").iloc[0]
    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", second, {"code": "600000"})
    second_manifest = store.read_dataset_partition_manifest("akshare_cn_stock_daily_bar_unadjusted").iloc[0]

    assert second_manifest["content_hash"] != first_manifest["content_hash"]
    assert second_manifest["semantic_hash"] == first_manifest["semantic_hash"]


def test_touching_parquet_does_not_update_semantic_manifest(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    path = store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}).primary_path
    before = store.read_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq").iloc[0]["semantic_hash"]

    os.utime(path, None)

    after = store.read_dataset_partition_manifest("baostock_cn_stock_daily_bar_qfq").iloc[0]["semantic_hash"]
    assert after == before


def _akshare_daily(fetched_at: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "600000",
                "source_symbol": "600000",
                "open": 8.1,
                "high": 8.4,
                "low": 8.0,
                "close": 8.2,
                "volume": 1000,
                "amount": 8200.0,
                "amplitude": 1.0,
                "pct_change": 2.5,
                "price_change": 0.2,
                "turnover_rate": 0.1,
                "adjustment": "unadjusted",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "daily_bar_confirmed",
                "fetched_at": pd.Timestamp(fetched_at),
            }
        ]
    )
