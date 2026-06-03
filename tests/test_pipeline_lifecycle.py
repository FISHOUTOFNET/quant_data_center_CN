from __future__ import annotations

import subprocess
import sys
from datetime import datetime

import pandas as pd
import pytest

from src.pipeline.lifecycle import LifecycleTaskRef, PipelineLifecycle
from src.storage.parquet_store import ParquetStore


def test_pipeline_lifecycle_records_success_failure_and_skipped_rows(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    lifecycle = PipelineLifecycle(store, flush_size=100)
    task = LifecycleTaskRef(
        pipeline="update_test",
        dataset="baostock_cn_stock_daily_bar_qfq",
        code="sh.600000",
        start_date="2024-01-01",
        end_date="2024-01-31",
        output_path=tmp_path / "data.parquet",
    )

    success = lifecycle.record_success(
        task,
        started_at=datetime(2024, 1, 31, 9, 0),
        ended_at=datetime(2024, 1, 31, 9, 1),
        row_count=2,
    )
    failure = lifecycle.record_failure(
        task,
        started_at=datetime(2024, 2, 1, 9, 0),
        ended_at=datetime(2024, 2, 1, 9, 1),
        error_stack="planned failure",
    )
    skipped = lifecycle.record_skipped(
        task,
        status="skipped_checkpoint",
        started_at=datetime(2024, 2, 2, 9, 0),
        ended_at=datetime(2024, 2, 2, 9, 0),
        reason="checkpoint",
    )
    lifecycle.flush()

    assert success.run_row["status"] == "success"
    assert success.status_row is not None
    assert success.status_row["last_success_date"] == "2024-01-31"
    assert success.checkpoint_row is not None
    assert success.checkpoint_row["start_date"] == "2024-01-01"

    assert failure.run_row["status"] == "failed"
    assert failure.status_row is not None
    assert failure.status_row["last_success_date"] is None
    assert failure.checkpoint_row is not None
    assert failure.checkpoint_row["error_stack"] == "planned failure"

    assert skipped.run_row["status"] == "skipped_checkpoint"
    assert skipped.status_row is not None
    assert skipped.status_row["status"] == "skipped_checkpoint"
    assert skipped.checkpoint_row is not None
    assert skipped.checkpoint_row["status"] == "skipped_checkpoint"

    runs = store.read_pipeline_runs()
    checkpoints = store.read_pipeline_checkpoints()
    assert list(runs["status"].astype(str)) == ["success", "failed", "skipped_checkpoint"]
    assert list(checkpoints["status"].astype(str)) == ["skipped_checkpoint"]


def test_pipeline_lifecycle_uses_checkpoint_start_and_last_success_override(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    lifecycle = PipelineLifecycle(store, flush_size=100)
    task = LifecycleTaskRef(
        pipeline="update_test",
        dataset="baostock_cn_stock_daily_bar_qfq",
        code="sh.600000",
        start_date="2024-01-20",
        end_date="2024-01-31",
        output_path=tmp_path / "data.parquet",
        checkpoint_start_date="2024-01-01",
    )

    lifecycle.record_success(
        task,
        started_at=datetime(2024, 1, 31, 9, 0),
        ended_at=datetime(2024, 1, 31, 9, 1),
        row_count=2,
        last_success_date="2024-01-30",
    )
    lifecycle.flush()

    status = store.read_dataset_update_status()
    checkpoints = store.read_pipeline_checkpoints()
    assert pd.to_datetime(status.loc[0, "last_success_date"]).date().isoformat() == "2024-01-30"
    assert pd.to_datetime(checkpoints.loc[0, "start_date"]).date().isoformat() == "2024-01-01"


def test_pipeline_lifecycle_flush_failure_requeues_rows(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    lifecycle = PipelineLifecycle(store, flush_size=1)
    task = LifecycleTaskRef(
        pipeline="update_test",
        dataset="baostock_cn_stock_daily_bar_qfq",
        code="sh.600000",
        start_date="2024-01-01",
        end_date="2024-01-31",
        output_path=tmp_path / "data.parquet",
    )
    original = store.persist_update_metadata
    calls = 0

    def flaky_persist(run_rows, status_rows, checkpoint_rows):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("planned flush failure")
        return original(run_rows, status_rows, checkpoint_rows)

    monkeypatch.setattr(store, "persist_update_metadata", flaky_persist)

    with pytest.raises(RuntimeError, match="planned flush failure"):
        lifecycle.record_success(
            task,
            started_at=datetime(2024, 1, 31, 9, 0),
            ended_at=datetime(2024, 1, 31, 9, 1),
            row_count=2,
        )

    lifecycle.flush()
    assert calls == 2
    assert len(store.read_pipeline_runs()) == 1


def test_pipeline_lifecycle_releases_duckdb_between_metadata_flushes(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    lifecycle = PipelineLifecycle(store, flush_size=1)
    task = LifecycleTaskRef(
        pipeline="update_test",
        dataset="baostock_cn_stock_daily_bar_qfq",
        code="sh.600000",
        start_date="2024-01-01",
        end_date="2024-01-31",
        output_path=tmp_path / "data.parquet",
    )

    lifecycle.record_success(
        task,
        started_at=datetime(2024, 1, 31, 9, 0),
        ended_at=datetime(2024, 1, 31, 9, 1),
        row_count=2,
    )

    duckdb_file = tmp_path / "data" / "duckdb" / "quant.duckdb"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import duckdb; "
                f"conn = duckdb.connect({str(duckdb_file)!r}); "
                "conn.close()"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    store.close()

    assert result.returncode == 0, result.stderr


def test_pipeline_lifecycle_finish_refreshes_dirty_datasets(tmp_path, monkeypatch, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})
    refreshed: list[tuple[str, ...]] = []

    class FakeRegistry:
        def __init__(self, root):
            self.root = root

        def write_catalog(self):
            return None

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            refreshed.append(tuple(dataset_ids or ()))

    monkeypatch.setattr("src.pipeline.lifecycle.DataRegistry", FakeRegistry)

    lifecycle = PipelineLifecycle(store, flush_size=100)
    lifecycle.finish()

    assert refreshed == [("baostock_cn_stock_daily_bar_qfq",)]
