from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.sources.akshare.pipeline.capital_structure_pending import (
    drain_capital_structure_pending,
    enqueue_capital_structure_pending,
    read_capital_structure_pending,
)
from src.pipeline.lifecycle import PipelineMetadataBatch
from src.sources.baostock.update_daily_worker import _DailyUpdateBackgroundWorker
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


def test_enqueue_capital_structure_pending_normalizes_baostock_code(tmp_path) -> None:
    enqueue_capital_structure_pending(
        tmp_path,
        code="sh.600000",
        trigger_dataset="baostock_cn_stock_adjustment_factor",
        trigger_reason="adjustment_factor_changed",
        now=lambda: datetime(2024, 1, 3, 12, 0),
    )

    pending = read_capital_structure_pending(tmp_path)

    assert pending[["code", "trigger_dataset", "trigger_reason", "status"]].to_dict("records") == [
        {
            "code": "600000",
            "trigger_dataset": "baostock_cn_stock_adjustment_factor",
            "trigger_reason": "adjustment_factor_changed",
            "status": "pending",
        }
    ]


def test_drain_capital_structure_pending_marks_success_and_retryable_failure(tmp_path) -> None:
    enqueue_capital_structure_pending(
        tmp_path,
        code="sh.600000",
        trigger_dataset="baostock_cn_stock_adjustment_factor",
        trigger_reason="adjustment_factor_changed",
        now=lambda: datetime(2024, 1, 3, 12, 0),
    )
    enqueue_capital_structure_pending(
        tmp_path,
        code="sz.000001",
        trigger_dataset="baostock_cn_stock_adjustment_factor",
        trigger_reason="adjustment_factor_changed",
        now=lambda: datetime(2024, 1, 3, 12, 1),
    )
    calls = []

    def updater(request):
        calls.append((request.target, tuple(request.code), request.force, request.build_views))
        if tuple(request.code) == ("000001",):
            raise RuntimeError("planned failure")
        return [{"status": "success"}]

    drain_capital_structure_pending(tmp_path, updater=updater, now=lambda: datetime(2024, 1, 3, 12, 2, 0, 123456))

    pending = read_capital_structure_pending(tmp_path).sort_values("code").reset_index(drop=True)
    assert calls == [
        ("capital_structure", ("600000",), True, False),
        ("capital_structure", ("000001",), True, False),
    ]
    assert pending[["code", "status"]].to_dict("records") == [
        {"code": "000001", "status": "failed_retryable"},
        {"code": "600000", "status": "success"},
    ]
    assert pending["last_attempt_at"].dt.microsecond.tolist() == [123000, 123000]
    assert "planned failure" in str(pending.loc[0, "error_stack"])


def test_enqueue_capital_structure_pending_serializes_concurrent_writes(tmp_path) -> None:
    codes = [f"sh.60{index:04d}" for index in range(40)]

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda code: enqueue_capital_structure_pending(
                    tmp_path,
                    code=code,
                    trigger_dataset="baostock_cn_stock_adjustment_factor",
                    trigger_reason="adjustment_factor_changed",
                    now=lambda: datetime(2024, 1, 3, 12, 0),
                ),
                codes,
            )
        )

    pending = read_capital_structure_pending(tmp_path)

    assert sorted(pending["code"].astype(str).tolist()) == sorted(code.split(".", 1)[1] for code in codes)
    assert pending["status"].value_counts().to_dict() == {"pending": len(codes)}


def test_read_capital_structure_pending_quarantines_corrupt_file(tmp_path) -> None:
    pending_path = tmp_path / "data" / "metadata" / "akshare_capital_structure_pending.parquet"
    pending_path.parent.mkdir(parents=True)
    pending_path.write_bytes(b"PAR1corruptPAR1")

    pending = read_capital_structure_pending(tmp_path)

    assert pending.empty
    assert not pending_path.exists()
    assert len(list(pending_path.parent.glob("akshare_capital_structure_pending.corrupt.*.parquet"))) == 1


def test_adjustment_factor_change_enqueues_capital_structure_update(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_adjustment_factor", _factor_frame(1.0), {"code": "sh.600000"})
    enqueued = []
    monkeypatch.setattr(
        "src.sources.baostock.update_daily_worker.enqueue_capital_structure_pending",
        lambda root, code, trigger_dataset, trigger_reason: enqueued.append(
            (root, code, trigger_dataset, trigger_reason)
        ),
    )
    worker = _worker(tmp_path, store)

    worker.process_baostock_cn_stock_adjustment_factor_success(
        "sh.600000",
        _factor_frame(2.0),
        datetime(2024, 1, 3, 12, 0),
        store.dataset_path("baostock_cn_stock_adjustment_factor", {"code": "sh.600000"}),
    )

    assert enqueued == [
        (tmp_path.resolve(), "sh.600000", "baostock_cn_stock_adjustment_factor", "adjustment_factor_changed")
    ]


def test_unchanged_adjustment_factor_does_not_enqueue_capital_structure_update(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_adjustment_factor", _factor_frame(1.0), {"code": "sh.600000"})
    enqueued = []
    monkeypatch.setattr(
        "src.sources.baostock.update_daily_worker.enqueue_capital_structure_pending",
        lambda root, code, trigger_dataset, trigger_reason: enqueued.append(code),
    )
    worker = _worker(tmp_path, store)

    worker.process_baostock_cn_stock_adjustment_factor_success(
        "sh.600000",
        _factor_frame(1.0),
        datetime(2024, 1, 3, 12, 0),
        store.dataset_path("baostock_cn_stock_adjustment_factor", {"code": "sh.600000"}),
    )

    assert enqueued == []


def test_adjustment_factor_enqueue_failure_does_not_fail_baostock_update(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_adjustment_factor", _factor_frame(1.0), {"code": "sh.600000"})

    def fail_enqueue(*args, **kwargs):
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr("src.sources.baostock.update_daily_worker.enqueue_capital_structure_pending", fail_enqueue)
    worker = _worker(tmp_path, store)

    result = worker.process_baostock_cn_stock_adjustment_factor_success(
        "sh.600000",
        _factor_frame(2.0),
        datetime(2024, 1, 3, 12, 0),
        store.dataset_path("baostock_cn_stock_adjustment_factor", {"code": "sh.600000"}),
    )

    assert [record["status"] for record in result.run_records] == ["success"]


def test_update_daily_drains_capital_structure_pending_after_adjustment_factor_target(monkeypatch) -> None:
    import src.sources.baostock.update_daily as update_daily_module

    drained = []
    monkeypatch.setattr(
        update_daily_module,
        "_update_daily_impl",
        lambda **kwargs: [{"dataset": kwargs["dataset"], "status": "success"}],
    )
    monkeypatch.setattr(
        update_daily_module,
        "drain_capital_structure_pending",
        lambda root: (
            drained.append(root) or [{"dataset": "akshare_cn_stock_capital_structure_em", "status": "success"}]
        ),
    )

    records = update_daily_module.update_daily(dataset="baostock_cn_stock_adjustment_factor", root=Path("C:/tmp/qdc"))

    assert drained == [Path("C:/tmp/qdc")]
    assert [item["dataset"] for item in records] == [
        "baostock_cn_stock_adjustment_factor",
        "akshare_cn_stock_capital_structure_em",
    ]


def test_update_daily_does_not_drain_capital_structure_pending_after_adjustment_factor_failure(monkeypatch) -> None:
    import src.sources.baostock.update_daily as update_daily_module

    drained = []
    monkeypatch.setattr(
        update_daily_module,
        "_update_daily_impl",
        lambda **kwargs: [{"dataset": kwargs["dataset"], "status": "failed"}],
    )
    monkeypatch.setattr(
        update_daily_module,
        "drain_capital_structure_pending",
        lambda root: (
            drained.append(root) or [{"dataset": "akshare_cn_stock_capital_structure_em", "status": "success"}]
        ),
    )

    records = update_daily_module.update_daily(dataset="baostock_cn_stock_adjustment_factor", root=Path("C:/tmp/qdc"))

    assert drained == []
    assert [item["dataset"] for item in records] == ["baostock_cn_stock_adjustment_factor"]


def _worker(tmp_path, store: ParquetStore) -> _DailyUpdateBackgroundWorker:
    return _DailyUpdateBackgroundWorker(
        store=store,
        config=ConfigManager(tmp_path),
        mode="partial",
        start_date="2024-01-03",
        end_date="2024-01-03",
        metadata_batch=PipelineMetadataBatch(store, 100, count_by="run"),
    )


def _factor_frame(value: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividend_operate_date": "2024-01-02",
                "forward_adjust_factor": value,
                "backward_adjust_factor": value,
                "adjustment_factor": value,
            }
        ]
    )


def _write_settings(root) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "project:",
                "  timezone: Asia/Shanghai",
                "pipeline:",
                "  metadata_flush_size: 100",
                "",
            ]
        ),
        encoding="utf-8",
    )
