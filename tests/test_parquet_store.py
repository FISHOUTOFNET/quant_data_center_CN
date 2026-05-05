from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import pytest

from src.pipeline.common import PipelineCheckpointLookup, should_skip_checkpoint
from src.pipeline.services import PipelineMetadataBatch
import src.storage.parquet_store as parquet_store_module
from src.storage.parquet_store import ParquetStore


def test_daily_k_atomic_write(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    raw = daily_sample().astype({"volume": "string", "peTTM": "string"})
    raw.loc[0, "peTTM"] = ""

    path = store.write_daily_k("daily_k_qfq", "sh.600000", raw)

    assert path.exists()
    assert not (path.parent / "data.tmp.parquet").exists()
    loaded = pd.read_parquet(path)
    assert len(loaded) == 2
    assert loaded["volume"].tolist() == [1000, 1200]
    assert pd.isna(loaded.loc[0, "peTTM"])


def test_stock_basic_codes_from_latest_snapshot(tmp_path, stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())

    assert store.stock_basic_codes("all") == ["sh.000001", "sh.600000", "sz.000001"]
    assert store.stock_basic_codes("active") == ["sh.600000"]


def test_write_calendar_merges_existing_dates(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_calendar(
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-05", "is_trading_day": "1"},
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
            ]
        )
    )
    store.write_calendar(
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
                {"calendar_date": "2024-01-07", "is_trading_day": "0"},
            ]
        )
    )

    calendar = store.read_calendar()
    assert pd.to_datetime(calendar["calendar_date"], errors="coerce").dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
    ]


def test_adjust_factor_write_and_read(tmp_path, adjust_factor_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    path = store.write_adjust_factor("sh.600000", adjust_factor_sample().astype({"foreAdjustFactor": "string"}))

    assert path.exists()
    loaded = store.read_adjust_factor("sh.600000")
    assert len(loaded) == 1
    assert loaded.loc[0, "foreAdjustFactor"] == 1.0


def test_akshare_dataset_write_and_read(tmp_path, stock_value_em_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    value_path = store.write_stock_value_em("600000", stock_value_em_sample().astype({"pe_ttm": "string"}))

    assert value_path == tmp_path / "data" / "parquet" / "stock_value_em" / "code=600000" / "data.parquet"
    loaded = store.read_stock_value_em("600000")
    assert len(loaded) == 2
    assert loaded.loc[0, "pe_ttm"] == 5.0


def test_akshare_a_stock_writes_and_hist_upsert_overrides_spot(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    fetched_at = datetime(2024, 1, 3, 16, 0)

    delist_path = store.write_stock_info_sh_delist(
        "2024-01-03",
        pd.DataFrame(
            [
                {
                    "snapshot_date": "2024-01-03",
                    "exchange": "sh",
                    "market": "全部",
                    "code": "600001",
                    "source_symbol": "600001",
                    "name": "Old Corp",
                    "list_date": "2000-01-01",
                    "delist_date": "2024-01-02",
                    "source_endpoint": "stock_info_sh_delist",
                    "fetched_at": fetched_at,
                }
            ]
        ),
    )
    assert delist_path == tmp_path / "data" / "parquet" / "stock_info_sh_delist" / "snapshot_date=2024-01-03" / "data.parquet"
    assert store.read_latest_stock_info_sh_delist().loc[0, "code"] == "600001"

    sz_delist_path = store.write_stock_info_sz_delist(
        "2024-01-03",
        pd.DataFrame(
            [
                {
                    "snapshot_date": "2024-01-03",
                    "exchange": "sz",
                    "market": "全部",
                    "code": "000001",
                    "source_symbol": "000001",
                    "name": "Old SZ Corp",
                    "list_date": "2000-01-01",
                    "delist_date": "2024-01-02",
                    "source_endpoint": "stock_info_sz_delist",
                    "fetched_at": fetched_at,
                }
            ]
        ),
    )
    assert sz_delist_path == tmp_path / "data" / "parquet" / "stock_info_sz_delist" / "snapshot_date=2024-01-03" / "data.parquet"
    assert store.read_latest_stock_info_sz_delist().loc[0, "code"] == "000001"

    spot_path = store.write_stock_zh_a_spot_em(
        "2024-01-03",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
                    "code": "600000",
                    "source_symbol": "600000",
                    "name": "PF Bank",
                    "latest_price": "8.30",
                    "change_amount": 0.1,
                    "pct_chg": 1.2,
                    "open": 8.2,
                    "high": 8.4,
                    "low": 8.1,
                    "preclose": 8.2,
                    "volume": 120000.0,
                    "amount": 9960.0,
                    "turnover_rate": 0.12,
                    "amplitude": 3.0,
                    "pe_dynamic": 5.1,
                    "pb": 0.71,
                    "total_market_cap": 101000000.0,
                    "float_market_cap": 81000000.0,
                    "source_endpoint": "stock_zh_a_spot_em",
                    "fetched_at": fetched_at,
                }
            ]
        ),
    )
    assert spot_path.exists()
    assert store.read_latest_stock_zh_a_spot_em().loc[0, "latest_price"] == 8.3

    sina_path = store.write_stock_zh_a_spot_sina(
        "2024-01-03",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
                    "code": "600000",
                    "source_symbol": "sh600000",
                    "name": "PF Bank",
                    "latest_price": 8.3,
                    "change_amount": 0.1,
                    "pct_chg": 1.2,
                    "bid": 8.29,
                    "ask": 8.31,
                    "preclose": 8.2,
                    "open": 8.2,
                    "high": 8.4,
                    "low": 8.1,
                    "volume": 120000.0,
                    "amount": 9960.0,
                    "source_timestamp": "15:00:00",
                    "source_endpoint": "stock_zh_a_spot",
                    "is_fallback": "true",
                    "fallback_reason": "planned",
                    "fetched_at": fetched_at,
                }
            ]
        ),
    )
    assert sina_path.exists()
    assert bool(store.read_stock_zh_a_spot_sina("2024-01-03").loc[0, "is_fallback"])

    spot_hist = _akshare_hist_row("stock_zh_a_spot_em", "spot_close", close=8.3)
    hist_confirmed = _akshare_hist_row("stock_zh_a_hist", "hist_confirmed", close=8.31)
    store.write_stock_zh_a_hist("none", "600000", pd.DataFrame([spot_hist]))
    store.upsert_stock_zh_a_hist("none", "600000", pd.DataFrame([hist_confirmed]))
    hist = store.read_stock_zh_a_hist("none", "600000")
    assert len(hist) == 1
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "hist_confirmed"


def test_writes_reject_missing_partition_keys(
    tmp_path,
    daily_sample,
    adjust_factor_sample,
    stock_value_em_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    with pytest.raises(ValueError, match="Daily file code missing code"):
        store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample().drop(columns=["code"]))
    with pytest.raises(ValueError, match="Adjust factor file code missing code"):
        store.write_adjust_factor("sh.600000", adjust_factor_sample().drop(columns=["code"]))
    with pytest.raises(ValueError, match="Stock value file code missing code"):
        store.write_stock_value_em("600000", stock_value_em_sample().drop(columns=["code"]))


def test_writes_reject_partition_key_mismatch(tmp_path, stock_value_em_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    with pytest.raises(ValueError, match="Stock value file code mismatch"):
        store.write_stock_value_em("600000", stock_value_em_sample("000001"))


def test_daily_k_write_logs_parquet_success(tmp_path, daily_sample, monkeypatch) -> None:
    logs = []

    class FakeLogger:
        def info(self, message, *args, **kwargs) -> None:
            logs.append((message, args))

        def warning(self, message, *args, **kwargs) -> None:
            return None

    monkeypatch.setattr(parquet_store_module, "logger", FakeLogger())
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    path = store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample())

    assert logs == [
        (
            "Daily Parquet stored dataset={} code={} rows={} path={}",
            ("daily_k_qfq", "sh.600000", 2, path),
        )
    ]


def test_checkpoint_write_does_not_log_parquet_success(tmp_path, monkeypatch) -> None:
    logs = []

    class FakeLogger:
        def info(self, message, *args, **kwargs) -> None:
            logs.append((message, args))

        def warning(self, message, *args, **kwargs) -> None:
            return None

    monkeypatch.setattr(parquet_store_module, "logger", FakeLogger())
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    store.upsert_pipeline_checkpoints(
        pd.DataFrame(
            [
                {
                    "pipeline": "update_daily",
                    "dataset": "daily_k_qfq",
                    "code": "sh.600000",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "status": "success",
                    "row_count": 2,
                    "output_path": "daily_k_qfq/code=sh.600000/data.parquet",
                    "updated_at": datetime(2024, 1, 31, 16, 0),
                    "error_stack": "",
                }
            ]
        )
    )

    assert logs == []


def test_pipeline_checkpoint_requires_success_and_output_file(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    output_path = store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample())

    checkpoint = pd.DataFrame(
        [
            {
                "pipeline": "update_daily",
                "dataset": "daily_k_qfq",
                "code": "sh.600000",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "status": "success",
                "row_count": 2,
                "output_path": str(output_path),
                "updated_at": datetime(2024, 1, 31, 16, 0),
                "error_stack": "",
            }
        ]
    )
    store.upsert_pipeline_checkpoints(checkpoint)

    assert store.pipeline_checkpoint_succeeded(
        "update_daily", "daily_k_qfq", "sh.600000", "2024-01-01", "2024-01-31", output_path
    )
    assert should_skip_checkpoint(
        store,
        "update_daily",
        "daily_k_qfq",
        "sh.600000",
        "2024-01-01",
        "2024-01-31",
        output_path,
        resume=True,
        force=False,
    )
    assert not should_skip_checkpoint(
        store,
        "update_daily",
        "daily_k_qfq",
        "sh.600000",
        "2024-01-01",
        "2024-01-31",
        output_path,
        resume=True,
        force=True,
    )
    assert not should_skip_checkpoint(
        store,
        "update_daily",
        "daily_k_qfq",
        "sh.600000",
        "2024-01-01",
        "2024-01-31",
        output_path,
        resume=False,
        force=False,
    )

    output_path.unlink()

    assert not store.pipeline_checkpoint_succeeded(
        "update_daily", "daily_k_qfq", "sh.600000", "2024-01-01", "2024-01-31", output_path
    )


def test_checkpoint_date_resume_matches_update_daily_end_date(tmp_path, daily_sample) -> None:
    def store_with_checkpoint(root, start_date: str):
        store = ParquetStore(root=root)
        store.ensure_layout()
        output_path = store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample())
        store.upsert_pipeline_checkpoints(
            pd.DataFrame(
                [
                    {
                        "pipeline": "update_daily",
                        "dataset": "daily_k_qfq",
                        "code": "sh.600000",
                        "start_date": start_date,
                        "end_date": "2024-01-31",
                        "status": "success",
                        "row_count": 2,
                        "output_path": str(output_path),
                        "updated_at": datetime(2024, 1, 31, 16, 0),
                        "error_stack": "",
                    }
                ]
            )
        )
        return store, output_path, PipelineCheckpointLookup.from_store(store)

    def assert_skip(store, output_path, lookup, expected: bool) -> None:
        args = (
            store,
            "update_daily",
            "daily_k_qfq",
            "sh.600000",
            "2024-01-15",
            "2024-01-31",
            output_path,
        )
        assert should_skip_checkpoint(*args, resume=True, force=False) == expected
        assert should_skip_checkpoint(*args, resume=True, force=False, checkpoint_lookup=lookup) == expected

    update_store, update_output_path, update_lookup = store_with_checkpoint(tmp_path / "update_only", "2024-01-01")
    assert_skip(update_store, update_output_path, update_lookup, True)


def test_checkpoint_lookup_matches_store_resume_semantics(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    output_path = store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample())
    missing_path = store.daily_k_path("daily_k_qfq", "sz.000001")

    store.upsert_pipeline_checkpoints(
        pd.DataFrame(
            [
                {
                    "pipeline": "update_daily",
                    "dataset": "daily_k_qfq",
                    "code": "sh.600000",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "status": "success",
                    "row_count": 2,
                    "output_path": str(output_path),
                    "updated_at": datetime(2024, 2, 1, 16, 0),
                    "error_stack": "",
                },
                {
                    "pipeline": "update_daily",
                    "dataset": "daily_k_qfq",
                    "code": "sz.000001",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "status": "failed",
                    "row_count": 0,
                    "output_path": str(missing_path),
                    "updated_at": datetime(2024, 2, 1, 16, 0),
                    "error_stack": "boom",
                },
            ]
        )
    )
    lookup = PipelineCheckpointLookup.from_store(store)

    scenarios = [
        ("update_daily", "daily_k_qfq", "sh.600000", "2024-01-01", "2024-01-31", output_path),
        ("update_daily", "daily_k_qfq", "sh.600000", "2024-01-15", "2024-01-31", output_path),
        ("update_daily", "daily_k_qfq", "sz.000001", "2024-01-01", "2024-01-31", missing_path),
    ]
    for pipeline, dataset, code, start_date, end_date, path in scenarios:
        assert should_skip_checkpoint(
            store,
            pipeline,
            dataset,
            code,
            start_date,
            end_date,
            path,
            resume=True,
            force=False,
        ) == should_skip_checkpoint(
            store,
            pipeline,
            dataset,
            code,
            start_date,
            end_date,
            path,
            resume=True,
            force=False,
            checkpoint_lookup=lookup,
        )


def test_persist_update_metadata_batches_match_individual_writes(tmp_path) -> None:
    run_rows = [
        {
            "task_id": "task-1",
            "dataset": "daily_k_qfq",
            "code": "sh.600000",
            "status": "success",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "start_time": datetime(2024, 1, 31, 9, 0),
            "end_time": datetime(2024, 1, 31, 9, 1),
            "row_count": 2,
            "error_stack": "",
        }
    ]
    status_rows = [
        {
            "dataset": "daily_k_qfq",
            "code": "sh.600000",
            "last_success_date": "2024-01-31",
            "row_count": 2,
            "status": "success",
            "updated_at": datetime(2024, 1, 31, 9, 1),
            "error_stack": "",
        }
    ]
    checkpoint_rows = [
        {
            "pipeline": "update_daily",
            "dataset": "daily_k_qfq",
            "code": "sh.600000",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "status": "success",
            "row_count": 2,
            "output_path": "daily_k_qfq/code=sh.600000/data.parquet",
            "updated_at": datetime(2024, 1, 31, 9, 1),
            "error_stack": "",
        }
    ]

    individual = ParquetStore(root=tmp_path / "individual")
    batched = ParquetStore(root=tmp_path / "batched")
    individual.ensure_layout()
    batched.ensure_layout()

    individual.append_update_runs(pd.DataFrame(run_rows))
    individual.upsert_update_status(pd.DataFrame(status_rows))
    individual.upsert_pipeline_checkpoints(pd.DataFrame(checkpoint_rows))
    batched.persist_update_metadata(run_rows, status_rows, checkpoint_rows)

    readers = {
        "update_runs": ParquetStore.read_update_runs,
        "update_status": ParquetStore.read_update_status,
        "pipeline_checkpoints": ParquetStore.read_pipeline_checkpoints,
    }
    for reader in readers.values():
        left = reader(individual)
        right = reader(batched)
        pd.testing.assert_frame_equal(left, right)


def test_duckdb_metadata_ignores_parquet_metadata_files(tmp_path) -> None:
    metadata_dir = tmp_path / "data" / "metadata"
    metadata_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "pipeline": "update_daily",
                "dataset": "daily_k_qfq",
                "code": "sh.600000",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "status": "success",
                "row_count": 2,
                "output_path": "daily_k_qfq/code=sh.600000/data.parquet",
                "updated_at": datetime(2024, 1, 31, 9, 1),
                "error_stack": "",
            }
        ]
    ).to_parquet(metadata_dir / "pipeline_checkpoints.parquet")

    store = ParquetStore(root=tmp_path)
    checkpoints = store.read_pipeline_checkpoints()

    assert checkpoints.empty
    assert (tmp_path / "data" / "duckdb" / "quant.duckdb").exists()


def test_update_status_upsert_replaces_existing_row(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    first = pd.DataFrame(
        [
            {
                "dataset": "daily_k_qfq",
                "code": "sh.600000",
                "last_success_date": "2024-01-30",
                "row_count": 1,
                "status": "success",
                "updated_at": datetime(2024, 1, 30, 9, 1),
                "error_stack": "",
            }
        ]
    )
    second = first.assign(last_success_date="2024-01-31", row_count=2, updated_at=datetime(2024, 1, 31, 9, 1))

    store.upsert_update_status(first)
    store.upsert_update_status(second)

    status = store.read_update_status()
    assert len(status) == 1
    assert status.loc[0, "row_count"] == 2
    assert pd.to_datetime(status.loc[0, "last_success_date"]).date().isoformat() == "2024-01-31"


def test_metadata_batch_flush_size_one_keeps_concurrent_rows(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    batch = PipelineMetadataBatch(store, flush_size=1, count_by="run")

    def add_rows(index: int) -> None:
        code = f"sh.{600000 + index}"
        now = datetime(2024, 1, 31, 9, index % 60)
        batch.add(
            run_row={
                "task_id": f"task-{index}",
                "dataset": "daily_k_qfq",
                "code": code,
                "status": "success",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "start_time": now,
                "end_time": now,
                "row_count": 2,
                "error_stack": "",
            },
            status_row={
                "dataset": "daily_k_qfq",
                "code": code,
                "last_success_date": "2024-01-31",
                "row_count": 2,
                "status": "success",
                "updated_at": now,
                "error_stack": "",
            },
            checkpoint={
                "pipeline": "update_daily",
                "dataset": "daily_k_qfq",
                "code": code,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "status": "success",
                "row_count": 2,
                "output_path": f"daily_k_qfq/code={code}/data.parquet",
                "updated_at": now,
                "error_stack": "",
            },
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(add_rows, range(20)))
    batch.flush()

    assert len(store.read_update_runs()) == 20
    assert len(store.read_update_status()) == 20
    assert len(store.read_pipeline_checkpoints()) == 20


def _akshare_hist_row(source_endpoint: str, quality_status: str, close: float) -> dict[str, object]:
    return {
        "date": "2024-01-03",
        "code": "600000",
        "source_symbol": "600000",
        "open": 8.2,
        "high": 8.4,
        "low": 8.1,
        "close": close,
        "volume": 120000,
        "amount": 9960.0,
        "amplitude": 3.0,
        "pct_chg": 1.2,
        "change_amount": 0.1,
        "turnover_rate": 0.12,
        "adjust": "none",
        "source_endpoint": source_endpoint,
        "quality_status": quality_status,
        "fetched_at": datetime(2024, 1, 3, 16, 0),
    }
