from __future__ import annotations

import gc
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import pytest

import src.storage.metadata_store as metadata_store_module
import src.storage.parquet_store as parquet_store_module
from src.pipeline.common import PipelineCheckpointLookup, baostock_cn_stock_basic_codes, should_skip_checkpoint
from src.pipeline.lifecycle import PipelineMetadataBatch
from src.storage.parquet_store import ParquetStore


def test_dataset_interface_replaces_partitioned_daily_bar(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    raw = daily_sample().astype({"volume": "string", "pe_ttm": "string"})
    raw.loc[0, "pe_ttm"] = ""

    result = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq",
        raw,
        partition={"code": "sh.600000"},
    )

    assert result.primary_path == (
        tmp_path / "data" / "parquet" / "baostock_cn_stock_daily_bar_qfq" / "code=sh.600000" / "data.parquet"
    )
    assert result.row_count == 2
    assert result.updated_partitions == 1
    assert result.skipped_partitions == 0
    assert store.dataset_exists("baostock_cn_stock_daily_bar_qfq", {"code": "sh.600000"})
    loaded = store.read_dataset("baostock_cn_stock_daily_bar_qfq", {"code": "sh.600000"})
    assert loaded["volume"].tolist() == [1000, 1200]
    assert pd.isna(loaded.loc[0, "pe_ttm"])


def test_dataset_interface_merges_unpartitioned_trading_calendar(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-05", "is_trading_day": "1"},
                {"calendar_date": "2024-01-06", "is_trading_day": "1"},
            ]
        ),
    )

    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
                {"calendar_date": "2024-01-07", "is_trading_day": "0"},
            ]
        ),
    )

    loaded = store.read_dataset("baostock_cn_trading_calendar")
    assert pd.to_datetime(loaded["calendar_date"], errors="coerce").dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
    ]
    assert loaded["is_trading_day"].astype("string").tolist() == ["1", "0", "0"]


def test_dataset_interface_upserts_daily_bars_and_skips_existing(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    spot_hist = _akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", close=8.3)
    daily_bar_confirmed = _akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", close=8.31)

    first = store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted",
        pd.DataFrame([spot_hist]),
        partition={"code": "600000"},
        mode="upsert",
    )
    second = store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted",
        pd.DataFrame([daily_bar_confirmed]),
        partition={"code": "600000"},
        mode="upsert",
    )
    skipped = store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted",
        pd.DataFrame([daily_bar_confirmed]),
        partition={"code": "600000"},
        mode="upsert",
        skip_existing=True,
    )

    hist = store.read_dataset("akshare_cn_stock_daily_bar_unadjusted", {"code": "600000"})
    assert first.updated_partitions == 1
    assert second.updated_partitions == 1
    assert skipped.updated_partitions == 0
    assert skipped.skipped_partitions == 1
    assert len(hist) == 1
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "daily_bar_confirmed"


def test_dataset_interface_upserts_multiple_partitions(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    rows = pd.DataFrame(
        [
            _akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", close=8.3),
            {
                **_akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", close=11.2),
                "code": "000001",
                "source_symbol": "000001",
            },
        ]
    )

    result = store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted",
        rows,
        mode="upsert",
    )

    assert result.row_count == 2
    assert result.updated_partitions == 2
    assert len(result.paths) == 2
    assert store.read_dataset("akshare_cn_stock_daily_bar_unadjusted", {"code": "600000"}).loc[0, "close"] == 8.3
    assert store.read_dataset("akshare_cn_stock_daily_bar_unadjusted", {"code": "000001"}).loc[0, "close"] == 11.2


def test_dataset_interface_latest_partition_and_path_validation(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    fetched_at = datetime(2024, 1, 3, 16, 0)

    with pytest.raises(ValueError, match="Unsupported dataset"):
        store.dataset_path("missing_dataset")
    with pytest.raises(ValueError, match="requires partition code"):
        store.dataset_path("akshare_cn_stock_valuation_eastmoney")
    with pytest.raises(ValueError, match="unexpected partition"):
        store.dataset_path("akshare_cn_stock_valuation_eastmoney", {"snapshot_date": "2024-01-03"})
    with pytest.raises(ValueError, match="AkShare partition code must be 6 digits"):
        store.dataset_path("akshare_cn_stock_valuation_eastmoney", {"code": "sh.600000"})

    store.write_dataset(
        "akshare_cn_stock_delist_sh",
        pd.DataFrame(
            [
                {
                    "snapshot_date": "2024-01-02",
                    "exchange": "sh",
                    "market": "全部",
                    "code": "600001",
                    "source_symbol": "600001",
                    "name": "Old Corp",
                    "list_date": "2000-01-01",
                    "delist_date": "2024-01-02",
                    "source_endpoint": "akshare_cn_stock_delist_sh",
                    "fetched_at": fetched_at,
                }
            ]
        ),
        partition={"snapshot_date": "2024-01-02"},
    )
    store.write_dataset(
        "akshare_cn_stock_delist_sh",
        pd.DataFrame(
            [
                {
                    "snapshot_date": "2024-01-03",
                    "exchange": "sh",
                    "market": "全部",
                    "code": "600002",
                    "source_symbol": "600002",
                    "name": "New Corp",
                    "list_date": "2000-01-01",
                    "delist_date": "2024-01-03",
                    "source_endpoint": "akshare_cn_stock_delist_sh",
                    "fetched_at": fetched_at,
                }
            ]
        ),
        partition={"snapshot_date": "2024-01-03"},
    )

    assert store.latest_dataset_partition("akshare_cn_stock_delist_sh") == "2024-01-03"
    assert store.read_latest_dataset("akshare_cn_stock_delist_sh").loc[0, "code"] == "600002"


def test_list_dataset_partitions_returns_empty_for_non_partitioned_dataset(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    assert store.list_dataset_partitions("baostock_cn_trading_calendar") == ()


def test_list_dataset_partitions_returns_stable_values(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    sh_rows = daily_sample()
    sz_rows = daily_sample().assign(code="sz.000001")

    store.write_dataset("baostock_cn_stock_daily_bar_qfq", sz_rows, {"code": "sz.000001"})
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", sh_rows, {"code": "sh.600000"})

    assert store.list_dataset_partitions("baostock_cn_stock_daily_bar_qfq") == ("sh.600000", "sz.000001")


def test_list_dataset_partitions_returns_empty_for_empty_dataset(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    assert store.list_dataset_partitions("akshare_cn_stock_daily_bar_unadjusted") == ()


def test_list_dataset_partitions_ignores_tmp_parquet_only(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    partition_dir = tmp_path / "data" / "parquet" / "akshare_cn_stock_daily_bar_unadjusted" / "code=600000"
    partition_dir.mkdir(parents=True)
    (partition_dir / "data.tmp.parquet").write_text("pending", encoding="utf-8")

    assert store.list_dataset_partitions("akshare_cn_stock_daily_bar_unadjusted") == ()


def test_daily_bar_atomic_write(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    raw = daily_sample().astype({"volume": "string", "pe_ttm": "string"})
    raw.loc[0, "pe_ttm"] = ""

    path = store.write_dataset("baostock_cn_stock_daily_bar_qfq", raw, {"code": "sh.600000"}).primary_path

    assert path.exists()
    assert not (path.parent / "data.tmp.parquet").exists()
    loaded = pd.read_parquet(path)
    assert len(loaded) == 2
    assert loaded["volume"].tolist() == [1000, 1200]
    assert pd.isna(loaded.loc[0, "pe_ttm"])


def test_baostock_cn_stock_basic_codes_from_latest_snapshot(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    assert baostock_cn_stock_basic_codes(store, "all") == ["sh.000001", "sh.600000", "sz.000001"]
    assert baostock_cn_stock_basic_codes(store, "active") == ["sh.000001", "sh.600000"]
    assert baostock_cn_stock_basic_codes(store, "all", security_type="1") == ["sh.600000", "sz.000001"]
    assert baostock_cn_stock_basic_codes(store, "active", security_type="1") == ["sh.600000"]


def test_write_baostock_cn_trading_calendar_merges_existing_dates(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-05", "is_trading_day": "1"},
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
            ]
        ),
    )
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
                {"calendar_date": "2024-01-07", "is_trading_day": "0"},
            ]
        ),
    )

    baostock_cn_trading_calendar = store.read_dataset("baostock_cn_trading_calendar")
    assert pd.to_datetime(baostock_cn_trading_calendar["calendar_date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    ).tolist() == [
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
    ]


def test_baostock_cn_stock_adjustment_factor_write_and_read(
    tmp_path, baostock_cn_stock_adjustment_factor_sample
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    path = store.write_dataset(
        "baostock_cn_stock_adjustment_factor",
        baostock_cn_stock_adjustment_factor_sample().astype({"forward_adjust_factor": "string"}),
        {"code": "sh.600000"},
    ).primary_path

    assert path.exists()
    loaded = store.read_dataset("baostock_cn_stock_adjustment_factor", {"code": "sh.600000"})
    assert len(loaded) == 1
    assert loaded.loc[0, "forward_adjust_factor"] == 1.0


def test_akshare_dataset_write_and_read(tmp_path, akshare_cn_stock_valuation_eastmoney_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    value_path = store.write_dataset(
        "akshare_cn_stock_valuation_eastmoney",
        akshare_cn_stock_valuation_eastmoney_sample().astype({"pe_ttm": "string"}),
        {"code": "600000"},
    ).primary_path

    assert (
        value_path
        == tmp_path / "data" / "parquet" / "akshare_cn_stock_valuation_eastmoney" / "code=600000" / "data.parquet"
    )
    loaded = store.read_dataset("akshare_cn_stock_valuation_eastmoney", {"code": "600000"})
    assert len(loaded) == 2
    assert loaded.loc[0, "pe_ttm"] == 5.0


def test_akshare_a_stock_writes_and_hist_upsert_overrides_spot(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    fetched_at = datetime(2024, 1, 3, 16, 0)

    delist_path = store.write_dataset(
        "akshare_cn_stock_delist_sh",
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
                    "source_endpoint": "akshare_cn_stock_delist_sh",
                    "fetched_at": fetched_at,
                }
            ]
        ),
        {"snapshot_date": "2024-01-03"},
    ).primary_path
    assert (
        delist_path
        == tmp_path / "data" / "parquet" / "akshare_cn_stock_delist_sh" / "snapshot_date=2024-01-03" / "data.parquet"
    )
    assert store.read_latest_dataset("akshare_cn_stock_delist_sh").loc[0, "code"] == "600001"

    sz_delist_path = store.write_dataset(
        "akshare_cn_stock_delist_sz",
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
                    "source_endpoint": "akshare_cn_stock_delist_sz",
                    "fetched_at": fetched_at,
                }
            ]
        ),
        {"snapshot_date": "2024-01-03"},
    ).primary_path
    assert (
        sz_delist_path
        == tmp_path / "data" / "parquet" / "akshare_cn_stock_delist_sz" / "snapshot_date=2024-01-03" / "data.parquet"
    )
    assert store.read_latest_dataset("akshare_cn_stock_delist_sz").loc[0, "code"] == "000001"

    spot_path = store.write_dataset(
        "akshare_cn_stock_spot_quote_eastmoney",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
                    "code": "600000",
                    "source_symbol": "600000",
                    "name": "PF Bank",
                    "last_price": "8.30",
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
                    "source_endpoint": "stock_zh_a_spot_em",
                    "fetched_at": fetched_at,
                }
            ]
        ),
        {"trade_date": "2024-01-03"},
    ).primary_path
    assert spot_path.exists()
    assert store.read_latest_dataset("akshare_cn_stock_spot_quote_eastmoney").loc[0, "last_price"] == 8.3

    sina_path = store.write_dataset(
        "akshare_cn_stock_spot_quote_sina",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
                    "code": "600000",
                    "source_symbol": "sh600000",
                    "name": "PF Bank",
                    "last_price": 8.3,
                    "price_change": 0.1,
                    "pct_change": 1.2,
                    "bid": 8.29,
                    "ask": 8.31,
                    "prev_close": 8.2,
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
        {"trade_date": "2024-01-03"},
    ).primary_path
    assert sina_path.exists()
    assert bool(
        store.read_dataset("akshare_cn_stock_spot_quote_sina", {"trade_date": "2024-01-03"}).loc[0, "is_fallback"]
    )

    spot_hist = _akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", close=8.3)
    daily_bar_confirmed = _akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", close=8.31)
    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", pd.DataFrame([spot_hist]), {"code": "600000"})
    store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted", pd.DataFrame([daily_bar_confirmed]), {"code": "600000"}, mode="upsert"
    )
    hist = store.read_dataset("akshare_cn_stock_daily_bar_unadjusted", {"code": "600000"})
    assert len(hist) == 1
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "daily_bar_confirmed"


def test_writes_reject_missing_partition_keys(
    tmp_path,
    daily_sample,
    baostock_cn_stock_adjustment_factor_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    with pytest.raises(ValueError, match="baostock_cn_stock_daily_bar_qfq partition code missing code"):
        store.write_dataset(
            "baostock_cn_stock_daily_bar_qfq", daily_sample().drop(columns=["code"]), {"code": "sh.600000"}
        )
    with pytest.raises(ValueError, match="baostock_cn_stock_adjustment_factor partition code missing code"):
        store.write_dataset(
            "baostock_cn_stock_adjustment_factor",
            baostock_cn_stock_adjustment_factor_sample().drop(columns=["code"]),
            {"code": "sh.600000"},
        )
    with pytest.raises(ValueError, match="akshare_cn_stock_valuation_eastmoney partition code missing code"):
        store.write_dataset(
            "akshare_cn_stock_valuation_eastmoney",
            akshare_cn_stock_valuation_eastmoney_sample().drop(columns=["code"]),
            {"code": "600000"},
        )


def test_writes_reject_partition_key_mismatch(tmp_path, akshare_cn_stock_valuation_eastmoney_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    with pytest.raises(ValueError, match="akshare_cn_stock_valuation_eastmoney partition code mismatch"):
        store.write_dataset(
            "akshare_cn_stock_valuation_eastmoney",
            akshare_cn_stock_valuation_eastmoney_sample("000001"),
            {"code": "600000"},
        )


def test_daily_bar_write_logs_parquet_success(tmp_path, daily_sample, monkeypatch) -> None:
    logs = []

    class FakeLogger:
        def info(self, message, *args, **kwargs) -> None:
            logs.append((message, args))

        def warning(self, message, *args, **kwargs) -> None:
            return None

    monkeypatch.setattr(parquet_store_module, "logger", FakeLogger())
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    path = store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}).primary_path

    assert len(logs) == 1
    message, args = logs[0]
    assert message == "Dataset Parquet stored run_id={} pid={} thread={} dataset={} rows={} path={}"
    assert args[0] == "-"
    assert isinstance(args[1], int)
    assert args[2:] == ("MainThread", "baostock_cn_stock_daily_bar_qfq", 2, path)


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
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
                    "code": "sh.600000",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "status": "success",
                    "row_count": 2,
                    "output_path": "baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet",
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
    output_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}
    ).primary_path

    checkpoint = pd.DataFrame(
        [
            {
                "pipeline": "update_daily",
                "dataset": "baostock_cn_stock_daily_bar_qfq",
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
        "update_daily", "baostock_cn_stock_daily_bar_qfq", "sh.600000", "2024-01-01", "2024-01-31", output_path
    )
    assert should_skip_checkpoint(
        store,
        "update_daily",
        "baostock_cn_stock_daily_bar_qfq",
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
        "baostock_cn_stock_daily_bar_qfq",
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
        "baostock_cn_stock_daily_bar_qfq",
        "sh.600000",
        "2024-01-01",
        "2024-01-31",
        output_path,
        resume=False,
        force=False,
    )

    output_path.unlink()

    assert not store.pipeline_checkpoint_succeeded(
        "update_daily", "baostock_cn_stock_daily_bar_qfq", "sh.600000", "2024-01-01", "2024-01-31", output_path
    )


def test_checkpoint_date_resume_matches_update_daily_end_date(tmp_path, daily_sample) -> None:
    def store_with_checkpoint(root, start_date: str):
        store = ParquetStore(root=root)
        store.ensure_layout()
        output_path = store.write_dataset(
            "baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}
        ).primary_path
        store.upsert_pipeline_checkpoints(
            pd.DataFrame(
                [
                    {
                        "pipeline": "update_daily",
                        "dataset": "baostock_cn_stock_daily_bar_qfq",
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
            "baostock_cn_stock_daily_bar_qfq",
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
    output_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"}
    ).primary_path
    missing_path = store.dataset_path("baostock_cn_stock_daily_bar_qfq", {"code": "sz.000001"})

    store.upsert_pipeline_checkpoints(
        pd.DataFrame(
            [
                {
                    "pipeline": "update_daily",
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
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
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
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
        ("update_daily", "baostock_cn_stock_daily_bar_qfq", "sh.600000", "2024-01-01", "2024-01-31", output_path),
        ("update_daily", "baostock_cn_stock_daily_bar_qfq", "sh.600000", "2024-01-15", "2024-01-31", output_path),
        ("update_daily", "baostock_cn_stock_daily_bar_qfq", "sz.000001", "2024-01-01", "2024-01-31", missing_path),
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
            "dataset": "baostock_cn_stock_daily_bar_qfq",
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
            "dataset": "baostock_cn_stock_daily_bar_qfq",
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
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "status": "success",
            "row_count": 2,
            "output_path": "baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet",
            "updated_at": datetime(2024, 1, 31, 9, 1),
            "error_stack": "",
        }
    ]

    individual = ParquetStore(root=tmp_path / "individual")
    batched = ParquetStore(root=tmp_path / "batched")
    individual.ensure_layout()
    batched.ensure_layout()

    individual.append_pipeline_runs(pd.DataFrame(run_rows))
    individual.upsert_dataset_update_status(pd.DataFrame(status_rows))
    individual.upsert_pipeline_checkpoints(pd.DataFrame(checkpoint_rows))
    batched.persist_update_metadata(run_rows, status_rows, checkpoint_rows)

    readers = {
        "pipeline_runs": ParquetStore.read_pipeline_runs,
        "dataset_update_status": ParquetStore.read_dataset_update_status,
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
                "dataset": "baostock_cn_stock_daily_bar_qfq",
                "code": "sh.600000",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "status": "success",
                "row_count": 2,
                "output_path": "baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet",
                "updated_at": datetime(2024, 1, 31, 9, 1),
                "error_stack": "",
            }
        ]
    ).to_parquet(metadata_dir / "pipeline_checkpoints.parquet")

    store = ParquetStore(root=tmp_path)
    checkpoints = store.read_pipeline_checkpoints()

    assert checkpoints.empty
    assert (tmp_path / "data" / "duckdb" / "quant.duckdb").exists()


def test_dataset_update_status_upsert_replaces_existing_row(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    first = pd.DataFrame(
        [
            {
                "dataset": "baostock_cn_stock_daily_bar_qfq",
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

    store.upsert_dataset_update_status(first)
    store.upsert_dataset_update_status(second)

    status = store.read_dataset_update_status()
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
                "dataset": "baostock_cn_stock_daily_bar_qfq",
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
                "dataset": "baostock_cn_stock_daily_bar_qfq",
                "code": code,
                "last_success_date": "2024-01-31",
                "row_count": 2,
                "status": "success",
                "updated_at": now,
                "error_stack": "",
            },
            checkpoint={
                "pipeline": "update_daily",
                "dataset": "baostock_cn_stock_daily_bar_qfq",
                "code": code,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "status": "success",
                "row_count": 2,
                "output_path": f"baostock_cn_stock_daily_bar_qfq/code={code}/data.parquet",
                "updated_at": now,
                "error_stack": "",
            },
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(add_rows, range(20)))
    batch.flush()

    assert len(store.read_pipeline_runs()) == 20
    assert len(store.read_dataset_update_status()) == 20
    assert len(store.read_pipeline_checkpoints()) == 20


def test_metadata_store_global_locks_do_not_retain_closed_store_paths(tmp_path) -> None:
    before = len(metadata_store_module._DB_LOCKS)
    stores = [ParquetStore(root=tmp_path / f"store-{index}") for index in range(10)]
    created_paths = {store._metadata_store.duckdb_file for store in stores}

    assert len(metadata_store_module._DB_LOCKS) >= before + 10
    assert created_paths <= set(metadata_store_module._DB_LOCKS.keys())

    for store in stores:
        store.close()
    del store
    stores.clear()
    gc.collect()

    assert created_paths.isdisjoint(metadata_store_module._DB_LOCKS.keys())


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
        "pct_change": 1.2,
        "price_change": 0.1,
        "turnover_rate": 0.12,
        "adjustment": "unadjusted",
        "source_endpoint": source_endpoint,
        "quality_status": quality_status,
        "fetched_at": datetime(2024, 1, 3, 16, 0),
    }
