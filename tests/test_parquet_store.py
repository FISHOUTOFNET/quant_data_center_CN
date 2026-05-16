from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import threading
import time

import duckdb
import pandas as pd
import pytest

from src.analytics.valuation_percentile import compute_valuation_percentiles
from src.pipeline.common import PipelineCheckpointLookup, should_skip_checkpoint
from src.pipeline.services import PipelineMetadataBatch
import src.storage.metadata_store as metadata_store_module
import src.storage.parquet_store as parquet_store_module
from src.storage.parquet_store import ParquetStore


def test_daily_bar_atomic_write(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    raw = daily_sample().astype({"volume": "string", "pe_ttm": "string"})
    raw.loc[0, "pe_ttm"] = ""

    path = store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", raw)

    assert path.exists()
    assert not (path.parent / "data.tmp.parquet").exists()
    loaded = pd.read_parquet(path)
    assert len(loaded) == 2
    assert loaded["volume"].tolist() == [1000, 1200]
    assert pd.isna(loaded.loc[0, "pe_ttm"])


def test_read_baostock_daily_bars_supports_column_projection(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", daily_sample())

    loaded = store.read_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        "sh.600000",
        columns=["date", "code", "pe_ttm"],
    )

    assert loaded.columns.tolist() == ["date", "code", "pe_ttm"]
    assert len(loaded) == 2


def test_read_baostock_daily_bars_falls_back_to_full_file_when_projected_column_is_missing(
    tmp_path,
    daily_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", daily_sample())

    loaded = store.read_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        "sh.600000",
        columns=["date", "code", "pe_ttm", "legacy_missing_column"],
    )

    assert "open" in loaded.columns
    assert "legacy_missing_column" not in loaded.columns
    assert len(loaded) == 2


def test_baostock_cn_stock_basic_codes_from_latest_snapshot(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_stock_basic(baostock_cn_stock_basic_sample())

    assert store.baostock_cn_stock_basic_codes("all") == ["sh.000001", "sh.600000", "sz.000001"]
    assert store.baostock_cn_stock_basic_codes("active") == ["sh.600000"]


def test_write_baostock_cn_trading_calendar_merges_existing_dates(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_trading_calendar(
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-05", "is_trading_day": "1"},
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
            ]
        )
    )
    store.write_baostock_cn_trading_calendar(
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
                {"calendar_date": "2024-01-07", "is_trading_day": "0"},
            ]
        )
    )

    baostock_cn_trading_calendar = store.read_baostock_cn_trading_calendar()
    assert pd.to_datetime(baostock_cn_trading_calendar["calendar_date"], errors="coerce").dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-05",
        "2024-01-06",
        "2024-01-07",
    ]


def test_baostock_cn_stock_adjustment_factor_write_and_read(tmp_path, baostock_cn_stock_adjustment_factor_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    path = store.write_baostock_cn_stock_adjustment_factor("sh.600000", baostock_cn_stock_adjustment_factor_sample().astype({"forward_adjust_factor": "string"}))

    assert path.exists()
    loaded = store.read_baostock_cn_stock_adjustment_factor("sh.600000")
    assert len(loaded) == 1
    assert loaded.loc[0, "forward_adjust_factor"] == 1.0


def test_baostock_cn_stock_valuation_percentile_write_and_read(tmp_path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    frame = compute_valuation_percentiles(daily_sample())

    path = store.write_baostock_cn_stock_valuation_percentile("sh.600000", frame)

    assert path == tmp_path / "data" / "parquet" / "baostock_cn_stock_valuation_percentile" / "code=sh.600000" / "data.parquet"
    loaded = store.read_baostock_cn_stock_valuation_percentile("sh.600000")
    assert len(loaded) == 2
    assert loaded.loc[0, "pe_ttm_percentile_all_history"] == 100.0


def test_valuation_percentile_direct_write_defers_registry_inventory_by_default(
    tmp_path,
    daily_sample,
    monkeypatch,
) -> None:
    refresh_calls: list[list[str]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            del status_rows
            refresh_calls.append(list(dataset_ids or []))
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    frame = compute_valuation_percentiles(daily_sample())

    store.write_baostock_cn_stock_valuation_percentile("sh.600000", frame)

    assert refresh_calls == []


def test_valuation_percentile_write_can_defer_registry_inventory_refresh(
    tmp_path,
    daily_sample,
    monkeypatch,
) -> None:
    refresh_calls: list[list[str]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            del status_rows
            refresh_calls.append(list(dataset_ids or []))
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    frame = compute_valuation_percentiles(daily_sample())

    store.write_baostock_cn_stock_valuation_percentile(
        "sh.600000",
        frame,
        refresh_registry_inventory=False,
    )

    assert refresh_calls == []


def test_valuation_percentile_write_can_refresh_registry_inventory_immediately(
    tmp_path,
    daily_sample,
    monkeypatch,
) -> None:
    refresh_calls: list[list[str]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            del status_rows
            refresh_calls.append(list(dataset_ids or []))
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    frame = compute_valuation_percentiles(daily_sample())

    store.write_baostock_cn_stock_valuation_percentile(
        "sh.600000",
        frame,
        refresh_registry_inventory=True,
    )
    store.refresh_pending_registry_inventory()

    assert refresh_calls == [["baostock_cn_stock_valuation_percentile"]]


def test_parquet_store_refreshes_pending_registry_inventory_once_after_writes(
    tmp_path,
    daily_sample,
    monkeypatch,
) -> None:
    refresh_calls: list[dict[str, object]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            refresh_calls.append(
                {
                    "dataset_ids": list(dataset_ids or []),
                    "status_rows": len(pd.DataFrame(status_rows)),
                }
            )
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    frame = compute_valuation_percentiles(daily_sample())

    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())
    store.write_baostock_cn_stock_valuation_percentile("sh.600000", frame)
    store.refresh_pending_registry_inventory()
    store.refresh_pending_registry_inventory()

    assert refresh_calls == [
        {
            "dataset_ids": [
                "baostock_cn_stock_daily_bar_qfq",
                "baostock_cn_stock_valuation_percentile",
            ],
            "status_rows": 0,
        }
    ]


def test_parquet_store_metadata_status_marks_pending_inventory_without_immediate_refresh(
    tmp_path,
    monkeypatch,
) -> None:
    refresh_calls: list[dict[str, object]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            refresh_calls.append(
                {
                    "dataset_ids": list(dataset_ids or []),
                    "status_rows": len(pd.DataFrame(status_rows)),
                }
            )
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    status = pd.DataFrame(
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

    store.upsert_dataset_update_status(status)

    assert refresh_calls == []
    store.refresh_pending_registry_inventory()
    assert refresh_calls == [
        {
            "dataset_ids": ["baostock_cn_stock_daily_bar_qfq"],
            "status_rows": 1,
        }
    ]


def test_parquet_store_pending_registry_inventory_noops_without_changes(
    tmp_path,
    monkeypatch,
) -> None:
    refresh_calls: list[list[str]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            del status_rows
            refresh_calls.append(list(dataset_ids or []))
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    store.refresh_pending_registry_inventory()
    store.close()

    assert refresh_calls == []


def test_akshare_dataset_write_and_read(tmp_path, akshare_cn_stock_valuation_eastmoney_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    value_path = store.write_akshare_cn_stock_valuation_eastmoney("600000", akshare_cn_stock_valuation_eastmoney_sample().astype({"pe_ttm": "string"}))

    assert value_path == tmp_path / "data" / "parquet" / "akshare_cn_stock_valuation_eastmoney" / "code=600000" / "data.parquet"
    loaded = store.read_akshare_cn_stock_valuation_eastmoney("600000")
    assert len(loaded) == 2
    assert loaded.loc[0, "pe_ttm"] == 5.0


def test_akshare_a_stock_writes_and_hist_upsert_overrides_spot(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    fetched_at = datetime(2024, 1, 3, 16, 0)

    delist_path = store.write_akshare_cn_stock_delist_sh(
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
                    "source_endpoint": "akshare_cn_stock_delist_sh",
                    "fetched_at": fetched_at,
                }
            ]
        ),
    )
    assert delist_path == tmp_path / "data" / "parquet" / "akshare_cn_stock_delist_sh" / "snapshot_date=2024-01-03" / "data.parquet"
    assert store.read_latest_akshare_cn_stock_delist_sh().loc[0, "code"] == "600001"

    sz_delist_path = store.write_akshare_cn_stock_delist_sz(
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
                    "source_endpoint": "akshare_cn_stock_delist_sz",
                    "fetched_at": fetched_at,
                }
            ]
        ),
    )
    assert sz_delist_path == tmp_path / "data" / "parquet" / "akshare_cn_stock_delist_sz" / "snapshot_date=2024-01-03" / "data.parquet"
    assert store.read_latest_akshare_cn_stock_delist_sz().loc[0, "code"] == "000001"

    spot_path = store.write_stock_spot_quote_eastmoney(
        "2024-01-03",
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
    )
    assert spot_path.exists()
    assert store.read_latest_stock_spot_quote_eastmoney().loc[0, "last_price"] == 8.3

    sina_path = store.write_stock_spot_quote_sina(
        "2024-01-03",
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
    )
    assert sina_path.exists()
    assert bool(store.read_stock_spot_quote_sina("2024-01-03").loc[0, "is_fallback"])

    spot_hist = _akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", close=8.3)
    daily_bar_confirmed = _akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", close=8.31)
    store.write_akshare_daily_bars("unadjusted", "600000", pd.DataFrame([spot_hist]))
    store.upsert_akshare_daily_bars("unadjusted", "600000", pd.DataFrame([daily_bar_confirmed]))
    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert len(hist) == 1
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "daily_bar_confirmed"


def test_append_akshare_daily_bar_batch_appends_new_date_without_full_upsert(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", 8.2, daily_bar_date="2024-01-02")]),
    )

    def fail_full_upsert(*args, **kwargs):
        raise AssertionError("append-only path should not call full upsert")

    monkeypatch.setattr(store, "upsert_akshare_daily_bars", fail_full_upsert)

    stats = store.append_akshare_daily_bar_batch(
        "unadjusted",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", 8.3, daily_bar_date="2024-01-03")]),
    )

    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert stats == {"updated": 1, "skipped": 0, "fallback": 0}
    assert pd.to_datetime(hist["date"]).dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert hist["close"].tolist() == [8.2, 8.3]


def test_append_akshare_daily_bar_batch_skips_existing_single_date_without_rewrite(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", 8.3, daily_bar_date="2024-01-03")]),
    )
    path = store.akshare_daily_bar_path("unadjusted", "600000")
    before_mtime = path.stat().st_mtime_ns

    def fail_full_upsert(*args, **kwargs):
        raise AssertionError("existing spot date should skip without full upsert")

    monkeypatch.setattr(store, "upsert_akshare_daily_bars", fail_full_upsert)

    stats = store.append_akshare_daily_bar_batch(
        "unadjusted",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", 8.3, daily_bar_date="2024-01-03")]),
    )

    assert stats == {"updated": 0, "skipped": 1, "fallback": 0}
    assert path.stat().st_mtime_ns == before_mtime


def test_append_akshare_daily_bar_batch_falls_back_for_overlapping_date(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", 8.3, daily_bar_date="2024-01-03")]),
    )

    stats = store.append_akshare_daily_bar_batch(
        "unadjusted",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", 8.31, daily_bar_date="2024-01-03")]),
        skip_existing=False,
    )

    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert stats == {"updated": 1, "skipped": 0, "fallback": 1}
    assert len(hist) == 1
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "quality_status"] == "daily_bar_confirmed"


def test_append_akshare_daily_bar_batch_falls_back_for_date_before_existing_max(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame(
            [
                _akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", 8.1, daily_bar_date="2024-01-01"),
                _akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", 8.3, daily_bar_date="2024-01-03"),
            ]
        ),
    )

    stats = store.append_akshare_daily_bar_batch(
        "unadjusted",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", 8.2, daily_bar_date="2024-01-02")]),
        skip_existing=True,
    )

    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert stats == {"updated": 1, "skipped": 0, "fallback": 1}
    assert pd.to_datetime(hist["date"]).dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_append_akshare_daily_bar_batch_rejects_non_six_digit_code(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    row = _akshare_hist_row("stock_zh_a_spot_em", "spot_quote_close", 8.3)
    row["code"] = "sh.600000"

    with pytest.raises(ValueError, match="AkShare partition code must be 6 digits"):
        store.append_akshare_daily_bar_batch("unadjusted", pd.DataFrame([row]))


def test_atomic_write_allows_parallel_writes_to_different_paths(tmp_path, monkeypatch, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    original_write_table = parquet_store_module.pq.write_table
    active = 0
    max_active = 0
    active_lock = threading.Lock()

    def slow_write_table(*args, **kwargs):
        nonlocal active, max_active
        with active_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            return original_write_table(*args, **kwargs)
        finally:
            with active_lock:
                active -= 1

    monkeypatch.setattr(parquet_store_module.pq, "write_table", slow_write_table)
    first = daily_sample()
    second = daily_sample().assign(code="sh.600001")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(store.write_baostock_daily_bars, "baostock_cn_stock_daily_bar_unadjusted", "sh.600000", first),
            executor.submit(store.write_baostock_daily_bars, "baostock_cn_stock_daily_bar_unadjusted", "sh.600001", second),
        ]
        for future in futures:
            future.result()

    assert max_active == 2


def test_concurrent_akshare_upserts_to_same_code_keep_all_dates(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame([_akshare_hist_row("stock_zh_a_hist", "daily_bar_confirmed", 8.1, daily_bar_date="2024-01-01")]),
    )
    path = store.akshare_daily_bar_path("unadjusted", "600000")
    original_date_bounds = store._parquet_date_bounds
    first_reader_waiting = threading.Event()
    release_first_reader = threading.Event()

    def delayed_date_bounds(target_path):
        if target_path == path and not first_reader_waiting.is_set():
            first_reader_waiting.set()
            release_first_reader.wait(timeout=1)
        return original_date_bounds(target_path)

    monkeypatch.setattr(store, "_parquet_date_bounds", delayed_date_bounds)

    def append_date(daily_bar_date: str, close: float) -> None:
        store.upsert_akshare_daily_bars(
            "unadjusted",
            "600000",
            pd.DataFrame(
                [
                    _akshare_hist_row(
                        "stock_zh_a_hist",
                        "daily_bar_confirmed",
                        close,
                        daily_bar_date=daily_bar_date,
                    )
                ]
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(append_date, "2024-01-02", 8.2)
        assert first_reader_waiting.wait(timeout=1)
        second = executor.submit(append_date, "2024-01-03", 8.3)
        time.sleep(0.05)
        release_first_reader.set()
        first.result()
        second.result()

    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert pd.to_datetime(hist["date"]).dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_writes_reject_missing_partition_keys(
    tmp_path,
    daily_sample,
    baostock_cn_stock_adjustment_factor_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    with pytest.raises(ValueError, match="Daily file code missing code"):
        store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample().drop(columns=["code"]))
    with pytest.raises(ValueError, match="Adjust factor file code missing code"):
        store.write_baostock_cn_stock_adjustment_factor("sh.600000", baostock_cn_stock_adjustment_factor_sample().drop(columns=["code"]))
    with pytest.raises(ValueError, match="Stock value file code missing code"):
        store.write_akshare_cn_stock_valuation_eastmoney("600000", akshare_cn_stock_valuation_eastmoney_sample().drop(columns=["code"]))


def test_writes_reject_partition_key_mismatch(tmp_path, akshare_cn_stock_valuation_eastmoney_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    with pytest.raises(ValueError, match="Stock value file code mismatch"):
        store.write_akshare_cn_stock_valuation_eastmoney("600000", akshare_cn_stock_valuation_eastmoney_sample("000001"))


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

    path = store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())

    assert logs == [
        (
            "Daily Parquet stored dataset={} code={} rows={} path={}",
            ("baostock_cn_stock_daily_bar_qfq", "sh.600000", 2, path),
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
    output_path = store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())

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
        output_path = store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())
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
    output_path = store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_qfq", "sh.600000", daily_sample())
    missing_path = store.baostock_daily_bar_path("baostock_cn_stock_daily_bar_qfq", "sz.000001")

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


def test_persist_update_metadata_keeps_latest_status_and_checkpoint_with_duplicate_keys(tmp_path) -> None:
    now = datetime(2024, 1, 31, 9, 1)
    later = datetime(2024, 1, 31, 9, 2)
    run_rows = [
        {
            "task_id": "task-1",
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "status": "failed",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "start_time": now,
            "end_time": now,
            "row_count": 1,
            "error_stack": "first",
        },
        {
            "task_id": "task-2",
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "status": "success",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "start_time": later,
            "end_time": later,
            "row_count": 2,
            "error_stack": "",
        },
    ]
    status_rows = [
        {
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "last_success_date": "2024-01-30",
            "row_count": 1,
            "status": "failed",
            "updated_at": now,
            "error_stack": "first",
        },
        {
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "last_success_date": "2024-01-31",
            "row_count": 2,
            "status": "success",
            "updated_at": later,
            "error_stack": "",
        },
    ]
    checkpoint_rows = [
        {
            "pipeline": "update_daily",
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "status": "failed",
            "row_count": 1,
            "output_path": "old.parquet",
            "updated_at": now,
            "error_stack": "first",
        },
        {
            "pipeline": "update_daily",
            "dataset": "baostock_cn_stock_daily_bar_qfq",
            "code": "sh.600000",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "status": "success",
            "row_count": 2,
            "output_path": "new.parquet",
            "updated_at": later,
            "error_stack": "",
        },
    ]

    store = ParquetStore(root=tmp_path)
    store.persist_update_metadata(run_rows, status_rows, checkpoint_rows)

    runs = store.read_pipeline_runs()
    statuses = store.read_dataset_update_status()
    checkpoints = store.read_pipeline_checkpoints()
    assert len(runs) == 2
    assert len(statuses) == 1
    assert statuses.loc[0, "row_count"] == 2
    assert str(statuses.loc[0, "status"]) == "success"
    assert pd.to_datetime(statuses.loc[0, "last_success_date"]).date().isoformat() == "2024-01-31"
    assert len(checkpoints) == 1
    assert checkpoints.loc[0, "row_count"] == 2
    assert str(checkpoints.loc[0, "output_path"]) == "new.parquet"


def test_duckdb_metadata_store_does_not_reuse_connection_across_threads(tmp_path, monkeypatch) -> None:
    created_connections = []

    class ThreadBoundConnection:
        def __init__(self) -> None:
            import threading

            self.thread_id = threading.get_ident()
            self.closed = False
            created_connections.append(self)

        def _assert_current_thread(self) -> None:
            import threading

            if threading.get_ident() != self.thread_id:
                raise RuntimeError("connection used from a different thread")

        def execute(self, sql: str):
            del sql
            self._assert_current_thread()
            return self

        def register(self, name: str, df: pd.DataFrame) -> None:
            del name, df
            self._assert_current_thread()

        def unregister(self, name: str) -> None:
            del name
            self._assert_current_thread()

        def close(self) -> None:
            self._assert_current_thread()
            self.closed = True

    monkeypatch.setattr(metadata_store_module.duckdb, "connect", lambda path: ThreadBoundConnection())

    store = metadata_store_module.DuckDBMetadataStore(root=tmp_path)
    now = datetime(2024, 1, 31, 9, 1)

    def persist(index: int) -> None:
        store.persist_update_metadata(
            [
                {
                    "task_id": f"task-{index}",
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
                    "code": f"sh.{600000 + index}",
                    "status": "success",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "start_time": now,
                    "end_time": now,
                    "row_count": 2,
                    "error_stack": "",
                }
            ],
            [],
            [],
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(persist, 0).result()
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(persist, 1).result()

    assert len(created_connections) == 2
    assert all(conn.closed for conn in created_connections)


def test_persist_update_metadata_rolls_back_run_and_status_when_checkpoint_write_fails(tmp_path) -> None:
    duckdb_file = tmp_path / "data" / "duckdb" / "quant.duckdb"
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_file)) as conn:
        conn.execute(
            """
            CREATE TABLE pipeline_runs (
                task_id VARCHAR,
                dataset VARCHAR,
                code VARCHAR,
                status VARCHAR,
                start_date DATE,
                end_date DATE,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                row_count BIGINT,
                error_stack VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE dataset_update_status (
                dataset VARCHAR,
                code VARCHAR,
                last_success_date DATE,
                row_count BIGINT,
                status VARCHAR,
                updated_at TIMESTAMP,
                error_stack VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO dataset_update_status VALUES (
                'baostock_cn_stock_daily_bar_qfq',
                'sh.600000',
                DATE '2024-01-30',
                1,
                'success',
                TIMESTAMP '2024-01-30 09:01:00',
                ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE pipeline_checkpoints (
                pipeline VARCHAR,
                dataset VARCHAR,
                code VARCHAR,
                start_date DATE,
                end_date DATE,
                status VARCHAR,
                row_count BIGINT,
                output_path VARCHAR,
                updated_at TIMESTAMP,
                error_stack VARCHAR,
                required_marker VARCHAR NOT NULL
            )
            """
        )

    store = ParquetStore(root=tmp_path)
    now = datetime(2024, 1, 31, 9, 1)

    with pytest.raises(duckdb.BinderException):
        store.persist_update_metadata(
            [
                {
                    "task_id": "task-1",
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
                    "code": "sh.600000",
                    "status": "success",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "start_time": now,
                    "end_time": now,
                    "row_count": 2,
                    "error_stack": "",
                }
            ],
            [
                {
                    "dataset": "baostock_cn_stock_daily_bar_qfq",
                    "code": "sh.600000",
                    "last_success_date": "2024-01-31",
                    "row_count": 2,
                    "status": "success",
                    "updated_at": now,
                    "error_stack": "",
                }
            ],
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
                    "updated_at": now,
                    "error_stack": "",
                }
            ],
        )

    with duckdb.connect(str(duckdb_file)) as conn:
        run_count = conn.execute("SELECT count(*) FROM pipeline_runs").fetchone()
        status_row = conn.execute(
            "SELECT last_success_date, row_count FROM dataset_update_status WHERE code = 'sh.600000'"
        ).fetchone()

    assert run_count == (0,)
    assert status_row == (datetime(2024, 1, 30).date(), 1)


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


def _akshare_hist_row(
    source_endpoint: str,
    quality_status: str,
    close: float,
    *,
    daily_bar_date: str = "2024-01-03",
    code: str = "600000",
) -> dict[str, object]:
    return {
        "date": daily_bar_date,
        "code": code,
        "source_symbol": code,
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


