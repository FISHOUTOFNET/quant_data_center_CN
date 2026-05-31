from __future__ import annotations

import threading
import time
from pathlib import Path

import pandas as pd
import pytest
from update_daily_fakes import _fake_provider_factory, _write_settings

import src.pipeline.update_daily as update_daily_module
import src.pipeline.update_daily_worker as update_daily_worker_module
from src.pipeline.common import FULL_HISTORY_START_DATE, PIPELINE_UPDATE_DAILY, write_checkpoint
from src.pipeline.lifecycle import PipelineMetadataBatch
from src.storage.parquet_store import ParquetStore

pytestmark = [pytest.mark.performance, pytest.mark.slow]


def test_api_serial_delay_dominates_wall_time(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_perf_settings(tmp_path, background_workers=4, background_max_pending=16)
    codes = ("sh.600000", "sz.000001", "sh.000001")
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_trading_calendar", _calendar_frame("2023-10-05", "2024-01-03"))
    for code in codes:
        store.write_dataset(
            "baostock_cn_stock_daily_bar_unadjusted",
            daily_sample().assign(code=code, adjust_flag="3"),
            {"code": code},
        )
    provider_factory, state = _fake_provider_factory(
        baostock_cn_stock_basic_sample(),
        daily_sample(),
        api_delays={"daily": 0.005},
    )
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    started = time.perf_counter()
    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_unadjusted",
        code=codes,
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    elapsed = time.perf_counter() - started

    assert state["history_calls"] == list(codes)
    assert elapsed >= 0.005 * len(codes) * 0.8
    store.close()


def test_background_write_delay_blocks_when_pending_queue_full(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    codes = tuple(f"sh.60000{index}" for index in range(3))
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)
    original_write = ParquetStore.write_dataset
    active_case = {"label": ""}
    active_writes = 0
    max_active_writes = {"serial": 0, "parallel": 0}
    lock = threading.Lock()

    def slow_write(self, dataset_id: str, df: pd.DataFrame, partition=None, mode=None, skip_existing=False) -> Path:
        nonlocal active_writes
        with lock:
            label = active_case["label"]
            if label:
                active_writes += 1
                max_active_writes[label] = max(max_active_writes[label], active_writes)
        time.sleep(0.005)
        try:
            return original_write(self, dataset_id, df, partition, mode, skip_existing)
        finally:
            with lock:
                if label:
                    active_writes -= 1

    monkeypatch.setattr(ParquetStore, "write_dataset", slow_write)

    active_case["label"] = "serial"
    _run_unadjusted_batch(
        tmp_path / "serial",
        codes,
        background_workers=1,
        background_max_pending=1,
    )
    active_case["label"] = "parallel"
    _run_unadjusted_batch(
        tmp_path / "parallel",
        codes,
        background_workers=4,
        background_max_pending=16,
    )
    active_case["label"] = ""

    assert max_active_writes["serial"] == 1
    assert max_active_writes["parallel"] > 1


def test_metadata_flush_delay_scales_with_flush_count(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    codes = ("sh.600000", "sh.600001")
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)
    original_flush = PipelineMetadataBatch.flush
    flush_counts: dict[str, int] = {"small": 0, "large": 0}
    active_case = {"label": ""}
    lock = threading.Lock()

    def slow_counted_flush(self) -> None:
        with lock:
            label = active_case["label"]
        if label:
            flush_counts[label] += 1
        time.sleep(0.002)
        original_flush(self)

    monkeypatch.setattr(PipelineMetadataBatch, "flush", slow_counted_flush)

    active_case["label"] = "small"
    _run_unadjusted_batch(tmp_path / "small", codes, metadata_flush_size=1, background_workers=1)
    active_case["label"] = "large"
    _run_unadjusted_batch(tmp_path / "large", codes, metadata_flush_size=100, background_workers=1)

    assert flush_counts["small"] > flush_counts["large"]
    assert flush_counts["small"] >= len(codes)
    assert flush_counts["large"] == 1


def test_partial_all_does_not_full_refetch_checkpointed_codes(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
    baostock_cn_stock_adjustment_factor_sample,
) -> None:
    _write_settings(tmp_path)
    code = "sh.600000"
    start_date = "2024-01-02"
    end_date = "2024-01-03"
    store = _store_with_complete_all_checkpoints(
        tmp_path,
        code,
        start_date,
        end_date,
        daily_sample().assign(code=code),
        baostock_cn_stock_basic_sample(),
        baostock_cn_stock_adjustment_factor_sample().assign(code=code),
    )
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="all",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert state["calendar_params"] == []
    assert state["baostock_cn_stock_basic_calls"] == 0
    assert state["baostock_cn_stock_adjustment_factor_calls"] == []
    assert state["history_calls"] == []
    assert {item["status"] for item in records} == {"skipped_checkpoint"}
    store.close()


def test_missing_lookback_triggers_full_refetch_count(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    code = "sh.600000"
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        code=code,
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert [item["start_date"] for item in state["history_params"]] == ["2024-01-02", FULL_HISTORY_START_DATE]


def test_adjust_factor_change_recomputes_adjusted_without_extra_daily_api(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    code = "sh.600000"
    start_date = "2024-01-02"
    end_date = "2024-01-03"
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    old_factors = _factor_frame(code, 1.0, 1.0)
    new_factors = _factor_frame(code, 2.0, 3.0)
    store.write_dataset("baostock_cn_stock_adjustment_factor", old_factors, {"code": code})
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted", daily_sample().assign(code=code, adjust_flag="3"), {"code": code}
    )
    qfq_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample().assign(code=code, adjust_flag="1"), {"code": code}
    ).primary_path
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_daily_bar_qfq",
        code,
        start_date,
        end_date,
        "success",
        len(daily_sample()),
        qfq_path,
    )
    provider_factory, state = _fake_provider_factory(
        baostock_cn_stock_basic_sample(),
        daily_sample(),
        baostock_cn_stock_adjustment_factor_df=new_factors,
    )
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)
    original_calculate = update_daily_worker_module.calculate_adjusted_daily_bar
    calculations: list[str] = []

    def counted_calculate(unadjusted, factors, dataset, adjust_flag):
        calculations.append(dataset)
        return original_calculate(unadjusted, factors, dataset, adjust_flag)

    monkeypatch.setattr(update_daily_worker_module, "calculate_adjusted_daily_bar", counted_calculate)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert [item["start_date"] for item in state["history_params"]] == [start_date]
    assert calculations == ["baostock_cn_stock_daily_bar_qfq"]


def _run_unadjusted_batch(
    root: Path,
    codes: tuple[str, ...],
    *,
    metadata_flush_size: int = 200,
    background_workers: int,
    background_max_pending: int | None = None,
) -> float:
    _write_perf_settings(
        root,
        metadata_flush_size=metadata_flush_size,
        background_workers=background_workers,
        background_max_pending=background_max_pending,
    )
    started = time.perf_counter()
    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_unadjusted",
        code=codes,
        end="2024-01-03",
        lookback_days=1,
        root=root,
        build_views=False,
    )
    return time.perf_counter() - started


def _write_perf_settings(
    root: Path,
    *,
    metadata_flush_size: int = 200,
    background_workers: int = 4,
    background_max_pending: int | None = None,
) -> None:
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    pipeline_lines = [
        "pipeline:",
        "  lookback_days: 1",
        "  max_retries: 1",
        f"  metadata_flush_size: {metadata_flush_size}",
        f"  background_workers: {background_workers}",
    ]
    if background_max_pending is not None:
        pipeline_lines.append(f"  background_max_pending: {background_max_pending}")
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "api:",
                "  baostock:",
                "    adjust_flag_map:",
                '      unadjusted: "3"',
                '      qfq: "1"',
                '      hfq: "2"',
                "datasets:",
                "  daily_bar:",
                '    fields: "date,code,open,high,low,close,prev_close,volume,amount,adjust_flag,turn,trade_status,pct_change,pe_ttm,pb_mrq,ps_ttm,pcf_ncf_ttm,is_st"',
                "    frequency: d",
                *pipeline_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )


def _store_with_complete_all_checkpoints(
    root: Path,
    code: str,
    start_date: str,
    end_date: str,
    daily: pd.DataFrame,
    basic: pd.DataFrame,
    factors: pd.DataFrame,
) -> ParquetStore:
    store = ParquetStore(root=root)
    store.ensure_layout()
    calendar_path = store.write_dataset(
        "baostock_cn_trading_calendar", _calendar_frame("2023-10-05", end_date)
    ).primary_path
    basic_path = store.write_dataset("baostock_cn_stock_basic", basic).primary_path
    factor_path = store.write_dataset("baostock_cn_stock_adjustment_factor", factors, {"code": code}).primary_path
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_trading_calendar",
        "*",
        start_date,
        end_date,
        "success",
        3,
        calendar_path,
    )
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_basic",
        "*",
        start_date,
        end_date,
        "success",
        len(basic),
        basic_path,
    )
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_adjustment_factor",
        code,
        FULL_HISTORY_START_DATE,
        end_date,
        "success",
        len(factors),
        factor_path,
    )
    for dataset, adjust_flag in [
        ("baostock_cn_stock_daily_bar_unadjusted", "3"),
        ("baostock_cn_stock_daily_bar_qfq", "1"),
        ("baostock_cn_stock_daily_bar_hfq", "2"),
    ]:
        path = store.write_dataset(dataset, daily.assign(adjust_flag=adjust_flag), {"code": code}).primary_path
        write_checkpoint(store, PIPELINE_UPDATE_DAILY, dataset, code, start_date, end_date, "success", len(daily), path)
    return store


def _factor_frame(code: str, forward: float, backward: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": code,
                "dividend_operate_date": "2024-01-02",
                "forward_adjust_factor": forward,
                "backward_adjust_factor": backward,
                "adjustment_factor": forward,
            }
        ]
    )


def _calendar_frame(start_date: str, end_date: str) -> pd.DataFrame:
    dates = pd.date_range(start_date, end_date, freq="D")
    return pd.DataFrame(
        [
            {
                "calendar_date": item.date(),
                "is_trading_day": "1" if item.weekday() < 5 else "0",
            }
            for item in dates
        ]
    )
