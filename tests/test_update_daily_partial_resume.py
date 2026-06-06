from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest
from update_daily_fakes import _fake_provider_factory, _write_settings

import src.sources.baostock.update_daily as update_daily_module
from src.pipeline.common import PIPELINE_UPDATE_DAILY, write_checkpoint
from src.sources.baostock.adjustments import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET
from src.storage.parquet_store import ParquetStore

pytestmark = pytest.mark.slow


def test_update_daily_uses_active_baostock_cn_stock_basic_codes_and_resumes(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    update_daily_module.update_daily(
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert Counter(first_history_calls) == Counter({"sh.000001": 2, "sh.600000": 2})
    assert "sz.000001" not in first_history_calls
    assert state["history_calls"] == first_history_calls
    assert state["baostock_cn_stock_adjustment_factor_calls"] == []
    assert state["baostock_cn_stock_basic_calls"] == 1


def test_update_daily_checkpoint_lookup_reads_checkpoints_once_per_run(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    read_calls = {"count": 0}
    original_read_pipeline_checkpoints = ParquetStore.read_pipeline_checkpoints

    def counted_read_pipeline_checkpoints(self):
        read_calls["count"] += 1
        return original_read_pipeline_checkpoints(self)

    monkeypatch.setattr(ParquetStore, "read_pipeline_checkpoints", counted_read_pipeline_checkpoints)

    update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert read_calls["count"] == 1
    assert state["history_calls"] == first_history_calls


def test_update_daily_prefilter_skips_code_when_requested_targets_are_checkpointed(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    end_date = "2024-01-03"
    checkpoint_start = "2024-01-02"
    factors = pd.DataFrame(
        [
            {
                "code": code,
                "dividend_operate_date": "2024-01-02",
                "forward_adjust_factor": 1.0,
                "backward_adjust_factor": 1.0,
                "adjustment_factor": 1.0,
            }
        ]
    )
    factor_path = store.write_dataset("baostock_cn_stock_adjustment_factor", factors, {"code": code}).primary_path
    daily_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample().assign(code=code, adjust_flag="1"), {"code": code}
    ).primary_path
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        code,
        "1990-01-01",
        end_date,
        "success",
        len(factors),
        factor_path,
    )
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_daily_bar_qfq",
        code,
        checkpoint_start,
        end_date,
        "success",
        len(daily_sample()),
        daily_path,
    )

    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert {item["status"] for item in records} == {"skipped_checkpoint"}
    assert {item["dataset"] for item in records} == {"baostock_cn_stock_daily_bar_qfq"}
    assert state["baostock_cn_stock_adjustment_factor_calls"] == []
    assert state["history_calls"] == []


def test_update_daily_all_keeps_code_when_any_requested_daily_checkpoint_is_missing(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    end_date = "2024-01-03"
    checkpoint_start = "2024-01-02"
    factors = pd.DataFrame(
        [
            {
                "code": code,
                "dividend_operate_date": "2024-01-02",
                "forward_adjust_factor": 1.0,
                "backward_adjust_factor": 1.0,
                "adjustment_factor": 1.0,
            }
        ]
    )
    factor_path = store.write_dataset("baostock_cn_stock_adjustment_factor", factors, {"code": code}).primary_path
    qfq_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample().assign(code=code, adjust_flag="1"), {"code": code}
    ).primary_path
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        code,
        "1990-01-01",
        end_date,
        "success",
        len(factors),
        factor_path,
    )
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_daily_bar_qfq",
        code,
        checkpoint_start,
        end_date,
        "success",
        len(daily_sample()),
        qfq_path,
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

    daily_datasets = {item["dataset"] for item in records if item["dataset"].startswith("baostock_cn_stock_daily_bar_")}
    assert daily_datasets == {
        "baostock_cn_stock_daily_bar_unadjusted",
        "baostock_cn_stock_daily_bar_qfq",
        "baostock_cn_stock_daily_bar_hfq",
    }
    assert state["baostock_cn_stock_adjustment_factor_calls"] == [code]
    assert state["history_calls"]


def test_update_daily_adjusted_daily_bareeps_code_when_factor_checkpoint_is_missing(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    end_date = "2024-01-03"
    checkpoint_start = "2024-01-02"
    daily_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample().assign(code=code, adjust_flag="1"), {"code": code}
    ).primary_path
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_daily_bar_qfq",
        code,
        checkpoint_start,
        end_date,
        "success",
        len(daily_sample()),
        daily_path,
    )

    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    qfq_records = [item for item in records if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"]
    assert [item["status"] for item in qfq_records] == ["success"]
    assert state["baostock_cn_stock_adjustment_factor_calls"] == [code]
    assert state["history_calls"]


def test_update_daily_adjusted_daily_bar_reuses_checkpointed_local_factor(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    end_date = "2024-01-03"
    factors = pd.DataFrame(
        [
            {
                "code": code,
                "dividend_operate_date": "2024-01-02",
                "forward_adjust_factor": 2.0,
                "backward_adjust_factor": 3.0,
                "adjustment_factor": 2.0,
            }
        ]
    )
    factor_path = store.write_dataset("baostock_cn_stock_adjustment_factor", factors, {"code": code}).primary_path
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted", daily_sample().assign(code=code, adjust_flag="3"), {"code": code}
    )
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        code,
        "1990-01-01",
        end_date,
        "success",
        len(factors),
        factor_path,
    )

    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    qfq_records = [item for item in records if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"]
    assert [item["status"] for item in qfq_records] == ["success"]
    assert state["baostock_cn_stock_adjustment_factor_calls"] == []
    assert store.read_dataset("baostock_cn_stock_daily_bar_qfq", {"code": code}).loc[0, "close"] == 16.4


def test_update_daily_resolves_weekend_end_to_previous_trading_day(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        end="2024-01-06",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    first_history_params = list(state["history_params"])

    update_daily_module.update_daily(
        end="2024-01-06",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    history_windows = Counter((item["code"], item["start_date"], item["end_date"]) for item in first_history_params)
    assert history_windows == Counter(
        {
            ("sh.000001", "2024-01-04", "2024-01-05"): 1,
            ("sh.000001", "1990-01-01", "2024-01-05"): 1,
            ("sh.600000", "2024-01-04", "2024-01-05"): 1,
            ("sh.600000", "1990-01-01", "2024-01-05"): 1,
        }
    )
    assert {item["end_date"] for item in first_history_params} == {"2024-01-05"}
    assert state["history_params"] == first_history_params
    assert state["baostock_cn_stock_basic_calls"] == 1

    store = ParquetStore(root=tmp_path)
    assert store.dataset_exists("baostock_cn_stock_basic")
    checkpoints = store.read_pipeline_checkpoints()
    end_dates = pd.to_datetime(checkpoints["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    assert set(end_dates.dropna()) == {"2024-01-05"}


def test_update_daily_adjustment_factor_only_fetches_stock_type_codes(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_adjustment_factor",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert "sh.600000" in state["baostock_cn_stock_adjustment_factor_calls"]
    assert "sh.000001" not in state["baostock_cn_stock_adjustment_factor_calls"]


def test_update_daily_qfq_only_fetches_stock_type_adjustment_factors(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert "sh.600000" in state["baostock_cn_stock_adjustment_factor_calls"]
    assert "sh.000001" not in state["baostock_cn_stock_adjustment_factor_calls"]
