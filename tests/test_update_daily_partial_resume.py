from __future__ import annotations

import pandas as pd

import src.pipeline.update_daily as update_daily_module
from src.pipeline.adjustments import ADJUST_FACTOR_DATASET
from src.pipeline.common import PIPELINE_UPDATE_DAILY, write_checkpoint
from src.storage.parquet_store import ParquetStore
from update_daily_fakes import _fake_provider_factory, _write_settings


def test_update_daily_uses_active_stock_basic_codes_and_resumes(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
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

    assert first_history_calls == ["sh.600000", "sh.600000"]
    assert "sz.000001" not in first_history_calls
    assert state["history_calls"] == first_history_calls
    assert state["stock_basic_calls"] == 1


def test_update_daily_checkpoint_lookup_reads_checkpoints_once_per_run(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
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
    stock_basic_sample,
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
                "dividOperateDate": "2024-01-02",
                "foreAdjustFactor": 1.0,
                "backAdjustFactor": 1.0,
                "adjustFactor": 1.0,
            }
        ]
    )
    factor_path = store.write_adjust_factor(code, factors)
    daily_path = store.write_daily_k("daily_k_qfq", code, daily_sample().assign(code=code, adjustflag="2"))
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        ADJUST_FACTOR_DATASET,
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
        "daily_k_qfq",
        code,
        checkpoint_start,
        end_date,
        "success",
        len(daily_sample()),
        daily_path,
    )

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="daily_k_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert records == []
    assert state["adjust_factor_calls"] == []
    assert state["history_calls"] == []


def test_update_daily_all_keeps_code_when_any_requested_daily_checkpoint_is_missing(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
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
                "dividOperateDate": "2024-01-02",
                "foreAdjustFactor": 1.0,
                "backAdjustFactor": 1.0,
                "adjustFactor": 1.0,
            }
        ]
    )
    factor_path = store.write_adjust_factor(code, factors)
    qfq_path = store.write_daily_k("daily_k_qfq", code, daily_sample().assign(code=code, adjustflag="2"))
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        ADJUST_FACTOR_DATASET,
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
        "daily_k_qfq",
        code,
        checkpoint_start,
        end_date,
        "success",
        len(daily_sample()),
        qfq_path,
    )

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="daily_k_all",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    daily_datasets = {item["dataset"] for item in records if item["dataset"].startswith("daily_k_")}
    assert daily_datasets == {"daily_k_none", "daily_k_qfq", "daily_k_hfq"}
    assert state["adjust_factor_calls"] == [code]
    assert state["history_calls"]


def test_update_daily_adjusted_daily_keeps_code_when_factor_checkpoint_is_missing(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    end_date = "2024-01-03"
    checkpoint_start = "2024-01-02"
    daily_path = store.write_daily_k("daily_k_qfq", code, daily_sample().assign(code=code, adjustflag="2"))
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "daily_k_qfq",
        code,
        checkpoint_start,
        end_date,
        "success",
        len(daily_sample()),
        daily_path,
    )

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="daily_k_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    qfq_records = [item for item in records if item["dataset"] == "daily_k_qfq"]
    assert [item["status"] for item in qfq_records] == ["success"]
    assert state["adjust_factor_calls"] == [code]
    assert state["history_calls"]


def test_update_daily_resolves_weekend_end_to_previous_trading_day(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
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

    assert [item["start_date"] for item in first_history_params] == ["2024-01-04", "1990-01-01"]
    assert {item["end_date"] for item in first_history_params} == {"2024-01-05"}
    assert state["history_params"] == first_history_params
    assert state["stock_basic_calls"] == 1

    store = ParquetStore(root=tmp_path)
    assert store.stock_basic_path().exists()
    checkpoints = store.read_pipeline_checkpoints()
    end_dates = pd.to_datetime(checkpoints["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    assert set(end_dates.dropna()) == {"2024-01-05"}
