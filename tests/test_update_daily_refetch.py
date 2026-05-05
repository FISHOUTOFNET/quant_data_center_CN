from __future__ import annotations

import threading

import pandas as pd

import src.pipeline.update_daily as update_daily_module
from src.pipeline.common import PIPELINE_UPDATE_DAILY, write_checkpoint
from src.storage.parquet_store import ParquetStore
from update_daily_fakes import _fake_provider_factory, _provider_factory_for, _write_settings


def test_update_daily_refetches_full_history_on_lookback_mismatch(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    adjustflags = {"daily_k_none": "3", "daily_k_qfq": "1", "daily_k_hfq": "2"}
    for dataset, adjustflag in adjustflags.items():
        existing = daily_sample().assign(code="sh.600000", adjustflag=adjustflag)
        if dataset == "daily_k_none":
            existing.loc[0, "close"] = 99.0
        store.write_daily_k(dataset, "sh.600000", existing)

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    history_starts = [item["start_date"] for item in state["history_params"]]
    assert history_starts == [
        "2024-01-02",
        "1990-01-01",
    ]
    assert {item["adjustflag"] for item in state["history_params"]} == {"3"}
    daily_records = [item for item in records if item["dataset"].startswith("daily_k_")]
    assert [item["start_date"] for item in daily_records] == ["1990-01-01", "1990-01-01", "1990-01-01"]
    assert store.read_daily_k("daily_k_qfq", "sh.600000").loc[0, "close"] == 8.2


def test_update_daily_refetches_full_history_when_lookback_is_empty(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    empty_daily = daily_sample().iloc[0:0]

    def daily_by_start(**kwargs) -> pd.DataFrame:
        if kwargs["start_date"] == "1990-01-01":
            return daily_sample()
        return empty_daily

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_by_start)
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert [item["start_date"] for item in state["history_params"]] == ["2024-01-02", "1990-01-01"]
    assert {item["adjustflag"] for item in state["history_params"]} == {"3"}
    daily_records = [item for item in records if item["dataset"].startswith("daily_k_")]
    assert [item["row_count"] for item in daily_records] == [2, 2, 2]


def test_update_daily_does_not_refetch_when_overlap_values_match_with_different_dtypes(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    existing = daily_sample().assign(code=code, adjustflag="3")
    store.write_daily_k("daily_k_none", code, existing)

    target_day = existing.iloc[[1]].copy()
    target_day.loc[:, "date"] = "2024-01-04"
    target_day.loc[:, "open"] = 8.3
    target_day.loc[:, "high"] = 8.5
    target_day.loc[:, "low"] = 8.2
    target_day.loc[:, "close"] = 8.4
    target_day.loc[:, "preclose"] = 8.3
    target_day.loc[:, "volume"] = 1300
    target_day.loc[:, "amount"] = 10920.0
    target_day.loc[:, "pctChg"] = 1.2048
    provider_daily = pd.concat([existing, target_day], ignore_index=True).astype(str)

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), provider_daily)
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="daily_k_none",
        code=code,
        end="2024-01-04",
        lookback_days=2,
        root=tmp_path,
        build_views=False,
    )

    assert state["history_params"] == [
        {
            "code": code,
            "start_date": "2024-01-02",
            "end_date": "2024-01-04",
            "adjustflag": "3",
        },
    ]
    assert [item["start_date"] for item in records if item["dataset"] == "daily_k_none"] == ["2024-01-02"]
    stored = store.read_daily_k("daily_k_none", code)
    assert pd.to_datetime(stored["date"], errors="coerce").dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]


def test_update_daily_adjust_factor_change_recomputes_adjusted_without_full_refetch(
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

    old_factors = pd.DataFrame(
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
    new_factors = old_factors.assign(foreAdjustFactor=2.0, backAdjustFactor=3.0, adjustFactor=2.0)
    store.write_adjust_factor(code, old_factors)
    store.write_daily_k("daily_k_none", code, daily_sample().assign(code=code, adjustflag="3"))
    daily_path = store.write_daily_k("daily_k_qfq", code, daily_sample().assign(code=code, adjustflag="1"))
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

    provider_factory, state = _fake_provider_factory(
        stock_basic_sample(),
        daily_sample(),
        adjust_factor_df=new_factors,
    )
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
    assert state["history_params"] == [
        {
            "code": code,
            "start_date": checkpoint_start,
            "end_date": end_date,
            "adjustflag": "3",
        },
    ]
    assert store.read_daily_k("daily_k_qfq", code).loc[0, "close"] == 16.4


def test_update_daily_adjusted_mismatch_recomputes_without_full_refetch(
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

    store.write_daily_k("daily_k_none", code, daily_sample().assign(code=code, adjustflag="3"))
    stale_qfq = daily_sample().assign(code=code, adjustflag="1")
    stale_qfq.loc[0, "close"] = 99.0
    store.write_daily_k("daily_k_qfq", code, stale_qfq)

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
    assert state["history_params"] == [
        {
            "code": code,
            "start_date": checkpoint_start,
            "end_date": end_date,
            "adjustflag": "3",
        },
    ]
    assert store.read_daily_k("daily_k_qfq", code).loc[0, "close"] == 8.2


def test_update_daily_adjusted_recompute_uses_full_local_unadjusted_history(
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

    older_row = daily_sample().iloc[[0]].assign(date="2024-01-01", code=code, adjustflag="3")
    unadjusted_history = pd.concat(
        [older_row, daily_sample().assign(code=code, adjustflag="3")],
        ignore_index=True,
    )
    store.write_daily_k("daily_k_none", code, unadjusted_history)

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        dataset="daily_k_qfq",
        code=code,
        end=end_date,
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert state["history_params"] == [
        {
            "code": code,
            "start_date": "2024-01-02",
            "end_date": end_date,
            "adjustflag": "3",
        },
    ]
    stored = store.read_daily_k("daily_k_qfq", code)
    assert pd.to_datetime(stored["date"], errors="coerce").dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_update_daily_provider_calls_stay_on_main_thread(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    main_thread_id = threading.get_ident()

    class ObservingProvider(provider_factory.provider_cls):
        def query_daily_k(self, request) -> pd.DataFrame:
            assert threading.get_ident() == main_thread_id
            return super().query_daily_k(request)

        def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
            assert threading.get_ident() == main_thread_id
            return super().query_adjust_factor(code, start_date, end_date)

    monkeypatch.setattr(update_daily_module, "create_provider", _provider_factory_for(ObservingProvider))

    update_daily_module.update_daily(
        dataset="daily_k_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )
