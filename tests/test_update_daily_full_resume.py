from __future__ import annotations

import threading

import pandas as pd
from update_daily_fakes import _fake_provider_factory, _provider_factory_for, _write_settings

import src.pipeline.update_daily as update_daily_module
import src.pipeline.update_daily_worker as update_daily_worker_module
from src.pipeline.adjustments import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET
from src.storage.parquet_store import ParquetStore


def test_update_daily_full_resumes_failed_code(
    tmp_path, monkeypatch, daily_sample, baostock_cn_stock_basic_sample
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(
        baostock_cn_stock_basic_sample(), daily_sample(), fail_once={"sz.000001"}
    )
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    first = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    second = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in first if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"] == [
        "success",
        "failed",
    ]
    assert first_history_calls == ["sh.600000", "sz.000001"]
    assert state["history_calls"][len(first_history_calls) :] == ["sz.000001"]
    assert [item["status"] for item in second if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"] == ["success"]


def test_update_daily_full_resumes_write_failure(
    tmp_path, monkeypatch, daily_sample, baostock_cn_stock_basic_sample
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    original_write_dataset = ParquetStore.write_dataset
    failed_once = {"value": False}

    def flaky_write_dataset(self, dataset_id: str, df: pd.DataFrame, partition=None, mode=None, skip_existing=False):
        code = partition.get("code") if isinstance(partition, dict) else None
        if dataset_id == "baostock_cn_stock_daily_bar_qfq" and code == "sz.000001" and not failed_once["value"]:
            failed_once["value"] = True
            raise RuntimeError("temporary parquet write failure")
        return original_write_dataset(self, dataset_id, df, partition, mode, skip_existing)

    monkeypatch.setattr(ParquetStore, "write_dataset", flaky_write_dataset)

    first = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
    )
    second = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in first if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"] == [
        "success",
        "failed",
    ]
    assert [item["status"] for item in second if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"] == ["success"]
    assert state["history_calls"] == ["sh.600000", "sz.000001", "sz.000001"]


def test_update_daily_full_fetches_next_code_while_previous_write_is_pending(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())

    first_write_started = threading.Event()
    release_first_write = threading.Event()
    second_fetch_seen = threading.Event()
    original_write_dataset = ParquetStore.write_dataset

    def slow_first_write(self, dataset_id: str, df: pd.DataFrame, partition=None, mode=None, skip_existing=False):
        code = partition.get("code") if isinstance(partition, dict) else None
        if dataset_id == "baostock_cn_stock_daily_bar_qfq" and code == "sh.600000":
            first_write_started.set()
            release_first_write.wait(timeout=5)
        return original_write_dataset(self, dataset_id, df, partition, mode, skip_existing)

    class ObservingProvider(provider_factory.provider_cls):
        def query_daily_bars(
            self,
            request,
        ) -> pd.DataFrame:
            result = super().query_daily_bars(request)
            if request.code == "sz.000001":
                second_fetch_seen.set()
            return result

    monkeypatch.setattr(update_daily_module, "create_provider", _provider_factory_for(ObservingProvider))
    monkeypatch.setattr(ParquetStore, "write_dataset", slow_first_write)

    errors = []

    def run_pipeline() -> None:
        try:
            update_daily_module.update_daily(
                dataset="baostock_cn_stock_daily_bar_qfq",
                mode="full",
                start="2024-01-01",
                end="2024-01-31",
                code=("sh.600000", "sz.000001"),
                root=tmp_path,
                build_views=False,
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_pipeline)
    thread.start()
    try:
        assert first_write_started.wait(timeout=5)
        assert second_fetch_seen.wait(timeout=5)
    finally:
        release_first_write.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []


def test_update_daily_background_executor_uses_configured_default_workers(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    max_workers_seen = []
    original_executor = update_daily_module.ThreadPoolExecutor

    class ObservingExecutor(original_executor):
        def __init__(self, *args, **kwargs):
            if "max_workers" in kwargs:
                max_workers_seen.append(kwargs["max_workers"])
            elif args:
                max_workers_seen.append(args[0])
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(update_daily_module, "ThreadPoolExecutor", ObservingExecutor)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_unadjusted",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert max_workers_seen == [4]


def test_update_daily_waits_for_baostock_cn_stock_adjustment_factor_before_adjusted_calculation(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    code = "sh.600000"
    factor_df = pd.DataFrame(
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
    provider_factory, _state = _fake_provider_factory(
        baostock_cn_stock_basic_sample(),
        daily_sample(),
        baostock_cn_stock_adjustment_factor_df=factor_df,
    )
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    factor_write_started = threading.Event()
    release_factor_write = threading.Event()
    adjusted_calculation_started = threading.Event()
    factor_rows_seen = []
    original_write_dataset = ParquetStore.write_dataset
    original_calculate_adjusted = update_daily_worker_module.calculate_adjusted_daily_bar

    def slow_write_dataset(self, dataset_id: str, df: pd.DataFrame, partition=None, mode=None, skip_existing=False):
        stock_code = partition.get("code") if isinstance(partition, dict) else None
        if dataset_id == "baostock_cn_stock_adjustment_factor" and stock_code == code:
            factor_write_started.set()
            release_factor_write.wait(timeout=5)
        return original_write_dataset(self, dataset_id, df, partition, mode, skip_existing)

    def observing_calculate_adjusted(unadjusted, baostock_cn_stock_adjustment_factors, dataset, adjust_flag):
        adjusted_calculation_started.set()
        factor_rows_seen.append(len(baostock_cn_stock_adjustment_factors))
        return original_calculate_adjusted(unadjusted, baostock_cn_stock_adjustment_factors, dataset, adjust_flag)

    monkeypatch.setattr(ParquetStore, "write_dataset", slow_write_dataset)
    monkeypatch.setattr(update_daily_worker_module, "calculate_adjusted_daily_bar", observing_calculate_adjusted)

    records = []
    errors = []

    def run_pipeline() -> None:
        try:
            records.extend(
                update_daily_module.update_daily(
                    dataset="baostock_cn_stock_daily_bar_qfq",
                    mode="full",
                    start="2024-01-01",
                    end="2024-01-31",
                    code=code,
                    root=tmp_path,
                    build_views=False,
                )
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_pipeline)
    thread.start()
    try:
        assert factor_write_started.wait(timeout=10)
        assert not adjusted_calculation_started.wait(timeout=0.2)
    finally:
        release_factor_write.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert [item["status"] for item in records if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"] == ["success"]
    assert factor_rows_seen == [1]
    assert (
        ParquetStore(root=tmp_path).read_dataset("baostock_cn_stock_daily_bar_qfq", {"code": code}).loc[0, "close"]
        == 16.4
    )


def test_update_daily_worker_releases_code_state_after_initial_daily_processing(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    observed_sizes = []
    original_worker = update_daily_module._DailyUpdateBackgroundWorker

    class ObservingWorker(original_worker):
        def process_daily_initial(self, *args, **kwargs):
            result = super().process_daily_initial(*args, **kwargs)
            observed_sizes.append((len(self._factor_state), len(self._daily_plans_by_code), len(result.api_requests)))
            return result

    monkeypatch.setattr(update_daily_module, "_DailyUpdateBackgroundWorker", ObservingWorker)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert observed_sizes == [(0, 0, 0)]


def test_update_daily_worker_releases_code_state_after_full_refetch(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    observed_sizes = []
    original_worker = update_daily_module._DailyUpdateBackgroundWorker

    class ObservingWorker(original_worker):
        def process_daily_initial(self, *args, **kwargs):
            result = super().process_daily_initial(*args, **kwargs)
            observed_sizes.append(
                ("initial", len(self._factor_state), len(self._daily_plans_by_code), len(result.api_requests))
            )
            return result

        def process_daily_full_refetch(self, *args, **kwargs):
            result = super().process_daily_full_refetch(*args, **kwargs)
            observed_sizes.append(
                ("full_refetch", len(self._factor_state), len(self._daily_plans_by_code), len(result.api_requests))
            )
            return result

    monkeypatch.setattr(update_daily_module, "_DailyUpdateBackgroundWorker", ObservingWorker)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="partial",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert observed_sizes == [
        ("initial", 1, 1, 1),
        ("full_refetch", 0, 0, 0),
    ]


def test_update_daily_worker_does_not_retain_adjustment_factor_state_without_daily_targets(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    observed_sizes = []
    original_worker = update_daily_module._DailyUpdateBackgroundWorker

    class ObservingWorker(original_worker):
        def process_baostock_cn_stock_adjustment_factor_success(self, *args, **kwargs):
            result = super().process_baostock_cn_stock_adjustment_factor_success(*args, **kwargs)
            observed_sizes.append((len(self._factor_state), len(self._daily_plans_by_code)))
            return result

    monkeypatch.setattr(update_daily_module, "_DailyUpdateBackgroundWorker", ObservingWorker)

    update_daily_module.update_daily(
        dataset=BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert observed_sizes == [(0, 0)]


def test_update_daily_parallel_metadata_writes_do_not_drop_rows(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path, metadata_flush_size=1)
    codes = tuple(f"sh.60000{index}" for index in range(8))
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=codes,
        root=tmp_path,
        build_views=False,
    )

    store = ParquetStore(root=tmp_path)
    pipeline_runs = store.read_pipeline_runs()
    dataset_update_status = store.read_dataset_update_status()
    checkpoints = store.read_pipeline_checkpoints()
    expected_pairs = {(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, code) for code in codes} | {
        ("baostock_cn_stock_daily_bar_qfq", code) for code in codes
    }

    assert {(item["dataset"], item["code"]) for item in records} == expected_pairs
    assert (
        len(
            pipeline_runs.loc[
                pipeline_runs["dataset"].isin(
                    {BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, "baostock_cn_stock_daily_bar_qfq"}
                )
            ]
        )
        == 16
    )
    assert (
        set(zip(dataset_update_status["dataset"].astype(str), dataset_update_status["code"].astype(str), strict=False))
        >= expected_pairs
    )
    assert set(zip(checkpoints["dataset"].astype(str), checkpoints["code"].astype(str), strict=False)) >= expected_pairs


def test_update_daily_full_resolves_non_trading_end_to_trading_bound(
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
        mode="full",
        start="2024-01-01",
        end="2024-01-06",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert state["history_params"] == [
        {
            "code": "sh.600000",
            "start_date": "1990-01-01",
            "end_date": "2024-01-05",
            "adjust_flag": "3",
        }
    ]
    assert state["baostock_cn_stock_adjustment_factor_params"] == [
        {
            "code": "sh.600000",
            "start_date": "1990-01-01",
            "end_date": "2024-01-05",
        }
    ]


def test_update_daily_full_checkpoint_lookup_reads_checkpoints_once_per_run(
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
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
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
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert read_calls["count"] == 1
    assert state["history_calls"] == first_history_calls


def test_update_daily_full_batches_daily_checkpoints_by_flush_size(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path, metadata_flush_size=2)
    provider_factory, state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    flush_sizes = []
    original_persist_update_metadata = ParquetStore.persist_update_metadata

    def counted_persist_update_metadata(self, run_rows, status_rows, checkpoint_rows):
        flush_sizes.append(len(checkpoint_rows))
        return original_persist_update_metadata(self, run_rows, status_rows, checkpoint_rows)

    monkeypatch.setattr(ParquetStore, "persist_update_metadata", counted_persist_update_metadata)

    records = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.000001", "sh.600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
    )

    daily_records = [item for item in records if item["dataset"] == "baostock_cn_stock_daily_bar_qfq"]
    assert [item["status"] for item in daily_records] == ["success", "success", "success"]
    assert state["history_calls"] == ["sh.000001", "sh.600000", "sz.000001"]
    assert sum(flush_sizes) == 6
    assert len(flush_sizes) >= 2

    checkpoints = ParquetStore(root=tmp_path).read_pipeline_checkpoints()
    assert set(checkpoints["code"].astype(str)) == {"sh.000001", "sh.600000", "sz.000001"}
    assert set(checkpoints["dataset"].astype(str)) == {
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        "baostock_cn_stock_daily_bar_qfq",
    }
