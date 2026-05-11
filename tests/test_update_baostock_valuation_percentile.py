from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

import src.pipeline.update_baostock_valuation_percentile as valuation_update_module
from src.pipeline.update_baostock_valuation_percentile import update_baostock_valuation_percentile
import src.storage.parquet_store as parquet_store_module
from src.storage.parquet_store import ParquetStore
from update_daily_fakes import _write_settings


class FakeLogger:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, tuple[object, ...]]] = []

    def info(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("info", message, args))

    def warning(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("warning", message, args))

    def error(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("error", message, args))

    def exception(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("exception", message, args))


def test_update_baostock_valuation_percentile_full_generates_all_source_dates_and_metadata(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 0.0), ("2024-01-03", 5.0)]))

    records = update_baostock_valuation_percentile(
        mode="full",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert [(item["dataset"], item["code"], item["status"]) for item in records] == [
        ("baostock_cn_stock_valuation_percentile", "sh.600000", "success")
    ]
    output = store.read_baostock_cn_stock_valuation_percentile("sh.600000")
    assert len(output) == 2
    assert pd.isna(output.loc[0, "pe_ttm_percentile_all_history"])
    assert output.loc[1, "pe_ttm_percentile_all_history"] == 100.0
    checkpoints = store.read_pipeline_checkpoints()
    latest = checkpoints.sort_values("updated_at").iloc[-1]
    assert latest["pipeline"] == "update_baostock_valuation_percentile"
    assert latest["dataset"] == "baostock_cn_stock_valuation_percentile"
    assert latest["code"] == "sh.600000"


def test_update_baostock_valuation_percentile_partial_appends_new_source_dates(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0)]))
    update_baostock_valuation_percentile(mode="full", code=("sh.600000",), root=tmp_path, build_views=False)

    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0), ("2024-01-04", 7.5)]))

    records = update_baostock_valuation_percentile(
        mode="partial",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in records] == ["success"]
    output = store.read_baostock_cn_stock_valuation_percentile("sh.600000")
    assert pd.to_datetime(output["date"]).dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    assert output.loc[2, "pe_ttm_percentile_all_history"] == pytest.approx(100 * 2 / 3)


def test_update_baostock_valuation_percentile_partial_skips_when_no_new_source_dates(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0)]))
    update_baostock_valuation_percentile(mode="full", code=("sh.600000",), root=tmp_path, build_views=False)
    checkpoint_count = len(store.read_pipeline_checkpoints())

    records = update_baostock_valuation_percentile(
        mode="partial",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert records == []
    assert len(store.read_pipeline_checkpoints()) == checkpoint_count


def test_update_baostock_valuation_percentile_prefilter_skips_when_target_covers_source_latest_without_checkpoint(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0)]),
    )
    store.write_baostock_cn_stock_valuation_percentile(
        code,
        _valuation_rows([("2024-01-02", 5.0), ("2024-01-03", 10.0)], code=code),
    )
    compute_calls: list[str] = []
    original_compute = valuation_update_module.compute_valuation_percentiles

    def observing_compute(df: pd.DataFrame, start: str | None = None) -> pd.DataFrame:
        compute_calls.append(str(df["code"].iloc[0]))
        return original_compute(df, start=start)

    monkeypatch.setattr(valuation_update_module, "compute_valuation_percentiles", observing_compute)

    records = update_baostock_valuation_percentile(
        mode="partial",
        code=(code,),
        root=tmp_path,
        build_views=False,
    )

    assert records == []
    assert compute_calls == []
    assert store.read_pipeline_checkpoints().empty


def test_source_date_bounds_prefers_parquet_metadata_without_reading_date_column(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-04", 7.5)], code=code),
    )

    def fail_read_parquet(*args, **kwargs):
        raise AssertionError("date column should not be read when parquet metadata has date statistics")

    monkeypatch.setattr(valuation_update_module.pd, "read_parquet", fail_read_parquet)

    assert valuation_update_module._source_date_bounds(store, code) == ("2024-01-02", "2024-01-04")


def test_source_date_bounds_falls_back_to_date_column_when_metadata_bounds_are_missing(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-04", 7.5)], code=code),
    )
    read_columns: list[list[str] | None] = []
    original_read_parquet = pd.read_parquet

    def empty_metadata_bounds(path):
        del path
        return None

    def counted_read_parquet(*args, **kwargs):
        read_columns.append(kwargs.get("columns"))
        return original_read_parquet(*args, **kwargs)

    monkeypatch.setattr(valuation_update_module, "_parquet_date_bounds", empty_metadata_bounds)
    monkeypatch.setattr(valuation_update_module.pd, "read_parquet", counted_read_parquet)

    assert valuation_update_module._source_date_bounds(store, code) == ("2024-01-02", "2024-01-04")
    assert read_columns == [["date"]]


def test_update_baostock_valuation_percentile_prefilter_keeps_when_source_latest_is_newer(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0), ("2024-01-04", 7.5)]),
    )
    store.write_baostock_cn_stock_valuation_percentile(
        code,
        _valuation_rows([("2024-01-02", 5.0), ("2024-01-03", 10.0)], code=code),
    )
    compute_starts: list[str | None] = []
    original_compute = valuation_update_module.compute_valuation_percentiles

    def observing_compute(df: pd.DataFrame, start: str | None = None) -> pd.DataFrame:
        compute_starts.append(start)
        return original_compute(df, start=start)

    monkeypatch.setattr(valuation_update_module, "compute_valuation_percentiles", observing_compute)

    records = update_baostock_valuation_percentile(
        mode="partial",
        code=(code,),
        root=tmp_path,
        build_views=False,
        workers=1,
    )

    assert [item["status"] for item in records] == ["success"]
    assert compute_starts == ["2024-01-04"]
    output = store.read_baostock_cn_stock_valuation_percentile(code)
    assert pd.to_datetime(output["date"]).dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]


def test_update_baostock_valuation_percentile_force_full_bypasses_latest_prefilter(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0)]),
    )
    store.write_baostock_cn_stock_valuation_percentile(
        code,
        _valuation_rows([("2024-01-02", 5.0), ("2024-01-03", 10.0)], code=code),
    )
    compute_calls: list[str | None] = []
    original_compute = valuation_update_module.compute_valuation_percentiles

    def observing_compute(df: pd.DataFrame, start: str | None = None) -> pd.DataFrame:
        compute_calls.append(start)
        return original_compute(df, start=start)

    monkeypatch.setattr(valuation_update_module, "compute_valuation_percentiles", observing_compute)

    records = update_baostock_valuation_percentile(
        mode="full",
        code=(code,),
        root=tmp_path,
        build_views=False,
        force=True,
        workers=1,
    )

    assert [item["status"] for item in records] == ["success"]
    assert compute_calls == [None]


def test_update_baostock_valuation_percentile_compute_reads_only_valuation_columns(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0)], code=code),
    )
    read_columns: list[tuple[str, ...] | None] = []
    original_read = ParquetStore.read_baostock_daily_bars

    def observing_read(self, dataset: str, stock_code: str, columns=None):
        read_columns.append(tuple(columns) if columns is not None else None)
        return original_read(self, dataset, stock_code, columns=columns)

    monkeypatch.setattr(ParquetStore, "read_baostock_daily_bars", observing_read)

    records = update_baostock_valuation_percentile(
        mode="full",
        code=(code,),
        root=tmp_path,
        build_views=False,
        resume=False,
        workers=1,
    )

    assert [item["status"] for item in records] == ["success"]
    assert read_columns == [("date", "code", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm")]


def test_update_baostock_valuation_percentile_checkpoint_lookup_reads_checkpoints_once_per_run(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    for code, pe_ttm in [("sh.600000", 5.0), ("sz.000001", 6.0)]:
        store.write_baostock_daily_bars(
            "baostock_cn_stock_daily_bar_unadjusted",
            code,
            _source_daily(
                [("2024-01-02", pe_ttm), ("2024-01-03", pe_ttm + 1.0)],
                code=code,
            ),
        )
        store.write_baostock_cn_stock_valuation_percentile(
            code,
            _valuation_rows([("2024-01-02", pe_ttm)], code=code),
        )
    read_calls = {"count": 0}
    original_read_pipeline_checkpoints = ParquetStore.read_pipeline_checkpoints

    def counted_read_pipeline_checkpoints(self):
        read_calls["count"] += 1
        return original_read_pipeline_checkpoints(self)

    monkeypatch.setattr(ParquetStore, "read_pipeline_checkpoints", counted_read_pipeline_checkpoints)

    records = update_baostock_valuation_percentile(
        mode="partial",
        root=tmp_path,
        build_views=False,
        workers=1,
    )

    assert sorted((item["code"], item["status"]) for item in records) == [
        ("sh.600000", "success"),
        ("sz.000001", "success"),
    ]
    assert read_calls["count"] == 1


def test_update_baostock_valuation_percentile_explicit_start_bypasses_latest_prefilter(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    code = "sh.600000"
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        code,
        _source_daily([("2024-01-02", 5.0), ("2024-01-03", 10.0)]),
    )
    store.write_baostock_cn_stock_valuation_percentile(
        code,
        _valuation_rows([("2024-01-02", 5.0), ("2024-01-03", 10.0)], code=code),
    )
    compute_starts: list[str | None] = []
    original_compute = valuation_update_module.compute_valuation_percentiles

    def observing_compute(df: pd.DataFrame, start: str | None = None) -> pd.DataFrame:
        compute_starts.append(start)
        return original_compute(df, start=start)

    monkeypatch.setattr(valuation_update_module, "compute_valuation_percentiles", observing_compute)

    records = update_baostock_valuation_percentile(
        mode="partial",
        code=(code,),
        start="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
    )

    assert [item["status"] for item in records] == ["success"]
    assert compute_starts == ["2024-01-03"]


def test_update_baostock_valuation_percentile_start_force_recomputes_from_start_and_preserves_earlier_rows(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2020-01-01", 10.0), ("2021-01-01", 20.0), ("2022-01-01", 30.0)]))
    update_baostock_valuation_percentile(mode="full", code=("sh.600000",), root=tmp_path, build_views=False)

    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2020-01-01", 10.0), ("2021-01-01", 5.0), ("2022-01-01", 30.0)]))

    update_baostock_valuation_percentile(
        mode="partial",
        code=("sh.600000",),
        start="2021-01-01",
        force=True,
        root=tmp_path,
        build_views=False,
    )

    output = store.read_baostock_cn_stock_valuation_percentile("sh.600000")
    assert output.loc[0, "pe_ttm_percentile_all_history"] == 100.0
    assert output.loc[1, "pe_ttm"] == 5.0
    assert output.loc[1, "pe_ttm_percentile_all_history"] == 50.0
    assert output.loc[2, "pe_ttm_percentile_all_history"] == 100.0


def test_update_baostock_valuation_percentile_explicit_code_limits_outputs(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0)], code="sh.600000"))
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sz.000001", _source_daily([("2024-01-02", 6.0)], code="sz.000001"))

    update_baostock_valuation_percentile(
        mode="full",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert store.baostock_cn_stock_valuation_percentile_path("sh.600000").exists()
    assert not store.baostock_cn_stock_valuation_percentile_path("sz.000001").exists()


def test_update_baostock_valuation_percentile_workers_compute_in_parallel_and_write_serially(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0)], code="sh.600000"))
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sz.000001", _source_daily([("2024-01-02", 6.0)], code="sz.000001"))
    max_workers_seen: list[int | None] = []
    submitted_codes: list[str] = []
    write_codes: list[str] = []
    original_write = ParquetStore.write_baostock_cn_stock_valuation_percentile

    class ObservingExecutor:
        def __init__(self, max_workers: int | None = None, **kwargs) -> None:
            del kwargs
            max_workers_seen.append(max_workers)
            self._executor = ThreadPoolExecutor(max_workers=max_workers)

        def __enter__(self):
            self._executor.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._executor.__exit__(exc_type, exc, tb)

        def submit(self, fn, task):
            submitted_codes.append(task.code)
            return self._executor.submit(fn, task)

    def observing_write(self, code: str, df: pd.DataFrame, *args, **kwargs):
        write_codes.append(code)
        return original_write(self, code, df, *args, **kwargs)

    monkeypatch.setattr(valuation_update_module, "ProcessPoolExecutor", ObservingExecutor, raising=False)
    monkeypatch.setattr(ParquetStore, "write_baostock_cn_stock_valuation_percentile", observing_write)

    records = update_baostock_valuation_percentile(
        mode="full",
        root=tmp_path,
        build_views=False,
        workers=2,
    )

    assert max_workers_seen == [2]
    assert sorted(submitted_codes) == ["sh.600000", "sz.000001"]
    assert sorted(write_codes) == ["sh.600000", "sz.000001"]
    assert sorted((item["code"], item["status"]) for item in records) == [
        ("sh.600000", "success"),
        ("sz.000001", "success"),
    ]
    assert store.baostock_cn_stock_valuation_percentile_path("sh.600000").exists()
    assert store.baostock_cn_stock_valuation_percentile_path("sz.000001").exists()


def test_update_baostock_valuation_percentile_defers_per_stock_registry_inventory_refresh(
    tmp_path,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        "sh.600000",
        _source_daily([("2024-01-02", 5.0)], code="sh.600000"),
    )
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        "sz.000001",
        _source_daily([("2024-01-02", 6.0)], code="sz.000001"),
    )
    publish_calls: list[dict[str, object]] = []
    refresh_calls: list[dict[str, object]] = []

    class FakeRegistry:
        def __init__(self, root=None) -> None:
            self.root = root

        def publish_dataframe_write(
            self,
            dataset: str,
            code: str,
            df: pd.DataFrame,
            destination,
            refresh_inventory: bool = True,
        ) -> None:
            publish_calls.append(
                {
                    "dataset": dataset,
                    "code": code,
                    "rows": len(df),
                    "destination": destination,
                    "refresh_inventory": refresh_inventory,
                }
            )

        def refresh_inventory(self, dataset_ids=None, status_rows=None):
            refresh_calls.append(
                {
                    "dataset_ids": list(dataset_ids or []),
                    "status_rows": len(pd.DataFrame(status_rows)),
                }
            )
            return pd.DataFrame()

    monkeypatch.setattr(parquet_store_module, "DataRegistry", FakeRegistry)

    records = update_baostock_valuation_percentile(
        mode="full",
        root=tmp_path,
        build_views=False,
        resume=False,
        workers=1,
    )

    assert sorted((item["code"], item["status"]) for item in records) == [
        ("sh.600000", "success"),
        ("sz.000001", "success"),
    ]
    assert [call["refresh_inventory"] for call in publish_calls] == [False, False]
    assert refresh_calls == [
        {
            "dataset_ids": ["baostock_cn_stock_valuation_percentile"],
            "status_rows": 2,
        }
    ]


def test_update_baostock_valuation_percentile_rejects_concurrent_run(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars(
        "baostock_cn_stock_daily_bar_unadjusted",
        "sh.600000",
        _source_daily([("2024-01-02", 5.0)], code="sh.600000"),
    )
    first_started = threading.Event()
    release_first = threading.Event()
    call_count = 0
    original_task = valuation_update_module._compute_valuation_percentile_task

    def blocking_task(task):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            first_started.set()
            assert release_first.wait(timeout=5)
            return original_task(task)
        raise AssertionError("concurrent valuation run reached compute")

    monkeypatch.setattr(valuation_update_module, "_compute_valuation_percentile_task", blocking_task)

    with ThreadPoolExecutor(max_workers=1) as executor:
        first_run = executor.submit(
            update_baostock_valuation_percentile,
            mode="full",
            code=("sh.600000",),
            root=tmp_path,
            build_views=False,
            resume=False,
            force=True,
            workers=1,
        )
        assert first_started.wait(timeout=5)
        with pytest.raises(RuntimeError, match="already running"):
            update_baostock_valuation_percentile(
                mode="full",
                code=("sh.600000",),
                root=tmp_path,
                build_views=False,
                resume=False,
                force=True,
                workers=1,
            )
        release_first.set()
        assert [item["status"] for item in first_run.result(timeout=10)] == ["success"]


@pytest.mark.parametrize("workers", [1, 2])
def test_update_baostock_valuation_percentile_logs_progress_for_each_processed_stock(
    tmp_path,
    monkeypatch,
    workers: int,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0)], code="sh.600000"))
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sz.000001", _source_daily([("2024-01-02", 6.0)], code="sz.000001"))
    fake_logger = FakeLogger()
    monkeypatch.setattr(valuation_update_module, "logger", fake_logger)

    class ObservingExecutor:
        def __init__(self, max_workers: int | None = None, **kwargs) -> None:
            del max_workers, kwargs
            self._executor = ThreadPoolExecutor(max_workers=2)

        def __enter__(self):
            self._executor.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._executor.__exit__(exc_type, exc, tb)

        def submit(self, fn, task):
            return self._executor.submit(fn, task)

    if workers > 1:
        monkeypatch.setattr(valuation_update_module, "ProcessPoolExecutor", ObservingExecutor, raising=False)

    records = update_baostock_valuation_percentile(
        mode="full",
        root=tmp_path,
        build_views=False,
        workers=workers,
        force=True,
    )

    progress_entries = _log_entries(
        fake_logger,
        "Baostock valuation percentile progress {}/{} code={} status={} rows={}",
    )
    assert len(records) == 2
    assert len(progress_entries) == 2
    assert [entry[2][0] for entry in progress_entries] == [1, 2]
    assert all(entry[2][1] == 2 for entry in progress_entries)
    assert sorted(entry[2][2] for entry in progress_entries) == ["sh.600000", "sz.000001"]
    assert all(entry[2][3] == "success" for entry in progress_entries)


def test_update_baostock_valuation_percentile_records_write_failure_and_continues(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0)]))

    def failing_write(self, code: str, df: pd.DataFrame, *args, **kwargs):
        del args, kwargs
        raise PermissionError(f"locked {code}")

    monkeypatch.setattr(ParquetStore, "write_baostock_cn_stock_valuation_percentile", failing_write)

    records = update_baostock_valuation_percentile(
        mode="full",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert [(item["code"], item["status"], item["row_count"]) for item in records] == [("sh.600000", "failed", 0)]
    assert "PermissionError" in str(records[0]["error_stack"])
    checkpoints = store.read_pipeline_checkpoints()
    latest = checkpoints.sort_values("updated_at").iloc[-1]
    assert latest["status"] == "failed"
    assert "PermissionError" in str(latest["error_stack"])


def _source_daily(rows: list[tuple[str, float]], code: str = "sh.600000") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date_text,
                "code": code,
                "open": 8.1,
                "high": 8.3,
                "low": 8.0,
                "close": 8.2,
                "prev_close": 8.0,
                "volume": 1000,
                "amount": 8200.0,
                "adjust_flag": "3",
                "turnover_rate": 0.1,
                "trade_status": "1",
                "pct_change": 2.5,
                "pe_ttm": pe_ttm,
                "pb_mrq": 0.7,
                "ps_ttm": 1.2,
                "pcf_ncf_ttm": 3.0,
                "is_st": "0",
            }
            for date_text, pe_ttm in rows
        ]
    )


def _valuation_rows(rows: list[tuple[str, float]], code: str = "sh.600000") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date_text,
                "code": code,
                "pe_ttm": pe_ttm,
                "pb_mrq": 0.7,
                "ps_ttm": 1.2,
                "pcf_ncf_ttm": 3.0,
            }
            for date_text, pe_ttm in rows
        ]
    )


def _log_entries(logger: FakeLogger, message: str) -> list[tuple[str, str, tuple[object, ...]]]:
    return [entry for entry in logger.entries if entry[1] == message]
