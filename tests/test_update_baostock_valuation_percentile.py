from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

import src.pipeline.update_baostock_valuation_percentile as valuation_update_module
from src.pipeline.update_baostock_valuation_percentile import update_baostock_valuation_percentile
from src.storage.parquet_store import ParquetStore
from update_daily_fakes import _write_settings


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

    def observing_write(self, code: str, df: pd.DataFrame):
        write_codes.append(code)
        return original_write(self, code, df)

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


def test_update_baostock_valuation_percentile_records_write_failure_and_continues(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", "sh.600000", _source_daily([("2024-01-02", 5.0)]))

    def failing_write(self, code: str, df: pd.DataFrame):
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
