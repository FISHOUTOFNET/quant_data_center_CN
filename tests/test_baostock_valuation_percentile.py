from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import src.pipeline.baostock_valuation_percentile as valuation_percentile_module
from src.pipeline.baostock_valuation_percentile import (
    BAOSTOCK_VALUATION_PERCENTILE_DATASET,
    PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE,
    compute_valuation_percentiles,
    update_baostock_valuation_percentile,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore

pytestmark = pytest.mark.slow


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


def test_compute_percentiles_excludes_zero_and_null_and_sets_first_all_history_to_100() -> None:
    source = _daily_rows(
        "sh.600000",
        [
            ("2019-01-02", 10.0, 1.0, 1.0, 1.0),
            ("2020-01-02", 0.0, 2.0, 2.0, 2.0),
            ("2021-01-02", None, 3.0, 3.0, 3.0),
            ("2022-01-02", 20.0, 4.0, 4.0, 4.0),
        ],
    )

    result = compute_valuation_percentiles(source)

    assert result.loc[0, "pe_ttm_percentile_all_history"] == 100.0
    assert pd.isna(result.loc[1, "pe_ttm_percentile_all_history"])
    assert pd.isna(result.loc[2, "pe_ttm_percentile_all_history"])
    assert result.loc[3, "pe_ttm_percentile_all_history"] == 100.0
    assert result.loc[3, "pe_ttm_percentile_1y"] == 100.0


def test_compute_percentiles_applies_negative_value_rules() -> None:
    source = _daily_rows(
        "sh.600000",
        [
            ("2019-01-02", 1.0, -5.0, 1.0, 1.0),
            ("2020-01-02", 1.0, 10.0, 1.0, 1.0),
            ("2021-01-02", 1.0, -4.0, 1.0, 1.0),
            ("2022-01-02", 1.0, 20.0, 1.0, 1.0),
        ],
    )

    result = compute_valuation_percentiles(source)

    assert result.loc[1, "pb_mrq_percentile_all_history"] == 100.0
    assert result.loc[2, "pb_mrq_percentile_all_history"] == (2 / 3) * 100
    assert result.loc[3, "pb_mrq_percentile_all_history"] == 100.0


def test_compute_all_history_percentiles_matches_cumulative_business_rules() -> None:
    source = _daily_rows(
        "sh.600000",
        [
            ("2019-01-02", 10.0, 10.0, -5.0, -5.0),
            ("2020-01-02", 20.0, 10.0, 10.0, -5.0),
            ("2021-01-02", 0.0, 20.0, -4.0, -10.0),
            ("2022-01-02", None, 30.0, 20.0, -4.0),
        ],
    )
    work = source.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values(["code", "date"]).reset_index(drop=True)
    for field in valuation_percentile_module.VALUATION_FIELDS:
        work[field] = pd.to_numeric(work[field], errors="coerce")

    result_columns: dict[str, list[object]] = {
        column: work[column].tolist() for column in ("date", "code", *valuation_percentile_module.VALUATION_FIELDS)
    }
    for field in valuation_percentile_module.VALUATION_FIELDS:
        result_columns[f"{field}_percentile_all_history"] = [float("nan")] * len(work)

    valuation_percentile_module._compute_all_history_percentiles(result_columns, work)

    assert result_columns["pe_ttm_percentile_all_history"][:2] == [100.0, 100.0]
    assert pd.isna(result_columns["pe_ttm_percentile_all_history"][2])
    assert pd.isna(result_columns["pe_ttm_percentile_all_history"][3])
    assert result_columns["pb_mrq_percentile_all_history"] == [100.0, 100.0, 100.0, 100.0]
    assert result_columns["ps_ttm_percentile_all_history"] == [100.0, 100.0, (2 / 3) * 100, 100.0]
    assert result_columns["pcf_ncf_ttm_percentile_all_history"] == [100.0, 100.0, 100.0, 25.0]


def test_compute_percentiles_requires_fixed_window_history_maturity() -> None:
    source = _daily_rows(
        "sh.600000",
        [
            ("2018-01-02", 10.0, 1.0, 1.0, 1.0),
            ("2024-01-02", 20.0, 2.0, 2.0, 2.0),
        ],
    )

    result = compute_valuation_percentiles(source)

    assert result.loc[1, "pe_ttm_percentile_1y"] == 100.0
    assert result.loc[1, "pe_ttm_percentile_3y"] == 100.0
    assert result.loc[1, "pe_ttm_percentile_5y"] == 100.0
    assert pd.isna(result.loc[1, "pe_ttm_percentile_10y"])


def test_subtract_years_matches_dateoffset_for_leap_day_threshold() -> None:
    assert valuation_percentile_module._subtract_years(date(2020, 2, 29), 1) == date(2019, 2, 28)
    assert valuation_percentile_module._subtract_years(date(2021, 2, 28), 1) == date(2020, 2, 28)


def test_store_writes_reads_and_builds_valuation_percentile_view(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    computed = compute_valuation_percentiles(_daily_rows("sh.600000", [("2024-01-02", 5.0, 0.7, 1.2, 3.0)]))

    path = store.write_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, computed, {"code": "sh.600000"}).primary_path
    loaded = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"})
    sqls = DuckDBStore(root=tmp_path).build_views()

    assert (
        path
        == tmp_path / "data" / "parquet" / BAOSTOCK_VALUATION_PERCENTILE_DATASET / "code=sh.600000" / "data.parquet"
    )
    assert loaded.loc[0, "pe_ttm_percentile_all_history"] == 100.0
    assert any("v_baostock_cn_stock_valuation_percentile" in sql for sql in sqls)


def test_update_baostock_valuation_percentile_full_and_partial_modes(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sh.600000", [("2024-01-02", 5.0, 0.7, 1.2, 3.0), ("2024-01-03", 6.0, 0.8, 1.3, 3.1)]),
        {"code": "sh.600000"},
    )

    full_records = update_baostock_valuation_percentile(mode="full", root=tmp_path, build_views=False)
    store = ParquetStore(root=tmp_path)
    generated = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"})
    assert full_records[0]["dataset"] == BAOSTOCK_VALUATION_PERCENTILE_DATASET
    assert len(generated) == 2

    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows(
            "sh.600000",
            [
                ("2024-01-02", 5.0, 0.7, 1.2, 3.0),
                ("2024-01-03", 6.0, 0.8, 1.3, 3.1),
                ("2024-01-04", 7.0, 0.9, 1.4, 3.2),
            ],
        ),
        {"code": "sh.600000"},
    )

    partial_records = update_baostock_valuation_percentile(mode="partial", root=tmp_path, build_views=False)
    generated = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"})
    checkpoints = store.read_pipeline_checkpoints()

    assert partial_records[0]["row_count"] == 1
    assert len(generated) == 3
    assert set(checkpoints["pipeline"].astype(str)) == {PIPELINE_UPDATE_BAOSTOCK_VALUATION_PERCENTILE}


def test_update_baostock_valuation_percentile_force_start_replaces_from_start(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sh.600000", [("2024-01-02", 5.0, 0.7, 1.2, 3.0), ("2024-01-03", 6.0, 0.8, 1.3, 3.1)]),
        {"code": "sh.600000"},
    )
    update_baostock_valuation_percentile(mode="full", root=tmp_path, build_views=False)
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows(
            "sh.600000",
            [
                ("2024-01-02", 5.0, 0.7, 1.2, 3.0),
                ("2024-01-03", 60.0, 0.8, 1.3, 3.1),
                ("2024-01-04", 7.0, 0.9, 1.4, 3.2),
            ],
        ),
        {"code": "sh.600000"},
    )

    records = update_baostock_valuation_percentile(
        mode="partial",
        start="2024-01-03",
        force=True,
        root=tmp_path,
        build_views=False,
    )
    generated = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"})

    assert records[0]["row_count"] == 2
    assert generated["date"].astype(str).tolist() == ["2024-01-02", "2024-01-03", "2024-01-04"]
    assert generated.loc[1, "pe_ttm"] == 60.0


def test_update_baostock_valuation_percentile_code_limits_source_partitions(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sh.600000", [("2024-01-02", 5.0, 0.7, 1.2, 3.0)]),
        {"code": "sh.600000"},
    )
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sz.000001", [("2024-01-02", 15.0, 1.7, 2.2, 4.0)]),
        {"code": "sz.000001"},
    )

    records = update_baostock_valuation_percentile(
        mode="full",
        code=("sz.000001",),
        root=tmp_path,
        build_views=False,
    )

    assert [item["code"] for item in records] == ["sz.000001"]
    assert not store.dataset_path(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"}).exists()
    assert store.dataset_path(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sz.000001"}).exists()


def test_update_baostock_valuation_percentile_skips_requested_code_with_missing_source(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    records = update_baostock_valuation_percentile(
        mode="full",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in records] == ["skipped_missing_source"]
    assert "Missing source partition" in str(records[0]["error_stack"])
    assert not store.dataset_path(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"}).exists()


def test_update_baostock_valuation_percentile_force_logs_progress(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sh.600000", [("2024-01-02", 5.0, 0.7, 1.2, 3.0)]),
        {"code": "sh.600000"},
    )
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sz.000001", [("2024-01-02", 15.0, 1.7, 2.2, 4.0)]),
        {"code": "sz.000001"},
    )
    fake_logger = FakeLogger()
    monkeypatch.setattr(valuation_percentile_module, "logger", fake_logger, raising=False)

    records = update_baostock_valuation_percentile(
        mode="full",
        root=tmp_path,
        build_views=False,
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
    assert all(entry[2][3] == "success" for entry in progress_entries)
    assert _log_entries(
        fake_logger,
        "Baostock valuation percentile update started mode={} force={} planned_codes={} processing_codes={}",
    )
    assert _log_entries(
        fake_logger,
        "Baostock valuation percentile update completed processed={} success={} failed={} skipped={}",
    )


def test_update_baostock_valuation_percentile_prefilter_skips_checkpointed_codes_and_force_bypasses(
    tmp_path,
    monkeypatch,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted",
        _daily_rows("sh.600000", [("2024-01-02", 5.0, 0.7, 1.2, 3.0)]),
        {"code": "sh.600000"},
    )
    update_baostock_valuation_percentile(mode="full", root=tmp_path, build_views=False)
    fake_logger = FakeLogger()
    monkeypatch.setattr(valuation_percentile_module, "logger", fake_logger, raising=False)

    records = update_baostock_valuation_percentile(mode="full", root=tmp_path, build_views=False)

    assert records == []
    assert _log_entries(
        fake_logger,
        "Checkpoint prefilter skipped {}/{} baostock valuation percentile codes ({:.1f}%); processing {} codes",
    )

    fake_logger.entries.clear()
    forced_records = update_baostock_valuation_percentile(
        mode="full",
        root=tmp_path,
        build_views=False,
        force=True,
    )

    assert [item["status"] for item in forced_records] == ["success"]
    assert not _log_entries(
        fake_logger,
        "Checkpoint prefilter skipped {}/{} baostock valuation percentile codes ({:.1f}%); processing {} codes",
    )


def test_update_baostock_valuation_percentile_prefilter_skips_partial_when_existing_is_current(
    tmp_path,
    monkeypatch,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    source = _daily_rows(
        "sh.600000",
        [("2024-01-02", 5.0, 0.7, 1.2, 3.0), ("2024-01-03", 6.0, 0.8, 1.3, 3.1)],
    )
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", source, {"code": "sh.600000"})
    existing = compute_valuation_percentiles(source)
    store.write_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, existing, {"code": "sh.600000"})

    def fail_compute(_source: pd.DataFrame) -> pd.DataFrame:
        raise AssertionError("up-to-date partial run should not recompute percentiles")

    monkeypatch.setattr(valuation_percentile_module, "compute_valuation_percentiles", fail_compute)

    records = update_baostock_valuation_percentile(mode="partial", root=tmp_path, build_views=False)

    generated = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"})
    assert records == []
    assert generated["date"].astype(str).tolist() == ["2024-01-02", "2024-01-03"]


def test_update_baostock_valuation_percentile_partial_computes_when_source_has_new_date(
    tmp_path,
    monkeypatch,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    existing_source = _daily_rows(
        "sh.600000",
        [("2024-01-02", 5.0, 0.7, 1.2, 3.0), ("2024-01-03", 6.0, 0.8, 1.3, 3.1)],
    )
    store.write_dataset(
        BAOSTOCK_VALUATION_PERCENTILE_DATASET,
        compute_valuation_percentiles(existing_source),
        {"code": "sh.600000"},
    )
    updated_source = _daily_rows(
        "sh.600000",
        [
            ("2024-01-02", 5.0, 0.7, 1.2, 3.0),
            ("2024-01-03", 6.0, 0.8, 1.3, 3.1),
            ("2024-01-04", 7.0, 0.9, 1.4, 3.2),
        ],
    )
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", updated_source, {"code": "sh.600000"})

    def fail_full_compute(_source: pd.DataFrame) -> pd.DataFrame:
        raise AssertionError("append-only partial update should not run full percentile computation")

    monkeypatch.setattr(valuation_percentile_module, "compute_valuation_percentiles", fail_full_compute)

    records = update_baostock_valuation_percentile(mode="partial", root=tmp_path, build_views=False)

    generated = store.read_dataset(BAOSTOCK_VALUATION_PERCENTILE_DATASET, {"code": "sh.600000"})
    assert records[0]["row_count"] == 1
    assert generated["date"].astype(str).tolist() == ["2024-01-02", "2024-01-03", "2024-01-04"]


@pytest.mark.performance
@pytest.mark.slow
def test_append_only_percentiles_match_full_compute_for_large_increment() -> None:
    history_days = pd.date_range("2000-01-03", periods=3700, freq="D")
    append_days = pd.date_range(history_days[-1] + pd.Timedelta(days=1), periods=6, freq="D")
    values: list[tuple[str, float | None, float | None, float | None, float | None]] = []
    for index, day in enumerate([*history_days, *append_days], start=1):
        pe_ttm = float((index % 97) + 1)
        pb_mrq = float((index % 53) - 26)
        ps_ttm = float((index % 37) + 1)
        pcf_ncf_ttm = float((index % 71) - 35)
        values.append((day.date().isoformat(), pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm))

    values[-6] = (append_days[0].date().isoformat(), 0.0, -12.0, 1.5, 3.0)
    values[-5] = (append_days[1].date().isoformat(), None, 0.0, -4.0, 0.0)
    values[-4] = (append_days[2].date().isoformat(), 120.0, 15.0, None, -8.0)

    source = _daily_rows("sh.600000", values)
    existing = compute_valuation_percentiles(source.iloc[:-6])
    expected = compute_valuation_percentiles(source).tail(6).reset_index(drop=True)

    actual = valuation_percentile_module._compute_append_only_valuation_percentiles(source, existing)

    pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected, check_dtype=False)


def _log_entries(logger: FakeLogger, message: str) -> list[tuple[str, str, tuple[object, ...]]]:
    return [entry for entry in logger.entries if entry[1] == message]


def _daily_rows(
    code: str, values: list[tuple[str, float | None, float | None, float | None, float | None]]
) -> pd.DataFrame:
    rows = []
    for index, (day, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm) in enumerate(values, start=1):
        rows.append(
            {
                "date": date.fromisoformat(day),
                "code": code,
                "open": 8.0 + index,
                "high": 8.5 + index,
                "low": 7.9 + index,
                "close": 8.2 + index,
                "prev_close": 8.0 + index,
                "volume": 1000 + index,
                "amount": 8200.0 + index,
                "adjust_flag": "3",
                "turnover_rate": 0.1,
                "trade_status": "1",
                "pct_change": 1.0,
                "pe_ttm": pe_ttm,
                "pb_mrq": pb_mrq,
                "ps_ttm": ps_ttm,
                "pcf_ncf_ttm": pcf_ncf_ttm,
                "is_st": "0",
            }
        )
    return pd.DataFrame(rows)
