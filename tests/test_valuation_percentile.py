from __future__ import annotations

import time
from datetime import date, timedelta

import pandas as pd
import pytest

from src.analytics.valuation_percentile import (
    ALL_HISTORY_WINDOW,
    ROLLING_WINDOWS,
    VALUATION_FIELDS,
    compute_valuation_percentiles,
    output_columns,
    percentile_column,
    valuation_percentile,
)
from src.storage.dataset_catalog import BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET
from src.storage.parquet_store import ParquetStore


def test_valuation_percentile_applies_signed_history_rules() -> None:
    assert valuation_percentile(4.0, [-2.0, -1.0, 1.0, 3.0, 5.0]) == pytest.approx(200 / 3)
    assert valuation_percentile(-1.5, [-3.0, -2.0, -1.0, 1.0, 2.0]) == pytest.approx(60.0)
    assert valuation_percentile(3.0, [1.0, 3.0, 5.0]) == pytest.approx(200 / 3)


def test_compute_valuation_percentiles_treats_zero_as_missing_and_all_history_starts_at_first_valid_value() -> None:
    frame = _valuation_frame(
        [
            ("2024-01-02", 0.0),
            ("2024-01-03", None),
            ("2024-01-04", 5.0),
            ("2024-01-05", 10.0),
        ]
    )

    result = compute_valuation_percentiles(frame)

    assert pd.isna(result.loc[0, "pe_ttm"])
    assert pd.isna(result.loc[1, "pe_ttm"])
    assert pd.isna(result.loc[0, "pe_ttm_percentile_all_history"])
    assert pd.isna(result.loc[1, "pe_ttm_percentile_all_history"])
    assert result.loc[2, "pe_ttm_percentile_all_history"] == pytest.approx(100.0)
    assert result.loc[3, "pe_ttm_percentile_all_history"] == pytest.approx(100.0)
    assert result.filter(like="percentile_1y").isna().all().all()


def test_compute_valuation_percentiles_requires_full_window_history_and_includes_current_day() -> None:
    frame = _valuation_frame(
        [
            ("2020-01-01", 10.0),
            ("2020-12-31", 20.0),
            ("2021-01-01", 15.0),
        ]
    )

    result = compute_valuation_percentiles(frame)

    assert pd.isna(result.loc[1, "pe_ttm_percentile_1y"])
    assert result.loc[2, "pe_ttm_percentile_1y"] == pytest.approx(200 / 3)
    assert pd.isna(result.loc[2, "pe_ttm_percentile_3y"])
    assert pd.isna(result.loc[2, "pe_ttm_percentile_5y"])
    assert pd.isna(result.loc[2, "pe_ttm_percentile_10y"])


def test_compute_valuation_percentiles_skips_ten_year_window_when_only_six_years_exist() -> None:
    frame = _valuation_frame(
        [
            ("2020-01-01", 10.0),
            ("2026-01-01", 20.0),
        ]
    )

    result = compute_valuation_percentiles(frame)

    assert result.loc[1, "pe_ttm_percentile_1y"] == pytest.approx(100.0)
    assert result.loc[1, "pe_ttm_percentile_3y"] == pytest.approx(100.0)
    assert result.loc[1, "pe_ttm_percentile_5y"] == pytest.approx(100.0)
    assert pd.isna(result.loc[1, "pe_ttm_percentile_10y"])
    assert result.loc[1, "pe_ttm_percentile_all_history"] == pytest.approx(100.0)


def test_compute_valuation_percentiles_matches_naive_reference_for_mixed_history() -> None:
    frame = _mixed_valuation_frame()

    result = compute_valuation_percentiles(frame, start="2020-03-01")
    expected = _naive_expected_percentiles(frame, start="2020-03-01")

    assert list(result.columns) == output_columns()
    pd.testing.assert_series_equal(result["date"], expected["date"], check_names=False)
    pd.testing.assert_series_equal(result["code"], expected["code"], check_names=False)
    for field in VALUATION_FIELDS:
        pd.testing.assert_series_equal(result[field], expected[field], check_names=False, check_dtype=False)
        for window, _ in ROLLING_WINDOWS:
            _assert_percentile_series_equal(result[percentile_column(field, window)], expected[percentile_column(field, window)])
        _assert_percentile_series_equal(
            result[percentile_column(field, ALL_HISTORY_WINDOW)],
            expected[percentile_column(field, ALL_HISTORY_WINDOW)],
        )


def test_compute_valuation_percentiles_outputs_schema_ready_numeric_percentile_columns(tmp_path) -> None:
    frame = _performance_frame(400)

    result = compute_valuation_percentiles(frame)

    assert list(result.columns) == output_columns()
    percentile_columns = [column for column in result.columns if "_percentile_" in column]
    assert percentile_columns
    assert all(pd.api.types.is_numeric_dtype(result[column]) for column in percentile_columns)
    cleaned = ParquetStore(root=tmp_path).clean_dataframe_for_schema(
        result,
        BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.schema,
    )
    BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET.validator(cleaned)


def test_compute_valuation_percentiles_has_lightweight_performance_budget() -> None:
    frame = _performance_frame(1000)

    started_at = time.perf_counter()
    result = compute_valuation_percentiles(frame)
    elapsed = time.perf_counter() - started_at

    assert len(result) == len(frame)
    assert elapsed < 3.0


def _valuation_frame(rows: list[tuple[str, float | None]]) -> pd.DataFrame:
    records = []
    for date_text, pe_ttm in rows:
        records.append(
            {
                "date": date.fromisoformat(date_text),
                "code": "sh.600000",
                "pe_ttm": pe_ttm,
                "pb_mrq": None,
                "ps_ttm": None,
                "pcf_ncf_ttm": None,
            }
        )
    return pd.DataFrame(records)


def _mixed_valuation_frame() -> pd.DataFrame:
    rows = []
    start_date = date(2018, 1, 1)
    pe_values = [0.0, None, 5.0, 10.0, -3.0, 5.0, 20.0]
    for index in range(95):
        row_date = start_date + timedelta(days=index * 23)
        rows.append(
            {
                "date": row_date,
                "code": "sh.600000",
                "pe_ttm": pe_values[index % len(pe_values)],
                "pb_mrq": (index % 11) + 0.5,
                "ps_ttm": None if index % 9 == 0 else (index % 5) - 2.0,
                "pcf_ncf_ttm": ((-1) ** index) * ((index % 13) + 1.0),
            }
        )
    return pd.DataFrame(rows).sample(frac=1, random_state=42).reset_index(drop=True)


def _performance_frame(row_count: int) -> pd.DataFrame:
    start_date = date(2010, 1, 1)
    return pd.DataFrame(
        {
            "date": [start_date + timedelta(days=index) for index in range(row_count)],
            "code": ["sh.600000"] * row_count,
            "pe_ttm": [(index % 97) + 1.0 for index in range(row_count)],
            "pb_mrq": [(index % 53) / 10.0 + 0.1 for index in range(row_count)],
            "ps_ttm": [(index % 37) / 10.0 + 0.1 for index in range(row_count)],
            "pcf_ncf_ttm": [((-1) ** index) * ((index % 29) + 1.0) for index in range(row_count)],
        }
    )


def _naive_expected_percentiles(df: pd.DataFrame, start: str | None = None) -> pd.DataFrame:
    work = df.copy()
    work["_input_order"] = range(len(work))
    work["_date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values(["code", "_date", "_input_order"]).reset_index(drop=True)

    result = pd.DataFrame({"date": work["date"], "code": work["code"].astype("string")})
    for field in VALUATION_FIELDS:
        result[field] = _clean_values(work[field] if field in work.columns else pd.Series(pd.NA, index=work.index))

    for field in VALUATION_FIELDS:
        _assign_naive_field_percentiles(work["_date"], result[field], result, field)

    if start is not None:
        dates = pd.to_datetime(result["date"], errors="coerce")
        result = result.loc[dates >= pd.Timestamp(start)].reset_index(drop=True)
    return result.reset_index(drop=True)


def _assign_naive_field_percentiles(
    dates: pd.Series,
    values: pd.Series,
    result: pd.DataFrame,
    field: str,
) -> None:
    valid_dates = dates.loc[values.notna()]
    first_valid_date = valid_dates.min() if not valid_dates.empty else None
    history: list[tuple[pd.Timestamp, float]] = []

    for index, row_date in dates.items():
        current = values.loc[index]
        current_value = None if pd.isna(current) else float(current)
        if pd.isna(row_date) or current_value is None:
            _assign_naive_empty_percentiles(result, index, field)
            continue

        history.append((row_date, current_value))
        for window, years in ROLLING_WINDOWS:
            cutoff = row_date - pd.DateOffset(years=years)
            if first_valid_date is not None and first_valid_date <= cutoff:
                values_in_window = [value for value_date, value in history if value_date >= cutoff]
                result.loc[index, percentile_column(field, window)] = valuation_percentile(current_value, values_in_window)
            else:
                result.loc[index, percentile_column(field, window)] = float("nan")
        result.loc[index, percentile_column(field, ALL_HISTORY_WINDOW)] = valuation_percentile(
            current_value,
            [value for _, value in history],
        )


def _assign_naive_empty_percentiles(result: pd.DataFrame, index: int, field: str) -> None:
    for window, _ in ROLLING_WINDOWS:
        result.loc[index, percentile_column(field, window)] = float("nan")
    result.loc[index, percentile_column(field, ALL_HISTORY_WINDOW)] = float("nan")


def _clean_values(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").astype("float64")
    return values.where(values != 0.0)


def _assert_percentile_series_equal(left: pd.Series, right: pd.Series) -> None:
    pd.testing.assert_series_equal(
        pd.to_numeric(left, errors="coerce"),
        pd.to_numeric(right, errors="coerce"),
        check_names=False,
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
