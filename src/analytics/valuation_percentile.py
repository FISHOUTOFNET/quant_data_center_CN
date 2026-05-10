"""Valuation percentile calculations for local Baostock daily-bar data."""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from math import isfinite

import pandas as pd


VALUATION_FIELDS = ("pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm")
ROLLING_WINDOWS = (("1y", 1), ("3y", 3), ("5y", 5), ("10y", 10))
ALL_HISTORY_WINDOW = "all_history"


@dataclass
class _WindowState:
    years: int
    values: list[float]
    rows: deque[tuple[pd.Timestamp, float]]

    def add(self, row_date: pd.Timestamp, value: float) -> None:
        self.rows.append((row_date, value))
        insort(self.values, value)

    def expire_before(self, cutoff: pd.Timestamp) -> None:
        while self.rows and self.rows[0][0] < cutoff:
            _, value = self.rows.popleft()
            index = bisect_left(self.values, value)
            if index < len(self.values):
                self.values.pop(index)


def valuation_percentile(current_value: object, history_values: Iterable[object]) -> float | None:
    """Return the valuation percentile using signed-history rules."""

    current = _valid_valuation(current_value)
    if current is None:
        return None

    history = [_valid_valuation(value) for value in history_values]
    values = [value for value in history if value is not None]
    if not values:
        return None

    positive_values = [value for value in values if value > 0]
    negative_values = [value for value in values if value < 0]
    if negative_values and current > 0:
        if not positive_values:
            return None
        return sum(value <= current for value in positive_values) / len(positive_values) * 100.0
    if current < 0:
        numerator = len(positive_values) + sum(value >= current for value in negative_values)
        return numerator / len(values) * 100.0
    return sum(value <= current for value in values) / len(values) * 100.0


def compute_valuation_percentiles(df: pd.DataFrame, start: str | None = None) -> pd.DataFrame:
    """Compute all configured valuation percentiles for one stock dataframe."""

    if df.empty:
        return _empty_result()

    work = df.copy()
    work["_input_order"] = range(len(work))
    work["_date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values(["code", "_date", "_input_order"]).reset_index(drop=True)

    result = pd.DataFrame({"date": work["date"], "code": work["code"].astype("string")})
    for field in VALUATION_FIELDS:
        result[field] = _clean_valuation_series(work[field] if field in work.columns else pd.Series(pd.NA, index=work.index))

    for field in VALUATION_FIELDS:
        _compute_field_percentiles(work["_date"], result[field], result, field)

    if start is not None:
        start_ts = pd.Timestamp(start)
        dates = pd.to_datetime(result["date"], errors="coerce")
        result = result.loc[dates >= start_ts].reset_index(drop=True)

    return result.reset_index(drop=True)


def percentile_column(field: str, window: str) -> str:
    return f"{field}_percentile_{window}"


def output_columns() -> list[str]:
    columns = ["date", "code", *VALUATION_FIELDS]
    for field in VALUATION_FIELDS:
        for window, _ in ROLLING_WINDOWS:
            columns.append(percentile_column(field, window))
        columns.append(percentile_column(field, ALL_HISTORY_WINDOW))
    return columns


def _compute_field_percentiles(
    dates: pd.Series,
    values: pd.Series,
    result: pd.DataFrame,
    field: str,
) -> None:
    valid_dates = dates.loc[values.notna()]
    first_valid_date = valid_dates.min() if not valid_dates.empty else None
    all_history_values: list[float] = []
    rolling_states = {
        window: _WindowState(years=years, values=[], rows=deque())
        for window, years in ROLLING_WINDOWS
    }

    for index, row_date in dates.items():
        current = values.loc[index]
        current_value = None if pd.isna(current) else float(current)
        if pd.isna(row_date) or current_value is None:
            _assign_empty_percentiles(result, index, field)
            continue

        insort(all_history_values, current_value)
        for window, state in rolling_states.items():
            state.add(row_date, current_value)
            cutoff = row_date - pd.DateOffset(years=state.years)
            state.expire_before(cutoff)
            if first_valid_date is not None and first_valid_date <= cutoff:
                result.loc[index, percentile_column(field, window)] = _percentile_from_sorted(current_value, state.values)
            else:
                result.loc[index, percentile_column(field, window)] = pd.NA

        result.loc[index, percentile_column(field, ALL_HISTORY_WINDOW)] = _percentile_from_sorted(
            current_value,
            all_history_values,
        )


def _assign_empty_percentiles(result: pd.DataFrame, index: int, field: str) -> None:
    for window, _ in ROLLING_WINDOWS:
        result.loc[index, percentile_column(field, window)] = pd.NA
    result.loc[index, percentile_column(field, ALL_HISTORY_WINDOW)] = pd.NA


def _percentile_from_sorted(current_value: float, sorted_values: list[float]) -> float | None:
    if not sorted_values:
        return None
    negative_count = bisect_left(sorted_values, 0.0)
    if negative_count and current_value > 0:
        positive_count = len(sorted_values) - bisect_right(sorted_values, 0.0)
        if positive_count == 0:
            return None
        less_equal_positive = bisect_right(sorted_values, current_value) - bisect_right(sorted_values, 0.0)
        return less_equal_positive / positive_count * 100.0
    if current_value < 0:
        positive_count = len(sorted_values) - bisect_right(sorted_values, 0.0)
        negative_greater_equal = negative_count - bisect_left(sorted_values, current_value)
        return (positive_count + negative_greater_equal) / len(sorted_values) * 100.0
    return bisect_right(sorted_values, current_value) / len(sorted_values) * 100.0


def _clean_valuation_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").astype("float64")
    return values.where(values != 0.0)


def _valid_valuation(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(number) or number == 0.0:
        return None
    return number


def _empty_result() -> pd.DataFrame:
    return pd.DataFrame(columns=output_columns())
