"""Valuation percentile calculations for local Baostock daily-bar data."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import deque
from collections.abc import Iterable
from math import isfinite

import numpy as np
import pandas as pd


VALUATION_FIELDS = ("pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm")
ROLLING_WINDOWS = (("1y", 1), ("3y", 3), ("5y", 5), ("10y", 10))
ALL_HISTORY_WINDOW = "all_history"


class _FenwickTree:
    """Count values by compressed rank with O(log n) updates and prefix sums."""

    def __init__(self, size: int) -> None:
        self._tree = [0] * (size + 1)
        self.total = 0

    def add(self, rank: int, delta: int) -> None:
        self.total += delta
        index = rank + 1
        while index < len(self._tree):
            self._tree[index] += delta
            index += index & -index

    def prefix_count(self, rank_count: int) -> int:
        total = 0
        index = rank_count
        while index > 0:
            total += self._tree[index]
            index -= index & -index
        return total


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
    row_count = len(values)
    outputs = {window: np.full(row_count, np.nan, dtype="float64") for window, _ in ROLLING_WINDOWS}
    outputs[ALL_HISTORY_WINDOW] = np.full(row_count, np.nan, dtype="float64")

    date_values = dates.to_numpy(dtype="datetime64[ns]")
    numeric_values = values.to_numpy(dtype="float64", na_value=np.nan)
    valid_mask = ~pd.isna(date_values) & ~np.isnan(numeric_values)
    if not bool(valid_mask.any()):
        _assign_field_outputs(result, field, outputs)
        return

    unique_values = np.array(sorted(set(float(value) for value in numeric_values[valid_mask])), dtype="float64")
    value_ranks = {float(value): rank for rank, value in enumerate(unique_values)}
    zero_left_rank = int(np.searchsorted(unique_values, 0.0, side="left"))
    zero_right_rank = int(np.searchsorted(unique_values, 0.0, side="right"))
    compressed_size = len(unique_values)

    date_ordinals = date_values.astype("int64")
    date_series = pd.Series(dates, copy=False)
    cutoff_ordinals = {
        window: (date_series - pd.DateOffset(years=years)).to_numpy(dtype="datetime64[ns]").astype("int64")
        for window, years in ROLLING_WINDOWS
    }
    first_valid_ordinal = np.datetime64(dates.loc[valid_mask].min()).astype("datetime64[ns]").astype("int64")

    all_history = _FenwickTree(compressed_size)
    rolling_states = {
        window: (_FenwickTree(compressed_size), deque())
        for window, _ in ROLLING_WINDOWS
    }

    for index in range(row_count):
        if not valid_mask[index]:
            continue

        current_value = float(numeric_values[index])
        current_rank = value_ranks[current_value]
        all_history.add(current_rank, 1)
        for window, _ in ROLLING_WINDOWS:
            state, rows = rolling_states[window]
            state.add(current_rank, 1)
            rows.append((index, current_rank))
            cutoff_ordinal = cutoff_ordinals[window][index]
            while rows and date_ordinals[rows[0][0]] < cutoff_ordinal:
                _, expired_rank = rows.popleft()
                state.add(expired_rank, -1)
            if first_valid_ordinal <= cutoff_ordinal:
                outputs[window][index] = _percentile_from_counts(
                    current_value,
                    current_rank,
                    state,
                    zero_left_rank,
                    zero_right_rank,
                )

        outputs[ALL_HISTORY_WINDOW][index] = _percentile_from_counts(
            current_value,
            current_rank,
            all_history,
            zero_left_rank,
            zero_right_rank,
        )

    _assign_field_outputs(result, field, outputs)


def _assign_empty_percentiles(result: pd.DataFrame, index: int, field: str) -> None:
    for window, _ in ROLLING_WINDOWS:
        result.loc[index, percentile_column(field, window)] = pd.NA
    result.loc[index, percentile_column(field, ALL_HISTORY_WINDOW)] = pd.NA


def _assign_field_outputs(result: pd.DataFrame, field: str, outputs: dict[str, np.ndarray]) -> None:
    for window, _ in ROLLING_WINDOWS:
        result[percentile_column(field, window)] = outputs[window]
    result[percentile_column(field, ALL_HISTORY_WINDOW)] = outputs[ALL_HISTORY_WINDOW]


def _percentile_from_counts(
    current_value: float,
    current_rank: int,
    counts: _FenwickTree,
    zero_left_rank: int,
    zero_right_rank: int,
) -> float:
    total = counts.total
    if total <= 0:
        return float("nan")
    negative_count = counts.prefix_count(zero_left_rank)
    if negative_count and current_value > 0:
        positive_count = total - counts.prefix_count(zero_right_rank)
        if positive_count == 0:
            return float("nan")
        less_equal_positive = counts.prefix_count(current_rank + 1) - counts.prefix_count(zero_right_rank)
        return less_equal_positive / positive_count * 100.0
    if current_value < 0:
        positive_count = total - counts.prefix_count(zero_right_rank)
        negative_greater_equal = negative_count - counts.prefix_count(current_rank)
        return (positive_count + negative_greater_equal) / total * 100.0
    return counts.prefix_count(current_rank + 1) / total * 100.0


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
