from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.analytics.valuation_percentile import compute_valuation_percentiles, valuation_percentile


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
