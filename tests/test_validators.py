from __future__ import annotations

import pandas as pd
import pytest

from src.quality.validators import (
    ValidationError,
    validate_adjust_factor,
    validate_daily_k,
    validate_non_negative,
    validate_stock_institute_hold,
    validate_stock_value_em,
)


def test_validate_daily_k_accepts_valid_data(daily_sample) -> None:
    validate_daily_k(daily_sample())


def test_validate_daily_k_rejects_duplicate_code_date(daily_sample) -> None:
    df = daily_sample()
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_daily_k(df)


def test_validate_daily_k_warns_on_invalid_ohlc(daily_sample) -> None:
    df = daily_sample()
    df.loc[0, "close"] = 99.0
    validate_daily_k(df)


def test_validate_daily_k_accepts_ohlc_within_tolerance(daily_sample) -> None:
    df = daily_sample()
    df.loc[0, "close"] = df.loc[0, "high"] * (1 + 1e-5)
    validate_daily_k(df)


def test_validate_non_negative_accepts_valid_data(daily_sample) -> None:
    df = daily_sample()
    validate_non_negative(df, "volume")
    validate_non_negative(df, "amount")


def test_validate_non_negative_warns_on_null_values(daily_sample) -> None:
    df = daily_sample()
    df.loc[0, "volume"] = None
    validate_non_negative(df, "volume")


def test_validate_non_negative_warns_on_non_numeric_values(daily_sample) -> None:
    df = daily_sample()
    df["volume"] = df["volume"].astype(object)
    df.loc[0, "volume"] = "invalid"
    validate_non_negative(df, "volume")


def test_validate_non_negative_warns_on_negative_values(daily_sample) -> None:
    df = daily_sample()
    df.loc[0, "volume"] = -1000000
    validate_non_negative(df, "volume")


def test_validate_adjust_factor_rejects_duplicate_code_date(adjust_factor_sample) -> None:
    df = pd.concat([adjust_factor_sample(), adjust_factor_sample()], ignore_index=True)
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_adjust_factor(df)


def test_validate_stock_institute_hold_rejects_duplicate_period_code(stock_institute_hold_sample) -> None:
    df = stock_institute_hold_sample()
    df.loc[1, "code"] = df.loc[0, "code"]
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_stock_institute_hold(df)


def test_validate_stock_value_em_rejects_duplicate_code_date(stock_value_em_sample) -> None:
    df = stock_value_em_sample()
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_stock_value_em(df)


def test_validate_stock_value_em_rejects_non_monotonic_dates(stock_value_em_sample) -> None:
    df = stock_value_em_sample()
    df = df.iloc[::-1].reset_index(drop=True)
    with pytest.raises(ValidationError, match="monotonically"):
        validate_stock_value_em(df)
