from __future__ import annotations

import pandas as pd
import pytest

from src.quality.validators import (
    ValidationError,
    validate_adjust_factor,
    validate_daily_k,
    validate_non_negative,
    validate_stock_zh_a_hist,
    validate_stock_zh_a_spot_em,
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


def test_validate_akshare_a_stock_rejects_duplicate_spot_date_code() -> None:
    df = _spot_em_sample()
    df = pd.concat([df, df], ignore_index=True)
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_stock_zh_a_spot_em(df)


def test_validate_akshare_hist_rejects_duplicate_adjust_rows() -> None:
    df = _hist_sample()
    df = pd.concat([df, df], ignore_index=True)
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_stock_zh_a_hist(df)


def _spot_em_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2024-01-03",
                "code": "600000",
                "source_symbol": "600000",
                "name": "PF Bank",
                "latest_price": 8.3,
                "change_amount": 0.1,
                "pct_chg": 1.2,
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "preclose": 8.2,
                "volume": 120000.0,
                "amount": 9960.0,
                "turnover_rate": 0.12,
                "amplitude": 3.0,
                "pe_dynamic": 5.1,
                "pb": 0.71,
                "total_market_cap": 101000000.0,
                "float_market_cap": 81000000.0,
                "source_endpoint": "stock_zh_a_spot_em",
                "fetched_at": pd.Timestamp("2024-01-03 16:00:00"),
            }
        ]
    )


def _hist_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2024-01-03",
                "code": "600000",
                "source_symbol": "600000",
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "close": 8.3,
                "volume": 120000,
                "amount": 9960.0,
                "amplitude": 3.0,
                "pct_chg": 1.2,
                "change_amount": 0.1,
                "turnover_rate": 0.12,
                "adjust": "none",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "hist_confirmed",
                "fetched_at": pd.Timestamp("2024-01-03 16:00:00"),
            }
        ]
    )
