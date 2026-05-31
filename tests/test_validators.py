from __future__ import annotations

import pandas as pd
import pytest

from src.quality.validators import (
    ValidationError,
    validate_akshare_cn_stock_daily_bar,
    validate_akshare_cn_stock_spot_quote_eastmoney,
    validate_akshare_cn_stock_valuation_eastmoney,
    validate_baostock_cn_stock_adjustment_factor,
    validate_daily_bar,
    validate_non_negative,
)


def test_validate_daily_bar_accepts_valid_data(daily_sample) -> None:
    validate_daily_bar(daily_sample())


def test_validate_daily_bar_rejects_duplicate_code_date(daily_sample) -> None:
    df = daily_sample()
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_daily_bar(df)


def test_validate_daily_bar_warns_on_invalid_ohlc(daily_sample) -> None:
    df = daily_sample()
    df.loc[0, "close"] = 99.0
    validate_daily_bar(df)


def test_validate_daily_bar_accepts_ohlc_within_tolerance(daily_sample) -> None:
    df = daily_sample()
    df.loc[0, "close"] = df.loc[0, "high"] * (1 + 1e-5)
    validate_daily_bar(df)


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


def test_validate_baostock_cn_stock_adjustment_factor_rejects_duplicate_code_date(
    baostock_cn_stock_adjustment_factor_sample,
) -> None:
    df = pd.concat(
        [baostock_cn_stock_adjustment_factor_sample(), baostock_cn_stock_adjustment_factor_sample()], ignore_index=True
    )
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_baostock_cn_stock_adjustment_factor(df)


def test_validate_akshare_cn_stock_valuation_eastmoney_rejects_duplicate_code_date(
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    df = akshare_cn_stock_valuation_eastmoney_sample()
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_akshare_cn_stock_valuation_eastmoney(df)


def test_validate_akshare_cn_stock_valuation_eastmoney_rejects_non_monotonic_dates(
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    df = akshare_cn_stock_valuation_eastmoney_sample()
    df = df.iloc[::-1].reset_index(drop=True)
    with pytest.raises(ValidationError, match="monotonically"):
        validate_akshare_cn_stock_valuation_eastmoney(df)


def test_validate_akshare_a_stock_rejects_duplicate_spot_date_code() -> None:
    df = _spot_em_sample()
    df = pd.concat([df, df], ignore_index=True)
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_akshare_cn_stock_spot_quote_eastmoney(df)


def test_validate_akshare_daily_bar_rejects_duplicate_adjustment_rows() -> None:
    df = _hist_sample()
    df = pd.concat([df, df], ignore_index=True)
    with pytest.raises(ValidationError, match="Duplicate"):
        validate_akshare_cn_stock_daily_bar(df)


def _spot_em_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2024-01-03",
                "code": "600000",
                "source_symbol": "600000",
                "name": "PF Bank",
                "last_price": 8.3,
                "price_change": 0.1,
                "pct_change": 1.2,
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "prev_close": 8.2,
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
                "pct_change": 1.2,
                "price_change": 0.1,
                "turnover_rate": 0.12,
                "adjustment": "unadjusted",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "daily_bar_confirmed",
                "fetched_at": pd.Timestamp("2024-01-03 16:00:00"),
            }
        ]
    )
