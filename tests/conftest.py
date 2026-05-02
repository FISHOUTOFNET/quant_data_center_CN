from __future__ import annotations

from datetime import date

import pandas as pd
import pytest


@pytest.fixture
def daily_sample():
    return _daily_sample


@pytest.fixture
def stock_basic_sample():
    return _stock_basic_sample


@pytest.fixture
def adjust_factor_sample():
    return _adjust_factor_sample


@pytest.fixture
def stock_institute_hold_sample():
    return _stock_institute_hold_sample


@pytest.fixture
def stock_value_em_sample():
    return _stock_value_em_sample


def _daily_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "sh.600000",
                "open": 8.1,
                "high": 8.3,
                "low": 8.0,
                "close": 8.2,
                "preclose": 8.0,
                "volume": 1000,
                "amount": 8200.0,
                "adjustflag": "2",
                "turn": 0.1,
                "tradestatus": "1",
                "pctChg": 2.5,
                "peTTM": 5.0,
                "pbMRQ": 0.7,
                "psTTM": 1.2,
                "pcfNcfTTM": 3.0,
                "isST": "0",
            },
            {
                "date": date(2024, 1, 3),
                "code": "sh.600000",
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "close": 8.3,
                "preclose": 8.2,
                "volume": 1200,
                "amount": 9960.0,
                "adjustflag": "2",
                "turn": 0.12,
                "tradestatus": "1",
                "pctChg": 1.2,
                "peTTM": 5.1,
                "pbMRQ": 0.71,
                "psTTM": 1.21,
                "pcfNcfTTM": 3.1,
                "isST": "0",
            },
        ]
    )


def _stock_basic_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "code_name": "PF Bank",
                "ipoDate": date(1999, 11, 10),
                "outDate": None,
                "type": "1",
                "status": "1",
            },
            {
                "code": "sz.000001",
                "code_name": "PA Bank",
                "ipoDate": date(1991, 4, 3),
                "outDate": None,
                "type": "1",
                "status": "0",
            },
            {
                "code": "sh.000001",
                "code_name": "SSE Composite",
                "ipoDate": date(1991, 7, 15),
                "outDate": None,
                "type": "2",
                "status": "1",
            },
        ]
    )


def _adjust_factor_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividOperateDate": date(2024, 1, 2),
                "foreAdjustFactor": 1.0,
                "backAdjustFactor": 1.0,
                "adjustFactor": 1.0,
            }
        ]
    )


def _stock_institute_hold_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "report_period": "2024Q1",
                "period_end_date": date(2024, 3, 31),
                "code": "600000",
                "code_name": "PF Bank",
                "institution_count": 3,
                "institution_count_change": 1,
                "holding_ratio": 2.1,
                "holding_ratio_change": 0.2,
                "float_holding_ratio": 3.1,
                "float_holding_ratio_change": 0.3,
            },
            {
                "report_period": "2024Q1",
                "period_end_date": date(2024, 3, 31),
                "code": "000001",
                "code_name": "PA Bank",
                "institution_count": 2,
                "institution_count_change": 0,
                "holding_ratio": 1.1,
                "holding_ratio_change": 0.0,
                "float_holding_ratio": 1.5,
                "float_holding_ratio_change": 0.1,
            },
        ]
    )


def _stock_value_em_sample(code: str = "600000") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": code,
                "close": 8.2,
                "pct_chg": 2.5,
                "total_market_cap": 100000000.0,
                "float_market_cap": 80000000.0,
                "total_shares": 12000000.0,
                "float_shares": 10000000.0,
                "pe_ttm": 5.0,
                "pe_static": 5.5,
                "pb": 0.7,
                "peg": 0.8,
                "pcf": 3.0,
                "ps": 1.2,
            },
            {
                "date": date(2024, 1, 3),
                "code": code,
                "close": 8.3,
                "pct_chg": 1.2,
                "total_market_cap": 101000000.0,
                "float_market_cap": 81000000.0,
                "total_shares": 12000000.0,
                "float_shares": 10000000.0,
                "pe_ttm": 5.1,
                "pe_static": 5.6,
                "pb": 0.71,
                "peg": 0.81,
                "pcf": 3.1,
                "ps": 1.21,
            },
        ]
    )
