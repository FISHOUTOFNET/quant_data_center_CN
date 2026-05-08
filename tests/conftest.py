from __future__ import annotations

from datetime import date

import pandas as pd
import pytest


@pytest.fixture
def daily_sample():
    return _daily_sample


@pytest.fixture
def baostock_cn_stock_basic_sample():
    return _baostock_cn_stock_basic_sample


@pytest.fixture
def baostock_cn_stock_adjustment_factor_sample():
    return _baostock_cn_stock_adjustment_factor_sample


@pytest.fixture
def akshare_cn_stock_valuation_eastmoney_sample():
    return _akshare_cn_stock_valuation_eastmoney_sample


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
                "prev_close": 8.0,
                "volume": 1000,
                "amount": 8200.0,
                "adjust_flag": "2",
                "turnover_rate": 0.1,
                "trade_status": "1",
                "pct_change": 2.5,
                "pe_ttm": 5.0,
                "pb_mrq": 0.7,
                "ps_ttm": 1.2,
                "pcf_ncf_ttm": 3.0,
                "is_st": "0",
            },
            {
                "date": date(2024, 1, 3),
                "code": "sh.600000",
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "close": 8.3,
                "prev_close": 8.2,
                "volume": 1200,
                "amount": 9960.0,
                "adjust_flag": "2",
                "turnover_rate": 0.12,
                "trade_status": "1",
                "pct_change": 1.2,
                "pe_ttm": 5.1,
                "pb_mrq": 0.71,
                "ps_ttm": 1.21,
                "pcf_ncf_ttm": 3.1,
                "is_st": "0",
            },
        ]
    )


def _baostock_cn_stock_basic_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "name": "PF Bank",
                "ipo_date": date(1999, 11, 10),
                "delist_date": None,
                "security_type": "1",
                "listing_status": "1",
            },
            {
                "code": "sz.000001",
                "name": "PA Bank",
                "ipo_date": date(1991, 4, 3),
                "delist_date": None,
                "security_type": "1",
                "listing_status": "0",
            },
            {
                "code": "sh.000001",
                "name": "SSE Composite",
                "ipo_date": date(1991, 7, 15),
                "delist_date": None,
                "security_type": "2",
                "listing_status": "1",
            },
        ]
    )


def _baostock_cn_stock_adjustment_factor_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividend_operate_date": date(2024, 1, 2),
                "forward_adjust_factor": 1.0,
                "backward_adjust_factor": 1.0,
                "adjustment_factor": 1.0,
            }
        ]
    )


def _akshare_cn_stock_valuation_eastmoney_sample(code: str = "600000") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": code,
                "close": 8.2,
                "pct_change": 2.5,
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
                "pct_change": 1.2,
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

