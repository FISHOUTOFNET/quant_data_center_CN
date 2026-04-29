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
