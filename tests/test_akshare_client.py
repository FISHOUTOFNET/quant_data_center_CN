from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from src.api.akshare_client import (
    AkShareCircuitOpen,
    AkShareClient,
    AkShareEmptyDataError,
    AkShareNetworkError,
    report_period_end_date,
)


class FakeConfig:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self._values = {
            "api.akshare.max_retries": 3,
            "api.akshare.jitter_seconds": [0, 0],
            "api.akshare.endpoints.stock_institute_hold.failure_threshold": 5,
            "api.akshare.endpoints.stock_institute_hold.cooldown_minutes": 30,
            "api.akshare.endpoints.stock_value_em.failure_threshold": 5,
            "api.akshare.endpoints.stock_value_em.cooldown_minutes": 30,
            **(values or {}),
        }

    def get(self, dotted_key: str, default=None):
        return self._values.get(dotted_key, default)


def test_akshare_client_maps_institute_fields_and_keeps_source_codes(stock_basic_sample) -> None:
    class FakeAk:
        __version__ = "fake-1"

        def stock_institute_hold(self, symbol: str) -> pd.DataFrame:
            assert symbol == "20241"
            return pd.DataFrame(
                [
                    {
                        "证券代码": 600000,
                        "证券简称": "PF Bank",
                        "机构数": 3,
                        "机构数变化": 1,
                        "持股比例": 2.1,
                        "持股比例增幅": 0.2,
                        "占流通股比例": 3.1,
                        "占流通股比例增幅": 0.3,
                    }
                ]
            )

    client = AkShareClient(config=FakeConfig(), stock_basic_df=stock_basic_sample(), ak_module=FakeAk())

    df = client.query_stock_institute_hold("2024Q1")

    assert df.loc[0, "report_period"] == "2024Q1"
    assert df.loc[0, "period_end_date"] == date(2024, 3, 31)
    assert df.loc[0, "code"] == "600000"
    assert df.loc[0, "institution_count"] == 3
    assert report_period_end_date("20241") == date(2024, 3, 31)


def test_akshare_client_maps_stock_value_and_stores_source_code(stock_basic_sample) -> None:
    calls: list[str] = []

    class FakeAk:
        __version__ = "fake-1"

        def stock_value_em(self, symbol: str) -> pd.DataFrame:
            calls.append(symbol)
            return pd.DataFrame(
                [
                    {
                        "数据日期": "2024-01-02",
                        "当日收盘价": 8.2,
                        "当日涨跌幅": 2.5,
                        "总市值": 100.0,
                        "流通市值": 80.0,
                        "总股本": 12.0,
                        "流通股本": 10.0,
                        "PE(TTM)": 5.0,
                        "PE(静)": 5.5,
                        "市净率": 0.7,
                        "PEG值": 0.8,
                        "市现率": 3.0,
                        "市销率": 1.2,
                    }
                ]
            )

    client = AkShareClient(config=FakeConfig(), stock_basic_df=stock_basic_sample(), ak_module=FakeAk())

    df = client.query_stock_value("sh.600000")

    assert calls == ["600000"]
    assert df.loc[0, "code"] == "600000"
    assert df.loc[0, "pe_ttm"] == 5.0


def test_akshare_client_handles_empty_data(stock_basic_sample) -> None:
    class FakeAk:
        __version__ = "fake-1"

        def stock_value_em(self, symbol: str) -> pd.DataFrame:
            return pd.DataFrame()

        def stock_institute_hold(self, symbol: str) -> pd.DataFrame:
            return pd.DataFrame()

    client = AkShareClient(config=FakeConfig(), stock_basic_df=stock_basic_sample(), ak_module=FakeAk())

    value_df = client.query_stock_value("sh.600000")
    assert value_df.empty
    assert list(value_df.columns) == [
        "date",
        "code",
        "close",
        "pct_chg",
        "total_market_cap",
        "float_market_cap",
        "total_shares",
        "float_shares",
        "pe_ttm",
        "pe_static",
        "pb",
        "peg",
        "pcf",
        "ps",
    ]
    with pytest.raises(AkShareEmptyDataError):
        client.query_stock_institute_hold("2024Q1")


def test_akshare_client_retries_failures(stock_basic_sample) -> None:
    calls = {"count": 0}

    class FakeAk:
        __version__ = "fake-1"

        def stock_value_em(self, symbol: str) -> pd.DataFrame:
            calls["count"] += 1
            if calls["count"] < 3:
                raise OSError("temporary")
            return _stock_value_raw()

    client = AkShareClient(config=FakeConfig(), stock_basic_df=stock_basic_sample(), ak_module=FakeAk())

    df = client.query_stock_value("sh.600000")

    assert len(df) == 1
    assert calls["count"] == 3


def test_akshare_client_opens_endpoint_circuit(stock_basic_sample) -> None:
    calls = {"count": 0}

    class FakeAk:
        __version__ = "fake-1"

        def stock_value_em(self, symbol: str) -> pd.DataFrame:
            calls["count"] += 1
            raise OSError("blocked")

    client = AkShareClient(
        config=FakeConfig(
            {
                "api.akshare.max_retries": 1,
                "api.akshare.endpoints.stock_value_em.failure_threshold": 2,
                "api.akshare.endpoints.stock_value_em.cooldown_minutes": 30,
            }
        ),
        stock_basic_df=stock_basic_sample(),
        ak_module=FakeAk(),
        now=lambda: datetime(2024, 1, 2, 10, 0),
    )

    with pytest.raises(AkShareNetworkError):
        client.query_stock_value("sh.600000")
    with pytest.raises(AkShareNetworkError):
        client.query_stock_value("sh.600000")
    with pytest.raises(AkShareCircuitOpen):
        client.query_stock_value("sh.600000")
    assert calls["count"] == 2


def _stock_value_raw() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "数据日期": "2024-01-02",
                "当日收盘价": 8.2,
                "当日涨跌幅": 2.5,
                "总市值": 100.0,
                "流通市值": 80.0,
                "总股本": 12.0,
                "流通股本": 10.0,
                "PE(TTM)": 5.0,
                "PE(静)": 5.5,
                "市净率": 0.7,
                "PEG值": 0.8,
                "市现率": 3.0,
                "市销率": 1.2,
            }
        ]
    )
