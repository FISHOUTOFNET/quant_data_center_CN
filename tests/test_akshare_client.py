from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.api.akshare_client import (
    AkShareCircuitOpen,
    AkShareClient,
    AkShareNetworkError,
    akshare_symbol_to_project_code,
    normalize_akshare_code,
)


class FakeConfig:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self._values = {
            "api.akshare.max_retries": 3,
            "api.akshare.jitter_seconds": [0, 0],
            "api.akshare.endpoints.stock_value_em.failure_threshold": 5,
            "api.akshare.endpoints.stock_value_em.cooldown_minutes": 30,
            **(values or {}),
        }

    def get(self, dotted_key: str, default=None):
        return self._values.get(dotted_key, default)


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


def test_akshare_client_handles_none_type_subscript_error(stock_basic_sample) -> None:
    class FakeAk:
        __version__ = "fake-1"

        def stock_value_em(self, symbol: str) -> pd.DataFrame:
            raise TypeError("'NoneType' object is not subscriptable")

    client = AkShareClient(config=FakeConfig(), stock_basic_df=stock_basic_sample(), ak_module=FakeAk())

    df = client.query_stock_value("sh.600000")

    assert df.empty
    assert list(df.columns) == [
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


def test_akshare_client_normalizes_a_stock_endpoints(stock_basic_sample) -> None:
    calls: dict[str, object] = {}

    class FakeAk:
        __version__ = "fake-2"

        def stock_info_sh_delist(self, symbol: str) -> pd.DataFrame:
            calls["delist_symbol"] = symbol
            return pd.DataFrame(
                [
                    {
                        "公司代码": "600001",
                        "公司简称": "Old Corp",
                        "上市日期": "2000-01-01",
                        "暂停上市日期": "2024-01-02",
                    }
                ]
            )

        def stock_zh_a_spot_em(self) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "代码": "600000",
                        "名称": "PF Bank",
                        "最新价": 8.3,
                        "涨跌额": 0.1,
                        "涨跌幅": 1.2,
                        "今开": 8.2,
                        "最高": 8.4,
                        "最低": 8.1,
                        "昨收": 8.2,
                        "成交量": 10,
                        "成交额": 9960.0,
                        "换手率": 0.12,
                        "振幅": 3.0,
                        "市盈率-动态": 5.1,
                        "市净率": 0.71,
                        "总市值": 101000000.0,
                        "流通市值": 81000000.0,
                    },
                    {
                        "代码": "430017",
                        "名称": "BJ Corp",
                        "最新价": 3.0,
                        "涨跌额": 0.0,
                        "涨跌幅": 0.0,
                        "今开": 3.0,
                        "最高": 3.1,
                        "最低": 2.9,
                        "昨收": 3.0,
                        "成交量": 5,
                        "成交额": 1500.0,
                        "换手率": 0.1,
                        "振幅": 1.0,
                        "市盈率-动态": 12.0,
                        "市净率": 1.1,
                        "总市值": 1000.0,
                        "流通市值": 900.0,
                    },
                ]
            )

        def stock_zh_a_spot(self) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "代码": "sh600000",
                        "名称": "PF Bank",
                        "最新价": 8.3,
                        "涨跌额": 0.1,
                        "涨跌幅": 1.2,
                        "买入": 8.29,
                        "卖出": 8.31,
                        "昨收": 8.2,
                        "今开": 8.2,
                        "最高": 8.4,
                        "最低": 8.1,
                        "成交量": 10,
                        "成交额": 83.0,
                        "时间戳": "15:00:00",
                    }
                ]
            )

        def stock_zh_a_hist(self, symbol: str, period: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
            calls["hist"] = (symbol, period, start_date, end_date, adjust)
            return pd.DataFrame(
                [
                    {
                        "日期": "2024-01-03",
                        "股票代码": "000001",
                        "开盘": 8.2,
                        "收盘": 8.3,
                        "最高": 8.4,
                        "最低": 8.1,
                        "成交量": 10,
                        "成交额": 9960.0,
                        "振幅": 3.0,
                        "涨跌幅": 1.2,
                        "涨跌额": 0.1,
                        "换手率": 0.12,
                    }
                ]
            )

    client = AkShareClient(
        config=FakeConfig({"api.akshare.max_retries": 1}),
        stock_basic_df=stock_basic_sample(),
        ak_module=FakeAk(),
        now=lambda: datetime(2024, 1, 3, 16, 0),
    )

    delist = client.fetch_stock_info_sh_delist(symbol="全部", snapshot_date="2024-01-03").data
    spot_em = client.fetch_stock_zh_a_spot_em(trade_date="2024-01-03").data
    spot_sina = client.fetch_stock_zh_a_spot_sina(trade_date="2024-01-03", fallback_reason="planned").data
    hist = client.fetch_stock_zh_a_hist("sz.000001", "2024-01-01", "2024-01-03", "none").data

    assert calls["delist_symbol"] == "全部"
    assert delist.loc[0, "code"] == "600001"
    assert spot_em.loc[0, "code"] == "600000"
    assert spot_em.loc[0, "volume"] == 1000
    assert spot_em.loc[1, "code"] == "430017"
    assert spot_sina.loc[0, "source_symbol"] == "sh600000"
    assert spot_sina.loc[0, "volume"] == 10
    assert bool(spot_sina.loc[0, "is_fallback"])
    assert calls["hist"] == ("000001", "daily", "20240101", "20240103", "")
    assert hist.loc[0, "code"] == "000001"
    assert hist.loc[0, "volume"] == 1000
    assert hist.loc[0, "quality_status"] == "hist_confirmed"


def test_akshare_code_normalizers_return_six_digits() -> None:
    assert akshare_symbol_to_project_code("bj430017") == "430017"
    assert akshare_symbol_to_project_code("430017") == "430017"
    assert akshare_symbol_to_project_code("600000") == "600000"
    assert normalize_akshare_code("sh.600000") == "600000"
    assert normalize_akshare_code("sh600000") == "600000"
    assert normalize_akshare_code("600000.0") == "600000"


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
