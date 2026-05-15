from __future__ import annotations

import pandas as pd
import pytest

from src.api import baostock_client
from src.api.baostock_client import BaostockClient, BaostockError


def test_query_trade_dates_passes_optional_range_to_baostock(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class FakeResult:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]

        def next(self) -> bool:
            return False

        def get_row_data(self) -> list[str]:
            return []

    def fake_query_trade_dates(**kwargs):
        captured.update(kwargs)
        return FakeResult()

    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", fake_query_trade_dates)

    client = BaostockClient()
    client.logged_in = True
    result = client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")

    assert captured == {"start_date": "2024-01-01", "end_date": "2024-01-31"}
    assert isinstance(result, pd.DataFrame)


def test_query_baostock_cn_stock_adjustment_factor_passes_range_to_baostock(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class FakeResult:
        error_code = "0"
        error_msg = ""
        fields = ["code", "dividend_operate_date", "forward_adjust_factor", "backward_adjust_factor", "adjustment_factor"]

        def next(self) -> bool:
            return False

        def get_row_data(self) -> list[str]:
            return []

    def fake_query_baostock_cn_stock_adjustment_factor(**kwargs):
        captured.update(kwargs)
        return FakeResult()

    monkeypatch.setattr(baostock_client.bs, "query_adjust_factor", fake_query_baostock_cn_stock_adjustment_factor)

    client = BaostockClient()
    client.logged_in = True
    result = client.query_baostock_cn_stock_adjustment_factor("sh.600000", "1990-01-01", "2024-01-31")

    assert captured == {"code": "sh.600000", "start_date": "1990-01-01", "end_date": "2024-01-31"}
    assert isinstance(result, pd.DataFrame)


def test_baostock_client_honors_configured_max_attempts(monkeypatch) -> None:
    calls = 0

    class FakeResult:
        error_code = "1"
        error_msg = "temporary failure"
        fields: list[str] = []

        def next(self) -> bool:
            return False

        def get_row_data(self) -> list[str]:
            return []

    def fake_query_trade_dates(**kwargs):
        nonlocal calls
        calls += 1
        return FakeResult()

    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", fake_query_trade_dates)

    client = BaostockClient(max_attempts=1)
    client.logged_in = True

    with pytest.raises(BaostockError, match="query_trade_dates failed: 1 temporary failure"):
        client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")

    assert calls == 1

