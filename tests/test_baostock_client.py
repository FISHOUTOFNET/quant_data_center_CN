from __future__ import annotations

import pandas as pd

from src.api import baostock_client
from src.api.baostock_client import BaostockClient


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
