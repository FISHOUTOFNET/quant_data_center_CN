from __future__ import annotations

import pandas as pd
import pytest

from src.api import baostock_client
from src.api.baostock_client import BaostockClient, BaostockConnectionError, BaostockError, BaostockTimeoutError


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
        fields = [
            "code",
            "dividend_operate_date",
            "forward_adjust_factor",
            "backward_adjust_factor",
            "adjustment_factor",
        ]

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


def test_result_to_dataframe_rejects_unbounded_next_loop(monkeypatch) -> None:
    class InfiniteResult:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]

        def next(self) -> bool:
            return True

        def get_row_data(self) -> list[str]:
            return ["2024-01-02", "1"]

    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", lambda **kwargs: InfiniteResult())

    client = BaostockClient(max_attempts=1, max_rows_per_result=2)
    client.logged_in = True

    with pytest.raises(BaostockError, match="exceeded max_rows_per_result=2"):
        client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")


def test_result_to_dataframe_rejects_malformed_rows(monkeypatch) -> None:
    class MalformedResult:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]
        calls = 0

        def next(self) -> bool:
            self.calls += 1
            return self.calls == 1

        def get_row_data(self) -> list[str]:
            return ["2024-01-02"]

    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", lambda **kwargs: MalformedResult())

    client = BaostockClient(max_attempts=1, max_rows_per_result=10)
    client.logged_in = True

    with pytest.raises(BaostockError, match="returned 1 columns; expected 2"):
        client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")


def test_result_to_dataframe_rejects_silent_full_page_stop(monkeypatch) -> None:
    class FullPageResult:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]
        data = [["2024-01-02", "1"], ["2024-01-03", "1"]]
        per_page_count = 2
        cur_row_num = 0

        def next(self) -> bool:
            return self.cur_row_num < len(self.data)

        def get_row_data(self) -> list[str]:
            row = self.data[self.cur_row_num]
            self.cur_row_num += 1
            return row

    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", lambda **kwargs: FullPageResult())

    client = BaostockClient(max_attempts=1, max_rows_per_result=10)
    client.logged_in = True

    with pytest.raises(BaostockError, match="stopped after a full page"):
        client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")


def test_baostock_client_sets_default_socket_timeout(monkeypatch) -> None:
    timeouts: list[float] = []

    class FakeSocket:
        def settimeout(self, value: float) -> None:
            timeouts.append(value)

    class LoginResult:
        error_code = "0"
        error_msg = ""

    monkeypatch.setattr(baostock_client.bs, "login", lambda: LoginResult())
    monkeypatch.setattr(baostock_client.baostock_context, "default_socket", FakeSocket(), raising=False)

    client = BaostockClient(timeout_seconds=60)
    client.login()

    assert timeouts
    assert set(timeouts) == {60}


def test_baostock_timeout_errors_are_retried(monkeypatch) -> None:
    calls = {"count": 0}

    class SocketTimeoutError(TimeoutError):
        pass

    class SuccessfulResult:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]

        def next(self) -> bool:
            return False

        def get_row_data(self) -> list[str]:
            return []

    def fake_query_trade_dates(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise SocketTimeoutError("timed out")
        return SuccessfulResult()

    class LoginResult:
        error_code = "0"
        error_msg = ""

    monkeypatch.setattr(baostock_client.bs, "login", lambda: LoginResult())
    monkeypatch.setattr(baostock_client.socket, "timeout", SocketTimeoutError)
    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", fake_query_trade_dates)

    client = BaostockClient(max_attempts=2, timeout_seconds=60)
    client.logged_in = True

    result = client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")

    assert calls["count"] == 2
    assert result.empty


def test_baostock_timeout_error_raised_after_attempts_exhausted(monkeypatch) -> None:
    class SocketTimeoutError(TimeoutError):
        pass

    monkeypatch.setattr(baostock_client.socket, "timeout", SocketTimeoutError)
    monkeypatch.setattr(
        baostock_client.bs,
        "query_trade_dates",
        lambda **kwargs: (_ for _ in ()).throw(SocketTimeoutError("timed out")),
    )

    client = BaostockClient(max_attempts=1, timeout_seconds=60)
    client.logged_in = True

    with pytest.raises(BaostockTimeoutError, match="query_trade_dates timed out"):
        client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")


def test_baostock_safe_send_rejects_empty_socket_recv(monkeypatch) -> None:
    class EmptyRecvSocket:
        def settimeout(self, value: float) -> None:
            return None

        def send(self, payload: bytes) -> int:
            return len(payload)

        def recv(self, size: int) -> bytes:
            return b""

    monkeypatch.setattr(baostock_client.baostock_context, "default_socket", EmptyRecvSocket(), raising=False)

    with pytest.raises(BaostockConnectionError, match="closed connection"):
        baostock_client._safe_baostock_send_msg("request", timeout_seconds=0.1)


def test_baostock_safe_send_decodes_uncompressed_response(monkeypatch) -> None:
    response = b"00.9.10\x0134\x0100000000050\x01ok\n<![CDATA[]]>\n"

    class ResponseSocket:
        def settimeout(self, value: float) -> None:
            return None

        def send(self, payload: bytes) -> int:
            return len(payload)

        def recv(self, size: int) -> bytes:
            return response

    monkeypatch.setattr(baostock_client.baostock_context, "default_socket", ResponseSocket(), raising=False)

    assert baostock_client._safe_baostock_send_msg("request", timeout_seconds=0.1) == response.decode()


def test_baostock_network_error_invalidates_session_and_relogs_before_retry(monkeypatch) -> None:
    calls: list[str] = []

    class LoginResult:
        error_code = "0"
        error_msg = ""

    class SuccessfulResult:
        error_code = "0"
        error_msg = ""
        fields = ["calendar_date", "is_trading_day"]

        def next(self) -> bool:
            return False

        def get_row_data(self) -> list[str]:
            return []

    def fake_login():
        calls.append("login")
        return LoginResult()

    def fake_query_trade_dates(**kwargs):
        calls.append("query")
        if calls.count("query") == 1:
            raise BaostockConnectionError("connection dropped")
        return SuccessfulResult()

    class ClosingSocket:
        def __init__(self) -> None:
            self.closed = False

        def settimeout(self, value: float) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    socket_obj = ClosingSocket()
    monkeypatch.setattr(baostock_client.bs, "login", fake_login)
    monkeypatch.setattr(baostock_client.bs, "query_trade_dates", fake_query_trade_dates)
    monkeypatch.setattr(baostock_client.baostock_context, "default_socket", socket_obj, raising=False)

    client = BaostockClient(max_attempts=2, timeout_seconds=0.1)
    client.logged_in = True

    result = client.query_trade_dates(start_date="2024-01-01", end_date="2024-01-31")

    assert result.empty
    assert socket_obj.closed
    assert calls == ["query", "login", "query"]
