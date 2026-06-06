from __future__ import annotations

import time
from datetime import datetime

import pandas as pd
import pytest

from src.sources.akshare.client import AkShareCircuitOpen, AkShareNetworkError
from src.sources.akshare.core.runtime import AkShareRuntime


class FakeConfig:
    def __init__(self, values: dict[str, object] | None = None) -> None:
        self._values = {
            "api.akshare.max_retries": 3,
            "api.akshare.jitter_seconds": [0, 0],
            **(values or {}),
        }

    def get(self, dotted_key: str, default=None):
        return self._values.get(dotted_key, default)


class FakeAk:
    __version__ = "fake-1"


def test_akshare_runtime_retries_failures_and_returns_response() -> None:
    calls = {"count": 0}
    runtime = AkShareRuntime(config=FakeConfig(), ak_module=FakeAk())

    def caller() -> pd.DataFrame:
        calls["count"] += 1
        if calls["count"] < 3:
            raise OSError("temporary")
        return pd.DataFrame([{"value": 1}])

    response = runtime.fetch(
        endpoint="stock_value_em",
        params={"symbol": "600000"},
        caller=caller,
        normalizer=lambda df: df.assign(mapped=df["value"] + 1),
    )

    assert calls["count"] == 3
    assert response.endpoint == "stock_value_em"
    assert response.params == {"symbol": "600000"}
    assert response.akshare_version == "fake-1"
    assert response.data.loc[0, "mapped"] == 2


def test_akshare_runtime_endpoint_timeout_override_opens_circuit() -> None:
    calls = {"count": 0}
    runtime = AkShareRuntime(
        config=FakeConfig(
            {
                "api.akshare.max_retries": 1,
                "api.akshare.call_timeout_seconds": 1,
                "api.akshare.endpoints.stock_value_em.call_timeout_seconds": 0.01,
                "api.akshare.endpoints.stock_value_em.failure_threshold": 2,
                "api.akshare.endpoints.stock_value_em.cooldown_minutes": 30,
            }
        ),
        ak_module=FakeAk(),
        now=lambda: datetime(2024, 1, 2, 10, 0),
    )

    def caller() -> pd.DataFrame:
        calls["count"] += 1
        time.sleep(0.05)
        return pd.DataFrame([{"value": 1}])

    with pytest.raises(AkShareNetworkError, match=r"stock_value_em timed out after 0\.01s"):
        runtime.fetch("stock_value_em", {}, caller, lambda df: df)
    with pytest.raises(AkShareNetworkError, match=r"stock_value_em timed out after 0\.01s"):
        runtime.fetch("stock_value_em", {}, caller, lambda df: df)
    with pytest.raises(AkShareCircuitOpen):
        runtime.fetch("stock_value_em", {}, caller, lambda df: df)
    assert calls["count"] == 2
    runtime.close()


def test_akshare_runtime_close_shuts_down_timeout_executor(monkeypatch) -> None:
    from src.sources.akshare.core import runtime as runtime_module

    created = []
    original_executor = runtime_module.ThreadPoolExecutor

    class ObservingExecutor(original_executor):
        def __init__(self, *args, **kwargs):
            self.shutdown_calls = []
            created.append(self)
            super().__init__(*args, **kwargs)

        def shutdown(self, wait=True, *, cancel_futures=False):
            self.shutdown_calls.append({"wait": wait, "cancel_futures": cancel_futures})
            return super().shutdown(wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(runtime_module, "ThreadPoolExecutor", ObservingExecutor)
    runtime = AkShareRuntime(config=FakeConfig({"api.akshare.max_retries": 1}), ak_module=FakeAk())

    runtime.fetch("stock_value_em", {}, lambda: pd.DataFrame([{"value": 1}]), lambda df: df)

    assert len(created) == 1
    assert created[0].shutdown_calls == []

    runtime.close()

    assert created[0].shutdown_calls == [{"wait": False, "cancel_futures": True}]
