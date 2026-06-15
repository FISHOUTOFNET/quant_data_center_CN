from __future__ import annotations

import time
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.sources.akshare.client import AkShareCircuitOpen, AkShareNetworkError
from src.sources.akshare.core.runtime import AkShareRuntime
import src.sources.akshare.pipeline.execution as akshare_execution
from src.sources.akshare.pipeline.execution_types import AkShareUpdateRequest, ConcurrencyPolicy, FetchResult
from src.utils.config_mgr import ConfigManager


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


def test_update_akshare_uses_direct_network_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text("pipeline:\n  metadata_flush_size: 1\n", encoding="utf-8")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example")
    monkeypatch.setenv("ALL_PROXY", "socks://proxy.example")
    monkeypatch.delenv("QDC_NETWORK_PROFILE", raising=False)
    observed: dict[str, str | None] = {}

    class FakeClient:
        def close(self) -> None:
            return None

    class FakeModule:
        target = "fake"

        def plan(self, request, context):
            return ["task"]

        def prefilter(self, tasks, context):
            return tasks

        def fetch(self, task, context):
            observed["HTTPS_PROXY"] = os.environ.get("HTTPS_PROXY")
            observed["ALL_PROXY"] = os.environ.get("ALL_PROXY")
            observed["QDC_NETWORK_PROFILE"] = os.environ.get("QDC_NETWORK_PROFILE")
            return FetchResult(task=task, started_at=datetime(2024, 1, 1), ended_at=datetime(2024, 1, 1))

        def record_result(self, result, context):
            return [{"status": "success"}]

        def record_skip(self, task, context, status="skipped_checkpoint", reason="checkpoint"):
            return [{"status": status, "reason": reason}]

        def progress_row(self, task, rows):
            return {"task": task, "status": rows[0]["status"]}

        def concurrency(self, request, context):
            return ConcurrencyPolicy(workers=1)

    monkeypatch.setattr(akshare_execution, "validate_request_target_options", lambda request: None)
    monkeypatch.setattr(akshare_execution, "modules_for_target", lambda target: [FakeModule()])

    records = akshare_execution.update_akshare(
        AkShareUpdateRequest(
            target="fake",
            root=tmp_path,
            build_views=False,
            client_factory=lambda config: FakeClient(),
        )
    )

    assert records == [{"status": "success"}]
    assert observed == {
        "HTTPS_PROXY": None,
        "ALL_PROXY": None,
        "QDC_NETWORK_PROFILE": "direct",
    }
    assert os.environ["HTTPS_PROXY"] == "http://proxy.example"
    assert os.environ["ALL_PROXY"] == "socks://proxy.example"


def test_akshare_runtime_endpoint_jitter_override_disables_stock_value_sleep() -> None:
    sleep_calls = []
    runtime = AkShareRuntime(
        config=FakeConfig(
            {
                "api.akshare.max_retries": 1,
                "api.akshare.jitter_seconds": [10, 10],
                "api.akshare.endpoints.stock_value_em.jitter_seconds": [0, 0],
            }
        ),
        ak_module=FakeAk(),
        sleep=sleep_calls.append,
        random_uniform=lambda low, high: high,
    )

    response = runtime.fetch("stock_value_em", {}, lambda: pd.DataFrame([{"value": 1}]), lambda df: df)

    assert response.endpoint == "stock_value_em"
    assert sleep_calls == []


def test_akshare_runtime_endpoint_without_jitter_override_uses_global_jitter() -> None:
    sleep_calls = []
    runtime = AkShareRuntime(
        config=FakeConfig(
            {
                "api.akshare.max_retries": 1,
                "api.akshare.jitter_seconds": [10, 10],
            }
        ),
        ak_module=FakeAk(),
        sleep=sleep_calls.append,
        random_uniform=lambda low, high: high,
    )

    response = runtime.fetch("stock_zh_a_hist", {}, lambda: pd.DataFrame([{"value": 1}]), lambda df: df)

    assert response.endpoint == "stock_zh_a_hist"
    assert sleep_calls == [10]


def test_akshare_runtime_dataset_named_jitter_key_does_not_affect_endpoint() -> None:
    sleep_calls = []
    runtime = AkShareRuntime(
        config=FakeConfig(
            {
                "api.akshare.max_retries": 1,
                "api.akshare.jitter_seconds": [10, 10],
                "api.akshare.endpoints.akshare_cn_stock_valuation_eastmoney.jitter_seconds": [0, 0],
            }
        ),
        ak_module=FakeAk(),
        sleep=sleep_calls.append,
        random_uniform=lambda low, high: high,
    )

    response = runtime.fetch("stock_value_em", {}, lambda: pd.DataFrame([{"value": 1}]), lambda df: df)

    assert response.endpoint == "stock_value_em"
    assert sleep_calls == [10]


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


def test_default_akshare_settings_have_stock_value_endpoint_jitter_override() -> None:
    config = ConfigManager(root=Path(__file__).resolve().parents[1])

    assert config.get("api.akshare.jitter_seconds") == [1, 5]
    assert config.get("api.akshare.endpoints.stock_value_em.jitter_seconds") == [0, 0]
    assert config.get("api.akshare.endpoints.stock_value_em.failure_threshold") == 5
    assert config.get("api.akshare.endpoints.stock_value_em.cooldown_minutes") == 30
    assert config.get("api.akshare.endpoints.akshare_cn_stock_valuation_eastmoney.jitter_seconds") is None
    assert config.get("api.akshare.endpoints.akshare_cn_stock_valuation_eastmoney.failure_threshold") is None
