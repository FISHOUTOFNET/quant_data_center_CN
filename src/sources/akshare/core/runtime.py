"""Runtime policy for AkShare endpoint calls."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, cast

import pandas as pd

from src.sources.akshare.core.errors import (
    AkShareCircuitOpen,
    AkShareEmptyDataError,
    AkShareNetworkError,
    AkShareSchemaDriftError,
)
from src.sources.akshare.core.models import AkShareResponse
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


@dataclass
class _EndpointState:
    consecutive_failures: int = 0
    circuit_open_until: datetime | None = None


@dataclass(frozen=True)
class _EndpointRuntimeConfig:
    max_retries: int
    jitter_seconds: tuple[float, float]
    failure_threshold: int
    cooldown: timedelta
    call_timeout_seconds: float


class AkShareRuntime:
    """Shared timeout, retry, jitter, and circuit policy for AkShare endpoints."""

    def __init__(
        self,
        config: ConfigManager | None = None,
        ak_module: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._ak_module = ak_module
        self._sleep = sleep
        self._random_uniform = random_uniform or _default_random_uniform
        self._now = now or datetime.now
        self._states: dict[str, _EndpointState] = {}
        self._state_lock = RLock()
        self._ak_lock = RLock()
        self._timeout_executor: ThreadPoolExecutor | None = None
        self._timeout_executor_lock = RLock()

    def close(self) -> None:
        with self._timeout_executor_lock:
            executor = self._timeout_executor
            self._timeout_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def ak(self) -> Any:
        if self._ak_module is None:
            with self._ak_lock:
                if self._ak_module is None:
                    try:
                        import akshare as ak  # type: ignore
                    except ModuleNotFoundError as exc:
                        raise AkShareNetworkError("akshare is not installed") from exc
                    self._ak_module = ak
        return self._ak_module

    def fetch(
        self,
        endpoint: str,
        params: dict[str, object],
        caller: Callable[[], object],
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> AkShareResponse:
        runtime = self._endpoint_runtime_config(endpoint)
        self._raise_if_circuit_open(endpoint)
        attempts = max(runtime.max_retries, 1)
        last_error: AkShareNetworkError | None = None

        for attempt in range(1, attempts + 1):
            self._sleep_jitter(runtime)
            try:
                source_df = _as_dataframe(self._call_with_timeout(endpoint, caller, runtime.call_timeout_seconds))
                normalized = normalizer(source_df)
                self._record_success(endpoint)
                return AkShareResponse(
                    endpoint=endpoint,
                    params=params,
                    akshare_version=self._akshare_version(),
                    data=normalized,
                )
            except (AkShareSchemaDriftError, AkShareEmptyDataError):
                self._record_failure(endpoint, runtime)
                raise
            except AkShareNetworkError as exc:
                last_error = exc
                if attempt < attempts:
                    logger.warning(
                        "AkShare endpoint={} attempt={}/{} failed: {}",
                        endpoint,
                        attempt,
                        attempts,
                        exc,
                    )
                    continue
                self._record_failure(endpoint, runtime)
            except TypeError as exc:
                if "'NoneType' object is not subscriptable" in str(exc):
                    source_df = pd.DataFrame()
                    normalized = normalizer(source_df)
                    self._record_success(endpoint)
                    return AkShareResponse(
                        endpoint=endpoint,
                        params=params,
                        akshare_version=self._akshare_version(),
                        data=normalized,
                    )
                last_error = AkShareNetworkError(f"{endpoint} failed on attempt {attempt}/{attempts}: {exc}")
                if attempt < attempts:
                    logger.warning(
                        "AkShare endpoint={} attempt={}/{} failed: {}",
                        endpoint,
                        attempt,
                        attempts,
                        exc,
                    )
                    continue
                self._record_failure(endpoint, runtime)
            except Exception as exc:
                last_error = AkShareNetworkError(f"{endpoint} failed on attempt {attempt}/{attempts}: {exc}")
                if attempt < attempts:
                    logger.warning(
                        "AkShare endpoint={} attempt={}/{} failed: {}",
                        endpoint,
                        attempt,
                        attempts,
                        exc,
                    )
                    continue
                self._record_failure(endpoint, runtime)

        if last_error is not None:
            raise last_error
        raise AkShareNetworkError(f"{endpoint} failed without a captured error")

    def _endpoint_runtime_config(self, endpoint: str) -> _EndpointRuntimeConfig:
        retries = int(
            _config_get(self._config, "api.akshare.max_retries", _config_get(self._config, "pipeline.max_retries", 3))
        )
        raw_jitter = _config_get(
            self._config,
            f"api.akshare.endpoints.{endpoint}.jitter_seconds",
            _config_get(self._config, "api.akshare.jitter_seconds", [0, 0]),
        )
        jitter = _parse_jitter(raw_jitter)
        default_threshold = 1 if endpoint == "stock_zh_a_spot" else 5
        default_cooldown_minutes = 180 if endpoint == "stock_zh_a_spot" else 30
        threshold = int(
            _config_get(
                self._config,
                f"api.akshare.endpoints.{endpoint}.failure_threshold",
                default_threshold,
            )
        )
        cooldown_minutes = float(
            _config_get(
                self._config,
                f"api.akshare.endpoints.{endpoint}.cooldown_minutes",
                default_cooldown_minutes,
            )
        )
        call_timeout_seconds = float(
            _config_get(
                self._config,
                f"api.akshare.endpoints.{endpoint}.call_timeout_seconds",
                _config_get(self._config, "api.akshare.call_timeout_seconds", 120),
            )
        )
        return _EndpointRuntimeConfig(
            max_retries=max(retries, 1),
            jitter_seconds=jitter,
            failure_threshold=max(threshold, 1),
            cooldown=timedelta(minutes=cooldown_minutes),
            call_timeout_seconds=max(call_timeout_seconds, 0.001),
        )

    def _raise_if_circuit_open(self, endpoint: str) -> None:
        with self._state_lock:
            state = self._states.setdefault(endpoint, _EndpointState())
            if state.circuit_open_until is None:
                return
            now = self._now()
            if now < state.circuit_open_until:
                raise AkShareCircuitOpen(
                    f"AkShare endpoint {endpoint} circuit is open until {state.circuit_open_until.isoformat()}"
                )
            state.circuit_open_until = None
            state.consecutive_failures = 0

    def _record_success(self, endpoint: str) -> None:
        with self._state_lock:
            self._states[endpoint] = _EndpointState()

    def _record_failure(self, endpoint: str, runtime: _EndpointRuntimeConfig) -> None:
        with self._state_lock:
            state = self._states.setdefault(endpoint, _EndpointState())
            state.consecutive_failures += 1
            if state.consecutive_failures >= runtime.failure_threshold:
                state.circuit_open_until = self._now() + runtime.cooldown
                logger.warning(
                    "AkShare endpoint={} circuit opened after {} consecutive failures until {}",
                    endpoint,
                    state.consecutive_failures,
                    state.circuit_open_until,
                )

    def _sleep_jitter(self, runtime: _EndpointRuntimeConfig) -> None:
        low, high = runtime.jitter_seconds
        if low <= 0 and high <= 0:
            return
        delay = max(self._random_uniform(low, high), 0.0)
        if delay > 0:
            self._sleep(delay)

    def _akshare_version(self) -> str:
        return str(getattr(self.ak(), "__version__", "unknown"))

    def _call_with_timeout(self, endpoint: str, caller: Callable[[], object], timeout_seconds: float) -> object:
        future = self._timeout_call_executor().submit(caller)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            raise AkShareNetworkError(f"{endpoint} timed out after {timeout_seconds:g}s") from exc

    def _timeout_call_executor(self) -> ThreadPoolExecutor:
        with self._timeout_executor_lock:
            if self._timeout_executor is None:
                self._timeout_executor = ThreadPoolExecutor(
                    max_workers=_resolve_timeout_workers(self._config),
                    thread_name_prefix="akshare-timeout",
                )
            return self._timeout_executor


def _default_random_uniform(low: float, high: float) -> float:
    import random

    return random.uniform(low, high)


def _config_get(config: ConfigManager | None, dotted_key: str, default: Any = None) -> Any:
    if config is None:
        return default
    getter = getattr(config, "get", None)
    if getter is None:
        return default
    return getter(dotted_key, default)


def _parse_jitter(raw_jitter: object) -> tuple[float, float]:
    if isinstance(raw_jitter, (list, tuple)) and len(raw_jitter) >= 2:
        low = _float_or_zero(raw_jitter[0])
        high = _float_or_zero(raw_jitter[1])
    else:
        low = high = _float_or_zero(raw_jitter)
    if high < low:
        low, high = high, low
    return low, high


def _as_dataframe(value: object) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(cast(Any, value))


def _float_or_zero(value: object) -> float:
    if isinstance(value, (str, bytes, int, float)):
        return float(value)
    return 0.0


def _resolve_timeout_workers(config: ConfigManager | None) -> int:
    return max(int(_config_get(config, "api.akshare.workers", 3)), 1)
