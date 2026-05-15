from __future__ import annotations

import pytest
from loguru import logger

from src.pipeline.finalization import _finalize_write_pipeline


class _FakeStore:
    def __init__(self, calls: list[str], close_error: Exception | None = None) -> None:
        self._calls = calls
        self._close_error = close_error

    def close(self) -> None:
        self._calls.append("close")
        if self._close_error is not None:
            raise self._close_error


class _FakeDuckStore:
    def __init__(self, calls: list[str], build_error: Exception | None = None) -> None:
        self._calls = calls
        self._build_error = build_error

    def build_views(self, cleanup_tmp_files: bool = True) -> None:
        self._calls.append(f"build_views:{cleanup_tmp_files}")
        if self._build_error is not None:
            raise self._build_error


def test_finalize_write_pipeline_runs_all_finalizers_in_order_and_raises_first_finalization_error() -> None:
    calls: list[str] = []

    def flush() -> None:
        calls.append("flush")
        raise RuntimeError("flush failed")

    duck_store = _FakeDuckStore(calls, build_error=RuntimeError("view failed"))

    with pytest.raises(RuntimeError, match="flush failed"):
        with _finalize_write_pipeline(
            store=_FakeStore(calls),
            metadata_flush=flush,
            build_views=True,
            cleanup_tmp_files=True,
            duckdb_store_factory=lambda: duck_store,
        ):
            calls.append("work")

    assert calls == ["work", "flush", "close", "build_views:True"]


def test_finalize_write_pipeline_preserves_primary_error_and_logs_finalization_failures() -> None:
    calls: list[str] = []
    messages: list[str] = []

    def flush() -> None:
        calls.append("flush")
        raise RuntimeError("flush failed")

    sink_id = logger.add(lambda message: messages.append(str(message)), level="ERROR")
    try:
        with pytest.raises(ValueError, match="work failed"):
            with _finalize_write_pipeline(
                store=_FakeStore(calls, close_error=RuntimeError("close failed")),
                metadata_flush=flush,
                build_views=True,
                cleanup_tmp_files=False,
                duckdb_store_factory=lambda: _FakeDuckStore(calls, build_error=RuntimeError("view failed")),
            ):
                calls.append("work")
                raise ValueError("work failed")
    finally:
        logger.remove(sink_id)

    assert calls == ["work", "flush", "close", "build_views:False"]
    logged = "\n".join(messages)
    assert "metadata flush" in logged
    assert "store close" in logged
    assert "DuckDB view build" in logged


def test_finalize_write_pipeline_can_close_without_building_views() -> None:
    calls: list[str] = []

    with _finalize_write_pipeline(
        store=_FakeStore(calls),
        metadata_flush=None,
        build_views=False,
        duckdb_store_factory=lambda: _FakeDuckStore(calls),
    ):
        calls.append("work")

    assert calls == ["work", "close"]
