"""Run-scoped diagnostic context for pipeline logs."""

from __future__ import annotations

import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

_PIPELINE_RUN_ID: ContextVar[str | None] = ContextVar("pipeline_run_id", default=None)


def new_pipeline_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def current_pipeline_run_id() -> str | None:
    return _PIPELINE_RUN_ID.get()


def set_pipeline_run_id(run_id: str) -> Token[str | None]:
    return _PIPELINE_RUN_ID.set(run_id)


def reset_pipeline_run_id(token: Token[str | None]) -> None:
    _PIPELINE_RUN_ID.reset(token)


@contextmanager
def pipeline_run_context(run_id: str) -> Iterator[None]:
    token = set_pipeline_run_id(run_id)
    try:
        yield
    finally:
        reset_pipeline_run_id(token)


def pipeline_log_identity() -> dict[str, object]:
    return {
        "run_id": current_pipeline_run_id() or "-",
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
    }


def pipeline_log_values() -> tuple[object, object, object]:
    identity = pipeline_log_identity()
    return identity["run_id"], identity["pid"], identity["thread"]
