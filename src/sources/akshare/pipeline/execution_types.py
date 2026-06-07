"""Shared AkShare execution data types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from src.pipeline.common import PipelineCheckpointLookup
from src.pipeline.lifecycle import PipelineLifecycle
from src.sources.akshare.client import AkShareResponse
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


@dataclass(frozen=True)
class AkShareUpdateRequest:
    target: str = "valuation"
    mode: str = "partial"
    adjustment: str | None = None
    code: tuple[str, ...] | list[str] | str | None = None
    include_inactive: bool = False
    market: str | None = None
    start: str | Any | None = None
    end: str | Any | None = None
    max_tasks: int | None = None
    workers: int | None = None
    root: Path | None = None
    resume: bool = True
    force: bool = False
    build_views: bool = True
    client: Any | None = None
    client_factory: Callable[[ConfigManager], Any] | None = None
    now: Callable[[], datetime] | None = None
    period: tuple[str, ...] | list[str] | str | None = ()


@dataclass(frozen=True)
class FetchResult:
    task: Any
    started_at: datetime
    ended_at: datetime
    response: AkShareResponse | None = None
    error: Exception | None = None
    error_stack: str = ""
    skipped: bool = False
    skip_status: str = "skipped_checkpoint"
    skip_reason: str = "checkpoint"


@dataclass
class ConcurrencyPolicy:
    workers: int = 1
    thread_name_prefix: str = "update-akshare-fetch"
    adaptive_controller: Any | None = None
    stop_on_circuit_open: bool = False

    @property
    def target_workers(self) -> int:
        if self.adaptive_controller is None:
            return self.workers
        return int(self.adaptive_controller.target_workers)

    def record_fetch_result(self, success: bool) -> None:
        if self.adaptive_controller is not None:
            self.adaptive_controller.record_fetch_result(success)


@dataclass
class AkShareExecutionContext:
    config: ConfigManager
    store: ParquetStore
    client: Any
    checkpoint_lookup: PipelineCheckpointLookup | None
    lifecycle: PipelineLifecycle


class AkShareDatasetModule(Protocol):
    target: str

    def plan(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> list[Any]: ...

    def prefilter(self, tasks: list[Any], context: AkShareExecutionContext) -> list[Any]: ...

    def fetch(self, task: Any, context: AkShareExecutionContext) -> FetchResult: ...

    def record_result(self, result: FetchResult, context: AkShareExecutionContext) -> list[dict[str, object]]: ...

    def record_skip(
        self,
        task: Any,
        context: AkShareExecutionContext,
        status: str = "skipped_checkpoint",
        reason: str = "checkpoint",
    ) -> list[dict[str, object]]: ...

    def progress_row(self, task: Any, rows: list[dict[str, object]]) -> dict[str, object]: ...

    def concurrency(self, request: AkShareUpdateRequest, context: AkShareExecutionContext) -> ConcurrencyPolicy: ...
