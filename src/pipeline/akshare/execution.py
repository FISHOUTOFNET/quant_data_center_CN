"""Execution kernel for AkShare dataset modules."""

from __future__ import annotations

import traceback
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from src.api.akshare_client import AkShareCircuitOpen, AkShareClient, AkShareResponse
from src.pipeline.common import PipelineCheckpointLookup
from src.pipeline.lifecycle import PipelineLifecycle
from src.storage.duckdb_store import DuckDBStore
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


@dataclass
class _Progress:
    total: int
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0

    def record(self, row: dict[str, object]) -> None:
        self.processed += 1
        status = str(row.get("status", "unknown"))
        if status == "success":
            self.success += 1
        elif status == "failed":
            self.failed += 1
        elif status.startswith("skipped"):
            self.skipped += 1


def update_akshare(request: AkShareUpdateRequest) -> list[dict[str, object]]:
    """Run one or more AkShare dataset modules."""

    _validate_request(request)
    config = ConfigManager(request.root)
    store = ParquetStore(root=config.root)
    store.ensure_layout()
    checkpoint_lookup = PipelineCheckpointLookup.from_store(store) if request.resume and not request.force else None
    owns_client = request.client is None
    client = request.client or (
        request.client_factory(config) if request.client_factory is not None else AkShareClient(config=config)
    )
    lifecycle = PipelineLifecycle(
        store,
        flush_size=int(config.get("pipeline.metadata_flush_size", 200)),
        count_by="run",
    )
    context = AkShareExecutionContext(
        config=config,
        store=store,
        client=client,
        checkpoint_lookup=checkpoint_lookup,
        lifecycle=lifecycle,
    )
    records: list[dict[str, object]] = []
    progress_success = 0

    try:
        for module in _modules_for_target(request.target):
            module_records, module_success = _run_module(module, request, context)
            records.extend(module_records)
            progress_success += module_success
        lifecycle.finish()
    finally:
        store.close()
        if owns_client:
            close = getattr(client, "close", None)
            if close is not None:
                close()

    if request.build_views:
        DuckDBStore(root=config.root).build_views(cleanup_tmp_files=progress_success > 0)
    return records


def _run_module(
    module: AkShareDatasetModule,
    request: AkShareUpdateRequest,
    context: AkShareExecutionContext,
) -> tuple[list[dict[str, object]], int]:
    tasks = module.plan(request, context)
    selected_tasks = module.prefilter(tasks, context)
    policy = module.concurrency(request, context)
    progress = _Progress(total=len(selected_tasks))
    records: list[dict[str, object]] = []

    _call_optional(module, "log_started", request, len(tasks), len(selected_tasks), policy.workers)
    if policy.workers <= 1:
        for task_index, task in enumerate(selected_tasks, start=1):
            result = module.fetch(task, context)
            rows = module.record_result(result, context)
            records.extend(rows)
            _log_progress(module, task, rows, progress)
            policy.record_fetch_result(result.error is None)
            if policy.stop_on_circuit_open and isinstance(result.error, AkShareCircuitOpen):
                _call_optional(module, "log_circuit_open", task_index)
                for skipped_task in selected_tasks[task_index:]:
                    _record_skip(
                        module,
                        skipped_task,
                        context,
                        records,
                        progress,
                        status="skipped_circuit_open",
                        reason="circuit_open",
                    )
                break
    else:
        _run_concurrent_fetches(module, selected_tasks, context, policy, records, progress)

    _call_optional(module, "log_completed", progress)
    return records, progress.success


def _run_concurrent_fetches(
    module: AkShareDatasetModule,
    tasks: list[Any],
    context: AkShareExecutionContext,
    policy: ConcurrencyPolicy,
    records: list[dict[str, object]],
    progress: _Progress,
) -> None:
    pending: set[Future[FetchResult]] = set()
    future_tasks: dict[Future[FetchResult], Any] = {}
    task_index = 0
    stop_submitting = False
    circuit_opened = False

    def submit_until_target(executor: ThreadPoolExecutor) -> None:
        nonlocal task_index
        while not stop_submitting and task_index < len(tasks) and len(pending) < policy.target_workers:
            task = tasks[task_index]
            task_index += 1
            future = executor.submit(module.fetch, task, context)
            pending.add(future)
            future_tasks[future] = task

    with ThreadPoolExecutor(max_workers=policy.workers, thread_name_prefix=policy.thread_name_prefix) as executor:
        submit_until_target(executor)
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                task = future_tasks.pop(future)
                try:
                    result = future.result()
                except Exception as exc:
                    result = FetchResult(
                        task=task,
                        started_at=datetime.now(),
                        ended_at=datetime.now(),
                        error=exc,
                        error_stack=traceback.format_exc(),
                    )
                result_is_circuit_open = isinstance(result.error, AkShareCircuitOpen)
                if circuit_opened and result_is_circuit_open:
                    _record_skip(
                        module,
                        task,
                        context,
                        records,
                        progress,
                        status="skipped_circuit_open",
                        reason="circuit_open",
                    )
                    continue

                rows = module.record_result(result, context)
                records.extend(rows)
                _log_progress(module, task, rows, progress)
                policy.record_fetch_result(result.error is None)
                if policy.stop_on_circuit_open and result_is_circuit_open:
                    circuit_opened = True
                    stop_submitting = True
                    _call_optional(module, "log_circuit_open", task_index)
                    skipped_tasks = tasks[task_index:]
                    task_index = len(tasks)
                    for skipped_task in skipped_tasks:
                        _record_skip(
                            module,
                            skipped_task,
                            context,
                            records,
                            progress,
                            status="skipped_circuit_open",
                            reason="circuit_open",
                        )
            submit_until_target(executor)


def _record_skip(
    module: AkShareDatasetModule,
    task: Any,
    context: AkShareExecutionContext,
    records: list[dict[str, object]],
    progress: _Progress,
    *,
    status: str,
    reason: str,
) -> None:
    rows = module.record_skip(task, context, status=status, reason=reason)
    records.extend(rows)
    _log_progress(module, task, rows, progress)


def _log_progress(
    module: AkShareDatasetModule,
    task: Any,
    rows: list[dict[str, object]],
    progress: _Progress,
) -> None:
    row = module.progress_row(task, rows)
    progress.record(row)
    _call_optional(module, "log_progress", progress, task, row)


def _modules_for_target(target: str) -> Iterable[AkShareDatasetModule]:
    from src.pipeline.akshare.modules.capital_structure_em import CapitalStructureEmModule
    from src.pipeline.akshare.modules.daily_bar import DailyBarModule
    from src.pipeline.akshare.modules.delist import DelistModule
    from src.pipeline.akshare.modules.report_disclosure import ReportDisclosureModule
    from src.pipeline.akshare.modules.spot_quote import SpotQuoteModule
    from src.pipeline.akshare.modules.valuation_eastmoney import ValuationEastmoneyModule

    registry: dict[str, AkShareDatasetModule] = {
        "valuation": ValuationEastmoneyModule(),
        "capital_structure": CapitalStructureEmModule(),
        "daily_bar": DailyBarModule(),
        "spot_quote": SpotQuoteModule(),
        "delist": DelistModule(),
        "report_disclosure": ReportDisclosureModule(),
    }
    if target == "all":
        return [
            registry["valuation"],
            registry["capital_structure"],
            registry["delist"],
            registry["spot_quote"],
            registry["report_disclosure"],
            registry["daily_bar"],
        ]
    try:
        return [registry[target]]
    except KeyError as exc:
        raise ValueError(f"Unsupported AkShare update target: {target}") from exc


def _validate_request(request: AkShareUpdateRequest) -> None:
    if request.target not in {
        "valuation",
        "capital_structure",
        "daily_bar",
        "spot_quote",
        "delist",
        "report_disclosure",
        "all",
    }:
        raise ValueError(f"Unsupported AkShare update target: {request.target}")
    if request.adjustment is not None and request.target != "daily_bar":
        raise ValueError("--adjustment is only valid for --target daily_bar")
    if request.period and request.target != "report_disclosure":
        raise ValueError("--period is only valid for --target report_disclosure")


def _call_optional(module: AkShareDatasetModule, name: str, *args: Any) -> Any:
    method = getattr(module, name, None)
    if method is None:
        return None
    return method(*args)


__all__ = [
    "AkShareDatasetModule",
    "AkShareExecutionContext",
    "AkShareUpdateRequest",
    "ConcurrencyPolicy",
    "FetchResult",
    "update_akshare",
]
