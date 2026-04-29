"""Single-writer queue for pipeline storage work."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from dataclasses import dataclass
from queue import Queue
from threading import Thread

from src.utils.logging import logger


PipelineWriteResult = dict[str, object] | list[dict[str, object]] | None

_SENTINEL = object()


@dataclass(frozen=True)
class _PipelineWriteTask:
    action: Callable[[], PipelineWriteResult]
    on_error: Callable[[str], PipelineWriteResult] | None
    description: str


class PipelineWriteQueue:
    """Run pipeline write tasks on one background thread.

    A single writer keeps Parquet metadata upserts serialized while allowing the
    main thread to continue issuing API requests after each dataframe is ready.
    """

    def __init__(self, maxsize: int = 32) -> None:
        self._queue: Queue[_PipelineWriteTask | object] = Queue(maxsize=maxsize)
        self._results: list[dict[str, object]] = []
        self._closed = False
        self._thread = Thread(target=self._run, name="pipeline-write-queue", daemon=True)
        self._thread.start()

    def submit(
        self,
        action: Callable[[], PipelineWriteResult],
        on_error: Callable[[str], PipelineWriteResult] | None = None,
        description: str = "",
    ) -> None:
        if self._closed:
            raise RuntimeError("Cannot submit pipeline write task after close")
        self._queue.put(_PipelineWriteTask(action, on_error, description))

    def close(self) -> list[dict[str, object]]:
        if not self._closed:
            self._closed = True
            self._queue.put(_SENTINEL)
        self._queue.join()
        self._thread.join()
        return list(self._results)

    def _run(self) -> None:
        while True:
            task = self._queue.get()
            try:
                if task is _SENTINEL:
                    return
                result = self._execute(task)
                self._record_result(result)
            finally:
                self._queue.task_done()

    def _execute(self, task: _PipelineWriteTask | object) -> PipelineWriteResult:
        if not isinstance(task, _PipelineWriteTask):
            return None
        try:
            return task.action()
        except Exception:
            error_stack = traceback.format_exc()
            if task.description:
                logger.exception("Pipeline write task failed: {}", task.description)
            else:
                logger.exception("Pipeline write task failed")
            if task.on_error is None:
                return {"status": "failed", "error_stack": error_stack}
            try:
                return task.on_error(error_stack)
            except Exception:
                handler_stack = traceback.format_exc()
                logger.exception("Pipeline write failure handler failed: {}", task.description)
                return {
                    "status": "failed",
                    "error_stack": f"{error_stack}\nFailure handler failed:\n{handler_stack}",
                }

    def _record_result(self, result: PipelineWriteResult) -> None:
        if result is None:
            return
        if isinstance(result, list):
            self._results.extend(result)
        else:
            self._results.append(result)
