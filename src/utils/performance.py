"""Performance monitoring and profiling utilities."""

from __future__ import annotations

import statistics
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from threading import Lock
from typing import Any, Callable, TypeVar

from src.utils.logging import logger

T = TypeVar("T")


@dataclass
class TimingResult:
    name: str
    elapsed: float
    metadata: dict[str, Any] = field(default_factory=dict)


class PerformanceTimer:
    def __init__(self, name: str, metadata: dict[str, Any] | None = None):
        self.name = name
        self.metadata = metadata or {}
        self.start: float | None = None
        self.elapsed: float | None = None

    def __enter__(self) -> "PerformanceTimer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args: object) -> None:
        if self.start is not None:
            self.elapsed = time.perf_counter() - self.start
            logger.info(
                "Performance: {} took {:.3f}s",
                self.name,
                self.elapsed,
            )


class PerformanceCollector:
    def __init__(self) -> None:
        self._timings: list[TimingResult] = []
        self._lock = Lock()

    def add(self, result: TimingResult) -> None:
        with self._lock:
            self._timings.append(result)

    @contextmanager
    def measure(self, name: str, metadata: dict[str, Any] | None = None) -> Any:
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.add(TimingResult(name, elapsed, metadata or {}))

    def get_statistics(self, name_pattern: str | None = None) -> dict[str, dict[str, float]]:
        with self._lock:
            timings = list(self._timings)

        if name_pattern:
            timings = [t for t in timings if name_pattern in t.name]

        grouped: dict[str, list[float]] = {}
        for timing in timings:
            if timing.name not in grouped:
                grouped[timing.name] = []
            grouped[timing.name].append(timing.elapsed)

        stats: dict[str, dict[str, float]] = {}
        for name, times in grouped.items():
            if len(times) >= 2:
                stats[name] = {
                    "count": len(times),
                    "total": sum(times),
                    "mean": statistics.mean(times),
                    "median": statistics.median(times),
                    "stdev": statistics.stdev(times),
                    "min": min(times),
                    "max": max(times),
                }
            else:
                stats[name] = {
                    "count": len(times),
                    "total": sum(times),
                    "mean": times[0],
                    "median": times[0],
                    "stdev": 0.0,
                    "min": times[0],
                    "max": times[0],
                }

        return stats

    def clear(self) -> None:
        with self._lock:
            self._timings.clear()

    def summary(self) -> str:
        stats = self.get_statistics()
        if not stats:
            return "No performance data collected"

        lines = ["Performance Summary:", "-" * 80]
        for name, data in sorted(stats.items(), key=lambda x: x[1]["total"], reverse=True):
            lines.append(
                f"{name:40s} | count: {data['count']:5d} | "
                f"total: {data['total']:8.3f}s | mean: {data['mean']:7.3f}s | "
                f"median: {data['median']:7.3f}s"
            )
        return "\n".join(lines)


_global_collector = PerformanceCollector()


def get_collector() -> PerformanceCollector:
    return _global_collector


def timing_decorator(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> T:
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            logger.info("Performance: {} took {:.3f}s", func.__name__, elapsed)

    return wrapper


def measure_time(name: str, metadata: dict[str, Any] | None = None) -> PerformanceTimer:
    return PerformanceTimer(name, metadata)


class MemoryMonitor:
    def __init__(self) -> None:
        self._snapshots: list[tuple[str, int]] = []
        self._lock = Lock()

    def snapshot(self, name: str) -> int:
        try:
            import tracemalloc

            if not tracemalloc.is_tracing():
                tracemalloc.start()

            current, _ = tracemalloc.get_traced_memory()
            with self._lock:
                self._snapshots.append((name, current))
            return current
        except ImportError:
            logger.warning("tracemalloc not available for memory monitoring")
            return 0

    def get_memory_diff(self, start_name: str, end_name: str) -> int:
        with self._lock:
            snapshots = list(self._snapshots)

        start_mem = None
        end_mem = None
        for name, mem in snapshots:
            if name == start_name:
                start_mem = mem
            if name == end_name:
                end_mem = mem

        if start_mem is None or end_mem is None:
            return 0
        return end_mem - start_mem

    def summary(self) -> str:
        with self._lock:
            snapshots = list(self._snapshots)

        if not snapshots:
            return "No memory snapshots collected"

        lines = ["Memory Snapshots:", "-" * 80]
        for name, mem in snapshots:
            lines.append(f"{name:40s} | {mem / 1024 / 1024:10.2f} MB")
        return "\n".join(lines)


_global_memory_monitor = MemoryMonitor()


def get_memory_monitor() -> MemoryMonitor:
    return _global_memory_monitor


class FunctionProfiler:
    def __init__(self) -> None:
        self._call_counts: dict[str, int] = {}
        self._total_times: dict[str, float] = {}
        self._lock = Lock()

    def record_call(self, func_name: str, elapsed: float) -> None:
        with self._lock:
            self._call_counts[func_name] = self._call_counts.get(func_name, 0) + 1
            self._total_times[func_name] = self._total_times.get(func_name, 0.0) + elapsed

    def get_stats(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            result = {}
            for func_name in self._call_counts:
                count = self._call_counts[func_name]
                total = self._total_times.get(func_name, 0.0)
                result[func_name] = {
                    "call_count": count,
                    "total_time": total,
                    "avg_time": total / count if count > 0 else 0.0,
                }
            return result

    def summary(self) -> str:
        stats = self.get_stats()
        if not stats:
            return "No function profiling data"

        lines = ["Function Profiling Summary:", "-" * 80]
        sorted_stats = sorted(stats.items(), key=lambda x: x[1]["total_time"], reverse=True)
        for func_name, data in sorted_stats:
            lines.append(
                f"{func_name:40s} | calls: {data['call_count']:5d} | "
                f"total: {data['total_time']:8.3f}s | avg: {data['avg_time']:7.3f}s"
            )
        return "\n".join(lines)


_global_profiler = FunctionProfiler()


def profile_function(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> T:
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            _global_profiler.record_call(func.__name__, elapsed)

    return wrapper


def format_bytes(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def format_duration(seconds: float) -> str:
    if seconds < 1.0:
        return f"{seconds * 1000:.2f} ms"
    elif seconds < 60.0:
        return f"{seconds:.2f} s"
    elif seconds < 3600.0:
        minutes = seconds / 60.0
        return f"{minutes:.2f} min"
    else:
        hours = seconds / 3600.0
        return f"{hours:.2f} h"
