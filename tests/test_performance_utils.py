from __future__ import annotations

import src.utils.performance as performance
from src.utils.performance import (
    FunctionProfiler,
    MemoryMonitor,
    PerformanceCollector,
    PerformanceTimer,
    TimingResult,
    format_bytes,
    format_duration,
    profile_function,
    timing_decorator,
)


def test_performance_timer_records_elapsed(monkeypatch) -> None:
    ticks = iter([10.0, 10.25])
    monkeypatch.setattr(performance.time, "perf_counter", lambda: next(ticks))

    with PerformanceTimer("load") as timer:
        assert timer.name == "load"

    assert timer.elapsed == 0.25


def test_performance_collector_statistics_summary_and_clear(monkeypatch) -> None:
    ticks = iter([1.0, 1.2])
    monkeypatch.setattr(performance.time, "perf_counter", lambda: next(ticks))

    collector = PerformanceCollector()
    collector.add(TimingResult("read", 0.1, {"dataset": "daily"}))
    collector.add(TimingResult("read", 0.3))
    with collector.measure("write", {"dataset": "daily"}):
        pass

    stats = collector.get_statistics()
    assert stats["read"]["count"] == 2
    assert stats["read"]["total"] == 0.4
    assert stats["write"]["count"] == 1
    assert stats["write"]["stdev"] == 0.0
    assert collector.get_statistics("rea") == {"read": stats["read"]}
    assert "Performance Summary:" in collector.summary()

    collector.clear()
    assert collector.get_statistics() == {}
    assert collector.summary() == "No performance data collected"


def test_timing_decorator_preserves_result_and_logs_elapsed(monkeypatch) -> None:
    ticks = iter([2.0, 2.5])
    monkeypatch.setattr(performance.time, "perf_counter", lambda: next(ticks))

    @timing_decorator
    def add(left: int, right: int) -> int:
        return left + right

    assert add(2, 3) == 5
    assert add.__name__ == "add"


def test_memory_monitor_snapshots_and_summary() -> None:
    monitor = MemoryMonitor()
    start = monitor.snapshot("start")
    end = monitor.snapshot("end")

    assert isinstance(start, int)
    assert isinstance(end, int)
    assert monitor.get_memory_diff("missing", "end") == 0
    assert isinstance(monitor.get_memory_diff("start", "end"), int)
    assert "Memory Snapshots:" in monitor.summary()
    assert MemoryMonitor().summary() == "No memory snapshots collected"


def test_function_profiler_and_profile_decorator(monkeypatch) -> None:
    profiler = FunctionProfiler()
    profiler.record_call("fetch", 0.2)
    profiler.record_call("fetch", 0.4)

    stats = profiler.get_stats()
    assert stats["fetch"] == {"call_count": 2, "total_time": 0.6000000000000001, "avg_time": 0.30000000000000004}
    assert "Function Profiling Summary:" in profiler.summary()
    assert FunctionProfiler().summary() == "No function profiling data"

    performance._global_profiler = FunctionProfiler()
    ticks = iter([5.0, 5.75])
    monkeypatch.setattr(performance.time, "perf_counter", lambda: next(ticks))

    @profile_function
    def multiply(value: int) -> int:
        return value * 2

    assert multiply(4) == 8
    assert performance._global_profiler.get_stats()["multiply"]["call_count"] == 1


def test_format_helpers_cover_units_and_duration_ranges() -> None:
    assert format_bytes(512) == "512.00 B"
    assert format_bytes(2048) == "2.00 KB"
    assert format_bytes(5 * 1024**5) == "5.00 PB"

    assert format_duration(0.5) == "500.00 ms"
    assert format_duration(2.0) == "2.00 s"
    assert format_duration(120.0) == "2.00 min"
    assert format_duration(7200.0) == "2.00 h"
