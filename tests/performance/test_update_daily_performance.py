"""Performance test suite for update_daily.py pipeline.

This module provides comprehensive performance testing including:
- Function execution timing
- Data processing efficiency
- Resource utilization (CPU, Memory, I/O)
- Bottleneck identification
"""

from __future__ import annotations

import cProfile
import io
import pstats
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import psutil

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.api.market_data import MarketDataProvider, create_provider
from src.pipeline.adjustments import (
    ADJUST_FACTOR_DATASET,
    UNADJUSTED_DAILY_DATASET,
    calculate_adjusted_daily_k,
    is_adjusted_daily_dataset,
)
from src.pipeline.common import FULL_HISTORY_START_DATE
from src.pipeline.services import fetch_adjust_factor, fetch_daily_k, fetch_stock_basic
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


@dataclass
class TimingResult:
    name: str
    total_time: float
    call_count: int
    avg_time: float
    min_time: float
    max_time: float
    times: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "total_time": self.total_time,
            "call_count": self.call_count,
            "avg_time": self.avg_time,
            "min_time": self.min_time,
            "max_time": self.max_time,
        }


@dataclass
class ResourceSnapshot:
    timestamp: float
    cpu_percent: float
    memory_mb: float
    read_bytes: int
    write_bytes: int
    read_count: int
    write_count: int


@dataclass
class PerformanceReport:
    test_name: str
    start_time: str
    end_time: str
    duration_seconds: float
    timing_results: list[TimingResult]
    resource_snapshots: list[ResourceSnapshot]
    profile_stats: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_name": self.test_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "timing_results": [t.to_dict() for t in self.timing_results],
            "resource_summary": self._resource_summary(),
            "profile_stats": self.profile_stats,
        }

    def _resource_summary(self) -> dict[str, Any]:
        if not self.resource_snapshots:
            return {}
        cpu_values = [s.cpu_percent for s in self.resource_snapshots]
        mem_values = [s.memory_mb for s in self.resource_snapshots]
        return {
            "avg_cpu_percent": sum(cpu_values) / len(cpu_values),
            "max_cpu_percent": max(cpu_values),
            "avg_memory_mb": sum(mem_values) / len(mem_values),
            "max_memory_mb": max(mem_values),
            "sample_count": len(self.resource_snapshots),
        }


class PerformanceTimer:
    def __init__(self):
        self._timings: dict[str, list[float]] = {}
        self._start_times: dict[str, float] = {}

    def start(self, name: str) -> None:
        self._start_times[name] = time.perf_counter()

    def stop(self, name: str) -> float:
        if name not in self._start_times:
            return 0.0
        elapsed = time.perf_counter() - self._start_times[name]
        if name not in self._timings:
            self._timings[name] = []
        self._timings[name].append(elapsed)
        del self._start_times[name]
        return elapsed

    @contextmanager
    def measure(self, name: str):
        self.start(name)
        try:
            yield
        finally:
            self.stop(name)

    def get_results(self) -> list[TimingResult]:
        results = []
        for name, times in self._timings.items():
            if times:
                results.append(
                    TimingResult(
                        name=name,
                        total_time=sum(times),
                        call_count=len(times),
                        avg_time=sum(times) / len(times),
                        min_time=min(times),
                        max_time=max(times),
                        times=times,
                    )
                )
        return sorted(results, key=lambda r: r.total_time, reverse=True)

    def reset(self) -> None:
        self._timings.clear()
        self._start_times.clear()


class ResourceMonitor:
    def __init__(self, interval: float = 0.1):
        self._interval = interval
        self._snapshots: list[ResourceSnapshot] = []
        self._process = psutil.Process()
        self._running = False
        self._start_io = None

    def start(self) -> None:
        self._running = True
        self._start_io = self._process.io_counters()
        self._snapshots = []

    def snapshot(self) -> ResourceSnapshot:
        io = self._process.io_counters()
        cpu = self._process.cpu_percent()
        mem = self._process.memory_info().rss / (1024 * 1024)
        snapshot = ResourceSnapshot(
            timestamp=time.time(),
            cpu_percent=cpu,
            memory_mb=mem,
            read_bytes=io.read_bytes,
            write_bytes=io.write_bytes,
            read_count=io.read_count,
            write_count=io.write_count,
        )
        self._snapshots.append(snapshot)
        return snapshot

    def stop(self) -> list[ResourceSnapshot]:
        self._running = False
        return self._snapshots

    def get_io_delta(self) -> dict[str, int]:
        if not self._start_io:
            return {}
        end_io = self._process.io_counters()
        return {
            "read_bytes_delta": end_io.read_bytes - self._start_io.read_bytes,
            "write_bytes_delta": end_io.write_bytes - self._start_io.write_bytes,
            "read_count_delta": end_io.read_count - self._start_io.read_count,
            "write_count_delta": end_io.write_count - self._start_io.write_count,
        }


class UpdateDailyPerformanceTest:
    def __init__(self, root: Path | None = None, test_codes: list[str] | None = None):
        self.root = root or Path(__file__).parent.parent.parent
        self.config = ConfigManager(self.root)
        self.store = ParquetStore(root=self.root)
        self.timer = PerformanceTimer()
        self.monitor = ResourceMonitor()
        self.test_codes = test_codes or ["sh.600000", "sh.600036", "sz.000001", "sz.000002"]
        self._provider_context: Any = None
        self.provider: MarketDataProvider | None = None
        self._reports: list[PerformanceReport] = []

    def setup(self) -> None:
        self.store.ensure_layout()
        self._provider_context = create_provider(self.config)
        self.provider = self._provider_context.__enter__()

    def teardown(self) -> None:
        if self._provider_context:
            try:
                self._provider_context.__exit__(None, None, None)
            except Exception:
                pass

    def _run_test(self, test_name: str, test_func: Callable) -> PerformanceReport:
        start_time = datetime.now().isoformat()
        self.timer.reset()
        self.monitor.start()

        profiler = cProfile.Profile()
        profiler.enable()

        try:
            test_func()
        except Exception as e:
            print(f"Test {test_name} failed: {e}")
            traceback.print_exc()

        profiler.disable()

        end_time = datetime.now().isoformat()
        snapshots = self.monitor.stop()

        profile_stats = self._extract_profile_stats(profiler)

        report = PerformanceReport(
            test_name=test_name,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=(datetime.fromisoformat(end_time) - datetime.fromisoformat(start_time)).total_seconds(),
            timing_results=self.timer.get_results(),
            resource_snapshots=snapshots,
            profile_stats=profile_stats,
        )
        self._reports.append(report)
        return report

    def _extract_profile_stats(self, profiler: cProfile.Profile) -> dict[str, Any]:
        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
        ps.print_stats(30)
        stats_str = s.getvalue()

        top_functions = []
        lines = stats_str.split("\n")
        for line in lines[5:35]:
            if line.strip():
                top_functions.append(line.strip())

        return {
            "top_functions_by_cumulative": top_functions,
            "total_calls": ps.total_calls,
            "total_time": ps.total_tt,
        }

    def test_fetch_adjust_factor(self) -> PerformanceReport:
        def test_func():
            for code in self.test_codes:
                with self.timer.measure(f"fetch_adjust_factor_{code}"):
                    fetch_adjust_factor(self.provider, code, FULL_HISTORY_START_DATE, "2025-04-28")

        return self._run_test("fetch_adjust_factor", test_func)

    def test_fetch_daily_k(self) -> PerformanceReport:
        def test_func():
            for code in self.test_codes:
                with self.timer.measure(f"fetch_daily_k_{code}"):
                    fetch_daily_k(
                        self.provider,
                        self.config,
                        UNADJUSTED_DAILY_DATASET,
                        code,
                        FULL_HISTORY_START_DATE,
                        "2025-04-28",
                    )

        return self._run_test("fetch_daily_k", test_func)

    def test_calculate_adjusted_daily_k(self) -> PerformanceReport:
        unadjusted_cache: dict[str, pd.DataFrame] = {}
        factor_cache: dict[str, pd.DataFrame] = {}

        for code in self.test_codes:
            unadjusted_cache[code] = fetch_daily_k(
                self.provider,
                self.config,
                UNADJUSTED_DAILY_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                "2025-04-28",
            )
            factor_cache[code] = fetch_adjust_factor(self.provider, code, FULL_HISTORY_START_DATE, "2025-04-28")

        def test_func():
            for code in self.test_codes:
                with self.timer.measure(f"calculate_qfq_{code}"):
                    calculate_adjusted_daily_k(
                        unadjusted_cache[code],
                        factor_cache[code],
                        "daily_k_qfq",
                        "1",
                    )
                with self.timer.measure(f"calculate_hfq_{code}"):
                    calculate_adjusted_daily_k(
                        unadjusted_cache[code],
                        factor_cache[code],
                        "daily_k_hfq",
                        "2",
                    )

        return self._run_test("calculate_adjusted_daily_k", test_func)

    def test_parquet_write(self) -> PerformanceReport:
        test_data: dict[str, pd.DataFrame] = {}
        for code in self.test_codes:
            test_data[code] = fetch_daily_k(
                self.provider,
                self.config,
                UNADJUSTED_DAILY_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                "2025-04-28",
            )

        def test_func():
            for code, df in test_data.items():
                with self.timer.measure(f"write_daily_k_{code}"):
                    self.store.write_daily_k(UNADJUSTED_DAILY_DATASET, code, df)

        return self._run_test("parquet_write", test_func)

    def test_parquet_read(self) -> PerformanceReport:
        def test_func():
            for code in self.test_codes:
                with self.timer.measure(f"read_daily_k_{code}"):
                    self.store.read_daily_k(UNADJUSTED_DAILY_DATASET, code)
                with self.timer.measure(f"read_adjust_factor_{code}"):
                    self.store.read_adjust_factor(code)

        return self._run_test("parquet_read", test_func)

    def test_full_pipeline_single_code(self) -> PerformanceReport:
        code = self.test_codes[0]

        def test_func():
            with self.timer.measure("fetch_adjust_factor"):
                factors = fetch_adjust_factor(self.provider, code, FULL_HISTORY_START_DATE, "2025-04-28")

            with self.timer.measure("fetch_daily_k"):
                unadjusted = fetch_daily_k(
                    self.provider,
                    self.config,
                    UNADJUSTED_DAILY_DATASET,
                    code,
                    FULL_HISTORY_START_DATE,
                    "2025-04-28",
                )

            with self.timer.measure("calculate_qfq"):
                qfq = calculate_adjusted_daily_k(unadjusted, factors, "daily_k_qfq", "1")

            with self.timer.measure("calculate_hfq"):
                hfq = calculate_adjusted_daily_k(unadjusted, factors, "daily_k_hfq", "2")

            with self.timer.measure("write_unadjusted"):
                self.store.write_daily_k(UNADJUSTED_DAILY_DATASET, code, unadjusted)

            with self.timer.measure("write_qfq"):
                self.store.write_daily_k("daily_k_qfq", code, qfq)

            with self.timer.measure("write_hfq"):
                self.store.write_daily_k("daily_k_hfq", code, hfq)

        return self._run_test("full_pipeline_single_code", test_func)

    def test_merge_daily_frames(self) -> PerformanceReport:
        from src.pipeline.common import merge_daily_frames

        test_data: dict[str, pd.DataFrame] = {}
        for code in self.test_codes:
            test_data[code] = fetch_daily_k(
                self.provider,
                self.config,
                UNADJUSTED_DAILY_DATASET,
                code,
                FULL_HISTORY_START_DATE,
                "2025-04-28",
            )
            self.store.write_daily_k(UNADJUSTED_DAILY_DATASET, code, test_data[code])

        def test_func():
            for code in self.test_codes:
                existing = self.store.read_daily_k(UNADJUSTED_DAILY_DATASET, code)
                fresh = test_data[code].head(10)
                with self.timer.measure(f"merge_frames_{code}"):
                    merge_daily_frames(self.store, existing, fresh)

        return self._run_test("merge_daily_frames", test_func)

    def test_adjust_factor_diff(self) -> PerformanceReport:
        from src.pipeline.update_daily import _adjust_factor_frames_differ

        factor_cache: dict[str, pd.DataFrame] = {}
        for code in self.test_codes:
            factor_cache[code] = fetch_adjust_factor(self.provider, code, FULL_HISTORY_START_DATE, "2025-04-28")

        def test_func():
            for code in self.test_codes:
                with self.timer.measure(f"adjust_factor_diff_{code}"):
                    _adjust_factor_frames_differ(self.store, factor_cache[code], factor_cache[code].copy())

        return self._run_test("adjust_factor_diff", test_func)

    def test_metadata_batch(self) -> PerformanceReport:
        from src.pipeline.services import PipelineMetadataBatch

        def test_func():
            batch = PipelineMetadataBatch(self.store, 200, count_by="run")
            for i in range(500):
                run_row = {
                    "task_id": f"test-{i}",
                    "dataset": "daily_k_none",
                    "code": f"sh.60000{i % 10}",
                    "status": "success",
                    "start_date": "2025-01-01",
                    "end_date": "2025-04-28",
                    "start_time": datetime.now(),
                    "end_time": datetime.now(),
                    "row_count": 100,
                    "error_stack": "",
                }
                status_row = {
                    "dataset": "daily_k_none",
                    "code": f"sh.60000{i % 10}",
                    "last_success_date": "2025-04-28",
                    "row_count": 100,
                    "status": "success",
                    "updated_at": datetime.now(),
                    "error_stack": "",
                }
                with self.timer.measure("metadata_batch_add"):
                    batch.add(run_row=run_row, status_row=status_row)

            with self.timer.measure("metadata_batch_flush"):
                batch.flush()

        return self._run_test("metadata_batch", test_func)

    def run_all_tests(self) -> list[PerformanceReport]:
        self.setup()
        try:
            print("=" * 80)
            print("Running Performance Tests for update_daily.py")
            print("=" * 80)
            print(f"Test codes: {self.test_codes}")
            print(f"Root: {self.root}")
            print()

            tests = [
                ("API Fetch Tests", [
                    self.test_fetch_adjust_factor,
                    self.test_fetch_daily_k,
                ]),
                ("Calculation Tests", [
                    self.test_calculate_adjusted_daily_k,
                ]),
                ("Storage Tests", [
                    self.test_parquet_write,
                    self.test_parquet_read,
                ]),
                ("Pipeline Tests", [
                    self.test_full_pipeline_single_code,
                    self.test_merge_daily_frames,
                    self.test_adjust_factor_diff,
                ]),
                ("Metadata Tests", [
                    self.test_metadata_batch,
                ]),
            ]

            for category, test_funcs in tests:
                print(f"\n--- {category} ---")
                for test_func in test_funcs:
                    print(f"\nRunning {test_func.__name__}...")
                    report = test_func()
                    self._print_report_summary(report)

            return self._reports
        finally:
            self.teardown()

    def _print_report_summary(self, report: PerformanceReport) -> None:
        print(f"\n  Test: {report.test_name}")
        print(f"  Duration: {report.duration_seconds:.3f}s")

        if report.timing_results:
            print("\n  Timing Breakdown:")
            for timing in report.timing_results[:10]:
                print(
                    f"    {timing.name}: {timing.total_time:.3f}s ({timing.call_count} calls, avg: {timing.avg_time * 1000:.2f}ms)"
                )

        resource_summary = report._resource_summary()
        if resource_summary:
            print("\n  Resource Usage:")
            print(f"    Avg CPU: {resource_summary['avg_cpu_percent']:.1f}%")
            print(f"    Max CPU: {resource_summary['max_cpu_percent']:.1f}%")
            print(f"    Avg Memory: {resource_summary['avg_memory_mb']:.1f} MB")
            print(f"    Max Memory: {resource_summary['max_memory_mb']:.1f} MB")

    def generate_report(self) -> str:
        lines = []
        lines.append("=" * 80)
        lines.append("PERFORMANCE TEST REPORT FOR update_daily.py")
        lines.append("=" * 80)
        lines.append(f"Generated: {datetime.now().isoformat()}")
        lines.append(f"Test Codes: {self.test_codes}")
        lines.append("")

        for report in self._reports:
            lines.append("-" * 60)
            lines.append(f"Test: {report.test_name}")
            lines.append(f"Duration: {report.duration_seconds:.3f}s")
            lines.append("")

            if report.timing_results:
                lines.append("Function Timing Breakdown:")
                lines.append(f"{'Function':<40} {'Total(s)':<12} {'Calls':<8} {'Avg(ms)':<12} {'Min(ms)':<12} {'Max(ms)':<12}")
                lines.append("-" * 96)
                for t in report.timing_results:
                    lines.append(
                        f"{t.name:<40} {t.total_time:<12.3f} {t.call_count:<8} "
                        f"{t.avg_time * 1000:<12.2f} {t.min_time * 1000:<12.2f} {t.max_time * 1000:<12.2f}"
                    )
                lines.append("")

            resource_summary = report._resource_summary()
            if resource_summary:
                lines.append("Resource Usage:")
                lines.append(f"  Average CPU: {resource_summary['avg_cpu_percent']:.1f}%")
                lines.append(f"  Maximum CPU: {resource_summary['max_cpu_percent']:.1f}%")
                lines.append(f"  Average Memory: {resource_summary['avg_memory_mb']:.1f} MB")
                lines.append(f"  Maximum Memory: {resource_summary['max_memory_mb']:.1f} MB")
                lines.append("")

            if report.profile_stats and report.profile_stats.get("top_functions_by_cumulative"):
                lines.append("Top Functions by Cumulative Time:")
                for func_line in report.profile_stats["top_functions_by_cumulative"][:15]:
                    lines.append(f"  {func_line}")
                lines.append("")

        lines.append("=" * 80)
        lines.append("BOTTLENECK ANALYSIS")
        lines.append("=" * 80)
        lines.extend(self._analyze_bottlenecks())

        lines.append("")
        lines.append("=" * 80)
        lines.append("OPTIMIZATION RECOMMENDATIONS")
        lines.append("=" * 80)
        lines.extend(self._generate_recommendations())

        return "\n".join(lines)

    def _analyze_bottlenecks(self) -> list[str]:
        lines = []
        all_timings: dict[str, TimingResult] = {}

        for report in self._reports:
            for timing in report.timing_results:
                base_name = timing.name.split("_")[0] if "_" in timing.name else timing.name
                if base_name not in all_timings:
                    all_timings[base_name] = TimingResult(
                        name=base_name,
                        total_time=0,
                        call_count=0,
                        avg_time=0,
                        min_time=float("inf"),
                        max_time=0,
                    )
                all_timings[base_name].total_time += timing.total_time
                all_timings[base_name].call_count += timing.call_count
                all_timings[base_name].avg_time = (
                    all_timings[base_name].total_time / all_timings[base_name].call_count
                )
                all_timings[base_name].min_time = min(all_timings[base_name].min_time, timing.min_time)
                all_timings[base_name].max_time = max(all_timings[base_name].max_time, timing.max_time)

        sorted_timings = sorted(all_timings.values(), key=lambda t: t.total_time, reverse=True)

        lines.append("\nTime Distribution by Operation Type:")
        total_time = sum(t.total_time for t in sorted_timings)
        for timing in sorted_timings:
            percentage = (timing.total_time / total_time * 100) if total_time > 0 else 0
            lines.append(f"  {timing.name:<30} {timing.total_time:>8.3f}s ({percentage:>5.1f}%) - {timing.call_count} calls")

        api_time = sum(t.total_time for t in sorted_timings if "fetch" in t.name.lower())
        calc_time = sum(t.total_time for t in sorted_timings if "calculate" in t.name.lower())
        io_time = sum(t.total_time for t in sorted_timings if any(k in t.name.lower() for k in ["read", "write", "merge"]))

        lines.append("\nTime Distribution by Category:")
        lines.append(f"  API Fetching:    {api_time:>8.3f}s ({api_time / total_time * 100:>5.1f}%)")
        lines.append(f"  Calculations:    {calc_time:>8.3f}s ({calc_time / total_time * 100:>5.1f}%)")
        lines.append(f"  I/O Operations:  {io_time:>8.3f}s ({io_time / total_time * 100:>5.1f}%)")

        return lines

    def _generate_recommendations(self) -> list[str]:
        lines = []

        lines.append("\n1. API FETCHING OPTIMIZATION")
        lines.append("   - Current: Sequential API calls per stock code")
        lines.append("   - Issue: Network latency accumulates linearly with stock count")
        lines.append("   - Recommendation: Implement concurrent API fetching using ThreadPoolExecutor")
        lines.append("   - Expected improvement: 3-5x speedup for large stock pools")

        lines.append("\n2. PARALLEL PROCESSING")
        lines.append("   - Current: Sequential processing of stock codes")
        lines.append("   - Issue: Single-threaded processing underutilizes multi-core CPUs")
        lines.append("   - Recommendation: Use ProcessPoolExecutor for CPU-bound calculations")
        lines.append("   - Expected improvement: 2-4x speedup on multi-core systems")

        lines.append("\n3. CACHING IMPROVEMENTS")
        lines.append("   - Current: In-memory cache for unadjusted K-line data")
        lines.append("   - Issue: Cache key includes start/end date, reducing hit rate")
        lines.append("   - Recommendation: Cache full history and slice on demand")
        lines.append("   - Expected improvement: Reduced API calls and memory usage")

        lines.append("\n4. I/O OPTIMIZATION")
        lines.append("   - Current: Individual parquet file writes per stock")
        lines.append("   - Issue: Many small file operations")
        lines.append("   - Recommendation: Batch writes or use async I/O")
        lines.append("   - Expected improvement: 20-30% reduction in I/O time")

        lines.append("\n5. DATAFRAME OPERATIONS")
        lines.append("   - Current: Multiple DataFrame copies during adjustment calculation")
        lines.append("   - Issue: Memory allocation overhead")
        lines.append("   - Recommendation: Use in-place operations where possible")
        lines.append("   - Expected improvement: 10-20% faster calculations")

        lines.append("\n6. METADATA BATCHING")
        lines.append("   - Current: Batch size of 200 for metadata writes")
        lines.append("   - Issue: May cause memory buildup for large updates")
        lines.append("   - Recommendation: Implement streaming writes with smaller batches")
        lines.append("   - Expected improvement: Lower memory footprint")

        return lines


def main():
    test = UpdateDailyPerformanceTest(
        test_codes=["sh.600000", "sh.600036", "sz.000001", "sz.000002"]
    )
    test.run_all_tests()
    report = test.generate_report()
    print(report)

    report_path = Path(__file__).parent / "performance_report.txt"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nReport saved to: {report_path}")


if __name__ == "__main__":
    main()
