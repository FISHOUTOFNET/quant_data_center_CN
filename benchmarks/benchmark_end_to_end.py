"""End-to-end performance benchmark."""

from __future__ import annotations

import cProfile
import io
import pstats
import tempfile
import time
from pathlib import Path
from pstats import SortKey

from src.pipeline.update_daily import update_daily
from src.utils.logging import logger
from src.utils.performance import get_collector, get_memory_monitor
from benchmark_utils import (
    BenchmarkEnvironment,
    BenchmarkReporter,
    generate_test_codes,
)


def benchmark_first_full_update(
    env: BenchmarkEnvironment,
    codes: list[str],
    end_date: str = "2024-01-31",
) -> dict[str, float]:
    logger.info("Running first full update benchmark...")

    collector = get_collector()
    collector.clear()

    memory_monitor = get_memory_monitor()
    memory_monitor.snapshot("before_update")

    start = time.perf_counter()
    records = update_daily(
        dataset="daily_k_none",
        code=codes[:10],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=False,
        mode="full",
        force=True,
    )
    elapsed = time.perf_counter() - start

    memory_monitor.snapshot("after_update")
    memory_diff = memory_monitor.get_memory_diff("before_update", "after_update")

    stats = collector.get_statistics()

    return {
        "elapsed": elapsed,
        "records_count": len(records),
        "codes_processed": len(codes[:10]),
        "memory_diff_mb": memory_diff / 1024 / 1024,
        "performance_stats": stats,
    }


def benchmark_incremental_update(
    env: BenchmarkEnvironment,
    codes: list[str],
    end_date: str = "2024-01-31",
) -> dict[str, float]:
    logger.info("Running incremental update benchmark...")

    collector = get_collector()
    collector.clear()

    start = time.perf_counter()
    records = update_daily(
        dataset="daily_k_none",
        code=codes[:10],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=False,
        mode="partial",
        force=False,
    )
    elapsed = time.perf_counter() - start

    stats = collector.get_statistics()

    return {
        "elapsed": elapsed,
        "records_count": len(records),
        "codes_processed": len(codes[:10]),
        "performance_stats": stats,
    }


def benchmark_resume_mode(
    env: BenchmarkEnvironment,
    codes: list[str],
    end_date: str = "2024-01-31",
) -> dict[str, float]:
    logger.info("Running resume mode benchmark...")

    update_daily(
        dataset="daily_k_none",
        code=codes[:5],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=False,
        mode="partial",
        force=True,
    )

    collector = get_collector()
    collector.clear()

    start = time.perf_counter()
    records = update_daily(
        dataset="daily_k_none",
        code=codes[:10],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=False,
        mode="partial",
        resume=True,
        force=False,
    )
    elapsed = time.perf_counter() - start

    stats = collector.get_statistics()

    return {
        "elapsed": elapsed,
        "records_count": len(records),
        "codes_processed": len(codes[:10]),
        "codes_skipped": 5,
        "performance_stats": stats,
    }


def benchmark_with_profiling(
    env: BenchmarkEnvironment,
    codes: list[str],
    end_date: str = "2024-01-31",
) -> dict[str, str]:
    logger.info("Running profiled update...")

    profiler = cProfile.Profile()
    profiler.enable()

    start = time.perf_counter()
    records = update_daily(
        dataset="daily_k_none",
        code=codes[:5],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=False,
        mode="partial",
        force=True,
    )
    elapsed = time.perf_counter() - start

    profiler.disable()

    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats(SortKey.CUMULATIVE)
    stats.print_stats(30)

    profile_output = stream.getvalue()

    return {
        "elapsed": elapsed,
        "records_count": len(records),
        "profile": profile_output,
    }


def benchmark_all_datasets(
    env: BenchmarkEnvironment,
    codes: list[str],
    end_date: str = "2024-01-31",
) -> dict[str, float]:
    logger.info("Running all datasets update benchmark...")

    collector = get_collector()
    collector.clear()

    start = time.perf_counter()
    records = update_daily(
        dataset="all",
        code=codes[:5],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=True,
        mode="partial",
        force=True,
    )
    elapsed = time.perf_counter() - start

    stats = collector.get_statistics()

    return {
        "elapsed": elapsed,
        "records_count": len(records),
        "codes_processed": len(codes[:5]),
        "performance_stats": stats,
    }


def benchmark_large_scale(
    env: BenchmarkEnvironment,
    codes: list[str],
    end_date: str = "2024-01-31",
) -> dict[str, float]:
    logger.info("Running large scale update benchmark...")

    collector = get_collector()
    collector.clear()

    memory_monitor = get_memory_monitor()
    memory_monitor.snapshot("before_large_scale")

    start = time.perf_counter()
    records = update_daily(
        dataset="daily_k_none",
        code=codes[:50],
        end=end_date,
        lookback_days=30,
        root=env.root,
        build_views=False,
        mode="partial",
        force=True,
    )
    elapsed = time.perf_counter() - start

    memory_monitor.snapshot("after_large_scale")
    memory_diff = memory_monitor.get_memory_diff("before_large_scale", "after_large_scale")

    stats = collector.get_statistics()

    return {
        "elapsed": elapsed,
        "records_count": len(records),
        "codes_processed": len(codes[:50]),
        "memory_diff_mb": memory_diff / 1024 / 1024,
        "throughput": len(codes[:50]) / elapsed if elapsed > 0 else 0,
        "performance_stats": stats,
    }


def run_end_to_end_benchmarks(output_dir: Path) -> None:
    reporter = BenchmarkReporter(output_dir)

    with tempfile.TemporaryDirectory() as tmpdir:
        env = BenchmarkEnvironment(Path(tmpdir))
        env.setup()
        codes = generate_test_codes(100)

        logger.info("=" * 80)
        logger.info("END-TO-END PERFORMANCE BENCHMARKS")
        logger.info("=" * 80)

        logger.info("\n1. Benchmarking first full update (10 codes)...")
        result = benchmark_first_full_update(env, codes)
        reporter.add_result("first_full_update", {k: v for k, v in result.items() if k != "performance_stats"})
        logger.info("   Elapsed: {:.3f}s, Records: {}, Memory: {:.2f} MB", result["elapsed"], result["records_count"], result["memory_diff_mb"])

        logger.info("\n2. Benchmarking incremental update (10 codes)...")
        result = benchmark_incremental_update(env, codes)
        reporter.add_result("incremental_update", {k: v for k, v in result.items() if k != "performance_stats"})
        logger.info("   Elapsed: {:.3f}s, Records: {}", result["elapsed"], result["records_count"])

        logger.info("\n3. Benchmarking resume mode (10 codes, 5 already processed)...")
        result = benchmark_resume_mode(env, codes)
        reporter.add_result("resume_mode", {k: v for k, v in result.items() if k != "performance_stats"})
        logger.info("   Elapsed: {:.3f}s, Records: {}, Skipped: {}", result["elapsed"], result["records_count"], result["codes_skipped"])

        logger.info("\n4. Benchmarking with profiling (5 codes)...")
        result = benchmark_with_profiling(env, codes)
        reporter.add_result("profiled_update", {k: v for k, v in result.items() if k != "profile"})
        logger.info("   Elapsed: {:.3f}s", result["elapsed"])

        profile_path = output_dir / "profile_output.txt"
        with profile_path.open("w", encoding="utf-8") as f:
            f.write(result["profile"])
        logger.info("   Profile saved to: {}", profile_path)

        logger.info("\n5. Benchmarking all datasets (5 codes)...")
        result = benchmark_all_datasets(env, codes)
        reporter.add_result("all_datasets_update", {k: v for k, v in result.items() if k != "performance_stats"})
        logger.info("   Elapsed: {:.3f}s, Records: {}", result["elapsed"], result["records_count"])

        logger.info("\n6. Benchmarking large scale (50 codes)...")
        result = benchmark_large_scale(env, codes)
        reporter.add_result("large_scale_update", {k: v for k, v in result.items() if k != "performance_stats"})
        logger.info("   Elapsed: {:.3f}s, Throughput: {:.2f} codes/s, Memory: {:.2f} MB", result["elapsed"], result["throughput"], result["memory_diff_mb"])

        report_path = reporter.save_report("end_to_end_benchmark_report.json")
        logger.info("\nEnd-to-end benchmark report saved to: {}", report_path)

        logger.info("\n" + reporter.generate_summary())

        env.teardown()


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "benchmark_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        run_end_to_end_benchmarks(output_dir)
