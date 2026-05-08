"""Benchmark I/O operation performance."""

from __future__ import annotations

import statistics
import tempfile
import time
from pathlib import Path

import pandas as pd

from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger
from benchmark_utils import (
    BenchmarkEnvironment,
    BenchmarkReporter,
    generate_daily_bar_dataframe,
    generate_baostock_cn_stock_adjustment_factor_dataframe,
)


def benchmark_parquet_write(
    store: ParquetStore,
    code: str,
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"write_times": []}

    for size in sizes:
        df = generate_daily_bar_dataframe(code, "2020-01-01", "2024-12-31", rows=size)

        start = time.perf_counter()
        store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", code, df)
        elapsed = time.perf_counter() - start

        results["write_times"].append((size, elapsed))
        logger.info("Write {} rows: {:.3f}s", size, elapsed)

    return results


def benchmark_parquet_read(
    store: ParquetStore,
    code: str,
    iterations: int = 100,
) -> dict[str, float]:
    times = []

    for _ in range(iterations):
        start = time.perf_counter()
        df = store.read_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", code)
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "iterations": iterations,
        "total_time": sum(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
        "throughput": iterations / sum(times) if times else 0.0,
    }


def benchmark_baostock_cn_stock_adjustment_factor_write(
    store: ParquetStore,
    code: str,
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"write_times": []}

    for size in sizes:
        df = generate_baostock_cn_stock_adjustment_factor_dataframe(code, "2020-01-01", "2024-12-31", num_factors=size)

        start = time.perf_counter()
        store.write_baostock_cn_stock_adjustment_factor(code, df)
        elapsed = time.perf_counter() - start

        results["write_times"].append((size, elapsed))
        logger.info("Write {} factors: {:.3f}s", size, elapsed)

    return results


def benchmark_baostock_cn_stock_adjustment_factor_read(
    store: ParquetStore,
    code: str,
    iterations: int = 100,
) -> dict[str, float]:
    times = []

    for _ in range(iterations):
        start = time.perf_counter()
        df = store.read_baostock_cn_stock_adjustment_factor(code)
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "iterations": iterations,
        "total_time": sum(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
        "throughput": iterations / sum(times) if times else 0.0,
    }


def benchmark_metadata_operations(
    store: ParquetStore,
    iterations: int = 50,
) -> dict[str, float]:
    write_times = []
    read_times = []

    for i in range(iterations):
        run_row = {
            "pipeline": "test_pipeline",
            "dataset": "test_dataset",
            "code": f"sh.{600000 + i}",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "status": "success",
            "rows": 100,
            "start_time": "2024-01-01 00:00:00",
            "end_time": "2024-01-01 00:00:01",
            "error_stack": "",
        }

        start = time.perf_counter()
        store.append_pipeline_runs(pd.DataFrame([run_row]))
        write_times.append(time.perf_counter() - start)

        start = time.perf_counter()
        checkpoints = store.read_pipeline_checkpoints()
        read_times.append(time.perf_counter() - start)

    return {
        "iterations": iterations,
        "write_mean": statistics.mean(write_times),
        "write_median": statistics.median(write_times),
        "write_total": sum(write_times),
        "read_mean": statistics.mean(read_times),
        "read_median": statistics.median(read_times),
        "read_total": sum(read_times),
    }


def benchmark_batch_writes(
    store: ParquetStore,
    code: str,
    batch_sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"batch_write_times": []}

    for batch_size in batch_sizes:
        dfs = []
        for i in range(batch_size):
            df = generate_daily_bar_dataframe(
                f"sh.{600000 + i}",
                "2024-01-01",
                "2024-01-31",
                rows=100,
            )
            dfs.append(df)

        start = time.perf_counter()
        for i, df in enumerate(dfs):
            store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", f"sh.{600000 + i}", df)
        elapsed = time.perf_counter() - start

        results["batch_write_times"].append((batch_size, elapsed))
        logger.info("Batch write {} files: {:.3f}s ({:.3f}s per file)", batch_size, elapsed, elapsed / batch_size)

    return results


def benchmark_atomic_write_overhead(
    store: ParquetStore,
    code: str,
    rows: int = 1000,
    iterations: int = 10,
) -> dict[str, float]:
    times = []

    for i in range(iterations):
        df = generate_daily_bar_dataframe(code, "2024-01-01", "2024-12-31", rows=rows)

        start = time.perf_counter()
        store.write_baostock_daily_bars("baostock_cn_stock_daily_bar_unadjusted", code, df)
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "iterations": iterations,
        "rows": rows,
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
    }


def run_io_benchmarks(output_dir: Path) -> None:
    reporter = BenchmarkReporter(output_dir)

    with tempfile.TemporaryDirectory() as tmpdir:
        env = BenchmarkEnvironment(Path(tmpdir))
        env.setup()

        store = env.store
        code = "sh.600000"

        logger.info("=" * 80)
        logger.info("I/O PERFORMANCE BENCHMARKS")
        logger.info("=" * 80)

        logger.info("\n1. Benchmarking parquet write performance...")
        result = benchmark_parquet_write(store, code, [100, 1000, 10000, 50000])
        reporter.add_result("parquet_write", result)
        for size, elapsed in result["write_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n2. Benchmarking parquet read performance (100 iterations)...")
        result = benchmark_parquet_read(store, code, iterations=100)
        reporter.add_result("parquet_read", result)
        logger.info("   Mean: {:.4f}s, Median: {:.4f}s, Throughput: {:.1f} reads/s", result["mean"], result["median"], result["throughput"])

        logger.info("\n3. Benchmarking baostock_cn_stock_adjustment_factor write performance...")
        result = benchmark_baostock_cn_stock_adjustment_factor_write(store, code, [10, 50, 100, 500])
        reporter.add_result("baostock_cn_stock_adjustment_factor_write", result)
        for size, elapsed in result["write_times"]:
            logger.info("   {} factors: {:.3f}s", size, elapsed)

        logger.info("\n4. Benchmarking baostock_cn_stock_adjustment_factor read performance (100 iterations)...")
        result = benchmark_baostock_cn_stock_adjustment_factor_read(store, code, iterations=100)
        reporter.add_result("baostock_cn_stock_adjustment_factor_read", result)
        logger.info("   Mean: {:.4f}s, Median: {:.4f}s", result["mean"], result["median"])

        logger.info("\n5. Benchmarking metadata operations (50 iterations)...")
        result = benchmark_metadata_operations(store, iterations=50)
        reporter.add_result("metadata_operations", result)
        logger.info("   Write mean: {:.4f}s, Read mean: {:.4f}s", result["write_mean"], result["read_mean"])

        logger.info("\n6. Benchmarking batch writes...")
        result = benchmark_batch_writes(store, code, [10, 50, 100])
        reporter.add_result("batch_writes", result)
        for size, elapsed in result["batch_write_times"]:
            logger.info("   {} files: {:.3f}s ({:.3f}s per file)", size, elapsed, elapsed / size)

        logger.info("\n7. Benchmarking atomic write overhead...")
        result = benchmark_atomic_write_overhead(store, code, rows=1000, iterations=10)
        reporter.add_result("atomic_write_overhead", result)
        logger.info("   Mean: {:.3f}s, Median: {:.3f}s", result["mean"], result["median"])

        report_path = reporter.save_report("io_benchmark_report.json")
        logger.info("\nI/O benchmark report saved to: {}", report_path)

        logger.info("\n" + reporter.generate_summary())

        env.teardown()


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "benchmark_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        run_io_benchmarks(output_dir)
