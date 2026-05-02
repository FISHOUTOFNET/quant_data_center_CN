"""Benchmark data processing performance."""

from __future__ import annotations

import statistics
import tempfile
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa

from src.pipeline.adjustments import calculate_adjusted_daily_k
from src.storage.dataset_catalog import daily_k_definition
from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger
from benchmark_utils import (
    BenchmarkEnvironment,
    BenchmarkReporter,
    generate_daily_k_dataframe,
    generate_adjust_factor_dataframe,
)


def benchmark_clean_dataframe(
    store: ParquetStore,
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"clean_times": []}
    schema = daily_k_definition("daily_k_none").schema

    for size in sizes:
        df = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)

        start = time.perf_counter()
        cleaned = store.clean_dataframe_for_schema(df, schema)
        elapsed = time.perf_counter() - start

        results["clean_times"].append((size, elapsed))
        logger.info("Clean {} rows: {:.3f}s ({:.0f} rows/s)", size, elapsed, size / elapsed if elapsed > 0 else 0)

    return results


def benchmark_adjust_calculation(
    sizes: list[int],
    num_factors: int = 10,
) -> dict[str, list[tuple[int, float]]]:
    results = {"adjust_times": []}

    for size in sizes:
        unadjusted = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)
        factors = generate_adjust_factor_dataframe("sh.600000", "2020-01-01", "2024-12-31", num_factors=num_factors)

        start = time.perf_counter()
        adjusted = calculate_adjusted_daily_k(unadjusted, factors, "daily_k_qfq", "2")
        elapsed = time.perf_counter() - start

        results["adjust_times"].append((size, elapsed))
        logger.info("Adjust {} rows with {} factors: {:.3f}s ({:.0f} rows/s)", size, num_factors, elapsed, size / elapsed if elapsed > 0 else 0)

    return results


def benchmark_merge_operations(
    store: ParquetStore,
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    from src.pipeline.common import merge_daily_frames

    results = {"merge_times": []}

    for size in sizes:
        existing = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2023-12-31", rows=size // 2)
        fresh = generate_daily_k_dataframe("sh.600000", "2024-01-01", "2024-12-31", rows=size // 2)

        start = time.perf_counter()
        merged = merge_daily_frames(store, existing, fresh)
        elapsed = time.perf_counter() - start

        results["merge_times"].append((size, elapsed))
        logger.info("Merge {} total rows: {:.3f}s", size, elapsed)

    return results


def benchmark_sorting_operations(
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"sort_times": []}

    for size in sizes:
        df = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)
        df = df.sample(frac=1).reset_index(drop=True)

        start = time.perf_counter()
        sorted_df = df.sort_values(["code", "date"]).reset_index(drop=True)
        elapsed = time.perf_counter() - start

        results["sort_times"].append((size, elapsed))
        logger.info("Sort {} rows: {:.3f}s", size, elapsed)

    return results


def benchmark_deduplication_operations(
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"dedup_times": []}

    for size in sizes:
        df1 = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)
        df2 = generate_daily_k_dataframe("sh.600000", "2024-01-01", "2024-12-31", rows=size // 4)
        df = pd.concat([df1, df2], ignore_index=True)

        start = time.perf_counter()
        deduped = df.drop_duplicates(["code", "date"], keep="last").reset_index(drop=True)
        elapsed = time.perf_counter() - start

        results["dedup_times"].append((size, elapsed))
        logger.info("Deduplicate {} rows: {:.3f}s", len(df), elapsed)

    return results


def benchmark_type_conversion(
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"conversion_times": []}

    for size in sizes:
        df = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)

        numeric_columns = ["open", "high", "low", "close", "preclose", "volume", "amount"]

        start = time.perf_counter()
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elapsed = time.perf_counter() - start

        results["conversion_times"].append((size, elapsed))
        logger.info("Type conversion {} rows: {:.3f}s", size, elapsed)

    return results


def benchmark_dataframe_copy(
    sizes: list[int],
) -> dict[str, list[tuple[int, float]]]:
    results = {"copy_times": []}

    for size in sizes:
        df = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)

        start = time.perf_counter()
        copied = df.copy()
        elapsed = time.perf_counter() - start

        results["copy_times"].append((size, elapsed))
        logger.info("Copy {} rows: {:.3f}s", size, elapsed)

    return results


def benchmark_merge_asof_operations(
    sizes: list[int],
    num_factors: int = 10,
) -> dict[str, list[tuple[int, float]]]:
    results = {"merge_asof_times": []}

    for size in sizes:
        daily_df = generate_daily_k_dataframe("sh.600000", "2020-01-01", "2024-12-31", rows=size)
        factor_df = generate_adjust_factor_dataframe("sh.600000", "2020-01-01", "2024-12-31", num_factors=num_factors)

        daily_df["_date_key"] = pd.to_datetime(daily_df["date"], errors="coerce")
        factor_df["_factor_date"] = pd.to_datetime(factor_df["dividOperateDate"], errors="coerce")

        daily_sorted = daily_df.dropna(subset=["_date_key"]).sort_values("_date_key")
        factor_sorted = factor_df.dropna(subset=["_factor_date"]).sort_values("_factor_date")

        start = time.perf_counter()
        merged = pd.merge_asof(
            daily_sorted,
            factor_sorted[["_factor_date", "foreAdjustFactor"]],
            left_on="_date_key",
            right_on="_factor_date",
            direction="backward",
        )
        elapsed = time.perf_counter() - start

        results["merge_asof_times"].append((size, elapsed))
        logger.info("Merge_asof {} rows with {} factors: {:.3f}s", size, num_factors, elapsed)

    return results


def run_data_processing_benchmarks(output_dir: Path) -> None:
    reporter = BenchmarkReporter(output_dir)

    with tempfile.TemporaryDirectory() as tmpdir:
        env = BenchmarkEnvironment(Path(tmpdir))
        env.setup()
        store = env.store

        logger.info("=" * 80)
        logger.info("DATA PROCESSING PERFORMANCE BENCHMARKS")
        logger.info("=" * 80)

        logger.info("\n1. Benchmarking clean_dataframe operations...")
        result = benchmark_clean_dataframe(store, [1000, 10000, 100000, 500000])
        reporter.add_result("clean_dataframe", result)
        for size, elapsed in result["clean_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n2. Benchmarking adjust calculation...")
        result = benchmark_adjust_calculation([1000, 10000, 100000, 500000], num_factors=10)
        reporter.add_result("adjust_calculation", result)
        for size, elapsed in result["adjust_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n3. Benchmarking merge operations...")
        result = benchmark_merge_operations(store, [1000, 10000, 100000])
        reporter.add_result("merge_operations", result)
        for size, elapsed in result["merge_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n4. Benchmarking sorting operations...")
        result = benchmark_sorting_operations([1000, 10000, 100000, 500000])
        reporter.add_result("sorting_operations", result)
        for size, elapsed in result["sort_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n5. Benchmarking deduplication operations...")
        result = benchmark_deduplication_operations([1000, 10000, 100000])
        reporter.add_result("deduplication_operations", result)
        for size, elapsed in result["dedup_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n6. Benchmarking type conversion...")
        result = benchmark_type_conversion([1000, 10000, 100000, 500000])
        reporter.add_result("type_conversion", result)
        for size, elapsed in result["conversion_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n7. Benchmarking DataFrame copy...")
        result = benchmark_dataframe_copy([1000, 10000, 100000, 500000])
        reporter.add_result("dataframe_copy", result)
        for size, elapsed in result["copy_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        logger.info("\n8. Benchmarking merge_asof operations...")
        result = benchmark_merge_asof_operations([1000, 10000, 100000], num_factors=10)
        reporter.add_result("merge_asof_operations", result)
        for size, elapsed in result["merge_asof_times"]:
            logger.info("   {} rows: {:.3f}s", size, elapsed)

        report_path = reporter.save_report("data_processing_benchmark_report.json")
        logger.info("\nData processing benchmark report saved to: {}", report_path)

        logger.info("\n" + reporter.generate_summary())

        env.teardown()


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "benchmark_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        run_data_processing_benchmarks(output_dir)
