"""Benchmark concurrency performance."""

from __future__ import annotations

import statistics
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.storage.parquet_store import ParquetStore
from src.utils.logging import logger
from benchmark_utils import (
    BenchmarkEnvironment,
    BenchmarkReporter,
    generate_daily_k_dataframe,
    generate_test_codes,
)


def benchmark_thread_pool_write(
    store: ParquetStore,
    codes: list[str],
    max_workers_list: list[int],
    rows_per_code: int = 100,
) -> dict[str, list[tuple[int, float, float]]]:
    results = {"concurrent_writes": []}

    for max_workers in max_workers_list:
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for code in codes:
                df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=rows_per_code)
                future = executor.submit(store.write_daily_k, "daily_k_none", code, df)
                futures[future] = code

            completed = 0
            for future in as_completed(futures):
                try:
                    future.result()
                    completed += 1
                except Exception as e:
                    logger.error("Write failed for {}: {}", futures[future], e)

        elapsed = time.perf_counter() - start
        throughput = completed / elapsed if elapsed > 0 else 0

        results["concurrent_writes"].append((max_workers, elapsed, throughput))
        logger.info("Max workers: {}, Time: {:.3f}s, Throughput: {:.2f} writes/s", max_workers, elapsed, throughput)

    return results


def benchmark_thread_pool_read(
    store: ParquetStore,
    codes: list[str],
    max_workers_list: list[int],
    iterations: int = 10,
) -> dict[str, list[tuple[int, float, float]]]:
    results = {"concurrent_reads": []}

    for code in codes[:10]:
        df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=1000)
        store.write_daily_k("daily_k_none", code, df)

    for max_workers in max_workers_list:
        times = []

        for _ in range(iterations):
            start = time.perf_counter()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(store.read_daily_k, "daily_k_none", code) for code in codes[:10]]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error("Read failed: {}", e)

            elapsed = time.perf_counter() - start
            times.append(elapsed)

        mean_time = statistics.mean(times)
        throughput = 10 / mean_time if mean_time > 0 else 0

        results["concurrent_reads"].append((max_workers, mean_time, throughput))
        logger.info("Max workers: {}, Mean time: {:.3f}s, Throughput: {:.2f} reads/s", max_workers, mean_time, throughput)

    return results


def benchmark_mixed_operations(
    store: ParquetStore,
    codes: list[str],
    max_workers_list: list[int],
    read_ratio: float = 0.7,
) -> dict[str, list[tuple[int, float, float]]]:
    results = {"mixed_operations": []}

    for code in codes[:10]:
        df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=1000)
        store.write_daily_k("daily_k_none", code, df)

    for max_workers in max_workers_list:
        start = time.perf_counter()
        operations = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, code in enumerate(codes[:20]):
                if i < int(20 * read_ratio):
                    future = executor.submit(store.read_daily_k, "daily_k_none", code)
                else:
                    df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=100)
                    future = executor.submit(store.write_daily_k, "daily_k_none", code, df)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                    operations += 1
                except Exception as e:
                    logger.error("Operation failed: {}", e)

        elapsed = time.perf_counter() - start
        throughput = operations / elapsed if elapsed > 0 else 0

        results["mixed_operations"].append((max_workers, elapsed, throughput))
        logger.info("Max workers: {}, Time: {:.3f}s, Throughput: {:.2f} ops/s", max_workers, elapsed, throughput)

    return results


def benchmark_lock_contention(
    store: ParquetStore,
    codes: list[str],
    max_workers_list: list[int],
) -> dict[str, list[tuple[int, float]]]:
    from src.pipeline.services import PipelineMetadataBatch

    results = {"lock_contention": []}

    for max_workers in max_workers_list:
        metadata_batch = PipelineMetadataBatch(store, flush_size=10, count_by="run")

        start = time.perf_counter()

        def write_metadata(code: str) -> None:
            for i in range(10):
                metadata_batch.add(
                    run_row={
                        "pipeline": "test",
                        "dataset": "test_dataset",
                        "code": code,
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-31",
                        "status": "success",
                        "rows": 100,
                        "start_time": "2024-01-01 00:00:00",
                        "end_time": "2024-01-01 00:00:01",
                        "error_stack": "",
                    }
                )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(write_metadata, code) for code in codes[:20]]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error("Metadata write failed: {}", e)

        metadata_batch.flush()
        elapsed = time.perf_counter() - start

        results["lock_contention"].append((max_workers, elapsed))
        logger.info("Max workers: {}, Time: {:.3f}s", max_workers, elapsed)

    return results


def benchmark_sequential_vs_parallel(
    store: ParquetStore,
    codes: list[str],
    rows_per_code: int = 100,
) -> dict[str, float]:
    results = {}

    start = time.perf_counter()
    for code in codes[:20]:
        df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=rows_per_code)
        store.write_daily_k("daily_k_none", code, df)
    sequential_time = time.perf_counter() - start
    results["sequential_time"] = sequential_time
    logger.info("Sequential write time: {:.3f}s", sequential_time)

    for code in codes[20:40]:
        df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=rows_per_code)
        store.write_daily_k("daily_k_none", code, df)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for code in codes[40:60]:
            df = generate_daily_k_dataframe(code, "2024-01-01", "2024-12-31", rows=rows_per_code)
            future = executor.submit(store.write_daily_k, "daily_k_none", code, df)
            futures.append(future)
        for future in as_completed(futures):
            future.result()
    parallel_time = time.perf_counter() - start
    results["parallel_time_4_workers"] = parallel_time
    logger.info("Parallel write time (4 workers): {:.3f}s", parallel_time)

    results["speedup"] = sequential_time / parallel_time if parallel_time > 0 else 0
    logger.info("Speedup: {:.2f}x", results["speedup"])

    return results


def run_concurrency_benchmarks(output_dir: Path) -> None:
    reporter = BenchmarkReporter(output_dir)

    with tempfile.TemporaryDirectory() as tmpdir:
        env = BenchmarkEnvironment(Path(tmpdir))
        env.setup()
        store = env.store
        codes = generate_test_codes(100)

        logger.info("=" * 80)
        logger.info("CONCURRENCY PERFORMANCE BENCHMARKS")
        logger.info("=" * 80)

        logger.info("\n1. Benchmarking thread pool write performance...")
        result = benchmark_thread_pool_write(store, codes[:50], [1, 2, 4, 8, 16], rows_per_code=100)
        reporter.add_result("thread_pool_write", result)
        for workers, elapsed, throughput in result["concurrent_writes"]:
            logger.info("   Workers: {}, Time: {:.3f}s, Throughput: {:.2f} writes/s", workers, elapsed, throughput)

        logger.info("\n2. Benchmarking thread pool read performance...")
        result = benchmark_thread_pool_read(store, codes[:10], [1, 2, 4, 8, 16], iterations=5)
        reporter.add_result("thread_pool_read", result)
        for workers, elapsed, throughput in result["concurrent_reads"]:
            logger.info("   Workers: {}, Time: {:.3f}s, Throughput: {:.2f} reads/s", workers, elapsed, throughput)

        logger.info("\n3. Benchmarking mixed read/write operations...")
        result = benchmark_mixed_operations(store, codes[:20], [1, 2, 4, 8])
        reporter.add_result("mixed_operations", result)
        for workers, elapsed, throughput in result["mixed_operations"]:
            logger.info("   Workers: {}, Time: {:.3f}s, Throughput: {:.2f} ops/s", workers, elapsed, throughput)

        logger.info("\n4. Benchmarking lock contention...")
        result = benchmark_lock_contention(store, codes[:20], [1, 2, 4, 8, 16])
        reporter.add_result("lock_contention", result)
        for workers, elapsed in result["lock_contention"]:
            logger.info("   Workers: {}, Time: {:.3f}s", workers, elapsed)

        logger.info("\n5. Benchmarking sequential vs parallel...")
        result = benchmark_sequential_vs_parallel(store, codes, rows_per_code=100)
        reporter.add_result("sequential_vs_parallel", result)
        logger.info("   Sequential: {:.3f}s, Parallel: {:.3f}s, Speedup: {:.2f}x", result["sequential_time"], result["parallel_time_4_workers"], result["speedup"])

        report_path = reporter.save_report("concurrency_benchmark_report.json")
        logger.info("\nConcurrency benchmark report saved to: {}", report_path)

        logger.info("\n" + reporter.generate_summary())

        env.teardown()


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "benchmark_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        run_concurrency_benchmarks(output_dir)
