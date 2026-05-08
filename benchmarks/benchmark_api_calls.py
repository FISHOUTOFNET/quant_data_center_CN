"""Benchmark API call performance."""

from __future__ import annotations

import statistics
import time
from pathlib import Path

from src.api.market_data import DailyBarRequest, create_provider
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger
from src.utils.performance import PerformanceCollector, get_collector
from benchmark_utils import (
    BenchmarkEnvironment,
    BenchmarkReporter,
    generate_test_codes,
)


def benchmark_api_baostock_cn_stock_adjustment_factor(
    codes: list[str],
    start_date: str = "1990-01-01",
    end_date: str = "2024-12-31",
    sample_size: int = 20,
) -> dict[str, float]:
    config = ConfigManager()
    collector = PerformanceCollector()

    times = []
    errors = 0

    test_codes = codes[:sample_size]

    with create_provider(config) as provider:
        for code in test_codes:
            try:
                start = time.perf_counter()
                provider.query_baostock_cn_stock_adjustment_factor(code, start_date, end_date)
                elapsed = time.perf_counter() - start
                times.append(elapsed)
                collector.add(
                    type("TimingResult", (), {"name": "baostock_cn_stock_adjustment_factor_api", "elapsed": elapsed, "metadata": {}})()
                )
            except Exception as e:
                logger.error("API call failed for {}: {}", code, e)
                errors += 1

    if not times:
        return {"error": "All API calls failed", "error_count": errors}

    return {
        "sample_size": len(times),
        "error_count": errors,
        "total_time": sum(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
        "throughput": len(times) / sum(times) if times else 0.0,
    }


def benchmark_api_daily_bar(
    codes: list[str],
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    sample_size: int = 20,
) -> dict[str, float]:
    config = ConfigManager()
    collector = PerformanceCollector()

    times = []
    errors = 0
    total_rows = 0

    test_codes = codes[:sample_size]

    with create_provider(config) as provider:
        for code in test_codes:
            try:
                request = DailyBarRequest(
                    dataset="baostock_cn_stock_daily_bar_unadjusted",
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=config.daily_bar_fields(),
                    frequency="d",
                )

                start = time.perf_counter()
                df = provider.query_daily_bars(request)
                elapsed = time.perf_counter() - start

                times.append(elapsed)
                total_rows += len(df)
                collector.add(
                    type("TimingResult", (), {"name": "daily_bar_api", "elapsed": elapsed, "metadata": {}})()
                )
            except Exception as e:
                logger.error("API call failed for {}: {}", code, e)
                errors += 1

    if not times:
        return {"error": "All API calls failed", "error_count": errors}

    return {
        "sample_size": len(times),
        "error_count": errors,
        "total_rows": total_rows,
        "total_time": sum(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
        "throughput": len(times) / sum(times) if times else 0.0,
        "rows_per_second": total_rows / sum(times) if times else 0.0,
    }


def benchmark_api_baostock_cn_stock_basic() -> dict[str, float]:
    config = ConfigManager()

    times = []
    errors = 0
    total_rows = 0

    with create_provider(config) as provider:
        for i in range(5):
            try:
                start = time.perf_counter()
                df = provider.query_baostock_cn_stock_basic()
                elapsed = time.perf_counter() - start

                times.append(elapsed)
                total_rows = len(df)
            except Exception as e:
                logger.error("Stock basic API call failed: {}", e)
                errors += 1

    if not times:
        return {"error": "All API calls failed", "error_count": errors}

    return {
        "sample_size": len(times),
        "error_count": errors,
        "total_rows": total_rows,
        "total_time": sum(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
    }


def benchmark_api_trade_dates(
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
) -> dict[str, float]:
    config = ConfigManager()

    times = []
    errors = 0
    total_rows = 0

    with create_provider(config) as provider:
        for i in range(5):
            try:
                start = time.perf_counter()
                df = provider.query_trade_dates(start_date=start_date, end_date=end_date)
                elapsed = time.perf_counter() - start

                times.append(elapsed)
                total_rows = len(df)
            except Exception as e:
                logger.error("Trade dates API call failed: {}", e)
                errors += 1

    if not times:
        return {"error": "All API calls failed", "error_count": errors}

    return {
        "sample_size": len(times),
        "error_count": errors,
        "total_rows": total_rows,
        "total_time": sum(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "stdev": statistics.stdev(times) if len(times) >= 2 else 0.0,
        "min": min(times),
        "max": max(times),
    }


def run_api_benchmarks(output_dir: Path) -> None:
    reporter = BenchmarkReporter(output_dir)
    codes = generate_test_codes(100)

    logger.info("=" * 80)
    logger.info("API PERFORMANCE BENCHMARKS")
    logger.info("=" * 80)

    logger.info("\n1. Benchmarking baostock_cn_stock_adjustment_factor API (20 samples)...")
    result = benchmark_api_baostock_cn_stock_adjustment_factor(codes, sample_size=20)
    reporter.add_result("api_baostock_cn_stock_adjustment_factor", result)
    logger.info("   Mean: {:.3f}s, Median: {:.3f}s, Throughput: {:.2f} calls/s", result.get("mean", 0), result.get("median", 0), result.get("throughput", 0))

    logger.info("\n2. Benchmarking daily_bar API (20 samples)...")
    result = benchmark_api_daily_bar(codes, sample_size=20)
    reporter.add_result("api_daily_bar", result)
    logger.info("   Mean: {:.3f}s, Median: {:.3f}s, Rows/sec: {:.0f}", result.get("mean", 0), result.get("median", 0), result.get("rows_per_second", 0))

    logger.info("\n3. Benchmarking baostock_cn_stock_basic API (5 samples)...")
    result = benchmark_api_baostock_cn_stock_basic()
    reporter.add_result("api_baostock_cn_stock_basic", result)
    logger.info("   Mean: {:.3f}s, Rows: {}", result.get("mean", 0), result.get("total_rows", 0))

    logger.info("\n4. Benchmarking trade_dates API (5 samples)...")
    result = benchmark_api_trade_dates()
    reporter.add_result("api_trade_dates", result)
    logger.info("   Mean: {:.3f}s, Rows: {}", result.get("mean", 0), result.get("total_rows", 0))

    report_path = reporter.save_report("api_benchmark_report.json")
    logger.info("\nAPI benchmark report saved to: {}", report_path)

    logger.info("\n" + reporter.generate_summary())


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "benchmark_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        run_api_benchmarks(output_dir)
