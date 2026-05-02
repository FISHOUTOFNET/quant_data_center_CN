"""Main benchmark runner for comprehensive performance testing."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

from src.utils.logging import logger
from benchmark_api_calls import run_api_benchmarks
from benchmark_io_operations import run_io_benchmarks
from benchmark_data_processing import run_data_processing_benchmarks
from benchmark_concurrency import run_concurrency_benchmarks
from benchmark_end_to_end import run_end_to_end_benchmarks


def run_all_benchmarks(output_dir: Path, skip_long_tests: bool = False) -> dict[str, float]:
    output_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.perf_counter()
    all_results = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "skip_long_tests": skip_long_tests,
        },
        "benchmarks": {},
    }

    logger.info("=" * 80)
    logger.info("COMPREHENSIVE PERFORMANCE BENCHMARK SUITE")
    logger.info("=" * 80)
    logger.info("Output directory: {}", output_dir)
    logger.info("Timestamp: {}", all_results["metadata"]["timestamp"])
    logger.info("=" * 80)

    try:
        logger.info("\n" + "=" * 80)
        logger.info("SECTION 1: API CALL BENCHMARKS")
        logger.info("=" * 80)
        section_start = time.perf_counter()
        run_api_benchmarks(output_dir)
        all_results["benchmarks"]["api"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "completed",
        }
    except Exception as e:
        logger.exception("API benchmarks failed")
        all_results["benchmarks"]["api"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "failed",
            "error": str(e),
        }

    try:
        logger.info("\n" + "=" * 80)
        logger.info("SECTION 2: I/O OPERATIONS BENCHMARKS")
        logger.info("=" * 80)
        section_start = time.perf_counter()
        run_io_benchmarks(output_dir)
        all_results["benchmarks"]["io"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "completed",
        }
    except Exception as e:
        logger.exception("I/O benchmarks failed")
        all_results["benchmarks"]["io"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "failed",
            "error": str(e),
        }

    try:
        logger.info("\n" + "=" * 80)
        logger.info("SECTION 3: DATA PROCESSING BENCHMARKS")
        logger.info("=" * 80)
        section_start = time.perf_counter()
        run_data_processing_benchmarks(output_dir)
        all_results["benchmarks"]["data_processing"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "completed",
        }
    except Exception as e:
        logger.exception("Data processing benchmarks failed")
        all_results["benchmarks"]["data_processing"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "failed",
            "error": str(e),
        }

    try:
        logger.info("\n" + "=" * 80)
        logger.info("SECTION 4: CONCURRENCY BENCHMARKS")
        logger.info("=" * 80)
        section_start = time.perf_counter()
        run_concurrency_benchmarks(output_dir)
        all_results["benchmarks"]["concurrency"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "completed",
        }
    except Exception as e:
        logger.exception("Concurrency benchmarks failed")
        all_results["benchmarks"]["concurrency"] = {
            "elapsed": time.perf_counter() - section_start,
            "status": "failed",
            "error": str(e),
        }

    if not skip_long_tests:
        try:
            logger.info("\n" + "=" * 80)
            logger.info("SECTION 5: END-TO-END BENCHMARKS")
            logger.info("=" * 80)
            section_start = time.perf_counter()
            run_end_to_end_benchmarks(output_dir)
            all_results["benchmarks"]["end_to_end"] = {
                "elapsed": time.perf_counter() - section_start,
                "status": "completed",
            }
        except Exception as e:
            logger.exception("End-to-end benchmarks failed")
            all_results["benchmarks"]["end_to_end"] = {
                "elapsed": time.perf_counter() - section_start,
                "status": "failed",
                "error": str(e),
            }
    else:
        logger.info("\nSkipping end-to-end benchmarks (skip_long_tests=True)")
        all_results["benchmarks"]["end_to_end"] = {
            "elapsed": 0,
            "status": "skipped",
        }

    total_elapsed = time.perf_counter() - start_time
    all_results["total_elapsed"] = total_elapsed

    summary_path = output_dir / "benchmark_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, default=str)

    logger.info("\n" + "=" * 80)
    logger.info("BENCHMARK SUITE COMPLETED")
    logger.info("=" * 80)
    logger.info("Total elapsed time: {:.3f}s", total_elapsed)
    logger.info("Summary saved to: {}", summary_path)

    print_summary(all_results)

    return all_results


def print_summary(results: dict) -> None:
    logger.info("\n" + "=" * 80)
    logger.info("BENCHMARK SUMMARY")
    logger.info("=" * 80)

    for benchmark_name, benchmark_data in results["benchmarks"].items():
        status = benchmark_data.get("status", "unknown")
        elapsed = benchmark_data.get("elapsed", 0)
        logger.info("\n{}: {} ({:.3f}s)", benchmark_name.upper(), status, elapsed)
        if status == "failed":
            logger.info("  Error: {}", benchmark_data.get("error", "Unknown error"))

    logger.info("\n" + "-" * 80)
    logger.info("TOTAL TIME: {:.3f}s", results.get("total_elapsed", 0))
    logger.info("=" * 80)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run comprehensive performance benchmarks")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmark_results"),
        help="Output directory for benchmark results",
    )
    parser.add_argument(
        "--skip-long-tests",
        action="store_true",
        help="Skip long-running end-to-end tests",
    )

    args = parser.parse_args()

    try:
        results = run_all_benchmarks(
            output_dir=args.output_dir,
            skip_long_tests=args.skip_long_tests,
        )

        failed_benchmarks = [
            name for name, data in results["benchmarks"].items()
            if data.get("status") == "failed"
        ]

        if failed_benchmarks:
            logger.error("Some benchmarks failed: {}", ", ".join(failed_benchmarks))
            return 1

        return 0
    except Exception as e:
        logger.exception("Benchmark suite failed with error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
