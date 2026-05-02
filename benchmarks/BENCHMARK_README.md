# Performance Benchmark Suite

This directory contains comprehensive performance benchmarks for the `update_daily.py` pipeline.

## Overview

The benchmark suite is designed to identify performance bottlenecks in the following areas. The current `update_daily`
implementation keeps provider API calls on the main thread and overlaps storage/dataframe work through a background
worker pool (`pipeline.background_workers`, default `3`).

1. **API Calls** - Measure latency and throughput of Baostock API calls
2. **I/O Operations** - Measure Parquet file read/write performance
3. **Data Processing** - Measure data cleaning, adjustment calculation, and merge operations
4. **Concurrency** - Measure thread pool performance and lock contention
5. **End-to-End** - Measure complete pipeline performance with profiling

## Quick Start

### Run All Benchmarks

```bash
python tests/run_all_benchmarks.py --output-dir benchmark_results
```

### Run Specific Benchmark Suites

```bash
# API benchmarks
python tests/benchmark_api_calls.py

# I/O benchmarks
python tests/benchmark_io_operations.py

# Data processing benchmarks
python tests/benchmark_data_processing.py

# Concurrency benchmarks
python tests/benchmark_concurrency.py

# End-to-end benchmarks
python tests/benchmark_end_to_end.py
```

### Skip Long-Running Tests

```bash
python tests/run_all_benchmarks.py --skip-long-tests
```

## Benchmark Scripts

### 1. `benchmark_api_calls.py`

Tests API call performance:
- Adjust factor API latency
- Daily K API latency
- Stock basic API latency
- Trade dates API latency

**Key Metrics:**
- Mean/median/min/max latency
- Throughput (calls per second)
- Error rate

### 2. `benchmark_io_operations.py`

Tests I/O performance:
- Parquet write performance (different row counts)
- Parquet read performance (multiple iterations)
- Adjust factor write/read performance
- DuckDB-backed metadata operations performance
- Batch write performance
- Atomic write overhead

**Key Metrics:**
- Write time per row count
- Read throughput
- Metadata operation latency

### 3. `benchmark_data_processing.py`

Tests data processing performance:
- DataFrame cleaning operations
- Adjustment calculation
- Merge operations
- Sorting operations
- Deduplication operations
- Type conversion
- DataFrame copy
- Merge_asof operations

**Key Metrics:**
- Processing time per row count
- Rows per second throughput

### 4. `benchmark_concurrency.py`

Tests concurrency performance:
- Thread pool write performance (different worker counts)
- Thread pool read performance
- Mixed read/write operations
- Lock contention
- Sequential vs parallel comparison

**Key Metrics:**
- Throughput at different worker counts
- Speedup ratio
- Lock contention impact

### 5. `benchmark_end_to_end.py`

Tests complete pipeline performance:
- First full update
- Incremental update
- Resume mode
- Profiled execution
- All datasets update
- Large scale update

**Key Metrics:**
- Total elapsed time
- Memory usage
- Code throughput
- Profile analysis

## Output Files

Each benchmark generates:
- JSON report with detailed metrics
- Console output with summary
- Profile data (for end-to-end tests)

Example output structure:
```
benchmark_results/
├── api_benchmark_report.json
├── io_benchmark_report.json
├── data_processing_benchmark_report.json
├── concurrency_benchmark_report.json
├── end_to_end_benchmark_report.json
├── profile_output.txt
└── benchmark_summary.json
```

The checked-in `benchmark_results/` reports are historical benchmark outputs. They should be refreshed by rerunning the
suite when performance-sensitive implementation details change.

## Performance Monitoring Tools

### `src/utils/performance.py`

Provides utilities for performance monitoring:
- `PerformanceTimer` - Context manager for timing code blocks
- `PerformanceCollector` - Collects and aggregates timing data
- `MemoryMonitor` - Tracks memory usage
- `FunctionProfiler` - Profiles function calls

### Usage Example

```python
from src.utils.performance import PerformanceTimer, get_collector

# Time a code block
with PerformanceTimer("my_operation"):
    # ... code to measure ...

# Collect timing statistics
collector = get_collector()
stats = collector.get_statistics()
print(collector.summary())
```

## Interpreting Results

### Performance Bottlenecks

Based on code analysis, the main bottlenecks are:

1. **API Calls (High Priority)**
   - Serial API calls for each stock code
   - Network latency and retry delays
   - Expected improvement: 30-50%

2. **I/O Operations (High Priority)**
   - Atomic write overhead
   - Windows file lock issues
   - DuckDB metadata table updates through `ParquetStore`
   - Expected improvement: 20-30%

3. **Data Processing (Medium Priority)**
   - DataFrame cleaning and type conversion
   - Adjustment calculation with merge_asof
   - Expected improvement: 10-20%

4. **Concurrency (Medium Priority)**
   - Fixed thread pool size (4 workers)
   - Pending background task cap defaults to 16 when `background_max_pending` is unset
   - Lock contention in metadata and Parquet writes
   - Expected improvement: 20-40%

5. **Metadata Operations (Low Priority)**
   - Full table upserts
   - Expected improvement: 5-10%

### Optimization Recommendations

1. **API Parallelization**
   - Evaluate provider-safe API parallelization before moving API calls off the main thread
   - Implement request batching
   - Add caching layer

2. **I/O Optimization**
   - Use async I/O operations
   - Batch small writes
   - Optimize Parquet compression

3. **Concurrency Tuning**
   - Dynamic thread pool sizing
   - Reduce lock scope
   - Use lock-free data structures

4. **Memory Optimization**
   - Stream large datasets
   - Use efficient data types
   - Implement lazy evaluation

## Custom Benchmarks

To create custom benchmarks:

1. Use `BenchmarkEnvironment` for test setup
2. Use `BenchmarkReporter` for result collection
3. Follow the pattern in existing benchmarks

Example:
```python
from benchmarks.benchmark_utils import BenchmarkEnvironment, BenchmarkReporter
from src.utils.performance import PerformanceTimer

def my_benchmark():
    with tempfile.TemporaryDirectory() as tmpdir:
        env = BenchmarkEnvironment(Path(tmpdir))
        env.setup()
        
        with PerformanceTimer("my_operation"):
            # Your code here
            pass
        
        env.teardown()
```

## Continuous Integration

To run benchmarks in CI:

```yaml
# .github/workflows/benchmark.yml
- name: Run Performance Benchmarks
  run: python tests/run_all_benchmarks.py --skip-long-tests --output-dir benchmark_results

- name: Upload Benchmark Results
  uses: actions/upload-artifact@v2
  with:
    name: benchmark-results
    path: benchmark_results/
```

## Troubleshooting

### Common Issues

1. **"Baostock login failed"**
   - Check network connectivity
   - Verify Baostock service is available

2. **"Permission denied" errors**
   - Close other applications accessing files
   - Run with appropriate permissions

3. **"Out of memory" errors**
   - Reduce test data size
   - Use `--skip-long-tests` flag

4. **Slow execution**
   - API calls are rate-limited by Baostock
   - Consider using cached data for testing

## Contributing

When adding new benchmarks:
1. Follow existing code patterns
2. Use appropriate test data sizes
3. Document expected performance characteristics
4. Add to main benchmark runner
