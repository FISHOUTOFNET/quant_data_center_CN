# update_daily.py 性能测试计划

## 一、性能瓶颈分析

通过对 `update_daily.py` 及其相关模块的代码分析，识别出以下潜在的性能瓶颈：

### 1. API 调用瓶颈（高优先级）
- **位置**: [update_daily.py:282-371](file:///c:/PycharmProjects/quant_data_center/src/pipeline/update_daily.py#L282-L371)
- **问题**: 
  - 每个股票代码串行调用 `fetch_adjust_factor` 和 `fetch_daily_k`
  - API 调用在主线程中执行，阻塞后续处理
  - Baostock API 有重试机制（最多3次，指数退避），失败时延迟显著
- **影响**: 对于5000+股票代码，API调用是主要时间消耗

### 2. I/O 操作瓶颈（高优先级）
- **位置**: 
  - [parquet_store.py:146-199](file:///c:/PycharmProjects/quant_data_center/src/storage/parquet_store.py#L146-L199) - atomic_write
  - [parquet_store.py:54-77](file:///c:/PycharmProjects/quant_data_center/src/storage/parquet_store.py#L54-L77) - _safe_read_parquet
- **问题**:
  - 每次写入都需要创建临时文件、写入、原子替换
  - Windows 文件锁问题导致重试（最多3次，指数退避）
  - 元数据表每次更新都需要读取、合并、写入整个表

### 3. 数据处理瓶颈（中优先级）
- **位置**:
  - [parquet_store.py:108-140](file:///c:/PycharmProjects/quant_data_center/src/storage/parquet_store.py#L108-L140) - clean_dataframe_for_schema
  - [adjustments.py:21-77](file:///c:/PycharmProjects/quant_data_center/src/pipeline/adjustments.py#L21-L77) - calculate_adjusted_daily_k
- **问题**:
  - 每个DataFrame都需要类型转换和清洗
  - 复权计算涉及 pd.merge_asof 和分组操作
  - 数据合并需要排序和去重

### 4. 并发处理瓶颈（中优先级）
- **位置**: [update_daily.py:281](file:///c:/PycharmProjects/quant_data_center/src/pipeline/update_daily.py#L281)
- **问题**:
  - ThreadPoolExecutor 固定为 4 个线程
  - 主线程和后台线程之间存在等待
  - 元数据批处理使用 RLock，可能存在锁竞争

### 5. 元数据操作瓶颈（低优先级）
- **位置**: 
  - [services.py:21-75](file:///c:/PycharmProjects/quant_data_center/src/pipeline/services.py#L21-L75) - PipelineMetadataBatch
  - [parquet_store.py:304-356](file:///c:/PycharmProjects/quant_data_center/src/storage/parquet_store.py#L304-L356) - 元数据持久化
- **问题**:
  - 批量大小固定为 200
  - 每次 flush 需要写入三个元数据表
  - upsert 操作需要读取现有数据、合并、去重

## 二、性能测试方案

### 测试环境准备
```python
# 创建性能测试配置文件
test_config = {
    "small": {"codes": 10, "days": 30},      # 小规模测试
    "medium": {"codes": 100, "days": 30},    # 中规模测试
    "large": {"codes": 500, "days": 30},     # 大规模测试
    "full": {"codes": 5000, "days": 30}      # 全量测试
}
```

### 测试脚本设计

#### 1. API 调用性能测试
**目标**: 测量单个API调用的延迟和吞吐量

```python
# tests/benchmark_api_calls.py
import time
import statistics
from src.api.market_data import create_provider
from src.utils.config_mgr import ConfigManager

def benchmark_api_calls():
    """测试 API 调用性能"""
    config = ConfigManager()
    
    with create_provider(config) as provider:
        # 测试 adjust_factor API
        adjust_times = []
        for code in test_codes[:100]:
            start = time.perf_counter()
            provider.query_adjust_factor(code, "1990-01-01", "2024-12-31")
            adjust_times.append(time.perf_counter() - start)
        
        # 测试 daily_k API
        daily_times = []
        for code in test_codes[:100]:
            start = time.perf_counter()
            provider.query_daily_k(DailyKRequest(...))
            daily_times.append(time.perf_counter() - start)
    
    return {
        "adjust_factor": {
            "mean": statistics.mean(adjust_times),
            "median": statistics.median(adjust_times),
            "stdev": statistics.stdev(adjust_times),
            "min": min(adjust_times),
            "max": max(adjust_times)
        },
        "daily_k": {...}
    }
```

#### 2. I/O 操作性能测试
**目标**: 测量 Parquet 文件读写性能

```python
# tests/benchmark_io_operations.py
import time
import pandas as pd
from src.storage.parquet_store import ParquetStore

def benchmark_io_operations():
    """测试 I/O 操作性能"""
    store = ParquetStore(root=test_root)
    
    # 测试写入性能
    write_times = []
    for size in [100, 1000, 10000, 100000]:
        df = generate_test_dataframe(size)
        start = time.perf_counter()
        store.write_daily_k("daily_k_none", "sh.600000", df)
        write_times.append((size, time.perf_counter() - start))
    
    # 测试读取性能
    read_times = []
    for _ in range(100):
        start = time.perf_counter()
        store.read_daily_k("daily_k_none", "sh.600000")
        read_times.append(time.perf_counter() - start)
    
    # 测试元数据操作性能
    metadata_times = benchmark_metadata_operations(store)
    
    return {
        "write": write_times,
        "read": read_times,
        "metadata": metadata_times
    }
```

#### 3. 数据处理性能测试
**目标**: 测量数据清洗和复权计算性能

```python
# tests/benchmark_data_processing.py
import time
import pandas as pd
from src.storage.parquet_store import ParquetStore
from src.pipeline.adjustments import calculate_adjusted_daily_k

def benchmark_data_processing():
    """测试数据处理性能"""
    store = ParquetStore()
    
    # 测试数据清洗性能
    clean_times = []
    for size in [1000, 10000, 100000, 1000000]:
        df = generate_raw_dataframe(size)
        start = time.perf_counter()
        cleaned = store.clean_dataframe_for_schema(df, schema)
        clean_times.append((size, time.perf_counter() - start))
    
    # 测试复权计算性能
    adjust_times = []
    for size in [1000, 10000, 100000]:
        unadjusted = generate_unadjusted_df(size)
        factors = generate_factor_df(10)
        start = time.perf_counter()
        adjusted = calculate_adjusted_daily_k(unadjusted, factors, "daily_k_qfq", "2")
        adjust_times.append((size, time.perf_counter() - start))
    
    return {
        "clean": clean_times,
        "adjust": adjust_times
    }
```

#### 4. 并发性能测试
**目标**: 测量不同线程数下的性能表现

```python
# tests/benchmark_concurrency.py
import time
from concurrent.futures import ThreadPoolExecutor
from src.pipeline.update_daily import update_daily

def benchmark_concurrency():
    """测试并发性能"""
    results = {}
    
    for max_workers in [1, 2, 4, 8, 16]:
        # 修改 ThreadPoolExecutor 的 max_workers
        # 需要临时修改 update_daily.py 或提供配置参数
        
        start = time.perf_counter()
        records = update_daily(
            dataset="daily_k_none",
            code=test_codes[:100],
            end="2024-01-03",
            lookback_days=30,
            root=test_root,
            build_views=False
        )
        elapsed = time.perf_counter() - start
        
        results[max_workers] = {
            "elapsed": elapsed,
            "records": len(records)
        }
    
    return results
```

#### 5. 端到端性能测试
**目标**: 测量完整更新流程的性能

```python
# tests/benchmark_end_to_end.py
import time
import cProfile
import pstats
from io import StringIO
from src.pipeline.update_daily import update_daily

def benchmark_end_to_end():
    """端到端性能测试"""
    
    # 使用 cProfile 进行性能分析
    profiler = cProfile.Profile()
    
    profiler.enable()
    start = time.perf_counter()
    
    records = update_daily(
        dataset="all",
        end="2024-01-03",
        lookback_days=30,
        root=test_root,
        build_views=True
    )
    
    elapsed = time.perf_counter() - start
    profiler.disable()
    
    # 分析性能数据
    stats_stream = StringIO()
    stats = pstats.Stats(profiler, stream=stats_stream)
    stats.sort_stats('cumulative')
    stats.print_stats(50)  # 打印前50个耗时函数
    
    return {
        "elapsed": elapsed,
        "records": len(records),
        "profile": stats_stream.getvalue()
    }
```

### 测试场景设计

#### 场景1: 首次全量更新
```python
def test_first_full_update():
    """首次全量更新性能测试"""
    # 清空所有数据
    # 执行 update_daily(mode="full")
    # 记录总耗时和各阶段耗时
```

#### 场景2: 增量更新
```python
def test_incremental_update():
    """增量更新性能测试"""
    # 已有历史数据
    # 执行 update_daily(mode="partial")
    # 记录总耗时和各阶段耗时
```

#### 场景3: Resume 模式
```python
def test_resume_update():
    """Resume 模式性能测试"""
    # 中断后重新执行
    # 测试 checkpoint 过滤效率
    # 记录跳过的代码数量和耗时
```

#### 场景4: 大规模并发测试
```python
def test_large_scale_concurrent():
    """大规模并发测试"""
    # 测试 5000+ 股票代码
    # 监控内存使用
    # 监控 CPU 使用
    # 监控磁盘 I/O
```

## 三、性能监控工具

### 1. Python 内置工具
```python
# 使用 time.perf_counter() 进行高精度计时
import time
start = time.perf_counter()
# ... 操作 ...
elapsed = time.perf_counter() - start

# 使用 cProfile 进行性能分析
import cProfile
profiler = cProfile.Profile()
profiler.enable()
# ... 操作 ...
profiler.disable()
profiler.print_stats(sort='cumulative')
```

### 2. 内存监控
```python
# 使用 memory_profiler
from memory_profiler import profile

@profile
def update_daily_memory_profile():
    return update_daily(...)

# 使用 tracemalloc
import tracemalloc
tracemalloc.start()
# ... 操作 ...
snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')
```

### 3. I/O 监控
```python
# 使用 psutil 监控磁盘 I/O
import psutil
io_before = psutil.disk_io_counters()
# ... 操作 ...
io_after = psutil.disk_io_counters()
io_diff = {k: io_after[k] - io_before[k] for k in io_before}
```

### 4. 自定义性能装饰器
```python
# src/utils/performance.py
import time
from functools import wraps
from src.utils.logging import logger

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info(f"{func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

class PerformanceTimer:
    def __init__(self, name):
        self.name = name
        self.start = None
        self.elapsed = None
    
    def __enter__(self):
        self.start = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.start
        logger.info(f"{self.name} took {self.elapsed:.3f}s")
```

## 四、性能优化建议

基于代码分析，提出以下优化建议：

### 1. API 调用优化（预期提升：30-50%）
- **并行化 API 调用**: 将 API 调用移到线程池中并行执行
- **批量请求**: 如果 Baostock 支持，使用批量 API
- **缓存机制**: 对重复请求的数据进行缓存

### 2. I/O 优化（预期提升：20-30%）
- **异步 I/O**: 使用 asyncio 和 aiofiles 进行异步文件操作
- **批量写入**: 将多个小文件合并为批量写入
- **压缩优化**: 调整 Parquet 压缩算法和压缩级别

### 3. 数据处理优化（预期提升：10-20%）
- **向量化操作**: 使用 pandas 向量化操作替代循环
- **延迟计算**: 使用 Dask 或 Vaex 进行延迟计算
- **内存优化**: 使用更高效的数据类型

### 4. 并发优化（预期提升：20-40%）
- **动态线程数**: 根据股票代码数量动态调整线程数
- **任务队列**: 使用优先级队列优化任务调度
- **无锁设计**: 减少锁的使用，使用无锁数据结构

### 5. 元数据优化（预期提升：5-10%）
- **增量更新**: 元数据表使用增量更新而非全量替换
- **索引优化**: 为元数据表添加索引
- **批量大小**: 根据实际情况调整批量大小

## 五、测试实施步骤

### 第一阶段：基准测试（1-2天）
1. 创建性能测试框架
2. 实现各类性能测试脚本
3. 运行基准测试，收集性能数据
4. 分析性能瓶颈，确定优化优先级

### 第二阶段：详细分析（2-3天）
1. 使用 cProfile 进行函数级性能分析
2. 使用 memory_profiler 进行内存分析
3. 使用 I/O 监控工具分析磁盘性能
4. 生成性能分析报告

### 第三阶段：优化实施（3-5天）
1. 按优先级实施优化方案
2. 每个优化后运行性能测试
3. 对比优化前后的性能数据
4. 记录优化效果

### 第四阶段：验证测试（1-2天）
1. 运行完整的性能测试套件
2. 验证优化效果
3. 确保功能正确性
4. 生成最终性能报告

## 六、预期成果

### 性能测试报告
- 各模块的性能基准数据
- 性能瓶颈的详细分析
- 优化前后的性能对比
- 优化建议的优先级排序

### 优化后的代码
- 实施的优化代码
- 性能测试脚本
- 监控工具集成
- 文档更新

### 持续监控
- 性能回归测试
- 性能监控仪表板
- 性能告警机制
