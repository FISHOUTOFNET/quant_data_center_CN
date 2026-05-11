# 性能测试计划：update-baostock-valuation-percentile --mode full

## 问题背景

用户报告 `update-baostock-valuation-percentile --mode full` 运行速度慢，约 1 分钟只能处理 10 个股票代码。需要设计性能测试来定位瓶颈。

## 代码架构分析

### 核心流程

```
update_baostock_valuation_percentile()
├── _resolve_source_codes()           # 获取所有股票代码列表
├── for each stock_code:
│   ├── _source_date_bounds()         # 读取 parquet 获取日期范围
│   ├── _target_covers_source_latest() # 检查是否需要更新（读取 parquet）
│   ├── _replace_start()              # 读取现有估值百分位数据
│   └── _should_skip()                # 检查 checkpoint
├── ProcessPoolExecutor (并行计算)
│   └── _compute_valuation_percentile_task()
│       ├── read_baostock_daily_bars()  # 读取完整日线数据
│       └── compute_valuation_percentiles() # 核心计算
└── _record_valuation_result()        # 写入结果和元数据
```

### 潜在性能瓶颈

#### 1. 重复 I/O 操作（高优先级）

| 位置 | 函数 | 操作 | 问题 |
|------|------|------|------|
| L369-378 | `_source_date_bounds` | `pd.read_parquet(path, columns=["date"])` | 每只股票读取一次完整文件 |
| L381-392 | `_target_covers_source_latest` | `pd.read_parquet(path, columns=["date"])` | 再次读取相同文件 |
| L408-414 | `_replace_start` | `read_baostock_cn_stock_valuation_percentile()` | 读取估值百分位数据 |
| L219 | `_compute_valuation_percentile_task` | `read_baostock_daily_bars()` | 第三次读取相同文件 |

**问题**：每只股票在任务准备阶段可能读取 2-3 次相同文件，计算阶段再读取一次完整数据。

#### 2. ParquetStore 实例创建开销

```python
# L217: 每个任务创建新的 ParquetStore
store = ParquetStore(root=task.root)
```

每个 worker 进程都创建新的 ParquetStore 实例，包括 DuckDBMetadataStore 初始化。

#### 3. 计算复杂度

`compute_valuation_percentiles` 函数：
- 时间复杂度：O(n * m * log(k))，其中 n 是行数，m 是估值字段数(4)，k 是唯一值数
- 空间复杂度：O(n + k)
- 滚动窗口计算需要维护 5 个 Fenwick Tree 状态

#### 4. 元数据写入

```python
# L200-201: 批量写入元数据
if run_records or status_rows or checkpoint_rows:
    store.persist_update_metadata(run_records, status_rows, checkpoint_rows)
```

每次更新都会写入 DuckDB 元数据，可能造成 I/O 等待。

## 性能测试设计

### 测试 1：I/O 操作计时测试

**目标**：量化各 I/O 操作的时间占比

**测试代码位置**：`tests/performance/test_valuation_percentile_io.py`

```python
def test_io_timing_breakdown(tmp_path, sample_stock_count=100):
    """测量各 I/O 操作的时间占比"""
    metrics = {
        'source_date_bounds': [],
        'target_covers_source_latest': [],
        'replace_start': [],
        'read_daily_bars': [],
        'compute_percentiles': [],
        'write_result': [],
    }
    
    # 为 sample_stock_count 只股票生成测试数据
    # 记录每个操作的耗时
    # 输出统计报告
```

**预期输出**：
```
I/O Timing Breakdown (100 stocks):
┌─────────────────────────────────┬──────────┬──────────┬────────┐
│ Operation                       │ Total(s) │ Avg(ms)  │ %      │
├─────────────────────────────────┼──────────┼──────────┼────────┤
│ _source_date_bounds             │ XX.XX    │ XX.XX    │ XX.X%  │
│ _target_covers_source_latest    │ XX.XX    │ XX.XX    │ XX.X%  │
│ _replace_start                  │ XX.XX    │ XX.XX    │ XX.X%  │
│ read_baostock_daily_bars        │ XX.XX    │ XX.XX    │ XX.X%  │
│ compute_valuation_percentiles   │ XX.XX    │ XX.XX    │ XX.X%  │
│ write_result                    │ XX.XX    │ XX.XX    │ XX.X%  │
└─────────────────────────────────┴──────────┴──────────┴────────┘
```

### 测试 2：计算性能基准测试

**目标**：验证 `compute_valuation_percentiles` 的性能

**测试代码位置**：`tests/performance/test_valuation_percentile_compute.py`

```python
@pytest.mark.parametrize("row_count", [1000, 3000, 5000, 10000])
def test_compute_performance_by_row_count(row_count):
    """测试不同数据量下的计算性能"""
    frame = _performance_frame(row_count)
    
    start = time.perf_counter()
    result = compute_valuation_percentiles(frame)
    elapsed = time.perf_counter() - start
    
    # 验证性能预算
    assert elapsed < row_count * 0.001  # 每行不超过 1ms
```

### 测试 3：并行效率测试

**目标**：验证 ProcessPoolExecutor 的并行效率

**测试代码位置**：`tests/performance/test_valuation_percentile_parallel.py`

```python
@pytest.mark.parametrize("workers", [1, 2, 4, 8])
def test_parallel_efficiency(workers, sample_stock_count=50):
    """测试不同 worker 数量下的性能"""
    # 准备测试数据
    
    # 运行并计时
    start = time.perf_counter()
    update_baostock_valuation_percentile(
        mode="full",
        workers=workers,
        ...
    )
    elapsed = time.perf_counter() - start
    
    # 计算加速比
```

### 测试 4：内存使用分析

**目标**：检测内存泄漏和峰值使用

**测试代码位置**：`tests/performance/test_valuation_percentile_memory.py`

```python
def test_memory_usage_during_update():
    """监控更新过程中的内存使用"""
    import tracemalloc
    
    tracemalloc.start()
    # 运行更新
    snapshot = tracemalloc.take_snapshot()
    
    # 分析内存使用
    top_stats = snapshot.statistics('lineno')
```

### 测试 5：端到端性能测试

**目标**：模拟真实场景，验证整体性能

**测试代码位置**：`tests/performance/test_valuation_percentile_e2e.py`

```python
def test_end_to_end_performance(tmp_path, stock_count=100):
    """端到端性能测试"""
    # 生成真实规模的测试数据（每只股票约 3000 行）
    # 运行完整更新流程
    # 验证：100 只股票应在 2 分钟内完成
```

## 实施步骤

### 第一步：创建性能测试框架

1. 创建 `tests/performance/` 目录
2. 创建 `conftest.py` 包含共享 fixtures
3. 创建测试数据生成器

### 第二步：实现各性能测试

按优先级顺序实现：
1. I/O 操作计时测试（最重要）
2. 计算性能基准测试
3. 并行效率测试
4. 端到端性能测试
5. 内存使用分析

### 第三步：运行测试并分析结果

```bash
# 运行所有性能测试
pytest tests/performance/ -v --tb=short

# 运行特定测试
pytest tests/performance/test_valuation_percentile_io.py -v

# 生成报告
pytest tests/performance/ -v --html=performance_report.html
```

## 预期发现与优化方向

### 可能的优化方向

1. **减少重复 I/O**
   - 合并 `_source_date_bounds` 和 `_target_covers_source_latest` 的读取
   - 缓存日期范围信息
   - 使用 parquet metadata 快速获取统计信息

2. **优化 ParquetStore**
   - 使用连接池或单例模式
   - 延迟初始化 DuckDBMetadataStore

3. **批量处理优化**
   - 预读取所有需要处理的股票元数据
   - 批量写入结果

4. **计算优化**
   - 使用 numba 加速核心计算
   - 考虑使用 polars 替代 pandas

## 测试数据规模

| 测试场景 | 股票数量 | 每股行数 | 总行数 |
|----------|----------|----------|--------|
| 快速测试 | 10 | 1000 | 10,000 |
| 标准测试 | 100 | 3000 | 300,000 |
| 压力测试 | 500 | 5000 | 2,500,000 |

## 验收标准

1. 能够明确识别性能瓶颈所在（I/O vs 计算 vs 并行）
2. I/O 操作时间占比超过 50% 时，提供优化建议
3. 计算性能符合预期（每行 < 1ms）
4. 并行效率不低于 70%（4 workers vs 1 worker）
