# 项目文档更新计划

## 一、文档现状分析

### 1.1 现有文档清单

| 文档路径 | 状态 | 说明 |
|---------|------|------|
| `README.md` | ✅ 完整 | 项目主文档，内容详尽 |
| `ARCHITECTURE.md` | ✅ 完整 | 架构设计文档，内容详尽 |
| `benchmarks/BENCHMARK_README.md` | ✅ 完整 | 性能基准测试说明 |
| `benchmark_results/performance_test_report.md` | ✅ 完整 | 性能测试报告 |
| `references/BaoStock前复权日K线数据计算简介.md` | ✅ 参考文档 | BaoStock 官方文档 |
| `references/BaoStock复权因子简介.md` | ✅ 参考文档 | BaoStock 官方文档 |

### 1.2 文档与代码一致性检查

#### 测试文件对比

**文档中记录的测试**:
- test_parquet_store.py ✓
- test_duckdb_store.py ✓
- test_dataset_catalog.py ✓
- test_schema.py ✓
- test_validators.py ✓
- test_baostock_client.py ✓
- test_market_data_provider.py ✓
- test_cli_provider.py ✓
- test_update_daily_full_resume.py ✓
- test_update_daily_partial_resume.py ✓
- test_update_daily_refetch.py ✓
- test_code_pool.py ✓
- test_trading_dates.py ✓
- test_repair_tool.py ✓
- test_adjustments.py ✓

**实际测试文件**:
- conftest.py (测试配置，无需记录)
- test_adjustments.py ✓
- test_baostock_client.py ✓
- test_cli_provider.py ✓
- test_code_pool.py ✓
- test_dataset_catalog.py ✓
- test_duckdb_store.py ✓
- test_market_data_provider.py ✓
- test_parquet_store.py ✓
- test_repair_tool.py ✓
- test_schema.py ✓
- test_trading_dates.py ✓
- test_update_daily_full_resume.py ✓
- test_update_daily_partial_resume.py ✓
- test_update_daily_refetch.py ✓
- test_validators.py ✓
- update_daily_fakes.py (测试辅助模块，无需记录)

**结论**: 测试文件列表完全一致 ✅

#### 项目结构对比

文档中记录的项目结构与实际结构一致 ✅

#### CLI 命令对比

文档中记录的 CLI 命令与 `src/cli.py` 实现一致 ✅

## 二、需要更新的内容

### 2.1 README.md 更新

1. **性能监控工具说明** - 新增 `src/utils/performance.py` 模块的使用说明
2. **性能基准测试说明** - 补充 `benchmarks/` 目录和运行基准测试的说明

### 2.2 ARCHITECTURE.md 更新

1. **性能监控模块** - 添加 `src/utils/performance.py` 模块的架构说明
2. **基准测试目录** - 补充 `benchmarks/` 目录的说明

## 三、具体实施步骤

### 步骤 1: 更新 README.md

在"测试"章节后添加"性能基准测试"章节：

```markdown
## 性能基准测试

项目包含完整的性能基准测试套件，位于 `benchmarks/` 目录。

### 运行基准测试

```powershell
# 运行所有基准测试
python benchmarks/run_all_benchmarks.py --output-dir benchmark_results

# 跳过长时间测试
python benchmarks/run_all_benchmarks.py --skip-long-tests
```

### 基准测试内容

- **API 调用性能**: Baostock API 延迟和吞吐量
- **I/O 操作性能**: Parquet 文件读写性能
- **数据处理性能**: 数据清洗、复权计算性能
- **并发性能**: 线程池性能和锁竞争
- **端到端性能**: 完整管道性能

详细说明见 [benchmarks/BENCHMARK_README.md](benchmarks/BENCHMARK_README.md)。
```

在"项目结构"章节中补充 `src/utils/performance.py` 的说明。

### 步骤 2: 更新 ARCHITECTURE.md

在"4. 项目目录结构"章节中补充 `benchmarks/` 目录：

```markdown
├── benchmarks/               # 性能基准测试
│   ├── BENCHMARK_README.md   # 基准测试说明
│   ├── benchmark_api_calls.py
│   ├── benchmark_concurrency.py
│   ├── benchmark_data_processing.py
│   ├── benchmark_end_to_end.py
│   ├── benchmark_io_operations.py
│   ├── benchmark_utils.py
│   └── run_all_benchmarks.py
```

在"src/utils/"章节中补充 `performance.py` 的说明：

```markdown
│   ├── performance.py        # 性能监控工具
```

在"12. 性能优化"章节后添加"12.5 性能监控工具"子章节：

```markdown
### 12.5 性能监控工具

`src/utils/performance.py` 提供性能监控和性能分析工具：

- **PerformanceTimer**: 代码块计时上下文管理器
- **PerformanceCollector**: 计时数据收集和统计
- **MemoryMonitor**: 内存使用监控
- **FunctionProfiler**: 函数调用性能分析

使用示例：

```python
from src.utils.performance import PerformanceTimer, get_collector

with PerformanceTimer("my_operation"):
    # ... 需要计时的代码 ...

collector = get_collector()
print(collector.summary())
```
```

### 步骤 3: 验证更新

1. 检查文档格式正确
2. 确保代码示例可运行
3. 验证链接有效

## 四、预期结果

更新后的文档将：

1. ✅ 完整记录性能基准测试功能
2. ✅ 完整记录性能监控工具
3. ✅ 保持与代码实现一致
4. ✅ 提供更完整的项目概览
