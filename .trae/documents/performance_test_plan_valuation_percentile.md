# 性能测试报告：update-baostock-valuation-percentile 性能瓶颈分析

## 问题背景

运行 `python -m src.cli update-baostock-valuation-percentile --mode full` 命令十分钟后只有一条日志输出，说明程序运行极其缓慢。

## 数据规模

- **股票数量**: 7543 个
- **每个股票数据量**: 约 8632 行（约 34 年交易日数据）
- **估值字段**: 4 个 (pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm)
- **滚动窗口**: 5 个 (1y, 3y, 5y, 10y, all_history)

## 测试结果

### 快速测试 (单股票 sh.000001)

| 阶段 | 耗时 | 占比 |
|------|------|------|
| 数据读取 | 0.258s | 0.3% |
| 数据清洗 | 0.074s | 0.1% |
| **核心计算** | **88.983s** | **99.2%** |
| 数据写入 | 0.413s | 0.5% |
| **总计** | **89.728s** | 100% |

**预计处理 7543 个股票总耗时**: 7543 × 89s ≈ **186 小时 (约 7.75 天)**

### 详细性能分析

| 操作 | 耗时 | 占比 |
|------|------|------|
| **expire_before (滚动窗口过期移除)** | 18.162s | 55.9% |
| **df_loc_assign (DataFrame.loc 逐行赋值)** | 13.885s | 42.7% |
| insort_rolling (滚动窗口有序插入) | 0.292s | 0.9% |
| insort_all_history (全历史有序插入) | 0.143s | 0.4% |
| percentile_calc (百分位计算) | 0.007s | 0.0% |

### DataFrame.loc vs NumPy 数组性能对比

| 操作 | 耗时 | 相对性能 |
|------|------|----------|
| DataFrame.loc 逐行赋值 (8632行×25列) | 141.263s | 基准 |
| NumPy 数组逐行赋值 (8632行×25列) | 0.159s | **快 888 倍** |

## 性能瓶颈根因

### 瓶颈 1: DataFrame.loc 逐行赋值 (42.7%)

**问题代码位置**: `src/analytics/valuation_percentile.py:127, 129, 131, 139, 140`

```python
result.loc[index, percentile_column(field, window)] = value
```

**问题分析**:
- `DataFrame.loc` 逐行赋值会触发 pandas 内部的索引查找和类型检查
- 每次赋值都可能触发 DataFrame 的内部重建
- 对于 8632 行 × 25 列的数据，需要执行 215,800 次逐行赋值

### 瓶颈 2: expire_before 滚动窗口过期移除 (55.9%)

**问题代码位置**: `src/analytics/valuation_percentile.py:29-34`

```python
def expire_before(self, cutoff: pd.Timestamp) -> None:
    while self.rows and self.rows[0][0] < cutoff:
        _, value = self.rows.popleft()
        index = bisect_left(self.values, value)
        if index < len(self.values):
            self.values.pop(index)
```

**问题分析**:
- 每次移除过期元素需要 `bisect_left` 查找 + `pop` 操作
- `pop(index)` 在列表中间删除元素是 O(n) 操作
- 对于每个滚动窗口，每行数据都可能触发过期移除

## 优化方案

### 方案 1: 使用 NumPy 数组替代 DataFrame.loc (推荐)

**预期效果**: 核心计算时间从 89s 降至约 5s (提升 17 倍)

**实施步骤**:
1. 在 `_compute_field_percentiles` 中使用 NumPy 数组存储中间结果
2. 计算完成后一次性构建 DataFrame

**示例代码**:
```python
def _compute_field_percentiles(dates, values, result, field):
    n = len(dates)
    # 使用 NumPy 数组存储结果
    percentile_cols = {window: np.full(n, np.nan) for window, _ in ROLLING_WINDOWS}
    percentile_cols[ALL_HISTORY_WINDOW] = np.full(n, np.nan)
    
    # ... 计算逻辑 ...
    
    # 一次性赋值到 DataFrame
    for window, arr in percentile_cols.items():
        result[percentile_column(field, window)] = arr
```

### 方案 2: 优化滚动窗口数据结构

**预期效果**: expire_before 时间从 18s 降至约 1s

**实施步骤**:
1. 使用 `sortedcontainers.SortedList` 替代 Python 列表
2. 或使用 NumPy 数组 + 二分查找 + 切片操作

**示例代码**:
```python
from sortedcontainers import SortedList

class _WindowState:
    def __init__(self, years: int):
        self.years = years
        self.values = SortedList()  # O(log n) 插入和删除
        self.rows = deque()
```

### 方案 3: 多进程并行处理

**预期效果**: 根据 CPU 核心数线性提升 (如 8 核可提升 6-8 倍)

**实施步骤**:
1. 使用 `concurrent.futures.ProcessPoolExecutor` 并行处理多个股票
2. 每个进程处理一批股票

### 方案 4: 增量计算优化

**预期效果**: partial 模式只计算新增数据，大幅减少计算量

**实施步骤**:
1. 只对新增日期计算百分位
2. 复用已有历史数据的百分位结果

## 推荐优化路径

1. **立即实施**: 方案 1 (NumPy 数组替代 DataFrame.loc)
   - 改动最小，效果最明显
   - 预计可将单股票处理时间从 89s 降至 5-10s

2. **后续优化**: 方案 2 (优化滚动窗口)
   - 进一步提升性能
   - 预计可将单股票处理时间降至 2-5s

3. **长期优化**: 方案 3 (多进程并行)
   - 充分利用多核 CPU
   - 预计可将总处理时间降至 1-2 小时

## 测试脚本

已创建以下测试脚本：
- `scripts/profile_valuation_percentile.py` - 主测试脚本
- `scripts/profile_valuation_detailed.py` - 详细性能分析脚本

### 使用方法

```bash
# 快速测试
python scripts/profile_valuation_percentile.py quick

# 多股票测试
python scripts/profile_valuation_percentile.py multi

# 详细性能分析
python scripts/profile_valuation_detailed.py
```
