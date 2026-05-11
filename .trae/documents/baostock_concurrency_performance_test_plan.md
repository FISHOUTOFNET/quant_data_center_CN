# BaoStock 并发性能测试计划

## 目标

测试 BaoStock API 在并发模式下的性能提升，对比当前顺序执行方式的效率差异。

## 背景

### 当前实现
- `BaostockClient` 使用单一登录会话
- API 调用顺序执行
- 实测 QPS 约 0.9-1.0

### 并发方案
- 多线程并发，每个线程独立登录
- 使用 `ThreadPoolExecutor` 管理线程池
- 需控制请求频率避免触发 BaoStock 服务端限流

## 测试方案

### 1. 测试场景

| 场景 | 描述 | 预期 |
|------|------|------|
| 顺序执行 | 当前实现，单连接顺序调用 | 基准 QPS ~1.0 |
| 并发-2线程 | 2 个独立连接并发 | QPS ~1.5-2.0 |
| 并发-4线程 | 4 个独立连接并发 | QPS ~2.0-3.0 |
| 并发-8线程 | 8 个独立连接并发 | QPS ~3.0-5.0 |
| 并发+限流 | 并发 + 请求间隔控制 | 稳定 QPS |

### 2. 测试数据

- **股票数量**: 50 只（代表性样本）
- **日期范围**: 2026-04-22 至 2026-05-11（20 个交易日）
- **API 类型**: 
  - `query_history_k_data_plus` (日线数据)
  - `query_adjust_factor` (复权因子)

### 3. 测试指标

| 指标 | 说明 |
|------|------|
| 总耗时 | 完成所有 API 调用的总时间 |
| QPS | 每秒查询数 |
| 成功率 | 成功调用次数 / 总调用次数 |
| 平均延迟 | 单次 API 调用的平均响应时间 |
| 错误率 | 失败调用占比 |

## 实现步骤

### Step 1: 创建并发 API 客户端

创建 `src/api/baostock_concurrent_client.py`:
- 实现 `ConcurrentBaostockClient` 类
- 支持多线程并发调用
- 每个线程独立登录/登出
- 支持请求间隔控制（rate limiting）

### Step 2: 创建性能测试脚本

创建 `benchmarks/benchmark_baostock_concurrency.py`:
- 实现 `benchmark_sequential_api()` - 顺序执行基准测试
- 实现 `benchmark_concurrent_api()` - 并发执行测试
- 实现 `benchmark_with_rate_limit()` - 带限流的并发测试
- 生成对比报告

### Step 3: 运行测试

```bash
python -m benchmarks.benchmark_baostock_concurrency
```

### Step 4: 分析结果

对比不同并发级别下的性能指标，确定：
1. 最优并发线程数
2. 是否需要请求间隔控制
3. 性能提升幅度

## 代码实现

### 并发客户端核心逻辑

```python
from concurrent.futures import ThreadPoolExecutor
import baostock as bs
import time

def fetch_single_stock(code: str, start_date: str, end_date: str, rate_limit: float = 0.1) -> pd.DataFrame:
    """每个线程独立登录，获取单只股票数据"""
    bs.login()  # 独立登录
    if rate_limit > 0:
        time.sleep(rate_limit)  # 请求间隔控制
    rs = bs.query_history_k_data_plus(
        code=code,
        fields="date,code,open,high,low,close,volume,amount",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3"
    )
    data = []
    while (rs.error_code == '0') & rs.next():
        data.append(rs.get_row_data())
    bs.logout()
    return pd.DataFrame(data, columns=rs.fields)

def fetch_concurrent(codes: list[str], start_date: str, end_date: str, max_workers: int = 4) -> list[pd.DataFrame]:
    """并发获取多只股票数据"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_single_stock, code, start_date, end_date) for code in codes]
        results = [f.result() for f in futures]
    return results
```

### 测试脚本结构

```python
def benchmark_sequential(codes, start_date, end_date):
    """顺序执行基准测试"""
    start = time.perf_counter()
    with BaostockClient() as client:
        for code in codes:
            client.query_history_k_data_plus(code, ..., start_date, end_date)
    elapsed = time.perf_counter() - start
    return {"total_time": elapsed, "qps": len(codes) / elapsed}

def benchmark_concurrent(codes, start_date, end_date, max_workers):
    """并发执行测试"""
    start = time.perf_counter()
    fetch_concurrent(codes, start_date, end_date, max_workers)
    elapsed = time.perf_counter() - start
    return {"total_time": elapsed, "qps": len(codes) / elapsed, "workers": max_workers}
```

## 预期结果

### 性能对比表

| 模式 | 线程数 | 预期 QPS | 预期耗时(50只股票) | 风险 |
|------|--------|----------|-------------------|------|
| 顺序 | 1 | ~1.0 | ~50s | 无 |
| 并发 | 2 | ~1.8 | ~28s | 低 |
| 并发 | 4 | ~3.0 | ~17s | 中 |
| 并发 | 8 | ~4.5 | ~11s | 高(可能限流) |
| 并发+限流 | 4 | ~2.5 | ~20s | 低 |

### 结论判断标准

1. **值得采用并发**: QPS 提升 > 50%，错误率 < 5%
2. **最优配置**: 在错误率可控的前提下，QPS 最高的配置
3. **建议**: 根据测试结果推荐生产环境配置

## 风险与注意事项

1. **BaoStock 服务端限流**: 过高并发可能触发限流，需要逐步测试
2. **连接数限制**: BaoStock 可能对并发连接数有限制
3. **数据一致性**: 并发获取的数据需要确保完整性
4. **错误处理**: 需要完善的错误重试机制

## 输出文件

- `benchmarks/benchmark_baostock_concurrency.py` - 测试脚本
- `benchmarks/results/baostock_concurrency_report.json` - 测试结果
- `benchmarks/results/baostock_concurrency_summary.md` - 测试总结
