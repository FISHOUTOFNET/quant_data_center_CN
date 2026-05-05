# 修复 Baostock qfq/hfq 复权配置计划

## 问题分析

根据 BaoStock 官方文档，adjustflag 参数定义如下：
- `"1"` = 前复权 (qfq)
- `"2"` = 后复权 (hfq)
- `"3"` = 不复权 (none)

当前代码中多处硬编码了**相反**的映射关系，需要全面修正。

## 当前状态

- **配置文件** `config/settings.yaml` 已修复 ✓
- **本地数据**：当前没有 daily_k_qfq/daily_k_hfq 数据存储，无需数据迁移 ✓
- **所有测试通过** ✓

## 需要修改的文件

### 1. 测试文件中的硬编码映射

| 文件 | 行号 | 当前错误值 | 修正值 |
|------|------|-----------|--------|
| `tests/test_update_daily_refetch.py` | 22 | `{"daily_k_qfq": "2", "daily_k_hfq": "1"}` | `{"daily_k_qfq": "1", "daily_k_hfq": "2"}` |
| `tests/test_update_daily_refetch.py` | 163, 219 | `adjustflag="2"` (for qfq) | `adjustflag="1"` |
| `tests/test_update_daily_partial_resume.py` | 107, 172, 226 | `adjustflag="2"` (for qfq) | `adjustflag="1"` |
| `tests/test_adjustments.py` | 12, 14 | qfq 使用 `"2"` | qfq 使用 `"1"` |
| `tests/test_adjustments.py` | 22, 24 | hfq 使用 `"1"` | hfq 使用 `"2"` |
| `tests/test_repair_tool.py` | 55 | `{"daily_k_qfq": "2", "daily_k_hfq": "1"}` | `{"daily_k_qfq": "1", "daily_k_hfq": "2"}` |

### 2. 测试 fixture 中的 adjustflag 值

| 文件 | 行号 | 说明 |
|------|------|------|
| `tests/conftest.py` | 42, 62 | `adjustflag="2"` - 这是 daily_sample() 的默认值，用于通用测试数据，不需要修改 |
| `tests/test_market_data_provider.py` | 92 | 测试 daily_k_qfq 时期望 adjustflag="2"，应改为 "1" |

### 3. 基准测试配置

| 文件 | 行号 | 修正内容 |
|------|------|----------|
| `benchmarks/benchmark_utils.py` | 204 | `"qfq": "2", "hfq": "1"` → `"qfq": "1", "hfq": "2"` |

### 4. 比较脚本

| 文件 | 行号 | 修正内容 |
|------|------|----------|
| `scripts/compare_hist_daily_k.py` | 106-109 | 映射已正确（qfq→"1", hfq→"2"），无需修改 |

## 实施步骤

1. 修改 `tests/test_update_daily_refetch.py`
   - 第 22 行：修正 adjustflags 字典
   - 第 163 行：修正 adjustflag 值
   - 第 219 行：修正 adjustflag 值

2. 修改 `tests/test_update_daily_partial_resume.py`
   - 第 107 行：修正 adjustflag 值
   - 第 172 行：修正 adjustflag 值
   - 第 226 行：修正 adjustflag 值

3. 修改 `tests/test_adjustments.py`
   - 第 12 行：qfq 测试使用 adjustflag="1"
   - 第 14 行：断言 adjustflag 为 ["1", "1", "1"]
   - 第 22 行：hfq 测试使用 adjustflag="2"
   - 第 24 行：断言 adjustflag 为 ["2", "2", "2"]
   - 第 32 行：qfq 测试使用 adjustflag="1"

4. 修改 `tests/test_repair_tool.py`
   - 第 55 行：修正 adjustflags 字典

5. 修改 `tests/test_market_data_provider.py`
   - 第 92 行：期望 adjustflag 从 "2" 改为 "1"（因为测试的是 daily_k_qfq）

6. 修改 `benchmarks/benchmark_utils.py`
   - 第 204 行：修正 adjustflag_map

7. 运行测试验证
   - 执行相关测试确保修改正确

## 注意事项

- `conftest.py` 中的 `daily_sample()` 使用 `adjustflag="2"`，这可能只是测试数据的默认值，不一定代表 qfq，需要根据使用场景判断
- 所有修改仅涉及测试代码和配置，不影响生产代码逻辑
