# 项目精简与优化计划（修订版）

## 概述

基于代码审查和反馈，确定以下优化方向：

1. **删除未使用代码** - 低风险，立即可做
2. **提取公共常量** - `1990-01-01` 统一为共享常量
3. **提取公共 checkpoint 写入逻辑** - 减少重复代码
4. **谨慎抽象 metadata batch** - 注意 init_history 和 update_daily 的差异
5. **小修异常处理** - 仅对临时文件清理使用 `contextlib.suppress`

---

## 一、删除未使用代码（低风险，立即执行）

### 1.1 未使用的函数

| 文件 | 函数 | 行号 | 说明 |
|------|------|------|------|
| [update_daily.py](file:///c:/PycharmProjects/quant_data_center/src/pipeline/update_daily.py#L427) | `_persist_metadata` | 427-436 | 未被调用 |
| [update_daily.py](file:///c:/PycharmProjects/quant_data_center/src/pipeline/update_daily.py#L629) | `_add_run` | 629-643 | 未被调用 |
| [paths.py](file:///c:/PycharmProjects/quant_data_center/src/utils/paths.py#L63) | `stock_basic_file` | 63-66 | 旧分区模式残留，无引用 |

**操作**：直接删除这三个函数

**预期收益**：减少约 25 行代码

---

## 二、提取公共常量（低风险）

### 2.1 `1990-01-01` 常量化

**现状**：
- `update_daily.py:37` 已定义 `FULL_HISTORY_START_DATE = "1990-01-01"`
- 但其他文件仍硬编码使用（共 14 处）

**涉及文件**：
- [init_history.py](file:///c:/PycharmProjects/quant_data_center/src/pipeline/init_history.py) - 10 处
- [baostock_client.py](file:///c:/PycharmProjects/quant_data_center/src/api/baostock_client.py#L123) - 1 处
- [repair_tool.py](file:///c:/PycharmProjects/quant_data_center/src/pipeline/repair_tool.py#L46) - 1 处
- [performance_test.py](file:///c:/PycharmProjects/quant_data_center/src/pipeline/performance_test.py#L384) - 1 处
- [cli.py](file:///c:/PycharmProjects/quant_data_center/src/cli.py#L36) - 1 处（默认参数，可保留）

**操作**：
1. 将 `FULL_HISTORY_START_DATE` 移至 `common.py`
2. 各文件从 `common.py` 导入使用

**预期收益**：统一管理，便于未来修改

---

## 三、提取公共 checkpoint 写入逻辑（中风险）

### 3.1 `_write_checkpoint` 函数

**现状**：
- [init_history.py:319-346](file:///c:/PycharmProjects/quant_data_center/src/pipeline/init_history.py#L319-L346)
- [update_daily.py:448-475](file:///c:/PycharmProjects/quant_data_center/src/pipeline/update_daily.py#L448-L475)

两个函数几乎完全相同，仅 `PIPELINE_*` 常量不同。

**操作**：
1. 在 `common.py` 中创建通用版本，接受 `pipeline` 参数
2. 两个 pipeline 文件改为调用公共函数

**代码示例**：
```python
# common.py
def write_checkpoint(
    store: ParquetStore,
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    status: str,
    row_count: int,
    output_path: str | Path,
    error_stack: str = "",
) -> None:
    store.upsert_pipeline_checkpoints(
        pd.DataFrame([
            checkpoint_row(pipeline, dataset, code, start_date, end_date, status, row_count, output_path, error_stack)
        ])
    )
```

**预期收益**：减少约 30 行重复代码

---

## 四、谨慎抽象 Metadata Batch（中风险）

### 4.1 现状分析

**init_history** 的 `_InitHistoryCheckpointBatch`：
- 只处理 `checkpoint_rows`
- 计数基于 `len(self._checkpoint_rows)`

**update_daily** 的 `_UpdateDailyMetadataBatch`：
- 处理 `run_rows`、`status_rows`、`checkpoint_rows`
- 计数基于 `len(self._run_rows)`

**关键差异**：两者计数逻辑不同，不能简单合并。

### 4.2 建议方案

**方案A（推荐）**：保持现状，仅提取公共接口

```python
# common.py
class MetadataBatchBase:
    """Abstract base for metadata batch writes."""
    
    def flush(self) -> None:
        raise NotImplementedError
    
    @property
    def _pending_count(self) -> int:
        raise NotImplementedError
```

**方案B**：完全重构为统一类（风险较高）

**建议**：采用方案A，保持现有实现，仅提取公共接口用于类型提示

---

## 五、异常处理小修（低风险）

### 5.1 临时文件清理

**现状**：[parquet_store.py:171-172](file:///c:/PycharmProjects/quant_data_center/src/storage/parquet_store.py#L171-L172) 等处使用空 `except` 清理临时文件

**操作**：使用 `contextlib.suppress` 明确表达意图

```python
# 当前写法
try:
    tmp_path.unlink()
except Exception:
    pass

# 改为
from contextlib import suppress
with suppress(Exception):
    tmp_path.unlink()
```

### 5.2 主循环异常处理（不修改）

**原因**：
- 已正确使用 `traceback.format_exc()` 和 `logger.exception(...)`
- 广义捕获有业务意义（单只股票失败后继续处理）
- 当前实现符合预期

---

## 六、不做的事项

| 项目 | 原因 |
|------|------|
| 测试 fixture 修改 | 工厂函数模式正确，避免共享可变对象问题 |
| 合并辅助函数 | `_add_success_run` 等小函数可读性好，合并反而降低可读性 |
| 移除 YAML fallback | 优先级低，且 fallback 有其价值 |
| 修改主循环异常处理 | 已正确实现，有业务意义 |

---

## 七、实施计划

### 阶段一：低风险清理
1. 删除 `_persist_metadata`、`_add_run`、`stock_basic_file`
2. 运行测试确认无影响

### 阶段二：常量提取
1. 将 `FULL_HISTORY_START_DATE` 移至 `common.py`
2. 更新各文件导入
3. 运行测试

### 阶段三：公共函数提取
1. 提取 `_write_checkpoint` 到 `common.py`
2. 更新 `init_history.py` 和 `update_daily.py`
3. 运行测试

### 阶段四：异常处理小修
1. 临时文件清理改用 `contextlib.suppress`
2. 运行测试

---

## 八、测试基线

当前状态：`pytest -q` → **39 passed**

每次修改后需确认测试通过。

---

## 九、预期收益

| 优化项 | 代码行数变化 | 风险 |
|--------|-------------|------|
| 删除未使用函数 | -25 行 | 低 |
| 提取公共常量 | 0 行（重构） | 低 |
| 提取公共 checkpoint | -30 行 | 中 |
| 异常处理小修 | 0 行（质量提升） | 低 |

**总计**：减少约 55 行代码，提升代码质量和可维护性。

---

## 十、风险评估

| 优化项 | 风险等级 | 缓解措施 |
|--------|---------|---------|
| 删除未使用函数 | 低 | grep 确认无引用 |
| 提取公共常量 | 低 | 全局搜索替换 |
| 提取公共 checkpoint | 中 | 完整测试覆盖 |
| 异常处理小修 | 低 | 仅影响错误路径 |
