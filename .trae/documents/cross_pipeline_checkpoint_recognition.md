# 修改 should_skip_checkpoint 实现跨 Pipeline 识别

## 问题分析

当前 `init_history` 和 `update_daily` 的 checkpoint 是完全隔离的，因为：
1. `pipeline` 字段不同：`"init_history"` vs `"update_daily"`
2. `start_date` 不同：`"1990-01-01"` vs lookback 日期

导致即使 `init_history` 已完成到最新日期，`update_daily` 仍会重复执行，反之亦然。

## 设计方案

修改 checkpoint 检查逻辑：**只要存在相同 dataset、code、end_date 的成功 checkpoint，就跳过执行**，不再要求 pipeline 和 start_date 完全匹配。

### 修改文件

#### 1. `src/storage/parquet_store.py`

添加新方法 `checkpoint_succeeded_for_date`：
- 检查是否存在指定 dataset、code、end_date 的成功 checkpoint
- 忽略 pipeline 和 start_date 字段

#### 2. `src/pipeline/common.py`

修改 `should_skip_checkpoint` 函数：
- 先检查当前 pipeline 的精确匹配（保持原有逻辑）
- 如果没有精确匹配，再检查是否有相同 end_date 的成功 checkpoint（跨 pipeline 识别）

## 实现步骤

### Step 1: 在 `parquet_store.py` 添加新方法

在 `pipeline_checkpoint_succeeded` 方法后添加：

```python
def checkpoint_succeeded_for_date(
    self,
    dataset: str,
    code: str,
    end_date: str,
    output_path: str | Path,
) -> bool:
    """Check if any successful checkpoint exists for the given dataset/code/end_date.
    
    This allows cross-pipeline checkpoint recognition between init_history and update_daily.
    """
    if not Path(output_path).exists():
        return False
    checkpoints = self.read_pipeline_checkpoints()
    if checkpoints.empty:
        return False

    work = checkpoints.copy()
    end_keys = pd.to_datetime(work["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    matches = work.loc[
        (work["dataset"].astype("string") == dataset)
        & (work["code"].astype("string") == code)
        & (end_keys == end_date)
    ]
    if matches.empty:
        return False
    latest = matches.sort_values("updated_at").iloc[-1]
    return str(latest["status"]) == "success"
```

### Step 2: 修改 `common.py` 的 `should_skip_checkpoint` 函数

```python
def should_skip_checkpoint(
    store: ParquetStore,
    pipeline: str,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    output_path: Path,
    resume: bool,
    force: bool,
) -> bool:
    if force or not resume:
        return False
    # 先检查当前 pipeline 的精确匹配
    if store.pipeline_checkpoint_succeeded(pipeline, dataset, code, start_date, end_date, output_path):
        return True
    # 再检查是否有相同 end_date 的成功 checkpoint（跨 pipeline 识别）
    return store.checkpoint_succeeded_for_date(dataset, code, end_date, output_path)
```

## 效果

| 场景 | 修改前 | 修改后 |
|------|--------|--------|
| `init_history` 完成后运行 `update_daily` | 重复执行 | 跳过 |
| `update_daily` 完成后运行 `init_history` | 重复执行 | 跳过 |
| 同一 pipeline 重复运行 | 跳过 | 跳过（保持原有行为） |
| `force=True` | 强制执行 | 强制执行（保持原有行为） |
| `resume=False` | 强制执行 | 强制执行（保持原有行为） |

## 注意事项

- 此修改假设：相同 dataset + code + end_date 的成功 checkpoint 意味着数据已完整
- 如果 `update_daily` 使用 `mode="partial"` 只更新部分数据，后续 `init_history` 也会跳过（这可能不是期望行为）
- 如需更精细控制，可考虑添加额外的检查条件
