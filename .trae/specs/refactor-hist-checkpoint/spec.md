# Refactor hist Checkpoint Mechanism Spec

## Why
当前 `update_akshare_hist` 的断点续传机制存在两个问题：
1. `start_date` 和 `end_date` 是运行时指定的参数，而非已存储数据的实际日期范围，导致 checkpoint 记录不准确
2. 断点续传判断在任务循环中进行，即使任务会被跳过也需要先调用 API，造成不必要的 API 调用

## What Changes
- 修改 `AkShareHistTask` 的 `start_date` 和 `end_date` 为已存储数据的实际日期范围（参考 `stock_value_em`）
- 新增 `_prefilter_hist_tasks()` 预过滤函数，在调用 API 之前跳过 `end_date >= calendar_date` 的任务
- 修改 `plan_akshare_hist_tasks()` 函数，从已存储文件中读取实际日期范围

## Impact
- Affected specs: `update_akshare_hist` pipeline
- Affected code: 
  - `src/pipeline/update_akshare_hist.py`
  - `src/pipeline/akshare_tasks.py`（可能需要调整）

## ADDED Requirements

### Requirement: Hist Task Date Range from Stored Data
系统应当从已存储的 parquet 文件中读取实际的数据日期范围，而非使用运行时参数。

#### Scenario: Existing data file
- **WHEN** 已存在某股票的历史数据文件
- **THEN** `start_date` 应为文件中日期列的最小值，`end_date` 应为文件中日期列的最大值

#### Scenario: No existing data file
- **WHEN** 某股票的历史数据文件不存在
- **THEN** `start_date` 和 `end_date` 应为 `None`

### Requirement: Pre-filter Tasks by Calendar Date
系统应当在调用 API 之前预过滤任务，跳过已同步到最新日历日期的任务。

#### Scenario: Task end_date >= calendar date
- **WHEN** 任务的 `end_date` 大于或等于最新日历日期
- **AND** 输出文件已存在
- **THEN** 应当跳过该任务，不调用 API

#### Scenario: Task end_date < calendar date
- **WHEN** 任务的 `end_date` 小于最新日历日期
- **THEN** 应当执行该任务

#### Scenario: Task end_date is None
- **WHEN** 任务的 `end_date` 为 `None`（新股票，无历史数据）
- **THEN** 应当执行该任务

## MODIFIED Requirements

### Requirement: AkShareHistTask Date Fields
`AkShareHistTask` 的 `start_date` 和 `end_date` 字段应当反映已存储数据的实际日期范围，而非运行时参数。

**原实现**:
```python
AkShareHistTask(
    start_date="1990-01-01",  # 来自配置或参数
    end_date="2026-05-03",    # 来自参数或自动计算
)
```

**新实现**:
```python
AkShareHistTask(
    start_date="2020-01-05",  # 已存储数据的最小日期
    end_date="2026-05-02",    # 已存储数据的最大日期
)
```

### Requirement: Checkpoint Record Accuracy
checkpoint 记录中的 `start_date` 和 `end_date` 应当准确反映实际处理的数据范围。
