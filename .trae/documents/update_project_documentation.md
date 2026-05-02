# 项目文档更新计划

## 概述

根据项目代码的实际检查,需要更新以下文档以反映最新的功能和技术实现:
- README.md
- ARCHITECTURE.md

## 主要更新内容

### 1. AkShare 并发拉取功能

#### 1.1 新增功能说明

**位置**: README.md 功能特性部分

**更新内容**:
- 新增 "AkShare 并发拉取" 功能特性说明
- 说明 `stock_value_em` 数据集支持并发拉取,提升数据更新效率
- 说明自适应并发控制机制,根据成功率动态调整并发度

**具体描述**:
```
- **AkShare 并发拉取**: `stock_value_em` 数据集支持并发拉取,通过 `--workers` 参数或 `api.akshare.workers` 配置项控制并发度,默认 3 个并发 worker
- **自适应并发控制**: 内置 `_AdaptiveConcurrencyController` 根据请求成功率动态调整并发度,失败率过高时自动降速,成功率恢复后自动提速
```

#### 1.2 CLI 参数更新

**位置**: README.md `qdc update-akshare` 参数说明表格

**更新内容**:
在参数表格中新增:

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--workers` | `config/settings.yaml` 中的 `api.akshare.workers` (默认 3) | `stock_value_em` 并发拉取的 worker 数量 |

#### 1.3 配置文件说明更新

**位置**: README.md 配置文件部分

**更新内容**:
在 `config/settings.yaml` 示例中,更新 `api.akshare` 部分:

```yaml
api:
  akshare:
    max_retries: 3
    workers: 3                    # 新增: stock_value_em 并发拉取 worker 数量
    jitter_seconds: [2, 8]
    lookback_quarters: 8
    endpoints:
      stock_institute_hold:
        source: sina
        failure_threshold: 5
        cooldown_minutes: 30
      stock_value_em:
        source: eastmoney
        failure_threshold: 5
        cooldown_minutes: 30
```

在配置说明表格中新增:

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `api.akshare.workers` | 3 | stock_value_em 并发拉取的 worker 数量 |

#### 1.4 性能优化建议更新

**位置**: README.md 性能优化建议部分

**更新内容**:
在 "AkShare 优化" 部分新增:

```
- 使用 `--workers` 参数调整并发度,根据网络环境和源站响应能力优化性能
- 自适应并发控制会自动调整并发度,无需手动干预
```

### 2. 架构设计文档更新

#### 2.1 AkShare 管道设计更新

**位置**: ARCHITECTURE.md 第 5.7 节 "AkShare 管道设计"

**更新内容**:
在管道流程后新增 "并发拉取机制" 小节:

```
**并发拉取机制**:

`stock_value_em` 数据集支持并发拉取,通过 `_AdaptiveConcurrencyController` 实现自适应并发控制:

```python
class _AdaptiveConcurrencyController:
    """Conservative fetch concurrency control for crawler-style AkShare endpoints."""
    
    def __init__(
        self,
        max_workers: int,
        window_size: int = 20,
        failure_rate_threshold: float = 0.15,
        recovery_successes: int = 50,
        consecutive_failure_threshold: int = 3,
    ) -> None:
        # ...
    
    def record_fetch_result(self, success: bool) -> None:
        # 根据成功率动态调整并发度
```

**控制策略**:
- **降速条件**:
  - 连续失败次数达到 `consecutive_failure_threshold` (默认 3 次)
  - 滑动窗口内失败率超过 `failure_rate_threshold` (默认 15%)
- **提速条件**:
  - 连续成功次数达到 `recovery_successes` (默认 50 次)
- **并发度范围**: 1 到 `max_workers` (由 `api.akshare.workers` 配置)

**优势**:
- 根据源站响应能力自动调整并发度
- 避免过载导致被限流
- 在网络状况良好时最大化吞吐量
```

#### 2.2 配置设计更新

**位置**: ARCHITECTURE.md 第 3 节 "全局配置设计"

**更新内容**:
在 `api.akshare` 配置部分新增 `workers` 字段:

```yaml
api:
  akshare:
    max_retries: 3
    workers: 3                    # 新增
    jitter_seconds: [2, 8]
    lookback_quarters: 8
    endpoints:
      stock_institute_hold:
        source: sina
        failure_threshold: 5
        cooldown_minutes: 30
      stock_value_em:
        source: eastmoney
        failure_threshold: 5
        cooldown_minutes: 30
```

#### 2.3 CLI 指令体系更新

**位置**: ARCHITECTURE.md 第 6.2 节 "qdc update-akshare"

**更新内容**:
在参数列表中新增:

```
- `--workers`: 并发拉取 worker 数量(仅对 stock_value_em 生效),默认使用 `api.akshare.workers`
```

#### 2.4 性能优化更新

**位置**: ARCHITECTURE.md 第 12.3 节 "更新优化"

**更新内容**:
在 AkShare 相关优化部分新增:

```
- **AkShare 并发拉取**: stock_value_em 支持并发拉取,通过线程池和自适应并发控制器优化性能
- **自适应并发控制**: 根据请求成功率动态调整并发度,避免过载和被限流
```

### 3. 配置默认值修正

#### 3.1 pipeline.background_workers 默认值

**位置**: README.md 和 ARCHITECTURE.md 中所有提到 `background_workers` 默认值的地方

**更新内容**:
- 文档中写的是默认 3,但实际配置文件中是 4
- 需要统一更新为: "默认 4,由 `pipeline.background_workers` 配置"

**涉及位置**:
- README.md 功能特性部分
- README.md 配置文件说明部分
- ARCHITECTURE.md 配置设计部分
- ARCHITECTURE.md 后台处理池部分

### 4. 数据流图更新

**位置**: ARCHITECTURE.md 第 10.2 节 "AkShare 管道数据流"

**更新内容**:
在数据流图中添加并发拉取的表示:

```
┌─────────────────────┐
│ CLI update-akshare  │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ plan_akshare_tasks  │ (生成任务列表)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   AkShareClient     │ (熔断、重试、抖动、字段映射)
└────────┬────────────┘
         │
         ├──────────────────────┐
         │                      │
         ▼                      ▼
┌─────────────────┐  ┌─────────────────────┐
│ stock_institute │  │ stock_value_em      │ (并发拉取)
│ _hold (串行)    │  │ ThreadPoolExecutor  │
└────────┬────────┘  │ + AdaptiveCtrl      │
         │           └────────┬────────────┘
         │                    │
         ▼                    ▼
┌─────────────────┐  ┌─────────────────┐
│   AkShare API   │  │   AkShare API   │
└────────┬────────┘  └────────┬────────┘
         │                    │
         └────────┬───────────┘
                  │
                  ▼
         ┌─────────────────┐
         │   Validators    │
         └────────┬────────┘
                  │
                  ▼
         ┌─────────────────────┐
         │  ParquetStore       │
         └────────┬────────────┘
                  │
                  ├──────────────┐
                  │              │
                  ▼              ▼
         ┌─────────────────┐  ┌─────────────────┐
         │  Parquet Files  │  │MetadataBatch    │
         └────────┬────────┘  └─────────────────┘
                  │              │
                  ▼              ▼
         ┌─────────────────┐  ┌─────────────────┐
         │   DuckDB Views  │  │ DuckDB Metadata │
         └─────────────────┘  └─────────────────┘
```

## 实施步骤

### 步骤 1: 更新 README.md

1. 在功能特性部分添加 AkShare 并发拉取和自适应并发控制的说明
2. 更新 `qdc update-akshare` 参数说明表格,添加 `--workers` 参数
3. 更新配置文件示例,添加 `api.akshare.workers` 配置项
4. 更新配置说明表格,添加 `api.akshare.workers` 说明
5. 更新性能优化建议部分,添加并发拉取相关建议
6. 修正 `pipeline.background_workers` 默认值(从 3 改为 4)

### 步骤 2: 更新 ARCHITECTURE.md

1. 在第 5.7 节 "AkShare 管道设计" 中添加并发拉取机制的详细说明
2. 更新第 3 节 "全局配置设计" 中的 `api.akshare` 配置示例
3. 更新第 6.2 节 "qdc update-akshare" 的参数说明
4. 更新第 12.3 节 "更新优化" 中的 AkShare 优化部分
5. 更新第 10.2 节 "AkShare 管道数据流" 的数据流图
6. 修正所有提到 `background_workers` 默认值的地方

### 步骤 3: 验证更新

1. 检查所有更新是否与实际代码一致
2. 确保配置示例与 `config/settings.yaml` 一致
3. 确保参数说明与 `src/cli.py` 一致
4. 确保架构说明与实际实现一致

## 注意事项

1. **保持一致性**: 确保所有文档中的配置默认值、参数说明与实际代码保持一致
2. **完整性**: 确保所有新增功能都在文档中得到体现
3. **准确性**: 确保技术细节的描述准确无误
4. **可读性**: 保持文档的清晰易懂,避免过于技术化的描述

## 预期成果

更新后的文档将:
1. 完整反映项目当前的功能特性
2. 准确描述 AkShare 并发拉取的实现机制
3. 提供准确的配置说明和 CLI 参数说明
4. 帮助用户更好地理解和使用项目的各项功能
