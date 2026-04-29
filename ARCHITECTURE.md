# 低频量化数据中心架构设计文档

## 1. 系统概述

本系统基于 Python + DuckDB + Parquet 构建，面向 Windows 单机环境的 A 股低频量化数据底座。

系统设计遵循四个核心原则：
- **边界清晰**：核心管道依赖 provider 接口，当前内置 Baostock provider
- **数据可靠**：回看覆盖 + 原子写入 + 强 Schema
- **可维护**：状态机 + CLI + 数据质量监控
- **高性能**：Parquet + DuckDB 零拷贝查询

## 2. 数据源与数据模型（ODS 层）

数据源接入通过 `MarketDataProvider` 接口解耦。管道只依赖标准化后的 DataFrame、`DailyKRequest` 请求对象和 `create_provider` 工厂，不直接依赖具体 SDK。

当前内置 provider 为 `baostock`，封装以下 4 个 Baostock API：
- `query_history_k_data_plus`：历史行情数据
- `query_adjust_factor`：复权因子
- `query_stock_basic`：股票基础信息
- `query_trade_dates`：交易日历

所有字段：
- 字段名完全保留原始 API 名称
- 日期统一转为 date32
- 数值字段强制转换为数值类型（避免 string 漂移）

### 2.1 数据源 provider 抽象

`src/api/market_data.py` 定义 provider-neutral 接口：

```python
@dataclass(frozen=True)
class DailyKRequest:
    dataset: str
    code: str
    start_date: str
    end_date: str
    fields: str
    frequency: str

class MarketDataProvider(Protocol):
    name: str
    def query_trade_dates(self, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame: ...
    def query_stock_basic(self, code: str | None = None, code_name: str | None = None) -> pd.DataFrame: ...
    def query_daily_k(self, request: DailyKRequest) -> pd.DataFrame: ...
    def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame: ...
```

Provider 注册与创建：
- `register_provider(name, factory)`：注册 provider factory
- `registered_provider_names()`：列出已注册 provider
- `create_provider(config, provider=None)`：优先使用 CLI `--provider`，否则使用 `api.provider`，默认回退 `baostock`
- `BaostockProvider`：将标准请求映射到 `BaostockClient`；日线 API 只在管道中用于未复权数据，前/后复权由本地因子计算

### 2.2 历史行情数据（daily_k）

本地物化 3 套互相隔离的日线数据集：

| 数据集 | 含义 | adjustflag |
|--------|------|------------|
| daily_k_none | BaoStock 未复权日线 API 获取 | "3" |
| daily_k_qfq | 未复权日线 × foreAdjustFactor 本地计算 | "2" |
| daily_k_hfq | 未复权日线 × backAdjustFactor 本地计算 | "1" |

📂 存储结构（Hive 分区）

```
data/parquet/daily_k_qfq/code=sh.600000/data.parquet
```

📊 Schema（PyArrow）

```
- date (date32)
- code (string)
- open, high, low, close, preclose (float64)
- volume (int64)
- amount (float64)
- adjustflag (string)
- turn (float64)
- tradestatus (string)
- pctChg (float64)
- peTTM, pbMRQ, psTTM, pcfNcfTTM (float64)
- isST (string)
```

### 2.3 复权因子（adjust_factor）

按代码保存 BaoStock 全量复权因子，前/后复权日线均由该数据集计算。

📂 存储结构（Hive 分区）

```
data/parquet/adjust_factor/code=sh.600000/data.parquet
```

📊 Schema

```
- code (string)
- dividOperateDate (date32)
- foreAdjustFactor (float64)
- backAdjustFactor (float64)
- adjustFactor (float64)
```

### 2.4 股票基础信息（stock_basic）

采用单文件存储模式，每次更新覆盖整个文件。

📂 存储结构

```
data/parquet/stock_basic/data.parquet
```

📊 Schema

```
- code (string)
- code_name (string)
- ipoDate (date32)
- outDate (date32, nullable)
- type (string)
- status (string)
```

⚠️ 注意：
- outDate 空值必须写入 NULL，不允许填充默认日期
- 每次更新会删除历史分区目录（如 `snapshot_date=YYYY-MM-DD/`）
- 单文件模式简化了数据管理，避免了多快照的复杂性

### 2.5 交易日历（calendar）

📂 存储结构

```
data/parquet/calendar/data.parquet
```

📊 Schema

```
- calendar_date (date32)
- is_trading_day (string)
```

calendar 保留自然日行，作为项目内交易日解析的唯一来源；刷新时按 calendar_date 合并，避免短窗口覆盖历史日历。

### 2.5 元数据表

#### update_runs（运行日志表）

```
- task_id (string)
- dataset (string)
- code (string)
- status (string)
- start_date (date32)
- end_date (date32)
- start_time (timestamp)
- end_time (timestamp)
- row_count (int64)
- error_stack (string)
```

#### update_status（状态表）

```
- dataset (string)
- code (string)
- last_success_date (date32)
- row_count (int64)
- status (string)
- updated_at (timestamp)
- error_stack (string)
```

#### pipeline_checkpoints（断点记录表）

```
- pipeline (string)
- dataset (string)
- code (string)
- start_date (date32)
- end_date (date32)
- status (string)
- row_count (int64)
- output_path (string)
- updated_at (timestamp)
- error_stack (string)
```

## 3. 全局配置设计（settings.yaml）

所有可变逻辑集中管理：

```yaml
project:
  name: quant_data_center
  timezone: Asia/Shanghai

paths:
  data_dir: data
  raw_dir: data/raw
  parquet_dir: data/parquet
  metadata_dir: data/metadata
  duckdb_dir: data/duckdb
  logs_dir: logs

api:
  provider: baostock
  baostock:
    adjustflag_map:
      none: "3"
      qfq: "2"
      hfq: "1"

datasets:
  daily_k:
    names:
      - daily_k_none
      - daily_k_qfq
      - daily_k_hfq
    fields: "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    frequency: d
  stock_basic:
    fields: "code,code_name,ipoDate,outDate,type,status"
  calendar:
    fields: "calendar_date,is_trading_day"
  adjust_factor:
    fields: "code,dividOperateDate,foreAdjustFactor,backAdjustFactor,adjustFactor"

pipeline:
  lookback_days: 30      # 交易日数量
  raw_cache_days: 7      # 原始数据缓存天数（预留）
  max_retries: 3         # API 调用最大重试次数
  default_code: sh.600000
  metadata_flush_size: 200  # 元数据批量写入阈值

storage:
  duckdb_file: data/duckdb/quant.duckdb

logging:
  file: logs/qdc.log
```

## 4. 项目目录结构

```
quant_data_center/
├── config/
│   ├── settings.yaml          # 主配置文件
│   └── universe.yaml          # 股票池配置（已弃用）
│
├── data/
│   ├── raw/                   # API 原始缓存（预留）
│   ├── parquet/
│   │   ├── daily_k_none/      # 不复权日线数据
│   │   ├── daily_k_qfq/       # 前复权日线数据
│   │   ├── daily_k_hfq/       # 后复权日线数据
│   │   ├── adjust_factor/     # 复权因子
│   │   ├── stock_basic/       # 股票基础信息快照
│   │   └── calendar/          # 交易日历
│   │
│   ├── metadata/
│   │   ├── update_runs.parquet        # 运行日志（追加）
│   │   ├── update_status.parquet      # 当前状态（覆盖）
│   │   └── pipeline_checkpoints.parquet  # 任务断点记录
│   │
│   └── duckdb/
│       └── quant.duckdb       # DuckDB 数据库文件
│
├── logs/
│   └── qdc.log               # 日志文件
│
├── reports/
│   └── performance/          # 性能报告等运行产物（生成后出现）
│
├── scripts/
│   └── run_update_daily.bat  # 定时任务脚本
│
├── src/
│   ├── api/
│   │   ├── market_data.py        # provider 接口、注册表与工厂
│   │   ├── baostock_provider.py  # Baostock provider 适配器
│   │   └── baostock_client.py    # Baostock API 封装
│   ├── storage/
│   │   ├── dataset_catalog.py    # 数据集目录
│   │   ├── duckdb_store.py       # DuckDB 存储层
│   │   ├── parquet_store.py      # Parquet 存储层
│   │   └── schema.py             # PyArrow Schema 定义
│   ├── pipeline/
│   │   ├── adjustments.py        # 本地复权计算
│   │   ├── common.py             # 共享工具函数
│   │   ├── repair_tool.py        # 数据修复管道
│   │   ├── services.py           # provider 拉取与元数据批处理服务
│   │   ├── update_daily.py       # 日常更新与历史初始化管道
│   │   └── write_queue.py        # 异步写入队列
│   ├── quality/
│   │   └── validators.py         # 数据验证器
│   ├── utils/
│   │   ├── config_mgr.py         # 配置管理
│   │   ├── logging.py            # 日志配置
│   │   └── paths.py              # 路径管理
│   └── cli.py                    # CLI 入口
│
├── tests/                    # 测试文件
│   ├── conftest.py
│   ├── test_baostock_client.py
│   ├── test_adjustments.py
│   ├── test_cli_provider.py
│   ├── test_code_pool.py
│   ├── test_dataset_catalog.py
│   ├── test_duckdb_store.py
│   ├── test_market_data_provider.py
│   ├── test_parquet_store.py
│   ├── test_pipeline_resume.py
│   ├── test_repair_tool.py
│   ├── test_schema.py
│   ├── test_trading_dates.py
│   └── test_validators.py
│
├── pyproject.toml            # 项目配置
├── README.md                 # 项目说明
└── ARCHITECTURE.md           # 架构设计文档
```

### 4.1 数据集目录（Dataset Catalog）

`src/storage/dataset_catalog.py` 是数据集元信息的中心：
- 维护数据集名称、Schema、validator、DuckDB view name 和是否按 code 分区
- 为存储层提供 `daily_k_definition()`、`dataset_definition()` 等查询函数
- 为 CLI 和管道提供 `expand_daily_k_selection()`，统一展开 `all`、`daily_k_all`、单个 daily_k 数据集
- DuckDB 视图和 Parquet 目录创建都从 catalog 派生，避免模块间重复维护 daily_k 数据集列表

## 5. 核心机制设计

### 5.1 回看覆盖（Lookback Update）

每日更新逻辑：

1. 未显式传入 `--end` 时，按 `project.timezone` 获取本地时间，18:00 前取前一自然日，18:00 后取当天
2. 用 calendar 将候选自然日解析为不晚于该日期的最近交易日
3. 按交易日数量获取最近 N 个交易日窗口（默认 30）
4. 与本地数据按 date 合并，去重（保留最新）
5. 全量覆盖写入该股票文件

解决问题：
- 停复牌数据缺失
- 历史数据修正
- 复权变动
- 周末/节假日运行不会产生自然日 checkpoint

### 5.2 回看自愈机制

当发现以下情况时，自动从 `1990-01-01` 重新拉取该股票的全部历史数据：

1. **回看窗口为空**：`empty_lookback`
   - 股票刚上市或长期停牌后复牌
   - 本地数据缺失

2. **回看窗口数据冲突**：`lookback_mismatch`
   - 回看窗口内的数据与本地数据不一致
   - 复权因子发生变化
   - 数据源修正历史数据

3. **复权因子变化**：`adjust_factor_changed`
   - 本地 `adjust_factor` 文件与新拉取全量因子不同
   - 前/后复权日线从 `1990-01-01` 开始用未复权历史数据重算

这种机制确保了数据的完整性和一致性，避免部分历史数据被遗漏或错误。

### 5.3 原子写入（Atomic Write）

写入流程：

```
1. 写入 data.{uuid}.tmp.parquet
2. → 校验成功
3. → os.replace 覆盖 data.parquet
```

保证：
- 不产生半文件
- 崩溃安全
- 数据完整性

**Windows 兼容性增强**：

Windows 系统中，文件可能被其他进程（杀毒软件、备份程序）临时锁定。系统实现了自动重试机制：

```python
def atomic_write(self, df: pd.DataFrame, schema: pa.Schema, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.parent / f"data.{uuid.uuid4().hex}.tmp.parquet"
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    
    last_error: Exception | None = None
    for attempt in range(PARQUET_WRITE_MAX_RETRIES):
        try:
            pq.write_table(table, tmp_path)
            os.replace(tmp_path, destination)
            return
        except (PermissionError, OSError) as e:
            last_error = e
            if tmp_path.exists():
                tmp_path.unlink()  # 清理临时文件
            if attempt < PARQUET_WRITE_MAX_RETRIES - 1:
                delay = PARQUET_WRITE_RETRY_DELAY * (2 ** attempt)
                time.sleep(delay)  # 指数退避
    raise last_error
```

**读取重试机制**：

```python
def _safe_read_parquet(self, path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(PARQUET_READ_MAX_RETRIES):
        try:
            return pd.read_parquet(path)
        except PermissionError as e:
            last_error = e
            if attempt < PARQUET_READ_MAX_RETRIES - 1:
                delay = PARQUET_READ_RETRY_DELAY * (2 ** attempt)
                time.sleep(delay)
    raise last_error
```

实现代码：

```python
PARQUET_READ_MAX_RETRIES = 3
PARQUET_READ_RETRY_DELAY = 0.1
PARQUET_WRITE_MAX_RETRIES = 3
PARQUET_WRITE_RETRY_DELAY = 0.1
```

### 5.4 数据质量校验（Validators）

入库前必须满足：

1. **Schema 匹配**：列名、类型完全一致
2. **唯一性约束**：`code + date` 唯一（daily_k）、`code` 唯一（stock_basic）、`calendar_date` 唯一（calendar）
3. **单调性约束**：`date` 单调递增
4. **逻辑约束**：`low <= open/close <= high`（警告级别）
5. **非负约束**：`volume >= 0`、`amount >= 0`（警告级别）
6. **停牌允许**：`volume = 0` 是合法的

验证函数：

```python
def validate_daily_k(df: pd.DataFrame, schema: pa.Schema = DAILY_K_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_ohlc(df)
    validate_non_negative(df, "volume")
    validate_non_negative(df, "amount")
```

### 5.5 续传机制（Checkpoint）

**checkpoint 记录**：

存储在 `data/metadata/pipeline_checkpoints.parquet`，包含：
- pipeline 名称（`update_daily`）
- dataset 名称
- code 股票代码
- start_date / end_date（解析后的交易日）
- status（`success` / `failed`）
- row_count
- output_path
- updated_at
- error_stack

**内存索引优化**：

`PipelineCheckpointLookup` 类在内存中构建 checkpoint 索引，避免频繁读取 Parquet 文件：

```python
class PipelineCheckpointLookup:
    """In-memory checkpoint index for hot resume checks."""

    def __init__(self, checkpoints: pd.DataFrame) -> None:
        self._pipeline_status: dict[tuple[str, str, str, str, str], str] = {}
        self._date_status: dict[tuple[str, str, str, str], str] = {}
        # ... 构建索引

    def pipeline_checkpoint_succeeded(
        self, pipeline, dataset, code, start_date, end_date, output_path
    ) -> bool:
        """精确匹配 (pipeline, dataset, code, start_date, end_date)"""

    def checkpoint_succeeded_for_date(
        self, pipeline, dataset, code, end_date, output_path
    ) -> bool:
        """日期匹配 (pipeline, dataset, code, end_date)"""
```

**续传逻辑**：

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
    checkpoint_lookup: PipelineCheckpointLookup | None = None,
) -> bool:
    if force or not resume:
        return False
    if checkpoint_lookup is not None:
        return (
            checkpoint_lookup.pipeline_checkpoint_succeeded(...)
            or checkpoint_lookup.checkpoint_succeeded_for_date(...)
        )
    return (
        store.pipeline_checkpoint_succeeded(...)
        or store.checkpoint_succeeded_for_date(...)
    )
```

**元数据批量写入**：

`PipelineMetadataBatch` 类实现元数据批量写入：

- 累积 run_rows、status_rows、checkpoint_rows
- 按 `count_by="run"` 或 `count_by="checkpoint"` 统计待刷新的行数
- 达到 `pipeline.metadata_flush_size` 阈值（默认 200）时自动刷新
- 减少 Parquet 文件写入次数
- 提升整体性能

### 5.6 异步写入队列（PipelineWriteQueue）

为了提升数据拉取效率，系统使用异步写入队列：

**优势**：
- API 拉取和磁盘写入并行执行
- 减少总体运行时间
- 更好的资源利用

**实现**：

```python
class PipelineWriteQueue:
    def submit(self, fn, on_error=None, description=""):
        # 提交写入任务到队列
        
    def close(self):
        # 等待所有任务完成并返回结果
```

### 5.7 DuckDB 查询层

**视图定义**：

```sql
CREATE OR REPLACE VIEW v_daily_k_qfq AS
SELECT *
FROM read_parquet(
    'data/parquet/daily_k_qfq/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
);
```

**查询示例**：

```sql
SELECT *
FROM v_daily_k_qfq
WHERE code='sh.600000'
AND date > '2023-01-01';
```

**优势**：
- 零拷贝查询
- 自动分区裁剪
- 列式存储高效压缩
- 在数据文件尚不存在时构建空视图，保持查询入口稳定

## 6. CLI 指令体系

### 6.1 qdc update-daily

日常增量更新与历史初始化统一入口。

```bash
qdc update-daily
qdc update-daily --mode full --dataset all --start 1990-01-01
```

**参数**：
- `--dataset`：数据集选择（`all` / `daily_k_all` / `daily_k_none` / `daily_k_qfq` / `daily_k_hfq` / `adjust_factor` / `stock_basic` / `calendar`）
- `--start`：full 模式开始日期（默认 `1990-01-01`）
- `--code`：股票代码（可重复；partial 默认使用 active 代码，full 默认使用全部 stock_basic 代码）
- `--universe`：已弃用的股票池名称，读取 `config/universe.yaml`
- `--lookback-days`：回看交易日数量（默认 30）
- `--end`：目标日期（默认按 18:00 cutoff 规则）
- `--mode`：更新模式（`partial` / `full`，默认 `partial`）
- `--provider`：数据源名称，默认使用 `api.provider`
- `--resume/--no-resume`：启用续传（默认启用）
- `--force`：强制重新拉取
- `--build-views/--no-build-views`：完成后构建视图（默认启用）

### 6.2 qdc repair

数据修复。

```bash
qdc repair --code sh.600000 --start 2024-01-01 --end 2024-04-26 --dataset daily_k_hfq
```

**参数**：
- `--code`：股票代码（必填）
- `--start`：开始日期（必填）
- `--end`：结束日期（必填）
- `--dataset`：数据集（必填，支持 `daily_k_all`）
- `--provider`：数据源名称，默认使用 `api.provider`
- `--build-views/--no-build-views`：完成后构建视图（默认启用）

### 6.3 qdc build-views

手动构建 DuckDB 视图。

```bash
qdc build-views
```

## 7. 交易日历处理

### 7.1 日期解析流程

```
1. 获取候选自然日（按 18:00 cutoff）
2. 从 calendar 查找不晚于候选日的最近交易日
3. 使用交易日进行后续操作
```

### 7.2 核心函数

```python
def latest_trading_day_on_or_before(calendar_df: pd.DataFrame, value: str | date) -> str:
    """查找不晚于指定日期的最近交易日"""
    
def first_trading_day_on_or_after(calendar_df: pd.DataFrame, value: str | date) -> str:
    """查找不早于指定日期的最近交易日"""
    
def trading_day_lookback_start(calendar_df: pd.DataFrame, end_date: str | date, lookback_days: int) -> str:
    """计算回看起始交易日"""
```

### 7.3 18:00 Cutoff 规则

```python
def default_candidate_date(config: ConfigManager, now: datetime | None = None) -> str:
    """根据当前时间确定候选日期"""
    timezone_name = str(config.get("project.timezone", "Asia/Shanghai"))
    local_zone = ZoneInfo(timezone_name)
    local_now = datetime.now(local_zone)
    
    candidate = local_now.date()
    if local_now.time() < time(18, 0):
        candidate -= timedelta(days=1)
    return candidate.isoformat()
```

## 8. 代码池管理

### 8.1 代码池来源

**update-daily full 模式**：
- 默认：最新 stock_basic 快照中的全部非空 code
- 可选：通过 `--code` 参数指定
- 兼容：`--universe` 参数（已弃用）

**update-daily partial 模式**：
- 默认：最新 stock_basic 快照中 type=1 且 status=1 的上市股票代码
- 可选：通过 `--code` 参数指定
- 兼容：`--universe` 参数（已弃用）

### 8.2 代码池解析

```python
def resolve_codes(
    config: ConfigManager,
    store: ParquetStore,
    code: tuple[str, ...] | list[str] | str | None,
    universe: str | None,
    stock_basic_mode: str,
) -> list[str]:
    """解析股票代码列表"""
    if isinstance(code, str):
        return [code]
    if code:
        return [str(item) for item in code]
    if universe:
        logger.warning("--universe/config/universe.yaml is deprecated; prefer stock_basic-derived code pools.")
        return config.universe_codes(universe)
    codes = store.stock_basic_codes(stock_basic_mode)
    if not codes:
        raise ValueError("No stock codes found in stock_basic data")
    return codes
```

## 9. Windows 自动化运行

### 9.1 批处理脚本

```batch
@echo off
setlocal
cd /d "%~dp0.."
if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)
python -m src.cli update-daily
endlocal
```

### 9.2 任务计划程序配置

**触发器**：
- 触发时间：交易日 18:10
- 重复间隔：每天
- 持续时间：无限期

**操作**：
- 启动程序：`scripts\run_update_daily.bat`
- 起始于：项目根目录

## 10. 数据流图

```
┌─────────────────┐
│ CLI / Pipeline  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ create_provider │ (CLI 参数或 api.provider)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│MarketDataProvider│ (标准查询接口)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ BaostockProvider │ (当前内置实现)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ BaostockClient  │ (自动重试、错误处理)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Baostock API   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Validators    │ (数据质量校验)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│PipelineWriteQueue│ (异步写入队列)
└────────┬────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
┌─────────────────┐  ┌─────────────────┐
│  ParquetStore   │  │MetadataBatch    │ (批量元数据写入)
│ (原子写入+重试) │  └─────────────────┘
└────────┬────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
┌─────────────────┐  ┌─────────────────┐
│  Parquet Files  │  │  Metadata Files │
└────────┬────────┘  └─────────────────┘
         │
         ▼
┌─────────────────┐
│   DuckDB Views  │ (零拷贝查询)
└─────────────────┘
```

**续传检查流程**：

```
┌─────────────────┐
│ Pipeline Start  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│Load Checkpoints │ (一次性加载到内存)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│CheckpointLookup │ (内存索引)
└────────┬────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
┌─────────────────┐  ┌─────────────────┐
│ Skip (matched)  │  │ Execute (new)   │
└─────────────────┘  └─────────────────┘
```

## 11. 错误处理策略

### 11.1 API 调用错误

- **自动重试**：使用 tenacity 库，指数退避，最多 3 次
- **错误记录**：记录到 checkpoint 和 update_runs
- **继续执行**：失败不影响其他股票

### 11.2 数据验证错误

- **硬错误**：Schema 不匹配、唯一性冲突、单调性违反
- **软警告**：OHLC 逻辑异常、负值检测
- **处理方式**：硬错误阻止写入，软警告仅记录日志

### 11.3 写入错误

- **原子写入**：确保不会产生半文件
- **错误传播**：向上传播异常，记录到 checkpoint
- **状态更新**：更新 update_status 为 failed

### 11.4 Windows 文件锁定错误

- **读取重试**：`PermissionError` 时指数退避重试，最多 3 次
- **写入重试**：`PermissionError` 或 `OSError` 时指数退避重试，最多 3 次
- **临时文件清理**：写入失败时自动清理临时文件
- **日志记录**：记录重试过程，便于问题排查

## 12. 性能优化

### 12.1 存储优化

- **Parquet 列式存储**：高效压缩和查询
- **Hive 分区**：按 code 分区，加速查询
- **Schema 强制**：避免类型推断开销

### 12.2 查询优化

- **DuckDB 零拷贝**：直接读取 Parquet 文件
- **分区裁剪**：自动跳过不相关分区
- **列裁剪**：只读取需要的列

### 12.3 更新优化

- **异步写入队列**：并行拉取和写入
- **续传机制**：避免重复拉取
- **回看窗口**：减少数据传输量
- **内存索引**：`PipelineCheckpointLookup` 避免频繁磁盘读取
- **批量元数据写入**：减少 Parquet 文件写入次数

### 12.4 内存优化

- **DuckDB 会自动管理内存**
- **大查询时考虑分批处理**
- **定期重启 Python 进程释放内存**
- **内存索引仅在续传时加载**

## 13. 扩展性设计

### 13.1 新增数据集

1. 在 `schema.py` 中定义 Schema
2. 在 `dataset_catalog.py` 中添加数据集定义、validator 和视图名
3. 在 `settings.yaml` 中添加配置
4. 在 `parquet_store.py` 中添加读写方法
5. 在 `duckdb_store.py` 中添加视图或复用 catalog 生成逻辑

### 13.2 新增数据源 provider

1. 实现 `MarketDataProvider` 协议，返回符合项目 Schema 的 DataFrame
2. 根据需要定义 provider 内部客户端和字段映射
3. 使用 `register_provider()` 注册稳定的小写名称
4. 在 `settings.yaml` 的 `api.provider` 或 CLI `--provider` 中选择该 provider

### 13.3 新增验证规则

1. 在 `validators.py` 中添加验证函数
2. 在对应的 validate_* 函数中调用

### 13.4 新增 CLI 命令

1. 在 `cli.py` 中添加命令函数
2. 使用 `@cli.command()` 装饰器
3. 调用对应的 pipeline 函数

## 14. 测试策略

### 14.1 单元测试

- **Schema 测试**：验证 Schema 定义正确性
- **验证器测试**：测试各种数据质量场景
- **存储层测试**：测试读写操作和原子写入
- **数据集目录测试**：验证 catalog 展开、视图生成和存储布局

### 14.2 集成测试

- **Pipeline 测试**：测试完整的数据拉取流程
- **续传测试**：测试 checkpoint 机制
- **修复工具测试**：测试数据修复功能
- **Provider 测试**：测试 provider 创建、Baostock 适配、CLI 参数传递

### 14.3 测试覆盖

```powershell
pytest -q
python -m src.cli update-daily --help
python -m src.cli repair --help
python -m src.cli build-views --help
```

## 15. 监控与运维

### 15.1 日志监控

- **日志文件**：`logs/qdc.log`
- **日志级别**：INFO / WARNING / ERROR
- **日志轮转**：10 MB 轮转，保留 30 天

### 15.2 状态监控

查询 `update_status` 表：

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')
df = con.execute("""
    select dataset, status, count(*) as count
    from read_parquet('data/metadata/update_status.parquet')
    group by dataset, status
""").fetchdf()
print(df)
```

### 15.3 数据质量监控

检查日志中的 WARNING：

```powershell
Select-String -Path logs\qdc.log -Pattern "WARNING" | Select-Object -Last 10
```

## 16. 总结

该架构实现了：

- ✔ **无未来函数污染**：复权隔离，三种复权模式独立存储
- ✔ **无数据损坏风险**：原子写入，崩溃安全
- ✔ **可追踪**：状态机记录所有操作
- ✔ **可修复**：repair 工具精准修复
- ✔ **可扩展**：模块化设计，易于添加新数据集
- ✔ **高性能**：DuckDB + Parquet 零拷贝查询
- ✔ **自动化**：续传机制，定时任务支持
- ✔ **数据质量**：多层验证，自动修复机制
- ✔ **Windows 兼容**：文件锁定重试，原子写入增强
- ✔ **性能优化**：内存索引，批量写入，异步队列

## 17. 未来规划

- [ ] 实现 raw API 缓存
- [ ] 支持分钟级数据
- [ ] 添加数据质量报告
- [ ] 支持多数据源
- [ ] 添加 Web UI
- [ ] 支持分布式部署
- [ ] 添加数据导出功能
- [ ] 支持自定义数据源插件
