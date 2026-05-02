# 低频量化数据中心架构设计文档

## 1. 系统概述

本系统基于 Python + DuckDB + Parquet 构建，面向 Windows 单机环境的 A 股低频量化数据底座。

系统设计遵循四个核心原则：
- **边界清晰**：行情服务型数据源走 provider 接口；爬虫型 AkShare 数据走独立采集管道
- **数据可靠**：回看覆盖 + 原子写入 + 强 Schema
- **可维护**：状态机 + CLI + 数据质量监控 + 原始响应追踪
- **高性能**：Parquet + DuckDB 零拷贝查询

## 2. 数据源与数据模型（ODS 层）

项目按数据源形态分为两类接入方式：

1. **服务型行情 provider**：稳定 SDK/API，适合抽象为 `MarketDataProvider`。当前 Baostock 属于这一类。
2. **爬虫型数据集 collector**：网页接口形态异构、字段变化和反爬风险更高，按数据集独立建模和调度。AkShare 属于这一类。

`MarketDataProvider` 只承载行情类通用能力。不要为了接入 AkShare 的 `stock_institute_hold`、`stock_value_em` 在 `MarketDataProvider` 上追加专用方法；这两个接口应走 `AkshareClient` + `update_akshare` 管道。

当前内置 provider 为 `baostock`，封装以下 4 个 Baostock API：
- `query_history_k_data_plus`：历史行情数据
- `query_adjust_factor`：复权因子
- `query_stock_basic`：股票基础信息
- `query_trade_dates`：交易日历

所有字段：
- Baostock 已有数据集保持当前字段名契约
- AkShare 新数据集使用项目标准化字段名，避免中文字段直接扩散到存储和查询层
- 日期统一转为 date32
- 数值字段强制转换为数值类型（避免 string 漂移）
- 存储层不得修改数据源字段值的格式或内容；股票代码格式归属数据源，业务标准化不在数据存储层处理

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

### 2.6 AkShare 扩展数据集（crawler ODS）

AkShare 接口作为爬虫型数据源接入，要求：

- 通过 `src/api/akshare_client.py` 封装 AkShare 顶层函数调用、字段映射、请求参数转换、限速、重试和熔断
- 通过 `src/pipeline/update_akshare.py` 编排任务，不进入 `update_daily`
- MVP 阶段不要新增通用 `source_runtime.py`；限速和熔断先作为 AkShare client 的内部实现
- 不修改 `pipeline_checkpoints` schema；AkShare 特有元信息写入 raw manifest

#### stock_institute_hold（机构持股一览）

来源：AkShare `stock_institute_hold`，底层目标为新浪财经机构持股一览表。任务粒度为财报季度。

存储结构：

```
data/parquet/stock_institute_hold/report_period=2020Q1/data.parquet
```

Schema：

```
- report_period (string)          # 2020Q1
- period_end_date (date32)        # 2020-03-31
- code (string)                   # AkShare 源侧代码，如 600000 / 000001
- code_name (string)
- institution_count (int64)
- institution_count_change (int64)
- holding_ratio (float64)         # %
- holding_ratio_change (float64)  # %
- float_holding_ratio (float64)   # %
- float_holding_ratio_change (float64)
```

主键约束：`report_period + code` 唯一。

刷新策略：
- `full`：从 `--start-quarter`（默认 `2005Q1`）到可披露季度逐季拉取
- `partial`：刷新最近 `api.akshare.lookback_quarters` 个季度，用于覆盖补发和历史修正

#### stock_value_em（个股估值历史）

来源：AkShare `stock_value_em`，底层目标为东方财富估值分析详情。任务粒度为股票代码。

存储结构：

```
data/parquet/stock_value_em/code=600000/data.parquet
```

Schema：

```
- date (date32)
- code (string)
- close (float64)
- pct_chg (float64)               # %
- total_market_cap (float64)      # 元
- float_market_cap (float64)      # 元
- total_shares (float64)          # 股
- float_shares (float64)          # 股
- pe_ttm (float64)
- pe_static (float64)
- pb (float64)
- peg (float64)
- pcf (float64)
- ps (float64)
```

主键约束：`code + date` 唯一，按 `date` 单调递增。

刷新策略：
- `full`：默认处理 `stock_basic` 中 `type == "1"` 的全部普通股票，也可通过 `--code` 指定
- `partial`：默认只处理 active 股票，即 `stock_basic` 中 `type == "1"` 且 `status == "1"`；退市或停用普通股票需显式 `--include-inactive` 或 `--mode full`
- 非普通股票（如 `type == "2"` 指数）不会进入默认任务池
- AkShare 该接口没有日期窗口参数，因此 partial 仍按股票全量拉取，再由本地数据 hash、最大日期和 checkpoint 判断是否覆盖写入

#### 代码格式边界

AkShare 多数接口使用 6 位证券代码。`--code sh.600000` 或来自 `stock_basic` 的项目格式代码，只允许在请求 AkShare 前转换为 `symbol=600000`；AkShare 返回或补入到 AkShare 数据集的 `code` 必须保持源侧 6 位格式，不得再转换为 `sh.600000` / `sz.000001`。数据标准化属于采集参数适配或后续分析层，不在存储层处理。

### 2.7 元数据表

运行元数据当前由 `DuckDBMetadataStore` 写入 `data/duckdb/quant.duckdb` 中的 DuckDB 表。`data/metadata/*.parquet` 是旧版元数据文件位置；如果这些文件存在，元数据层会在初始化时迁移一次到 DuckDB。

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
  akshare:
    max_retries: 3
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
  stock_institute_hold:
    start_quarter: 2005Q1
  stock_value_em:
    active_only: true

pipeline:
  lookback_days: 10      # 交易日数量
  raw_cache_days: 7      # 原始数据缓存天数（预留）
  max_retries: 3         # API 调用最大重试次数
  default_code: sh.600000
  metadata_flush_size: 200  # 元数据批量写入阈值
  background_workers: 4      # 后台处理线程数；background_max_pending 未配置时默认为该值的 4 倍

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
│   ├── raw/
│   │   └── akshare/           # AkShare 原始响应缓存和 fetch manifest
│   ├── parquet/
│   │   ├── daily_k_none/      # 不复权日线数据
│   │   ├── daily_k_qfq/       # 前复权日线数据
│   │   ├── daily_k_hfq/       # 后复权日线数据
│   │   ├── adjust_factor/     # 复权因子
│   │   ├── stock_basic/       # 股票基础信息快照
│   │   ├── calendar/          # 交易日历
│   │   ├── stock_institute_hold/  # AkShare 机构持股一览
│   │   └── stock_value_em/        # AkShare 个股估值历史
│   │
│   ├── duckdb/
│   │   └── quant.duckdb       # DuckDB 数据库文件，包含查询视图和运行元数据表
│   │
│   └── metadata/              # 旧版 Parquet 元数据迁移兼容目录
│
├── logs/
│   └── qdc.log               # 日志文件
│
├── benchmark_results/         # 性能基准报告输出
│
├── scripts/
│   └── run_update_daily.bat  # 定时任务脚本
│
├── src/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── market_data.py        # provider 接口、注册表与工厂
│   │   ├── baostock_provider.py  # Baostock provider 适配器
│   │   ├── baostock_client.py    # Baostock API 封装
│   │   └── akshare_client.py     # AkShare 爬虫型接口封装、限速和熔断
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── dataset_catalog.py    # 数据集目录
│   │   ├── duckdb_store.py       # DuckDB 存储层
│   │   ├── parquet_store.py      # Parquet 存储层
│   │   └── schema.py             # PyArrow Schema 定义
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── adjustments.py        # 本地复权计算
│   │   ├── common.py             # 共享工具函数
│   │   ├── repair_tool.py        # 数据修复管道
│   │   ├── akshare_tasks.py      # AkShare 季度任务和股票任务规划
│   │   ├── services.py           # provider 拉取与元数据批处理服务
│   │   ├── update_akshare.py     # AkShare 数据集更新管道入口
│   │   ├── update_daily.py       # 日常更新与历史初始化管道入口
│   │   ├── update_daily_calendar.py  # 更新日历窗口与写入
│   │   ├── update_daily_frames.py    # 日线 DataFrame 处理辅助
│   │   ├── update_daily_metadata.py  # 更新元数据写入辅助
│   │   ├── update_daily_targets.py   # 更新目标与断点预过滤
│   │   ├── update_daily_types.py     # 日更管道共享类型
│   │   ├── update_daily_worker.py    # 日更后台写入 worker
│   │   └── write_queue.py        # 写入队列工具（保留模块）
│   ├── quality/
│   │   ├── __init__.py
│   │   └── validators.py         # 数据验证器
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config_mgr.py         # 配置管理
│   │   ├── logging.py            # 日志配置
│   │   └── paths.py              # 路径管理
│   ├── __init__.py
│   └── cli.py                    # CLI 入口
│
├── tests/                    # 测试文件
│   ├── conftest.py
│   ├── test_baostock_client.py
│   ├── test_akshare_client.py
│   ├── test_update_akshare.py
│   ├── test_adjustments.py
│   ├── test_cli_provider.py
│   ├── test_code_pool.py
│   ├── test_dataset_catalog.py
│   ├── test_duckdb_store.py
│   ├── test_market_data_provider.py
│   ├── test_parquet_store.py
│   ├── test_update_daily_full_resume.py
│   ├── test_update_daily_partial_resume.py
│   ├── test_update_daily_refetch.py
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
- AkShare 数据集也必须注册到 catalog，但任务展开逻辑独立放在 `akshare_tasks.py`，避免污染 daily_k 选择语义

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
            with self._parquet_write_lock:
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

存储在 `data/duckdb/quant.duckdb` 的 `pipeline_checkpoints` 表中，包含：
- pipeline 名称（例如 `update_daily`、`update_akshare`）
- dataset 名称
- code 股票代码
- start_date / end_date（解析后的交易日）
- status（`success` / `failed`）
- row_count
- output_path
- updated_at
- error_stack

**内存索引优化**：

`PipelineCheckpointLookup` 类在内存中构建 checkpoint 索引，避免频繁读取 DuckDB 元数据表：

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
- 减少 DuckDB 元数据事务次数
- 提升整体性能

### 5.6 后台处理池与并发限流

为了提升数据拉取效率，`update_daily` 将 provider API 调用保留在主线程，并把清洗、复权计算、Parquet 写入和元数据批处理提交给后台处理池：

**优势**：
- API 拉取和后台数据处理并行推进
- 减少总体运行时间
- 更好的资源利用

**实现**：

```python
background_workers = max(int(config.get("pipeline.background_workers", 4)), 1)
background_max_pending = max(
    int(config.get("pipeline.background_max_pending", background_workers * 4)),
    1,
)

with ThreadPoolExecutor(max_workers=background_workers) as executor:
    # submit _DailyUpdateBackgroundWorker tasks
    # drain completed futures and keep run_records ordered
```

默认配置为 4 个后台 worker；如果未显式配置 `pipeline.background_max_pending`，待处理任务上限为 `background_workers * 4`。前/后复权日线任务会等待同一股票的复权因子任务完成后再计算。

### 5.7 AkShare 采集运行时

AkShare 管道以稳定采集为第一目标，不追求高并发：

- `AkshareClient` 内部按 endpoint 维护策略和状态，MVP 阶段不抽取独立 `source_runtime.py`
- endpoint 状态包括连续失败次数、冷却截止时间、最近错误类型
- 每次真实调用前执行 jitter sleep，默认区间来自 `api.akshare.jitter_seconds`
- 单个 endpoint 连续失败达到 `failure_threshold` 后进入 cooldown；cooldown 内跳过该 endpoint 的后续任务并记录失败
- `stock_institute_hold` 和 `stock_value_em` 分别维护 endpoint 状态，互不熔断

AkShare 原始缓存和 manifest：

```
data/raw/akshare/
├── stock_institute_hold/report_period=2020Q1/
├── stock_value_em/code=600000/
└── manifest/fetch_runs.jsonl
```

`fetch_runs.jsonl` 记录 AkShare 特有元信息：

```
{
  "pipeline": "update_akshare",
  "dataset": "stock_value_em",
  "endpoint": "stock_value_em",
  "code": "600000",
  "params": {"symbol": "600000"},
  "akshare_version": "1.x.x",
  "row_count": 1234,
  "data_hash": "...",
  "raw_path": "...",
  "status": "success",
  "error_type": "",
  "error_message": "",
  "started_at": "...",
  "ended_at": "..."
}
```

MVP 阶段 checkpoint 仍复用 `pipeline_checkpoints`，精确匹配 `pipeline + dataset + code + start_date + end_date + output_path`。不要为 AkShare 第一阶段改动 `PIPELINE_CHECKPOINTS_SCHEMA`；如果后续需要统一查询 source fetch 详情，再新增独立 `source_fetch_runs` 表。

### 5.8 DuckDB 查询层

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

AkShare 数据集视图：

- `v_stock_institute_hold`：读取 `data/parquet/stock_institute_hold/**/*.parquet`
- `v_stock_value_em`：读取 `data/parquet/stock_value_em/**/*.parquet`，其中 `code` 为 AkShare 源侧 6 位代码（如 `600000`）

**优势**：
- 零拷贝查询
- 自动分区裁剪
- 列式存储高效压缩
- 在数据文件尚不存在时构建空视图，保持查询入口稳定

## 6. CLI 指令体系

### 6.1 qdc update-daily

统一数据更新入口，支持日常增量更新和历史全量初始化。

```bash
qdc update-daily
qdc update-daily --mode full --dataset all --start 1990-01-01
```

**参数**：
- `--dataset`：数据集选择（`all` / `daily_k_all` / `daily_k_none` / `daily_k_qfq` / `daily_k_hfq` / `adjust_factor` / `stock_basic` / `calendar`）
- `--start`：full 模式开始日期（默认 `1990-01-01`）
- `--code`：股票代码（可重复；partial 默认使用 active 代码，full 默认使用全部 stock_basic 代码）
- `--universe`：已弃用的股票池名称，读取 `config/universe.yaml`
- `--lookback-days`：回看交易日数量（默认 10）
- `--end`：目标日期（默认按 18:00 cutoff 规则）
- `--mode`：更新模式（`partial` / `full`，默认 `partial`）
- `--provider`：数据源名称，默认使用 `api.provider`
- `--resume/--no-resume`：启用续传（默认启用）
- `--force`：强制重新拉取
- `--build-views/--no-build-views`：完成后构建视图（默认启用）

**模式说明**：
- `partial` 模式：日常回看更新，使用 lookback 窗口机制
- `full` 模式：历史全量初始化，从 `1990-01-01` 拉取到目标交易日

### 6.2 qdc update-akshare

AkShare 爬虫型数据集更新入口，独立于 `update-daily`。

```bash
qdc update-akshare --dataset stock_institute_hold --mode full --start-quarter 2005Q1
qdc update-akshare --dataset stock_value_em --mode partial --max-tasks 100
qdc update-akshare --dataset all --mode partial
```

**参数**：
- `--dataset`：数据集选择（`all` / `stock_institute_hold` / `stock_value_em`）
- `--mode`：更新模式（`partial` / `full`，默认 `partial`）
- `--start-quarter`：机构持股 full 模式开始季度，默认 `datasets.stock_institute_hold.start_quarter`
- `--end-quarter`：机构持股结束季度；未传入时自动解析到当前可披露季度
- `--code`：股票代码（可重复）；仅作用于 `stock_value_em`
- `--include-inactive`：partial 模式也处理 inactive 普通股票；默认不启用
- `--max-tasks`：最多执行任务数，用于分批采集和降低触发风控概率
- `--resume/--no-resume`：启用续传（默认启用）
- `--force`：忽略 checkpoint 强制重新拉取
- `--build-views/--no-build-views`：完成后构建视图（默认启用）

**模式说明**：
- `stock_institute_hold partial`：刷新最近 `api.akshare.lookback_quarters` 个季度
- `stock_institute_hold full`：从 `--start-quarter` 到 `--end-quarter` 逐季刷新
- `stock_value_em partial`：默认刷新 active 股票；接口无日期窗口，按股票全量拉取后本地去重覆盖
- `stock_value_em full`：刷新全部普通股票或显式 `--code` 股票

### 6.3 qdc repair

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

### 6.4 qdc build-views

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

**update-akshare stock_value_em partial 模式**：
- 默认：复用 `stock_basic_codes(mode="active")`，即 `type == "1"` 且 `status == "1"`
- 可选：通过 `--code` 参数指定一个或多个股票
- 可选：通过 `--include-inactive` 包含退市股或其他 inactive 普通股票
- `full` 模式默认使用 `stock_basic` 中全部 `type == "1"` 普通股票
- 非普通股票（如 `type == "2"` 指数）不会进入默认 `stock_value_em` 任务池

**update-akshare stock_institute_hold 模式**：
- 不依赖股票代码池，任务由季度列表生成
- 写入前仍要使用 `stock_basic` 进行 6 位代码到项目代码格式的映射；映射缺失时记录 warning，并保留可追踪的原始代码信息

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

**Baostock 行情数据流**：

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
│BackgroundWorker │ (后台清洗/复权/写入)
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
│  Parquet Files  │  │ DuckDB Metadata │
└────────┬────────┘  └─────────────────┘
         │
         ▼
┌─────────────────┐
│   DuckDB Views  │ (零拷贝查询)
└─────────────────┘
```

**AkShare 爬虫型数据流**：

```
┌────────────────────┐
│ qdc update-akshare │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│  akshare_tasks.py  │ (季度任务 / 股票任务)
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│  AkshareClient     │ (字段映射、限速、重试、熔断)
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ AkShare functions  │ (stock_institute_hold / stock_value_em)
└─────────┬──────────┘
          │
          ├───────────────┐
          │               │
          ▼               ▼
┌────────────────────┐  ┌────────────────────┐
│ Raw cache +        │  │ Normalized          │
│ fetch manifest     │  │ DataFrame           │
└────────────────────┘  └─────────┬──────────┘
                                   │
                                   ▼
                         ┌────────────────────┐
                         │ Validators         │
                         └─────────┬──────────┘
                                   │
                                   ▼
                         ┌────────────────────┐
                         │ ParquetStore       │
                         └─────────┬──────────┘
                                   │
                                   ▼
                         ┌────────────────────┐
                         │ DuckDB Views       │
                         └────────────────────┘
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

- **Baostock 自动重试**：使用 tenacity 库，指数退避，最多 3 次
- **AkShare 稳态采集**：按 endpoint 执行 jitter、重试和熔断；不做高并发爬取
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

### 11.5 AkShare 数据源错误

AkShare 错误需要区分来源，避免把反爬、空数据和字段变化混为一类：

- **网络/连接错误**：记录 `error_type=network`，按 endpoint 重试
- **熔断跳过**：记录 `error_type=circuit_open`，不写入目标 Parquet
- **字段缺失或字段改名**：记录 `error_type=schema_drift`，验证失败并阻止覆盖旧数据
- **空数据**：按数据集判断语义；`stock_institute_hold` 全市场季度为空默认视为失败，`stock_value_em` 单只股票为空需结合 stock_basic 状态判断
- **AkShare 版本变化**：manifest 记录 `akshare_version`，方便回溯接口行为变化

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

- **后台处理池**：API 拉取与清洗、复权计算、写入并行推进
- **续传机制**：避免重复拉取
- **回看窗口**：减少数据传输量
- **AkShare 分批采集**：通过 `--max-tasks`、低并发和 endpoint cooldown 控制单次运行压力
- **内存索引**：`PipelineCheckpointLookup` 避免频繁读取元数据表
- **批量元数据写入**：减少 DuckDB 元数据事务次数
- **跨 pipeline checkpoint 识别**：避免不同模式间的重复执行

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

### 13.2 新增服务型行情 provider

1. 实现 `MarketDataProvider` 协议，返回符合项目 Schema 的 DataFrame
2. 根据需要定义 provider 内部客户端和字段映射
3. 使用 `register_provider()` 注册稳定的小写名称
4. 在 `settings.yaml` 的 `api.provider` 或 CLI `--provider` 中选择该 provider

该路径适用于 Baostock、商业行情 API、稳定 SDK 等服务型数据源。不适用于 AkShare 这类字段和访问策略高度依赖单个网页接口的数据源。

### 13.3 新增爬虫型数据集

1. 在 `schema.py` 中定义标准化 Schema，避免中文字段直接进入长期存储契约
2. 在 `dataset_catalog.py` 中注册数据集、validator、view name 和分区方式
3. 在对应 client 中封装 endpoint 调用、字段映射、限速和失败分类
4. 在任务规划模块中定义自然任务粒度，例如季度、股票、日期或分页
5. 在专用 pipeline 中复用 checkpoint、原子写入和 DuckDB view 构建
6. 在 raw manifest 中记录 endpoint、参数、版本、row_count、hash、错误类型和 raw_path

### 13.4 新增验证规则

1. 在 `validators.py` 中添加验证函数
2. 在对应的 validate_* 函数中调用

### 13.5 新增 CLI 命令

1. 在 `cli.py` 中添加命令函数
2. 使用 `@cli.command()` 装饰器
3. 调用对应的 pipeline 函数

## 14. 测试策略

### 14.1 单元测试

- **Schema 测试**：验证 Schema 定义正确性
- **验证器测试**：测试各种数据质量场景
- **存储层测试**：测试读写操作和原子写入
- **数据集目录测试**：验证 catalog 展开、视图生成和存储布局
- **AkShare client 测试**：通过 fake callable 测字段映射、请求 symbol 转换、源侧代码存储、空数据、重试和熔断状态

### 14.2 集成测试

- **Pipeline 测试**：测试完整的数据拉取流程
- **续传测试**：测试 checkpoint 机制
- **修复工具测试**：测试数据修复功能
- **Provider 测试**：测试 provider 创建、Baostock 适配、CLI 参数传递
- **本地复权计算测试**：测试前复权和后复权计算逻辑
- **AkShare 管道测试**：使用 fake client 覆盖季度任务、股票任务、`--max-tasks`、`--resume`、`--force` 和 active-only 代码池

### 14.3 Contract 测试

AkShare 真实联网测试默认关闭，只在显式设置环境变量时运行：

```powershell
$env:RUN_AKSHARE_CONTRACT="1"
pytest -q tests/test_akshare_contract.py
```

Contract 测试只验证少量样本：
- `stock_institute_hold` 最近可披露季度返回非空且字段可映射
- `stock_value_em` 对固定样本股票返回非空且 date/code 主键可生成
- 不在普通 CI 或日常 `pytest -q` 中访问真实 AkShare

### 14.4 测试覆盖

```powershell
pytest -q
python -m src.cli update-daily --help
python -m src.cli update-akshare --help
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
    from update_status
    group by dataset, status
""").fetchdf()
print(df)
```

AkShare 特有 fetch 详情从 raw manifest 查看：

```powershell
Get-Content data\raw\akshare\manifest\fetch_runs.jsonl -Tail 20
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
- ✔ **性能优化**：内存索引，批量元数据写入，后台处理池，跨 pipeline checkpoint 识别
- ✔ **统一入口**：`update_daily` 支持 partial 和 full 两种模式
- ✔ **爬虫源隔离**：AkShare 数据集使用独立 client、pipeline、限速熔断和 raw manifest

## 17. 未来规划

- [ ] 完成 AkShare `stock_institute_hold` 和 `stock_value_em` 管道实现
- [ ] 增加 raw cache 清理和 manifest 查询工具
- [ ] 支持分钟级数据
- [ ] 添加数据质量报告
- [ ] 支持更多服务型 provider 和爬虫型数据集
- [ ] 添加 Web UI
- [ ] 支持分布式部署
- [ ] 添加数据导出功能
- [ ] 支持自定义数据源插件
