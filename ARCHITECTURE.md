# 低频量化数据中心架构设计文档

## 1. 系统概述

本系统基于 Python + DuckDB + Parquet 构建，面向 Windows 单机环境的 A 股低频量化数据底座。

系统设计遵循五个核心原则：
- **边界清晰**：核心管道依赖 provider 接口，当前内置 Baostock provider；AkShare 爬虫数据通过独立管道接入
- **数据可靠**：回看覆盖 + 原子写入 + 强 Schema
- **可维护**：状态机 + CLI + 数据质量监控
- **高性能**：Parquet + DuckDB 零拷贝查询
- **弹性容错**：AkShare 端点熔断 + 重试抖动 + 原始响应归档

## 2. 数据源与数据模型（ODS 层）

数据源接入通过两种路径：
1. **Baostock 路径**：通过 `MarketDataProvider` 接口解耦，管道只依赖标准化后的 DataFrame、`DailyKRequest` 请求对象和 `create_provider` 工厂
2. **AkShare 路径**：通过独立的 `AkShareClient` 和 `update_akshare` 管道处理，不经过 `MarketDataProvider` 接口

### 2.1 Baostock 数据源

当前内置 provider 为 `baostock`，封装以下 4 个 Baostock API：
- `query_history_k_data_plus`：历史行情数据
- `query_adjust_factor`：复权因子
- `query_stock_basic`：股票基础信息
- `query_trade_dates`：交易日历

所有字段：
- 字段名完全保留原始 API 名称
- 日期统一转为 date32
- 数值字段强制转换为数值类型（避免 string 漂移）

### 2.2 数据源 provider 抽象

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

### 2.3 AkShare 数据源

AkShare 数据源通过 `src/api/akshare_client.py` 中的 `AkShareClient` 类封装，独立于 `MarketDataProvider` 接口。

**设计原因**：
- AkShare 数据集（机构持股、估值指标）与 Baostock 的日线行情在数据结构、更新频率和 API 特性上差异较大
- AkShare API 是爬虫接口，需要熔断保护和请求抖动，与 Baostock 的 SDK 调用模式不同
- 独立管道可以独立演进，不影响现有 Baostock 管道的稳定性

**AkShareClient 核心设计**：

```python
class AkShareClient:
    """Dataset-specific AkShare wrapper with mapping, retry, jitter, and circuit breakers."""

    def query_stock_institute_hold(self, period: str) -> pd.DataFrame: ...
    def fetch_stock_institute_hold(self, period: str) -> AkShareResponse: ...
    def query_stock_value(self, code: str) -> pd.DataFrame: ...
    def fetch_stock_value(self, code: str) -> AkShareResponse: ...
```

**AkShareResponse 数据结构**：

```python
@dataclass(frozen=True)
class AkShareResponse:
    endpoint: str
    params: dict[str, object]
    akshare_version: str
    raw_df: pd.DataFrame
    data: pd.DataFrame
    data_hash: str
```

**错误类型体系**：

```python
class AkShareError(RuntimeError):
    error_type = "unknown"

class AkShareNetworkError(AkShareError):
    error_type = "network"

class AkShareCircuitOpen(AkShareError):
    error_type = "circuit_open"

class AkShareSchemaDriftError(AkShareError):
    error_type = "schema_drift"

class AkShareEmptyDataError(AkShareError):
    error_type = "empty_data"
```

**熔断机制**：

每个端点维护独立的 `_EndpointState`，包含连续失败计数和熔断截止时间：

```python
@dataclass
class _EndpointState:
    consecutive_failures: int = 0
    circuit_open_until: datetime | None = None
```

- 连续失败次数达到 `failure_threshold`（默认 5）时，端点进入熔断状态
- 熔断期间该端点的请求直接抛出 `AkShareCircuitOpen`
- 冷却时间（默认 30 分钟）后自动恢复
- 成功调用重置连续失败计数

**代码映射**：

AkShare 使用 6 位数字代码，项目使用 `sh.600000` 格式。`CodeMaps` 提供双向映射：

```python
@dataclass(frozen=True)
class CodeMaps:
    six_to_project: Mapping[str, str]
    project_to_six: Mapping[str, str]
```

映射基于本地 `stock_basic` 数据构建，确保代码格式一致性。

### 2.4 历史行情数据（daily_k）

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

### 2.5 复权因子（adjust_factor）

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

### 2.6 股票基础信息（stock_basic）

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

### 2.7 交易日历（calendar）

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

### 2.8 机构持股（stock_institute_hold）

按季度分区存储，每个季度一个 Parquet 文件。数据来源于 AkShare 的 `stock_institute_hold` 接口（新浪数据源）。

📂 存储结构（Hive 分区）

```
data/parquet/stock_institute_hold/report_period=2024Q1/data.parquet
```

📊 Schema

```
- report_period (string)       # 报告期，如 "2024Q1"
- period_end_date (date32)     # 报告期截止日期
- code (string)                # 股票代码（项目格式）
- code_name (string)           # 股票名称
- institution_count (int64)    # 机构数
- institution_count_change (int64)  # 机构数变化
- holding_ratio (float64)      # 持股比例
- holding_ratio_change (float64)    # 持股比例增幅
- float_holding_ratio (float64)     # 占流通股比例
- float_holding_ratio_change (float64)  # 占流通股比例增幅
```

**字段映射**：AkShare 返回中文字段名，通过 `INSTITUTE_HOLD_FIELD_ALIASES` 映射到项目标准字段名。

### 2.9 估值指标（stock_value_em）

按股票代码分区存储，每个代码一个 Parquet 文件。数据来源于 AkShare 的 `stock_value_em` 接口（东方财富数据源）。

📂 存储结构（Hive 分区）

```
data/parquet/stock_value_em/code=sh.600000/data.parquet
```

📊 Schema

```
- date (date32)               # 日期
- code (string)               # 股票代码（项目格式）
- close (float64)             # 当日收盘价
- pct_chg (float64)           # 当日涨跌幅
- total_market_cap (float64)  # 总市值
- float_market_cap (float64)  # 流通市值
- total_shares (float64)      # 总股本
- float_shares (float64)      # 流通股本
- pe_ttm (float64)            # PE(TTM)
- pe_static (float64)         # PE(静)
- pb (float64)                # 市净率
- peg (float64)               # PEG值
- pcf (float64)               # 市现率
- ps (float64)                # 市销率
```

**字段映射**：通过 `STOCK_VALUE_FIELD_ALIASES` 映射 AkShare 中文字段名到项目标准字段名。

**数据去重**：`stock_value_em` 更新时会比较新旧数据的 `dataframe_hash`，如果数据未变化则跳过写入，减少不必要的 I/O。

### 2.10 元数据表

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

### 2.11 AkShare 原始响应归档

每次 AkShare API 调用的原始响应和元数据自动归档到 `data/raw/akshare/`：

📂 归档结构

```
data/raw/akshare/
├── stock_institute_hold/
│   └── 20240426/
│       └── 183015123456_abc123def456_01234567.parquet
├── stock_value_em/
│   └── 20240426/
│       └── 183015654321_def456abc789_87654321.parquet
└── manifest/
    └── fetch_runs.jsonl
```

**文件命名规则**：`{HHMMSSFFFFFF}_{data_hash[:12]}_{uuid[:8]}.parquet`

**JSONL Manifest**：每次调用追加一行 JSON 记录，包含端点、参数、AkShare 版本、行数、数据哈希、原始文件路径、状态、错误信息、起止时间。

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
  background_workers: 3      # 后台处理线程数；background_max_pending 未配置时默认为该值的 4 倍
  # background_max_pending: 16  # 后台待处理任务上限（默认 background_workers * 4）

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
│   │   └── akshare/           # AkShare 原始响应归档
│   │       ├── stock_institute_hold/
│   │       ├── stock_value_em/
│   │       └── manifest/
│   │           └── fetch_runs.jsonl
│   ├── parquet/
│   │   ├── daily_k_none/      # 不复权日线数据
│   │   ├── daily_k_qfq/       # 前复权日线数据
│   │   ├── daily_k_hfq/       # 后复权日线数据
│   │   ├── adjust_factor/     # 复权因子
│   │   ├── stock_basic/       # 股票基础信息快照
│   │   ├── calendar/          # 交易日历
│   │   ├── stock_institute_hold/  # 机构持股（按季度分区）
│   │   └── stock_value_em/        # 估值指标（按代码分区）
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
├── benchmarks/               # 性能基准测试
│   ├── BENCHMARK_README.md   # 基准测试说明
│   ├── benchmark_api_calls.py
│   ├── benchmark_concurrency.py
│   ├── benchmark_data_processing.py
│   ├── benchmark_end_to_end.py
│   ├── benchmark_io_operations.py
│   ├── benchmark_utils.py
│   └── run_all_benchmarks.py
│
├── references/               # BaoStock 等数据源参考文档
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
│   │   └── akshare_client.py     # AkShare API 封装（熔断、重试、字段映射）
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── dataset_catalog.py    # 数据集目录
│   │   ├── duckdb_store.py       # DuckDB 存储层
│   │   ├── metadata_store.py     # DuckDB 元数据存储层
│   │   ├── parquet_store.py      # Parquet 存储层
│   │   └── schema.py             # PyArrow Schema 定义
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── adjustments.py        # 本地复权计算
│   │   ├── akshare_tasks.py      # AkShare 任务规划
│   │   ├── common.py             # 共享工具函数
│   │   ├── repair_tool.py        # 数据修复管道
│   │   ├── services.py           # provider 拉取与元数据批处理服务
│   │   ├── update_akshare.py     # AkShare 爬虫数据更新管道
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
│   │   ├── paths.py              # 路径管理
│   │   └── performance.py        # 性能监控工具
│   ├── __init__.py
│   └── cli.py                    # CLI 入口
│
├── tests/                    # 测试文件
│   ├── conftest.py
│   ├── test_baostock_client.py
│   ├── test_akshare_client.py
│   ├── test_akshare_contract.py
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
│   ├── test_update_daily_fakes.py   # 测试辅助模块
│   ├── test_update_akshare.py
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
- 为 AkShare 管道提供 `expand_akshare_selection()`，统一展开 `all`、单个 AkShare 数据集
- DuckDB 视图和 Parquet 目录创建都从 catalog 派生，避免模块间重复维护数据集列表

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
2. **唯一性约束**：`code + date` 唯一（daily_k、stock_value_em）、`code` 唯一（stock_basic）、`calendar_date` 唯一（calendar）、`report_period + code` 唯一（stock_institute_hold）、`code + dividOperateDate` 唯一（adjust_factor）
3. **单调性约束**：`date` 单调递增（daily_k、stock_value_em）、`dividOperateDate` 单调递增（adjust_factor）
4. **逻辑约束**：`low <= open/close <= high`（警告级别）
5. **非负约束**：`volume >= 0`、`amount >= 0`（警告级别）；`total_market_cap`、`float_market_cap`、`total_shares`、`float_shares` 非负（stock_value_em，警告级别）
6. **停牌允许**：`volume = 0` 是合法的
7. **非空约束**：`report_period`、`period_end_date`、`code` 不允许空值（stock_institute_hold）

验证函数：

```python
def validate_daily_k(df: pd.DataFrame, schema: pa.Schema = DAILY_K_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_ohlc(df)
    validate_non_negative(df, "volume")
    validate_non_negative(df, "amount")

def validate_stock_institute_hold(df: pd.DataFrame, schema: pa.Schema = STOCK_INSTITUTE_HOLD_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    # report_period + code 唯一性
    # report_period, period_end_date, code 非空

def validate_adjust_factor(df: pd.DataFrame, schema: pa.Schema = ADJUST_FACTOR_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    # code + dividOperateDate 唯一性
    # dividOperateDate 单调递增

def validate_stock_value_em(df: pd.DataFrame, schema: pa.Schema = STOCK_VALUE_EM_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_non_negative(df, "total_market_cap")
    validate_non_negative(df, "float_market_cap")
    validate_non_negative(df, "total_shares")
    validate_non_negative(df, "float_shares")
```

### 5.5 续传机制（Checkpoint）

**checkpoint 记录**：

存储在 `data/duckdb/quant.duckdb` 的 `pipeline_checkpoints` 表中，包含：
- pipeline 名称（`update_daily` 或 `update_akshare`）
- dataset 名称
- code 股票代码（或季度标识）
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
background_workers = max(int(config.get("pipeline.background_workers", 3)), 1)
background_max_pending = max(
    int(config.get("pipeline.background_max_pending", background_workers * 4)),
    1,
)

with ThreadPoolExecutor(max_workers=background_workers) as executor:
    # submit _DailyUpdateBackgroundWorker tasks
    # drain completed futures and keep run_records ordered
```

默认配置为 3 个后台 worker；如果未显式配置 `pipeline.background_max_pending`，待处理任务上限为 `background_workers * 4`。前/后复权日线任务会等待同一股票的复权因子任务完成后再计算。

**主线程-工作线程协作**：

`ApiFetchRequest` 允许后台 worker 向主线程请求额外的 API 调用（如回看自愈时的全量重拉），因为 provider API 调用必须在主线程执行：

```python
@dataclass(frozen=True)
class ApiFetchRequest:
    kind: str           # 请求类型，如 "daily_k_full_refetch"
    code: str
    start_date: str
    end_date: str
    datasets: tuple[str, ...]
    reason: str         # 触发原因，如 "unadjusted_empty_lookback"
```

### 5.7 AkShare 管道设计

AkShare 管道通过独立的 `update_akshare` 函数和 `qdc update-akshare` CLI 命令执行，与 Baostock 管道完全解耦。

**任务规划**（`src/pipeline/akshare_tasks.py`）：

```python
@dataclass(frozen=True)
class AkShareTask:
    dataset: str
    key: str                # 季度标识或股票代码
    start_date: str
    end_date: str
    output_path: Path
    report_period: str | None = None  # stock_institute_hold 使用
    code: str | None = None           # stock_value_em 使用
    active: bool = False              # 是否为活跃股票
```

**季度计算**：

```python
def latest_disclosable_quarter(today: date | None = None) -> str:
    """当前可披露的最新季度（当前季度的上一季度）"""

def quarter_range(start_quarter: str, end_quarter: str) -> list[str]:
    """生成季度列表"""

def shift_report_period(report_period: str, offset: int) -> str:
    """季度偏移计算"""
```

**管道流程**：

```
1. plan_akshare_tasks() → 生成任务列表
2. 加载 checkpoint_lookup（如果 resume）
3. 遍历任务：
   a. should_skip_checkpoint() → 跳过已完成的任务
   b. _fetch_task() → 调用 AkShareClient 获取数据
   c. _write_raw_response() → 归档原始响应
   d. _write_task_data() → 写入 Parquet 文件
   e. _append_manifest() → 追加 manifest 记录
   f. metadata_batch.add() → 批量写入元数据
4. metadata_batch.flush() → 刷新剩余元数据
5. build_views() → 构建 DuckDB 视图
```

**stock_value_em 去重优化**：

更新 `stock_value_em` 时会比较新旧数据的哈希值，如果完全相同则跳过写入：

```python
def _stock_value_em_unchanged(store: ParquetStore, code: str, df: pd.DataFrame) -> bool:
    if not store.stock_value_em_path(code).exists():
        return False
    cleaned = store.clean_dataframe_for_schema(df, STOCK_VALUE_EM_DATASET.schema)
    STOCK_VALUE_EM_DATASET.validator(cleaned)
    existing = store.read_stock_value_em(code)
    existing = store.clean_dataframe_for_schema(existing, STOCK_VALUE_EM_DATASET.schema)
    return dataframe_hash(existing) == dataframe_hash(cleaned)
```

### 5.8 DuckDB 查询层

**完整视图列表**：

| 视图名 | 说明 |
|--------|------|
| `v_daily_k_none` | 不复权日线数据 |
| `v_daily_k_qfq` | 前复权日线数据 |
| `v_daily_k_hfq` | 后复权日线数据 |
| `v_adjust_factor` | 复权因子 |
| `v_stock_basic` | 股票基础信息 |
| `v_calendar` | 交易日历 |
| `v_stock_institute_hold` | 机构持股数据 |
| `v_stock_value_em` | 估值指标数据 |

**视图定义**：

```sql
CREATE OR REPLACE VIEW v_daily_k_qfq AS
SELECT *
FROM read_parquet(
    'data/parquet/daily_k_qfq/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
);

CREATE OR REPLACE VIEW v_stock_institute_hold AS
SELECT *
FROM read_parquet(
    'data/parquet/stock_institute_hold/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
);

CREATE OR REPLACE VIEW v_stock_value_em AS
SELECT *
FROM read_parquet(
    'data/parquet/stock_value_em/**/*.parquet',
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

SELECT report_period, code, institution_count, holding_ratio
FROM v_stock_institute_hold
WHERE code = 'sh.600000'
ORDER BY report_period DESC;

SELECT date, code, pe_ttm, pb, total_market_cap
FROM v_stock_value_em
WHERE code = 'sh.600000'
ORDER BY date DESC
LIMIT 10;
```

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
- `--dataset`：数据集选择（`all` / `daily_k_all` / `daily_k`（别名） / `daily_k_none` / `daily_k_qfq` / `daily_k_hfq` / `adjust_factor` / `stock_basic` / `calendar`）
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

AkShare 爬虫数据集更新入口。

```bash
qdc update-akshare
qdc update-akshare --dataset stock_institute_hold --mode full --start-quarter 2005Q1
qdc update-akshare --dataset stock_value_em --code sh.600000
```

**参数**：
- `--dataset`：数据集选择（`all` / `stock_institute_hold` / `stock_value_em`）
- `--mode`：更新模式（`partial` / `full`，默认 `partial`）
- `--start-quarter`：full 模式起始季度（默认 `2005Q1`，来自 `datasets.stock_institute_hold.start_quarter`）；partial 模式为当前可披露季度前推 `api.akshare.lookback_quarters`（默认 8）个季度
- `--end-quarter`：结束季度（默认当前可披露季度）
- `--code`：股票代码（可重复；仅对 stock_value_em 生效）
- `--include-inactive`：在 partial 模式的 stock_value_em 中包含非活跃/非普通股票
- `--max-tasks`：最大任务数
- `--resume/--no-resume`：启用续传（默认启用）
- `--force`：强制重新拉取
- `--build-views/--no-build-views`：完成后构建视图（默认启用）

### 6.3 qdc repair

数据修复。

```bash
qdc repair --code sh.600000 --start 2024-01-01 --end 2024-04-26 --dataset daily_k_hfq
```

**参数**：
- `--code`：股票代码（必填）
- `--start`：开始日期（必填）
- `--end`：结束日期（必填）
- `--dataset`：数据集（必填，支持 `daily_k_none` / `daily_k_qfq` / `daily_k_hfq` / `daily_k_all` / `daily_k`（别名） / `adjust_factor`）
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

**update-akshare stock_value_em**：
- 默认：最新 stock_basic 中 active 代码（`datasets.stock_value_em.active_only` 为 true 时）
- `--include-inactive` 或 `--mode full` 时使用全部代码
- 可选：通过 `--code` 参数指定

**update-akshare stock_institute_hold**：
- 按季度范围生成任务，不涉及代码池

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

### 10.1 Baostock 管道数据流

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

### 10.2 AkShare 管道数据流

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
         ▼
┌─────────────────────┐
│   AkShare API       │ (stock_institute_hold / stock_value_em)
└────────┬────────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
┌─────────────────┐  ┌─────────────────┐
│  Validators     │  │ Raw Archive     │ (原始响应归档)
└────────┬────────┘  └─────────────────┘
         │
         ▼
┌─────────────────────┐
│  ParquetStore       │ (原子写入+重试)
└────────┬────────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
┌─────────────────┐  ┌─────────────────┐
│  Parquet Files  │  │MetadataBatch    │ (批量元数据写入)
└────────┬────────┘  └─────────────────┘
         │              │
         ▼              ▼
┌─────────────────┐  ┌─────────────────┐
│   DuckDB Views  │  │ DuckDB Metadata │
└─────────────────┘  └─────────────────┘
```

### 10.3 续传检查流程

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

**Baostock**：
- **自动重试**：使用 tenacity 库，指数退避，最多 3 次
- **错误记录**：记录到 checkpoint 和 update_runs
- **继续执行**：失败不影响其他股票

**AkShare**：
- **自动重试**：内置重试循环，最多 `api.akshare.max_retries` 次
- **请求抖动**：每次请求前随机延迟 `jitter_seconds` 范围内的秒数
- **熔断保护**：连续失败达到阈值后暂停请求，冷却期后恢复
- **错误分类**：`network`（网络错误）、`circuit_open`（熔断）、`schema_drift`（字段漂移）、`empty_data`（空数据）
- **原始归档**：即使失败也归档已获取的原始响应

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

### 11.5 AkShare Schema 漂移

AkShare 接口可能因源站变更导致字段名变化。系统通过字段别名映射（`INSTITUTE_HOLD_FIELD_ALIASES`、`STOCK_VALUE_FIELD_ALIASES`）处理常见变体：

- 每个目标字段定义多个候选源字段名
- 按优先级顺序匹配
- 必需字段缺失时抛出 `AkShareSchemaDriftError`

## 12. 性能优化

### 12.1 存储优化

- **Parquet 列式存储**：高效压缩和查询
- **Hive 分区**：按 code 分区（daily_k、adjust_factor、stock_value_em），按 report_period 分区（stock_institute_hold），加速查询
- **Schema 强制**：避免类型推断开销

### 12.2 查询优化

- **DuckDB 零拷贝**：直接读取 Parquet 文件
- **分区裁剪**：自动跳过不相关分区
- **列裁剪**：只读取需要的列

### 12.3 更新优化

- **后台处理池**：API 拉取与清洗、复权计算、写入并行推进
- **续传机制**：避免重复拉取
- **回看窗口**：减少数据传输量
- **内存索引**：`PipelineCheckpointLookup` 避免频繁读取元数据表
- **批量元数据写入**：减少 DuckDB 元数据事务次数
- **跨 pipeline checkpoint 识别**：避免不同模式间的重复执行
- **AkShare 数据去重**：`stock_value_em` 更新时比较哈希值，数据未变化时跳过写入

### 12.4 内存优化

- **DuckDB 会自动管理内存**
- **大查询时考虑分批处理**
- **定期重启 Python 进程释放内存**
- **内存索引仅在续传时加载**

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

### 13.3 新增 AkShare 数据集

1. 在 `schema.py` 中定义 Schema
2. 在 `dataset_catalog.py` 中添加数据集定义和 validator
3. 在 `akshare_client.py` 中添加字段别名映射、标准化方法和查询/获取方法
4. 在 `akshare_tasks.py` 中添加任务规划逻辑
5. 在 `update_akshare.py` 中添加任务执行和写入逻辑
6. 在 `settings.yaml` 中添加端点配置
7. 在 `cli.py` 的 `update-akshare` 命令中更新 dataset 选项

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

### 14.2 集成测试

- **Pipeline 测试**：测试完整的数据拉取流程
- **续传测试**：测试 checkpoint 机制
- **修复工具测试**：测试数据修复功能
- **Provider 测试**：测试 provider 创建、Baostock 适配、CLI 参数传递
- **本地复权计算测试**：测试前复权和后复权计算逻辑
- **AkShare 客户端测试**：测试字段映射、代码转换、季度计算
- **AkShare 管道测试**：测试完整的 AkShare 更新流程

### 14.3 测试覆盖

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

### 15.3 数据质量监控

检查日志中的 WARNING：

```powershell
Select-String -Path logs\qdc.log -Pattern "WARNING" | Select-Object -Last 10
```

### 15.4 AkShare 运行监控

查询 AkShare manifest：

```powershell
Get-Content data\raw\akshare\manifest\fetch_runs.jsonl -Tail 10
```

查询 AkShare 更新状态：

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')
df = con.execute("""
    select dataset, code, status, row_count, error_stack
    from update_status
    where dataset in ('stock_institute_hold', 'stock_value_em')
    order by updated_at desc
    limit 20
""").fetchdf()
print(df)
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
- ✔ **多数据源**：Baostock（provider 接口）+ AkShare（独立管道）
- ✔ **弹性容错**：AkShare 端点熔断、重试抖动、原始响应归档
- ✔ **数据去重**：AkShare stock_value_em 哈希比较避免重复写入

## 17. 未来规划

- [ ] 实现 Baostock raw API 缓存
- [ ] 支持分钟级数据
- [ ] 添加数据质量报告
- [ ] 支持多数据源（通过 MarketDataProvider 接口）
- [ ] 添加 Web UI
- [ ] 支持分布式部署
- [ ] 添加数据导出功能
- [ ] 支持自定义数据源插件
- [ ] AkShare 数据源 Schema 漂移自动检测与告警
- [ ] 更多 AkShare 数据集接入（如融资融券、大宗交易等）
