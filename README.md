# Quant Data Center

Windows 单机低频量化数据中心，使用 Python 拉取 A 股低频数据，使用 Parquet 做本地存储，使用 DuckDB 做零拷贝查询。当前内置 Baostock 数据源（日线行情、复权因子、股票基础信息、交易日历），并通过 provider 接口保留后续接入其他数据源的扩展点；同时集成 AkShare 爬虫数据源（机构持股、估值指标），通过独立的 `update-akshare` 管道拉取。

## 功能特性

- **多复权模式支持**：同时支持不复权、前复权、后复权三种日线数据；前/后复权由本地复权因子计算生成
- **统一更新入口**：`qdc update-daily` 支持 `partial`（日常增量）和 `full`（历史全量）两种模式
- **AkShare 爬虫数据**：`qdc update-akshare` 支持机构持股（按季度）和估值指标（按股票）两种数据集
- **续传机制**：通过 checkpoint 记录任务状态，支持断点续传；跨 pipeline 识别避免重复执行
- **数据验证**：写入前自动验证 OHLC 逻辑、volume/amount 非负等约束
- **原子写入**：使用临时文件确保数据完整性，Windows 文件锁定自动重试
- **自动重试**：Baostock API 调用失败时自动重试（指数退避）；AkShare 端点级别熔断保护
- **数据源抽象**：通过 MarketDataProvider 接口隔离数据源实现，CLI 可用 `--provider` 选择数据源
- **交易日历**：默认日期按 18:00 cutoff 生成候选日，并通过 calendar 回退到最近交易日
- **回看自愈**：日常更新发现回看窗口数据冲突或窗口为空时，自动从 `1990-01-01` 重拉该代码历史数据
- **后台处理池**：使用 `ThreadPoolExecutor` 与 `_DailyUpdateBackgroundWorker` 并行处理清洗、复权计算、写入和元数据落库，默认 4 个后台 worker
- **内存索引优化**：PipelineCheckpointLookup 内存索引减少批量任务中的重复元数据读取
- **批量元数据写入**：PipelineMetadataBatch 批量写入 DuckDB 元数据表，减少小事务开销
- **AkShare 熔断与重试**：端点级别连续失败计数、熔断冷却、请求间随机抖动，防止被源站限流
- **AkShare 原始数据归档**：每次 AkShare API 调用的原始响应和元数据写入 `data/raw/akshare/` 归档，附带 JSONL manifest

## 环境要求

- Python >= 3.10
- Windows 操作系统

## 安装

```powershell
py -m venv venv
.\venv\Scripts\activate
python -m pip install -U pip
python -m pip install -e .
```

依赖在 `pyproject.toml` 中声明：`pandas`、`pyarrow`、`duckdb`、`akshare`、`baostock`、`pydantic`、`pyyaml`、`loguru`、`tenacity`、`click`、`pytest`。

## CLI 命令

### qdc update-daily

统一数据更新入口。默认执行日常增量更新；使用 `--mode full` 初始化历史数据。

```powershell
qdc update-daily
qdc update-daily --mode full --dataset all --start 1990-01-01 --end 2024-04-26 --code sh.600000
```

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--dataset` | `all` | 数据集：`daily_k_none`/`daily_k_qfq`/`daily_k_hfq`/`daily_k_all`/`daily_k`（别名）/`adjust_factor`/`all`/`stock_basic`/`calendar` |
| `--start` | `1990-01-01` | full 模式的开始日期，格式 `YYYY-MM-DD`；非交易日时顺延到区间内首个交易日 |
| `--code` | partial：最新 `stock_basic` 中 active 代码；full：最新 `stock_basic` 中全部代码 | 股票代码，可重复指定多个 |
| `--universe` | 无 | 已弃用的股票池名称，读取 `config/universe.yaml`，仅用于兼容旧流程 |
| `--lookback-days` | `config/settings.yaml` 中的 `pipeline.lookback_days` (默认 10) | 回看交易日数量 |
| `--end` | 18:00 前为前一自然日，18:00 后为当天，并回退到最近交易日 | 目标日期，格式 `YYYY-MM-DD`；显式传入非交易日时回退到最近交易日 |
| `--mode` | `partial` | `partial` 为日常回看更新；`full` 为历史全量初始化 |
| `--provider` | `config/settings.yaml` 中的 `api.provider` | 数据源名称；当前内置 `baostock` |
| `--resume/--no-resume` | `--resume` | 启用续传 |
| `--force` | 否 | 强制重新拉取 |
| `--build-views/--no-build-views` | `--build-views` | 完成后构建 DuckDB 视图 |

**行为说明**：

1. 未传 `--end` 时，18:00 前先以"前一自然日"为候选日，18:00 后以"当天"为候选日
2. 拉取或读取交易日历，将候选日回退为 calendar 中不晚于该日期的最近交易日
3. 用目标交易日刷新 `stock_basic` 快照，并用交易日写入 checkpoint
4. 按代码拉取未复权日线和全量复权因子；前/后复权日线由本地因子计算生成
5. 若回看窗口为空，自动改为从 `1990-01-01` 到目标交易日重拉该代码数据
6. 若回看窗口与本地同日期数据存在差异，自动改为从 `1990-01-01` 到目标交易日重拉该代码数据，防止更早历史数据也已变化
7. full 模式按 `--start`/`--end` 解析交易区间，但 daily_k 文件始终从 `1990-01-01` 拉到目标交易日，保证本地单代码文件为完整历史

### qdc update-akshare

AkShare 爬虫数据集更新入口。支持机构持股和估值指标两种数据集。

```powershell
qdc update-akshare
qdc update-akshare --dataset stock_institute_hold --mode full --start-quarter 2005Q1
qdc update-akshare --dataset stock_value_em --code sh.600000 --code sh.600001
qdc update-akshare --dataset stock_value_em --include-inactive
qdc update-akshare --max-tasks 50
```

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--dataset` | `all` | 数据集：`all`/`stock_institute_hold`/`stock_value_em` |
| `--mode` | `partial` | `partial` 为日常增量更新；`full` 为历史全量初始化 |
| `--start-quarter` | full 模式：`config/settings.yaml` 中的 `datasets.stock_institute_hold.start_quarter`（默认 `2005Q1`）；partial 模式：当前可披露季度前推 `api.akshare.lookback_quarters`（默认 8）个季度 | stock_institute_hold 的起始季度，格式 `YYYYQN` |
| `--end-quarter` | 当前可披露季度（当前季度的上一季度） | stock_institute_hold 的结束季度，格式 `YYYYQN` |
| `--code` | stock_value_em：最新 `stock_basic` 中 active 代码（`datasets.stock_value_em.active_only` 为 true 时） | 股票代码，可重复指定多个；仅对 stock_value_em 生效 |
| `--include-inactive` | 否 | 在 partial 模式的 stock_value_em 中包含非活跃/非普通股票 |
| `--max-tasks` | 无 | 本次运行最多执行的任务数 |
| `--resume/--no-resume` | `--resume` | 启用续传 |
| `--force` | 否 | 强制重新拉取 |
| `--build-views/--no-build-views` | `--build-views` | 完成后构建 DuckDB 视图 |

**行为说明**：

1. AkShare 管道独立于 Baostock 管道，不经过 `MarketDataProvider` 接口
2. `stock_institute_hold` 按季度拉取，每个季度一个 Parquet 文件
3. `stock_value_em` 按股票代码拉取，每个代码一个 Parquet 文件
4. 每次调用自动归档原始响应到 `data/raw/akshare/`，并写入 JSONL manifest
5. 内置端点级别熔断机制：连续失败超过阈值后暂停请求，冷却期后自动恢复
6. 请求间自动添加随机抖动延迟，防止被源站限流

### qdc repair

修复指定股票、日期范围的数据，替换本地同区间数据，保留区间外历史数据。

```powershell
qdc repair --code sh.600000 --start 2024-01-01 --end 2024-04-26 --dataset daily_k_hfq
```

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--code` | 必填 | 股票代码，如 `sh.600000` |
| `--start` | 必填 | 开始日期，格式 `YYYY-MM-DD`；非交易日时顺延到区间内首个交易日 |
| `--end` | 必填 | 结束日期，格式 `YYYY-MM-DD`；非交易日时回退到最近交易日 |
| `--dataset` | 必填 | 数据集：`daily_k_none`/`daily_k_qfq`/`daily_k_hfq`/`daily_k_all`/`adjust_factor` |
| `--provider` | `config/settings.yaml` 中的 `api.provider` | 数据源名称；当前内置 `baostock` |
| `--build-views/--no-build-views` | `--build-views` | 完成后构建 DuckDB 视图 |

**使用场景**：

- 发现某只股票某段时间数据异常时，可以精准修复该区间
- 不影响其他时间段的历史数据
- 支持同时修复三种复权数据（`daily_k_all`）

### qdc build-views

手动构建 DuckDB 视图，扫描当前 Parquet 文件。

```powershell
qdc build-views
```

## 数据存储结构

```
data/
├── parquet/
│   ├── daily_k_none/           # 不复权日线
│   │   └── code=sh.600000/
│   │       └── data.parquet
│   ├── daily_k_qfq/            # 前复权日线
│   │   └── code=sh.600000/
│   │       └── data.parquet
│   ├── daily_k_hfq/            # 后复权日线
│   │   └── code=sh.600000/
│   │       └── data.parquet
│   ├── adjust_factor/          # 复权因子
│   │   └── code=sh.600000/
│   │       └── data.parquet
│   ├── stock_basic/            # 股票基础信息（单文件存储）
│   │   └── data.parquet
│   ├── calendar/               # 交易日历
│   │   └── data.parquet
│   ├── stock_institute_hold/   # 机构持股（按季度分区）
│   │   └── report_period=2024Q1/
│   │       └── data.parquet
│   └── stock_value_em/         # 估值指标（按股票代码分区）
│       └── code=sh.600000/
│           └── data.parquet
├── duckdb/
│   └── quant.duckdb            # DuckDB 数据库文件，包含查询视图和运行元数据表
├── raw/
│   └── akshare/                # AkShare 原始响应归档
│       ├── stock_institute_hold/
│       │   └── YYYYMMDD/
│       │       └── HHMMSSFFFF_hash_prefix_uuid.parquet
│       ├── stock_value_em/
│       │   └── YYYYMMDD/
│       │       └── HHMMSSFFFF_hash_prefix_uuid.parquet
│       └── manifest/
│           └── fetch_runs.jsonl  # AkShare 调用 manifest
├── metadata/                   # 旧版 Parquet 元数据兼容目录；存在时会迁移到 DuckDB
└── logs/                       # 日志文件（实际位于项目根目录 logs/）
```

**注意**：日志文件实际存储在项目根目录的 `logs/` 目录下（`logs/qdc.log`），而非 `data/logs/`。`stock_basic` 采用单文件存储模式，每次更新会覆盖整个文件。历史分区目录（如 `snapshot_date=YYYY-MM-DD/`）会在更新时自动清理。`update_runs`、`update_status`、`pipeline_checkpoints` 当前保存在 `data/duckdb/quant.duckdb` 的 DuckDB 表中；旧版 `data/metadata/*.parquet` 文件仅作为一次性迁移来源。

## 数据 Schema

### daily_k（日线数据）

| 字段 | 类型 | 说明 |
|------|------|------|
| date | date32 | 交易日期 |
| code | string | 股票代码 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| preclose | float64 | 前收盘价 |
| volume | int64 | 成交量 |
| amount | float64 | 成交额 |
| adjustflag | string | 复权标志 |
| turn | float64 | 换手率 |
| tradestatus | string | 交易状态 |
| pctChg | float64 | 涨跌幅 |
| peTTM | float64 | 市盈率TTM |
| pbMRQ | float64 | 市净率 |
| psTTM | float64 | 市销率TTM |
| pcfNcfTTM | float64 | 市现率TTM |
| isST | string | 是否ST |

### stock_basic（股票基础信息）

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 股票代码 |
| code_name | string | 股票名称 |
| ipoDate | date32 | 上市日期 |
| outDate | date32 | 退市日期（可为空） |
| type | string | 证券类型 |
| status | string | 上市状态 |

### calendar（交易日历）

| 字段 | 类型 | 说明 |
|------|------|------|
| calendar_date | date32 | 日期 |
| is_trading_day | string | 是否交易日 |

### adjust_factor（复权因子）

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 股票代码 |
| dividOperateDate | date32 | 除权除息日 |
| foreAdjustFactor | float64 | 前复权因子 |
| backAdjustFactor | float64 | 后复权因子 |
| adjustFactor | float64 | BaoStock 原始复权因子 |

### stock_institute_hold（机构持股）

| 字段 | 类型 | 说明 |
|------|------|------|
| report_period | string | 报告期（如 `2024Q1`） |
| period_end_date | date32 | 报告期截止日期 |
| code | string | 股票代码 |
| code_name | string | 股票名称 |
| institution_count | int64 | 机构数 |
| institution_count_change | int64 | 机构数变化 |
| holding_ratio | float64 | 持股比例 |
| holding_ratio_change | float64 | 持股比例增幅 |
| float_holding_ratio | float64 | 占流通股比例 |
| float_holding_ratio_change | float64 | 占流通股比例增幅 |

### stock_value_em（估值指标）

| 字段 | 类型 | 说明 |
|------|------|------|
| date | date32 | 日期 |
| code | string | 股票代码 |
| close | float64 | 当日收盘价 |
| pct_chg | float64 | 当日涨跌幅 |
| total_market_cap | float64 | 总市值 |
| float_market_cap | float64 | 流通市值 |
| total_shares | float64 | 总股本 |
| float_shares | float64 | 流通股本 |
| pe_ttm | float64 | PE(TTM) |
| pe_static | float64 | PE(静) |
| pb | float64 | 市净率 |
| peg | float64 | PEG值 |
| pcf | float64 | 市现率 |
| ps | float64 | 市销率 |

## DuckDB 视图

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

## 查询示例

### 基础查询

使用 DuckDB 查询数据：

```powershell
python -c "import duckdb; con=duckdb.connect('data/duckdb/quant.duckdb'); print(con.execute(\"select date, code, close from v_daily_k_qfq where code='sh.600000' order by date desc limit 5\").fetchdf())"
```

或在 Python 中：

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')
df = con.execute("""
    select date, code, close, volume
    from v_daily_k_qfq
    where code = 'sh.600000'
    order by date desc
    limit 10
""").fetchdf()
print(df)
```

### 多股票查询

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

df = con.execute("""
    select code, date, close, volume, pctChg
    from v_daily_k_qfq
    where code in ('sh.600000', 'sh.600001', 'sz.000001')
    and date = '2024-04-26'
    order by code
""").fetchdf()
print(df)
```

### 时间范围查询

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

df = con.execute("""
    select date, code, close, volume
    from v_daily_k_qfq
    where code = 'sh.600000'
    and date between '2024-01-01' and '2024-04-30'
    order by date
""").fetchdf()
print(df)
```

### 机构持股查询

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

df = con.execute("""
    select report_period, code, code_name, institution_count, holding_ratio
    from v_stock_institute_hold
    where code = 'sh.600000'
    order by report_period desc
    limit 10
""").fetchdf()
print(df)
```

### 估值指标查询

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

df = con.execute("""
    select date, code, close, pe_ttm, pb, ps, total_market_cap
    from v_stock_value_em
    where code = 'sh.600000'
    order by date desc
    limit 10
""").fetchdf()
print(df)
```

### 股票基础信息查询

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

df = con.execute("""
    select code, code_name, ipoDate, status
    from v_stock_basic
    where status = '1'
    order by code
    limit 10
""").fetchdf()
print(df)
```

### 交易日历查询

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

df = con.execute("""
    select calendar_date
    from v_calendar
    where is_trading_day = '1'
    and calendar_date <= current_date
    order by calendar_date desc
    limit 30
""").fetchdf()
print(df)
```

## 配置文件

### config/settings.yaml

主配置文件，包含数据集定义、API 参数、管道参数等：

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
      none: "3"    # 不复权
      qfq: "2"     # 前复权
      hfq: "1"     # 后复权
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
  lookback_days: 10      # update-daily 默认回看交易日数量
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

### config/universe.yaml

股票池配置（已弃用，保留向后兼容）：

```yaml
universe:
  default:
    - sh.600000
```

### 数据源 provider

- `api.provider` 是默认数据源名称，当前内置值为 `baostock`
- `qdc update-daily`、`qdc repair` 都支持用 `--provider` 覆盖默认数据源
- Provider 通过 `src/api/market_data.py` 中的 `MarketDataProvider` 接口注册和创建，Baostock 适配器位于 `src/api/baostock_provider.py`
- AkShare 数据源不经过 `MarketDataProvider` 接口，而是通过独立的 `AkShareClient` 和 `update_akshare` 管道处理

### AkShare 配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `api.akshare.max_retries` | 3 | AkShare API 最大重试次数 |
| `api.akshare.jitter_seconds` | [2, 8] | 请求间随机抖动延迟范围（秒） |
| `api.akshare.lookback_quarters` | 8 | partial 模式下 stock_institute_hold 回看季度数 |
| `api.akshare.endpoints.<name>.failure_threshold` | 5 | 端点连续失败熔断阈值 |
| `api.akshare.endpoints.<name>.cooldown_minutes` | 30 | 端点熔断冷却时间（分钟） |
| `datasets.stock_institute_hold.start_quarter` | 2005Q1 | full 模式下 stock_institute_hold 起始季度 |
| `datasets.stock_value_em.active_only` | true | partial 模式下仅拉取活跃股票的估值数据 |

## 定时任务

Windows 任务计划程序可调用批处理脚本：

```powershell
scripts\run_update_daily.bat
```

该脚本会切换到项目根目录，自动激活 `venv`（如存在），并执行 `python -m src.cli update-daily`。

**推荐配置**：

- 触发时间：交易日 18:10（确保交易所数据已更新）
- 重复间隔：每天
- 持续时间：无限期

**AkShare 定时更新**：

可创建类似的定时任务执行 AkShare 数据更新：

```powershell
python -m src.cli update-akshare
```

建议在交易日 18:30 后执行，与 Baostock 更新错开。

## 代码池与续传机制

### 代码池来源

**update-daily**：

- `qdc update-daily --mode full` 默认使用最新本地 `stock_basic` 快照中的全部非空 `code`
- `qdc update-daily` 默认使用最新 `stock_basic` 快照中 `type=1` 且 `status=1` 的上市股票代码
- 显式传入 `--code` 时优先使用指定代码
- `--universe` 仅保留向后兼容，默认不再读取 `config/universe.yaml`

**update-akshare**：

- `stock_institute_hold`：按季度范围生成任务，不涉及代码池
- `stock_value_em`：默认使用最新 `stock_basic` 中 active 代码（`datasets.stock_value_em.active_only` 为 true 时）；`--include-inactive` 或 `--mode full` 时使用全部代码
- 显式传入 `--code` 时优先使用指定代码

### 续传机制

- `--resume` 默认开启，会跳过已有成功 checkpoint 且输出文件仍存在的任务
- 使用 `--no-resume` 或 `--force` 可以强制重新拉取选中的任务
- checkpoint 记录存储在 `data/duckdb/quant.duckdb` 的 `pipeline_checkpoints` 表中，日期字段使用解析后的交易日
- 旧版 `data/metadata/pipeline_checkpoints.parquet` 若存在，会在元数据层初始化时迁移到 DuckDB
- 续传检查会优先加载 `PipelineCheckpointLookup` 内存索引，减少批量任务中的重复元数据读取
- `pipeline.metadata_flush_size` 控制运行记录、状态和 checkpoint 的批量写入阈值
- `pipeline.background_workers` 默认 3；`pipeline.background_max_pending` 未配置时默认为 `background_workers * 4`
- AkShare 管道使用独立的 `update_akshare` pipeline 名称，与 `update_daily` 的 checkpoint 互不干扰

## 数据验证

写入 Parquet 文件前，系统会自动执行以下验证：

### daily_k 数据验证

- Schema 列类型匹配
- `code` + `date` 唯一性
- `date` 单调递增
- OHLC 逻辑检查（`high >= low`，`open/close` 在 `[low, high]` 范围内，异常时记录警告但不阻止写入）
- `volume`、`amount` 非负检查（发现空值、非数值或负值时记录警告但不阻止写入）

### stock_institute_hold 数据验证

- Schema 列类型匹配
- `report_period` + `code` 唯一性
- `report_period`、`period_end_date`、`code` 不允许空值

### stock_value_em 数据验证

- Schema 列类型匹配
- `code` + `date` 唯一性
- `date` 单调递增
- `total_market_cap`、`float_market_cap`、`total_shares`、`float_shares` 非负检查

### validate_non_negative 函数说明

验证指定列的值是否为非负数值，用于检查 `volume` 和 `amount` 列。

**参数**：
- `df`: pandas.DataFrame - 待验证的数据框，需包含 `code`、`date` 和目标列
- `column`: str - 待验证的列名

**返回值**：
- None

**验证行为**：
- 空值检查：若列中存在空值，记录警告并返回，不阻止数据写入
- 非数值检查：若列中存在无法转换为数值的值，记录警告并返回，不阻止数据写入
- 负值检查：若列中存在负值，记录警告但不阻止数据写入

**使用示例**：

```python
from src.quality.validators import validate_non_negative
import pandas as pd

df = pd.DataFrame({
    'code': ['sh.600000', 'sh.600001'],
    'date': ['2024-01-01', '2024-01-02'],
    'volume': [1000000, -500000],
    'amount': [50000000, None]
})

validate_non_negative(df, 'volume')
validate_non_negative(df, 'amount')
```

**注意事项**：
- 该函数仅记录警告，不会抛出异常或阻止数据写入
- 警告日志包含问题数据的样本（最多5条），便于定位问题
- 建议在数据写入后检查日志，确认数据质量

### adjust_factor 数据验证

- Schema 列类型匹配
- `code` + `dividOperateDate` 唯一性
- `dividOperateDate` 单调递增

### stock_basic 数据验证

- Schema 列类型匹配
- `code` 唯一性

### calendar 数据验证

- Schema 列类型匹配
- `calendar_date` 唯一性

## 日志

日志文件位于 `logs/qdc.log`，配置：
- 轮转大小：10 MB
- 保留天数：30 天
- 编码：UTF-8

**日志级别**：
- INFO：正常操作日志
- WARNING：数据质量警告（不阻止写入）
- ERROR：操作失败日志

**查看日志**：

```powershell
# 查看最新日志
Get-Content logs\qdc.log -Tail 50

# 搜索错误日志
Select-String -Path logs\qdc.log -Pattern "ERROR"

# 搜索警告日志
Select-String -Path logs\qdc.log -Pattern "WARNING"
```

## 测试

```powershell
pytest
```

测试覆盖：
- Parquet 存储层（`test_parquet_store.py`）
- DuckDB 视图构建（`test_duckdb_store.py`）
- 数据集目录（`test_dataset_catalog.py`）
- Schema 定义（`test_schema.py`）
- 数据验证器（`test_validators.py`）
- Baostock 客户端（`test_baostock_client.py`）
- AkShare 客户端（`test_akshare_client.py`）
- AkShare 契约测试（`test_akshare_contract.py`）
- 数据源 provider 抽象（`test_market_data_provider.py`）
- CLI provider 参数传递（`test_cli_provider.py`）
- Pipeline 续传机制（`test_update_daily_full_resume.py`、`test_update_daily_partial_resume.py`、`test_update_daily_refetch.py`）
- 代码池解析（`test_code_pool.py`）
- 交易日历处理（`test_trading_dates.py`）
- 数据修复工具（`test_repair_tool.py`）
- 本地复权计算（`test_adjustments.py`）
- AkShare 管道（`test_update_akshare.py`）

## 性能基准测试

项目包含完整的性能基准测试套件，位于 `benchmarks/` 目录。

### 运行基准测试

```powershell
# 运行所有基准测试
python benchmarks/run_all_benchmarks.py --output-dir benchmark_results

# 跳过长时间测试
python benchmarks/run_all_benchmarks.py --skip-long-tests
```

### 基准测试内容

- **API 调用性能**: Baostock API 延迟和吞吐量
- **I/O 操作性能**: Parquet 文件读写性能
- **数据处理性能**: 数据清洗、复权计算性能
- **并发性能**: 线程池性能和锁竞争
- **端到端性能**: 完整管道性能

详细说明见 [benchmarks/BENCHMARK_README.md](benchmarks/BENCHMARK_README.md)，最新测试报告见 [benchmark_results/performance_test_report.md](benchmark_results/performance_test_report.md)。

## 常见问题

### 1. 数据拉取失败怎么办？

**原因**：
- Baostock 服务不可用
- 网络连接问题
- 股票代码已退市

**解决方案**：
- 检查网络连接
- 查看日志文件 `logs/qdc.log` 了解详细错误信息
- 使用 `--force` 参数重新拉取
- 使用 `qdc repair` 修复特定股票的数据

### 2. 如何处理停牌股票？

系统会自动处理停牌股票：
- 停牌期间数据为空，不影响其他数据
- 复牌后会自动更新数据
- 回看窗口机制会检测数据变化并自动修复

### 3. 如何处理复权因子变化？

系统会自动检测复权因子变化：
- 每次更新会保存 `1990-01-01` 到目标交易日的本地复权因子
- 前复权/后复权日线不再直接调用 BaoStock 复权 K 线 API
- 复权因子变化时自动从未复权历史数据重算前/后复权数据

### 4. 如何只更新特定股票？

使用 `--code` 参数：

```powershell
qdc update-daily --code sh.600000 --code sh.600001
qdc update-akshare --dataset stock_value_em --code sh.600000
```

### 5. 如何查看数据更新状态？

查询元数据表：

```python
import duckdb

con = duckdb.connect('data/duckdb/quant.duckdb')

# 查看更新状态
df = con.execute("""
    select dataset, code, last_success_date, row_count, status
    from update_status
    order by last_success_date desc
    limit 10
""").fetchdf()
print(df)
```

### 6. 如何处理数据质量问题？

系统会自动记录数据质量警告：
- 检查日志文件 `logs/qdc.log`
- 搜索 `WARNING` 关键字
- 根据警告信息决定是否需要修复数据

### 7. 如何优化性能？

**建议**：
- 使用 SSD 存储数据
- 定期清理旧日志文件
- 使用 `--no-build-views` 跳过视图构建（批量操作时）
- 合理设置 `lookback_days` 参数

### 8. 如何备份和恢复数据？

**备份**：

```powershell
# 备份整个数据目录
Copy-Item -Path data -Destination data_backup -Recurse
```

**恢复**：

```powershell
# 恢复数据目录
Copy-Item -Path data_backup -Destination data -Recurse -Force
```

### 9. AkShare 熔断后如何处理？

系统会自动处理 AkShare 端点熔断：
- 连续失败超过阈值（默认 5 次）后，端点进入熔断状态
- 熔断期间该端点的请求会被直接拒绝
- 冷却时间（默认 30 分钟）后自动恢复
- 可通过 `config/settings.yaml` 调整 `failure_threshold` 和 `cooldown_minutes`
- 使用 `--force` 参数可绕过 checkpoint 但不会绕过熔断

### 10. AkShare 数据为空怎么办？

- `stock_institute_hold`：某些季度可能确实没有数据（如新上市股票），系统会抛出 `AkShareEmptyDataError` 并记录失败
- `stock_value_em`：非活跃股票可能返回空数据，系统仅对活跃股票（`task.active=True`）抛出异常
- 使用 `--force` 重新拉取可验证数据是否恢复

## 性能优化建议

### 1. 存储优化

- 使用 SSD 存储 Parquet 文件
- 定期清理旧的 `stock_basic` 快照
- 保留必要的交易日历数据

### 2. 查询优化

- 使用 DuckDB 的分区裁剪功能
- 避免全表扫描，使用 WHERE 条件过滤
- 合理使用索引（DuckDB 自动优化）

### 3. 更新优化

- 批量操作时使用 `--no-build-views`
- 合理设置 `lookback_days`（默认 10 天）
- 使用 `--resume` 避免重复拉取
- 利用跨 pipeline checkpoint 识别避免重复执行
- 内存索引优化减少元数据读取次数
- 批量元数据写入减少 DuckDB 元数据事务

### 4. AkShare 优化

- 使用 `--max-tasks` 限制单次运行任务数，避免长时间运行
- 使用 `--resume` 避免重复拉取已成功的季度/股票
- 调整 `jitter_seconds` 控制请求频率
- 调整 `failure_threshold` 和 `cooldown_minutes` 适应网络环境

### 5. 内存优化

- DuckDB 会自动管理内存
- 大查询时考虑分批处理
- 定期重启 Python 进程释放内存

## 当前限制

- 当前唯一内置数据源为 `baostock`，接口需要联网，可用性受交易所日历和服务状态影响
- AkShare 数据源通过独立管道接入，不经过 `MarketDataProvider` 接口
- Provider 抽象已经就位，但其他数据源适配器尚未实现
- 当前版本聚焦低频日线、股票基础信息、交易日历、机构持股和估值指标
- `update-daily` 默认将非交易日候选日回退到最近交易日，避免周末/节假日产生自然日 checkpoint
- 暂未实现 raw API 缓存（Baostock 管道）；AkShare 管道已实现原始响应归档
- AkShare 数据源依赖外部网站，可用性和数据格式可能随网站变更而变化

## 项目结构

```
quant_data_center/
├── config/
│   ├── settings.yaml      # 主配置
│   └── universe.yaml      # 股票池配置（已弃用）
├── data/                  # 数据目录
│   ├── parquet/          # Parquet 数据文件
│   ├── duckdb/           # DuckDB 数据库、查询视图和运行元数据表
│   ├── raw/              # 原始数据缓存
│   │   └── akshare/     # AkShare 原始响应归档和 manifest
│   ├── metadata/         # 旧版 Parquet 元数据迁移兼容目录
│   └── logs/             # 日志文件
├── benchmark_results/     # 性能基准报告输出
├── benchmarks/            # 性能基准测试套件
├── references/            # BaoStock 等数据源参考文档
├── scripts/
│   └── run_update_daily.bat  # 定时任务脚本
├── src/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── market_data.py      # provider 接口与注册表
│   │   ├── baostock_provider.py # Baostock provider 适配器
│   │   ├── baostock_client.py  # Baostock API 封装
│   │   └── akshare_client.py   # AkShare API 封装（熔断、重试、字段映射）
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── adjustments.py      # 本地复权计算
│   │   ├── akshare_tasks.py    # AkShare 任务规划
│   │   ├── common.py          # 共享工具函数
│   │   ├── repair_tool.py     # 数据修复管道
│   │   ├── services.py        # provider 拉取与元数据批处理服务
│   │   ├── update_akshare.py  # AkShare 爬虫数据更新管道
│   │   ├── update_daily.py    # 日常更新与历史初始化管道入口
│   │   ├── update_daily_calendar.py  # 更新日历窗口与写入
│   │   ├── update_daily_frames.py    # 日线 DataFrame 处理辅助
│   │   ├── update_daily_metadata.py  # 更新元数据写入辅助
│   │   ├── update_daily_targets.py   # 更新目标与断点预过滤
│   │   ├── update_daily_types.py     # 日更管道共享类型
│   │   ├── update_daily_worker.py    # 日更后台写入 worker
│   │   └── write_queue.py     # 写入队列工具（保留模块）
│   ├── quality/
│   │   ├── __init__.py
│   │   └── validators.py      # 数据验证器
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── dataset_catalog.py # 数据集目录
│   │   ├── duckdb_store.py    # DuckDB 存储层
│   │   ├── metadata_store.py  # DuckDB 元数据存储层
│   │   ├── parquet_store.py   # Parquet 存储层
│   │   └── schema.py          # PyArrow Schema 定义
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config_mgr.py      # 配置管理
│   │   ├── logging.py         # 日志配置
│   │   ├── paths.py           # 路径管理
│   │   └── performance.py     # 性能监控工具
│   ├── __init__.py
│   └── cli.py                 # CLI 入口
├── tests/                  # 测试文件
├── pyproject.toml          # 项目配置
├── README.md               # 项目说明
└── ARCHITECTURE.md         # 架构设计文档
```

## 更新日志

### v0.1.0

- 初始版本发布
- 支持三种复权模式的日线数据
- 实现断点续传机制
- 实现数据验证和原子写入
- 实现回看自愈机制
- 实现 DuckDB 零拷贝查询
- 统一 `update_daily` 入口，支持 partial 和 full 模式
- 实现跨 pipeline checkpoint 识别
- 实现内存索引优化（PipelineCheckpointLookup）
- 实现批量元数据写入（PipelineMetadataBatch + DuckDBMetadataStore）
- 实现 Windows 文件锁定自动重试机制
- 实现后台处理池（_DailyUpdateBackgroundWorker + ThreadPoolExecutor）
- 集成 AkShare 数据源，支持机构持股（stock_institute_hold）和估值指标（stock_value_em）
- 实现 AkShare 端点级别熔断与重试机制
- 实现 AkShare 原始响应归档和 JSONL manifest
- 新增 `qdc update-akshare` CLI 命令
- 新增 stock_institute_hold 和 stock_value_em 数据集的 Schema、Validator 和 DuckDB 视图

## 许可证

MIT License

## 贡献指南

欢迎提交 Issue 和 Pull Request。在提交代码前，请确保：

1. 运行所有测试：`pytest`
2. 遵循代码风格规范
3. 添加必要的文档和注释
4. 更新相关文档

## 联系方式

如有问题或建议，请提交 Issue。
