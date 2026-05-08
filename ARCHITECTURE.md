# Architecture

本项目是 Windows 单机 A 股低频数据底座，核心目标是可重复更新、可追踪、可修复、易查询。

## 数据源边界

系统有两条互相独立的数据路径。

### Baostock 路径

`qdc update-baostock-daily` 和 `qdc repair-baostock-daily` 通过 `MarketDataProvider` 接口访问 Baostock。该路径负责：

- `baostock_cn_stock_daily_bar_unadjusted`：Baostock 未复权日线。
- `baostock_cn_stock_daily_bar_qfq` / `baostock_cn_stock_daily_bar_hfq`：由未复权日线和本地复权因子计算生成。
- `baostock_cn_stock_adjustment_factor`：复权因子。
- `baostock_cn_stock_basic`：Baostock 股票基础信息，代码保持 `sh.600000` / `sz.000001`。
- `baostock_cn_trading_calendar`：交易日历。

这条路径仍使用市场前缀代码，因为 Baostock API 和既有日线数据都依赖该格式。

### AkShare 路径

AkShare 不进入 `MarketDataProvider`，而是使用独立的 `AkShareClient` 与 AkShare pipeline。AkShare 数据集全部使用 6 位股票代码作为 `code` 和分区键，例如 `600000`。

当前 AkShare 数据集：

- `akshare_cn_stock_valuation_eastmoney`：东方财富估值指标，按 6 位代码分区。
- `akshare_cn_stock_delist_sh`：上交所退市/暂停上市辅助清单。
- `akshare_cn_stock_spot_quote_eastmoney`：东方财富 A 股 spot 快照。
- `akshare_cn_stock_spot_quote_sina`：Sina spot fallback 快照。
- `akshare_cn_stock_daily_bar_unadjusted/qfq/hfq`：AkShare 历史日线。

`source_symbol` 字段保留源接口原始代码形态，只用于追溯，不作为项目标准代码。

## AkShare 代码与股票池

AkShare 显式 `--code`、client 入参、manifest 任务键和 Parquet 分区统一使用 6 位代码：

```text
600000
```

源接口返回的 `source_symbol` 可能带市场前缀或数值后缀，写入前会规范化为 6 位 `code`，原始形态保留在 `source_symbol`。

AkShare 默认股票池来自本地 AkShare 数据：

- active 池：最新 `akshare_cn_stock_spot_quote_eastmoney` 的代码，排除最新 `akshare_cn_stock_delist_sh`。
- full 池：active spot 代码与 delisted 代码的并集。

使用方式：

- `akshare_cn_stock_valuation_eastmoney` partial 默认 active 池。
- `akshare_cn_stock_valuation_eastmoney` full 或 `--include-inactive` 使用 full 池。
- `update-akshare-daily-bar` incremental 默认 active 池。
- `update-akshare-daily-bar` full 默认 full 池。
- 显式 `--code` 会跳过股票池解析，只做 6 位规范化。

如果本地没有 AkShare 清单且未显式传 `--code`，pipeline 会提示先运行 `update-akshare-spot-quote` 或 `update-akshare-delist`。

## Spot 与 Daily Bar

`update-akshare-spot-quote` 只允许在 `project.timezone` 下 `[18:00, 次日 08:00)` 执行接口访问和 daily-bar 写入。窗口外中断，防止交易时间实时数据污染日线。

数据流：

```text
akshare_cn_stock_spot_quote_eastmoney success
    -> write akshare_cn_stock_spot_quote_eastmoney snapshot
    -> map active rows to akshare_cn_stock_daily_bar_unadjusted
       source_endpoint = stock_zh_a_spot_em
       quality_status = spot_quote_close

akshare_cn_stock_spot_quote_eastmoney failed
    -> write failed metadata
    -> fetch stock_zh_a_spot
    -> write akshare_cn_stock_spot_quote_sina snapshot
    -> map rows to akshare_cn_stock_daily_bar_unadjusted
       source_endpoint = stock_zh_a_spot
       quality_status = spot_quote_close

update-akshare-daily-bar incremental/full
    -> write akshare_cn_stock_daily_bar_<adjustment>
       source_endpoint = stock_zh_a_hist
       quality_status = daily_bar_confirmed
```

Daily-bar upsert 使用 `(code, date, adjustment)` 去重，后写入的 `daily_bar_confirmed` 行会覆盖同日 `spot_quote_close` 行。

## 存储模型

Parquet 是数据主存储，DuckDB 只构建查询视图和运行元数据表。

```text
data/parquet/
├── baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet
├── baostock_cn_stock_adjustment_factor/code=sh.600000/data.parquet
├── akshare_cn_stock_valuation_eastmoney/code=600000/data.parquet
├── akshare_cn_stock_delist_sh/snapshot_date=YYYY-MM-DD/data.parquet
├── akshare_cn_stock_spot_quote_eastmoney/trade_date=YYYY-MM-DD/data.parquet
└── akshare_cn_stock_daily_bar_unadjusted/code=600000/data.parquet
```

写入策略：

- PyArrow schema 强制列顺序和类型。
- 写入前运行 validator。
- 使用临时文件加 `os.replace` 做原子替换。
- Windows 文件锁导致的短暂读写失败会自动重试。

## 元数据与续传

运行状态存储在 `data/duckdb/quant.duckdb`：

- `pipeline_runs`：每个任务的运行记录。
- `dataset_update_status`：每个数据集/代码的最近状态。
- `pipeline_checkpoints`：续传判断依据。

`--resume` 默认开启，只有 checkpoint 成功且目标文件仍存在时才跳过。`--force` 会忽略 checkpoint 重新执行。

AkShare 原始响应归档到：

```text
data/raw/akshare/<endpoint>/YYYYMMDD/*.parquet
data/raw/akshare/manifest/fetch_runs.jsonl
```

Manifest 记录 endpoint、参数、版本、行数、数据哈希、原始文件路径、状态和错误信息。

## DuckDB 视图

`DuckDBStore.build_views()` 根据 catalog 创建视图。空数据集会创建空视图，避免查询方因为缺表失败。

常用视图：

- `v_baostock_cn_stock_daily_bar_unadjusted`
- `v_baostock_cn_stock_daily_bar_qfq`
- `v_baostock_cn_stock_daily_bar_hfq`
- `v_baostock_cn_stock_adjustment_factor`
- `v_baostock_cn_stock_basic`
- `v_baostock_cn_trading_calendar`
- `v_akshare_cn_stock_valuation_eastmoney`
- `v_akshare_cn_stock_spot_quote_eastmoney`
- `v_akshare_cn_stock_spot_quote_sina`
- `v_akshare_cn_stock_daily_bar_unadjusted`
- `v_akshare_cn_stock_daily_bar_qfq`
- `v_akshare_cn_stock_daily_bar_hfq`

## 扩展点

新增 Baostock 类 provider：

1. 实现 `MarketDataProvider` 协议。
2. 注册 provider factory。
3. 通过 `api.provider` 或 CLI `--provider` 选择。

新增 AkShare 数据集：

1. 在 `schema.py` 定义 schema。
2. 在 `validators.py` 添加验证。
3. 在 `dataset_catalog.py` 注册 dataset 和 view。
4. 在 `AkShareClient` 添加 endpoint normalizer。
5. 使用 `akshare_universe.py` 获取默认股票池。
6. 在 pipeline 中处理任务、原始归档、metadata 和写入。

## 验证

常用验证命令：

```powershell
pytest -q
python -m src.cli update-akshare-valuation --help
python -m src.cli update-akshare-spot-quote --help
python -m src.cli update-akshare-daily-bar --help
```



