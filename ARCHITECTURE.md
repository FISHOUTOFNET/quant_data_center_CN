# Architecture

本项目是 Windows 单机 A 股低频数据底座，核心目标是可重复更新、可追踪、可修复、易查询。

## 数据源边界

系统有三条互相独立的数据路径。

### Baostock 路径

`qdc update-baostock-daily` 和 `qdc repair-baostock-daily` 通过 `MarketDataProvider` 接口访问 Baostock。`update-baostock-daily` 默认只更新未复权日线，显式 `--dataset all` 时覆盖完整 Baostock 日线/复权目标。该路径负责：

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
- `akshare_cn_stock_capital_structure_em`：东方财富股本结构历史，按 6 位代码分区。
- `akshare_cn_stock_delist_sh`：上交所退市/暂停上市辅助清单。
- `akshare_cn_stock_delist_sz`：深交所终止上市辅助清单。
- `akshare_cn_stock_spot_quote_eastmoney`：东方财富 A 股 spot 快照。
- `akshare_cn_stock_spot_quote_sina`：Sina spot fallback 快照。
- `akshare_cn_stock_daily_bar_unadjusted/qfq/hfq`：AkShare 历史日线。
- `akshare_cn_stock_report_disclosure`：巨潮资讯预约披露时间，按 `report_period` 分区，唯一键为 `(report_period, code)`。
- `akshare_cn_stock_yysj_em`：东方财富预约披露时间，按 `report_period` 分区，唯一键为 `(report_period, symbol, code)`。
- `akshare_cn_stock_financial_report_sina`：新浪三大财报长表，按 6 位 `code` 分区，唯一键为 `(code, report_type, report_date, item_name)`。
- `akshare_cn_stock_institution_holding`：历史保留机构持仓数据集，生命周期为 `legacy_unmanaged`，不再新增采集 pipeline。

### Qlib 路径

`qdc sync-qlib` 从远程 Qlib 二进制数据包下载并同步到本地 Parquet。Qlib 数据集使用 `qlib_symbol`（如 `sh600000`）作为代码和分区键，与 Baostock 的 `sh.600000` 和 AkShare 的 `600000` 不同。

当前 Qlib 数据集：

- `qlib_cn_calendar_day`：Qlib 交易日历。
- `qlib_cn_instrument_membership`：Qlib instrument 宇宙成员，按 universe 分组。
- `qlib_cn_stock_features_day`：Qlib 日线特征数据，按 `qlib_symbol` 分区，包含 `open`、`high`、`low`、`close`、`volume`、`amount`、`factor`、`change`、`vwap`、`adjclose` 字段。

Qlib 同步默认在周五至周日执行（`is_qlib_update_day`），支持 `--allow-weekday` 跳过该限制。同步状态记录在 `data/metadata/qlib_sync_state.parquet`。

`source_symbol` 字段保留源接口原始代码形态，只用于追溯，不作为项目标准代码。

## Derived Local Layer / 本地统一数据层

The derived local layer adds a canonical and query-first surface without replacing source datasets.

- `cn_security_master` is the canonical mapping layer. It standardizes `security_id`, exchange, six-digit code, listing status, board, and Baostock/AkShare/Qlib source-code mappings.
- `cn_stock_daily_bar` is the materialized unified daily-bar table, partitioned by `security_id`.
- `cn_stock_valuation` is the materialized unified valuation table, partitioned by `security_id`.

Source-layer datasets (`baostock_*`, `akshare_*`, `qlib_*`) keep their original schemas and remain the audit, repair, and traceability source of truth. Downstream research and backtesting should prefer the `cn_*` tables when a unified local view is needed.

`cn_stock_daily_bar` and `cn_stock_valuation` use partition-level materializers: each builder iterates `cn_security_master`, reads only the matching source partitions for one security, merges that security's data, and writes `data/parquet/<dataset>/security_id=<SECURITY_ID>/data.parquet`. The builders do not concatenate full-market daily-bar or valuation sources into one in-memory DataFrame.

Manual full rebuild:

```powershell
qdc build-derived --target all --mode full
```

Daily incremental rebuild:

```powershell
qdc build-derived --target all --mode incremental --no-build-duckdb-views
```

`--mode incremental` is the daily default. It refreshes `cn_security_master` in full because master is small, then uses local metadata and Parquet partition mtimes to map changed source partitions to affected `security_id` values. `cn_stock_daily_bar` and `cn_stock_valuation` then stage and atomically promote only those partitions. If the changed set cannot be determined reliably, the affected target safely falls back to full and logs a warning.

Single-partition repair:

```powershell
qdc build-derived --target daily_bar --security-id SH.600000 --mode incremental --no-build-duckdb-views
qdc build-derived --target valuation --security-id SH.600000 --mode incremental --no-build-duckdb-views
```

## AkShare 代码与股票池

AkShare 显式 `--code`、client 入参、manifest 任务键和 Parquet 分区统一使用 6 位代码：

```text
600000
```

源接口返回的 `source_symbol` 可能带市场前缀或数值后缀，写入前会规范化为 6 位 `code`，原始形态保留在 `source_symbol`。

AkShare 默认股票池来自本地 AkShare 数据：

- active 池：最新 `akshare_cn_stock_spot_quote_eastmoney` 的代码，排除最新 `akshare_cn_stock_delist_sh` 和 `akshare_cn_stock_delist_sz`。
- full 池：active spot 代码与 delisted 代码的并集。

使用方式：

- `akshare_cn_stock_valuation_eastmoney` partial 默认 active 池。
- `akshare_cn_stock_valuation_eastmoney` full 或 `--include-inactive` 使用 full 池。
- `akshare update --target daily_bar` incremental 默认 active 池。
- `akshare update --target daily_bar` full 默认 full 池。
- 显式 `--code` 会跳过股票池解析，只做 6 位规范化。

如果本地没有 AkShare 清单且未显式传 `--code`，pipeline 会提示先运行 `akshare update --target spot_quote` 或 `akshare update --target delist`。

## Report Disclosure

`akshare update --target report_disclosure` 使用 AkShare `stock_report_disclosure(market, period)` 一次性获取全市场预约披露时间。该目标不依赖 AkShare 股票池，也不循环股票代码。
`akshare update --target yysj_em` 使用 AkShare `stock_yysj_em(symbol, date)` 获取东方财富预约披露时间，独立保存，不替代 `report_disclosure`。

运行规则：

- 默认 `market=沪深京`；CLI 可用 `--market` 覆盖。
- 财报期格式为 `<年份><一季|半年报|三季|年报>`，对应期末 `03-31`、`06-30`、`09-30`、`12-31`。
- partial 默认取当前日期之前最近四个已完成财报期。
- full 从 `datasets.akshare_cn_stock_report_disclosure.full_start_year` 生成到当前最近已完成财报期，默认起始年份为 1990。
- `yysj_em` 默认同时更新 `symbol=沪深A股` 与 `symbol=京市A股`，CLI 可用 `--market` 覆盖为单个 AkShare symbol。
- `yysj_em` full 从 `datasets.akshare_cn_stock_yysj_em.full_start_year` 生成到当前最近已完成财报期，默认起始年份为 2008。
- 显式 `--period` 可重复传入，并且只允许用于 `report_disclosure` 与 `yysj_em` target。
- 巨潮/AkShare 对部分历史早期无数据期会抛出空数据列匹配异常；adapter 将该固定空历史期错误规范化为空 DataFrame，pipeline 写入空 schema 分区并记录 success。

周五至周日的 `scripts/run_update_daily.bat` 会在周末窗口内运行 `report_disclosure` 与 `yysj_em` partial 更新，并在最终统一 build DuckDB views 前完成。

`akshare update --target financial_report` 使用 AkShare `stock_financial_report_sina(stock, symbol)` 获取新浪三大财报。该接口按股票一次返回全部历史，pipeline 因此按股票分区 replace 写入。full 使用 AkShare full universe，包含 delisted；incremental 不刷新披露日历，只读取本地 `akshare_cn_stock_report_disclosure`、`akshare_cn_stock_yysj_em` 和 `akshare_financial_report_pending.parquet`。

财报增量触发规则：

- 对每条披露记录计算触发日期，优先级为 `actual_disclosure_date`、最新非空变更日期、`first_scheduled_date`。
- 同一 `(code, report_period)` 在两个来源之间合并时，先取更高优先级；同优先级取较晚日期。
- 北京时间达到 `datasets.akshare_cn_stock_financial_report_sina.close_after_time` 后，按下一自然日披露处理。
- 如果已触发但 Sina 财报尚未包含目标期，任务保留在 pending 文件中，后续每日继续重试。
- `financial_report` incremental 每日运行，作为最终 build DuckDB views 前最后一个数据更新步骤。

## Spot 与 Daily Bar

`akshare update --target spot_quote` 只允许在 `project.timezone` 下 `[18:00, 次日 08:00)` 执行接口访问和 daily-bar 写入。窗口外中断，防止交易时间实时数据污染日线。

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

akshare update --target daily_bar incremental/full
    -> write akshare_cn_stock_daily_bar_<adjustment>
       source_endpoint = stock_zh_a_hist
       quality_status = daily_bar_confirmed
```

Daily-bar upsert 使用 `(code, date, adjustment)` 去重，后写入的 `daily_bar_confirmed` 行会覆盖同日 `spot_quote_close` 行。

## 存储模型

Parquet 是数据主存储。DuckDB 被拆成两个互不依赖的本地库：查询视图库和 pipeline 元数据库。

```text
data/
├── parquet/
│   ├── baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet
│   ├── baostock_cn_stock_adjustment_factor/code=sh.600000/data.parquet
│   ├── akshare_cn_stock_valuation_eastmoney/code=600000/data.parquet
│   ├── akshare_cn_stock_capital_structure_em/code=600000/data.parquet
│   ├── akshare_cn_stock_report_disclosure/report_period=YYYY年报/data.parquet
│   ├── akshare_cn_stock_yysj_em/report_period=YYYY年报/data.parquet
│   ├── akshare_cn_stock_financial_report_sina/code=600000/data.parquet
│   ├── akshare_cn_stock_delist_sh/snapshot_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_delist_sz/snapshot_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_spot_quote_eastmoney/trade_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_spot_quote_sina/trade_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_daily_bar_unadjusted/code=600000/data.parquet
│   ├── qlib_cn_calendar_day/data.parquet
│   ├── qlib_cn_instrument_membership/data.parquet
│   └── qlib_cn_stock_features_day/qlib_symbol=sh600000/data.parquet
├── metadata/
│   ├── pipeline_runs.parquet
│   ├── dataset_update_status.parquet
│   ├── pipeline_checkpoints.parquet
│   ├── qdc_metadata.duckdb
│   ├── akshare_capital_structure_pending.parquet
│   └── qlib_sync_state.parquet
├── registry/
│   ├── catalog.json
│   ├── inventory.parquet
│   └── events.jsonl
└── duckdb/quant.duckdb
```

- `data/duckdb/quant.duckdb` is the query-view database owned by `DuckDBStore.build_views()`.
- `data/metadata/qdc_metadata.duckdb` is the pipeline metadata database owned by `DuckDBMetadataStore`.
- Old metadata tables left in `data/duckdb/quant.duckdb` are not modified by default. Run `qdc migrate-metadata-duckdb` to copy `pipeline_runs`, `dataset_update_status`, and `pipeline_checkpoints` into the new metadata database. The migration is idempotent and skips missing old tables.

写入策略：

- `ParquetStore` 只暴露 catalog 驱动的 Dataset Interface：`dataset_path`、`read_dataset`、`read_latest_dataset`、`write_dataset` 等。
- `DATASET_CATALOG` 是 dataset 存储规则的唯一来源，声明 schema、validator、partition、sort、unique、default write mode、固定列值和旧分区清理规则。
- PyArrow schema 强制列顺序和类型，写入前统一补齐分区值、固定列值并运行 validator。
- `replace` 做单目标原子替换；`merge` 读取目标后按 catalog `unique_columns` keep last；`upsert` 支持单分区或按 `partition_column` 自动拆多分区写入。
- 使用临时文件加 `os.replace` 做原子替换。
- Windows 文件锁导致的短暂读写失败会自动重试。

## 元数据与续传

运行状态存储在 `data/metadata/qdc_metadata.duckdb`：

- `pipeline_runs`：每个任务的运行记录。
- `dataset_update_status`：每个数据集/代码的最近状态。
- `pipeline_checkpoints`：续传判断依据。

`--resume` 默认开启，只有 checkpoint 成功且目标文件仍存在时才跳过。`--force` 会忽略 checkpoint 重新执行。

AkShare pipeline 只保存规范化后的 Parquet 数据和统一运行元数据，不再归档原始响应文件。

## Daily Workflow 配置

每日任务由 `config/daily_workflow.yaml` 生成 `DailyStep`，而不是在 `src/tools/run_update_daily.py` 中硬编码。配置项包括：

- `id` / `name`：step 标识和日志名称。
- `command`：命令数组，支持 `{python}`、`{qdc}`、`{today}`、`{hist_start}`。
- `depends_on`：依赖 step id；未启用的 step 会从依赖中自动过滤。
- `optional` / `timeout_seconds` / `enabled`：保持原有 optional、超时和开关语义。
- `when`：支持 `weekday`、`weekend`、`friday_to_sunday` 和具体英文星期名。

默认 flow 保持现有行为：工作日运行核心源更新、`financial_report` incremental、`build-derived --mode incremental` 和 `build-duckdb-views`；周五至周日额外运行 delist、复权、AkShare valuation full、report disclosure、yysj、AkShare daily bar incremental 与 qlib sync。

## Data Registry

`DataRegistry` 是可选的本地元数据只读模型，位于 `data/registry/`，用于诊断 catalog、inventory 和写入事件。pipeline 和 repair 命令写入 Parquet 后，会按 dirty dataset best-effort 刷新 catalog 与 inventory；刷新失败不会阻断主数据写入、derived build 或 DuckDB views 构建。

- `catalog.json` 来自 `DATASET_CATALOG`，记录 schema、view、source、endpoint、code format、partition column 和 lifecycle。
- `inventory.parquet` 扫描物理 Parquet 文件，记录分区数、文件数、行数、日期边界、最新分区和最近 pipeline 状态。
- `events.jsonl` 是按 `event_id` 递增的写入事件日志。

## DuckDB 视图

`DuckDBStore.build_views()` 根据 catalog 创建视图。空数据集会创建空视图，避免查询方因为缺表失败。

常用视图：

- `v_baostock_cn_stock_daily_bar_unadjusted`
- `v_baostock_cn_stock_daily_bar_qfq`
- `v_baostock_cn_stock_daily_bar_hfq`
- `v_baostock_cn_stock_adjustment_factor`
- `v_baostock_cn_stock_basic`
- `v_baostock_cn_trading_calendar`
- `v_baostock_cn_stock_valuation_percentile`
- `v_akshare_cn_stock_valuation_eastmoney`
- `v_akshare_cn_stock_capital_structure_em`
- `v_akshare_cn_stock_delist_sh`
- `v_akshare_cn_stock_delist_sz`
- `v_akshare_cn_stock_spot_quote_eastmoney`
- `v_akshare_cn_stock_spot_quote_sina`
- `v_akshare_cn_stock_daily_bar_unadjusted`
- `v_akshare_cn_stock_daily_bar_qfq`
- `v_akshare_cn_stock_daily_bar_hfq`
- `v_akshare_cn_stock_report_disclosure`
- `v_akshare_cn_stock_yysj_em`
- `v_akshare_cn_stock_financial_report_sina`
- `v_akshare_cn_stock_institution_holding`
- `v_qlib_cn_calendar_day`
- `v_qlib_cn_instrument_membership`
- `v_qlib_cn_stock_features_day`
- `v_cn_security_master`
- `v_cn_stock_daily_bar`
- `v_cn_stock_valuation`

## 扩展点

新增 Baostock 类 provider：

1. 实现 `MarketDataProvider` 协议。
2. 注册 provider factory。
3. 通过 `api.provider` 或 CLI `--provider` 选择。

新增 AkShare Dataset：

1. 先按 AkShare 文档确认端点上游来源，并在对应目录下创建 Adapter 和 Module：`src/sources/akshare/eastmoney/`、`src/sources/akshare/sina/`、`src/sources/akshare/cninfo/` 或 `src/sources/akshare/exchange/`。
2. Module 类实现 `AkShareDatasetModule` 协议（`plan`、`prefilter`、`fetch`、`record_result`、`record_skip`、`progress_row`、`concurrency`）。
3. 在 `src/sources/akshare/pipeline/execution.py` 的 `_modules_for_target` 注册新 target。
4. 在 CLI `akshare update --target` 的 Choice 列表中添加新 target。

AkShare Module 架构的详细决策见 `docs/adr/0002-akshare-update-module-interface.md`。

新增 Dataset：

1. 在 `schema.py` 定义 schema。
2. 在 `validators.py` 添加验证。
3. 在 `dataset_catalog.py` 注册 dataset 和 view，并声明 `partition_column`、`sort_columns`、`unique_columns`、`default_write_mode`；需要固定业务列时声明 `fixed_column_values`，需要清理历史分区形态时声明 `legacy_partition_prefixes`。
4. 在 pipeline 中只调用统一存储接口：`write_dataset(dataset_id, df, partition)`、`read_dataset(dataset_id, partition)`、`read_latest_dataset(dataset_id)`、`dataset_path(dataset_id, partition)`。
5. 如果是 AkShare 数据集，在 `src/sources/akshare/client.py` 添加 endpoint normalizer，并在对应上游来源目录下创建 Module；只有按股票代码循环的 target 才使用 `src/sources/akshare/pipeline/universe.py` 获取默认股票池。
6. 如果是 Qlib 数据集，在 `src/sources/qlib/sync.py` 中添加同步逻辑。
7. 在 pipeline 中处理任务和 metadata，写入后刷新 registry；排序、去重、merge/upsert 规则不要散落在 pipeline 中。

## 验证

常用验证命令：

```powershell
pytest -q
pytest -m slow -q
python -m src.cli akshare update --help
```



