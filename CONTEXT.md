# Context

本项目是 Windows 单机 A 股低频量化数据中心。核心语言如下，后续代码、文档、测试和迁移脚本应使用这些词汇。

## 领域词汇

- 数据源 Source：外部数据提供方，例如 `baostock`、`akshare`。
- 源接口 Endpoint：外部库真实接口名，例如 `query_history_k_data_plus`、`stock_zh_a_hist`。源接口名只用于追溯和熔断配置，不作为项目 dataset id。
- 数据集 Dataset：项目管理的数据单元，使用稳定的 `dataset_id` 命名，例如 `baostock_cn_stock_daily_bar_qfq`。
- 逻辑名称 Logical Name：跨数据源的业务语义，例如 `cn_stock_daily_bar`、`cn_stock_spot_quote`。
- 日线 Daily Bar：一只股票一个交易日的 OHLCV 行情。项目内不再使用 `daily_k` 或 `hist` 表示业务日线。
- 现货快照 Spot Quote：交易日收盘后采集的当日行情快照，可转写为 AkShare 未复权 daily bar 的 `spot_quote_close` 行。
- 复权 Adjustment：行情复权口径，枚举为 `unadjusted`、`qfq`、`hfq`。
- 复权因子 Adjustment Factor：Baostock 提供的复权因子数据集 `baostock_cn_stock_adjustment_factor`。
- 股票代码 Code：数据源内自洽。Baostock 使用 `sh.600000`，AkShare 使用 `600000`。
- 源代码 Source Symbol：源接口返回的原始代码形态，仅用于追溯。
- 运行元数据 Pipeline Metadata：`pipeline_runs`、`dataset_update_status`、`pipeline_checkpoints`。
- 视图 View：DuckDB 查询入口，命名为 `v_<dataset_id>`。
- Legacy Unmanaged：历史保留数据集，只保留 schema、视图和迁移，不新增采集 pipeline。

## 当前数据集

- `baostock_cn_stock_daily_bar_unadjusted`
- `baostock_cn_stock_daily_bar_qfq`
- `baostock_cn_stock_daily_bar_hfq`
- `baostock_cn_stock_adjustment_factor`
- `baostock_cn_stock_basic`
- `baostock_cn_trading_calendar`
- `akshare_cn_stock_valuation_eastmoney`
- `akshare_cn_stock_delist_sh`
- `akshare_cn_stock_delist_sz`
- `akshare_cn_stock_spot_quote_eastmoney`
- `akshare_cn_stock_spot_quote_sina`
- `akshare_cn_stock_daily_bar_unadjusted`
- `akshare_cn_stock_daily_bar_qfq`
- `akshare_cn_stock_daily_bar_hfq`
- `akshare_cn_stock_institution_holding` (`legacy_unmanaged`)
