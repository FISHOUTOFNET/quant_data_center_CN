# Naming Standard

本规范是项目命名的唯一入口。新增数据集、字段、视图、CLI 和 pipeline 时必须先对齐这里的规则。

## Dataset ID

格式：

```text
<source>_<market>_<asset>_<subject>[_variant]
```

规则：

- `source` 使用数据源名：`baostock`、`akshare`、`qlib`。
- `market` 使用市场范围：当前为 `cn`。
- `asset` 使用资产类型：当前为 `stock`，交易日历可省略 asset，使用 `baostock_cn_trading_calendar`。
- `subject` 使用业务对象：`daily_bar`、`spot_quote`、`valuation`、`valuation_percentile`、`adjustment_factor`、`capital_structure`、`report_disclosure`、`basic`、`delist`、`calendar_day`、`instrument_membership`、`features_day`。
- `variant` 用于复权口径、交易所或来源：`unadjusted`、`qfq`、`hfq`、`eastmoney`、`sina`、`sh`、`sz`。

示例：

- `baostock_cn_stock_daily_bar_qfq`
- `baostock_cn_stock_valuation_percentile`
- `akshare_cn_stock_daily_bar_unadjusted`
- `akshare_cn_stock_capital_structure_em`
- `akshare_cn_stock_report_disclosure`
- `akshare_cn_stock_spot_quote_eastmoney`
- `akshare_cn_stock_delist_sz`
- `akshare_cn_stock_institution_holding` (`legacy_unmanaged`)
- `qlib_cn_calendar_day`
- `qlib_cn_instrument_membership`
- `qlib_cn_stock_features_day`

## Source Endpoint

`source_endpoint` 保留外部接口原名，用于追溯、熔断和数据来源标记：

- AkShare 估值：`stock_value_em`
- AkShare 股本结构：`stock_zh_a_gbjg_em`
- AkShare 预约披露时间：`stock_report_disclosure`
- AkShare spot：`stock_zh_a_spot_em`、`stock_zh_a_spot`
- AkShare 日线：`stock_zh_a_hist`
- Baostock 日线：`query_history_k_data_plus`
- Baostock 估值分位为本地派生数据集，`endpoint` 为 `None`
- Qlib 数据集为本地二进制同步，`endpoint` 为 `qlib_bin`

不要把 `source_endpoint` 当作 dataset id，也不要把项目 dataset id 写入 `source_endpoint`。

## DuckDB

- 查询视图命名为 `v_<dataset_id>`。
- 元数据表命名为：
- `pipeline_runs`
- `dataset_update_status`
- `pipeline_checkpoints`
- `schema_migrations`

禁止新增 `update_runs`、`update_status`、`metadata_migrations`。

## Schema 字段

字段统一使用 `snake_case`。

通用字段：

- `pctChg` / `pct_chg` -> `pct_change`
- `preclose` -> `prev_close`
- `adjustflag` -> `adjust_flag`
- `tradestatus` -> `trade_status`
- `turn` -> `turnover_rate`
- `latest_price` -> `last_price`
- `change_amount` -> `price_change`
- `adjust` -> `adjustment`

Baostock 字段：

- `peTTM` -> `pe_ttm`
- `pbMRQ` -> `pb_mrq`
- `psTTM` -> `ps_ttm`
- `pcfNcfTTM` -> `pcf_ncf_ttm`
- `isST` -> `is_st`
- `ipoDate` -> `ipo_date`
- `outDate` -> `delist_date`
- `code_name` -> `name`
- `type` -> `security_type`
- `status` -> `listing_status`

复权因子字段：

- `dividOperateDate` -> `dividend_operate_date`
- `foreAdjustFactor` -> `forward_adjust_factor`
- `backAdjustFactor` -> `backward_adjust_factor`
- `adjustFactor` -> `adjustment_factor`

## Adjustment

项目内复权枚举为：

- `unadjusted`
- `qfq`
- `hfq`

外部接口需要空字符串或 `none` 时，只能在 adapter 层转换。CLI、schema、Parquet、DuckDB 和测试都使用 `unadjusted`。

## Code Format

- 代码形态按数据源自洽，不跨源强行统一：

- Baostock：`sh.600000`、`sz.000001`
- AkShare：`600000`、`000001`
- Qlib：`sh600000`、`sz000001`（`qlib_symbol`，交易所前缀加 6 位代码，无点号）

`DatasetDefinition.code_format` 必须显式记录代码形态，当前枚举为 `baostock_prefixed`、`six_digit`、`qlib_symbol`、`none`。

## CLI

当前公开命令：

- `update-baostock-daily`
- `update-baostock-valuation-percentile`
- `repair-baostock-daily`
- `akshare update`
- `sync-qlib`
- `build-duckdb-views`
- `serve-registry`

旧命令不保留 alias。调用旧命令应由 Click 抛出不可用错误。

## Python

命名使用业务语义：

- `daily_k` -> `daily_bar`
- `hist` -> `daily_bar`
- `stock_value` -> `valuation`
- `spot` 在业务对象中写作 `spot_quote`
- `adjust` 参数写作 `adjustment`

保留源接口原名只允许出现在 adapter 调用、`source_endpoint` 和命名迁移映射中。

## Migration

命名 v1 迁移脚本：

```powershell
python scripts/migrate_naming_v1.py --dry-run
python scripts/migrate_naming_v1.py --apply
```

脚本会生成：

```text
logs/naming_migration_<timestamp>.json
```

默认 dry-run；只有 `--apply` 会改写 Parquet、DuckDB 元数据和视图。
