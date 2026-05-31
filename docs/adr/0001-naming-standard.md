# ADR 0001: Naming Standard

## Status

Accepted

## Date

2026-05-07

## Context

项目早期同时使用了数据源接口名、业务名和临时缩写，例如 `daily_k_*`、`stock_zh_a_hist_*`、`stock_value_em`、`update_status`。这些名称泄漏到 Parquet 目录、DuckDB 视图、schema 字段、CLI 和 pipeline 函数后，扩展新数据源或新增数据集时需要反复猜测“这是源接口、业务概念还是存储对象”。

本项目需要同时满足两个约束：

- 存储层名称必须可追溯数据源，避免同一业务对象的跨源冲突。
- 代码和文档必须使用业务语义，避免源接口名成为领域模型。

## Decision

采用混合分层命名：

- Dataset ID 使用 `<source>_<market>_<asset>_<subject>[_variant]`。
- DuckDB 视图使用 `v_<dataset_id>`。
- Schema 字段统一 `snake_case`。
- Python 和 CLI 使用业务语义：`daily_bar`、`valuation`、`spot_quote`、`adjustment`。
- `source_endpoint` 保留外部接口原名，例如 `stock_zh_a_hist`，仅用于熔断配置、追溯和数据来源标记。
- 股票代码源内自洽：Baostock 保持 `sh.600000`，AkShare 保持 `600000`。
- `stock_institute_hold` 迁移为 `akshare_cn_stock_institution_holding`，生命周期标记为 `legacy_unmanaged`。
- 旧 dataset id、旧 DuckDB 视图和旧 CLI 命令不保留 alias。

## Consequences

正向影响：

- 存储、查询、pipeline 和文档使用同一套词汇，新增数据集时有明确模板。
- 业务语义和源接口名分离，AkShare 或 Baostock 的接口名变化不会污染领域模型。
- 破坏式迁移能尽早暴露下游依赖，避免长期维护双命名。

代价：

- 已有下游查询、定时任务和手工脚本需要改名。
- 旧 Parquet 目录和 DuckDB 元数据必须通过 `scripts/migrate_naming_v1.py` 迁移。
- 静态扫描需要允许 `source_endpoint` 和迁移映射保留源接口名。

## Follow-ups

- 命名变更必须同步更新 `docs/NAMING.md`。
- 新增领域概念必须同步更新 `CONTEXT.md`。
- 如需再次破坏式命名迁移，应新增 ADR 和版本化迁移脚本。
