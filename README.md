# Quant Data Center

Windows 单机低频量化数据中心。项目使用 Python 拉取 A 股低频数据，Parquet 做本地存储，DuckDB 做查询视图。
命名规范见 `docs/NAMING.md`，领域词汇见 `CONTEXT.md`。

当前有三条数据源路径：

- Baostock：`update-baostock-daily` 默认更新未复权日线，显式 `--dataset all` 时保存日线、复权因子、股票基础信息和交易日历；`update-baostock-valuation-percentile` 基于本地未复权日线派生估值历史分位。股票代码保持 `sh.600000` 格式。
- AkShare：独立爬虫流程，保存估值、股本结构和 A 股行情相关数据，所有 AkShare 数据集的 `code` 和分区键统一为 6 位数字，例如 `600000`。
- Qlib：`sync-qlib` 从远程 Qlib 二进制数据包同步日线特征和交易日历，代码使用 `qlib_symbol` 格式（如 `sh600000`）。

## 安装

```powershell
py -m venv venv
.\venv\Scripts\activate
python -m pip install -U pip
python -m pip install -e .
```

开发环境安装：

```powershell
python -m pip install -e ".[dev]"
```

运行完整质量门禁：

```powershell
pwsh scripts/check_quality.ps1
```

单独运行测试：

```powershell
pytest -q
```

运行覆盖率门禁：

```powershell
python -m pytest --cov=src --cov-report=term-missing
```

提交前检查：

```powershell
pre-commit install
pre-commit run --all-files
```

## 常用命令

### Baostock 日线

```powershell
qdc update-baostock-daily
qdc update-baostock-daily --mode full --dataset all --start 1990-01-01 --end 2024-04-26 --code sh.600000
qdc update-baostock-daily --dataset baostock_cn_stock_adjustment_factor --code sh.600000
qdc update-baostock-daily --dataset baostock_cn_stock_daily_bar_qfq --code sh.600000
qdc update-baostock-daily --dataset baostock_cn_stock_daily_bar_hfq --code sh.600000
qdc repair-baostock-daily --code sh.600000 --start 2024-01-01 --end 2024-04-26 --dataset baostock_cn_stock_daily_bar_hfq
```

`update-baostock-daily` 默认只把 `baostock_cn_stock_daily_bar_unadjusted` 作为显式目标；交易日历和股票基础信息仍会按解析交易窗口和默认股票池的需要自动补齐。前复权、后复权和复权因子可以通过对应 `--dataset` 独立更新；计算前/后复权时，如果当前交易日的复权因子 checkpoint 缺失，会先更新复权因子。该命令默认在 18:00 前使用前一自然日作为候选日，18:00 后使用当天，并通过本地交易日历回退到最近交易日。

### Baostock 估值分位

```powershell
qdc update-baostock-valuation-percentile
qdc update-baostock-valuation-percentile --mode full --code sh.600000
qdc update-baostock-valuation-percentile --start 2024-01-03 --force
```

`baostock_cn_stock_valuation_percentile` 只读取本地 `baostock_cn_stock_daily_bar_unadjusted`，输出按 `code` 分区，DuckDB 视图为 `v_baostock_cn_stock_valuation_percentile`。默认只补源数据新增日期；`--start --force` 会保留指定日期前的输出并重算指定日期至最新源日期。

### AkShare 估值

```powershell
qdc akshare update --target valuation
qdc akshare update --target valuation --code 600000 --code 000001
qdc akshare update --target valuation --include-inactive
qdc akshare update --target capital_structure --mode full
qdc akshare update --target capital_structure --code 600000 --force
```

`akshare_cn_stock_valuation_eastmoney` 默认股票池来自本地 AkShare 清单，不再使用 Baostock `baostock_cn_stock_basic`：

- partial：使用最新 `akshare_cn_stock_spot_quote_eastmoney` 清单并排除本地 delisted。
- full 或 `--include-inactive`：使用最新 `akshare_cn_stock_spot_quote_eastmoney` 与 delisted 的并集。
- 显式 `--code` 只接受 6 位代码，例如 `600000`。
- `akshare update --target capital_structure` 通过 `stock_zh_a_gbjg_em` 获取东财股本结构历史，保存为
  `akshare_cn_stock_capital_structure_em`，可用 `full` 初始化全量股票池或用 `--code --force` 手动刷新单票。
- `baostock_cn_stock_adjustment_factor` 发生变化时，会把对应 6 位 AkShare 股票代码写入
  `data/metadata/akshare_capital_structure_pending.parquet`，并在该 Baostock 更新结束后自动刷新对应
  `capital_structure`。该联动只作为公司行动变化的保守触发信号，不替代手动 full/force 修复。

### AkShare A 股行情

```powershell
qdc akshare update --target delist
qdc akshare update --target delist --end 2026-05-03
qdc akshare update --target delist --market 终止上市公司
qdc akshare update --target spot_quote --end 2026-05-03
qdc akshare update --target daily_bar --mode full --adjustment all --start 1990-01-01
qdc akshare update --target daily_bar --mode incremental --adjustment unadjusted --start 2026-05-01 --end 2026-05-03
```

重要规则：

- `akshare update --target spot_quote` 只允许在北京时间 `[18:00, 次日 08:00)` 访问 `stock_zh_a_spot_em` / `stock_zh_a_spot` 并写入 daily bar。窗口外会在请求接口前中断，避免交易时间实时数据污染日线。
- `akshare_cn_stock_spot_quote_eastmoney` 成功时，先保存 spot 快照，再转换为 `akshare_cn_stock_daily_bar_unadjusted` 的 `spot_quote_close` 行；写 daily bar 时会排除本地 delisted。
- `akshare_cn_stock_spot_quote_eastmoney` 失败时，使用 `stock_zh_a_spot` fallback，保存 Sina 快照，并转换为同一 daily-bar 格式写入 `akshare_cn_stock_daily_bar_unadjusted`。
- `akshare update --target daily_bar` 通过源接口 `stock_zh_a_hist` 获取历史日线；full 默认使用 AkShare 全量池，incremental 默认使用 active 池并排除 delisted。
- `akshare update --target delist` 默认同时更新上交所 `akshare_cn_stock_delist_sh` 和深交所 `akshare_cn_stock_delist_sz`；`--end` 可固定快照日期。

### Qlib 同步

```powershell
qdc sync-qlib
qdc sync-qlib --allow-weekday
qdc sync-qlib --force-download
qdc sync-qlib --max-runtime-seconds 7200 --workers 4
```

`sync-qlib` 从远程 Qlib 二进制数据包下载并同步到本地 Parquet。默认仅在周五至周日执行（`is_qlib_update_day`），`--allow-weekday` 可跳过该限制。同步状态记录在 `data/metadata/qlib_sync_state.parquet`。

Qlib 数据集使用 `qlib_symbol`（如 `sh600000`）作为代码和分区键：

- `qlib_cn_calendar_day`：Qlib 交易日历。
- `qlib_cn_instrument_membership`：Qlib instrument 宇宙成员。
- `qlib_cn_stock_features_day`：Qlib 日线特征数据，包含 `open`、`high`、`low`、`close`、`volume`、`amount`、`factor`、`change`、`vwap`、`adjclose` 字段。

## 数据目录

```text
data/
├── parquet/
│   ├── baostock_cn_stock_daily_bar_unadjusted/code=sh.600000/data.parquet
│   ├── baostock_cn_stock_daily_bar_qfq/code=sh.600000/data.parquet
│   ├── baostock_cn_stock_daily_bar_hfq/code=sh.600000/data.parquet
│   ├── baostock_cn_stock_valuation_percentile/code=sh.600000/data.parquet
│   ├── baostock_cn_stock_adjustment_factor/code=sh.600000/data.parquet
│   ├── baostock_cn_stock_basic/data.parquet
│   ├── baostock_cn_trading_calendar/data.parquet
│   ├── akshare_cn_stock_valuation_eastmoney/code=600000/data.parquet
│   ├── akshare_cn_stock_capital_structure_em/code=600000/data.parquet
│   ├── akshare_cn_stock_delist_sh/snapshot_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_delist_sz/snapshot_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_spot_quote_eastmoney/trade_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_spot_quote_sina/trade_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_daily_bar_unadjusted/code=600000/data.parquet
│   ├── akshare_cn_stock_daily_bar_qfq/code=600000/data.parquet
│   ├── akshare_cn_stock_daily_bar_hfq/code=600000/data.parquet
│   ├── akshare_cn_stock_institution_holding/report_period=YYYYQn/data.parquet
│   ├── qlib_cn_calendar_day/data.parquet
│   ├── qlib_cn_instrument_membership/data.parquet
│   └── qlib_cn_stock_features_day/qlib_symbol=sh600000/data.parquet
├── metadata/
│   ├── pipeline_runs.parquet
│   ├── dataset_update_status.parquet
│   ├── pipeline_checkpoints.parquet
│   ├── akshare_capital_structure_pending.parquet
│   └── qlib_sync_state.parquet
├── registry/
│   ├── catalog.json
│   ├── inventory.parquet
│   └── events.jsonl
└── duckdb/
    └── quant.duckdb
```

## 查询示例

```python
import duckdb

con = duckdb.connect("data/duckdb/quant.duckdb")

daily = con.execute("""
    select date, code, close
    from v_baostock_cn_stock_daily_bar_qfq
    where code = 'sh.600000'
    order by date desc
    limit 5
""").fetchdf()

value = con.execute("""
    select date, code, pe_ttm, pb
    from v_akshare_cn_stock_valuation_eastmoney
    where code = '600000'
    order by date desc
    limit 5
""").fetchdf()

percentile = con.execute("""
    select date, code, pe_ttm, pe_ttm_percentile_1y, pe_ttm_percentile_all_history
    from v_baostock_cn_stock_valuation_percentile
    where code = 'sh.600000'
    order by date desc
    limit 5
""").fetchdf()
```

手动重建视图：

```powershell
qdc build-duckdb-views
```

## 应用层接入

其他项目应优先通过只读 Registry 网关发现和查询数据，而不是直接打开 `data/duckdb/quant.duckdb`：

```powershell
qdc serve-registry --host 127.0.0.1 --port 8765
```

常用入口：

- `GET /v1/status`：查看 Registry 数据集数量、库存更新时间和最新事件编号。
- `GET /v1/datasets`：查看当前有哪些 Dataset、schema、视图名和最新状态。
- `GET /v1/datasets/{dataset_id}`：查看单个 Dataset 的 schema、分区列、生命周期和库存状态。
- `GET /v1/datasets/{dataset_id}/partitions`：列出该 Dataset 当前 Parquet 分区。
- `GET /v1/events?since_event_id=0`：轮询最新写入事件。
- `GET /v1/events/stream`：用 SSE 秒级订阅写入事件。
- `POST /v1/query`：用结构化 JSON 查询 Parquet 数据，默认返回 1000 行，最多 50000 行。

详细协议见 `docs/DATA_REGISTRY.md`。

## 配置

主要配置在 `config/settings.yaml`。

常用项：

- `project.timezone`：默认 `Asia/Shanghai`，用于 18:00/08:00 窗口判断。
- `api.akshare.max_retries`：AkShare 最大重试次数。
- `api.akshare.workers`：AkShare 并发任务数。
- `api.akshare.jitter_seconds`：请求前随机延迟。
- `api.akshare.endpoints.<name>.failure_threshold` / `cooldown_minutes`：端点熔断配置。
- `datasets.akshare_cn_stock_valuation_eastmoney.active_only`：partial 模式是否只取 active AkShare 股票池。
- `datasets.akshare_cn_stock_capital_structure_em.active_only`：partial 模式是否只取 active AkShare 股票池。
- `datasets.akshare_cn_stock_spot_quote.update_daily_bar_from_spot`：spot 快照是否同步写入 `akshare_cn_stock_daily_bar_unadjusted`。
- `pipeline.qlib_sync_workers`：Qlib 同步并发数，默认回退到 `pipeline.background_workers`。

## 命名迁移

旧数据目录或旧 DuckDB 元数据可使用命名 v1 迁移脚本升级：

```powershell
python scripts/migrate_naming_v1.py --dry-run
python scripts/migrate_naming_v1.py --apply
```

迁移报告写入 `logs/naming_migration_<timestamp>.json`。

## 运维

查看日志：

```powershell
Get-Content logs\qdc.log -Tail 50
Select-String -Path logs\qdc.log -Pattern "ERROR|WARNING"
```

查询任务状态：

```python
import duckdb

con = duckdb.connect("data/duckdb/quant.duckdb")
df = con.execute("""
    select dataset, code, status, last_success_date, row_count
    from dataset_update_status
    order by updated_at desc
    limit 20
""").fetchdf()
print(df)
```

## 项目结构

```text
src/
├── api/                    # Baostock provider 与 AkShare client
│   └── akshare/            # AkShare adapters、normalization、symbols
├── pipeline/               # update-baostock-daily、Baostock 派生数据、AkShare 管道、Qlib 同步、repair-baostock-daily
│   └── akshare/            # AkShare 执行内核与 Dataset Modules
│       └── modules/        # valuation、capital_structure、daily_bar、spot_quote、delist
├── quality/                # 写入前验证
├── storage/                # Schema、Dataset catalog、ParquetStore、DuckDBStore、DataRegistry
├── tools/                  # 辅助工具（API 审计等）
├── utils/                  # 配置、路径、日志、运行上下文
├── cli.py                  # Click CLI 入口
└── registry_server.py      # 只读 Registry HTTP 网关
tests/                      # 单元与管道测试
config/                     # settings.yaml
scripts/                    # Windows 定时任务脚本
```
