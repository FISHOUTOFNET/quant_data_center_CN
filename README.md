# Quant Data Center

Windows 单机低频量化数据中心。项目使用 Python 拉取 A 股低频数据，Parquet 做本地存储，DuckDB 做查询视图。
命名规范见 `docs/NAMING.md`，领域词汇见 `CONTEXT.md`。

当前有三条数据源路径：

- Baostock：`update-baostock-daily` 默认更新未复权日线，显式 `--dataset all` 时保存日线、复权因子、股票基础信息和交易日历；`update-baostock-valuation-percentile` 基于本地未复权日线派生估值历史分位。股票代码保持 `sh.600000` 格式。
- AkShare：独立爬虫流程，保存估值、股本结构、预约披露时间和 A 股行情相关数据。按股票取数的数据集使用 6 位 `code`，预约披露时间按接口参数和财报期一次性取数，不循环股票代码。
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

`pytest -q` 默认排除 `slow` 标记的长耗时集成/性能测试。如需单独运行这些测试：

```powershell
pytest -m slow -q
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

### AkShare 估值、股本结构、预约披露

```powershell
qdc akshare update --target valuation
qdc akshare update --target valuation --code 600000 --code 000001
qdc akshare update --target valuation --include-inactive
qdc akshare update --target capital_structure --mode full
qdc akshare update --target capital_structure --code 600000 --force
qdc akshare update --target report_disclosure
qdc akshare update --target report_disclosure --mode full
qdc akshare update --target report_disclosure --period 2025年报 --period 2026一季
qdc akshare update --target yysj_em
qdc akshare update --target yysj_em --market 沪深A股 --period 2025年报
qdc akshare update --target financial_report --mode full
qdc akshare update --target financial_report --mode incremental
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
- `akshare update --target report_disclosure` 调用 AkShare `stock_report_disclosure`，默认 `market=沪深京`，按财报期一次性获取全市场预约披露时间，不使用股票池也不循环 `code`。
- `report_disclosure` partial 默认更新最近四个已完成财报期；`--mode full` 从
  `datasets.akshare_cn_stock_report_disclosure.full_start_year`（默认 1990）生成历史财报期。历史早期无数据期会写入空 schema 分区并记录成功，避免反复重试。
- `akshare update --target yysj_em` 调用 AkShare `stock_yysj_em`，默认同时更新 `symbol=沪深A股` 和 `symbol=京市A股`，覆盖沪深京并独立保存为 `akshare_cn_stock_yysj_em`。
- `yysj_em` partial 同样默认更新最近四个已完成财报期；`--mode full` 从
  `datasets.akshare_cn_stock_yysj_em.full_start_year`（默认 2008）生成历史财报期。
- `--period` 只适用于 `report_disclosure` 和 `yysj_em`，可重复传入，格式如 `2025年报`、`2026一季`。`--market` 在 `yysj_em` 下表示 `stock_yysj_em` 的 `symbol` 参数。
- `akshare update --target financial_report` 调用 AkShare `stock_financial_report_sina` 获取新浪三大财报，保存为
  `akshare_cn_stock_financial_report_sina` 长表并按 `code` 分区。
- `financial_report` full 使用 active 与 delisted 的并集；incremental 只读取本地 `report_disclosure` / `yysj_em` 披露日历和
  `data/metadata/akshare_financial_report_pending.parquet`，不主动刷新披露日历。触发日期优先级为实际披露、最新变更、首次预约；18:00 后按次日披露处理。

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
- `stock_zh_a_spot_em` 是 spot quote 的首选来源之一，不是单点成功条件；成功时先保存 `akshare_cn_stock_spot_quote_eastmoney` 快照，再转换为 `akshare_cn_stock_daily_bar_unadjusted` 的 `spot_quote_close` 行；写 daily bar 时会排除本地 delisted。
- `stock_zh_a_spot_em` 失败时，使用 `stock_zh_a_spot` fallback。fallback 成功时保存 `akshare_cn_stock_spot_quote_sina` 快照，并转换为同一 daily-bar 格式写入 `akshare_cn_stock_daily_bar_unadjusted`；此时 CLI 返回成功，Eastmoney 子源记录为 `skipped_fallback`。
- 只有 `stock_zh_a_spot_em` 和 `stock_zh_a_spot` 都失败时，`akshare update --target spot_quote` 才返回失败。`scripts/run_update_daily.bat` 将 `spot_quote` 作为非阻塞预更新步骤；即使该目标全部失败，也会记录 warning 并继续后续每日更新。
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
│   ├── akshare_cn_stock_report_disclosure/report_period=2025年报/data.parquet
│   ├── akshare_cn_stock_yysj_em/report_period=2025年报/data.parquet
│   ├── akshare_cn_stock_financial_report_sina/code=600000/data.parquet
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

disclosure = con.execute("""
    select report_period, code, name, first_scheduled_date, actual_disclosure_date
    from v_akshare_cn_stock_report_disclosure
    where report_period = '2025年报'
    order by code
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
- `datasets.akshare_cn_stock_financial_report_sina.close_after_time`：财报增量在该时间后按下一自然日披露处理，默认 `18:00`。
- `datasets.akshare_cn_stock_capital_structure_em.active_only`：partial 模式是否只取 active AkShare 股票池。
- `datasets.akshare_cn_stock_report_disclosure.full_start_year`：预约披露时间 full 模式的起始年份。
- `datasets.akshare_cn_stock_yysj_em.full_start_year`：东方财富预约披露时间 full 模式的起始年份。
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

`scripts\run_update_daily.bat` starts by cleaning log files older than 30 days. Preview cleanup manually:

```powershell
python -m src.tools.log_cleanup --dry-run
```

`scripts\run_update_daily.bat` delegates scheduling to `python -m src.cli run-update-daily`.
The orchestrator is dependency-aware: a failed non-optional step is recorded and
independent later steps continue, while steps that explicitly depend on the
failed step are recorded as `blocked`. The final process exit code remains
non-zero when any required step failed or was blocked, so Windows Task Scheduler
and external monitors can still alert on partial failures.

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
├── sources/                # 按数据来源组织的采集程序
│   ├── baostock/           # Baostock client、provider、daily update、repair、派生数据
│   ├── akshare/            # AkShare client、runtime、执行内核与按上游来源拆分的 modules/adapters
│   │   ├── eastmoney/      # 东方财富/东财接口：valuation、capital_structure、daily_bar、spot_quote_em、yysj_em
│   │   ├── sina/           # 新浪接口：spot_quote fallback、financial_report
│   │   ├── cninfo/         # 巨潮资讯接口：report_disclosure
│   │   ├── exchange/       # 交易所接口：上交所/深交所 delist
│   │   ├── core/           # AkShare runtime、models、errors、normalization、symbols
│   │   └── pipeline/       # AkShare 跨子源执行内核、universe、pending、spot_quote orchestration
│   ├── qlib/               # Qlib 同步
│   └── common/             # 跨来源 provider 协议等共享接口
├── pipeline/               # 通用 pipeline checkpoint/lifecycle/registry 基础设施
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
