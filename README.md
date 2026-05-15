# Quant Data Center

Windows 单机低频量化数据中心。项目使用 Python 拉取 A 股低频数据，Parquet 做本地存储，DuckDB 做查询视图。
命名规范见 `docs/NAMING.md`，领域词汇见 `CONTEXT.md`。

当前有两条数据源路径：

- Baostock：`update-baostock-daily` 主流程，保存日线、复权因子、股票基础信息和交易日历，股票代码保持 `sh.600000` 格式。
- AkShare：独立爬虫流程，保存估值和 A 股行情相关数据，所有 AkShare 数据集的 `code` 和分区键统一为 6 位数字，例如 `600000`。

## 安装

```powershell
py -m venv venv
.\venv\Scripts\activate
python -m pip install -U pip
python -m pip install -e .
```

运行测试：

```powershell
pytest -q
```

## 常用命令

### Baostock 日线

```powershell
qdc update-baostock-daily
qdc update-baostock-daily --mode full --dataset all --start 1990-01-01 --end 2024-04-26 --code sh.600000
qdc update-baostock-valuation-percentile --mode full --code sh.600000
qdc repair-baostock-daily --code sh.600000 --start 2024-01-01 --end 2024-04-26 --dataset baostock_cn_stock_daily_bar_hfq
```

`update-baostock-daily` 默认在 18:00 前使用前一自然日作为候选日，18:00 后使用当天，并通过本地交易日历回退到最近交易日。

### AkShare 估值

```powershell
qdc update-akshare-valuation
qdc update-akshare-valuation --dataset akshare_cn_stock_valuation_eastmoney --code 600000 --code 000001
qdc update-akshare-valuation --dataset akshare_cn_stock_valuation_eastmoney --include-inactive
```

`akshare_cn_stock_valuation_eastmoney` 默认股票池来自本地 AkShare 清单，不再使用 Baostock `baostock_cn_stock_basic`：

- partial：使用最新 `akshare_cn_stock_spot_quote_eastmoney` 清单并排除本地 delisted。
- full 或 `--include-inactive`：使用最新 `akshare_cn_stock_spot_quote_eastmoney` 与 delisted 的并集。
- 显式 `--code` 只接受 6 位代码，例如 `600000`。

### AkShare A 股行情

```powershell
qdc update-akshare-delist --market 全部
qdc update-akshare-spot-quote --end 2026-05-03
qdc update-akshare-daily-bar --mode full --adjustment all --start 1990-01-01
qdc update-akshare-daily-bar --mode incremental --adjustment unadjusted --start 2026-05-01 --end 2026-05-03
```

重要规则：

- `update-akshare-spot-quote` 只允许在北京时间 `[18:00, 次日 08:00)` 访问 `stock_zh_a_spot_em` / `stock_zh_a_spot` 并写入 daily bar。窗口外会在请求接口前中断，避免交易时间实时数据污染日线。
- `akshare_cn_stock_spot_quote_eastmoney` 成功时，先保存 spot 快照，再转换为 `akshare_cn_stock_daily_bar_unadjusted` 的 `spot_quote_close` 行；写 daily bar 时会排除本地 delisted。
- `akshare_cn_stock_spot_quote_eastmoney` 失败时，使用 `stock_zh_a_spot` fallback，保存 Sina 快照，并转换为同一 daily-bar 格式写入 `akshare_cn_stock_daily_bar_unadjusted`。
- `update-akshare-daily-bar` 通过源接口 `stock_zh_a_hist` 获取历史日线；full 默认使用 AkShare 全量池，incremental 默认使用 active 池并排除 delisted。

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
│   ├── akshare_cn_stock_delist_sh/snapshot_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_spot_quote_eastmoney/trade_date=YYYY-MM-DD/data.parquet
│   ├── akshare_cn_stock_spot_quote_sina/trade_date=YYYY-MM-DD/data.parquet
│   └── akshare_cn_stock_daily_bar_unadjusted/code=600000/data.parquet
└── duckdb/quant.duckdb
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

- `GET /v1/datasets`：查看当前有哪些 Dataset、schema、视图名和最新状态。
- `GET /v1/datasets/{dataset_id}/partitions`：查看某个 Dataset 的物理分区文件。
- `GET /v1/status`：查看 Registry 当前状态。
- `POST /v1/query`：用结构化 JSON 查询 Parquet 数据。

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
- `datasets.akshare_cn_stock_spot_quote.update_daily_bar_from_spot`：spot 快照是否同步写入 `akshare_cn_stock_daily_bar_unadjusted`。

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
├── api/          # Baostock provider 与 AkShare client
├── pipeline/     # update-baostock-daily、AkShare 管道、repair-baostock-daily
├── quality/      # 写入前验证
├── storage/      # Schema、ParquetStore、DuckDBStore
└── utils/        # 配置、路径、日志、性能工具
tests/            # 单元与管道测试
config/           # settings.yaml
scripts/          # Windows 定时任务脚本
```
