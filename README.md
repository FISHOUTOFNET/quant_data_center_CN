# Quant Data Center

Windows 单机低频量化数据中心。项目使用 Python 拉取 A 股低频数据，Parquet 做本地存储，DuckDB 做查询视图。

当前有两条数据源路径：

- Baostock：`update-daily` 主流程，保存日线、复权因子、股票基础信息和交易日历，股票代码保持 `sh.600000` 格式。
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
qdc update-daily
qdc update-daily --mode full --dataset all --start 1990-01-01 --end 2024-04-26 --code sh.600000
qdc repair --code sh.600000 --start 2024-01-01 --end 2024-04-26 --dataset daily_k_hfq
```

`update-daily` 默认在 18:00 前使用前一自然日作为候选日，18:00 后使用当天，并通过本地交易日历回退到最近交易日。

### AkShare 估值

```powershell
qdc update-akshare
qdc update-akshare --dataset stock_value_em --code 600000 --code 000001
qdc update-akshare --dataset stock_value_em --include-inactive
```

`stock_value_em` 默认股票池来自本地 AkShare 清单，不再使用 Baostock `stock_basic`：

- partial：使用最新 `stock_zh_a_spot_em` 清单并排除本地 delisted。
- full 或 `--include-inactive`：使用最新 `stock_zh_a_spot_em` 与 delisted 的并集。
- 显式 `--code` 可传 `600000`、`sh.600000` 或 `sh600000`，入库统一为 `600000`。

### AkShare A 股行情

```powershell
qdc update-akshare-universe --market 全部
qdc update-akshare-spot --end 2026-05-03
qdc update-akshare-hist --mode full --adjust all --start 1990-01-01
qdc update-akshare-hist --mode incremental --adjust none --start 2026-05-01 --end 2026-05-03
```

重要规则：

- `update-akshare-spot` 只允许在北京时间 `[18:00, 次日 08:00)` 访问 `stock_zh_a_spot_em` / `stock_zh_a_spot` 并写入 hist。窗口外会在请求接口前中断，避免交易时间实时数据污染日线。
- `stock_zh_a_spot_em` 成功时，先保存 spot 快照，再转换为 `stock_zh_a_hist_none` 的 `spot_close` 行；写 hist 时会排除本地 delisted。
- `stock_zh_a_spot_em` 失败时，使用 `stock_zh_a_spot` fallback，保存 Sina 快照，并转换为同一 hist 格式写入 `stock_zh_a_hist_none`。
- `stock_zh_a_hist` full 默认使用 AkShare 全量池；incremental 默认使用 active 池并排除 delisted。

## 数据目录

```text
data/
├── parquet/
│   ├── daily_k_none/code=sh.600000/data.parquet
│   ├── daily_k_qfq/code=sh.600000/data.parquet
│   ├── daily_k_hfq/code=sh.600000/data.parquet
│   ├── adjust_factor/code=sh.600000/data.parquet
│   ├── stock_basic/data.parquet
│   ├── calendar/data.parquet
│   ├── stock_value_em/code=600000/data.parquet
│   ├── stock_info_sh_delist/snapshot_date=YYYY-MM-DD/data.parquet
│   ├── stock_zh_a_spot_em/trade_date=YYYY-MM-DD/data.parquet
│   ├── stock_zh_a_spot_sina/trade_date=YYYY-MM-DD/data.parquet
│   └── stock_zh_a_hist_none/code=600000/data.parquet
├── raw/akshare/
│   ├── <endpoint>/YYYYMMDD/*.parquet
│   └── manifest/fetch_runs.jsonl
└── duckdb/quant.duckdb
```

旧 AkShare 分区如果曾写成 `code=sh.600000`，本次不会自动迁移。建议重建 AkShare 数据，或后续单独编写迁移脚本。

## 查询示例

```python
import duckdb

con = duckdb.connect("data/duckdb/quant.duckdb")

daily = con.execute("""
    select date, code, close
    from v_daily_k_qfq
    where code = 'sh.600000'
    order by date desc
    limit 5
""").fetchdf()

value = con.execute("""
    select date, code, pe_ttm, pb
    from v_stock_value_em
    where code = '600000'
    order by date desc
    limit 5
""").fetchdf()
```

手动重建视图：

```powershell
qdc build-views
```

## 配置

主要配置在 `config/settings.yaml`。

常用项：

- `project.timezone`：默认 `Asia/Shanghai`，用于 18:00/08:00 窗口判断。
- `api.akshare.max_retries`：AkShare 最大重试次数。
- `api.akshare.workers`：AkShare 并发任务数。
- `api.akshare.jitter_seconds`：请求前随机延迟。
- `api.akshare.endpoints.<name>.failure_threshold` / `cooldown_minutes`：端点熔断配置。
- `datasets.stock_value_em.active_only`：partial 模式是否只取 active AkShare 股票池。
- `datasets.stock_zh_a_spot.update_hist_from_spot`：spot 快照是否同步写入 `stock_zh_a_hist_none`。

## 运维

查看日志：

```powershell
Get-Content logs\qdc.log -Tail 50
Select-String -Path logs\qdc.log -Pattern "ERROR|WARNING"
```

查看 AkShare 调用记录：

```powershell
Get-Content data\raw\akshare\manifest\fetch_runs.jsonl -Tail 10
```

查询任务状态：

```python
import duckdb

con = duckdb.connect("data/duckdb/quant.duckdb")
df = con.execute("""
    select dataset, code, status, last_success_date, row_count
    from update_status
    order by updated_at desc
    limit 20
""").fetchdf()
print(df)
```

## 项目结构

```text
src/
├── api/          # Baostock provider 与 AkShare client
├── pipeline/     # update-daily、AkShare 管道、repair
├── quality/      # 写入前验证
├── storage/      # Schema、ParquetStore、DuckDBStore
└── utils/        # 配置、路径、日志、性能工具
tests/            # 单元与管道测试
config/           # settings.yaml 与旧 universe.yaml
scripts/          # Windows 定时任务脚本
```
