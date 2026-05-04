# AkShare A 股管道契约

本文档保留原文件名，作为 AkShare A 股行情重构后的短版契约说明。详细使用方式见 `README.md`，系统结构见 `ARCHITECTURE.md`。

## 当前目标

AkShare A 股能力独立于 Baostock `update-daily`，负责：

- `stock_value_em`：估值指标。
- `stock_info_sh_delist`：上交所退市/暂停上市清单。
- `stock_zh_a_spot_em`：东方财富 A 股 spot 快照。
- `stock_zh_a_spot_sina`：Sina fallback 快照。
- `stock_zh_a_hist_none/qfq/hfq`：AkShare 历史日线。

## 代码格式

所有 AkShare 数据集的 `code` 和 `code=...` 分区统一使用 6 位数字：

```text
600000
000001
430017
```

允许输入 `sh.600000`、`sh600000`、`600000.0`，但入库一律转为 `600000`。`source_symbol` 保留接口原始值。

Baostock 数据集不受此约定影响，仍使用 `sh.600000` 等市场前缀代码。

## 股票池

默认 AkShare 股票池来自本地 AkShare 数据，不使用 Baostock `stock_basic`。

- active 池：最新 `stock_zh_a_spot_em` 减去最新 `stock_info_sh_delist`。
- full 池：active spot 代码与 delisted 代码并集。
- `stock_value_em` partial 使用 active 池。
- `stock_value_em` full 或 `--include-inactive` 使用 full 池。
- `stock_zh_a_hist` incremental 使用 active 池。
- `stock_zh_a_hist` full 使用 full 池。

没有本地 AkShare 清单时，应先运行：

```powershell
qdc update-akshare-spot
qdc update-akshare-universe --market 全部
```

## Spot 写 Hist

`update-akshare-spot` 访问 `stock_zh_a_spot_em` / `stock_zh_a_spot` 时必须在北京时间 `[18:00, 次日 08:00)`。窗口外直接中断。

成功路径：

```text
stock_zh_a_spot_em
  -> stock_zh_a_spot_em snapshot
  -> stock_zh_a_hist_none spot_close rows
```

Fallback 路径：

```text
stock_zh_a_spot_em failed
  -> stock_zh_a_spot_sina snapshot
  -> stock_zh_a_hist_none spot_close rows
```

Spot 写入 hist 时：

- `adjust = none`
- `quality_status = spot_close`
- `source_endpoint = stock_zh_a_spot_em` 或 `stock_zh_a_spot`
- EM spot 写 hist 时排除本地 delisted

`stock_zh_a_hist` 写入时：

- `quality_status = hist_confirmed`
- `source_endpoint = stock_zh_a_hist`
- upsert 按 `(code, date, adjust)` 覆盖同日 spot 行

## 存储布局

```text
data/parquet/
├── stock_value_em/code=600000/data.parquet
├── stock_info_sh_delist/snapshot_date=YYYY-MM-DD/data.parquet
├── stock_zh_a_spot_em/trade_date=YYYY-MM-DD/data.parquet
├── stock_zh_a_spot_sina/trade_date=YYYY-MM-DD/data.parquet
├── stock_zh_a_hist_none/code=600000/data.parquet
├── stock_zh_a_hist_qfq/code=600000/data.parquet
└── stock_zh_a_hist_hfq/code=600000/data.parquet
```

旧版 `code=sh.600000` AkShare 分区不会自动迁移。需要一致查询时，建议重建 AkShare 数据或另写显式迁移脚本。

## 验收命令

```powershell
pytest -q
python -m src.cli update-akshare --help
python -m src.cli update-akshare-spot --help
python -m src.cli update-akshare-hist --help
```
