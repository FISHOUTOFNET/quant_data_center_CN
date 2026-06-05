from __future__ import annotations

from datetime import datetime

import pandas as pd

import src.sources.akshare.eastmoney.modules.daily_bar as daily_bar_module
from src.sources.akshare.eastmoney.modules.daily_bar import DailyBarTask, plan_daily_bar_tasks, prefilter_daily_bar_tasks
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


def test_akshare_daily_bar_prefilter_uses_latest_trading_day_not_calendar_max(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-05", "is_trading_day": "1"},
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
                {"calendar_date": "2024-01-07", "is_trading_day": "0"},
            ]
        ),
    )
    store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted",
        pd.DataFrame(
            [
                {
                    "date": "2024-01-05",
                    "code": "600000",
                    "source_symbol": "600000",
                    "open": 8.0,
                    "high": 8.3,
                    "low": 7.9,
                    "close": 8.2,
                    "volume": 1000,
                    "amount": 8200.0,
                    "amplitude": 1.0,
                    "pct_change": 0.5,
                    "price_change": 0.04,
                    "turnover_rate": 0.1,
                    "adjustment": "unadjusted",
                    "source_endpoint": "stock_zh_a_hist",
                    "quality_status": "ok",
                    "fetched_at": datetime(2024, 1, 5, 16, 0),
                }
            ]
        ),
        {"code": "600000"},
    )
    DuckDBStore(root=tmp_path).build_views()

    tasks = [
        DailyBarTask(
            code="600000",
            adjustment="unadjusted",
            dataset="akshare_cn_stock_daily_bar_unadjusted",
            start_date="2024-01-05",
            end_date="2024-01-07",
            output_path=store.dataset_path("akshare_cn_stock_daily_bar_unadjusted", {"code": "600000"}),
            api_start_date="2024-01-05",
            api_end_date="2024-01-07",
            write_mode="upsert",
        )
    ]

    assert prefilter_daily_bar_tasks(tasks, store, checkpoint_lookup=object()) == []


def test_akshare_daily_bar_default_end_resolves_weekend_to_latest_trading_day(tmp_path, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {"calendar_date": "2024-01-05", "is_trading_day": "1"},
                {"calendar_date": "2024-01-06", "is_trading_day": "0"},
                {"calendar_date": "2024-01-07", "is_trading_day": "0"},
            ]
        ),
    )
    monkeypatch.setattr(daily_bar_module, "default_candidate_date", lambda config: "2024-01-07")

    tasks = plan_daily_bar_tasks(
        ConfigManager(tmp_path),
        store,
        mode="incremental",
        adjustment="unadjusted",
        code=("600000",),
        start="2024-01-05",
        end=None,
    )

    assert tasks[0].end_date == "2024-01-05"
    assert tasks[0].api_end_date == "2024-01-05"
