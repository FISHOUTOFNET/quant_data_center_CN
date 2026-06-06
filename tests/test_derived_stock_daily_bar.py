from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from src.sources.derived.stock_daily_bar import build_cn_stock_daily_bar
from src.storage.parquet_store import ParquetStore

NOW = datetime(2024, 1, 5, 12, 0)
DAILY_SOURCE_DATASETS = {
    "baostock_cn_stock_daily_bar_unadjusted",
    "baostock_cn_stock_daily_bar_qfq",
    "baostock_cn_stock_daily_bar_hfq",
    "akshare_cn_stock_daily_bar_unadjusted",
    "akshare_cn_stock_daily_bar_qfq",
    "akshare_cn_stock_daily_bar_hfq",
}


def test_build_cn_stock_daily_bar_materializes_one_security_partition(tmp_path, daily_sample, monkeypatch) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})
    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", _akshare_daily(close=99.0), {"code": "600000"})
    calls: list[tuple[str, object]] = []
    original_read_dataset = ParquetStore.read_dataset

    def spy_read_dataset(self: ParquetStore, dataset_id: str, partition=None):
        if dataset_id in DAILY_SOURCE_DATASETS:
            assert partition is not None
            calls.append((dataset_id, partition))
        return original_read_dataset(self, dataset_id, partition)

    monkeypatch.setattr(ParquetStore, "read_dataset", spy_read_dataset)

    result = build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_stock_daily_bar", {"security_id": "SH.600000"})

    assert result["rows"] == 2
    assert result["partitions"] == 1
    assert calls
    assert loaded["security_id"].unique().tolist() == ["SH.600000"]
    assert loaded.loc[loaded["date"] == date(2024, 1, 2), "close"].iloc[0] == 8.2
    assert loaded.loc[loaded["date"] == date(2024, 1, 2), "source_dataset"].iloc[0].startswith("baostock_")
    assert (tmp_path / "data" / "parquet" / "cn_stock_daily_bar" / "security_id=SH.600000" / "data.parquet").exists()


def test_build_cn_stock_daily_bar_builds_missing_master(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("akshare_cn_stock_spot_quote_eastmoney", _spot(), {"trade_date": "2024-01-05"})
    store.write_dataset("akshare_cn_stock_daily_bar_unadjusted", _akshare_daily(close=8.3), {"code": "600000"})

    result = build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    assert result["rows"] == 1
    assert store.dataset_exists("cn_security_master")
    assert store.dataset_exists("cn_stock_daily_bar", {"security_id": "SH.600000"})


def _master() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "SH.600000",
                "code": "600000",
                "exchange": "SH",
                "name": "PF Bank",
                "security_type": "1",
                "board": "main",
                "baostock_code": "sh.600000",
                "akshare_code": "600000",
                "qlib_symbol": "sh600000",
                "ipo_date": date(1999, 11, 10),
                "delist_date": None,
                "listing_status": "active",
                "is_active": True,
                "source_priority": "mixed",
                "latest_source_date": date(2024, 1, 5),
                "updated_at": NOW,
            }
        ]
    )


def _akshare_daily(close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "600000",
                "source_symbol": "600000",
                "open": 8.1,
                "high": max(close, 8.4),
                "low": 8.0,
                "close": close,
                "volume": 1000,
                "amount": 8200.0,
                "amplitude": 1.0,
                "pct_change": 2.5,
                "price_change": 0.2,
                "turnover_rate": 0.1,
                "adjustment": "unadjusted",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "daily_bar_confirmed",
                "fetched_at": NOW,
            }
        ]
    )


def _spot() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": date(2024, 1, 5),
                "code": "600000",
                "source_symbol": "600000",
                "name": "PF Bank",
                "last_price": 8.3,
                "price_change": 0.1,
                "pct_change": 1.2,
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "prev_close": 8.2,
                "volume": 120000.0,
                "amount": 9960.0,
                "turnover_rate": 0.12,
                "amplitude": 3.0,
                "pe_dynamic": 5.1,
                "pb": 0.71,
                "total_market_cap": 101000000.0,
                "float_market_cap": 81000000.0,
                "source_endpoint": "spot",
                "fetched_at": NOW,
            }
        ]
    )
