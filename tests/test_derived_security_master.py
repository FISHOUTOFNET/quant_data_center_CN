from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from src.sources.derived.security_master import build_security_master
from src.storage.parquet_store import ParquetStore

NOW = datetime(2024, 1, 5, 12, 0)


def test_build_security_master_combines_sources(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset(
        "akshare_cn_stock_spot_quote_eastmoney", _spot("600000", "PF Bank"), {"trade_date": "2024-01-05"}
    )
    store.write_dataset("akshare_cn_stock_delist_sz", _delist("000001", "Old SZ"), {"snapshot_date": "2024-01-04"})
    store.write_dataset(
        "baostock_cn_stock_basic",
        pd.DataFrame(
            [
                {
                    "code": "sh.600000",
                    "name": "Baostock PF",
                    "ipo_date": date(1999, 11, 10),
                    "delist_date": None,
                    "security_type": "1",
                    "listing_status": "1",
                },
                {
                    "code": "sz.000001",
                    "name": "Baostock Old SZ",
                    "ipo_date": date(1991, 4, 3),
                    "delist_date": None,
                    "security_type": "1",
                    "listing_status": "1",
                },
            ]
        ),
    )
    store.write_dataset(
        "qlib_cn_instrument_membership",
        pd.DataFrame(
            [
                {
                    "universe": "all",
                    "qlib_symbol": "sh600000",
                    "exchange": "SH",
                    "code": "600000",
                    "start_date": date(2020, 1, 1),
                    "end_date": None,
                },
                {
                    "universe": "all",
                    "qlib_symbol": "bj830000",
                    "exchange": "BJ",
                    "code": "830000",
                    "start_date": date(2020, 1, 1),
                    "end_date": None,
                },
            ]
        ),
    )

    result = build_security_master(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_security_master")

    assert result["rows"] == 3
    sh = loaded.loc[loaded["security_id"] == "SH.600000"].iloc[0]
    assert sh["name"] == "PF Bank"
    assert sh["baostock_code"] == "sh.600000"
    assert sh["akshare_code"] == "600000"
    assert sh["qlib_symbol"] == "sh600000"
    assert sh["listing_status"] == "active"
    assert bool(sh["is_active"]) is True
    assert sh["source_priority"] == "mixed"

    sz = loaded.loc[loaded["security_id"] == "SZ.000001"].iloc[0]
    assert sz["name"] == "Old SZ"
    assert sz["listing_status"] == "delisted"
    assert bool(sz["is_active"]) is False

    bj = loaded.loc[loaded["security_id"] == "BJ.830000"].iloc[0]
    assert bj["source_priority"] == "qlib_only"
    assert bj["baostock_code"] == ""
    assert bj["qlib_symbol"] == "bj830000"
    assert bj["listing_status"] == "unknown"


def test_build_security_master_writes_empty_schema_when_sources_empty(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()

    result = build_security_master(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    assert result["rows"] == 0
    assert store.dataset_exists("cn_security_master")
    assert store.read_dataset("cn_security_master").empty


def test_build_security_master_uses_sina_spot_fallback(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("akshare_cn_stock_spot_quote_sina", _spot("000001", "PA Bank"), {"trade_date": "2024-01-05"})

    build_security_master(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_security_master")

    assert loaded.loc[0, "security_id"] == "SZ.000001"
    assert loaded.loc[0, "listing_status"] == "active"


def test_build_security_master_uses_qlib_partition_fallback_without_reading_features(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    partition_dir = tmp_path / "data" / "parquet" / "qlib_cn_stock_features_day" / "qlib_symbol=bj830000"
    partition_dir.mkdir(parents=True)
    (partition_dir / "data.parquet").write_text("not read by security master", encoding="utf-8")

    build_security_master(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    loaded = store.read_dataset("cn_security_master")

    assert loaded.loc[0, "security_id"] == "BJ.830000"
    assert loaded.loc[0, "qlib_symbol"] == "bj830000"


def _spot(code: str, name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": date(2024, 1, 5),
                "code": code,
                "source_symbol": code,
                "name": name,
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


def _delist(code: str, name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "snapshot_date": date(2024, 1, 4),
                "exchange": "sz",
                "market": "all",
                "code": code,
                "source_symbol": code,
                "name": name,
                "list_date": date(2000, 1, 1),
                "delist_date": date(2024, 1, 4),
                "source_endpoint": "delist",
                "fetched_at": NOW,
            }
        ]
    )
