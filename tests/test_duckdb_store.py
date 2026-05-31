from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd

from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore


def test_duckdb_views_can_be_created_and_queried(
    tmp_path,
    daily_sample,
    baostock_cn_stock_adjustment_factor_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_daily_bar_qfq", daily_sample(), {"code": "sh.600000"})
    store.write_dataset(
        "baostock_cn_stock_adjustment_factor", baostock_cn_stock_adjustment_factor_sample(), {"code": "sh.600000"}
    )
    store.write_dataset(
        "akshare_cn_stock_valuation_eastmoney", akshare_cn_stock_valuation_eastmoney_sample(), {"code": "600000"}
    )
    store.write_dataset(
        "akshare_cn_stock_spot_quote_eastmoney",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
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
                    "source_endpoint": "stock_zh_a_spot_em",
                    "fetched_at": datetime(2024, 1, 3, 16, 0),
                }
            ]
        ),
        {"trade_date": "2024-01-03"},
    )

    duck_store = DuckDBStore(root=tmp_path)
    duckdb_file = tmp_path / "data" / "duckdb" / "quant.duckdb"
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_file)) as conn:
        conn.execute("create view v_daily_k_none as select 1 as stale")

    sqls = duck_store.build_views()

    assert any("v_baostock_cn_stock_daily_bar_qfq" in sql for sql in sqls)
    assert any("v_baostock_cn_stock_adjustment_factor" in sql for sql in sqls)
    assert any("v_akshare_cn_stock_valuation_eastmoney" in sql for sql in sqls)
    assert any("v_akshare_cn_stock_spot_quote_eastmoney" in sql for sql in sqls)
    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        result = conn.execute(
            "select count(*) from v_baostock_cn_stock_daily_bar_qfq where code='sh.600000'"
        ).fetchone()
        factor_result = conn.execute(
            "select count(*) from v_baostock_cn_stock_adjustment_factor where code='sh.600000'"
        ).fetchone()
        value_result = conn.execute(
            "select count(*) from v_akshare_cn_stock_valuation_eastmoney where code='600000'"
        ).fetchone()
        spot_result = conn.execute(
            "select count(*) from v_akshare_cn_stock_spot_quote_eastmoney where code='600000'"
        ).fetchone()
        old_view_count = conn.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = current_schema()
              and table_name = 'v_daily_k_none'
            """
        ).fetchone()
    assert result == (2,)
    assert factor_result == (1,)
    assert value_result == (2,)
    assert spot_result == (1,)
    assert old_view_count == (0,)
