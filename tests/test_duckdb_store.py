from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd

from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore


def test_duckdb_views_can_be_created_and_queried(
    tmp_path,
    daily_sample,
    adjust_factor_sample,
    stock_value_em_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample())
    store.write_adjust_factor("sh.600000", adjust_factor_sample())
    store.write_stock_value_em("600000", stock_value_em_sample())
    store.write_stock_zh_a_spot_em(
        "2024-01-03",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
                    "code": "600000",
                    "source_symbol": "600000",
                    "name": "PF Bank",
                    "latest_price": 8.3,
                    "change_amount": 0.1,
                    "pct_chg": 1.2,
                    "open": 8.2,
                    "high": 8.4,
                    "low": 8.1,
                    "preclose": 8.2,
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
    )

    duck_store = DuckDBStore(root=tmp_path)
    sqls = duck_store.build_views()

    assert any("v_daily_k_qfq" in sql for sql in sqls)
    assert any("v_adjust_factor" in sql for sql in sqls)
    assert any("v_stock_value_em" in sql for sql in sqls)
    assert any("v_stock_zh_a_spot_em" in sql for sql in sqls)
    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        result = conn.execute("select count(*) from v_daily_k_qfq where code='sh.600000'").fetchone()
        factor_result = conn.execute("select count(*) from v_adjust_factor where code='sh.600000'").fetchone()
        value_result = conn.execute("select count(*) from v_stock_value_em where code='600000'").fetchone()
        spot_result = conn.execute("select count(*) from v_stock_zh_a_spot_em where code='600000'").fetchone()
    assert result == (2,)
    assert factor_result == (1,)
    assert value_result == (2,)
    assert spot_result == (1,)
