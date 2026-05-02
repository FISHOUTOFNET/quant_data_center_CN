from __future__ import annotations

import duckdb

from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore


def test_duckdb_views_can_be_created_and_queried(
    tmp_path,
    daily_sample,
    adjust_factor_sample,
    stock_institute_hold_sample,
    stock_value_em_sample,
) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_daily_k("daily_k_qfq", "sh.600000", daily_sample())
    store.write_adjust_factor("sh.600000", adjust_factor_sample())
    store.write_stock_institute_hold("2024Q1", stock_institute_hold_sample())
    store.write_stock_value_em("600000", stock_value_em_sample())

    duck_store = DuckDBStore(root=tmp_path)
    sqls = duck_store.build_views()

    assert any("v_daily_k_qfq" in sql for sql in sqls)
    assert any("v_adjust_factor" in sql for sql in sqls)
    assert any("v_stock_institute_hold" in sql for sql in sqls)
    assert any("v_stock_value_em" in sql for sql in sqls)
    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        result = conn.execute("select count(*) from v_daily_k_qfq where code='sh.600000'").fetchone()
        factor_result = conn.execute("select count(*) from v_adjust_factor where code='sh.600000'").fetchone()
        hold_result = conn.execute("select count(*) from v_stock_institute_hold where report_period='2024Q1'").fetchone()
        value_result = conn.execute("select count(*) from v_stock_value_em where code='600000'").fetchone()
    assert result == (2,)
    assert factor_result == (1,)
    assert hold_result == (2,)
    assert value_result == (2,)
