from __future__ import annotations

import pytest

from src.storage.dataset_catalog import (
    AKSHARE_CAPITAL_STRUCTURE_EM_DATASET,
    AKSHARE_DELIST_SH_DATASET,
    AKSHARE_DELIST_SZ_DATASET,
    AKSHARE_REPORT_DISCLOSURE_DATASET,
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
    AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET,
    AKSHARE_VALUATION_EASTMONEY_DATASET,
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    akshare_a_stock_dataset_names,
    akshare_a_stock_definitions,
    akshare_daily_bar_dataset_id,
    daily_bar_dataset_names,
    daily_bar_definition,
    daily_bar_definitions,
    expand_akshare_selection,
    expand_daily_bar_selection,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore


def test_daily_bar_catalog_expands_supported_selections() -> None:
    assert expand_daily_bar_selection("all") == list(daily_bar_dataset_names())
    assert expand_daily_bar_selection("baostock_cn_stock_daily_bar_qfq") == ["baostock_cn_stock_daily_bar_qfq"]


def test_daily_bar_catalog_rejects_unsupported_dataset() -> None:
    with pytest.raises(ValueError, match="Unsupported daily_bar dataset"):
        daily_bar_definition("daily_bar_unknown")


def test_akshare_catalog_expands_supported_selections() -> None:
    assert expand_akshare_selection("all") == [
        "akshare_cn_stock_valuation_eastmoney",
        "akshare_cn_stock_capital_structure_em",
    ]
    assert expand_akshare_selection("akshare_cn_stock_valuation_eastmoney") == ["akshare_cn_stock_valuation_eastmoney"]
    assert expand_akshare_selection("akshare_cn_stock_capital_structure_em") == [
        "akshare_cn_stock_capital_structure_em"
    ]
    with pytest.raises(ValueError, match="Unsupported AkShare dataset"):
        expand_akshare_selection("akshare_cn_stock_institution_holding")


def test_akshare_a_stock_catalog_registers_independent_datasets() -> None:
    assert AKSHARE_DELIST_SH_DATASET.name in akshare_a_stock_dataset_names()
    assert AKSHARE_DELIST_SZ_DATASET.name in akshare_a_stock_dataset_names()
    assert AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.name in akshare_a_stock_dataset_names()
    assert AKSHARE_REPORT_DISCLOSURE_DATASET.name in akshare_a_stock_dataset_names()
    assert akshare_daily_bar_dataset_id("unadjusted") == "akshare_cn_stock_daily_bar_unadjusted"
    assert [definition.name for definition in akshare_a_stock_definitions()] == list(akshare_a_stock_dataset_names())


def test_storage_layout_uses_daily_bar_catalog(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)

    store.ensure_layout()

    for definition in daily_bar_definitions():
        assert (tmp_path / "data" / "parquet" / definition.name).is_dir()
    assert (tmp_path / "data" / "parquet" / BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.name).is_dir()
    assert (tmp_path / "data" / "parquet" / AKSHARE_VALUATION_EASTMONEY_DATASET.name).is_dir()
    assert (tmp_path / "data" / "parquet" / AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.name).is_dir()
    for definition in akshare_a_stock_definitions():
        assert (tmp_path / "data" / "parquet" / definition.name).is_dir()


def test_duckdb_views_use_daily_bar_catalog(tmp_path) -> None:
    sqls = DuckDBStore(root=tmp_path).view_sqls()

    for definition in daily_bar_definitions():
        assert any((definition.view_name or f"v_{definition.name}") in sql for sql in sqls)
    assert any(
        (BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.view_name or "v_baostock_cn_stock_adjustment_factor") in sql
        for sql in sqls
    )
    assert any(
        (AKSHARE_VALUATION_EASTMONEY_DATASET.view_name or "v_akshare_cn_stock_valuation_eastmoney") in sql
        for sql in sqls
    )
    assert any("v_akshare_cn_stock_capital_structure_em" in sql for sql in sqls)
    assert any("v_akshare_cn_stock_spot_quote_eastmoney" in sql for sql in sqls)
    assert any("v_akshare_cn_stock_report_disclosure" in sql for sql in sqls)
    assert any("v_akshare_cn_stock_daily_bar_unadjusted" in sql for sql in sqls)
    assert any("v_akshare_cn_stock_institution_holding" in sql for sql in sqls)
    assert AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET.lifecycle == "legacy_unmanaged"
