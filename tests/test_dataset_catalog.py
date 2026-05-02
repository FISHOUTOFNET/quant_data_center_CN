from __future__ import annotations

import pytest

from src.storage.dataset_catalog import (
    ADJUST_FACTOR_DATASET,
    STOCK_INSTITUTE_HOLD_DATASET,
    STOCK_VALUE_EM_DATASET,
    daily_k_dataset_names,
    daily_k_definitions,
    daily_k_definition,
    expand_akshare_selection,
    expand_daily_k_selection,
)
from src.storage.duckdb_store import DuckDBStore
from src.storage.parquet_store import ParquetStore


def test_daily_k_catalog_expands_supported_selections() -> None:
    assert expand_daily_k_selection("daily_k_all") == list(daily_k_dataset_names())
    assert expand_daily_k_selection("daily_k_qfq") == ["daily_k_qfq"]


def test_daily_k_catalog_rejects_unsupported_dataset() -> None:
    with pytest.raises(ValueError, match="Unsupported daily_k dataset"):
        daily_k_definition("daily_k_unknown")


def test_akshare_catalog_expands_supported_selections() -> None:
    assert expand_akshare_selection("all") == ["stock_institute_hold", "stock_value_em"]
    assert expand_akshare_selection("stock_value_em") == ["stock_value_em"]


def test_storage_layout_uses_daily_k_catalog(tmp_path) -> None:
    store = ParquetStore(root=tmp_path)

    store.ensure_layout()

    for definition in daily_k_definitions():
        assert (tmp_path / "data" / "parquet" / definition.name).is_dir()
    assert (tmp_path / "data" / "parquet" / ADJUST_FACTOR_DATASET.name).is_dir()
    assert (tmp_path / "data" / "parquet" / STOCK_INSTITUTE_HOLD_DATASET.name).is_dir()
    assert (tmp_path / "data" / "parquet" / STOCK_VALUE_EM_DATASET.name).is_dir()


def test_duckdb_views_use_daily_k_catalog(tmp_path) -> None:
    sqls = DuckDBStore(root=tmp_path).view_sqls()

    for definition in daily_k_definitions():
        assert any((definition.view_name or f"v_{definition.name}") in sql for sql in sqls)
    assert any((ADJUST_FACTOR_DATASET.view_name or "v_adjust_factor") in sql for sql in sqls)
    assert any((STOCK_INSTITUTE_HOLD_DATASET.view_name or "v_stock_institute_hold") in sql for sql in sqls)
    assert any((STOCK_VALUE_EM_DATASET.view_name or "v_stock_value_em") in sql for sql in sqls)
