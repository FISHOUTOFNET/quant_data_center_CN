from __future__ import annotations

import pyarrow as pa

from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.schema import (
    CN_SECURITY_MASTER_SCHEMA,
    CN_STOCK_DAILY_BAR_SCHEMA,
    CN_STOCK_VALUATION_SCHEMA,
    schema_for_dataset,
)


def test_derived_schemas_are_registered() -> None:
    assert schema_for_dataset("cn_security_master") == CN_SECURITY_MASTER_SCHEMA
    assert schema_for_dataset("cn_stock_daily_bar") == CN_STOCK_DAILY_BAR_SCHEMA
    assert schema_for_dataset("cn_stock_valuation") == CN_STOCK_VALUATION_SCHEMA
    assert CN_SECURITY_MASTER_SCHEMA.field("updated_at").type == pa.timestamp("ms")
    assert CN_STOCK_DAILY_BAR_SCHEMA.field("security_id").type == pa.string()
    assert CN_STOCK_VALUATION_SCHEMA.field("pe_ttm_percentile_all_history").type == pa.float64()


def test_derived_catalog_entries_are_registered() -> None:
    master = DATASET_CATALOG["cn_security_master"]
    daily = DATASET_CATALOG["cn_stock_daily_bar"]
    valuation = DATASET_CATALOG["cn_stock_valuation"]

    assert master.source == "derived"
    assert master.partition_column is None
    assert daily.partition_column == "security_id"
    assert daily.unique_columns == ("date", "security_id", "adjustment")
    assert valuation.partition_column == "security_id"
    assert valuation.unique_columns == ("date", "security_id")
