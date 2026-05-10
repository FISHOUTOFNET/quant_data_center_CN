"""Central catalog for dataset names, schemas, validators, and views."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from src.quality.validators import (
    validate_baostock_cn_stock_valuation_percentile,
    validate_akshare_cn_stock_daily_bar,
    validate_akshare_cn_stock_delist_sh,
    validate_akshare_cn_stock_delist_sz,
    validate_akshare_cn_stock_institution_holding,
    validate_akshare_cn_stock_spot_quote_eastmoney,
    validate_akshare_cn_stock_spot_quote_sina,
    validate_akshare_cn_stock_valuation_eastmoney,
    validate_baostock_cn_stock_adjustment_factor,
    validate_baostock_cn_stock_basic,
    validate_baostock_cn_trading_calendar,
    validate_daily_bar,
)
from src.storage.schema import (
    BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_SCHEMA,
    AKSHARE_STOCK_INSTITUTION_HOLDING_SCHEMA,
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA,
    BAOSTOCK_CN_STOCK_BASIC_SCHEMA,
    BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA,
    DAILY_BAR_SCHEMA,
    AKSHARE_DELIST_SH_SCHEMA,
    AKSHARE_DELIST_SZ_SCHEMA,
    AKSHARE_VALUATION_EASTMONEY_SCHEMA,
    AKSHARE_DAILY_BAR_SCHEMA,
    AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA,
    AKSHARE_SPOT_QUOTE_SINA_SCHEMA,
)


Validator = Callable[[pd.DataFrame], None]


@dataclass(frozen=True)
class DatasetDefinition:
    id: str
    logical_name: str
    source: str
    endpoint: str | None
    code_format: str
    schema: pa.Schema
    validator: Validator
    view_name: str | None = None
    partitioned_by_code: bool = False
    partition_column: str | None = None
    lifecycle: str = "managed"

    @property
    def name(self) -> str:
        """Backward-compatible attribute for code paths not yet using ``id``."""

        return self.id


BAOSTOCK_DAILY_BAR_DATASET_IDS = (
    "baostock_cn_stock_daily_bar_unadjusted",
    "baostock_cn_stock_daily_bar_qfq",
    "baostock_cn_stock_daily_bar_hfq",
)
DAILY_BAR_DATASET_NAMES = BAOSTOCK_DAILY_BAR_DATASET_IDS

DAILY_BAR_DATASETS = tuple(
    DatasetDefinition(
        id=dataset_id,
        logical_name="cn_stock_daily_bar",
        source="baostock",
        endpoint="query_history_k_data_plus",
        code_format="baostock_prefixed",
        schema=DAILY_BAR_SCHEMA,
        validator=validate_daily_bar,
        view_name=f"v_{dataset_id}",
        partitioned_by_code=True,
        partition_column="code",
    )
    for dataset_id in BAOSTOCK_DAILY_BAR_DATASET_IDS
)

BAOSTOCK_CN_STOCK_BASIC_DATASET = DatasetDefinition(
    id="baostock_cn_stock_basic",
    logical_name="cn_stock_basic",
    source="baostock",
    endpoint="query_stock_basic",
    code_format="baostock_prefixed",
    schema=BAOSTOCK_CN_STOCK_BASIC_SCHEMA,
    validator=validate_baostock_cn_stock_basic,
    view_name="v_baostock_cn_stock_basic",
)

BAOSTOCK_CN_TRADING_CALENDAR_DATASET = DatasetDefinition(
    id="baostock_cn_trading_calendar",
    logical_name="cn_trading_calendar",
    source="baostock",
    endpoint="query_trade_dates",
    code_format="none",
    schema=BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA,
    validator=validate_baostock_cn_trading_calendar,
    view_name="v_baostock_cn_trading_calendar",
)

BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET = DatasetDefinition(
    id="baostock_cn_stock_adjustment_factor",
    logical_name="cn_stock_adjustment_factor",
    source="baostock",
    endpoint="query_adjust_factor",
    code_format="baostock_prefixed",
    schema=BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA,
    validator=validate_baostock_cn_stock_adjustment_factor,
    view_name="v_baostock_cn_stock_adjustment_factor",
    partitioned_by_code=True,
    partition_column="code",
)

BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET = DatasetDefinition(
    id="baostock_cn_stock_valuation_percentile",
    logical_name="cn_stock_valuation_percentile",
    source="baostock",
    endpoint=None,
    code_format="baostock_prefixed",
    schema=BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_SCHEMA,
    validator=validate_baostock_cn_stock_valuation_percentile,
    view_name="v_baostock_cn_stock_valuation_percentile",
    partitioned_by_code=True,
    partition_column="code",
)

AKSHARE_VALUATION_EASTMONEY_DATASET = DatasetDefinition(
    id="akshare_cn_stock_valuation_eastmoney",
    logical_name="cn_stock_valuation",
    source="akshare",
    endpoint="stock_value_em",
    code_format="six_digit",
    schema=AKSHARE_VALUATION_EASTMONEY_SCHEMA,
    validator=validate_akshare_cn_stock_valuation_eastmoney,
    view_name="v_akshare_cn_stock_valuation_eastmoney",
    partitioned_by_code=True,
    partition_column="code",
)

AKSHARE_DELIST_SH_DATASET = DatasetDefinition(
    id="akshare_cn_stock_delist_sh",
    logical_name="cn_stock_delist",
    source="akshare",
    endpoint="stock_info_sh_delist",
    code_format="six_digit",
    schema=AKSHARE_DELIST_SH_SCHEMA,
    validator=validate_akshare_cn_stock_delist_sh,
    view_name="v_akshare_cn_stock_delist_sh",
    partition_column="snapshot_date",
)

AKSHARE_DELIST_SZ_DATASET = DatasetDefinition(
    id="akshare_cn_stock_delist_sz",
    logical_name="cn_stock_delist",
    source="akshare",
    endpoint="stock_info_sz_delist",
    code_format="six_digit",
    schema=AKSHARE_DELIST_SZ_SCHEMA,
    validator=validate_akshare_cn_stock_delist_sz,
    view_name="v_akshare_cn_stock_delist_sz",
    partition_column="snapshot_date",
)

AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET = DatasetDefinition(
    id="akshare_cn_stock_spot_quote_eastmoney",
    logical_name="cn_stock_spot_quote",
    source="akshare",
    endpoint="stock_zh_a_spot_em",
    code_format="six_digit",
    schema=AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA,
    validator=validate_akshare_cn_stock_spot_quote_eastmoney,
    view_name="v_akshare_cn_stock_spot_quote_eastmoney",
    partition_column="trade_date",
)

AKSHARE_SPOT_QUOTE_SINA_DATASET = DatasetDefinition(
    id="akshare_cn_stock_spot_quote_sina",
    logical_name="cn_stock_spot_quote",
    source="akshare",
    endpoint="stock_zh_a_spot",
    code_format="six_digit",
    schema=AKSHARE_SPOT_QUOTE_SINA_SCHEMA,
    validator=validate_akshare_cn_stock_spot_quote_sina,
    view_name="v_akshare_cn_stock_spot_quote_sina",
    partition_column="trade_date",
)

AKSHARE_DAILY_BAR_ADJUSTMENTS = ("unadjusted", "qfq", "hfq")
AKSHARE_DAILY_BAR_DATASETS = tuple(
    DatasetDefinition(
        id=f"akshare_cn_stock_daily_bar_{adjustment}",
        logical_name="cn_stock_daily_bar",
        source="akshare",
        endpoint="stock_zh_a_hist",
        code_format="six_digit",
        schema=AKSHARE_DAILY_BAR_SCHEMA,
        validator=validate_akshare_cn_stock_daily_bar,
        view_name=f"v_akshare_cn_stock_daily_bar_{adjustment}",
        partitioned_by_code=True,
        partition_column="code",
    )
    for adjustment in AKSHARE_DAILY_BAR_ADJUSTMENTS
)

AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET = DatasetDefinition(
    id="akshare_cn_stock_institution_holding",
    logical_name="cn_stock_institution_holding",
    source="akshare",
    endpoint=None,
    code_format="six_digit",
    schema=AKSHARE_STOCK_INSTITUTION_HOLDING_SCHEMA,
    validator=validate_akshare_cn_stock_institution_holding,
    view_name="v_akshare_cn_stock_institution_holding",
    partition_column="report_period",
    lifecycle="legacy_unmanaged",
)

AKSHARE_DATASET_NAMES = (AKSHARE_VALUATION_EASTMONEY_DATASET.id,)
AKSHARE_A_STOCK_DATASET_NAMES = (
    AKSHARE_DELIST_SH_DATASET.id,
    AKSHARE_DELIST_SZ_DATASET.id,
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.id,
    AKSHARE_SPOT_QUOTE_SINA_DATASET.id,
    *(definition.id for definition in AKSHARE_DAILY_BAR_DATASETS),
    AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET.id,
)

DATASET_CATALOG = {
    definition.id: definition
    for definition in (
        *DAILY_BAR_DATASETS,
        BAOSTOCK_CN_STOCK_BASIC_DATASET,
        BAOSTOCK_CN_TRADING_CALENDAR_DATASET,
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET,
        AKSHARE_VALUATION_EASTMONEY_DATASET,
        AKSHARE_DELIST_SH_DATASET,
        AKSHARE_DELIST_SZ_DATASET,
        AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
        AKSHARE_SPOT_QUOTE_SINA_DATASET,
        *AKSHARE_DAILY_BAR_DATASETS,
        AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET,
    )
}


def daily_bar_dataset_names() -> tuple[str, ...]:
    return BAOSTOCK_DAILY_BAR_DATASET_IDS


def daily_bar_definitions() -> tuple[DatasetDefinition, ...]:
    return DAILY_BAR_DATASETS


def is_daily_bar_dataset(dataset: str) -> bool:
    return dataset in BAOSTOCK_DAILY_BAR_DATASET_IDS


def daily_bar_definition(dataset: str) -> DatasetDefinition:
    if not is_daily_bar_dataset(dataset):
        raise ValueError(f"Unsupported daily_bar dataset: {dataset}")
    return DATASET_CATALOG[dataset]


def dataset_definition(dataset: str) -> DatasetDefinition:
    try:
        return DATASET_CATALOG[dataset]
    except KeyError as exc:
        raise ValueError(f"Unsupported dataset: {dataset}") from exc


def expand_daily_bar_selection(dataset: str) -> list[str]:
    if dataset == "all":
        return list(BAOSTOCK_DAILY_BAR_DATASET_IDS)
    if not is_daily_bar_dataset(dataset):
        raise ValueError(f"Unsupported daily_bar dataset: {dataset}")
    return [dataset]


def akshare_dataset_names() -> tuple[str, ...]:
    return AKSHARE_DATASET_NAMES


def akshare_a_stock_dataset_names() -> tuple[str, ...]:
    return AKSHARE_A_STOCK_DATASET_NAMES


def akshare_a_stock_definitions() -> tuple[DatasetDefinition, ...]:
    return (
        AKSHARE_DELIST_SH_DATASET,
        AKSHARE_DELIST_SZ_DATASET,
        AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
        AKSHARE_SPOT_QUOTE_SINA_DATASET,
        *AKSHARE_DAILY_BAR_DATASETS,
        AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET,
    )


def akshare_daily_bar_adjustments() -> tuple[str, ...]:
    return AKSHARE_DAILY_BAR_ADJUSTMENTS


def akshare_daily_bar_dataset_id(adjustment: str) -> str:
    normalized = normalize_adjustment(adjustment)
    if normalized not in AKSHARE_DAILY_BAR_ADJUSTMENTS:
        raise ValueError(f"Unsupported AkShare daily bar adjustment: {adjustment}")
    return f"akshare_cn_stock_daily_bar_{normalized}"


def normalize_adjustment(adjustment: str) -> str:
    normalized = str(adjustment).strip().lower()
    if normalized in {"", "none", "unadjusted", "不复权"}:
        return "unadjusted"
    if normalized in {"qfq", "hfq"}:
        return normalized
    raise ValueError(f"Unsupported adjustment: {adjustment}")


def is_akshare_dataset(dataset: str) -> bool:
    return dataset in AKSHARE_DATASET_NAMES


def expand_akshare_selection(dataset: str) -> list[str]:
    if dataset == "all":
        return list(AKSHARE_DATASET_NAMES)
    if not is_akshare_dataset(dataset):
        raise ValueError(f"Unsupported AkShare dataset: {dataset}")
    return [dataset]
