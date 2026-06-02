"""Central catalog for dataset names, schemas, validators, and views."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import pandas as pd
import pyarrow as pa

from src.quality.validators import (
    validate_akshare_cn_stock_capital_structure_em,
    validate_akshare_cn_stock_daily_bar,
    validate_akshare_cn_stock_delist_sh,
    validate_akshare_cn_stock_delist_sz,
    validate_akshare_cn_stock_financial_report_sina,
    validate_akshare_cn_stock_institution_holding,
    validate_akshare_cn_stock_report_disclosure,
    validate_akshare_cn_stock_spot_quote_eastmoney,
    validate_akshare_cn_stock_spot_quote_sina,
    validate_akshare_cn_stock_valuation_eastmoney,
    validate_akshare_cn_stock_yysj_em,
    validate_baostock_cn_stock_adjustment_factor,
    validate_baostock_cn_stock_basic,
    validate_baostock_cn_stock_valuation_percentile,
    validate_baostock_cn_trading_calendar,
    validate_daily_bar,
    validate_qlib_cn_calendar_day,
    validate_qlib_cn_instrument_membership,
    validate_qlib_cn_stock_features_day,
)
from src.storage.schema import (
    AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA,
    AKSHARE_DAILY_BAR_SCHEMA,
    AKSHARE_DELIST_SH_SCHEMA,
    AKSHARE_DELIST_SZ_SCHEMA,
    AKSHARE_FINANCIAL_REPORT_SINA_SCHEMA,
    AKSHARE_REPORT_DISCLOSURE_SCHEMA,
    AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA,
    AKSHARE_SPOT_QUOTE_SINA_SCHEMA,
    AKSHARE_STOCK_INSTITUTION_HOLDING_SCHEMA,
    AKSHARE_VALUATION_EASTMONEY_SCHEMA,
    AKSHARE_YYSJ_EM_SCHEMA,
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA,
    BAOSTOCK_CN_STOCK_BASIC_SCHEMA,
    BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA,
    BAOSTOCK_VALUATION_PERCENTILE_SCHEMA,
    DAILY_BAR_SCHEMA,
    QLIB_CN_CALENDAR_DAY_SCHEMA,
    QLIB_CN_INSTRUMENT_MEMBERSHIP_SCHEMA,
    QLIB_CN_STOCK_FEATURES_DAY_SCHEMA,
)

Validator = Callable[[pd.DataFrame], None]
DatasetWriteMode = Literal["replace", "merge", "upsert"]


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
    sort_columns: tuple[str, ...] = ()
    unique_columns: tuple[str, ...] = ()
    default_write_mode: DatasetWriteMode = "replace"
    fixed_column_values: tuple[tuple[str, str], ...] = ()
    legacy_partition_prefixes: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        """Backward-compatible attribute for code paths not yet using ``id``."""

        return self.id


BAOSTOCK_DAILY_BAR_DATASET_IDS = (
    "baostock_cn_stock_daily_bar_unadjusted",
    "baostock_cn_stock_daily_bar_qfq",
    "baostock_cn_stock_daily_bar_hfq",
)

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
        sort_columns=("code", "date"),
        unique_columns=("code", "date"),
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
    sort_columns=("code",),
    unique_columns=("code",),
    legacy_partition_prefixes=("snapshot_date=",),
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
    sort_columns=("calendar_date",),
    unique_columns=("calendar_date",),
    default_write_mode="merge",
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
    sort_columns=("code", "dividend_operate_date"),
    unique_columns=("code", "dividend_operate_date"),
)

BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET = DatasetDefinition(
    id="baostock_cn_stock_valuation_percentile",
    logical_name="cn_stock_valuation_percentile",
    source="baostock",
    endpoint=None,
    code_format="baostock_prefixed",
    schema=BAOSTOCK_VALUATION_PERCENTILE_SCHEMA,
    validator=validate_baostock_cn_stock_valuation_percentile,
    view_name="v_baostock_cn_stock_valuation_percentile",
    partitioned_by_code=True,
    partition_column="code",
    sort_columns=("code", "date"),
    unique_columns=("code", "date"),
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
    sort_columns=("code", "date"),
    unique_columns=("code", "date"),
)

AKSHARE_CAPITAL_STRUCTURE_EM_DATASET = DatasetDefinition(
    id="akshare_cn_stock_capital_structure_em",
    logical_name="cn_stock_capital_structure",
    source="akshare",
    endpoint="stock_zh_a_gbjg_em",
    code_format="six_digit",
    schema=AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA,
    validator=validate_akshare_cn_stock_capital_structure_em,
    view_name="v_akshare_cn_stock_capital_structure_em",
    partitioned_by_code=True,
    partition_column="code",
    sort_columns=("code", "change_date", "change_reason"),
    unique_columns=("code", "change_date", "change_reason"),
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
    sort_columns=("market", "code"),
    unique_columns=("snapshot_date", "exchange", "code"),
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
    sort_columns=("market", "code"),
    unique_columns=("snapshot_date", "exchange", "code"),
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
    sort_columns=("code",),
    unique_columns=("trade_date", "code"),
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
    sort_columns=("code",),
    unique_columns=("trade_date", "code"),
)

AKSHARE_REPORT_DISCLOSURE_DATASET = DatasetDefinition(
    id="akshare_cn_stock_report_disclosure",
    logical_name="cn_stock_report_disclosure",
    source="akshare",
    endpoint="stock_report_disclosure",
    code_format="six_digit",
    schema=AKSHARE_REPORT_DISCLOSURE_SCHEMA,
    validator=validate_akshare_cn_stock_report_disclosure,
    view_name="v_akshare_cn_stock_report_disclosure",
    partition_column="report_period",
    sort_columns=("report_period", "code"),
    unique_columns=("report_period", "code"),
)

AKSHARE_YYSJ_EM_DATASET = DatasetDefinition(
    id="akshare_cn_stock_yysj_em",
    logical_name="cn_stock_report_disclosure",
    source="akshare",
    endpoint="stock_yysj_em",
    code_format="six_digit",
    schema=AKSHARE_YYSJ_EM_SCHEMA,
    validator=validate_akshare_cn_stock_yysj_em,
    view_name="v_akshare_cn_stock_yysj_em",
    partition_column="report_period",
    sort_columns=("report_period", "symbol", "code"),
    unique_columns=("report_period", "symbol", "code"),
)

AKSHARE_FINANCIAL_REPORT_SINA_DATASET = DatasetDefinition(
    id="akshare_cn_stock_financial_report_sina",
    logical_name="cn_stock_financial_report",
    source="akshare",
    endpoint="stock_financial_report_sina",
    code_format="six_digit",
    schema=AKSHARE_FINANCIAL_REPORT_SINA_SCHEMA,
    validator=validate_akshare_cn_stock_financial_report_sina,
    view_name="v_akshare_cn_stock_financial_report_sina",
    partitioned_by_code=True,
    partition_column="code",
    sort_columns=("code", "report_type", "report_date", "item_name"),
    unique_columns=("code", "report_type", "report_date", "item_name"),
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
        sort_columns=("code", "adjustment", "date"),
        unique_columns=("code", "date", "adjustment"),
        default_write_mode="replace",
        fixed_column_values=(("adjustment", adjustment),),
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
    sort_columns=("code", "report_period"),
    unique_columns=("report_period", "code"),
)

QLIB_CN_CALENDAR_DAY_DATASET = DatasetDefinition(
    id="qlib_cn_calendar_day",
    logical_name="cn_calendar_day",
    source="qlib",
    endpoint="qlib_bin",
    code_format="none",
    schema=QLIB_CN_CALENDAR_DAY_SCHEMA,
    validator=validate_qlib_cn_calendar_day,
    view_name="v_qlib_cn_calendar_day",
    sort_columns=("calendar_date",),
    unique_columns=("calendar_date",),
)

QLIB_CN_INSTRUMENT_MEMBERSHIP_DATASET = DatasetDefinition(
    id="qlib_cn_instrument_membership",
    logical_name="cn_instrument_membership",
    source="qlib",
    endpoint="qlib_bin",
    code_format="qlib_symbol",
    schema=QLIB_CN_INSTRUMENT_MEMBERSHIP_SCHEMA,
    validator=validate_qlib_cn_instrument_membership,
    view_name="v_qlib_cn_instrument_membership",
    sort_columns=("universe", "qlib_symbol", "start_date", "end_date"),
    unique_columns=("universe", "qlib_symbol", "start_date", "end_date"),
)

QLIB_CN_STOCK_FEATURES_DAY_DATASET = DatasetDefinition(
    id="qlib_cn_stock_features_day",
    logical_name="cn_stock_features_day",
    source="qlib",
    endpoint="qlib_bin",
    code_format="qlib_symbol",
    schema=QLIB_CN_STOCK_FEATURES_DAY_SCHEMA,
    validator=validate_qlib_cn_stock_features_day,
    view_name="v_qlib_cn_stock_features_day",
    partitioned_by_code=True,
    partition_column="qlib_symbol",
    sort_columns=("qlib_symbol", "date"),
    unique_columns=("qlib_symbol", "date"),
)

AKSHARE_DATASET_NAMES = (AKSHARE_VALUATION_EASTMONEY_DATASET.id, AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.id)
AKSHARE_A_STOCK_DATASET_NAMES = (
    AKSHARE_CAPITAL_STRUCTURE_EM_DATASET.id,
    AKSHARE_DELIST_SH_DATASET.id,
    AKSHARE_DELIST_SZ_DATASET.id,
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.id,
    AKSHARE_SPOT_QUOTE_SINA_DATASET.id,
    AKSHARE_REPORT_DISCLOSURE_DATASET.id,
    AKSHARE_YYSJ_EM_DATASET.id,
    AKSHARE_FINANCIAL_REPORT_SINA_DATASET.id,
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
        AKSHARE_CAPITAL_STRUCTURE_EM_DATASET,
        AKSHARE_DELIST_SH_DATASET,
        AKSHARE_DELIST_SZ_DATASET,
        AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
        AKSHARE_SPOT_QUOTE_SINA_DATASET,
        AKSHARE_REPORT_DISCLOSURE_DATASET,
        AKSHARE_YYSJ_EM_DATASET,
        AKSHARE_FINANCIAL_REPORT_SINA_DATASET,
        *AKSHARE_DAILY_BAR_DATASETS,
        AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET,
        QLIB_CN_CALENDAR_DAY_DATASET,
        QLIB_CN_INSTRUMENT_MEMBERSHIP_DATASET,
        QLIB_CN_STOCK_FEATURES_DAY_DATASET,
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
        AKSHARE_CAPITAL_STRUCTURE_EM_DATASET,
        AKSHARE_DELIST_SH_DATASET,
        AKSHARE_DELIST_SZ_DATASET,
        AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
        AKSHARE_SPOT_QUOTE_SINA_DATASET,
        AKSHARE_REPORT_DISCLOSURE_DATASET,
        AKSHARE_YYSJ_EM_DATASET,
        AKSHARE_FINANCIAL_REPORT_SINA_DATASET,
        *AKSHARE_DAILY_BAR_DATASETS,
        AKSHARE_STOCK_INSTITUTION_HOLDING_DATASET,
    )


def akshare_daily_bar_adjustments() -> tuple[str, ...]:
    return AKSHARE_DAILY_BAR_ADJUSTMENTS


def qlib_definitions() -> tuple[DatasetDefinition, ...]:
    return (
        QLIB_CN_CALENDAR_DAY_DATASET,
        QLIB_CN_INSTRUMENT_MEMBERSHIP_DATASET,
        QLIB_CN_STOCK_FEATURES_DAY_DATASET,
    )


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
