"""Central catalog for dataset names, schemas, validators, and views."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from src.quality.validators import (
    validate_adjust_factor,
    validate_calendar,
    validate_daily_k,
    validate_stock_basic,
    validate_stock_institute_hold,
    validate_stock_value_em,
)
from src.storage.schema import (
    ADJUST_FACTOR_SCHEMA,
    CALENDAR_SCHEMA,
    DAILY_K_SCHEMA,
    STOCK_BASIC_SCHEMA,
    STOCK_INSTITUTE_HOLD_SCHEMA,
    STOCK_VALUE_EM_SCHEMA,
)


Validator = Callable[[pd.DataFrame], None]


@dataclass(frozen=True)
class DatasetDefinition:
    name: str
    schema: pa.Schema
    validator: Validator
    view_name: str | None = None
    partitioned_by_code: bool = False
    partition_column: str | None = None


DAILY_K_DATASET_NAMES = ("daily_k_none", "daily_k_qfq", "daily_k_hfq")

DAILY_K_DATASETS = tuple(
    DatasetDefinition(
        name=name,
        schema=DAILY_K_SCHEMA,
        validator=validate_daily_k,
        view_name=f"v_{name}",
        partitioned_by_code=True,
    )
    for name in DAILY_K_DATASET_NAMES
)

STOCK_BASIC_DATASET = DatasetDefinition(
    name="stock_basic",
    schema=STOCK_BASIC_SCHEMA,
    validator=validate_stock_basic,
    view_name="v_stock_basic",
)

CALENDAR_DATASET = DatasetDefinition(
    name="calendar",
    schema=CALENDAR_SCHEMA,
    validator=validate_calendar,
    view_name="v_calendar",
)

ADJUST_FACTOR_DATASET = DatasetDefinition(
    name="adjust_factor",
    schema=ADJUST_FACTOR_SCHEMA,
    validator=validate_adjust_factor,
    view_name="v_adjust_factor",
    partitioned_by_code=True,
    partition_column="code",
)

STOCK_INSTITUTE_HOLD_DATASET = DatasetDefinition(
    name="stock_institute_hold",
    schema=STOCK_INSTITUTE_HOLD_SCHEMA,
    validator=validate_stock_institute_hold,
    view_name="v_stock_institute_hold",
    partition_column="report_period",
)

STOCK_VALUE_EM_DATASET = DatasetDefinition(
    name="stock_value_em",
    schema=STOCK_VALUE_EM_SCHEMA,
    validator=validate_stock_value_em,
    view_name="v_stock_value_em",
    partitioned_by_code=True,
    partition_column="code",
)

AKSHARE_DATASET_NAMES = (STOCK_INSTITUTE_HOLD_DATASET.name, STOCK_VALUE_EM_DATASET.name)

DATASET_CATALOG = {
    definition.name: definition
    for definition in (
        *DAILY_K_DATASETS,
        STOCK_BASIC_DATASET,
        CALENDAR_DATASET,
        ADJUST_FACTOR_DATASET,
        STOCK_INSTITUTE_HOLD_DATASET,
        STOCK_VALUE_EM_DATASET,
    )
}


def daily_k_dataset_names() -> tuple[str, ...]:
    return DAILY_K_DATASET_NAMES


def daily_k_definitions() -> tuple[DatasetDefinition, ...]:
    return DAILY_K_DATASETS


def is_daily_k_dataset(dataset: str) -> bool:
    return dataset in DAILY_K_DATASET_NAMES


def daily_k_definition(dataset: str) -> DatasetDefinition:
    if not is_daily_k_dataset(dataset):
        raise ValueError(f"Unsupported daily_k dataset: {dataset}")
    return DATASET_CATALOG[dataset]


def dataset_definition(dataset: str) -> DatasetDefinition:
    try:
        return DATASET_CATALOG[dataset]
    except KeyError as exc:
        raise ValueError(f"Unsupported dataset: {dataset}") from exc


def expand_daily_k_selection(dataset: str) -> list[str]:
    if dataset in {"all", "daily_k_all", "daily_k"}:
        return list(DAILY_K_DATASET_NAMES)
    if not is_daily_k_dataset(dataset):
        raise ValueError(f"Unsupported daily_k dataset: {dataset}")
    return [dataset]


def akshare_dataset_names() -> tuple[str, ...]:
    return AKSHARE_DATASET_NAMES


def is_akshare_dataset(dataset: str) -> bool:
    return dataset in AKSHARE_DATASET_NAMES


def expand_akshare_selection(dataset: str) -> list[str]:
    if dataset == "all":
        return list(AKSHARE_DATASET_NAMES)
    if not is_akshare_dataset(dataset):
        raise ValueError(f"Unsupported AkShare dataset: {dataset}")
    return [dataset]
