"""Central catalog for dataset names, schemas, validators, and views."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from src.quality.validators import validate_calendar, validate_daily_k, validate_stock_basic
from src.storage.schema import CALENDAR_SCHEMA, DAILY_K_SCHEMA, STOCK_BASIC_SCHEMA


Validator = Callable[[pd.DataFrame], None]


@dataclass(frozen=True)
class DatasetDefinition:
    name: str
    schema: pa.Schema
    validator: Validator
    view_name: str | None = None
    partitioned_by_code: bool = False


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

DATASET_CATALOG = {
    definition.name: definition
    for definition in (
        *DAILY_K_DATASETS,
        STOCK_BASIC_DATASET,
        CALENDAR_DATASET,
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
