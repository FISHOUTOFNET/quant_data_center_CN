"""Data quality checks executed before Parquet files are replaced."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
import pyarrow as pa

from src.storage.schema import (
    ADJUST_FACTOR_SCHEMA,
    CALENDAR_SCHEMA,
    DAILY_K_SCHEMA,
    STOCK_BASIC_SCHEMA,
    STOCK_INSTITUTE_HOLD_SCHEMA,
    STOCK_VALUE_EM_SCHEMA,
    field_names,
)
from src.utils.logging import logger


class ValidationError(ValueError):
    """Raised when a dataframe violates a storage contract."""


def validate_schema_matches(df: pd.DataFrame, schema: pa.Schema) -> None:
    expected = field_names(schema)
    actual = list(df.columns)
    if actual != expected:
        raise ValidationError(f"Schema columns mismatch. expected={expected}, actual={actual}")
    try:
        pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    except Exception as exc:
        raise ValidationError(f"Schema type conversion failed: {exc}") from exc


def validate_unique_code_date(df: pd.DataFrame) -> None:
    duplicated = df.duplicated(["code", "date"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["code", "date"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate code/date rows found: {sample}")


def validate_date_monotonic(df: pd.DataFrame) -> None:
    if df.empty:
        return
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("date contains null or invalid values")
    work = df.assign(_date=dates)
    for code, group in work.groupby("code", dropna=False, sort=False):
        if not group["_date"].is_monotonic_increasing:
            raise ValidationError(f"date is not monotonically increasing for code={code}")


def _require_columns_not_null(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if df[column].isna().any()]
    if missing:
        raise ValidationError(f"Columns contain null values: {missing}")


OHLC_RELATIVE_TOLERANCE = 1e-4


def validate_ohlc(df: pd.DataFrame) -> None:
    columns = ["open", "high", "low", "close"]
    null_columns = [col for col in columns if df[col].isna().any()]
    if null_columns:
        sample = df.loc[df[null_columns[0]].isna(), ["code", "date", *null_columns]].head(5).to_dict("records")
        logger.warning("OHLC validation warning (null values in {}): {}", null_columns, sample)
        return
    ohlc = df[columns].apply(pd.to_numeric, errors="coerce")
    if ohlc.isna().any().any():
        sample = df.loc[ohlc.isna().any(axis=1), ["code", "date", *columns]].head(5).to_dict("records")
        logger.warning("OHLC validation warning (non-numeric values): {}", sample)
        return
    tol = ohlc["high"].abs() * OHLC_RELATIVE_TOLERANCE
    invalid = (ohlc["high"] < ohlc["low"] - tol) | (
        ohlc["open"] < ohlc["low"] - tol
    ) | (ohlc["open"] > ohlc["high"] + tol) | (
        ohlc["close"] < ohlc["low"] - tol
    ) | (ohlc["close"] > ohlc["high"] + tol)
    if invalid.any():
        sample = df.loc[invalid, ["code", "date", *columns]].head(5).to_dict("records")
        logger.warning("OHLC validation warning (data source quality issue): {}", sample)


def validate_non_negative(df: pd.DataFrame, column: str) -> None:
    if df[column].isna().any():
        sample = df.loc[df[column].isna(), ["code", "date", column]].head(5).to_dict("records")
        logger.warning("{} validation warning (null values): {}", column, sample)
        return
    values = pd.to_numeric(df[column], errors="coerce")
    if values.isna().any():
        sample = df.loc[values.isna(), ["code", "date", column]].head(5).to_dict("records")
        logger.warning("{} validation warning (non-numeric values): {}", column, sample)
        return
    invalid = values < 0
    if invalid.any():
        sample = df.loc[invalid, ["code", "date", column]].head(5).to_dict("records")
        logger.warning("{} validation warning (negative values): {}", column, sample)


def validate_daily_k(df: pd.DataFrame, schema: pa.Schema = DAILY_K_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_ohlc(df)
    validate_non_negative(df, "volume")
    validate_non_negative(df, "amount")


def validate_stock_basic(df: pd.DataFrame, schema: pa.Schema = STOCK_BASIC_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    if df["code"].duplicated(keep=False).any():
        sample = df.loc[df["code"].duplicated(keep=False), ["code"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate stock_basic code rows found: {sample}")


def validate_calendar(df: pd.DataFrame, schema: pa.Schema = CALENDAR_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    if df["calendar_date"].duplicated(keep=False).any():
        sample = df.loc[df["calendar_date"].duplicated(keep=False), ["calendar_date"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate calendar_date rows found: {sample}")


def validate_adjust_factor(df: pd.DataFrame, schema: pa.Schema = ADJUST_FACTOR_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    duplicated = df.duplicated(["code", "dividOperateDate"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["code", "dividOperateDate"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate code/dividOperateDate rows found: {sample}")
    if df.empty:
        return
    dates = pd.to_datetime(df["dividOperateDate"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("dividOperateDate contains null or invalid values")
    work = df.assign(_divid_operate_date=dates)
    for code, group in work.groupby("code", dropna=False, sort=False):
        if not group["_divid_operate_date"].is_monotonic_increasing:
            raise ValidationError(f"dividOperateDate is not monotonically increasing for code={code}")


def validate_stock_institute_hold(
    df: pd.DataFrame,
    schema: pa.Schema = STOCK_INSTITUTE_HOLD_SCHEMA,
) -> None:
    validate_schema_matches(df, schema)
    duplicated = df.duplicated(["report_period", "code"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["report_period", "code"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate report_period/code rows found: {sample}")
    if df.empty:
        return
    _require_columns_not_null(df, ["report_period", "period_end_date", "code"])


def validate_stock_value_em(df: pd.DataFrame, schema: pa.Schema = STOCK_VALUE_EM_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_non_negative(df, "total_market_cap")
    validate_non_negative(df, "float_market_cap")
    validate_non_negative(df, "total_shares")
    validate_non_negative(df, "float_shares")
