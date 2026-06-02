"""Data quality checks executed before Parquet files are replaced."""

from __future__ import annotations

import re

import pandas as pd
import pyarrow as pa

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
    field_names,
)
from src.utils.logging import logger


class ValidationError(ValueError):
    """Raised when a dataframe violates a storage contract."""


AKSHARE_CODE_RE = re.compile(r"^\d{6}$")


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


def validate_unique_columns(df: pd.DataFrame, columns: list[str]) -> None:
    duplicated = df.duplicated(columns, keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, columns].head(5).to_dict("records")
        raise ValidationError(f"Duplicate rows found for {columns}: {sample}")


def validate_akshare_six_digit_codes(df: pd.DataFrame) -> None:
    if df.empty or "code" not in df.columns:
        return
    codes = df["code"].astype("string").str.strip()
    invalid = codes.isna() | ~codes.map(lambda value: bool(AKSHARE_CODE_RE.fullmatch(str(value))))
    if invalid.any():
        sample = df.loc[invalid, ["code"]].head(5).to_dict("records")
        raise ValidationError(f"AkShare code must be 6 digits: {sample}")


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
    invalid = (
        (ohlc["high"] < ohlc["low"] - tol)
        | (ohlc["open"] < ohlc["low"] - tol)
        | (ohlc["open"] > ohlc["high"] + tol)
        | (ohlc["close"] < ohlc["low"] - tol)
        | (ohlc["close"] > ohlc["high"] + tol)
    )
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


def validate_daily_bar(df: pd.DataFrame, schema: pa.Schema = DAILY_BAR_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_ohlc(df)
    validate_non_negative(df, "volume")
    validate_non_negative(df, "amount")


def validate_baostock_cn_stock_valuation_percentile(
    df: pd.DataFrame,
    schema: pa.Schema = BAOSTOCK_VALUATION_PERCENTILE_SCHEMA,
) -> None:
    validate_schema_matches(df, schema)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    percentile_columns = [
        column for column in df.columns if column.endswith(("_1y", "_3y", "_5y", "_10y", "_all_history"))
    ]
    for column in percentile_columns:
        values = pd.to_numeric(df[column], errors="coerce")
        invalid = values.notna() & ~values.between(0.0, 100.0, inclusive="both")
        if invalid.any():
            sample = df.loc[invalid, ["code", "date", column]].head(5).to_dict("records")
            raise ValidationError(f"{column} percentile out of range 0..100: {sample}")


def validate_baostock_cn_stock_basic(df: pd.DataFrame, schema: pa.Schema = BAOSTOCK_CN_STOCK_BASIC_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    if df["code"].duplicated(keep=False).any():
        sample = df.loc[df["code"].duplicated(keep=False), ["code"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate baostock_cn_stock_basic code rows found: {sample}")


def validate_baostock_cn_trading_calendar(
    df: pd.DataFrame, schema: pa.Schema = BAOSTOCK_CN_TRADING_CALENDAR_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    if df["calendar_date"].duplicated(keep=False).any():
        sample = df.loc[df["calendar_date"].duplicated(keep=False), ["calendar_date"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate calendar_date rows found: {sample}")


def validate_baostock_cn_stock_adjustment_factor(
    df: pd.DataFrame, schema: pa.Schema = BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    duplicated = df.duplicated(["code", "dividend_operate_date"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["code", "dividend_operate_date"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate code/dividend_operate_date rows found: {sample}")
    if df.empty:
        return
    dates = pd.to_datetime(df["dividend_operate_date"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("dividend_operate_date contains null or invalid values")
    work = df.assign(_divid_operate_date=dates)
    for code, group in work.groupby("code", dropna=False, sort=False):
        if not group["_divid_operate_date"].is_monotonic_increasing:
            raise ValidationError(f"dividend_operate_date is not monotonically increasing for code={code}")


def validate_akshare_cn_stock_valuation_eastmoney(
    df: pd.DataFrame, schema: pa.Schema = AKSHARE_VALUATION_EASTMONEY_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_code_date(df)
    validate_date_monotonic(df)
    validate_non_negative(df, "total_market_cap")
    validate_non_negative(df, "float_market_cap")
    validate_non_negative(df, "total_shares")
    validate_non_negative(df, "float_shares")


def validate_akshare_cn_stock_capital_structure_em(
    df: pd.DataFrame, schema: pa.Schema = AKSHARE_CAPITAL_STRUCTURE_EM_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["code", "change_date", "change_reason"])
    if df.empty:
        return
    dates = pd.to_datetime(df["change_date"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("change_date contains null or invalid values")
    work = df.assign(_change_date=dates)
    for code, group in work.groupby("code", dropna=False, sort=False):
        if not group["_change_date"].is_monotonic_increasing:
            raise ValidationError(f"change_date is not monotonically increasing for code={code}")
    for column in [
        "total_shares",
        "restricted_shares",
        "circulated_shares",
        "listed_a_shares",
    ]:
        validate_non_negative(df.rename(columns={"change_date": "date"}), column)


def validate_akshare_cn_stock_delist_sh(df: pd.DataFrame, schema: pa.Schema = AKSHARE_DELIST_SH_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    duplicated = df.duplicated(["snapshot_date", "market", "code"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["snapshot_date", "market", "code"]].head(5).to_dict("records")
        logger.warning("Duplicate rows found for ['snapshot_date', 'market', 'code']: {}", sample)


def validate_akshare_cn_stock_delist_sz(df: pd.DataFrame, schema: pa.Schema = AKSHARE_DELIST_SZ_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    duplicated = df.duplicated(["snapshot_date", "market", "code"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["snapshot_date", "market", "code"]].head(5).to_dict("records")
        logger.warning("Duplicate rows found for ['snapshot_date', 'market', 'code']: {}", sample)


def validate_akshare_cn_stock_spot_quote_eastmoney(
    df: pd.DataFrame, schema: pa.Schema = AKSHARE_SPOT_QUOTE_EASTMONEY_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["trade_date", "code"])
    for column in [
        "last_price",
        "volume",
        "amount",
        "total_market_cap",
        "float_market_cap",
    ]:
        validate_non_negative(df.rename(columns={"trade_date": "date"}), column)


def validate_akshare_cn_stock_spot_quote_sina(
    df: pd.DataFrame, schema: pa.Schema = AKSHARE_SPOT_QUOTE_SINA_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["trade_date", "code"])
    for column in ["last_price", "volume", "amount"]:
        validate_non_negative(df.rename(columns={"trade_date": "date"}), column)


def validate_akshare_cn_stock_report_disclosure(
    df: pd.DataFrame, schema: pa.Schema = AKSHARE_REPORT_DISCLOSURE_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["report_period", "code"])


def validate_akshare_cn_stock_yysj_em(df: pd.DataFrame, schema: pa.Schema = AKSHARE_YYSJ_EM_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["report_period", "symbol", "code"])


def validate_akshare_cn_stock_financial_report_sina(
    df: pd.DataFrame, schema: pa.Schema = AKSHARE_FINANCIAL_REPORT_SINA_SCHEMA
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["code", "report_type", "report_date", "item_name"])
    if df.empty:
        return
    dates = pd.to_datetime(df["report_date"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("report_date contains null or invalid values")
    work = df.assign(_report_date=dates)
    for key, group in work.groupby(["code", "report_type"], dropna=False, sort=False):
        if not group["_report_date"].is_monotonic_increasing:
            raise ValidationError(f"report_date is not monotonically increasing for code/report_type={key}")


def validate_akshare_cn_stock_daily_bar(df: pd.DataFrame, schema: pa.Schema = AKSHARE_DAILY_BAR_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["code", "date", "adjustment"])
    validate_date_monotonic(df)
    validate_ohlc(df)
    validate_non_negative(df, "volume")
    validate_non_negative(df, "amount")


def validate_akshare_cn_stock_institution_holding(
    df: pd.DataFrame,
    schema: pa.Schema = AKSHARE_STOCK_INSTITUTION_HOLDING_SCHEMA,
) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    validate_unique_columns(df, ["report_period", "code"])


def validate_qlib_cn_calendar_day(df: pd.DataFrame, schema: pa.Schema = QLIB_CN_CALENDAR_DAY_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    if df["calendar_date"].duplicated(keep=False).any():
        sample = df.loc[df["calendar_date"].duplicated(keep=False), ["calendar_date"]].head(5).to_dict("records")
        raise ValidationError(f"Duplicate qlib calendar_date rows found: {sample}")


def validate_qlib_cn_instrument_membership(
    df: pd.DataFrame,
    schema: pa.Schema = QLIB_CN_INSTRUMENT_MEMBERSHIP_SCHEMA,
) -> None:
    validate_schema_matches(df, schema)
    validate_unique_columns(df, ["universe", "qlib_symbol", "start_date", "end_date"])


def validate_qlib_cn_stock_features_day(
    df: pd.DataFrame,
    schema: pa.Schema = QLIB_CN_STOCK_FEATURES_DAY_SCHEMA,
) -> None:
    validate_schema_matches(df, schema)
    validate_unique_columns(df, ["qlib_symbol", "date"])
    if df.empty:
        return
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.isna().any():
        raise ValidationError("date contains null or invalid values")
    work = df.assign(_date=dates)
    for symbol, group in work.groupby("qlib_symbol", dropna=False, sort=False):
        if not group["_date"].is_monotonic_increasing:
            raise ValidationError(f"date is not monotonically increasing for qlib_symbol={symbol}")
