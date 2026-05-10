"""Parquet storage with schema coercion and atomic replacement."""

from __future__ import annotations

import os
import re
import time
import uuid
from contextlib import suppress
from datetime import date
from pathlib import Path
from threading import RLock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.dataset_catalog import (
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    BAOSTOCK_CN_TRADING_CALENDAR_DATASET,
    BAOSTOCK_CN_STOCK_BASIC_DATASET,
    AKSHARE_DELIST_SH_DATASET,
    AKSHARE_DELIST_SZ_DATASET,
    AKSHARE_VALUATION_EASTMONEY_DATASET,
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
    AKSHARE_SPOT_QUOTE_SINA_DATASET,
    akshare_a_stock_definitions,
    daily_bar_definition,
    daily_bar_definitions,
    dataset_definition,
    akshare_daily_bar_dataset_id,
    normalize_adjustment,
)
from src.storage.schema import field_names
from src.storage.metadata_store import DuckDBMetadataStore
from src.storage.data_registry import DataRegistry
from src.utils import paths
from src.utils.logging import logger


NULL_LIKE_VALUES = {"": pd.NA, "None": pd.NA, "none": pd.NA, "NaN": pd.NA, "nan": pd.NA}
PARQUET_READ_MAX_RETRIES = 3
PARQUET_READ_RETRY_DELAY = 0.1
PARQUET_WRITE_MAX_RETRIES = 3
PARQUET_WRITE_RETRY_DELAY = 0.1
AKSHARE_CODE_PATTERN = re.compile(r"^\d{6}$")
_SOURCE_PREV_CLOSE = "pre" + "close"
_SOURCE_ADJUST_FLAG = "adjust" + "flag"
_SOURCE_TRADE_STATUS = "trade" + "status"
_SOURCE_PCT_CHANGE = "pct" + "Chg"
_SOURCE_PE_TTM = "pe" + "TTM"
_SOURCE_PB_MRQ = "pb" + "MRQ"
_SOURCE_PS_TTM = "ps" + "TTM"
_SOURCE_PCF_NCF_TTM = "pcf" + "Ncf" + "TTM"
_SOURCE_IS_ST = "is" + "ST"
_SOURCE_IPO_DATE = "ipo" + "Date"
_SOURCE_DELIST_DATE = "out" + "Date"
_SOURCE_DIVIDEND_OPERATE_DATE = "divid" + "Operate" + "Date"
_SOURCE_FORWARD_ADJUST_FACTOR = "fore" + "Adjust" + "Factor"
_SOURCE_BACKWARD_ADJUST_FACTOR = "back" + "Adjust" + "Factor"
_SOURCE_ADJUSTMENT_FACTOR = "adjust" + "Factor"

COLUMN_ALIASES = {
    _SOURCE_PREV_CLOSE: "prev_close",
    _SOURCE_ADJUST_FLAG: "adjust_flag",
    "turn": "turnover_rate",
    _SOURCE_TRADE_STATUS: "trade_status",
    _SOURCE_PCT_CHANGE: "pct_change",
    "pct_chg": "pct_change",
    _SOURCE_PE_TTM: "pe_ttm",
    _SOURCE_PB_MRQ: "pb_mrq",
    _SOURCE_PS_TTM: "ps_ttm",
    _SOURCE_PCF_NCF_TTM: "pcf_ncf_ttm",
    _SOURCE_IS_ST: "is_st",
    "code_name": "name",
    _SOURCE_IPO_DATE: "ipo_date",
    _SOURCE_DELIST_DATE: "delist_date",
    "type": "security_type",
    "status": "listing_status",
    _SOURCE_DIVIDEND_OPERATE_DATE: "dividend_operate_date",
    _SOURCE_FORWARD_ADJUST_FACTOR: "forward_adjust_factor",
    _SOURCE_BACKWARD_ADJUST_FACTOR: "backward_adjust_factor",
    _SOURCE_ADJUSTMENT_FACTOR: "adjustment_factor",
    "latest_price": "last_price",
    "change_amount": "price_change",
    "adjust": "adjustment",
}


def _akshare_partition_code(code: object) -> str:
    if pd.isna(code):
        raise ValueError("AkShare partition code must be a 6-digit string")
    value = str(code).strip()
    if not AKSHARE_CODE_PATTERN.fullmatch(value):
        raise ValueError(f"AkShare partition code must be 6 digits, got: {value!r}")
    return value


def _normalize_optional_adjustment(value: object) -> object:
    if pd.isna(value):
        return value
    return normalize_adjustment(str(value))


class ParquetStore:
    """Read and write the project Parquet layout."""

    def __init__(
        self,
        root: Path | None = None,
        parquet_dir: Path | None = None,
        metadata_dir: Path | None = None,
    ) -> None:
        self.root = (root or paths.ROOT).resolve()
        self.parquet_dir = (parquet_dir or self.root / "data" / "parquet").resolve()
        self.metadata_dir = (metadata_dir or self.root / "data" / "metadata").resolve()
        self._parquet_write_lock = RLock()
        self._metadata_store = DuckDBMetadataStore(
            root=self.root,
        )

    def _safe_read_parquet(self, path: Path) -> pd.DataFrame:
        """Read parquet file with retry logic for transient permission errors.
        
        On Windows, files can be temporarily locked by other processes (antivirus,
        backup software, concurrent writes). This method retries reads with
        exponential backoff to handle transient PermissionError.
        """
        last_error: Exception | None = None
        for attempt in range(PARQUET_READ_MAX_RETRIES):
            try:
                return pd.read_parquet(path)
            except PermissionError as e:
                last_error = e
                if attempt < PARQUET_READ_MAX_RETRIES - 1:
                    delay = PARQUET_READ_RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        "Permission denied reading {}, retrying in {:.3f}s (attempt {}/{})",
                        path,
                        delay,
                        attempt + 1,
                        PARQUET_READ_MAX_RETRIES,
                    )
                    time.sleep(delay)
        raise last_error

    def ensure_layout(self) -> None:
        daily_bar_dirs = [self.parquet_dir / definition.name for definition in daily_bar_definitions()]
        akshare_a_stock_dirs = [self.parquet_dir / definition.name for definition in akshare_a_stock_definitions()]
        for directory in [
            *daily_bar_dirs,
            *akshare_a_stock_dirs,
            self.parquet_dir / BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.name,
            self.parquet_dir / AKSHARE_VALUATION_EASTMONEY_DATASET.name,
            self.parquet_dir / "baostock_cn_stock_basic",
            self.parquet_dir / "baostock_cn_trading_calendar",
            self.metadata_dir,
            self.root / "data" / "duckdb",
            self.root / "data" / "raw",
            self.root / "data" / "raw" / "akshare",
            self.root / "data" / "raw" / "akshare" / "manifest",
            self.root / "logs",
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        (self.root / "data" / "registry").mkdir(parents=True, exist_ok=True)

    def baostock_daily_bar_path(self, dataset: str, code: str) -> Path:
        return self.parquet_dir / dataset / f"code={code}" / "data.parquet"

    def baostock_cn_stock_adjustment_factor_path(self, code: str) -> Path:
        return self.parquet_dir / BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.name / f"code={code}" / "data.parquet"

    def baostock_cn_stock_basic_path(self) -> Path:
        return self.parquet_dir / "baostock_cn_stock_basic" / "data.parquet"

    def baostock_cn_trading_calendar_path(self) -> Path:
        return self.parquet_dir / "baostock_cn_trading_calendar" / "data.parquet"

    def akshare_cn_stock_valuation_eastmoney_path(self, code: str) -> Path:
        stock_code = _akshare_partition_code(code)
        return self.parquet_dir / AKSHARE_VALUATION_EASTMONEY_DATASET.name / f"code={stock_code}" / "data.parquet"

    def akshare_cn_stock_delist_sh_path(self, snapshot_date: str) -> Path:
        return self.parquet_dir / AKSHARE_DELIST_SH_DATASET.name / f"snapshot_date={snapshot_date}" / "data.parquet"

    def akshare_cn_stock_delist_sz_path(self, snapshot_date: str) -> Path:
        return self.parquet_dir / AKSHARE_DELIST_SZ_DATASET.name / f"snapshot_date={snapshot_date}" / "data.parquet"

    def akshare_cn_stock_spot_quote_eastmoney_path(self, trade_date: str) -> Path:
        return self.parquet_dir / AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.name / f"trade_date={trade_date}" / "data.parquet"

    def akshare_cn_stock_spot_quote_sina_path(self, trade_date: str) -> Path:
        return self.parquet_dir / AKSHARE_SPOT_QUOTE_SINA_DATASET.name / f"trade_date={trade_date}" / "data.parquet"

    def akshare_daily_bar_path(self, adjustment: str, code: str) -> Path:
        dataset = akshare_daily_bar_dataset_id(adjustment)
        stock_code = _akshare_partition_code(code)
        return self.parquet_dir / dataset / f"code={stock_code}" / "data.parquet"

    def akshare_manifest_path(self) -> Path:
        return self.root / "data" / "raw" / "akshare" / "manifest" / "fetch_runs.jsonl"

    def metadata_path(self, name: str) -> Path:
        return self.metadata_dir / f"{name}.parquet"

    def close(self) -> None:
        self._metadata_store.close()

    def clean_dataframe_for_schema(self, df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        """Return a dataframe with exactly schema columns and schema-ready values.

        Baostock returns every field as text. Empty strings in numeric columns
        become NaN/null, and empty strings in date columns become NULL.
        """

        target_names = {field.name for field in schema}
        work = df.rename(
            columns={
                old: new
                for old, new in COLUMN_ALIASES.items()
                if old in df.columns and new in target_names and new not in df.columns
            }
        )
        cleaned = pd.DataFrame(index=work.index)
        for field in schema:
            if field.name in work.columns:
                series = work[field.name]
            else:
                series = pd.Series(pd.NA, index=work.index, name=field.name)
            cleaned[field.name] = self._coerce_series(series, field.type)
        if "adjustment" in cleaned.columns and not cleaned.empty:
            cleaned["adjustment"] = cleaned["adjustment"].map(_normalize_optional_adjustment).astype("string")
        return cleaned.reset_index(drop=True)

    def _coerce_series(self, series: pd.Series, arrow_type: pa.DataType) -> pd.Series:
        if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            values = self._replace_null_like(series)
            dates = pd.to_datetime(values, errors="coerce")
            return dates.dt.date.where(dates.notna(), None)
        if pa.types.is_timestamp(arrow_type):
            values = self._replace_null_like(series)
            if pd.api.types.is_datetime64_any_dtype(values.dtype):
                return values.dt.floor("ms")
            return pd.to_datetime(values, errors="coerce").dt.floor("ms")
        if pa.types.is_integer(arrow_type):
            if pd.api.types.is_integer_dtype(series.dtype):
                return series
            values = self._replace_null_like(series)
            return pd.to_numeric(values, errors="coerce").astype("Int64")
        if pa.types.is_boolean(arrow_type):
            if pd.api.types.is_bool_dtype(series.dtype):
                return series
            values = self._replace_null_like(series)
            if values.empty:
                return values.astype("boolean")
            normalized = values.astype("string").str.strip().str.lower()
            return normalized.map(
                {
                    "true": True,
                    "t": True,
                    "1": True,
                    "yes": True,
                    "false": False,
                    "f": False,
                    "0": False,
                    "no": False,
                }
            ).astype("boolean")
        if pa.types.is_floating(arrow_type):
            if pd.api.types.is_float_dtype(series.dtype):
                return series
            if pd.api.types.is_integer_dtype(series.dtype):
                return series.astype("float64")
            values = self._replace_null_like(series)
            return pd.to_numeric(values, errors="coerce")
        if pa.types.is_string(arrow_type):
            if pd.api.types.is_string_dtype(series.dtype):
                return series
            return series.astype("string")
        return series

    def _replace_null_like(self, series: pd.Series) -> pd.Series:
        if series.empty:
            return series
        if not pd.api.types.is_object_dtype(series.dtype) and not pd.api.types.is_string_dtype(series.dtype):
            return series
        return series.replace(NULL_LIKE_VALUES)

    def _require_partition_value(
        self,
        df: pd.DataFrame,
        column: str,
        expected: str,
        context: str,
    ) -> None:
        if df.empty:
            return
        if df[column].isna().any():
            raise ValueError(f"{context} missing {column} for {expected}")
        values = set(df[column].astype(str))
        if values != {expected}:
            raise ValueError(f"{context} mismatch for {expected}: {sorted(values)}")

    def to_table(self, df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
        cleaned = self.clean_dataframe_for_schema(df, schema)
        return pa.Table.from_pandas(cleaned, schema=schema, preserve_index=False)

    def atomic_write(self, df: pd.DataFrame, schema: pa.Schema, destination: Path) -> None:
        """Write DataFrame to Parquet file with atomic replacement and retry logic.
        
        On Windows, files can be temporarily locked by other processes (antivirus,
        backup software, concurrent reads/writes). This method retries writes with
        exponential backoff to handle transient PermissionError and OSError.
        
        Args:
            df: DataFrame to write
            schema: PyArrow schema for the table
            destination: Target file path
            
        Raises:
            OSError: If all write attempts fail
            PermissionError: If all write attempts fail due to permission issues
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.parent / f"data.{uuid.uuid4().hex}.tmp.parquet"
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        
        last_error: Exception | None = None
        for attempt in range(PARQUET_WRITE_MAX_RETRIES):
            try:
                with self._parquet_write_lock:
                    pq.write_table(table, tmp_path)
                    os.replace(tmp_path, destination)
                return
            except (PermissionError, OSError) as e:
                last_error = e
                if tmp_path.exists():
                    with suppress(Exception):
                        tmp_path.unlink()
                
                if attempt < PARQUET_WRITE_MAX_RETRIES - 1:
                    delay = PARQUET_WRITE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        "Failed to write {}, retrying in {:.3f}s (attempt {}/{}): {}",
                        destination,
                        delay,
                        attempt + 1,
                        PARQUET_WRITE_MAX_RETRIES,
                        type(e).__name__,
                    )
                    time.sleep(delay)
            except Exception:
                if tmp_path.exists():
                    with suppress(Exception):
                        tmp_path.unlink()
                raise
        
        if tmp_path.exists():
            with suppress(Exception):
                tmp_path.unlink()
        
        raise last_error

    def write_baostock_daily_bars(self, dataset: str, code: str, df: pd.DataFrame) -> Path:
        definition = daily_bar_definition(dataset)
        cleaned = self.clean_dataframe_for_schema(df, definition.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "code", code, "Daily file code")
            cleaned = cleaned.sort_values(["code", "date"]).reset_index(drop=True)
        definition.validator(cleaned)
        destination = self.baostock_daily_bar_path(dataset, code)
        self.atomic_write(cleaned, definition.schema, destination)
        self._publish_dataset_write(dataset, code, cleaned, destination)
        logger.info(
            "Daily Parquet stored dataset={} code={} rows={} path={}",
            dataset,
            code,
            len(cleaned),
            destination,
        )
        return destination

    def read_baostock_daily_bars(self, dataset: str, code: str) -> pd.DataFrame:
        definition = daily_bar_definition(dataset)
        path = self.baostock_daily_bar_path(dataset, code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(definition.schema))
        return self._safe_read_parquet(path)

    def write_baostock_cn_stock_adjustment_factor(self, code: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_baostock_cn_stock_adjustment_factor_frame(code, df)
        destination = self.baostock_cn_stock_adjustment_factor_path(code)
        self.atomic_write(cleaned, BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.schema, destination)
        self._publish_dataset_write(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.name, code, cleaned, destination)
        return destination

    def clean_baostock_cn_stock_adjustment_factor_frame(self, code: str, df: pd.DataFrame) -> pd.DataFrame:
        cleaned = self.clean_dataframe_for_schema(df, BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "code", code, "Adjust factor file code")
            cleaned = cleaned.sort_values(["code", "dividend_operate_date"]).reset_index(drop=True)
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.validator(cleaned)
        return cleaned

    def read_baostock_cn_stock_adjustment_factor(self, code: str) -> pd.DataFrame:
        path = self.baostock_cn_stock_adjustment_factor_path(code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.schema)

    def read_baostock_cn_stock_basic(self) -> pd.DataFrame:
        path = self.baostock_cn_stock_basic_path()
        if not path.exists():
            return pd.DataFrame(columns=field_names(BAOSTOCK_CN_STOCK_BASIC_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), BAOSTOCK_CN_STOCK_BASIC_DATASET.schema)

    def baostock_cn_stock_basic_codes(self, mode: str = "all") -> list[str]:
        df = self.read_baostock_cn_stock_basic()
        if df.empty:
            return []
        if mode == "all":
            work = df
        elif mode == "active":
            status = df["listing_status"].astype("string").str.strip()
            stock_type = df["security_type"].astype("string").str.strip()
            work = df.loc[(stock_type == "1") & (status == "1")]
        else:
            raise ValueError(f"Unsupported baostock_cn_stock_basic code mode: {mode}")

        codes = work["code"].astype("string").str.strip()
        codes = codes.loc[codes.notna() & (codes != "")]
        return list(dict.fromkeys(codes.astype(str).tolist()))

    def write_baostock_cn_stock_basic(self, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, BAOSTOCK_CN_STOCK_BASIC_DATASET.schema)
        cleaned = cleaned.sort_values(["code"]).reset_index(drop=True) if not cleaned.empty else cleaned
        BAOSTOCK_CN_STOCK_BASIC_DATASET.validator(cleaned)
        destination = self.baostock_cn_stock_basic_path()
        dataset_dir = self.parquet_dir / "baostock_cn_stock_basic"
        for old_partition in dataset_dir.glob("snapshot_date=*"):
            if old_partition.is_dir():
                import shutil
                shutil.rmtree(old_partition, ignore_errors=True)
                logger.info("Removed old baostock_cn_stock_basic partition: {}", old_partition)
        self.atomic_write(cleaned, BAOSTOCK_CN_STOCK_BASIC_DATASET.schema, destination)
        self._publish_dataset_write(BAOSTOCK_CN_STOCK_BASIC_DATASET.name, "*", cleaned, destination)
        return destination

    def write_baostock_cn_trading_calendar(self, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema)
        destination = self.baostock_cn_trading_calendar_path()
        if destination.exists():
            existing = self.clean_dataframe_for_schema(self._safe_read_parquet(destination), BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema)
            cleaned = pd.concat([existing, cleaned], ignore_index=True)
            cleaned = self.clean_dataframe_for_schema(cleaned, BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema)
            cleaned = cleaned.drop_duplicates(["calendar_date"], keep="last").reset_index(drop=True)
        cleaned = cleaned.sort_values(["calendar_date"]).reset_index(drop=True) if not cleaned.empty else cleaned
        BAOSTOCK_CN_TRADING_CALENDAR_DATASET.validator(cleaned)
        self.atomic_write(cleaned, BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema, destination)
        self._publish_dataset_write(BAOSTOCK_CN_TRADING_CALENDAR_DATASET.name, "*", cleaned, destination)
        return destination

    def read_baostock_cn_trading_calendar(self) -> pd.DataFrame:
        path = self.baostock_cn_trading_calendar_path()
        if not path.exists():
            return pd.DataFrame(columns=field_names(BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema))
        return self._safe_read_parquet(path)

    def write_akshare_cn_stock_valuation_eastmoney(self, code: str, df: pd.DataFrame) -> Path:
        code = _akshare_partition_code(code)
        cleaned = self.clean_dataframe_for_schema(df, AKSHARE_VALUATION_EASTMONEY_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "code", code, "Stock value file code")
            cleaned = cleaned.sort_values(["code", "date"]).reset_index(drop=True)
        AKSHARE_VALUATION_EASTMONEY_DATASET.validator(cleaned)
        destination = self.akshare_cn_stock_valuation_eastmoney_path(code)
        self.atomic_write(cleaned, AKSHARE_VALUATION_EASTMONEY_DATASET.schema, destination)
        self._publish_dataset_write(AKSHARE_VALUATION_EASTMONEY_DATASET.name, code, cleaned, destination)
        logger.info(
            "AkShare Parquet stored dataset={} code={} rows={} path={}",
            AKSHARE_VALUATION_EASTMONEY_DATASET.name,
            code,
            len(cleaned),
            destination,
        )
        return destination

    def read_akshare_cn_stock_valuation_eastmoney(self, code: str) -> pd.DataFrame:
        code = _akshare_partition_code(code)
        path = self.akshare_cn_stock_valuation_eastmoney_path(code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(AKSHARE_VALUATION_EASTMONEY_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), AKSHARE_VALUATION_EASTMONEY_DATASET.schema)

    def write_akshare_cn_stock_delist_sh(self, snapshot_date: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, AKSHARE_DELIST_SH_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "snapshot_date", snapshot_date, "SH delist snapshot date")
            cleaned = cleaned.sort_values(["market", "code"]).reset_index(drop=True)
        AKSHARE_DELIST_SH_DATASET.validator(cleaned)
        destination = self.akshare_cn_stock_delist_sh_path(snapshot_date)
        self.atomic_write(cleaned, AKSHARE_DELIST_SH_DATASET.schema, destination)
        self._publish_dataset_write(AKSHARE_DELIST_SH_DATASET.name, "*", cleaned, destination)
        return destination

    def read_akshare_cn_stock_delist_sh(self, snapshot_date: str) -> pd.DataFrame:
        path = self.akshare_cn_stock_delist_sh_path(snapshot_date)
        if not path.exists():
            return pd.DataFrame(columns=field_names(AKSHARE_DELIST_SH_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), AKSHARE_DELIST_SH_DATASET.schema)

    def read_latest_akshare_cn_stock_delist_sh(self) -> pd.DataFrame:
        latest = self._latest_partition_value(AKSHARE_DELIST_SH_DATASET.name, "snapshot_date")
        if latest is None:
            return pd.DataFrame(columns=field_names(AKSHARE_DELIST_SH_DATASET.schema))
        return self.read_akshare_cn_stock_delist_sh(latest)

    def write_akshare_cn_stock_delist_sz(self, snapshot_date: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, AKSHARE_DELIST_SZ_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "snapshot_date", snapshot_date, "SZ delist snapshot date")
            cleaned = cleaned.sort_values(["market", "code"]).reset_index(drop=True)
        AKSHARE_DELIST_SZ_DATASET.validator(cleaned)
        destination = self.akshare_cn_stock_delist_sz_path(snapshot_date)
        self.atomic_write(cleaned, AKSHARE_DELIST_SZ_DATASET.schema, destination)
        self._publish_dataset_write(AKSHARE_DELIST_SZ_DATASET.name, "*", cleaned, destination)
        return destination

    def read_akshare_cn_stock_delist_sz(self, snapshot_date: str) -> pd.DataFrame:
        path = self.akshare_cn_stock_delist_sz_path(snapshot_date)
        if not path.exists():
            return pd.DataFrame(columns=field_names(AKSHARE_DELIST_SZ_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), AKSHARE_DELIST_SZ_DATASET.schema)

    def read_latest_akshare_cn_stock_delist_sz(self) -> pd.DataFrame:
        latest = self._latest_partition_value(AKSHARE_DELIST_SZ_DATASET.name, "snapshot_date")
        if latest is None:
            return pd.DataFrame(columns=field_names(AKSHARE_DELIST_SZ_DATASET.schema))
        return self.read_akshare_cn_stock_delist_sz(latest)

    def write_stock_spot_quote_eastmoney(self, trade_date: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "trade_date", trade_date, "akshare_cn_stock_spot_quote_eastmoney trade_date")
            cleaned = cleaned.sort_values(["code"]).reset_index(drop=True)
        AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.validator(cleaned)
        destination = self.akshare_cn_stock_spot_quote_eastmoney_path(trade_date)
        self.atomic_write(cleaned, AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.schema, destination)
        self._publish_dataset_write(AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.name, "*", cleaned, destination)
        return destination

    def read_stock_spot_quote_eastmoney(self, trade_date: str) -> pd.DataFrame:
        path = self.akshare_cn_stock_spot_quote_eastmoney_path(trade_date)
        if not path.exists():
            return pd.DataFrame(columns=field_names(AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.schema)

    def read_latest_stock_spot_quote_eastmoney(self) -> pd.DataFrame:
        latest = self._latest_partition_value(AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.name, "trade_date")
        if latest is None:
            return pd.DataFrame(columns=field_names(AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET.schema))
        return self.read_stock_spot_quote_eastmoney(latest)

    def write_stock_spot_quote_sina(self, trade_date: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, AKSHARE_SPOT_QUOTE_SINA_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "trade_date", trade_date, "akshare_cn_stock_spot_quote_sina trade_date")
            cleaned = cleaned.sort_values(["code"]).reset_index(drop=True)
        AKSHARE_SPOT_QUOTE_SINA_DATASET.validator(cleaned)
        destination = self.akshare_cn_stock_spot_quote_sina_path(trade_date)
        self.atomic_write(cleaned, AKSHARE_SPOT_QUOTE_SINA_DATASET.schema, destination)
        self._publish_dataset_write(AKSHARE_SPOT_QUOTE_SINA_DATASET.name, "*", cleaned, destination)
        return destination

    def read_stock_spot_quote_sina(self, trade_date: str) -> pd.DataFrame:
        path = self.akshare_cn_stock_spot_quote_sina_path(trade_date)
        if not path.exists():
            return pd.DataFrame(columns=field_names(AKSHARE_SPOT_QUOTE_SINA_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), AKSHARE_SPOT_QUOTE_SINA_DATASET.schema)

    def write_akshare_daily_bars(self, adjustment: str, code: str, df: pd.DataFrame) -> Path:
        code = _akshare_partition_code(code)
        dataset = akshare_daily_bar_dataset_id(adjustment)
        definition = dataset_definition(dataset)
        cleaned = self.clean_dataframe_for_schema(df, definition.schema)
        if not cleaned.empty:
            normalized_adjustment = akshare_daily_bar_dataset_id(adjustment).rsplit("_", 1)[-1]
            self._require_partition_value(cleaned, "code", code, "AkShare daily bar file code")
            self._require_partition_value(
                cleaned,
                "adjustment",
                normalized_adjustment,
                "AkShare daily bar adjustment",
            )
            cleaned = cleaned.sort_values(["code", "adjustment", "date"]).reset_index(drop=True)
        definition.validator(cleaned)
        destination = self.akshare_daily_bar_path(adjustment, code)
        self.atomic_write(cleaned, definition.schema, destination)
        self._publish_dataset_write(dataset, code, cleaned, destination)
        return destination

    def upsert_akshare_daily_bars(self, adjustment: str, code: str, df: pd.DataFrame) -> Path:
        code = _akshare_partition_code(code)
        dataset = akshare_daily_bar_dataset_id(adjustment)
        definition = dataset_definition(dataset)
        existing = self.read_akshare_daily_bars(adjustment, code)
        fresh = self.clean_dataframe_for_schema(df, definition.schema)
        combined = pd.concat([existing, fresh], ignore_index=True)
        combined = self.clean_dataframe_for_schema(combined, definition.schema)
        if not combined.empty:
            combined["_date_key"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            combined = (
                combined.drop_duplicates(["code", "_date_key", "adjustment"], keep="last")
                .drop(columns=["_date_key"])
                .sort_values(["code", "adjustment", "date"])
                .reset_index(drop=True)
            )
        return self.write_akshare_daily_bars(adjustment, code, combined)

    def append_akshare_daily_bar_batch(
        self, adjustment: str, daily_bar_rows: pd.DataFrame, skip_existing: bool = True
    ) -> dict[str, int]:
        """Batch append spot hist rows with optimized incremental update.
        
        This method is optimized for SPOT incremental updates where we only
        need to append a few new rows per stock. It avoids reading the entire
        history for each stock by checking if the new data already exists.
        
        Args:
            adjustment: Adjustment type (unadjusted, qfq, hfq)
            daily_bar_rows: DataFrame containing hist data for multiple stocks
            skip_existing: If True, skip stocks whose latest data already exists
            
        Returns:
            Dictionary with statistics: {'updated': count, 'skipped': count}
        """
        if daily_bar_rows.empty:
            return {'updated': 0, 'skipped': 0}
        
        dataset = akshare_daily_bar_dataset_id(adjustment)
        definition = dataset_definition(dataset)
        stats = {'updated': 0, 'skipped': 0}
        
        for code, group in daily_bar_rows.groupby("code", dropna=False, sort=False):
            if pd.isna(code) or str(code).strip() == "":
                continue
            
            code_str = _akshare_partition_code(str(code))
            daily_bar_path = self.akshare_daily_bar_path(adjustment, code_str)
            
            if skip_existing and daily_bar_path.exists():
                try:
                    existing_tail = pd.read_parquet(
                        daily_bar_path, 
                        columns=['date'],
                    ).tail(10)
                    
                    new_dates = set(
                        pd.to_datetime(group['date'], errors='coerce')
                        .dt.strftime('%Y-%m-%d')
                        .dropna()
                    )
                    existing_dates = set(
                        pd.to_datetime(existing_tail['date'], errors='coerce')
                        .dt.strftime('%Y-%m-%d')
                        .dropna()
                    )
                    
                    if new_dates.issubset(existing_dates):
                        stats['skipped'] += 1
                        continue
                except Exception:
                    pass
            
            self.upsert_akshare_daily_bars(adjustment, code_str, group.reset_index(drop=True))
            stats['updated'] += 1
        
        return stats

    def read_akshare_daily_bars(self, adjustment: str, code: str) -> pd.DataFrame:
        code = _akshare_partition_code(code)
        dataset = akshare_daily_bar_dataset_id(adjustment)
        definition = dataset_definition(dataset)
        path = self.akshare_daily_bar_path(adjustment, code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(definition.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), definition.schema)

    def _latest_partition_value(self, dataset: str, partition_column: str) -> str | None:
        dataset_dir = self.parquet_dir / dataset
        if not dataset_dir.exists():
            return None
        prefix = f"{partition_column}="
        values = sorted(
            item.name[len(prefix):]
            for item in dataset_dir.iterdir()
            if item.is_dir() and item.name.startswith(prefix) and (item / "data.parquet").exists()
        )
        return values[-1] if values else None

    def append_pipeline_runs(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("pipeline_runs")
        self._metadata_store.append_pipeline_runs(df)
        return path

    def upsert_dataset_update_status(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("dataset_update_status")
        self._metadata_store.upsert_dataset_update_status(df)
        self._refresh_registry_inventory_from_status(df.to_dict("records"))
        return path

    def read_pipeline_runs(self) -> pd.DataFrame:
        return self._metadata_store.read_pipeline_runs()

    def read_dataset_update_status(self) -> pd.DataFrame:
        return self._metadata_store.read_dataset_update_status()

    def read_pipeline_checkpoints(self) -> pd.DataFrame:
        return self._metadata_store.read_pipeline_checkpoints()

    def upsert_pipeline_checkpoints(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("pipeline_checkpoints")
        self._metadata_store.upsert_pipeline_checkpoints(df)
        return path

    def persist_update_metadata(
        self,
        run_rows: list[dict[str, object]],
        status_rows: list[dict[str, object]],
        checkpoint_rows: list[dict[str, object]],
    ) -> None:
        """Persist update metadata with one write per metadata table."""

        self._metadata_store.persist_update_metadata(run_rows, status_rows, checkpoint_rows)
        self._refresh_registry_inventory_from_status(status_rows)

    def pipeline_checkpoint_succeeded(
        self,
        pipeline: str,
        dataset: str,
        code: str,
        start_date: str,
        end_date: str,
        output_path: str | Path,
    ) -> bool:
        if not Path(output_path).exists():
            return False
        checkpoints = self.read_pipeline_checkpoints()
        if checkpoints.empty:
            return False

        work = checkpoints.copy()
        start_keys = pd.to_datetime(work["start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        end_keys = pd.to_datetime(work["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        matches = work.loc[
            (work["pipeline"].astype("string") == pipeline)
            & (work["dataset"].astype("string") == dataset)
            & (work["code"].astype("string") == code)
            & (start_keys == start_date)
            & (end_keys == end_date)
        ]
        if matches.empty:
            return False
        latest = matches.sort_values("updated_at").iloc[-1]
        return str(latest["status"]) == "success"

    def checkpoint_succeeded_for_date(
        self,
        pipeline: str,
        dataset: str,
        code: str,
        end_date: str,
        output_path: str | Path,
    ) -> bool:
        if not Path(output_path).exists():
            return False
        checkpoints = self.read_pipeline_checkpoints()
        if checkpoints.empty:
            return False

        work = checkpoints.copy()
        end_keys = pd.to_datetime(work["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        matches = work.loc[
            (work["pipeline"].astype("string") == pipeline)
            & (work["dataset"].astype("string") == dataset)
            & (work["code"].astype("string") == code)
            & (end_keys == end_date)
        ]
        if matches.empty:
            return False
        latest = matches.sort_values("updated_at").iloc[-1]
        return str(latest["status"]) == "success"

    def initialize_empty_metadata(self) -> None:
        self._metadata_store.initialize()

    def _publish_dataset_write(
        self,
        dataset: str,
        code: str,
        df: pd.DataFrame,
        destination: Path,
    ) -> None:
        try:
            DataRegistry(root=self.root).publish_dataframe_write(
                dataset,
                code,
                df,
                destination,
                refresh_inventory=len(df) <= 1000,
            )
        except Exception as exc:
            logger.warning("Failed to publish registry event dataset={} path={}: {}", dataset, destination, exc)

    def _refresh_registry_inventory_from_status(self, status_rows: list[dict[str, object]]) -> None:
        if not status_rows:
            return
        dataset_ids = sorted({str(row.get("dataset")) for row in status_rows if row.get("dataset")})
        if not dataset_ids:
            return
        try:
            DataRegistry(root=self.root).refresh_inventory(dataset_ids, status_rows=status_rows)
        except Exception as exc:
            logger.warning("Failed to refresh registry inventory for datasets={}: {}", dataset_ids, exc)
