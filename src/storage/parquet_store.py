"""Parquet storage with schema coercion and atomic replacement."""

from __future__ import annotations

import os
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
    ADJUST_FACTOR_DATASET,
    CALENDAR_DATASET,
    STOCK_BASIC_DATASET,
    STOCK_INSTITUTE_HOLD_DATASET,
    STOCK_VALUE_EM_DATASET,
    daily_k_definition,
    daily_k_definitions,
)
from src.storage.schema import field_names
from src.storage.metadata_store import DuckDBMetadataStore
from src.utils import paths
from src.utils.logging import logger


NULL_LIKE_VALUES = {"": pd.NA, "None": pd.NA, "none": pd.NA, "NaN": pd.NA, "nan": pd.NA}
PARQUET_READ_MAX_RETRIES = 3
PARQUET_READ_RETRY_DELAY = 0.1
PARQUET_WRITE_MAX_RETRIES = 3
PARQUET_WRITE_RETRY_DELAY = 0.1


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
            parquet_metadata_dir=self.metadata_dir,
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
        daily_k_dirs = [self.parquet_dir / definition.name for definition in daily_k_definitions()]
        for directory in [
            *daily_k_dirs,
            self.parquet_dir / ADJUST_FACTOR_DATASET.name,
            self.parquet_dir / STOCK_INSTITUTE_HOLD_DATASET.name,
            self.parquet_dir / STOCK_VALUE_EM_DATASET.name,
            self.parquet_dir / "stock_basic",
            self.parquet_dir / "calendar",
            self.metadata_dir,
            self.root / "data" / "duckdb",
            self.root / "data" / "raw",
            self.root / "data" / "raw" / "akshare",
            self.root / "data" / "raw" / "akshare" / "manifest",
            self.root / "logs",
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    def daily_k_path(self, dataset: str, code: str) -> Path:
        return self.parquet_dir / dataset / f"code={code}" / "data.parquet"

    def adjust_factor_path(self, code: str) -> Path:
        return self.parquet_dir / ADJUST_FACTOR_DATASET.name / f"code={code}" / "data.parquet"

    def stock_basic_path(self) -> Path:
        return self.parquet_dir / "stock_basic" / "data.parquet"

    def calendar_path(self) -> Path:
        return self.parquet_dir / "calendar" / "data.parquet"

    def stock_institute_hold_path(self, report_period: str) -> Path:
        return self.parquet_dir / STOCK_INSTITUTE_HOLD_DATASET.name / f"report_period={report_period}" / "data.parquet"

    def stock_value_em_path(self, code: str) -> Path:
        return self.parquet_dir / STOCK_VALUE_EM_DATASET.name / f"code={code}" / "data.parquet"

    def akshare_manifest_path(self) -> Path:
        return self.root / "data" / "raw" / "akshare" / "manifest" / "fetch_runs.jsonl"

    def metadata_path(self, name: str) -> Path:
        return self.metadata_dir / f"{name}.parquet"

    def close(self) -> None:
        self._metadata_store.close()

    def clean_dataframe_for_schema(self, df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        """Return a dataframe with exactly schema columns and compatible values.

        Baostock returns every field as text. Empty strings in numeric columns
        become NaN/null, and empty strings in date columns become NULL.
        """

        cleaned = pd.DataFrame(index=df.index)
        for field in schema:
            if field.name in df.columns:
                series = df[field.name]
            else:
                series = pd.Series(pd.NA, index=df.index, name=field.name)
            cleaned[field.name] = self._coerce_series(series, field.type)
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

    def write_daily_k(self, dataset: str, code: str, df: pd.DataFrame) -> Path:
        definition = daily_k_definition(dataset)
        cleaned = self.clean_dataframe_for_schema(df, definition.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "code", code, "Daily file code")
            cleaned = cleaned.sort_values(["code", "date"]).reset_index(drop=True)
        definition.validator(cleaned)
        destination = self.daily_k_path(dataset, code)
        self.atomic_write(cleaned, definition.schema, destination)
        logger.info(
            "Daily Parquet stored dataset={} code={} rows={} path={}",
            dataset,
            code,
            len(cleaned),
            destination,
        )
        return destination

    def read_daily_k(self, dataset: str, code: str) -> pd.DataFrame:
        definition = daily_k_definition(dataset)
        path = self.daily_k_path(dataset, code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(definition.schema))
        return self._safe_read_parquet(path)

    def write_adjust_factor(self, code: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_adjust_factor_frame(code, df)
        destination = self.adjust_factor_path(code)
        self.atomic_write(cleaned, ADJUST_FACTOR_DATASET.schema, destination)
        return destination

    def clean_adjust_factor_frame(self, code: str, df: pd.DataFrame) -> pd.DataFrame:
        cleaned = self.clean_dataframe_for_schema(df, ADJUST_FACTOR_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "code", code, "Adjust factor file code")
            cleaned = cleaned.sort_values(["code", "dividOperateDate"]).reset_index(drop=True)
        ADJUST_FACTOR_DATASET.validator(cleaned)
        return cleaned

    def read_adjust_factor(self, code: str) -> pd.DataFrame:
        path = self.adjust_factor_path(code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(ADJUST_FACTOR_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), ADJUST_FACTOR_DATASET.schema)

    def read_stock_basic(self) -> pd.DataFrame:
        path = self.stock_basic_path()
        if not path.exists():
            return pd.DataFrame(columns=field_names(STOCK_BASIC_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), STOCK_BASIC_DATASET.schema)

    def stock_basic_codes(self, mode: str = "all") -> list[str]:
        df = self.read_stock_basic()
        if df.empty:
            return []
        if mode == "all":
            work = df
        elif mode == "active":
            status = df["status"].astype("string").str.strip()
            stock_type = df["type"].astype("string").str.strip()
            work = df.loc[(stock_type == "1") & (status == "1")]
        else:
            raise ValueError(f"Unsupported stock_basic code mode: {mode}")

        codes = work["code"].astype("string").str.strip()
        codes = codes.loc[codes.notna() & (codes != "")]
        return list(dict.fromkeys(codes.astype(str).tolist()))

    def write_stock_basic(self, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, STOCK_BASIC_DATASET.schema)
        cleaned = cleaned.sort_values(["code"]).reset_index(drop=True) if not cleaned.empty else cleaned
        STOCK_BASIC_DATASET.validator(cleaned)
        destination = self.stock_basic_path()
        dataset_dir = self.parquet_dir / "stock_basic"
        for old_partition in dataset_dir.glob("snapshot_date=*"):
            if old_partition.is_dir():
                import shutil
                shutil.rmtree(old_partition, ignore_errors=True)
                logger.info("Removed old stock_basic partition: {}", old_partition)
        self.atomic_write(cleaned, STOCK_BASIC_DATASET.schema, destination)
        return destination

    def write_calendar(self, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, CALENDAR_DATASET.schema)
        destination = self.calendar_path()
        if destination.exists():
            existing = self.clean_dataframe_for_schema(self._safe_read_parquet(destination), CALENDAR_DATASET.schema)
            cleaned = pd.concat([existing, cleaned], ignore_index=True)
            cleaned = self.clean_dataframe_for_schema(cleaned, CALENDAR_DATASET.schema)
            cleaned = cleaned.drop_duplicates(["calendar_date"], keep="last").reset_index(drop=True)
        cleaned = cleaned.sort_values(["calendar_date"]).reset_index(drop=True) if not cleaned.empty else cleaned
        CALENDAR_DATASET.validator(cleaned)
        self.atomic_write(cleaned, CALENDAR_DATASET.schema, destination)
        return destination

    def read_calendar(self) -> pd.DataFrame:
        path = self.calendar_path()
        if not path.exists():
            return pd.DataFrame(columns=field_names(CALENDAR_DATASET.schema))
        return self._safe_read_parquet(path)

    def write_stock_institute_hold(self, report_period: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, STOCK_INSTITUTE_HOLD_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(
                cleaned,
                "report_period",
                report_period,
                "Institute hold file period",
            )
            cleaned = cleaned.sort_values(["report_period", "code"]).reset_index(drop=True)
        STOCK_INSTITUTE_HOLD_DATASET.validator(cleaned)
        destination = self.stock_institute_hold_path(report_period)
        self.atomic_write(cleaned, STOCK_INSTITUTE_HOLD_DATASET.schema, destination)
        logger.info(
            "AkShare Parquet stored dataset={} report_period={} rows={} path={}",
            STOCK_INSTITUTE_HOLD_DATASET.name,
            report_period,
            len(cleaned),
            destination,
        )
        return destination

    def read_stock_institute_hold(self, report_period: str) -> pd.DataFrame:
        path = self.stock_institute_hold_path(report_period)
        if not path.exists():
            return pd.DataFrame(columns=field_names(STOCK_INSTITUTE_HOLD_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), STOCK_INSTITUTE_HOLD_DATASET.schema)

    def write_stock_value_em(self, code: str, df: pd.DataFrame) -> Path:
        cleaned = self.clean_dataframe_for_schema(df, STOCK_VALUE_EM_DATASET.schema)
        if not cleaned.empty:
            self._require_partition_value(cleaned, "code", code, "Stock value file code")
            cleaned = cleaned.sort_values(["code", "date"]).reset_index(drop=True)
        STOCK_VALUE_EM_DATASET.validator(cleaned)
        destination = self.stock_value_em_path(code)
        self.atomic_write(cleaned, STOCK_VALUE_EM_DATASET.schema, destination)
        logger.info(
            "AkShare Parquet stored dataset={} code={} rows={} path={}",
            STOCK_VALUE_EM_DATASET.name,
            code,
            len(cleaned),
            destination,
        )
        return destination

    def read_stock_value_em(self, code: str) -> pd.DataFrame:
        path = self.stock_value_em_path(code)
        if not path.exists():
            return pd.DataFrame(columns=field_names(STOCK_VALUE_EM_DATASET.schema))
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), STOCK_VALUE_EM_DATASET.schema)

    def append_update_runs(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("update_runs")
        self._metadata_store.append_update_runs(df)
        return path

    def upsert_update_status(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("update_status")
        self._metadata_store.upsert_update_status(df)
        return path

    def read_update_runs(self) -> pd.DataFrame:
        return self._metadata_store.read_update_runs()

    def read_update_status(self) -> pd.DataFrame:
        return self._metadata_store.read_update_status()

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
