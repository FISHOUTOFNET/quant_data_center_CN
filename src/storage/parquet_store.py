"""Parquet storage with schema coercion and atomic replacement."""

from __future__ import annotations

import os
import re
import time
import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Literal, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.dataset_catalog import (
    DATASET_CATALOG,
    DatasetDefinition,
    DatasetWriteMode,
    akshare_a_stock_definitions,
    daily_bar_definitions,
    dataset_definition,
    normalize_adjustment,
)
from src.storage.metadata_store import DuckDBMetadataStore
from src.storage.partition_manifest import dataset_partition_manifest_row
from src.storage.schema import field_names
from src.utils import paths
from src.utils.logging import logger
from src.utils.run_context import pipeline_log_values

NULL_LIKE_VALUES = {"": pd.NA, "None": pd.NA, "none": pd.NA, "NaN": pd.NA, "nan": pd.NA}
PARQUET_READ_MAX_RETRIES = 3
PARQUET_READ_RETRY_DELAY = 0.1
PARQUET_WRITE_MAX_RETRIES = 3
PARQUET_WRITE_RETRY_DELAY = 0.1
AKSHARE_CODE_PATTERN = re.compile(r"^\d{6}$")
Partition = Mapping[str, object] | None
WriteMode = DatasetWriteMode | Literal["replace", "merge", "upsert"]
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


@dataclass(frozen=True)
class DatasetWriteResult:
    paths: tuple[Path, ...]
    row_count: int
    updated_partitions: int
    skipped_partitions: int

    @property
    def primary_path(self) -> Path:
        if len(self.paths) != 1:
            raise ValueError(f"Expected exactly one dataset path, got {len(self.paths)}")
        return self.paths[0]


def _akshare_partition_code(code: object) -> str:
    if pd.isna(cast(Any, code)):
        raise ValueError("AkShare partition code must be a 6-digit string")
    value = str(code).strip()
    if not AKSHARE_CODE_PATTERN.fullmatch(value):
        raise ValueError(f"AkShare partition code must be 6 digits, got: {value!r}")
    return value


def _normalize_optional_adjustment(value: object) -> object:
    if pd.isna(cast(Any, value)):
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
        self._dirty_datasets: set[str] = set()
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
                    delay = PARQUET_READ_RETRY_DELAY * (2**attempt)
                    logger.warning(
                        "Permission denied reading {}, retrying in {:.3f}s (attempt {}/{})",
                        path,
                        delay,
                        attempt + 1,
                        PARQUET_READ_MAX_RETRIES,
                    )
                    time.sleep(delay)
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Failed to read Parquet file {path} without a captured error")

    def ensure_layout(self) -> None:
        daily_bar_dirs = [self.parquet_dir / definition.id for definition in daily_bar_definitions()]
        akshare_a_stock_dirs = [self.parquet_dir / definition.id for definition in akshare_a_stock_definitions()]
        catalog_dirs = [self.parquet_dir / definition.id for definition in DATASET_CATALOG.values()]
        for directory in [
            *catalog_dirs,
            *daily_bar_dirs,
            *akshare_a_stock_dirs,
            self.metadata_dir,
            self.root / "data" / "duckdb",
            self.root / "logs",
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        (self.root / "data" / "registry").mkdir(parents=True, exist_ok=True)

    def dataset_path(self, dataset_id: str, partition: Partition = None) -> Path:
        definition = dataset_definition(dataset_id)
        partition_value = self._partition_value(definition, partition)
        dataset_dir = self.parquet_dir / definition.id
        if definition.partition_column is None:
            return dataset_dir / "data.parquet"
        return dataset_dir / f"{definition.partition_column}={partition_value}" / "data.parquet"

    def dataset_exists(self, dataset_id: str, partition: Partition = None) -> bool:
        return self.dataset_path(dataset_id, partition).exists()

    def empty_dataset_frame(self, dataset_id: str) -> pd.DataFrame:
        definition = dataset_definition(dataset_id)
        return pd.DataFrame(columns=field_names(definition.schema))

    def read_dataset(self, dataset_id: str, partition: Partition = None) -> pd.DataFrame:
        definition = dataset_definition(dataset_id)
        path = self.dataset_path(dataset_id, partition)
        if not path.exists():
            return self.empty_dataset_frame(dataset_id)
        return self.clean_dataframe_for_schema(self._safe_read_parquet(path), definition.schema)

    def latest_dataset_partition(self, dataset_id: str) -> str | None:
        definition = dataset_definition(dataset_id)
        if definition.partition_column is None:
            return None
        dataset_dir = self.parquet_dir / definition.id
        if not dataset_dir.exists():
            return None
        prefix = f"{definition.partition_column}="
        values = sorted(
            item.name[len(prefix) :]
            for item in dataset_dir.iterdir()
            if item.is_dir() and item.name.startswith(prefix) and (item / "data.parquet").exists()
        )
        return values[-1] if values else None

    def list_dataset_partitions(self, dataset_id: str) -> tuple[str, ...]:
        definition = dataset_definition(dataset_id)
        if definition.partition_column is None:
            return ()
        dataset_dir = self.parquet_dir / definition.id
        if not dataset_dir.exists():
            return ()
        prefix = f"{definition.partition_column}="
        return tuple(
            sorted(
                item.name[len(prefix) :]
                for item in dataset_dir.iterdir()
                if item.is_dir() and item.name.startswith(prefix) and (item / "data.parquet").exists()
            )
        )

    def read_latest_dataset(self, dataset_id: str) -> pd.DataFrame:
        definition = dataset_definition(dataset_id)
        if definition.partition_column is None:
            return self.read_dataset(dataset_id)
        latest = self.latest_dataset_partition(dataset_id)
        if latest is None:
            return self.empty_dataset_frame(dataset_id)
        return self.read_dataset(dataset_id, {definition.partition_column: latest})

    def metadata_path(self, name: str) -> Path:
        return self.metadata_dir / f"{name}.parquet"

    def close(self) -> None:
        self._metadata_store.close()

    def dirty_datasets(self) -> tuple[str, ...]:
        return tuple(sorted(self._dirty_datasets))

    def _mark_dataset_dirty(self, dataset: str) -> None:
        self._dirty_datasets.add(dataset)

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
        return series.mask(series.isin(NULL_LIKE_VALUES), pd.NA)

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
                    delay = PARQUET_WRITE_RETRY_DELAY * (2**attempt)
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

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Failed to write Parquet file {destination} without a captured error")

    def prepare_dataset_frame(
        self,
        dataset_id: str,
        df: pd.DataFrame,
        partition: Partition = None,
        validate: bool = True,
    ) -> pd.DataFrame:
        definition = dataset_definition(dataset_id)
        cleaned = self.clean_dataframe_for_schema(df, definition.schema)
        if not cleaned.empty:
            self._apply_fixed_column_values(definition, cleaned)
            if definition.partition_column is not None and partition is not None:
                partition_value = self._partition_value(definition, partition)
                self._require_partition_value(
                    cleaned,
                    definition.partition_column,
                    partition_value,
                    f"{definition.id} partition {definition.partition_column}",
                )
            cleaned = self._sort_dataset_frame(definition, cleaned)
        if validate:
            definition.validator(cleaned)
        return cleaned

    def write_dataset(
        self,
        dataset_id: str,
        df: pd.DataFrame,
        partition: Partition = None,
        mode: WriteMode | None = None,
        skip_existing: bool = False,
    ) -> DatasetWriteResult:
        definition = dataset_definition(dataset_id)
        write_mode = mode or definition.default_write_mode
        if write_mode not in {"replace", "merge", "upsert"}:
            raise ValueError(f"Unsupported write mode for {dataset_id}: {write_mode}")
        if skip_existing and write_mode != "upsert":
            raise ValueError("skip_existing is only supported for upsert writes")
        if write_mode == "upsert":
            return self._write_dataset_upsert(definition, df, partition, skip_existing)
        path = self.dataset_path(definition.id, partition)
        incoming = self.prepare_dataset_frame(definition.id, df, partition)
        if write_mode == "merge" and path.exists():
            existing = self.read_dataset(definition.id, partition)
            incoming = self.prepare_dataset_frame(
                definition.id,
                pd.concat([existing, incoming], ignore_index=True),
                partition,
                validate=False,
            )
            incoming = self._deduplicate_dataset_frame(definition, incoming)
            incoming = self._sort_dataset_frame(definition, incoming)
            definition.validator(incoming)
        self._write_prepared_dataset(definition, incoming, path, partition)
        return DatasetWriteResult(
            paths=(path,),
            row_count=len(incoming),
            updated_partitions=1,
            skipped_partitions=0,
        )

    def _write_dataset_upsert(
        self,
        definition: DatasetDefinition,
        df: pd.DataFrame,
        partition: Partition,
        skip_existing: bool,
    ) -> DatasetWriteResult:
        if definition.partition_column is None:
            path = self.dataset_path(definition.id, partition)
            return self._write_dataset_upsert_partition(definition, df, partition, path, skip_existing)
        if partition is not None:
            path = self.dataset_path(definition.id, partition)
            return self._write_dataset_upsert_partition(definition, df, partition, path, skip_existing)
        if df.empty:
            return DatasetWriteResult(paths=(), row_count=0, updated_partitions=0, skipped_partitions=0)

        cleaned = self.prepare_dataset_frame(definition.id, df)
        paths: list[Path] = []
        row_count = 0
        updated = 0
        skipped = 0
        for raw_partition_value, group in cleaned.groupby(definition.partition_column, dropna=False, sort=False):
            if pd.isna(raw_partition_value) or str(raw_partition_value).strip() == "":
                raise ValueError(f"{definition.id} requires partition {definition.partition_column}")
            partition_value = self._normalize_partition_value(
                definition, definition.partition_column, raw_partition_value
            )
            result = self._write_dataset_upsert_partition(
                definition,
                group.reset_index(drop=True),
                {definition.partition_column: partition_value},
                self.dataset_path(definition.id, {definition.partition_column: partition_value}),
                skip_existing,
            )
            paths.extend(result.paths)
            row_count += result.row_count
            updated += result.updated_partitions
            skipped += result.skipped_partitions
        return DatasetWriteResult(
            paths=tuple(paths),
            row_count=row_count,
            updated_partitions=updated,
            skipped_partitions=skipped,
        )

    def _write_dataset_upsert_partition(
        self,
        definition: DatasetDefinition,
        df: pd.DataFrame,
        partition: Partition,
        path: Path,
        skip_existing: bool,
    ) -> DatasetWriteResult:
        fresh = self.prepare_dataset_frame(definition.id, df, partition)
        existing = (
            self.read_dataset(definition.id, partition) if path.exists() else self.empty_dataset_frame(definition.id)
        )
        if skip_existing and self._incoming_keys_exist(definition, existing, fresh):
            return DatasetWriteResult(paths=(path,), row_count=0, updated_partitions=0, skipped_partitions=1)
        combined = self.prepare_dataset_frame(
            definition.id,
            pd.concat([existing, fresh], ignore_index=True),
            partition,
            validate=False,
        )
        combined = self._deduplicate_dataset_frame(definition, combined)
        combined = self._sort_dataset_frame(definition, combined)
        definition.validator(combined)
        self._write_prepared_dataset(definition, combined, path, partition)
        return DatasetWriteResult(
            paths=(path,),
            row_count=len(fresh),
            updated_partitions=1,
            skipped_partitions=0,
        )

    def _write_prepared_dataset(
        self,
        definition: DatasetDefinition,
        df: pd.DataFrame,
        path: Path,
        partition: Partition,
    ) -> None:
        self._cleanup_legacy_partitions(definition)
        self.atomic_write(df, definition.schema, path)
        self._upsert_partition_manifest_after_write(definition, df, path, partition)
        self._mark_dataset_dirty(definition.id)
        run_id, pid, thread = pipeline_log_values()
        logger.info(
            "Dataset Parquet stored run_id={} pid={} thread={} dataset={} rows={} path={}",
            run_id,
            pid,
            thread,
            definition.id,
            len(df),
            path,
        )

    def _upsert_partition_manifest_after_write(
        self,
        definition: DatasetDefinition,
        df: pd.DataFrame,
        path: Path,
        partition: Partition,
        *,
        source_signature_value: str = "",
        master_row_hash_value: str = "",
    ) -> None:
        if self._is_staging_path(path):
            return
        partition_column = definition.partition_column or ""
        partition_value = self._partition_value(definition, partition)
        run_id, pid, thread = pipeline_log_values()
        writer_pid = int(pid) if isinstance(pid, int | str) else 0
        row = dataset_partition_manifest_row(
            dataset=definition.id,
            partition_column=partition_column,
            partition_value=partition_value,
            output_path=path,
            root=self.root,
            df=df,
            schema=definition.schema,
            source_signature_value=source_signature_value,
            master_row_hash_value=master_row_hash_value,
            run_id=str(run_id),
            writer_pid=writer_pid,
            writer_thread=str(thread),
        )
        self.upsert_dataset_partition_manifest(pd.DataFrame([row]))

    def upsert_written_dataset_partition_manifest(
        self,
        dataset_id: str,
        df: pd.DataFrame,
        partition: Partition = None,
        *,
        source_signature_value: str = "",
        master_row_hash_value: str = "",
    ) -> Path:
        definition = dataset_definition(dataset_id)
        path = self.dataset_path(definition.id, partition)
        if not path.exists():
            raise FileNotFoundError(f"Cannot write manifest for missing partition: {path}")
        cleaned = self.prepare_dataset_frame(definition.id, df, partition)
        self._upsert_partition_manifest_after_write(
            definition,
            cleaned,
            path,
            partition,
            source_signature_value=source_signature_value,
            master_row_hash_value=master_row_hash_value,
        )
        return self.metadata_path("dataset_partition_manifest")

    def _is_staging_path(self, path: Path) -> bool:
        staging_dir = (self.root / "data" / "parquet" / ".staging").resolve()
        candidates = (self.parquet_dir.resolve(), path.resolve())
        return any(".staging" in candidate.parts for candidate in candidates) or any(
            _is_relative_to(candidate, staging_dir) for candidate in candidates
        )

    def _partition_value(self, definition: DatasetDefinition, partition: Partition) -> str:
        if definition.partition_column is None:
            if partition:
                unexpected = ", ".join(sorted(str(key) for key in partition))
                raise ValueError(f"{definition.id} unexpected partition fields: {unexpected}")
            return ""
        if partition is None:
            raise ValueError(f"{definition.id} requires partition {definition.partition_column}")
        if definition.partition_column not in partition:
            unexpected = ", ".join(sorted(str(key) for key in partition))
            raise ValueError(f"{definition.id} unexpected partition fields: {unexpected}")
        unexpected = sorted(str(key) for key in partition if key != definition.partition_column)
        if unexpected:
            raise ValueError(f"{definition.id} unexpected partition fields: {', '.join(unexpected)}")
        return self._normalize_partition_value(
            definition, definition.partition_column, partition[definition.partition_column]
        )

    def _normalize_partition_value(self, definition: DatasetDefinition, column: str, value: object) -> str:
        if column == "code" and definition.code_format == "six_digit":
            return _akshare_partition_code(value)
        if pd.isna(cast(Any, value)):
            raise ValueError(f"{definition.id} requires partition {column}")
        normalized = str(value).strip()
        if normalized == "":
            raise ValueError(f"{definition.id} requires partition {column}")
        return normalized

    def _apply_fixed_column_values(self, definition: DatasetDefinition, df: pd.DataFrame) -> None:
        for column, expected in definition.fixed_column_values:
            values = df[column].astype("string").str.strip()
            df[column] = values.mask(values.isna() | (values == ""), expected).astype("string")
            actual = set(df[column].dropna().astype(str))
            if actual != {expected}:
                raise ValueError(f"{definition.id} fixed {column} mismatch: {sorted(actual)}")

    def _deduplicate_dataset_frame(self, definition: DatasetDefinition, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or not definition.unique_columns:
            return df
        return df.drop_duplicates(list(definition.unique_columns), keep="last").reset_index(drop=True)

    def _sort_dataset_frame(self, definition: DatasetDefinition, df: pd.DataFrame) -> pd.DataFrame:
        sort_columns = [column for column in definition.sort_columns if column in df.columns]
        if df.empty or not sort_columns:
            return df.reset_index(drop=True)
        return df.sort_values(sort_columns).reset_index(drop=True)

    def _incoming_keys_exist(self, definition: DatasetDefinition, existing: pd.DataFrame, fresh: pd.DataFrame) -> bool:
        if existing.empty or fresh.empty or not definition.unique_columns:
            return False
        unique_columns = list(definition.unique_columns)
        existing_keys = set(map(tuple, existing[unique_columns].astype("string").to_numpy()))
        fresh_keys = set(map(tuple, fresh[unique_columns].astype("string").to_numpy()))
        return bool(fresh_keys) and fresh_keys.issubset(existing_keys)

    def _cleanup_legacy_partitions(self, definition: DatasetDefinition) -> None:
        if not definition.legacy_partition_prefixes:
            return
        dataset_dir = self.parquet_dir / definition.id
        for prefix in definition.legacy_partition_prefixes:
            for old_partition in dataset_dir.glob(f"{prefix}*"):
                if old_partition.is_dir():
                    import shutil

                    shutil.rmtree(old_partition, ignore_errors=True)
                    logger.info("Removed old {} partition: {}", definition.id, old_partition)

    def append_pipeline_runs(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("pipeline_runs")
        self._metadata_store.append_pipeline_runs(df)
        return path

    def upsert_dataset_update_status(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("dataset_update_status")
        self._metadata_store.upsert_dataset_update_status(df)
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

    def upsert_dataset_partition_manifest(self, df: pd.DataFrame) -> Path:
        path = self.metadata_path("dataset_partition_manifest")
        self._metadata_store.upsert_dataset_partition_manifest(df)
        return path

    def read_dataset_partition_manifest(self, dataset: str | None = None) -> pd.DataFrame:
        return self._metadata_store.read_dataset_partition_manifest(dataset)

    def delete_dataset_partition_manifest(self, dataset: str, partition_column: str, partition_value: str) -> None:
        self._metadata_store.delete_dataset_partition_manifest(dataset, partition_column, partition_value)

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


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True
