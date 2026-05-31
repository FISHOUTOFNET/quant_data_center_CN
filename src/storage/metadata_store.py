"""DuckDB-backed update metadata storage."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from threading import RLock
from weakref import WeakValueDictionary

import duckdb
import pandas as pd
import pyarrow as pa

from src.storage.schema import (
    DATASET_UPDATE_STATUS_SCHEMA,
    PIPELINE_CHECKPOINTS_SCHEMA,
    PIPELINE_RUNS_SCHEMA,
    field_names,
)
from src.utils import paths

CHECKPOINT_KEY_COLUMNS = ["pipeline", "dataset", "code", "start_date", "end_date"]
_DB_LOCKS: WeakValueDictionary[Path, RLock] = WeakValueDictionary()
_DB_LOCKS_GUARD = RLock()


def _lock_for(path: Path) -> RLock:
    resolved = path.resolve()
    with _DB_LOCKS_GUARD:
        lock = _DB_LOCKS.get(resolved)
        if lock is None:
            lock = RLock()
            _DB_LOCKS[resolved] = lock
        return lock


class DuckDBMetadataStore:
    """Persist update metadata in DuckDB while preserving ParquetStore APIs."""

    def __init__(
        self,
        root: Path | None = None,
        duckdb_file: Path | None = None,
    ) -> None:
        self.root = (root or paths.ROOT).resolve()
        self.duckdb_file = (duckdb_file or self.root / "data" / "duckdb" / "quant.duckdb").resolve()
        self._lock = _lock_for(self.duckdb_file)
        self._initialized = False
        self._conn: duckdb.DuckDBPyConnection | None = None

    def append_pipeline_runs(self, df: pd.DataFrame) -> None:
        cleaned = _clean_dataframe_for_schema(df, PIPELINE_RUNS_SCHEMA)
        if cleaned.empty:
            return
        with self._connection() as conn:
            self._register_and_execute(
                conn,
                cleaned,
                "INSERT INTO pipeline_runs SELECT * FROM incoming",
            )

    def upsert_dataset_update_status(self, df: pd.DataFrame) -> None:
        incoming = _clean_dataframe_for_schema(df, DATASET_UPDATE_STATUS_SCHEMA)
        if incoming.empty:
            return
        with self._connection() as conn:
            self._register_and_execute(
                conn,
                incoming,
                """
                DELETE FROM dataset_update_status
                WHERE EXISTS (
                    SELECT 1
                    FROM incoming
                    WHERE incoming.dataset = dataset_update_status.dataset
                      AND incoming.code = dataset_update_status.code
                )
                """,
                "INSERT INTO dataset_update_status SELECT * FROM incoming",
            )

    def upsert_pipeline_checkpoints(self, df: pd.DataFrame) -> None:
        incoming = _clean_dataframe_for_schema(df, PIPELINE_CHECKPOINTS_SCHEMA)
        if incoming.empty:
            return
        with self._connection() as conn:
            self._register_and_execute(
                conn,
                incoming,
                """
                DELETE FROM pipeline_checkpoints
                WHERE EXISTS (
                    SELECT 1
                    FROM incoming
                    WHERE incoming.pipeline = pipeline_checkpoints.pipeline
                      AND incoming.dataset = pipeline_checkpoints.dataset
                      AND incoming.code = pipeline_checkpoints.code
                      AND incoming.start_date = pipeline_checkpoints.start_date
                      AND incoming.end_date = pipeline_checkpoints.end_date
                )
                """,
                "INSERT INTO pipeline_checkpoints SELECT * FROM incoming",
            )

    def persist_update_metadata(
        self,
        run_rows: list[dict[str, object]],
        status_rows: list[dict[str, object]],
        checkpoint_rows: list[dict[str, object]],
    ) -> None:
        runs = _clean_dataframe_for_schema(pd.DataFrame(run_rows), PIPELINE_RUNS_SCHEMA) if run_rows else None
        statuses = (
            _clean_dataframe_for_schema(pd.DataFrame(status_rows), DATASET_UPDATE_STATUS_SCHEMA)
            if status_rows
            else None
        )
        checkpoints = (
            _clean_dataframe_for_schema(pd.DataFrame(checkpoint_rows), PIPELINE_CHECKPOINTS_SCHEMA)
            if checkpoint_rows
            else None
        )
        if _is_empty_frame(runs) and _is_empty_frame(statuses) and _is_empty_frame(checkpoints):
            return
        with self._connection() as conn:
            self._persist_update_metadata_transaction(conn, runs, statuses, checkpoints)

    def _persist_update_metadata_transaction(
        self,
        conn: duckdb.DuckDBPyConnection,
        runs: pd.DataFrame | None,
        statuses: pd.DataFrame | None,
        checkpoints: pd.DataFrame | None,
    ) -> None:
        registered: list[str] = []
        try:
            if runs is not None and not runs.empty:
                conn.register("incoming_runs", runs)
                registered.append("incoming_runs")
            if statuses is not None and not statuses.empty:
                conn.register("incoming_statuses", statuses)
                registered.append("incoming_statuses")
            if checkpoints is not None and not checkpoints.empty:
                conn.register("incoming_checkpoints", checkpoints)
                registered.append("incoming_checkpoints")

            conn.execute("BEGIN TRANSACTION")
            try:
                if runs is not None and not runs.empty:
                    conn.execute("INSERT INTO pipeline_runs SELECT * FROM incoming_runs")
                if statuses is not None and not statuses.empty:
                    conn.execute(
                        """
                        DELETE FROM dataset_update_status
                        WHERE EXISTS (
                            SELECT 1
                            FROM incoming_statuses
                            WHERE incoming_statuses.dataset = dataset_update_status.dataset
                              AND incoming_statuses.code = dataset_update_status.code
                        )
                        """
                    )
                    conn.execute("INSERT INTO dataset_update_status SELECT * FROM incoming_statuses")
                if checkpoints is not None and not checkpoints.empty:
                    conn.execute(
                        """
                        DELETE FROM pipeline_checkpoints
                        WHERE EXISTS (
                            SELECT 1
                            FROM incoming_checkpoints
                            WHERE incoming_checkpoints.pipeline = pipeline_checkpoints.pipeline
                              AND incoming_checkpoints.dataset = pipeline_checkpoints.dataset
                              AND incoming_checkpoints.code = pipeline_checkpoints.code
                              AND incoming_checkpoints.start_date = pipeline_checkpoints.start_date
                              AND incoming_checkpoints.end_date = pipeline_checkpoints.end_date
                        )
                        """
                    )
                    conn.execute("INSERT INTO pipeline_checkpoints SELECT * FROM incoming_checkpoints")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            for table_name in registered:
                conn.unregister(table_name)

    def read_pipeline_runs(self) -> pd.DataFrame:
        with self._connection() as conn:
            return _clean_dataframe_for_schema(
                conn.execute("SELECT * FROM pipeline_runs").df(),
                PIPELINE_RUNS_SCHEMA,
            )

    def read_dataset_update_status(self) -> pd.DataFrame:
        with self._connection() as conn:
            return _clean_dataframe_for_schema(
                conn.execute("SELECT * FROM dataset_update_status").df(),
                DATASET_UPDATE_STATUS_SCHEMA,
            )

    def read_pipeline_checkpoints(self) -> pd.DataFrame:
        with self._connection() as conn:
            return _clean_dataframe_for_schema(
                conn.execute("SELECT * FROM pipeline_checkpoints").df(),
                PIPELINE_CHECKPOINTS_SCHEMA,
            )

    def initialize(self) -> None:
        with self._connection():
            return

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
            self._initialized = False

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    @contextmanager
    def _connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        self.duckdb_file.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.execute("SELECT 1")
                except Exception:
                    with suppress(Exception):
                        self._conn.close()
                    self._conn = None
            if self._conn is None:
                self._conn = duckdb.connect(str(self.duckdb_file))
            self._ensure_initialized(self._conn)
            yield self._conn

    def _ensure_initialized(self, conn: duckdb.DuckDBPyConnection) -> None:
        if not self._initialized:
            self._create_tables(conn)
            self._initialized = True

    def _create_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                task_id VARCHAR,
                dataset VARCHAR,
                code VARCHAR,
                status VARCHAR,
                start_date DATE,
                end_date DATE,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                row_count BIGINT,
                error_stack VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_update_status (
                dataset VARCHAR,
                code VARCHAR,
                last_success_date DATE,
                row_count BIGINT,
                status VARCHAR,
                updated_at TIMESTAMP,
                error_stack VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
                pipeline VARCHAR,
                dataset VARCHAR,
                code VARCHAR,
                start_date DATE,
                end_date DATE,
                status VARCHAR,
                row_count BIGINT,
                output_path VARCHAR,
                updated_at TIMESTAMP,
                error_stack VARCHAR
            )
            """
        )

    def _register_and_execute(
        self,
        conn: duckdb.DuckDBPyConnection,
        incoming: pd.DataFrame,
        *sqls: str,
    ) -> None:
        conn.register("incoming", incoming)
        try:
            conn.execute("BEGIN TRANSACTION")
            try:
                for sql in sqls:
                    conn.execute(sql)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.unregister("incoming")


def _clean_dataframe_for_schema(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    cleaned = pd.DataFrame(index=df.index)
    for field in schema:
        series = df[field.name] if field.name in df.columns else pd.Series(pd.NA, index=df.index, name=field.name)
        cleaned[field.name] = _coerce_series(series, field.type)
    return cleaned.reset_index(drop=True)


def _is_empty_frame(df: pd.DataFrame | None) -> bool:
    return df is None or df.empty


def _coerce_series(series: pd.Series, arrow_type: pa.DataType) -> pd.Series:
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        dates = pd.to_datetime(_replace_null_like(series), errors="coerce")
        return dates.dt.date.where(dates.notna(), None)
    if pa.types.is_timestamp(arrow_type):
        return pd.to_datetime(_replace_null_like(series), errors="coerce").dt.floor("ms")
    if pa.types.is_integer(arrow_type):
        return pd.to_numeric(_replace_null_like(series), errors="coerce").astype("Int64")
    if pa.types.is_floating(arrow_type):
        return pd.to_numeric(_replace_null_like(series), errors="coerce")
    if pa.types.is_string(arrow_type):
        return series.astype("string")
    return series


def _replace_null_like(series: pd.Series) -> pd.Series:
    if series.empty or (series.dtype.kind not in {"O", "U", "S"} and not pd.api.types.is_string_dtype(series.dtype)):
        return series
    return series.mask(series.isin(["", "None", "none", "NaN", "nan"]), pd.NA)


def empty_metadata_frame(schema: pa.Schema) -> pd.DataFrame:
    return pd.DataFrame(columns=field_names(schema))
