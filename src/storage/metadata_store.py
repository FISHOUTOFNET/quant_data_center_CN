"""DuckDB-backed update metadata storage."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from threading import RLock
from weakref import WeakValueDictionary

import duckdb
import pandas as pd
import pyarrow as pa

from src.storage.schema import (
    DATASET_PARTITION_MANIFEST_SCHEMA,
    DATASET_UPDATE_STATUS_SCHEMA,
    PIPELINE_CHECKPOINTS_SCHEMA,
    PIPELINE_RUNS_SCHEMA,
    field_names,
)
from src.utils import paths
from src.utils.config_mgr import ConfigError, ConfigManager
from src.utils.logging import logger

CHECKPOINT_KEY_COLUMNS = ["pipeline", "dataset", "code", "start_date", "end_date"]
DUCKDB_METADATA_CONNECT_MAX_RETRIES = 5
DUCKDB_METADATA_CONNECT_RETRY_DELAY = 0.5
METADATA_TABLES = ("pipeline_runs", "dataset_update_status", "pipeline_checkpoints", "dataset_partition_manifest")
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
        self.duckdb_file = (duckdb_file or default_metadata_duckdb_file(self.root)).resolve()
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

    def upsert_dataset_partition_manifest(self, df: pd.DataFrame) -> None:
        incoming = _clean_dataframe_for_schema(df, DATASET_PARTITION_MANIFEST_SCHEMA)
        if incoming.empty:
            return
        with self._connection() as conn:
            self._register_and_execute(
                conn,
                incoming,
                """
                DELETE FROM dataset_partition_manifest
                WHERE EXISTS (
                    SELECT 1
                    FROM incoming
                    WHERE incoming.dataset = dataset_partition_manifest.dataset
                      AND incoming.partition_column = dataset_partition_manifest.partition_column
                      AND incoming.partition_value = dataset_partition_manifest.partition_value
                )
                """,
                "INSERT INTO dataset_partition_manifest SELECT * FROM incoming",
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

    def read_dataset_partition_manifest(self, dataset: str | None = None) -> pd.DataFrame:
        with self._connection() as conn:
            if dataset is None:
                df = conn.execute("SELECT * FROM dataset_partition_manifest").df()
            else:
                df = conn.execute(
                    "SELECT * FROM dataset_partition_manifest WHERE dataset = ?",
                    [dataset],
                ).df()
            return _clean_dataframe_for_schema(df, DATASET_PARTITION_MANIFEST_SCHEMA)

    def delete_dataset_partition_manifest(self, dataset: str, partition_column: str, partition_value: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                DELETE FROM dataset_partition_manifest
                WHERE dataset = ?
                  AND partition_column = ?
                  AND partition_value = ?
                """,
                [dataset, partition_column, partition_value],
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
            conn = self._connect()
            try:
                self._ensure_initialized(conn)
                yield conn
            finally:
                conn.close()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        last_error: Exception | None = None
        for attempt in range(1, DUCKDB_METADATA_CONNECT_MAX_RETRIES + 1):
            try:
                return duckdb.connect(str(self.duckdb_file))
            except duckdb.IOException as exc:
                last_error = exc
                if attempt >= DUCKDB_METADATA_CONNECT_MAX_RETRIES:
                    break
                time.sleep(DUCKDB_METADATA_CONNECT_RETRY_DELAY)
        if last_error is not None:
            raise last_error
        return duckdb.connect(str(self.duckdb_file))

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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_partition_manifest (
                dataset VARCHAR,
                partition_column VARCHAR,
                partition_value VARCHAR,
                output_path VARCHAR,
                row_count BIGINT,
                min_date VARCHAR,
                max_date VARCHAR,
                content_hash VARCHAR,
                semantic_hash VARCHAR,
                schema_hash VARCHAR,
                source_signature VARCHAR,
                master_row_hash VARCHAR,
                file_size_bytes BIGINT,
                file_mtime TIMESTAMP,
                run_id VARCHAR,
                writer_pid BIGINT,
                writer_thread VARCHAR,
                updated_at TIMESTAMP
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


def default_metadata_duckdb_file(root: Path | None = None) -> Path:
    base = (root or paths.ROOT).resolve()
    try:
        return ConfigManager(base).path("storage.metadata_duckdb_file", "data/metadata/qdc_metadata.duckdb")
    except ConfigError:
        return (base / "data" / "metadata" / "qdc_metadata.duckdb").resolve()


def legacy_metadata_duckdb_file(root: Path | None = None) -> Path:
    base = (root or paths.ROOT).resolve()
    return (base / "data" / "duckdb" / "quant.duckdb").resolve()


def migrate_metadata_duckdb(
    *,
    root: Path | None = None,
    source_file: Path | None = None,
    target_file: Path | None = None,
) -> dict[str, object]:
    """Copy legacy metadata tables from quant.duckdb into qdc_metadata.duckdb."""

    base = (root or paths.ROOT).resolve()
    source = (source_file or legacy_metadata_duckdb_file(base)).resolve()
    target = (target_file or default_metadata_duckdb_file(base)).resolve()
    migrated = dict.fromkeys(METADATA_TABLES, 0)
    result: dict[str, object] = {
        "source": str(source),
        "target": str(target),
        "migrated_rows": migrated,
        "skipped": False,
    }

    if source == target:
        result["skipped"] = True
        result["reason"] = "source and target are the same file"
        return result
    if not source.exists():
        result["skipped"] = True
        result["reason"] = "legacy metadata DuckDB file does not exist"
        return result

    target_store = DuckDBMetadataStore(root=base, duckdb_file=target)
    try:
        target_store.initialize()
        with duckdb.connect(str(source), read_only=True) as source_conn:
            if _duckdb_table_exists(source_conn, "pipeline_runs"):
                runs = _clean_dataframe_for_schema(
                    source_conn.execute("SELECT * FROM pipeline_runs").df(),
                    PIPELINE_RUNS_SCHEMA,
                )
                runs = _new_pipeline_runs(target_store, runs)
                if not runs.empty:
                    target_store.append_pipeline_runs(runs)
                migrated["pipeline_runs"] = len(runs)

            if _duckdb_table_exists(source_conn, "dataset_update_status"):
                statuses = _clean_dataframe_for_schema(
                    source_conn.execute("SELECT * FROM dataset_update_status").df(),
                    DATASET_UPDATE_STATUS_SCHEMA,
                )
                if not statuses.empty:
                    target_store.upsert_dataset_update_status(statuses)
                migrated["dataset_update_status"] = len(statuses)

            if _duckdb_table_exists(source_conn, "pipeline_checkpoints"):
                checkpoints = _clean_dataframe_for_schema(
                    source_conn.execute("SELECT * FROM pipeline_checkpoints").df(),
                    PIPELINE_CHECKPOINTS_SCHEMA,
                )
                if not checkpoints.empty:
                    target_store.upsert_pipeline_checkpoints(checkpoints)
                migrated["pipeline_checkpoints"] = len(checkpoints)

            if _duckdb_table_exists(source_conn, "dataset_partition_manifest"):
                manifests = _clean_dataframe_for_schema(
                    source_conn.execute("SELECT * FROM dataset_partition_manifest").df(),
                    DATASET_PARTITION_MANIFEST_SCHEMA,
                )
                if not manifests.empty:
                    target_store.upsert_dataset_partition_manifest(manifests)
                migrated["dataset_partition_manifest"] = len(manifests)
    finally:
        target_store.close()
    logger.info(
        "Migrated metadata DuckDB tables source={} target={} rows={}",
        source,
        target,
        migrated,
    )
    return result


def _duckdb_table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _new_pipeline_runs(store: DuckDBMetadataStore, incoming: pd.DataFrame) -> pd.DataFrame:
    if incoming.empty:
        return incoming
    existing = store.read_pipeline_runs()
    if existing.empty or "task_id" not in existing.columns:
        return incoming.drop_duplicates(["task_id"], keep="last").reset_index(drop=True)
    existing_ids = set(existing["task_id"].dropna().astype(str))
    work = incoming.drop_duplicates(["task_id"], keep="last")
    return work.loc[~work["task_id"].astype(str).isin(existing_ids)].reset_index(drop=True)
