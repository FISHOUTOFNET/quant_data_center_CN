"""DuckDB query layer over Parquet datasets."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from src.storage.dataset_catalog import (
    ADJUST_FACTOR_DATASET,
    CALENDAR_DATASET,
    STOCK_BASIC_DATASET,
    STOCK_VALUE_EM_DATASET,
    DatasetDefinition,
    akshare_a_stock_definitions,
    daily_k_definitions,
)
from src.utils import paths
from src.utils.logging import logger


class DuckDBStore:
    """Create quant.duckdb and views over Parquet files."""

    def __init__(self, root: Path | None = None, duckdb_file: Path | None = None) -> None:
        self.root = (root or paths.ROOT).resolve()
        self.parquet_dir = self.root / "data" / "parquet"
        self.duckdb_file = (duckdb_file or self.root / "data" / "duckdb" / "quant.duckdb").resolve()

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.duckdb_file.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.duckdb_file))

    def build_views(self) -> list[str]:
        self._cleanup_tmp_parquet_files()
        sqls = self.view_sqls()
        with self.connect() as conn:
            for sql in sqls:
                conn.execute(sql)
        logger.info("Built DuckDB views in {}", self.duckdb_file)
        return sqls

    def _cleanup_tmp_parquet_files(self) -> int:
        """Remove stale .tmp.parquet files left by interrupted writes."""
        if not self.parquet_dir.exists():
            return 0
        count = 0
        for tmp_file in self.parquet_dir.rglob("*.tmp.parquet"):
            try:
                tmp_file.unlink()
                count += 1
                logger.debug("Removed stale temp parquet file: {}", tmp_file)
            except OSError as e:
                logger.warning("Failed to remove temp parquet file {}: {}", tmp_file, e)
        if count > 0:
            logger.info("Cleaned up {} stale .tmp.parquet files", count)
        return count

    def view_sqls(self) -> list[str]:
        return [
            *[
                self._daily_view_sql(definition.view_name or f"v_{definition.name}", definition.name)
                for definition in daily_k_definitions()
            ],
            self._adjust_factor_view_sql(),
            self._partitioned_dataset_view_sql(STOCK_VALUE_EM_DATASET),
            *[
                self._partitioned_dataset_view_sql(definition)
                for definition in akshare_a_stock_definitions()
            ],
            self._stock_basic_view_sql(),
            self._calendar_view_sql(),
        ]

    def _daily_view_sql(self, view_name: str, dataset: str) -> str:
        dataset_dir = self.parquet_dir / dataset
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        definition = next(item for item in daily_k_definitions() if item.name == dataset)
        return self._empty_view_sql(view_name, definition.schema)

    def _adjust_factor_view_sql(self) -> str:
        dataset_dir = self.parquet_dir / ADJUST_FACTOR_DATASET.name
        view_name = ADJUST_FACTOR_DATASET.view_name or "v_adjust_factor"
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        return self._empty_view_sql(view_name, ADJUST_FACTOR_DATASET.schema)

    def _partitioned_dataset_view_sql(self, definition: DatasetDefinition) -> str:
        dataset_dir = self.parquet_dir / definition.name
        view_name = definition.view_name or f"v_{definition.name}"
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        return self._empty_view_sql(view_name, definition.schema)

    def _stock_basic_view_sql(self) -> str:
        dataset_dir = self.parquet_dir / STOCK_BASIC_DATASET.name
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {STOCK_BASIC_DATASET.view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        return self._empty_view_sql(
            STOCK_BASIC_DATASET.view_name or "v_stock_basic",
            STOCK_BASIC_DATASET.schema,
            extra_columns={"snapshot_date": "DATE"},
        )

    def _calendar_view_sql(self) -> str:
        path = self.parquet_dir / CALENDAR_DATASET.name / "data.parquet"
        if path.exists():
            return (
                f"CREATE OR REPLACE VIEW {CALENDAR_DATASET.view_name} AS\n"
                f"SELECT * FROM read_parquet('{self._duckdb_path(path)}', union_by_name = true);"
            )
        return self._empty_view_sql(CALENDAR_DATASET.view_name or "v_calendar", CALENDAR_DATASET.schema)

    def _empty_view_sql(
        self,
        view_name: str,
        schema: pa.Schema,
        extra_columns: dict[str, str] | None = None,
    ) -> str:
        expressions = [
            f"CAST(NULL AS {self._duckdb_type(field.type)}) AS {self._quote(field.name)}" for field in schema
        ]
        for name, sql_type in (extra_columns or {}).items():
            expressions.append(f"CAST(NULL AS {sql_type}) AS {self._quote(name)}")
        columns_sql = ",\n    ".join(expressions)
        return f"CREATE OR REPLACE VIEW {view_name} AS\nSELECT\n    {columns_sql}\nWHERE FALSE;"

    def _duckdb_type(self, arrow_type: pa.DataType) -> str:
        if pa.types.is_date(arrow_type):
            return "DATE"
        if pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        if pa.types.is_integer(arrow_type):
            return "BIGINT"
        if pa.types.is_floating(arrow_type):
            return "DOUBLE"
        return "VARCHAR"

    def _duckdb_path(self, path: Path) -> str:
        return path.as_posix().replace("'", "''")

    def _has_parquet_files(self, directory: Path) -> bool:
        if not directory.exists():
            return False
        for f in directory.rglob("*.parquet"):
            if ".tmp.parquet" not in f.name:
                return True
        return False

    def _quote(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'
