"""DuckDB query layer over Parquet datasets."""

from __future__ import annotations

from contextlib import suppress
import time
from pathlib import Path

import duckdb
import pyarrow as pa

from src.storage.dataset_catalog import (
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET,
    BAOSTOCK_CN_TRADING_CALENDAR_DATASET,
    BAOSTOCK_CN_STOCK_BASIC_DATASET,
    AKSHARE_VALUATION_EASTMONEY_DATASET,
    DatasetDefinition,
    akshare_a_stock_definitions,
    daily_bar_definitions,
)
from src.storage.dataset_catalog import (
    AKSHARE_SPOT_QUOTE_EASTMONEY_DATASET,
    AKSHARE_SPOT_QUOTE_SINA_DATASET,
)
from src.utils import paths
from src.utils.logging import logger


DUCKDB_CONNECT_MAX_RETRIES = 5
DUCKDB_CONNECT_RETRY_DELAY = 0.5
_OLD_DAILY_BAR_SOURCE = "daily" + "_k"
_OLD_AKSHARE_DAILY_BAR_SOURCE = "stock_zh_a_" + "hist"
LEGACY_VIEW_NAMES = (
    f"v_{_OLD_DAILY_BAR_SOURCE}_none",
    f"v_{_OLD_DAILY_BAR_SOURCE}_qfq",
    f"v_{_OLD_DAILY_BAR_SOURCE}_hfq",
    "v_adjust_factor",
    "v_stock_basic",
    "v_calendar",
    "v_stock_value_em",
    "v_stock_info_sh_delist",
    "v_stock_info_sz_delist",
    "v_stock_zh_a_spot_em",
    "v_stock_zh_a_spot_sina",
    f"v_{_OLD_AKSHARE_DAILY_BAR_SOURCE}_none",
    f"v_{_OLD_AKSHARE_DAILY_BAR_SOURCE}_qfq",
    f"v_{_OLD_AKSHARE_DAILY_BAR_SOURCE}_hfq",
    "v_stock_institute_hold",
)


class DuckDBStore:
    """Create quant.duckdb and views over Parquet files."""

    def __init__(self, root: Path | None = None, duckdb_file: Path | None = None) -> None:
        self.root = (root or paths.ROOT).resolve()
        self.parquet_dir = self.root / "data" / "parquet"
        self.duckdb_file = (duckdb_file or self.root / "data" / "duckdb" / "quant.duckdb").resolve()

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB with retry logic for file locking issues.
        
        On Windows, DuckDB files can be temporarily locked by other processes
        or due to delayed file handle release. This method retries connections
        with exponential backoff to handle transient IOException.
        """
        self.duckdb_file.parent.mkdir(parents=True, exist_ok=True)
        
        last_error: Exception | None = None
        for attempt in range(DUCKDB_CONNECT_MAX_RETRIES):
            try:
                return duckdb.connect(str(self.duckdb_file))
            except (duckdb.IOException, OSError) as e:
                last_error = e
                if attempt < DUCKDB_CONNECT_MAX_RETRIES - 1:
                    delay = DUCKDB_CONNECT_RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        "Failed to connect to DuckDB {}, retrying in {:.3f}s (attempt {}/{}): {}",
                        self.duckdb_file,
                        delay,
                        attempt + 1,
                        DUCKDB_CONNECT_MAX_RETRIES,
                        str(e),
                    )
                    time.sleep(delay)
        
        raise last_error

    def build_views(self, cleanup_tmp_files: bool = True) -> list[str]:
        if cleanup_tmp_files:
            self._cleanup_tmp_parquet_files()
        sqls = self.view_sqls()
        with self.connect() as conn:
            transaction_open = False
            try:
                conn.execute("BEGIN TRANSACTION")
                transaction_open = True
                for sql in sqls:
                    conn.execute(sql)
                for view_name in LEGACY_VIEW_NAMES:
                    conn.execute(f"DROP VIEW IF EXISTS {self._quote(view_name)}")
                conn.execute("COMMIT")
                transaction_open = False
            finally:
                if transaction_open:
                    with suppress(Exception):
                        conn.execute("ROLLBACK")
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
                for definition in daily_bar_definitions()
            ],
            self._baostock_cn_stock_adjustment_factor_view_sql(),
            self._partitioned_dataset_view_sql(BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET),
            self._partitioned_dataset_view_sql(AKSHARE_VALUATION_EASTMONEY_DATASET),
            *[
                self._partitioned_dataset_view_sql(definition)
                for definition in akshare_a_stock_definitions()
            ],
            self._baostock_cn_stock_basic_view_sql(),
            self._baostock_cn_trading_calendar_view_sql(),
        ]

    def _daily_view_sql(self, view_name: str, dataset: str) -> str:
        dataset_dir = self.parquet_dir / dataset
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        definition = next(item for item in daily_bar_definitions() if item.name == dataset)
        return self._empty_view_sql(view_name, definition.schema)

    def _baostock_cn_stock_adjustment_factor_view_sql(self) -> str:
        dataset_dir = self.parquet_dir / BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.name
        view_name = BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.view_name or "v_baostock_cn_stock_adjustment_factor"
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        return self._empty_view_sql(view_name, BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET.schema)

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

    def _baostock_cn_stock_basic_view_sql(self) -> str:
        dataset_dir = self.parquet_dir / BAOSTOCK_CN_STOCK_BASIC_DATASET.name
        if self._has_parquet_files(dataset_dir):
            pattern = self._duckdb_path(dataset_dir / "**" / "*.parquet")
            return (
                f"CREATE OR REPLACE VIEW {BAOSTOCK_CN_STOCK_BASIC_DATASET.view_name} AS\n"
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true);"
            )
        return self._empty_view_sql(
            BAOSTOCK_CN_STOCK_BASIC_DATASET.view_name or "v_baostock_cn_stock_basic",
            BAOSTOCK_CN_STOCK_BASIC_DATASET.schema,
            extra_columns={"snapshot_date": "DATE"},
        )

    def _baostock_cn_trading_calendar_view_sql(self) -> str:
        path = self.parquet_dir / BAOSTOCK_CN_TRADING_CALENDAR_DATASET.name / "data.parquet"
        if path.exists():
            return (
                f"CREATE OR REPLACE VIEW {BAOSTOCK_CN_TRADING_CALENDAR_DATASET.view_name} AS\n"
                f"SELECT * FROM read_parquet('{self._duckdb_path(path)}', union_by_name = true);"
            )
        return self._empty_view_sql(BAOSTOCK_CN_TRADING_CALENDAR_DATASET.view_name or "v_baostock_cn_trading_calendar", BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema)

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
