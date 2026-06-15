"""DuckDB query layer over Parquet datasets."""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pyarrow as pa

from src.storage.dataset_catalog import (
    AKSHARE_VALUATION_EASTMONEY_DATASET,
    BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
    BAOSTOCK_CN_STOCK_BASIC_DATASET,
    BAOSTOCK_CN_STOCK_VALUATION_PERCENTILE_DATASET,
    BAOSTOCK_CN_TRADING_CALENDAR_DATASET,
    CN_SECURITY_MASTER_DATASET,
    CN_STOCK_DAILY_BAR_DATASET,
    CN_STOCK_VALUATION_DATASET,
    DatasetDefinition,
    akshare_a_stock_definitions,
    daily_bar_definitions,
    qlib_definitions,
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
                    delay = DUCKDB_CONNECT_RETRY_DELAY * (2**attempt)
                    logger.warning(
                        "Failed to connect to DuckDB {}, retrying in {:.3f}s (attempt {}/{}): {}",
                        self.duckdb_file,
                        delay,
                        attempt + 1,
                        DUCKDB_CONNECT_MAX_RETRIES,
                        str(e),
                    )
                    time.sleep(delay)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Failed to connect to DuckDB without a captured error")

    def build_views(self, cleanup_tmp_files: bool = True) -> list[str]:
        if cleanup_tmp_files:
            self._cleanup_tmp_parquet_files()
        sqls = self.view_sqls()
        with self.connect() as conn:
            for sql in sqls:
                conn.execute(sql)
            for view_name in LEGACY_VIEW_NAMES:
                conn.execute(f"DROP VIEW IF EXISTS {self._quote(view_name)}")
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
            *[self._partitioned_dataset_view_sql(definition) for definition in akshare_a_stock_definitions()],
            *[self._generic_dataset_view_sql(definition) for definition in qlib_definitions()],
            self._generic_dataset_view_sql(CN_SECURITY_MASTER_DATASET),
            self._generic_dataset_view_sql(CN_STOCK_DAILY_BAR_DATASET),
            self._research_daily_bar_view_sql(),
            self._generic_dataset_view_sql(CN_STOCK_VALUATION_DATASET),
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

    def _research_daily_bar_view_sql(self) -> str:
        return """
CREATE OR REPLACE VIEW v_cn_stock_daily_bar_research AS
WITH daily AS (
    SELECT
        CAST(date AS DATE) AS date,
        upper(regexp_extract(CAST(code AS VARCHAR), '^(sh|sz|bj)\\.', 1))
            || '.'
            || regexp_extract(CAST(code AS VARCHAR), '(\\d{6})', 1) AS security_id,
        code,
        upper(regexp_extract(CAST(code AS VARCHAR), '^(sh|sz|bj)\\.', 1)) AS exchange,
        CAST(NULL AS VARCHAR) AS name,
        'unadjusted' AS adjustment,
        open,
        high,
        low,
        close,
        prev_close,
        CAST(volume AS DOUBLE) AS volume,
        amount,
        turnover_rate,
        pct_change,
        trade_status,
        is_st,
        TRUE AS is_active
    FROM v_baostock_cn_stock_daily_bar_unadjusted
    UNION ALL
    SELECT
        CAST(date AS DATE) AS date,
        upper(regexp_extract(CAST(code AS VARCHAR), '^(sh|sz|bj)\\.', 1))
            || '.'
            || regexp_extract(CAST(code AS VARCHAR), '(\\d{6})', 1) AS security_id,
        code,
        upper(regexp_extract(CAST(code AS VARCHAR), '^(sh|sz|bj)\\.', 1)) AS exchange,
        CAST(NULL AS VARCHAR) AS name,
        'qfq' AS adjustment,
        open,
        high,
        low,
        close,
        prev_close,
        CAST(volume AS DOUBLE) AS volume,
        amount,
        turnover_rate,
        pct_change,
        trade_status,
        is_st,
        TRUE AS is_active
    FROM v_baostock_cn_stock_daily_bar_qfq
    UNION ALL
    SELECT
        CAST(date AS DATE) AS date,
        upper(regexp_extract(CAST(code AS VARCHAR), '^(sh|sz|bj)\\.', 1))
            || '.'
            || regexp_extract(CAST(code AS VARCHAR), '(\\d{6})', 1) AS security_id,
        code,
        upper(regexp_extract(CAST(code AS VARCHAR), '^(sh|sz|bj)\\.', 1)) AS exchange,
        CAST(NULL AS VARCHAR) AS name,
        'hfq' AS adjustment,
        open,
        high,
        low,
        close,
        prev_close,
        CAST(volume AS DOUBLE) AS volume,
        amount,
        turnover_rate,
        pct_change,
        trade_status,
        is_st,
        TRUE AS is_active
    FROM v_baostock_cn_stock_daily_bar_hfq
),
pivoted AS (
    SELECT
        date,
        regexp_extract(CAST(code AS VARCHAR), '(\\d{6})', 1) AS code6,
        max(code) FILTER (WHERE adjustment = 'unadjusted') AS code,
        max(security_id) FILTER (WHERE adjustment = 'unadjusted') AS security_id,
        max(exchange) FILTER (WHERE adjustment = 'unadjusted') AS exchange,
        max(name) FILTER (WHERE adjustment = 'unadjusted') AS name,
        max(open) FILTER (WHERE adjustment = 'unadjusted') AS open,
        max(high) FILTER (WHERE adjustment = 'unadjusted') AS high,
        max(low) FILTER (WHERE adjustment = 'unadjusted') AS low,
        max(close) FILTER (WHERE adjustment = 'unadjusted') AS close,
        max(prev_close) FILTER (WHERE adjustment = 'unadjusted') AS prev_close,
        max(volume) FILTER (WHERE adjustment = 'unadjusted') AS volume,
        max(amount) FILTER (WHERE adjustment = 'unadjusted') AS amount,
        max(turnover_rate) FILTER (WHERE adjustment = 'unadjusted') AS turnover_rate,
        max(pct_change) FILTER (WHERE adjustment = 'unadjusted') AS pct_change,
        max(trade_status) FILTER (WHERE adjustment = 'unadjusted') AS trade_status,
        max(is_st) FILTER (WHERE adjustment = 'unadjusted') AS is_st,
        max(
            CASE
                WHEN adjustment = 'unadjusted'
                 AND lower(CAST(is_active AS VARCHAR)) IN ('true', '1', '1.0', 'yes')
                    THEN TRUE
                WHEN adjustment = 'unadjusted'
                 AND lower(CAST(is_active AS VARCHAR)) IN ('false', '0', '0.0', 'no')
                    THEN FALSE
                ELSE NULL
            END
        ) AS is_active,
        max(open) FILTER (WHERE adjustment = 'qfq') AS qfq_open,
        max(high) FILTER (WHERE adjustment = 'qfq') AS qfq_high,
        max(low) FILTER (WHERE adjustment = 'qfq') AS qfq_low,
        max(close) FILTER (WHERE adjustment = 'qfq') AS qfq_close,
        max(prev_close) FILTER (WHERE adjustment = 'qfq') AS qfq_prev_close,
        max(open) FILTER (WHERE adjustment = 'hfq') AS hfq_open,
        max(high) FILTER (WHERE adjustment = 'hfq') AS hfq_high,
        max(low) FILTER (WHERE adjustment = 'hfq') AS hfq_low,
        max(close) FILTER (WHERE adjustment = 'hfq') AS hfq_close,
        max(prev_close) FILTER (WHERE adjustment = 'hfq') AS hfq_prev_close,
        count(*) FILTER (WHERE adjustment = 'unadjusted') AS unadjusted_rows
    FROM daily
    GROUP BY date, code6
),
base AS (
    SELECT *
    FROM pivoted
    WHERE unadjusted_rows > 0
),
factor_events AS (
    SELECT
        regexp_extract(CAST(code AS VARCHAR), '(\\d{6})', 1) AS code6,
        CAST(dividend_operate_date AS DATE) AS date,
        max(adjustment_factor) AS adjustment_factor,
        max(forward_adjust_factor) AS forward_adjust_factor,
        max(backward_adjust_factor) AS backward_adjust_factor,
        TRUE AS has_factor_event
    FROM v_baostock_cn_stock_adjustment_factor
    GROUP BY code6, date
),
joined AS (
    SELECT
        base.date,
        base.security_id,
        base.code,
        base.exchange,
        base.name,
        base.open,
        base.high,
        base.low,
        base.close,
        base.prev_close,
        base.volume,
        base.amount,
        base.turnover_rate,
        base.pct_change,
        base.trade_status,
        base.is_st,
        base.is_active,
        base.qfq_open,
        base.qfq_high,
        base.qfq_low,
        base.qfq_close,
        base.qfq_prev_close,
        base.hfq_open,
        base.hfq_high,
        base.hfq_low,
        base.hfq_close,
        base.hfq_prev_close,
        factor_events.adjustment_factor,
        factor_events.forward_adjust_factor,
        factor_events.backward_adjust_factor,
        coalesce(factor_events.has_factor_event, FALSE) AS has_factor_event
    FROM base
    LEFT JOIN factor_events
        ON factor_events.code6 = base.code6
       AND factor_events.date = base.date
)
SELECT
    date,
    security_id,
    code,
    exchange,
    name,
    open,
    high,
    low,
    close,
    prev_close,
    volume,
    amount,
    turnover_rate,
    pct_change,
    trade_status,
    is_st,
    is_active,
    qfq_open,
    qfq_high,
    qfq_low,
    qfq_close,
    qfq_prev_close,
    hfq_open,
    hfq_high,
    hfq_low,
    hfq_close,
    hfq_prev_close,
    adjustment_factor,
    forward_adjust_factor,
    backward_adjust_factor,
    CASE
        WHEN has_factor_event THEN TRUE
        WHEN adjustment_factor IS NOT NULL
         AND lag(adjustment_factor) OVER (PARTITION BY code ORDER BY date) IS NOT NULL
         AND adjustment_factor <> lag(adjustment_factor) OVER (PARTITION BY code ORDER BY date)
            THEN TRUE
        ELSE FALSE
    END AS corporate_action_flag
FROM joined;
""".strip()

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

    def _generic_dataset_view_sql(self, definition: DatasetDefinition) -> str:
        if definition.partition_column is not None:
            return self._partitioned_dataset_view_sql(definition)
        path = self.parquet_dir / definition.name / "data.parquet"
        view_name = definition.view_name or f"v_{definition.name}"
        if path.exists():
            return (
                f"CREATE OR REPLACE VIEW {view_name} AS\n"
                f"SELECT * FROM read_parquet('{self._duckdb_path(path)}', union_by_name = true);"
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
        return self._empty_view_sql(
            BAOSTOCK_CN_TRADING_CALENDAR_DATASET.view_name or "v_baostock_cn_trading_calendar",
            BAOSTOCK_CN_TRADING_CALENDAR_DATASET.schema,
        )

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
        return any(".tmp.parquet" not in f.name for f in directory.rglob("*.parquet"))

    def _quote(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'
