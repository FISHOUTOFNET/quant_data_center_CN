"""Migrate legacy dataset, schema, and DuckDB names to naming standard v1.

The script is intentionally dry-run by default. Use ``--apply`` to write new
Parquet files, rename DuckDB metadata tables, rebuild views, and drop legacy
views after the new layout validates.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.storage.duckdb_store import DuckDBStore  # noqa: E402
from src.storage.parquet_store import ParquetStore  # noqa: E402
from src.storage.schema import schema_for_dataset  # noqa: E402

DATASET_RENAMES: dict[str, str] = {
    "daily_k_none": "baostock_cn_stock_daily_bar_unadjusted",
    "daily_k_qfq": "baostock_cn_stock_daily_bar_qfq",
    "daily_k_hfq": "baostock_cn_stock_daily_bar_hfq",
    "adjust_factor": "baostock_cn_stock_adjustment_factor",
    "stock_basic": "baostock_cn_stock_basic",
    "calendar": "baostock_cn_trading_calendar",
    "stock_value_em": "akshare_cn_stock_valuation_eastmoney",
    "stock_info_sh_delist": "akshare_cn_stock_delist_sh",
    "stock_info_sz_delist": "akshare_cn_stock_delist_sz",
    "stock_zh_a_spot_em": "akshare_cn_stock_spot_quote_eastmoney",
    "stock_zh_a_spot_sina": "akshare_cn_stock_spot_quote_sina",
    "stock_zh_a_hist_none": "akshare_cn_stock_daily_bar_unadjusted",
    "stock_zh_a_hist_qfq": "akshare_cn_stock_daily_bar_qfq",
    "stock_zh_a_hist_hfq": "akshare_cn_stock_daily_bar_hfq",
    "stock_institute_hold": "akshare_cn_stock_institution_holding",
}

COLUMN_RENAMES: dict[str, str] = {
    "pctChg": "pct_change",
    "pct_chg": "pct_change",
    "preclose": "prev_close",
    "adjustflag": "adjust_flag",
    "tradestatus": "trade_status",
    "turn": "turnover_rate",
    "peTTM": "pe_ttm",
    "pbMRQ": "pb_mrq",
    "psTTM": "ps_ttm",
    "pcfNcfTTM": "pcf_ncf_ttm",
    "isST": "is_st",
    "ipoDate": "ipo_date",
    "outDate": "delist_date",
    "dividOperateDate": "dividend_operate_date",
    "foreAdjustFactor": "forward_adjust_factor",
    "backAdjustFactor": "backward_adjust_factor",
    "adjustFactor": "adjustment_factor",
    "latest_price": "last_price",
    "change_amount": "price_change",
    "adjust": "adjustment",
    "code_name": "name",
    "type": "security_type",
    "status": "listing_status",
}

SOURCE_ENDPOINT_RENAMES: dict[str, str] = {
    "akshare_cn_stock_spot_quote_eastmoney": "stock_zh_a_spot_em",
    "akshare_cn_stock_delist_sh": "stock_info_sh_delist",
    "akshare_cn_stock_delist_sz": "stock_info_sz_delist",
}

METADATA_TABLE_RENAMES: dict[str, str] = {
    "update_runs": "pipeline_runs",
    "update_status": "dataset_update_status",
    "metadata_migrations": "schema_migrations",
}

PIPELINE_RENAMES: dict[str, str] = {
    "update_akshare_hist": "update_akshare_daily_bar",
}


@dataclass(frozen=True)
class MigrationConfig:
    root: Path
    apply: bool

    @property
    def parquet_dir(self) -> Path:
        return self.root / "data" / "parquet"

    @property
    def duckdb_file(self) -> Path:
        return self.root / "data" / "duckdb" / "quant.duckdb"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"


def migrate(config: MigrationConfig) -> dict[str, Any]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report: dict[str, Any] = {
        "version": "naming_v1",
        "timestamp": timestamp,
        "root": str(config.root),
        "dry_run": not config.apply,
        "dataset_renames": [],
        "duckdb": [],
        "views": [],
        "errors": [],
    }

    try:
        report["dataset_renames"] = migrate_parquet_datasets(config, timestamp)
        report["duckdb"] = migrate_duckdb_metadata(config)
        report["views"] = rebuild_duckdb_views(config)
    except Exception as exc:
        report["errors"].append({"type": type(exc).__name__, "message": str(exc)})
        raise
    finally:
        write_report(config, report, timestamp)

    return report


def migrate_parquet_datasets(config: MigrationConfig, timestamp: str) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    store = ParquetStore(root=config.root)

    for old_name, new_name in DATASET_RENAMES.items():
        old_dir = config.parquet_dir / old_name
        new_dir = config.parquet_dir / new_name
        action: dict[str, Any] = {
            "old_dataset": old_name,
            "new_dataset": new_name,
            "old_dir": str(old_dir),
            "new_dir": str(new_dir),
            "exists": old_dir.exists(),
            "files": [],
            "status": "skipped_missing",
        }
        if not old_dir.exists():
            actions.append(action)
            continue

        parquet_files = sorted(path for path in old_dir.rglob("*.parquet") if ".tmp.parquet" not in path.name)
        action["files"] = [str(path.relative_to(old_dir)) for path in parquet_files]
        if not parquet_files:
            action["status"] = "skipped_empty"
            actions.append(action)
            continue

        if not config.apply:
            action["status"] = "planned"
            actions.append(action)
            continue

        ensure_within(config.parquet_dir, old_dir)
        ensure_within(config.parquet_dir, new_dir)
        tmp_dir = config.parquet_dir / f".{new_name}.migrating_{timestamp}"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        try:
            write_migrated_parquet_tree(store, old_dir, tmp_dir, parquet_files, new_name)
            validate_migrated_tree(tmp_dir, parquet_files)
            if new_dir.exists():
                if any(new_dir.rglob("*.parquet")):
                    raise RuntimeError(f"Refusing to overwrite non-empty target dataset directory: {new_dir}")
                shutil.rmtree(new_dir)
            tmp_dir.rename(new_dir)
            shutil.rmtree(old_dir)
            action["status"] = "applied"
        except Exception:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
            action["status"] = "failed"
            raise
        finally:
            actions.append(action)

    return actions


def write_migrated_parquet_tree(
    store: ParquetStore,
    old_dir: Path,
    tmp_dir: Path,
    parquet_files: list[Path],
    new_dataset: str,
) -> None:
    schema = schema_for_dataset(new_dataset)
    adjustment = daily_bar_adjustment_from_dataset(new_dataset)
    for source_path in parquet_files:
        relative_path = source_path.relative_to(old_dir)
        target_path = tmp_dir / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.read_parquet(source_path)
        df = rename_columns(df)
        if adjustment is not None:
            df["adjustment"] = adjustment
        if "source_endpoint" in df.columns:
            df["source_endpoint"] = df["source_endpoint"].replace(SOURCE_ENDPOINT_RENAMES)
        cleaned = store.clean_dataframe_for_schema(df, schema)
        cleaned.to_parquet(target_path, index=False)


def validate_migrated_tree(tmp_dir: Path, expected_files: list[Path]) -> None:
    written_files = sorted(path for path in tmp_dir.rglob("*.parquet") if ".tmp.parquet" not in path.name)
    if len(written_files) != len(expected_files):
        raise RuntimeError(f"Expected {len(expected_files)} migrated parquet files, wrote {len(written_files)}")
    for path in written_files:
        pd.read_parquet(path)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {old: new for old, new in COLUMN_RENAMES.items() if old in df.columns and new not in df.columns}
    return df.rename(columns=rename_map)


def daily_bar_adjustment_from_dataset(dataset: str) -> str | None:
    prefix = "akshare_cn_stock_daily_bar_"
    if dataset.startswith(prefix):
        return dataset.removeprefix(prefix)
    return None


def migrate_duckdb_metadata(config: MigrationConfig) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if not config.duckdb_file.exists():
        return actions

    with duckdb.connect(str(config.duckdb_file)) as conn:
        for old_name, new_name in METADATA_TABLE_RENAMES.items():
            old_exists = duckdb_table_exists(conn, old_name)
            new_exists = duckdb_table_exists(conn, new_name)
            action = {
                "old_table": old_name,
                "new_table": new_name,
                "old_exists": old_exists,
                "new_exists": new_exists,
                "status": "skipped_missing",
            }
            if old_exists:
                action["status"] = "planned"
                if config.apply:
                    if new_exists:
                        conn.execute(f"INSERT INTO {quote_ident(new_name)} SELECT * FROM {quote_ident(old_name)}")
                        conn.execute(f"DROP TABLE {quote_ident(old_name)}")
                    else:
                        conn.execute(f"ALTER TABLE {quote_ident(old_name)} RENAME TO {quote_ident(new_name)}")
                    action["status"] = "applied"
            actions.append(action)

        if config.apply:
            for table_name in ("pipeline_runs", "dataset_update_status", "pipeline_checkpoints"):
                if duckdb_table_exists(conn, table_name) and duckdb_column_exists(conn, table_name, "dataset"):
                    update_dataset_values(conn, table_name)
            if duckdb_table_exists(conn, "pipeline_runs") and duckdb_column_exists(conn, "pipeline_runs", "pipeline"):
                update_pipeline_values(conn, "pipeline_runs")
            if duckdb_table_exists(conn, "pipeline_checkpoints") and duckdb_column_exists(
                conn, "pipeline_checkpoints", "pipeline"
            ):
                update_pipeline_values(conn, "pipeline_checkpoints")

    return actions


def update_dataset_values(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    for old_name, new_name in DATASET_RENAMES.items():
        conn.execute(
            f"UPDATE {quote_ident(table_name)} SET dataset = ? WHERE dataset = ?",
            [new_name, old_name],
        )


def update_pipeline_values(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    for old_name, new_name in PIPELINE_RENAMES.items():
        conn.execute(
            f"UPDATE {quote_ident(table_name)} SET pipeline = ? WHERE pipeline = ?",
            [new_name, old_name],
        )


def rebuild_duckdb_views(config: MigrationConfig) -> list[dict[str, Any]]:
    old_views = [f"v_{dataset}" for dataset in DATASET_RENAMES]
    if not config.apply:
        return [{"view": view, "status": "planned_drop"} for view in old_views]

    DuckDBStore(root=config.root).build_views()
    dropped: list[dict[str, Any]] = []
    with duckdb.connect(str(config.duckdb_file)) as conn:
        for view in old_views:
            conn.execute(f"DROP VIEW IF EXISTS {quote_ident(view)}")
            dropped.append({"view": view, "status": "dropped_if_exists"})
    return dropped


def duckdb_table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
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


def duckdb_column_exists(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and row[0])


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def ensure_within(parent: Path, child: Path) -> None:
    parent_resolved = parent.resolve()
    child_resolved = child.resolve()
    if parent_resolved != child_resolved and parent_resolved not in child_resolved.parents:
        raise RuntimeError(f"Refusing to operate outside {parent_resolved}: {child_resolved}")


def write_report(config: MigrationConfig, report: dict[str, Any], timestamp: str) -> Path:
    config.logs_dir.mkdir(parents=True, exist_ok=True)
    path = config.logs_dir / f"naming_migration_{timestamp}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate legacy names to naming standard v1.")
    parser.add_argument("--root", type=Path, default=ROOT, help="Project root. Defaults to repository root.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--apply", action="store_true", help="Apply the migration. Without this flag the script is dry-run."
    )
    mode.add_argument(
        "--dry-run", action="store_true", help="Plan the migration without writing data. This is the default."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = MigrationConfig(root=args.root.resolve(), apply=bool(args.apply))
    report = migrate(config)
    print(json.dumps({"dry_run": report["dry_run"], "errors": report["errors"]}, ensure_ascii=False))
    return 0 if not report["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
