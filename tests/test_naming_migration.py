from __future__ import annotations

import duckdb
import pandas as pd

from scripts.migrate_naming_v1 import MigrationConfig, migrate


def test_naming_migration_dry_run_reports_without_writing(tmp_path) -> None:
    old_dir = tmp_path / "data" / "parquet" / "daily_k_none" / "code=sh.600000"
    old_dir.mkdir(parents=True)
    _legacy_baostock_daily_bar().to_parquet(old_dir / "data.parquet", index=False)

    report = migrate(MigrationConfig(root=tmp_path, apply=False))

    planned = [item for item in report["dataset_renames"] if item["old_dataset"] == "daily_k_none"]
    assert planned[0]["status"] == "planned"
    assert old_dir.exists()
    assert not (tmp_path / "data" / "parquet" / "baostock_cn_stock_daily_bar_unadjusted").exists()
    assert list((tmp_path / "logs").glob("naming_migration_*.json"))


def test_naming_migration_apply_rewrites_parquet_columns_and_duckdb_metadata(tmp_path) -> None:
    _write_legacy_parquet_layout(tmp_path)
    _write_legacy_duckdb(tmp_path)

    report = migrate(MigrationConfig(root=tmp_path, apply=True))

    assert report["errors"] == []
    assert not (tmp_path / "data" / "parquet" / "daily_k_none").exists()
    assert not (tmp_path / "data" / "parquet" / "stock_zh_a_hist_none").exists()

    daily_bar = pd.read_parquet(
        tmp_path / "data" / "parquet" / "baostock_cn_stock_daily_bar_unadjusted" / "code=sh.600000" / "data.parquet"
    )
    assert "turnover_rate" in daily_bar.columns
    assert "turn" not in daily_bar.columns
    assert "pct_change" in daily_bar.columns

    akshare_daily_bar = pd.read_parquet(
        tmp_path / "data" / "parquet" / "akshare_cn_stock_daily_bar_unadjusted" / "code=600000" / "data.parquet"
    )
    assert akshare_daily_bar.loc[0, "adjustment"] == "unadjusted"
    assert akshare_daily_bar.loc[0, "source_endpoint"] == "stock_zh_a_hist"

    with duckdb.connect(str(tmp_path / "data" / "duckdb" / "quant.duckdb")) as conn:
        tables = {row[0] for row in conn.execute("show tables").fetchall()}
        assert "pipeline_runs" in tables
        assert "dataset_update_status" in tables
        assert "update_runs" not in tables
        assert "update_status" not in tables
        dataset = conn.execute("select dataset from dataset_update_status").fetchone()[0]
        pipeline = conn.execute("select pipeline from pipeline_checkpoints").fetchone()[0]
        views = {row[0] for row in conn.execute("show tables").fetchall() if row[0].startswith("v_")}

    assert dataset == "baostock_cn_stock_daily_bar_unadjusted"
    assert pipeline == "update_akshare_daily_bar"
    assert "v_baostock_cn_stock_daily_bar_unadjusted" in views
    assert "v_daily_k_none" not in views


def _write_legacy_parquet_layout(root) -> None:
    daily_dir = root / "data" / "parquet" / "daily_k_none" / "code=sh.600000"
    daily_dir.mkdir(parents=True)
    _legacy_baostock_daily_bar().to_parquet(daily_dir / "data.parquet", index=False)

    akshare_dir = root / "data" / "parquet" / "stock_zh_a_hist_none" / "code=600000"
    akshare_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "code": "600000",
                "source_symbol": "600000",
                "open": 8.1,
                "high": 8.3,
                "low": 8.0,
                "close": 8.2,
                "volume": 1000,
                "amount": 8200.0,
                "amplitude": 1.0,
                "pct_chg": 2.5,
                "change_amount": 0.2,
                "turn": 0.1,
                "adjust": "none",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "daily_bar_confirmed",
                "fetched_at": pd.Timestamp("2024-01-02 16:00:00"),
            }
        ]
    ).to_parquet(akshare_dir / "data.parquet", index=False)


def _write_legacy_duckdb(root) -> None:
    duckdb_file = root / "data" / "duckdb" / "quant.duckdb"
    duckdb_file.parent.mkdir(parents=True)
    with duckdb.connect(str(duckdb_file)) as conn:
        conn.execute("create table update_runs(dataset varchar, code varchar)")
        conn.execute("insert into update_runs values ('daily_k_none', 'sh.600000')")
        conn.execute("create table update_status(dataset varchar, code varchar)")
        conn.execute("insert into update_status values ('daily_k_none', 'sh.600000')")
        conn.execute("create table metadata_migrations(version varchar)")
        conn.execute("insert into metadata_migrations values ('legacy')")
        conn.execute("create table pipeline_checkpoints(pipeline varchar, dataset varchar, code varchar)")
        conn.execute(
            "insert into pipeline_checkpoints values ('update_akshare_hist', 'stock_zh_a_hist_none', '600000')"
        )
        conn.execute("create view v_daily_k_none as select 1 as old_view")


def _legacy_baostock_daily_bar() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2024-01-02",
                "code": "sh.600000",
                "open": 8.1,
                "high": 8.3,
                "low": 8.0,
                "close": 8.2,
                "preclose": 8.0,
                "volume": 1000,
                "amount": 8200.0,
                "adjustflag": "3",
                "turn": 0.1,
                "tradestatus": "1",
                "pctChg": 2.5,
                "peTTM": 5.0,
                "pbMRQ": 0.7,
                "psTTM": 1.2,
                "pcfNcfTTM": 3.0,
                "isST": "0",
            }
        ]
    )
