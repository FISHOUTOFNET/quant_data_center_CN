"""DuckDB CLI commands."""

from __future__ import annotations

import click

from src.storage.duckdb_store import DuckDBStore
from src.storage.metadata_store import migrate_metadata_duckdb


def register_duckdb_commands(root: click.Group) -> None:
    """Register DuckDB commands on the root CLI group."""

    @root.command("build-duckdb-views")
    def build_views() -> None:
        """Build DuckDB views over current Parquet files."""

        store = DuckDBStore()
        sqls = store.build_views()
        click.echo(f"Built {len(sqls)} views at {store.duckdb_file}")

    @root.command("migrate-metadata-duckdb")
    def migrate_metadata() -> None:
        """Migrate legacy pipeline metadata from quant.duckdb to qdc_metadata.duckdb."""

        result = migrate_metadata_duckdb()
        click.echo(f"Metadata DuckDB migration source={result['source']} target={result['target']}")
        if result.get("skipped"):
            click.echo(f"Skipped: {result.get('reason', 'no migration needed')}")
            return
        migrated = result["migrated_rows"]
        if isinstance(migrated, dict):
            for table_name, row_count in migrated.items():
                click.echo(f"{table_name}: {row_count} rows")
