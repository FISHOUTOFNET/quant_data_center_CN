"""DuckDB CLI commands."""

from __future__ import annotations

import click

from src.storage.duckdb_store import DuckDBStore
from src.utils import paths


def register_duckdb_commands(root: click.Group) -> None:
    """Register DuckDB commands on the root CLI group."""

    @root.command("build-duckdb-views")
    def build_views() -> None:
        """Build DuckDB views over current Parquet files."""

        sqls = DuckDBStore().build_views()
        click.echo(f"Built {len(sqls)} views at {paths.DUCKDB_FILE}")
