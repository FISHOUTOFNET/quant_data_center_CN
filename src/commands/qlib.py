"""Qlib CLI commands."""

from __future__ import annotations

from pathlib import Path

import click

from src.sources.qlib import sync as qlib_sync_module


def register_qlib_commands(root: click.Group) -> None:
    """Register Qlib commands on the root CLI group."""

    @root.command("sync-qlib")
    @click.option(
        "--source-dir",
        type=click.Path(path_type=Path),
        default=None,
        help=f"Qlib cn_data directory. Defaults to {qlib_sync_module.QLIB_SOURCE_DIR}.",
    )
    @click.option(
        "--target-date", default=None, help="Target trading date, YYYY-MM-DD. Defaults through local calendar."
    )
    @click.option(
        "--force-download",
        is_flag=True,
        help="Download and replace local ~/.qlib even if local data appears current.",
    )
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
    @click.option(
        "--allow-weekday",
        is_flag=True,
        help="Run qlib sync even when today is not Saturday or Sunday.",
    )
    @click.option(
        "--max-runtime-seconds",
        type=float,
        default=None,
        help="Abort qlib sync after this many seconds.",
    )
    @click.option(
        "--workers", type=int, default=None, help="Concurrent workers for Qlib feature conversion and writes."
    )
    def sync_qlib(
        source_dir: Path | None,
        target_date: str | None,
        force_download: bool,
        build_views: bool,
        allow_weekday: bool,
        max_runtime_seconds: float | None,
        workers: int | None,
    ) -> None:
        """Sync local Qlib binary data into project Parquet and DuckDB views."""

        if not allow_weekday and not qlib_sync_module.is_qlib_update_day():
            click.echo("qlib status=skipped_weekday reason=outside_friday_sunday_window")
            return

        result = qlib_sync_module.sync_qlib_data(
            source_dir=source_dir,
            target_date=target_date,
            force_download=force_download,
            build_views=build_views,
            max_runtime_seconds=max_runtime_seconds,
            workers=workers,
        )
        click.echo(
            f"qlib status={result.status} target_date={result.target_date} "
            f"source_latest_date={result.source_latest_date} project_latest_date={result.project_latest_date} "
            f"downloaded={result.downloaded} synced={result.synced}"
        )
