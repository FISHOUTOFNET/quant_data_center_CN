"""Command line interface for the quant data center."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from src.commands.akshare import register_akshare_commands
from src.commands.baostock import register_baostock_commands
from src.commands.daily import register_daily_commands
from src.commands.derived import register_derived_commands
from src.commands.duckdb import register_duckdb_commands
from src.commands.manifest import register_manifest_commands
from src.commands.qlib import register_qlib_commands
from src.utils import paths
from src.utils.logging import logger


def _env_truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def configure_logging(root: Path | None = None, *, runtime_paths: paths.RuntimePaths | None = None) -> paths.RuntimePaths:
    """Configure Loguru sinks using the unified RuntimePaths resolution.

    Subprocesses spawned by ``run-update-daily`` set ``QDC_DISABLE_FILE_LOG=1``
    so they do not re-open the application log; their stdout/stderr is captured
    by the orchestrator into the per-run log file instead. This keeps the
    application log single-writer (only the orchestrator writes to it) and the
    per-run log single-owner (only the orchestrator creates/appends it).
    """

    resolved = runtime_paths or paths.resolve_runtime_paths(root=root)
    file_logging_enabled = not _env_truthy(os.environ.get("QDC_DISABLE_FILE_LOG"))
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    if file_logging_enabled:
        # ``ensure_managed_log_root`` is the single authority that creates the
        # marker authorizing later cleanup. Application logging init is the
        # natural owner because it runs once per process, early, and already
        # creates the log directory. ``log_cleanup`` validates but never
        # creates the marker (P0-7).
        paths.ensure_managed_log_root(resolved.logs_dir)
        resolved.application_log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            resolved.application_log_path,
            level="INFO",
            rotation="10 MB",
            retention="30 days",
            encoding="utf-8",
        )
    return resolved


@click.group()
def cli() -> None:
    """Quant data center CLI."""

    configure_logging()


register_akshare_commands(cli)
register_baostock_commands(cli)
register_daily_commands(cli)
register_derived_commands(cli)
register_duckdb_commands(cli)
register_manifest_commands(cli)
register_qlib_commands(cli)


if __name__ == "__main__":
    cli()
