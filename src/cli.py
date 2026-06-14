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


def configure_logging(root: Path | None = None) -> None:
    base = (root or paths.ROOT).resolve()
    configured_log_dir = os.environ.get("QDC_LOG_DIR")
    log_dir = Path(configured_log_dir).expanduser().resolve() if configured_log_dir else base / "logs"
    file_logging_enabled = not _env_truthy(os.environ.get("QDC_DISABLE_FILE_LOG"))
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    if file_logging_enabled:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(log_dir / "qdc.log", level="INFO", rotation="10 MB", retention="30 days", encoding="utf-8")


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
