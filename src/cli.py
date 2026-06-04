"""Command line interface for the quant data center."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from src.api.akshare_client import normalize_akshare_code
from src.pipeline import qlib_sync as qlib_sync_module
from src.pipeline.akshare import AkShareUpdateRequest
from src.pipeline.akshare import update_akshare as run_update_akshare
from src.pipeline.baostock_valuation_percentile import (
    update_baostock_valuation_percentile as run_update_baostock_valuation_percentile,
)
from src.pipeline.repair_tool import repair as run_repair
from src.pipeline.update_daily import update_daily as run_update_daily
from src.registry_server import serve_registry
from src.storage.duckdb_store import DuckDBStore
from src.utils import paths
from src.utils.logging import logger


def _validate_akshare_codes(ctx: click.Context, param: click.Parameter, value: tuple[str, ...]) -> tuple[str, ...]:
    del ctx, param
    try:
        return tuple(normalize_akshare_code(item) for item in value)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc


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


@cli.group("akshare")
def akshare() -> None:
    """AkShare update commands."""


@akshare.command("update")
@click.option(
    "--target",
    type=click.Choice(
        [
            "valuation",
            "capital_structure",
            "daily_bar",
            "spot_quote",
            "delist",
            "report_disclosure",
            "yysj_em",
            "financial_report",
            "all",
        ]
    ),
    default="valuation",
    show_default=True,
)
@click.option("--mode", type=click.Choice(["partial", "full", "incremental"]), default="partial", show_default=True)
@click.option("--adjustment", type=click.Choice(["unadjusted", "qfq", "hfq", "all"]), default=None)
@click.option(
    "--code",
    multiple=True,
    callback=_validate_akshare_codes,
    help="AkShare 6-digit stock code, e.g. 600000. Can be repeated.",
)
@click.option(
    "--include-inactive",
    is_flag=True,
    help="Use the full local AkShare pool, including delisted codes, in partial valuation mode.",
)
@click.option(
    "--market", default=None, help="Delist market parameter. Uses exchange-specific defaults if not specified."
)
@click.option(
    "--period",
    multiple=True,
    help="Report disclosure period, e.g. 2025年报. Can be repeated.",
)
@click.option("--start", default=None, help="Start date, YYYY-MM-DD. Required for daily_bar incremental mode.")
@click.option(
    "--end", default=None, help="End or snapshot/trade date, YYYY-MM-DD. Defaults through target-specific resolution."
)
@click.option("--max-tasks", type=int, default=None, help="Maximum AkShare tasks to execute in this run.")
@click.option("--workers", type=int, default=None, help="Concurrent fetch workers for per-code AkShare targets.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch selected tasks.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def akshare_update(
    target: str,
    mode: str,
    adjustment: str | None,
    code: tuple[str, ...],
    include_inactive: bool,
    market: str | None,
    period: tuple[str, ...],
    start: str | None,
    end: str | None,
    max_tasks: int | None,
    workers: int | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Run AkShare dataset updates."""

    try:
        request = AkShareUpdateRequest(
            target=target,
            mode=mode,
            adjustment=adjustment,
            code=code,
            include_inactive=include_inactive,
            market=market,
            period=period,
            start=start,
            end=end,
            max_tasks=max_tasks,
            workers=workers,
            resume=resume,
            force=force,
            build_views=build_views,
        )
        records = run_update_akshare(request)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    failed = [item for item in records if str(item.get("status")) == "failed"]
    if failed:
        raise click.ClickException(f"AkShare update completed with {len(failed)} failed task(s)")


@cli.command("update-baostock-daily")
@click.option(
    "--dataset",
    default="baostock_cn_stock_daily_bar_unadjusted",
    show_default=True,
    help="Managed Baostock dataset id. Use all for explicit full Baostock daily/factor targets.",
)
@click.option("--start", default="1990-01-01", show_default=True, help="Full-mode start date, YYYY-MM-DD.")
@click.option(
    "--code",
    multiple=True,
    help="Stock code. Can be repeated. Defaults to active latest baostock_cn_stock_basic snapshot.",
)
@click.option("--lookback-days", type=int, default=None, help="Trading-day lookback count. Defaults to settings.yaml.")
@click.option("--end", default=None, help="Target date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option(
    "--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Update mode."
)
@click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def update_daily(
    dataset: str,
    start: str,
    code: tuple[str, ...],
    lookback_days: int | None,
    end: str | None,
    mode: str,
    provider: str | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Run daily lookback update or full historical initialization."""

    records = run_update_daily(
        dataset=dataset,
        start=start,
        code=code,
        lookback_days=lookback_days,
        end=end,
        mode=mode,
        provider=provider,
        build_views=build_views,
        resume=resume,
        force=force,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


@cli.command("update-baostock-valuation-percentile")
@click.option(
    "--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Derived update mode."
)
@click.option(
    "--code",
    multiple=True,
    help="Baostock stock code, e.g. sh.600000. Can be repeated. Defaults to local source partitions.",
)
@click.option("--start", default=None, help="Partial force recompute start date, YYYY-MM-DD.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints. Required with --start in partial mode.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def update_baostock_valuation_percentile(
    mode: str,
    code: tuple[str, ...],
    start: str | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Build Baostock valuation percentile derived dataset from local daily bars."""

    records = run_update_baostock_valuation_percentile(
        mode=mode,
        code=code,
        start=start,
        resume=resume,
        force=force,
        build_views=build_views,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


@cli.command("sync-qlib")
@click.option(
    "--source-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=f"Qlib cn_data directory. Defaults to {qlib_sync_module.QLIB_SOURCE_DIR}.",
)
@click.option("--target-date", default=None, help="Target trading date, YYYY-MM-DD. Defaults through local calendar.")
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
@click.option("--workers", type=int, default=None, help="Concurrent workers for Qlib feature conversion and writes.")
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


@cli.command("repair-baostock-daily")
@click.option("--code", required=True, help="Stock code, e.g. sh.600000.")
@click.option("--start", required=True, help="Start date, YYYY-MM-DD.")
@click.option("--end", required=True, help="End date, YYYY-MM-DD.")
@click.option("--dataset", required=True, help="Baostock daily-bar or adjustment-factor dataset id.")
@click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def repair(code: str, start: str, end: str, dataset: str, provider: str | None, build_views: bool) -> None:
    """Repair a stock/date range."""

    results = run_repair(code=code, start=start, end=end, dataset=dataset, provider=provider, build_views=build_views)
    for item in results:
        click.echo(
            f"{item['dataset']} {item['code']} replacement_rows={item['replacement_rows']} "
            f"total_rows={item['total_rows']} path={item['path']}"
        )


@cli.command("build-duckdb-views")
def build_views() -> None:
    """Build DuckDB views over current Parquet files."""

    sqls = DuckDBStore().build_views()
    click.echo(f"Built {len(sqls)} views at {paths.DUCKDB_FILE}")


@cli.command("serve-registry")
@click.option("--host", default="127.0.0.1", show_default=True, help="Bind host. Defaults to localhost-only access.")
@click.option("--port", default=8765, show_default=True, type=int, help="Bind port.")
def registry_server(host: str, port: int) -> None:
    """Serve read-only dataset registry and Parquet query endpoints."""

    click.echo(f"Serving QDC registry at http://{host}:{port}")
    serve_registry(host=host, port=port)


if __name__ == "__main__":
    cli()
