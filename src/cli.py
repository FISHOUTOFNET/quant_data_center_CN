"""Command line interface for the quant data center."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from src.api.akshare_client import normalize_akshare_code
from src.pipeline.repair_tool import repair as run_repair
from src.pipeline.update_akshare import update_akshare as run_update_akshare
from src.pipeline.update_akshare_daily_bar import update_akshare_daily_bar as run_update_akshare_daily_bar
from src.pipeline.update_akshare_spot import update_akshare_spot as run_update_akshare_spot
from src.pipeline.update_akshare_delist import update_akshare_delist as run_update_akshare_delist
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


def configure_logging(root: Path | None = None) -> None:
    base = (root or paths.ROOT).resolve()
    log_dir = base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(log_dir / "qdc.log", level="INFO", rotation="10 MB", retention="30 days", encoding="utf-8")


@click.group()
def cli() -> None:
    """Quant data center CLI."""

    configure_logging()


@cli.command("update-baostock-daily")
@click.option("--dataset", default="all", show_default=True, help="all or a managed Baostock dataset id, e.g. baostock_cn_stock_daily_bar_unadjusted.")
@click.option("--start", default="1990-01-01", show_default=True, help="Full-mode start date, YYYY-MM-DD.")
@click.option("--code", multiple=True, help="Stock code. Can be repeated. Defaults to active latest baostock_cn_stock_basic snapshot.")
@click.option("--lookback-days", type=int, default=None, help="Trading-day lookback count. Defaults to settings.yaml.")
@click.option("--end", default=None, help="Target date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Update mode.")
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


@cli.command("update-akshare-valuation")
@click.option("--dataset", default="all", show_default=True, help="all/akshare_cn_stock_valuation_eastmoney.")
@click.option("--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Update mode.")
@click.option("--code", multiple=True, callback=_validate_akshare_codes, help="AkShare 6-digit stock code, e.g. 600000. Can be repeated.")
@click.option("--include-inactive", is_flag=True, help="Use the full local AkShare pool, including delisted codes, in partial mode.")
@click.option("--max-tasks", type=int, default=None, help="Maximum AkShare tasks to execute in this run.")
@click.option("--workers", type=int, default=None, help="Concurrent fetch workers for akshare_cn_stock_valuation_eastmoney. Defaults to api.akshare.workers.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def update_akshare(
    dataset: str,
    mode: str,
    code: tuple[str, ...],
    include_inactive: bool,
    max_tasks: int | None,
    workers: int | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Run AkShare crawler dataset updates."""

    records = run_update_akshare(
        dataset=dataset,
        mode=mode,
        code=code,
        include_inactive=include_inactive,
        max_tasks=max_tasks,
        workers=workers,
        resume=resume,
        force=force,
        build_views=build_views,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


@cli.command("update-akshare-delist")
@click.option("--market", default=None, help="Delist market parameter. Uses exchange-specific defaults if not specified.")
@click.option("--snapshot-date", default=None, help="Snapshot date, YYYY-MM-DD. Defaults to today.")
@click.option("--exchange", multiple=True, help="Exchange to fetch: sh or sz. Can be repeated. Defaults to both.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch the snapshot.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def update_akshare_delist(
    market: str | None,
    snapshot_date: str | None,
    exchange: tuple[str, ...],
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Manually update AkShare SH and SZ delisted stock data."""

    exchanges = list(exchange) if exchange else None
    records = run_update_akshare_delist(
        market=market,
        snapshot_date=snapshot_date,
        exchanges=exchanges,
        resume=resume,
        force=force,
        build_views=build_views,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


@cli.command("update-akshare-spot-quote")
@click.option("--end", default=None, help="Target trade date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch the spot snapshot.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def update_akshare_spot(
    end: str | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Run AkShare A-share daily spot snapshot with fallback."""

    records = run_update_akshare_spot(
        end=end,
        resume=resume,
        force=force,
        build_views=build_views,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


@cli.command("update-akshare-daily-bar")
@click.option("--mode", type=click.Choice(["full", "incremental"]), required=True, help="History update mode.")
@click.option("--adjustment", type=click.Choice(["unadjusted", "qfq", "hfq", "all"]), default="unadjusted", show_default=True)
@click.option("--code", multiple=True, callback=_validate_akshare_codes, help="AkShare 6-digit stock code, e.g. 600000. Defaults to the local AkShare pool.")
@click.option("--start", default=None, help="Start date, YYYY-MM-DD. Required for incremental mode.")
@click.option("--end", default=None, help="End date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--max-tasks", type=int, default=None, help="Maximum daily-bar tasks to execute in this run.")
@click.option("--workers", type=int, default=None, help="Concurrent fetch workers for AkShare daily bars.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
def update_akshare_daily_bar(
    mode: str,
    adjustment: str,
    code: tuple[str, ...],
    start: str | None,
    end: str | None,
    max_tasks: int | None,
    workers: int | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Run AkShare daily-bar full or manual incremental update."""

    records = run_update_akshare_daily_bar(
        mode=mode,
        adjustment=adjustment,
        code=code,
        start=start,
        end=end,
        max_tasks=max_tasks,
        workers=workers,
        resume=resume,
        force=force,
        build_views=build_views,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


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



