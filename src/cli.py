"""Command line interface for the quant data center."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from src.pipeline.repair_tool import repair as run_repair
from src.pipeline.update_akshare import update_akshare as run_update_akshare
from src.pipeline.update_akshare_hist import update_akshare_hist as run_update_akshare_hist
from src.pipeline.update_akshare_spot import update_akshare_spot as run_update_akshare_spot
from src.pipeline.update_akshare_universe import update_akshare_universe as run_update_akshare_universe
from src.pipeline.update_daily import update_daily as run_update_daily
from src.storage.duckdb_store import DuckDBStore
from src.utils import paths
from src.utils.logging import logger


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


@cli.command("update-daily")
@click.option("--dataset", default="all", show_default=True, help="daily_k_none/daily_k_qfq/daily_k_hfq/daily_k_all/adjust_factor/all/stock_basic/calendar.")
@click.option("--start", default="1990-01-01", show_default=True, help="Full-mode start date, YYYY-MM-DD.")
@click.option("--code", multiple=True, help="Stock code. Can be repeated. Defaults to active latest stock_basic snapshot.")
@click.option("--universe", default=None, help="Deprecated universe name in config/universe.yaml.")
@click.option("--lookback-days", type=int, default=None, help="Trading-day lookback count. Defaults to settings.yaml.")
@click.option("--end", default=None, help="Target date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Update mode.")
@click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
@click.option("--build-views/--no-build-views", default=True, show_default=True)
def update_daily(
    dataset: str,
    start: str,
    code: tuple[str, ...],
    universe: str | None,
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
        universe=universe,
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


@cli.command("update-akshare")
@click.option("--dataset", default="all", show_default=True, help="all/stock_value_em.")
@click.option("--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Update mode.")
@click.option("--code", multiple=True, help="AkShare stock code. Accepts 600000/sh.600000/sh600000. Can be repeated.")
@click.option("--include-inactive", is_flag=True, help="Use the full local AkShare pool, including delisted codes, in partial mode.")
@click.option("--max-tasks", type=int, default=None, help="Maximum AkShare tasks to execute in this run.")
@click.option("--workers", type=int, default=None, help="Concurrent fetch workers for stock_value_em. Defaults to api.akshare.workers.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
@click.option("--build-views/--no-build-views", default=True, show_default=True)
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


@cli.command("update-akshare-universe")
@click.option("--market", default="全部", show_default=True, help="stock_info_sh_delist market parameter.")
@click.option("--snapshot-date", default=None, help="Snapshot date, YYYY-MM-DD. Defaults to today.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch the snapshot.")
@click.option("--build-views/--no-build-views", default=True, show_default=True)
def update_akshare_universe(
    market: str,
    snapshot_date: str | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Manually update AkShare SH delist universe data."""

    records = run_update_akshare_universe(
        market=market,
        snapshot_date=snapshot_date,
        resume=resume,
        force=force,
        build_views=build_views,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")


@cli.command("update-akshare-spot")
@click.option("--end", default=None, help="Target trade date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch the spot snapshot.")
@click.option("--build-views/--no-build-views", default=True, show_default=True)
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


@cli.command("update-akshare-hist")
@click.option("--mode", type=click.Choice(["full", "incremental"]), required=True, help="History update mode.")
@click.option("--adjust", type=click.Choice(["none", "qfq", "hfq", "all"]), default="all", show_default=True)
@click.option("--code", multiple=True, help="AkShare stock code. Accepts 600000/sh.600000/sh600000. Defaults to the local AkShare pool.")
@click.option("--start", default=None, help="Start date, YYYY-MM-DD. Required for incremental mode.")
@click.option("--end", default=None, help="End date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--max-tasks", type=int, default=None, help="Maximum hist tasks to execute in this run.")
@click.option("--workers", type=int, default=None, help="Concurrent fetch workers for stock_zh_a_hist.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
@click.option("--build-views/--no-build-views", default=True, show_default=True)
def update_akshare_hist(
    mode: str,
    adjust: str,
    code: tuple[str, ...],
    start: str | None,
    end: str | None,
    max_tasks: int | None,
    workers: int | None,
    resume: bool,
    force: bool,
    build_views: bool,
) -> None:
    """Run AkShare stock_zh_a_hist full or manual incremental update."""

    records = run_update_akshare_hist(
        mode=mode,
        adjust=adjust,
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


@cli.command("repair")
@click.option("--code", required=True, help="Stock code, e.g. sh.600000.")
@click.option("--start", required=True, help="Start date, YYYY-MM-DD.")
@click.option("--end", required=True, help="End date, YYYY-MM-DD.")
@click.option("--dataset", required=True, help="daily_k_none/daily_k_qfq/daily_k_hfq/daily_k_all/adjust_factor.")
@click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
@click.option("--build-views/--no-build-views", default=True, show_default=True)
def repair(code: str, start: str, end: str, dataset: str, provider: str | None, build_views: bool) -> None:
    """Repair a stock/date range."""

    results = run_repair(code=code, start=start, end=end, dataset=dataset, provider=provider, build_views=build_views)
    for item in results:
        click.echo(
            f"{item['dataset']} {item['code']} replacement_rows={item['replacement_rows']} "
            f"total_rows={item['total_rows']} path={item['path']}"
        )


@cli.command("build-views")
def build_views() -> None:
    """Build DuckDB views over current Parquet files."""

    sqls = DuckDBStore().build_views()
    click.echo(f"Built {len(sqls)} views at {paths.DUCKDB_FILE}")


if __name__ == "__main__":
    cli()
