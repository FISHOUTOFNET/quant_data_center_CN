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
from src.pipeline.update_baostock_valuation_percentile import update_baostock_valuation_percentile as run_update_baostock_valuation_percentile
from src.pipeline.update_daily import update_daily as run_update_daily
from src.registry_server import serve_registry
from src.storage.duckdb_store import DuckDBStore
from src.utils import paths
from src.utils.logging import logger


ROOT_OPTION = click.option(
    "--root",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    default=None,
    help="Project root containing config/settings.yaml. Defaults to repository root.",
)
DRY_RUN_OPTION = click.option("--dry-run", is_flag=True, help="Plan selected writes without fetching or writing data.")
MAX_CODES_OPTION = click.option("--max-codes", type=click.IntRange(min=0), default=None, help="Maximum stock codes to plan or execute.")
MAX_TASKS_OPTION = click.option("--max-tasks", type=click.IntRange(min=0), default=None, help="Maximum write tasks to plan or execute.")


def _validate_akshare_codes(ctx: click.Context, param: click.Parameter, value: tuple[str, ...]) -> tuple[str, ...]:
    del ctx, param
    try:
        return tuple(normalize_akshare_code(item) for item in value)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc


def configure_logging(root: Path | None = None, file_logging: bool = True) -> None:
    base = (root or paths.ROOT).resolve()
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    if file_logging:
        log_dir = base / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(log_dir / "qdc.log", level="INFO", rotation="10 MB", retention="30 days", encoding="utf-8")


def _configure_command_logging(root: Path | None, dry_run: bool = False) -> None:
    configure_logging(root, file_logging=not dry_run)


def _print_pipeline_run_summary(command: str, records: list[dict[str, object]]) -> None:
    total = len(records)
    success = 0
    failed = 0
    skipped = 0
    other_counts: dict[str, int] = {}
    failed_codes: list[str] = []
    seen_failed_codes: set[str] = set()

    for item in records:
        status = str(item.get("status", ""))
        if status == "success":
            success += 1
        elif status == "failed":
            failed += 1
            code = item.get("code")
            if code is not None:
                code_text = str(code)
                if code_text not in seen_failed_codes:
                    failed_codes.append(code_text)
                    seen_failed_codes.add(code_text)
        elif status.startswith("skipped"):
            skipped += 1
        else:
            other_counts[status] = other_counts.get(status, 0) + 1

    other_text = ",".join(f"{status}:{count}" for status, count in other_counts.items())
    failed_codes_text = ", ".join(failed_codes)
    summary = f"summary: total={total} success={success} failed={failed} skipped={skipped}"
    if other_text:
        summary = f"{summary} other={other_text}"

    click.echo(summary)
    if failed_codes:
        click.echo(f"failed_codes: {failed_codes_text}")
    logger.info(
        "Pipeline run summary command={} total={} success={} failed={} skipped={} other={} failed_codes={}",
        command,
        total,
        success,
        failed,
        skipped,
        other_text,
        failed_codes_text,
    )


@click.group()
def cli() -> None:
    """Quant data center CLI."""


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
@MAX_TASKS_OPTION
@MAX_CODES_OPTION
@DRY_RUN_OPTION
@ROOT_OPTION
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
    max_tasks: int | None,
    max_codes: int | None,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Run daily lookback update or full historical initialization."""

    _configure_command_logging(root, dry_run)
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
        root=root,
        dry_run=dry_run,
        max_codes=max_codes,
        max_tasks=max_tasks,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    _print_pipeline_run_summary("update-baostock-daily", records)


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
@MAX_CODES_OPTION
@DRY_RUN_OPTION
@ROOT_OPTION
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
    max_codes: int | None,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Run AkShare crawler dataset updates."""

    _configure_command_logging(root, dry_run)
    records = run_update_akshare(
        dataset=dataset,
        mode=mode,
        code=code,
        include_inactive=include_inactive,
        max_codes=max_codes,
        max_tasks=max_tasks,
        workers=workers,
        resume=resume,
        force=force,
        build_views=build_views,
        root=root,
        dry_run=dry_run,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    _print_pipeline_run_summary("update-akshare-valuation", records)


@cli.command("update-akshare-delist")
@click.option("--market", default=None, help="Delist market parameter. Uses exchange-specific defaults if not specified.")
@click.option("--snapshot-date", default=None, help="Snapshot date, YYYY-MM-DD. Defaults to today.")
@click.option("--exchange", multiple=True, help="Exchange to fetch: sh or sz. Can be repeated. Defaults to both.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch the snapshot.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
@MAX_TASKS_OPTION
@DRY_RUN_OPTION
@ROOT_OPTION
def update_akshare_delist(
    market: str | None,
    snapshot_date: str | None,
    exchange: tuple[str, ...],
    resume: bool,
    force: bool,
    build_views: bool,
    max_tasks: int | None,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Manually update AkShare SH and SZ delisted stock data."""

    _configure_command_logging(root, dry_run)
    exchanges = list(exchange) if exchange else None
    records = run_update_akshare_delist(
        market=market,
        snapshot_date=snapshot_date,
        exchanges=exchanges,
        resume=resume,
        force=force,
        build_views=build_views,
        root=root,
        dry_run=dry_run,
        max_tasks=max_tasks,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    _print_pipeline_run_summary("update-akshare-delist", records)


@cli.command("update-akshare-spot-quote")
@click.option("--end", default=None, help="Target trade date, YYYY-MM-DD. Defaults through 18:00 trading-day resolution.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch the spot snapshot.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
@DRY_RUN_OPTION
@ROOT_OPTION
def update_akshare_spot(
    end: str | None,
    resume: bool,
    force: bool,
    build_views: bool,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Run AkShare A-share daily spot snapshot with fallback."""

    _configure_command_logging(root, dry_run)
    records = run_update_akshare_spot(
        end=end,
        resume=resume,
        force=force,
        build_views=build_views,
        root=root,
        dry_run=dry_run,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    _print_pipeline_run_summary("update-akshare-spot-quote", records)


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
@MAX_CODES_OPTION
@DRY_RUN_OPTION
@ROOT_OPTION
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
    max_codes: int | None,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Run AkShare daily-bar full or manual incremental update."""

    _configure_command_logging(root, dry_run)
    records = run_update_akshare_daily_bar(
        mode=mode,
        adjustment=adjustment,
        code=code,
        start=start,
        end=end,
        max_codes=max_codes,
        max_tasks=max_tasks,
        workers=workers,
        resume=resume,
        force=force,
        build_views=build_views,
        root=root,
        dry_run=dry_run,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    _print_pipeline_run_summary("update-akshare-daily-bar", records)


@cli.command("update-baostock-valuation-percentile")
@click.option("--mode", type=click.Choice(["partial", "full"]), default="partial", show_default=True, help="Update mode.")
@click.option("--code", multiple=True, help="Baostock stock code, e.g. sh.600000. Can be repeated.")
@click.option("--start", default=None, help="Partial recompute start date, YYYY-MM-DD.")
@click.option("--workers", type=int, default=None, help="Concurrent compute workers. Defaults to pipeline.baostock_valuation_percentile_workers.")
@click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
@click.option("--force", is_flag=True, help="Ignore checkpoints and recompute selected tasks.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
@MAX_TASKS_OPTION
@MAX_CODES_OPTION
@DRY_RUN_OPTION
@ROOT_OPTION
def update_baostock_valuation_percentile(
    mode: str,
    code: tuple[str, ...],
    start: str | None,
    workers: int | None,
    resume: bool,
    force: bool,
    build_views: bool,
    max_tasks: int | None,
    max_codes: int | None,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Compute Baostock valuation percentile derived dataset."""

    _configure_command_logging(root, dry_run)
    records = run_update_baostock_valuation_percentile(
        mode=mode,
        code=code,
        start=start,
        workers=workers,
        resume=resume,
        force=force,
        build_views=build_views,
        root=root,
        dry_run=dry_run,
        max_codes=max_codes,
        max_tasks=max_tasks,
    )
    for item in records:
        click.echo(f"{item['dataset']} {item['code']} status={item['status']} rows={item['row_count']}")
    _print_pipeline_run_summary("update-baostock-valuation-percentile", records)


@cli.command("repair-baostock-daily")
@click.option("--code", required=True, help="Stock code, e.g. sh.600000.")
@click.option("--start", required=True, help="Start date, YYYY-MM-DD.")
@click.option("--end", required=True, help="End date, YYYY-MM-DD.")
@click.option("--dataset", required=True, help="Baostock daily-bar or adjustment-factor dataset id.")
@click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
@click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
@DRY_RUN_OPTION
@ROOT_OPTION
def repair(
    code: str,
    start: str,
    end: str,
    dataset: str,
    provider: str | None,
    build_views: bool,
    dry_run: bool,
    root: Path | None,
) -> None:
    """Repair a stock/date range."""

    _configure_command_logging(root, dry_run)
    results = run_repair(
        code=code,
        start=start,
        end=end,
        dataset=dataset,
        provider=provider,
        build_views=build_views,
        root=root,
        dry_run=dry_run,
    )
    for item in results:
        click.echo(
            f"{item['dataset']} {item['code']} replacement_rows={item['replacement_rows']} "
            f"total_rows={item['total_rows']} path={item['path']}"
        )


@cli.command("build-duckdb-views")
@DRY_RUN_OPTION
@ROOT_OPTION
def build_views(dry_run: bool, root: Path | None) -> None:
    """Build DuckDB views over current Parquet files."""

    _configure_command_logging(root, dry_run)
    store = DuckDBStore(root=root)
    if dry_run:
        sqls = store.view_sqls()
        click.echo(f"duckdb_views * status=dry_run rows=0 views={len(sqls)} path={store.duckdb_file}")
        return
    sqls = store.build_views()
    click.echo(f"Built {len(sqls)} views at {store.duckdb_file}")


@cli.command("serve-registry")
@click.option("--host", default="127.0.0.1", show_default=True, help="Bind host. Defaults to localhost-only access.")
@click.option("--port", default=8765, show_default=True, type=int, help="Bind port.")
def registry_server(host: str, port: int) -> None:
    """Serve read-only dataset registry and Parquet query endpoints."""

    configure_logging()
    click.echo(f"Serving QDC registry at http://{host}:{port}")
    serve_registry(host=host, port=port)


if __name__ == "__main__":
    cli()



