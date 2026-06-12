"""Baostock CLI commands."""

from __future__ import annotations

import click

from src.commands.records import echo_pipeline_records, raise_for_failed_records
from src.sources.baostock.repair_tool import repair as run_repair
from src.sources.baostock.update_daily import update_daily as run_update_daily
from src.sources.baostock.valuation_percentile import (
    update_baostock_valuation_percentile as run_update_baostock_valuation_percentile,
)


def register_baostock_commands(root: click.Group) -> None:
    """Register Baostock commands on the root CLI group."""

    @root.command("update-baostock-daily")
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
    @click.option(
        "--lookback-days", type=int, default=None, help="Trading-day lookback count. Defaults to settings.yaml."
    )
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
        echo_pipeline_records(records)
        raise_for_failed_records(records, label="Baostock daily update")

    @root.command("update-baostock-valuation-percentile")
    @click.option(
        "--mode",
        type=click.Choice(["partial", "full"]),
        default="partial",
        show_default=True,
        help="Derived update mode.",
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
        echo_pipeline_records(records)
        raise_for_failed_records(records, label="Baostock valuation percentile update")

    @root.command("repair-baostock-daily")
    @click.option("--code", required=True, help="Stock code, e.g. sh.600000.")
    @click.option("--start", required=True, help="Start date, YYYY-MM-DD.")
    @click.option("--end", required=True, help="End date, YYYY-MM-DD.")
    @click.option("--dataset", required=True, help="Baostock daily-bar or adjustment-factor dataset id.")
    @click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
    def repair(code: str, start: str, end: str, dataset: str, provider: str | None, build_views: bool) -> None:
        """Repair a stock/date range."""

        results = run_repair(
            code=code, start=start, end=end, dataset=dataset, provider=provider, build_views=build_views
        )
        for item in results:
            click.echo(
                f"{item['dataset']} {item['code']} replacement_rows={item['replacement_rows']} "
                f"total_rows={item['total_rows']} path={item['path']}"
            )
