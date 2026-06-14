"""Baostock CLI commands."""

from __future__ import annotations

from datetime import date, datetime

import click

from src.commands.records import echo_pipeline_records, raise_for_failed_records
from src.pipeline.common import date_iso, default_candidate_date
from src.sources.baostock.adjustments import UNADJUSTED_DAILY_DATASET
from src.sources.baostock.market_session import should_run_adjusted_market_session
from src.sources.baostock.market_session_manifest import write_baostock_market_session_manifest
from src.sources.baostock.repair_tool import repair as run_repair
from src.sources.baostock.update_daily import update_daily as run_update_daily
from src.sources.baostock.update_daily_targets import BAOSTOCK_MARKET_SESSION_DAILY_TARGET
from src.sources.baostock.valuation_percentile import (
    update_baostock_valuation_percentile as run_update_baostock_valuation_percentile,
)
from src.utils.config_mgr import ConfigManager


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

    @root.command("update-baostock-market-session")
    @click.option("--end", default=None, help="Target date, YYYY-MM-DD. Defaults like update-baostock-daily.")
    @click.option("--natural-date", default=None, help="Workflow natural date, YYYY-MM-DD.")
    @click.option("--candidate-date", default=None, help="Workflow candidate date, YYYY-MM-DD.")
    @click.option("--market-date", default=None, help="Workflow resolved market date, YYYY-MM-DD.")
    @click.option("--market-date-overridden/--no-market-date-overridden", default=False, show_default=True)
    @click.option(
        "--code",
        multiple=True,
        help="Stock code. Can be repeated. Defaults to active latest baostock_cn_stock_basic snapshot.",
    )
    @click.option(
        "--lookback-days", type=int, default=None, help="Trading-day lookback count. Defaults to settings.yaml."
    )
    @click.option("--provider", default=None, help="Data provider name. Defaults to api.provider.")
    @click.option("--resume/--no-resume", default=True, show_default=True, help="Resume from successful checkpoints.")
    @click.option("--force", is_flag=True, help="Ignore checkpoints and re-fetch all selected tasks.")
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=False, show_default=True)
    def update_baostock_market_session(
        end: str | None,
        natural_date: str | None,
        candidate_date: str | None,
        market_date: str | None,
        market_date_overridden: bool,
        code: tuple[str, ...],
        lookback_days: int | None,
        provider: str | None,
        resume: bool,
        force: bool,
        build_views: bool,
    ) -> None:
        """Run the aggregated Baostock market-session update."""

        natural_dt, candidate_dt, market_dt = _resolve_market_session_dates(
            end=end,
            natural_date=natural_date,
            candidate_date=candidate_date,
            market_date=market_date,
        )
        adjusted_session = should_run_adjusted_market_session(
            natural_dt,
            candidate_dt,
            market_dt,
            market_date_overridden=market_date_overridden,
        )
        if adjusted_session:
            dataset = BAOSTOCK_MARKET_SESSION_DAILY_TARGET
            session_mode = "adjusted_market_session"
        else:
            dataset = UNADJUSTED_DAILY_DATASET
            session_mode = "unadjusted_only"

        started_at = datetime.now()
        records = run_update_daily(
            dataset=dataset,
            code=code,
            lookback_days=lookback_days,
            end=end or market_dt.isoformat(),
            mode="partial",
            provider=provider,
            build_views=build_views,
            resume=resume,
            force=force,
        )
        ended_at = datetime.now()
        write_baostock_market_session_manifest(
            records,
            market_date=market_dt.isoformat(),
            session_mode=session_mode,
            started_at=started_at,
            ended_at=ended_at,
        )
        echo_pipeline_records(records)
        raise_for_failed_records(records, label="Baostock market-session update")

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


def _resolve_market_session_dates(
    *,
    end: str | None,
    natural_date: str | None,
    candidate_date: str | None,
    market_date: str | None,
) -> tuple[date, date, date]:
    """Resolve workflow dates for manual and orchestrated market-session runs."""

    fallback = date_iso(market_date or end) if market_date or end else default_candidate_date(ConfigManager())
    market_dt = _parse_date(market_date or fallback, "market_date")
    candidate_dt = _parse_date(candidate_date or fallback, "candidate_date")
    natural_dt = _parse_date(natural_date or date.today().isoformat(), "natural_date")
    return natural_dt, candidate_dt, market_dt


def _parse_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(date_iso(value), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise click.ClickException(f"Invalid {field_name}: {value!r}; expected YYYY-MM-DD") from exc
