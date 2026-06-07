"""AkShare CLI commands."""

from __future__ import annotations

import click

from src.sources.akshare.client import normalize_akshare_code
from src.sources.akshare.pipeline import AkShareUpdateRequest
from src.sources.akshare.pipeline import update_akshare as run_update_akshare
from src.sources.akshare.pipeline.registry import target_choices


def _validate_akshare_codes(ctx: click.Context, param: click.Parameter, value: tuple[str, ...]) -> tuple[str, ...]:
    del ctx, param
    try:
        return tuple(normalize_akshare_code(item) for item in value)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc


def register_akshare_commands(root: click.Group) -> None:
    """Register AkShare commands on the root CLI group."""

    @root.group("akshare")
    def akshare() -> None:
        """AkShare update commands."""

    @akshare.command("update")
    @click.option(
        "--target",
        type=click.Choice(target_choices(include_all=True)),
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
        "--market",
        default=None,
        help="Market/symbol parameter for delist, report_disclosure, and yysj_em.",
    )
    @click.option(
        "--period",
        multiple=True,
        help="Report period, e.g. 2025年报. Applies to report_disclosure, yysj_em, and yjyg_em. Can be repeated.",
    )
    @click.option("--start", default=None, help="Start date, YYYY-MM-DD. Required for daily_bar incremental mode.")
    @click.option(
        "--end",
        default=None,
        help="End or snapshot/trade date, YYYY-MM-DD. Defaults through target-specific resolution.",
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
