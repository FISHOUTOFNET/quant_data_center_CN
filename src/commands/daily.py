"""Daily workflow CLI commands."""

from __future__ import annotations

from pathlib import Path

import click

from src.tools.run_update_daily import RunDailyUpdateLockError, StateFileError, run_daily_update
from src.utils import paths


def register_daily_commands(root: click.Group) -> None:
    """Register daily workflow commands on the root CLI group."""

    @root.command("run-update-daily")
    @click.option(
        "--force",
        is_flag=True,
        help="Ignore workflow success state and rerun selected steps; corrupt state is reset.",
    )
    @click.option(
        "--ignore-state",
        is_flag=True,
        help="Do not skip steps from workflow success state, but still read and update the state file.",
    )
    @click.option("--start-at", default=None, help="Start at a specific daily step id, e.g. baostock-qfq.")
    @click.option("--as-of-date", default=None, help="Candidate date for market-date resolution, YYYY-MM-DD.")
    @click.option("--market-date", default=None, help="Override resolved market date for repair/backfill, YYYY-MM-DD.")
    @click.option("--state-file", type=click.Path(path_type=Path), default=None, help="Daily step state JSON path.")
    @click.option("--run-log", type=click.Path(path_type=Path), default=None, help="Run log path.")
    def run_update_daily_orchestrator(
        force: bool,
        ignore_state: bool,
        start_at: str | None,
        as_of_date: str | None,
        market_date: str | None,
        state_file: Path | None,
        run_log: Path | None,
    ) -> None:
        """Run the resumable daily update workflow."""

        try:
            exit_code = run_daily_update(
                root=paths.ROOT,
                force=force,
                ignore_state=ignore_state,
                start_at=start_at,
                as_of_date=as_of_date,
                market_date=market_date,
                state_file=state_file,
                run_log=run_log,
            )
        except StateFileError as exc:
            raise click.ClickException(str(exc)) from exc
        except RunDailyUpdateLockError as exc:
            raise click.ClickException(str(exc)) from exc
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc
        if exit_code != 0:
            raise click.ClickException(f"Daily update failed with exit code {exit_code}")
