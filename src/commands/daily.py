"""Daily workflow CLI commands."""

from __future__ import annotations

from pathlib import Path

import click

from src.tools.run_update_daily import StateFileError, run_daily_update
from src.utils import paths


def register_daily_commands(root: click.Group) -> None:
    """Register daily workflow commands on the root CLI group."""

    @root.command("run-update-daily")
    @click.option("--force", is_flag=True, help="Ignore saved daily step state and run from the beginning.")
    @click.option("--start-at", default=None, help="Start at a specific daily step id, e.g. baostock-qfq.")
    @click.option("--state-file", type=click.Path(path_type=Path), default=None, help="Daily step state JSON path.")
    @click.option("--run-log", type=click.Path(path_type=Path), default=None, help="Run log path.")
    def run_update_daily_orchestrator(
        force: bool,
        start_at: str | None,
        state_file: Path | None,
        run_log: Path | None,
    ) -> None:
        """Run the resumable daily update workflow."""

        try:
            exit_code = run_daily_update(
                root=paths.ROOT,
                force=force,
                start_at=start_at,
                state_file=state_file,
                run_log=run_log,
            )
        except StateFileError as exc:
            raise click.ClickException(str(exc)) from exc
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc
        if exit_code != 0:
            raise click.ClickException(f"Daily update failed with exit code {exit_code}")
