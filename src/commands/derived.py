"""Derived dataset CLI commands."""

from __future__ import annotations

import click

from src.sources.derived.update import build_derived_datasets as run_build_derived


def register_derived_commands(root: click.Group) -> None:
    """Register derived dataset commands on the root CLI group."""

    @root.command("build-derived")
    @click.option(
        "--target",
        multiple=True,
        type=click.Choice(["security_master", "daily_bar", "valuation", "all"]),
        default=("all",),
    )
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
    def build_derived(target: tuple[str, ...], build_views: bool) -> None:
        """Build canonical and curated derived datasets."""

        try:
            records = run_build_derived(targets=target, build_views=build_views)
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        _echo_derived_records(records)

    @root.command("build-security-master")
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
    def build_security_master(build_views: bool) -> None:
        """Build cn_security_master."""

        try:
            records = run_build_derived(targets=("security_master",), build_views=build_views)
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        _echo_derived_records(records)


def _echo_derived_records(records: list[dict[str, object]]) -> None:
    for item in records:
        fields = [
            f"{item.get('dataset')} status={item.get('status')}",
            f"rows={item.get('rows', 0)}",
        ]
        for key in ("active", "delisted", "partitions"):
            if key in item:
                fields.append(f"{key}={item[key]}")
        click.echo(" ".join(fields))
