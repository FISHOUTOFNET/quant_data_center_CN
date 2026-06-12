"""Derived dataset CLI commands."""

from __future__ import annotations

import click

from src.commands.records import raise_for_failed_records
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
    @click.option(
        "--mode",
        type=click.Choice(["incremental", "full"]),
        default="incremental",
        show_default=True,
        help="Build mode. Use full for manual repair of all derived partitions.",
    )
    @click.option(
        "--security-id",
        "security_ids",
        multiple=True,
        help="Rebuild one security_id partition, e.g. SH.600000. May be repeated.",
    )
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
    def build_derived(
        target: tuple[str, ...],
        mode: str,
        security_ids: tuple[str, ...],
        build_views: bool,
    ) -> None:
        """Build canonical and curated derived datasets."""

        try:
            records = run_build_derived(
                targets=target,
                mode=mode,
                security_ids=security_ids,
                build_views=build_views,
            )
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        _echo_derived_records(records)
        raise_for_failed_records(records, label="Derived dataset build")

    @root.command("build-security-master")
    @click.option("--build-duckdb-views/--no-build-duckdb-views", "build_views", default=True, show_default=True)
    def build_security_master(build_views: bool) -> None:
        """Build cn_security_master."""

        try:
            records = run_build_derived(targets=("security_master",), mode="full", build_views=build_views)
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc
        _echo_derived_records(records)
        raise_for_failed_records(records, label="Derived dataset build")


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
