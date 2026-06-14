"""Partition manifest maintenance commands."""

from __future__ import annotations

import click

from src.storage.dataset_catalog import DATASET_CATALOG
from src.storage.manifest_rebuild import rebuild_all_partition_manifests, rebuild_partition_manifest
from src.storage.parquet_store import ParquetStore


def register_manifest_commands(root: click.Group) -> None:
    @root.command("rebuild-partition-manifest")
    @click.option("--dataset", required=True, help="Dataset id to rebuild, or all.")
    @click.option("--include-derived", is_flag=True, help="Also rebuild derived dataset manifests.")
    @click.option("--force", is_flag=True, help="Recompute manifest rows even when they already exist.")
    def rebuild_partition_manifest_command(dataset: str, include_derived: bool, force: bool) -> None:
        """Rebuild dataset partition manifest rows from local Parquet files."""

        store = ParquetStore()
        try:
            if dataset == "all":
                results = rebuild_all_partition_manifests(
                    store=store,
                    include_derived=include_derived,
                    force=force,
                )
            else:
                if dataset not in DATASET_CATALOG:
                    raise click.BadParameter(f"Unsupported dataset: {dataset}")
                results = [
                    rebuild_partition_manifest(
                        store=store,
                        dataset_id=dataset,
                        include_derived=include_derived,
                        force=force,
                    )
                ]
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

        for result in results:
            click.echo(
                " ".join(
                    [
                        f"dataset={result.dataset}",
                        f"partition_count={result.partition_count}",
                        f"updated_count={result.updated_count}",
                        f"skipped_count={result.skipped_count}",
                    ]
                )
            )
