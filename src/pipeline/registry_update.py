"""End-of-run registry refresh helpers."""

from __future__ import annotations

from src.pipeline.lifecycle import refresh_dirty_registry
from src.storage.parquet_store import ParquetStore


def refresh_registry_after_run(
    store: ParquetStore,
    records: object = None,
) -> None:
    """Refresh registry once after a pipeline or repair command has finished writes."""

    del records
    refresh_dirty_registry(store)
