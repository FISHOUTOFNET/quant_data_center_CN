"""Private helpers for write pipeline finalization."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from src.storage.duckdb_store import DuckDBStore
from src.utils.logging import logger


@contextmanager
def _finalize_write_pipeline(
    *,
    store: Any,
    metadata_flush: Callable[[], None] | None = None,
    build_views: bool | Callable[[], bool] = False,
    cleanup_tmp_files: bool | Callable[[], bool] = False,
    duckdb_store_factory: Callable[[], Any] | None = None,
) -> Iterator[None]:
    """Run write-pipeline finalizers without masking the primary failure."""

    primary_error: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        first_finalization_error: BaseException | None = None

        finalizers: list[tuple[str, Callable[[], None]]] = []
        if metadata_flush is not None:
            finalizers.append(("metadata flush", metadata_flush))
        finalizers.append(("store close", store.close))
        should_build_views = build_views() if callable(build_views) else build_views
        if should_build_views:
            finalizers.append(
                (
                    "DuckDB view build",
                    lambda: (
                        duckdb_store_factory() if duckdb_store_factory is not None else DuckDBStore(root=store.root)
                    ).build_views(
                        cleanup_tmp_files=cleanup_tmp_files() if callable(cleanup_tmp_files) else cleanup_tmp_files
                    ),
                )
            )

        for name, finalizer in finalizers:
            try:
                finalizer()
            except BaseException as exc:
                if primary_error is not None:
                    logger.exception("Write pipeline finalization failed during {}", name)
                elif first_finalization_error is None:
                    first_finalization_error = exc
                else:
                    logger.exception("Write pipeline finalization failed during {}", name)

        if primary_error is None and first_finalization_error is not None:
            raise first_finalization_error
