"""Shared pipeline services for provider fetches and metadata batching."""

from __future__ import annotations

from threading import RLock

import pandas as pd

from src.api.market_data import DailyKRequest, MarketDataProvider
from src.pipeline.common import FULL_HISTORY_START_DATE, calendar_covers_range
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


class PipelineMetadataBatch:
    """Batch metadata writes while data files are written immediately."""

    def __init__(self, store: ParquetStore, flush_size: int, count_by: str) -> None:
        if count_by not in {"run", "checkpoint"}:
            raise ValueError(f"Unsupported metadata batch counter: {count_by}")
        self._store = store
        self._flush_size = max(int(flush_size), 1)
        self._count_by = count_by
        self._run_rows: list[dict[str, object]] = []
        self._status_rows: list[dict[str, object]] = []
        self._checkpoint_rows: list[dict[str, object]] = []
        self._lock = RLock()
        self._flush_write_lock = RLock()

    def add(
        self,
        run_row: dict[str, object] | None = None,
        status_row: dict[str, object] | None = None,
        checkpoint: dict[str, object] | None = None,
    ) -> None:
        should_flush = False
        with self._lock:
            if run_row is not None:
                self._run_rows.append(run_row)
            if status_row is not None:
                self._status_rows.append(status_row)
            if checkpoint is not None:
                self._checkpoint_rows.append(checkpoint)
            should_flush = self._pending_count >= self._flush_size
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._flush_write_lock:
            with self._lock:
                if self._pending_count == 0:
                    return
                run_rows = self._run_rows
                status_rows = self._status_rows
                checkpoint_rows = self._checkpoint_rows
                self._run_rows = []
                self._status_rows = []
                self._checkpoint_rows = []
            try:
                self._store.persist_update_metadata(run_rows, status_rows, checkpoint_rows)
            except Exception:
                with self._lock:
                    self._run_rows = [*run_rows, *self._run_rows]
                    self._status_rows = [*status_rows, *self._status_rows]
                    self._checkpoint_rows = [*checkpoint_rows, *self._checkpoint_rows]
                raise

    @property
    def _pending_count(self) -> int:
        if self._count_by == "run":
            return len(self._run_rows)
        return len(self._checkpoint_rows)


def ensure_calendar_range(
    store: ParquetStore,
    provider: MarketDataProvider,
    start_date: str,
    end_date: str,
    fetch_start_date: str | None = None,
    fetch_end_date: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Ensure local calendar covers a date range, fetching via provider if needed."""

    calendar_df = store.read_calendar()
    if calendar_covers_range(calendar_df, start_date, end_date):
        return calendar_df, None

    fetched = provider.query_trade_dates(start_date=fetch_start_date, end_date=fetch_end_date)
    log_api_fetch(
        "calendar",
        "*",
        fetch_start_date or FULL_HISTORY_START_DATE,
        fetch_end_date or "latest",
        fetched,
    )
    store.write_calendar(fetched)
    return store.read_calendar(), fetched


def fetch_stock_basic(provider: MarketDataProvider) -> pd.DataFrame:
    df = provider.query_stock_basic()
    return df


def fetch_adjust_factor(
    provider: MarketDataProvider,
    code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return provider.query_adjust_factor(code=code, start_date=start_date, end_date=end_date)


def fetch_daily_k(
    provider: MarketDataProvider,
    config: ConfigManager,
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return provider.query_daily_k(
        DailyKRequest(
            dataset=dataset,
            code=code,
            start_date=start_date,
            end_date=end_date,
            fields=config.daily_k_fields(),
            frequency=str(config.get("datasets.daily_k.frequency", "d")),
        )
    )


def log_api_fetch(
    dataset: str,
    code: str,
    start_date: str,
    end_date: str,
    df: pd.DataFrame,
) -> None:
    logger.info(
        "API fetch completed dataset={} code={} start_date={} end_date={} rows={}",
        dataset,
        code,
        start_date,
        end_date,
        len(df),
    )
