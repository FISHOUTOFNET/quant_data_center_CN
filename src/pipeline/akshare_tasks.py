"""Task planning for AkShare crawler datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.api.akshare_client import (
    normalize_akshare_code,
)
from src.pipeline.akshare_universe import latest_active_akshare_codes, resolve_akshare_universe_codes
from src.storage.dataset_catalog import (
    AKSHARE_VALUATION_EASTMONEY_DATASET,
    expand_akshare_selection,
)
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


@dataclass(frozen=True)
class AkShareTask:
    dataset: str
    key: str
    start_date: str | None
    end_date: str | None
    output_path: Path
    code: str | None = None
    active: bool = False


def plan_akshare_tasks(
    config: ConfigManager,
    store: ParquetStore,
    dataset: str = "all",
    mode: str = "partial",
    code: tuple[str, ...] | list[str] | str | None = None,
    include_inactive: bool = False,
    max_tasks: int | None = None,
) -> list[AkShareTask]:
    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported AkShare update mode: {mode}")

    selected = expand_akshare_selection(dataset)
    tasks: list[AkShareTask] = []
    active_codes = latest_active_akshare_codes(store)

    for selected_dataset in selected:
        if selected_dataset == AKSHARE_VALUATION_EASTMONEY_DATASET.name:
            tasks.extend(
                _akshare_cn_stock_valuation_eastmoney_tasks(
                    config,
                    store,
                    mode,
                    code,
                    include_inactive,
                    active_codes,
                )
            )
        else:
            raise ValueError(f"Unsupported AkShare dataset: {selected_dataset}")

    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def _akshare_cn_stock_valuation_eastmoney_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    code: tuple[str, ...] | list[str] | str | None,
    include_inactive: bool,
    active_codes: set[str],
) -> list[AkShareTask]:
    if isinstance(code, str):
        codes = [normalize_akshare_code(code)]
    elif code:
        codes = [normalize_akshare_code(item) for item in code]
    else:
        active_only = bool(config.get("datasets.akshare_cn_stock_valuation_eastmoney.active_only", True))
        codes = resolve_akshare_universe_codes(
            store,
            include_delisted=mode == "full" or include_inactive or not active_only,
            context="akshare_cn_stock_valuation_eastmoney",
        )

    codes = list(dict.fromkeys(item for item in codes if item))
    if not codes:
        raise ValueError("No AkShare stock codes found for akshare_cn_stock_valuation_eastmoney")
    tasks: list[AkShareTask] = []
    for stock_code in codes:
        output_path = store.akshare_cn_stock_valuation_eastmoney_path(stock_code)
        start_date, end_date = _akshare_cn_stock_valuation_eastmoney_date_range(store, stock_code)
        tasks.append(
            AkShareTask(
                dataset=AKSHARE_VALUATION_EASTMONEY_DATASET.name,
                key=stock_code,
                code=stock_code,
                start_date=start_date,
                end_date=end_date,
                output_path=output_path,
                active=stock_code in active_codes if active_codes else code is not None,
            )
        )
    return tasks


def _akshare_cn_stock_valuation_eastmoney_date_range(
    store: ParquetStore,
    code: str,
) -> tuple[str | None, str | None]:
    path = store.akshare_cn_stock_valuation_eastmoney_path(code)
    if not path.exists():
        return None, None
    df = store.read_akshare_cn_stock_valuation_eastmoney(code)
    if df.empty or "date" not in df.columns:
        return None, None
    dates = df["date"]
    if dates.empty:
        return None, None
    min_date = str(dates.min())
    max_date = str(dates.max())
    return min_date, max_date

