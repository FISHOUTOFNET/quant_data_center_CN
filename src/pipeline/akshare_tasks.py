"""Task planning for AkShare crawler datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.api.akshare_client import (
    build_code_maps,
    code_to_akshare_symbol,
    report_period_end_date,
    report_period_to_akshare_quarter,
)
from src.storage.dataset_catalog import (
    STOCK_INSTITUTE_HOLD_DATASET,
    STOCK_VALUE_EM_DATASET,
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
    report_period: str | None = None
    code: str | None = None
    active: bool = False


def plan_akshare_tasks(
    config: ConfigManager,
    store: ParquetStore,
    dataset: str = "all",
    mode: str = "partial",
    start_quarter: str | None = None,
    end_quarter: str | None = None,
    code: tuple[str, ...] | list[str] | str | None = None,
    include_inactive: bool = False,
    max_tasks: int | None = None,
    today: date | None = None,
) -> list[AkShareTask]:
    if mode not in {"partial", "full"}:
        raise ValueError(f"Unsupported AkShare update mode: {mode}")

    selected = expand_akshare_selection(dataset)
    tasks: list[AkShareTask] = []
    stock_basic_df = store.read_stock_basic()
    code_maps = build_code_maps(stock_basic_df)
    active_codes = {
        code_to_akshare_symbol(item, code_maps)
        for item in store.stock_basic_codes("active")
    }

    for selected_dataset in selected:
        if selected_dataset == STOCK_INSTITUTE_HOLD_DATASET.name:
            tasks.extend(_stock_institute_hold_tasks(config, store, mode, start_quarter, end_quarter, today))
        elif selected_dataset == STOCK_VALUE_EM_DATASET.name:
            tasks.extend(
                _stock_value_em_tasks(
                    config,
                    store,
                    mode,
                    code,
                    include_inactive,
                    code_maps,
                    active_codes,
                )
            )
        else:
            raise ValueError(f"Unsupported AkShare dataset: {selected_dataset}")

    if max_tasks is not None:
        tasks = tasks[: max(int(max_tasks), 0)]
    return tasks


def _stock_institute_hold_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    start_quarter: str | None,
    end_quarter: str | None,
    today: date | None,
) -> list[AkShareTask]:
    resolved_end = normalize_report_period(end_quarter) if end_quarter else latest_disclosable_quarter(today)
    if mode == "full":
        resolved_start = normalize_report_period(
            start_quarter or str(config.get("datasets.stock_institute_hold.start_quarter", "2005Q1"))
        )
    else:
        lookback = int(config.get("api.akshare.lookback_quarters", 8))
        resolved_start = shift_report_period(resolved_end, -(max(lookback, 1) - 1))

    return [
        AkShareTask(
            dataset=STOCK_INSTITUTE_HOLD_DATASET.name,
            key=period,
            report_period=period,
            start_date=report_period_end_date(period).isoformat(),
            end_date=report_period_end_date(period).isoformat(),
            output_path=store.stock_institute_hold_path(period),
        )
        for period in quarter_range(resolved_start, resolved_end)
    ]


def _stock_value_em_tasks(
    config: ConfigManager,
    store: ParquetStore,
    mode: str,
    code: tuple[str, ...] | list[str] | str | None,
    include_inactive: bool,
    code_maps,
    active_codes: set[str],
) -> list[AkShareTask]:
    if isinstance(code, str):
        raw_codes = [code]
    elif code:
        raw_codes = [str(item) for item in code]
    else:
        active_only = bool(config.get("datasets.stock_value_em.active_only", True))
        raw_codes = _stock_value_em_stock_basic_codes(
            store.read_stock_basic(),
            include_inactive=mode == "full" or include_inactive or not active_only,
        )
        if not raw_codes:
            raise ValueError("No stock codes found in stock_basic data")

    codes = list(dict.fromkeys(code_to_akshare_symbol(item, code_maps) for item in raw_codes))
    tasks: list[AkShareTask] = []
    for stock_code in codes:
        output_path = store.stock_value_em_path(stock_code)
        start_date, end_date = _stock_value_em_date_range(store, stock_code)
        tasks.append(
            AkShareTask(
                dataset=STOCK_VALUE_EM_DATASET.name,
                key=stock_code,
                code=stock_code,
                start_date=start_date,
                end_date=end_date,
                output_path=output_path,
                active=stock_code in active_codes,
            )
        )
    return tasks


def _stock_value_em_date_range(
    store: ParquetStore,
    code: str,
) -> tuple[str | None, str | None]:
    path = store.stock_value_em_path(code)
    if not path.exists():
        return None, None
    df = store.read_stock_value_em(code)
    if df.empty or "date" not in df.columns:
        return None, None
    dates = df["date"]
    if dates.empty:
        return None, None
    min_date = str(dates.min())
    max_date = str(dates.max())
    return min_date, max_date


def _stock_value_em_stock_basic_codes(stock_basic_df, include_inactive: bool) -> list[str]:
    if stock_basic_df.empty:
        return []

    stock_type = stock_basic_df["type"].astype("string").str.strip()
    work = stock_basic_df.loc[stock_type == "1"]
    if not include_inactive:
        status = work["status"].astype("string").str.strip()
        work = work.loc[status == "1"]

    codes = work["code"].astype("string").str.strip()
    codes = codes.loc[codes.notna() & (codes != "")]
    return list(dict.fromkeys(codes.astype(str).tolist()))


def normalize_report_period(value: str) -> str:
    quarter = report_period_to_akshare_quarter(value)
    return f"{quarter[:4]}Q{quarter[-1]}"


def latest_disclosable_quarter(today: date | None = None) -> str:
    current = today or date.today()
    current_quarter = (current.month - 1) // 3 + 1
    if current_quarter == 1:
        return f"{current.year - 1}Q4"
    return f"{current.year}Q{current_quarter - 1}"


def quarter_range(start_quarter: str, end_quarter: str) -> list[str]:
    start_index = _quarter_index(normalize_report_period(start_quarter))
    end_index = _quarter_index(normalize_report_period(end_quarter))
    if start_index > end_index:
        raise ValueError(f"start_quarter must be <= end_quarter: {start_quarter} > {end_quarter}")
    return [_quarter_from_index(index) for index in range(start_index, end_index + 1)]


def shift_report_period(report_period: str, offset: int) -> str:
    return _quarter_from_index(_quarter_index(normalize_report_period(report_period)) + int(offset))


def _quarter_index(report_period: str) -> int:
    value = normalize_report_period(report_period)
    return int(value[:4]) * 4 + int(value[-1]) - 1


def _quarter_from_index(index: int) -> str:
    year = index // 4
    quarter = index % 4 + 1
    return f"{year}Q{quarter}"
