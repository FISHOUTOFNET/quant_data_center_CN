"""Utilities for performance benchmarking."""

from __future__ import annotations

import json
import random
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from src.storage.dataset_catalog import daily_k_definition
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager
from src.utils.logging import logger


def generate_test_codes(count: int = 100) -> list[str]:
    codes = []
    for i in range(count):
        if i % 2 == 0:
            codes.append(f"sh.{600000 + i}")
        else:
            codes.append(f"sz.{1 + i // 2:06d}")
    return codes


def generate_daily_k_dataframe(
    code: str,
    start_date: str,
    end_date: str,
    rows: int | None = None,
) -> pd.DataFrame:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    if rows is None:
        delta = end_dt - start_dt
        rows = delta.days + 1

    dates = []
    current = start_dt
    while len(dates) < rows and current <= end_dt:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    if len(dates) < rows:
        dates = dates[:rows]

    data = []
    base_price = random.uniform(10.0, 100.0)
    for i, d in enumerate(dates[:rows]):
        change = random.uniform(-0.1, 0.1)
        open_price = base_price * (1 + random.uniform(-0.02, 0.02))
        close_price = base_price * (1 + change)
        high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.02))
        low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.02))
        volume = random.randint(100000, 10000000)
        amount = volume * (open_price + close_price) / 2

        data.append(
            {
                "date": d,
                "code": code,
                "open": f"{open_price:.2f}",
                "high": f"{high_price:.2f}",
                "low": f"{low_price:.2f}",
                "close": f"{close_price:.2f}",
                "preclose": f"{base_price:.2f}",
                "volume": str(volume),
                "amount": f"{amount:.2f}",
                "adjustflag": "3",
                "turn": f"{random.uniform(0.5, 5.0):.2f}",
                "tradestatus": "1",
                "pctChg": f"{change * 100:.2f}",
                "peTTM": f"{random.uniform(10.0, 100.0):.2f}",
                "pbMRQ": f"{random.uniform(1.0, 10.0):.2f}",
                "psTTM": f"{random.uniform(1.0, 10.0):.2f}",
                "pcfNcfTTM": f"{random.uniform(1.0, 20.0):.2f}",
                "isST": "0",
            }
        )
        base_price = close_price

    return pd.DataFrame(data[:rows])


def generate_adjust_factor_dataframe(
    code: str,
    start_date: str,
    end_date: str,
    num_factors: int = 10,
) -> pd.DataFrame:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_dt - start_dt

    factor_dates = []
    for i in range(num_factors):
        days = random.randint(0, delta.days)
        factor_date = start_dt + timedelta(days=days)
        if factor_date.weekday() < 5:
            factor_dates.append(factor_date.strftime("%Y-%m-%d"))

    factor_dates = sorted(set(factor_dates))[:num_factors]

    data = []
    cumulative_factor = 1.0
    for factor_date in factor_dates:
        adjustment = random.uniform(0.8, 1.2)
        cumulative_factor *= adjustment

        data.append(
            {
                "code": code,
                "dividOperateDate": factor_date,
                "foreAdjustFactor": f"{cumulative_factor:.6f}",
                "backAdjustFactor": f"{1.0 / cumulative_factor:.6f}",
                "adjustFactor": f"{adjustment:.6f}",
            }
        )

    return pd.DataFrame(data)


def generate_stock_basic_dataframe(codes: list[str]) -> pd.DataFrame:
    data = []
    for code in codes:
        data.append(
            {
                "code": code,
                "code_name": f"Stock_{code.replace('.', '_')}",
                "ipoDate": "2020-01-01",
                "outDate": "",
                "type": "1",
                "status": "1",
            }
        )
    return pd.DataFrame(data)


def generate_calendar_dataframe(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    data = []
    current = start_dt
    while current <= end_dt:
        is_trading = "1" if current.weekday() < 5 else "0"
        data.append(
            {
                "calendar_date": current.strftime("%Y-%m-%d"),
                "is_trading_day": is_trading,
            }
        )
        current += timedelta(days=1)

    return pd.DataFrame(data)


class BenchmarkEnvironment:
    def __init__(self, root: Path, codes: list[str] | None = None):
        self.root = root
        self.codes = codes or generate_test_codes(100)
        self.store = ParquetStore(root=root)
        self.config = ConfigManager(root=root)

    def setup(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.store.ensure_layout()
        self._write_settings()
        self._write_calendar()

    def teardown(self) -> None:
        self.store.close()
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)

    def _write_settings(self) -> None:
        config_dir = self.root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)

        settings = {
            "project": {"name": "quant_data_center", "timezone": "Asia/Shanghai"},
            "paths": {
                "data_dir": "data",
                "raw_dir": "data/raw",
                "parquet_dir": "data/parquet",
                "metadata_dir": "data/metadata",
                "duckdb_dir": "data/duckdb",
                "logs_dir": "logs",
            },
            "api": {
                "provider": "baostock",
                "baostock": {
                    "adjustflag_map": {"none": "3", "qfq": "2", "hfq": "1"}
                },
            },
            "datasets": {
                "daily_k": {
                    "names": ["daily_k_none", "daily_k_qfq", "daily_k_hfq"],
                    "fields": "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                    "frequency": "d",
                },
                "stock_basic": {
                    "fields": "code,code_name,ipoDate,outDate,type,status"
                },
                "calendar": {"fields": "calendar_date,is_trading_day"},
                "adjust_factor": {
                    "fields": "code,dividOperateDate,foreAdjustFactor,backAdjustFactor,adjustFactor"
                },
            },
            "pipeline": {
                "lookback_days": 30,
                "raw_cache_days": 7,
                "max_retries": 3,
                "default_code": "sh.600000",
                "metadata_flush_size": 200,
            },
            "storage": {"duckdb_file": "data/duckdb/quant.duckdb"},
            "logging": {"file": "logs/qdc.log"},
        }

        settings_path = config_dir / "settings.yaml"
        with settings_path.open("w", encoding="utf-8") as f:
            import yaml

            yaml.dump(settings, f, default_flow_style=False)

        universe = {"universe": {"default": self.codes[:10]}}
        universe_path = config_dir / "universe.yaml"
        with universe_path.open("w", encoding="utf-8") as f:
            import yaml

            yaml.dump(universe, f, default_flow_style=False)

    def _write_calendar(self) -> None:
        calendar_df = generate_calendar_dataframe("2020-01-01", "2024-12-31")
        self.store.write_calendar(calendar_df)

    def populate_test_data(
        self,
        codes: list[str] | None = None,
        start_date: str = "2024-01-01",
        end_date: str = "2024-01-31",
        include_adjust_factors: bool = True,
    ) -> None:
        codes = codes or self.codes[:10]

        stock_basic_df = generate_stock_basic_dataframe(codes)
        self.store.write_stock_basic(stock_basic_df)

        for code in codes:
            daily_df = generate_daily_k_dataframe(code, start_date, end_date)
            self.store.write_daily_k("daily_k_none", code, daily_df)

            if include_adjust_factors:
                factor_df = generate_adjust_factor_dataframe(code, start_date, end_date)
                self.store.write_adjust_factor(code, factor_df)


class BenchmarkReporter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results: dict[str, Any] = {}

    def add_result(self, name: str, data: dict[str, Any]) -> None:
        self.results[name] = data

    def save_report(self, filename: str = "benchmark_report.json") -> Path:
        report_path = self.output_dir / filename
        with report_path.open("w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2, default=str)
        return report_path

    def generate_summary(self) -> str:
        lines = ["=" * 80, "BENCHMARK SUMMARY", "=" * 80, ""]

        for name, data in self.results.items():
            lines.append(f"\n{name}:")
            lines.append("-" * 40)
            for key, value in data.items():
                if isinstance(value, float):
                    lines.append(f"  {key}: {value:.4f}")
                else:
                    lines.append(f"  {key}: {value}")

        return "\n".join(lines)


def run_benchmark_suite(
    benchmarks: list[tuple[str, Callable[[], dict[str, Any]]]],
    reporter: BenchmarkReporter,
) -> None:
    for name, benchmark_func in benchmarks:
        logger.info("Running benchmark: {}", name)
        try:
            start = time.perf_counter()
            result = benchmark_func()
            elapsed = time.perf_counter() - start
            result["total_elapsed"] = elapsed
            reporter.add_result(name, result)
            logger.info("Benchmark {} completed in {:.3f}s", name, elapsed)
        except Exception as e:
            logger.exception("Benchmark {} failed", name)
            reporter.add_result(name, {"error": str(e)})
