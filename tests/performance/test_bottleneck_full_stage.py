from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

from src.sources.baostock.update_daily import update_daily

DATASET_PARTITIONED_DIRS = {
    "baostock_cn_stock_daily_bar_unadjusted",
    "baostock_cn_stock_daily_bar_qfq",
    "baostock_cn_stock_daily_bar_hfq",
    "baostock_cn_stock_adjustment_factor",
}


def _copy_root_for_diagnostics(source: Path, destination: Path, *, codes: list[str]) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    _copy_if_exists(source / "config", destination / "config")
    _copy_if_exists(source / "data" / "duckdb", destination / "data" / "duckdb")
    parquet_source = source / "data" / "parquet"
    parquet_destination = destination / "data" / "parquet"
    if not parquet_source.exists():
        return
    for child in parquet_source.iterdir():
        if child.name in DATASET_PARTITIONED_DIRS:
            for code in codes:
                _copy_if_exists(child / f"code={code}", parquet_destination / child.name / f"code={code}")
        else:
            _copy_if_exists(child, parquet_destination / child.name)


def run_stage(
    *,
    root: Path,
    dataset: str,
    codes: list[str],
    end: str,
    workers: int,
    metadata_flush_size: int,
    build_views: bool,
) -> dict[str, object]:
    _update_pipeline_config(root, workers=workers, metadata_flush_size=metadata_flush_size)
    started = time.perf_counter()
    records = update_daily(
        dataset=dataset,
        mode="partial",
        end=end,
        code=tuple(codes),
        root=root,
        build_views=build_views,
    )
    elapsed = time.perf_counter() - started
    return {
        "dataset": dataset,
        "codes": codes,
        "workers": workers,
        "metadata_flush_size": metadata_flush_size,
        "wall_seconds": round(elapsed, 3),
        "records": len(records),
        "success": sum(1 for row in records if row.get("status") == "success"),
        "failed": sum(1 for row in records if row.get("status") == "failed"),
        "records_per_second": round(len(records) / elapsed, 3) if elapsed else 0.0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a focused update-baostock-daily bottleneck stage.")
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--work-root", type=Path, default=Path("tmp/baostock-bottleneck-stage"))
    parser.add_argument("--dataset", default="all")
    parser.add_argument("--codes", required=True, help="Comma-separated stock codes.")
    parser.add_argument("--end", required=True)
    parser.add_argument("--workers", default="4", help="Comma-separated worker counts.")
    parser.add_argument("--metadata-flush-size", default="200", help="Comma-separated flush sizes.")
    parser.add_argument("--build-duckdb-views", action="store_true", default=False)
    args = parser.parse_args(argv)

    codes = [item.strip() for item in args.codes.split(",") if item.strip()]
    _copy_root_for_diagnostics(args.source_root, args.work_root, codes=codes)
    results = [
        run_stage(
            root=args.work_root,
            dataset=args.dataset,
            codes=codes,
            end=args.end,
            workers=workers,
            metadata_flush_size=flush_size,
            build_views=args.build_duckdb_views,
        )
        for workers in _csv_ints(args.workers)
        for flush_size in _csv_ints(args.metadata_flush_size)
    ]
    print(json.dumps(results, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _copy_if_exists(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _update_pipeline_config(root: Path, *, workers: int, metadata_flush_size: int) -> None:
    try:
        import yaml
    except ImportError:
        return
    config_path = root / "config" / "settings.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    if not isinstance(config, dict):
        config = {}
    pipeline = config.setdefault("pipeline", {})
    if isinstance(pipeline, dict):
        pipeline["background_workers"] = workers
        pipeline["metadata_flush_size"] = metadata_flush_size
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _csv_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
