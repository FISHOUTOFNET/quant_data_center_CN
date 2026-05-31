from __future__ import annotations

from pathlib import Path

import yaml

from src.pipeline.update_daily import update_daily


def setup_test_environment(root: Path) -> dict[str, object]:
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "api": {
            "baostock": {
                "adjust_flag_map": {"unadjusted": "3", "qfq": "1", "hfq": "2"},
                "timeout_seconds": 60,
                "max_rows_per_result": 200000,
            }
        },
        "datasets": {
            "daily_bar": {
                "fields": "date,code,open,high,low,close,prev_close,volume,amount,adjust_flag,turn,trade_status,pct_change,pe_ttm,pb_mrq,ps_ttm,pcf_ncf_ttm,is_st",
                "frequency": "d",
            }
        },
        "pipeline": {
            "lookback_days": 1,
            "max_retries": 1,
            "metadata_flush_size": 200,
            "background_workers": 4,
        },
    }
    (config_dir / "settings.yaml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config


def run_concurrency_test(
    *,
    codes: list[str],
    end_date: str,
    root: Path,
    workers: int,
    config: dict[str, object],
) -> list[dict[str, object]]:
    pipeline_config = config.setdefault("pipeline", {})
    assert isinstance(pipeline_config, dict)
    pipeline_config["background_workers"] = workers
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return update_daily(
        dataset="all",
        mode="partial",
        end=end_date,
        code=tuple(codes),
        root=root,
        build_views=False,
    )
