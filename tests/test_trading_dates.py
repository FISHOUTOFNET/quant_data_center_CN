from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.pipeline.common import (
    default_candidate_date,
    latest_trading_day_on_or_before,
    trading_day_lookback_start,
    trading_range_bounds,
)
from src.utils.config_mgr import ConfigManager


def test_default_candidate_date_uses_1800_cutoff(tmp_path) -> None:
    _write_settings(tmp_path)
    config = ConfigManager(tmp_path)
    zone = ZoneInfo("Asia/Shanghai")

    assert default_candidate_date(config, datetime(2024, 1, 8, 17, 59, tzinfo=zone)) == "2024-01-07"
    assert default_candidate_date(config, datetime(2024, 1, 8, 18, 0, tzinfo=zone)) == "2024-01-08"


def test_non_trading_candidate_resolves_to_previous_trading_day() -> None:
    baostock_cn_trading_calendar = _baostock_cn_trading_calendar("2024-01-04", "2024-01-08")

    assert latest_trading_day_on_or_before(baostock_cn_trading_calendar, "2024-01-07") == "2024-01-05"


def test_trading_day_lookback_crosses_weekend() -> None:
    baostock_cn_trading_calendar = _baostock_cn_trading_calendar("2024-01-04", "2024-01-08")

    assert trading_day_lookback_start(baostock_cn_trading_calendar, "2024-01-08", 1) == "2024-01-05"


def test_trading_range_bounds_stay_inside_requested_range() -> None:
    baostock_cn_trading_calendar = _baostock_cn_trading_calendar("2024-01-06", "2024-01-14")

    assert trading_range_bounds(baostock_cn_trading_calendar, "2024-01-06", "2024-01-14") == (
        "2024-01-08",
        "2024-01-12",
    )


def _baostock_cn_trading_calendar(start: str, end: str) -> pd.DataFrame:
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        [
            {
                "calendar_date": item.date(),
                "is_trading_day": "1" if item.weekday() < 5 else "0",
            }
            for item in dates
        ]
    )


def _write_settings(root) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "project:",
                "  timezone: Asia/Shanghai",
                "",
            ]
        ),
        encoding="utf-8",
    )
