from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.pipeline.adjustments import calculate_adjusted_daily_k


def test_calculate_forward_adjusted_daily_k_matches_baostock_example() -> None:
    result = calculate_adjusted_daily_k(_raw_example(), _factor_example(), "daily_k_qfq", "2")

    assert result["adjustflag"].tolist() == ["2", "2", "2"]
    assert result.loc[0, "open"] == pytest.approx(11.6816483)
    assert result.loc[0, "close"] == pytest.approx(11.75000645)
    assert result.loc[1, "open"] == pytest.approx(11.75)
    assert result.loc[2, "close"] == pytest.approx(12.84)


def test_calculate_backward_adjusted_daily_k_matches_baostock_example() -> None:
    result = calculate_adjusted_daily_k(_raw_example(), _factor_example(), "daily_k_hfq", "1")

    assert result["adjustflag"].tolist() == ["1", "1", "1"]
    assert result.loc[0, "open"] == pytest.approx(109.64075944)
    assert result.loc[0, "close"] == pytest.approx(110.28235036)
    assert result.loc[1, "open"] == pytest.approx(110.282351)
    assert result.loc[2, "close"] == pytest.approx(120.51279888)


def test_calculate_adjusted_daily_k_defaults_to_one_without_factor() -> None:
    result = calculate_adjusted_daily_k(_raw_example(), pd.DataFrame(), "daily_k_qfq", "2")

    assert result["open"].tolist() == [15.38, 11.75, 12.81]
    assert result["close"].tolist() == [15.47, 12.93, 12.84]
    assert result["volume"].tolist() == [1000, 1100, 1200]


def _raw_example() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2017, 5, 24),
                "code": "sh.600000",
                "open": 15.38,
                "high": 15.5,
                "low": 15.3,
                "close": 15.47,
                "preclose": 15.43,
                "volume": 1000,
                "adjustflag": "3",
            },
            {
                "date": date(2017, 5, 25),
                "code": "sh.600000",
                "open": 11.75,
                "high": 12.95,
                "low": 11.7,
                "close": 12.93,
                "preclose": 11.75,
                "volume": 1100,
                "adjustflag": "3",
            },
            {
                "date": date(2017, 5, 26),
                "code": "sh.600000",
                "open": 12.81,
                "high": 12.9,
                "low": 12.7,
                "close": 12.84,
                "preclose": 12.93,
                "volume": 1200,
                "adjustflag": "3",
            },
        ]
    )


def _factor_example() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "sh.600000",
                "dividOperateDate": date(2016, 6, 23),
                "foreAdjustFactor": 0.759535,
                "backAdjustFactor": 7.128788,
                "adjustFactor": 7.128788,
            },
            {
                "code": "sh.600000",
                "dividOperateDate": date(2017, 5, 25),
                "foreAdjustFactor": 1.0,
                "backAdjustFactor": 9.385732,
                "adjustFactor": 9.385732,
            },
        ]
    )
