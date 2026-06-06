from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from update_daily_fakes import _write_settings

import src.sources.baostock.update_daily as update_daily_module
from src.pipeline.common import PIPELINE_UPDATE_DAILY, checkpoint_output_path, write_checkpoint
from src.sources.baostock.adjustments import BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET
from src.sources.baostock.client import BaostockTimeoutError
from src.storage.parquet_store import ParquetStore

pytestmark = pytest.mark.slow


def _login_failing_provider_factory(calls: dict[str, int]):
    class LoginFailingProvider:
        def __init__(self, config=None) -> None:
            self.config = config

        def __enter__(self):
            calls["enter"] += 1
            raise BaostockTimeoutError("login unavailable")

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def create_provider(config, provider: str | None = None):
        calls["create"] += 1
        return LoginFailingProvider(config)

    return create_provider


def _write_calendar(store: ParquetStore) -> None:
    dates = pd.date_range("2023-10-01", "2024-01-07", freq="D")
    store.write_dataset(
        "baostock_cn_trading_calendar",
        pd.DataFrame(
            [
                {
                    "calendar_date": item.date(),
                    "is_trading_day": "1" if item.weekday() < 5 else "0",
                }
                for item in dates
            ]
        ),
    )


def test_update_daily_calendar_checkpoint_skip_does_not_login_provider(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_calendar(store)
    end_date = "2024-01-03"
    output_path = checkpoint_output_path(store, "baostock_cn_trading_calendar", "*", end_date)
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_trading_calendar",
        "*",
        "2024-01-02",
        end_date,
        "success",
        99,
        output_path,
    )

    provider_calls = {"create": 0, "enter": 0}
    monkeypatch.setattr(update_daily_module, "create_provider", _login_failing_provider_factory(provider_calls))

    records = update_daily_module.update_daily(
        dataset="baostock_cn_trading_calendar",
        end=end_date,
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in records] == ["skipped_checkpoint"]
    assert provider_calls == {"create": 0, "enter": 0}


def test_update_daily_qfq_checkpoint_skip_does_not_login_provider(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_calendar(store)
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    code = "sh.600000"
    end_date = "2024-01-03"
    factor_df = pd.DataFrame(
        [
            {
                "code": code,
                "dividend_operate_date": date(2024, 1, 2),
                "forward_adjust_factor": 1.0,
                "backward_adjust_factor": 1.0,
                "adjustment_factor": 1.0,
            }
        ]
    )
    factor_path = store.write_dataset(
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET, factor_df, {"code": code}
    ).primary_path
    qfq_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample().assign(code=code, adjust_flag="1"), {"code": code}
    ).primary_path
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        BAOSTOCK_CN_STOCK_ADJUSTMENT_FACTOR_DATASET,
        code,
        "1990-01-01",
        end_date,
        "success",
        len(factor_df),
        factor_path,
    )
    write_checkpoint(
        store,
        PIPELINE_UPDATE_DAILY,
        "baostock_cn_stock_daily_bar_qfq",
        code,
        "2024-01-02",
        end_date,
        "success",
        len(daily_sample()),
        qfq_path,
    )

    provider_calls = {"create": 0, "enter": 0}
    monkeypatch.setattr(update_daily_module, "create_provider", _login_failing_provider_factory(provider_calls))

    records = update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        code=code,
        end=end_date,
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in records] == ["skipped_checkpoint"]
    assert [item["dataset"] for item in records] == ["baostock_cn_stock_daily_bar_qfq"]
    assert provider_calls == {"create": 0, "enter": 0}


def test_update_daily_missing_checkpoint_keeps_login_failure(
    tmp_path,
    monkeypatch,
    daily_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    _write_calendar(store)
    code = "sh.600000"
    store.write_dataset(
        "baostock_cn_stock_daily_bar_qfq", daily_sample().assign(code=code, adjust_flag="1"), {"code": code}
    )

    provider_calls = {"create": 0, "enter": 0}
    monkeypatch.setattr(update_daily_module, "create_provider", _login_failing_provider_factory(provider_calls))

    with pytest.raises(BaostockTimeoutError):
        update_daily_module.update_daily(
            dataset="baostock_cn_stock_daily_bar_qfq",
            code=code,
            end="2024-01-03",
            root=tmp_path,
            build_views=False,
        )

    assert provider_calls == {"create": 1, "enter": 1}
