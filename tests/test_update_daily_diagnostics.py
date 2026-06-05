from __future__ import annotations

import pytest
from update_daily_fakes import _fake_provider_factory, _write_settings

import src.sources.baostock.services as services_module
import src.sources.baostock.update_daily as update_daily_module
import src.storage.parquet_store as parquet_store_module
from src.storage.data_registry import DataRegistry

pytestmark = pytest.mark.slow


def test_update_daily_api_fetch_logs_include_run_identity(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    api_fetch_logs = []

    class FakeLogger:
        def info(self, message, *args, **kwargs) -> None:
            if message.startswith("API fetch completed"):
                api_fetch_logs.append((message, args))

        def warning(self, message, *args, **kwargs) -> None:
            return None

        def exception(self, message, *args, **kwargs) -> None:
            raise AssertionError(message.format(*args))

    monkeypatch.setattr(services_module, "logger", FakeLogger())

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert api_fetch_logs
    message, args = api_fetch_logs[0]
    assert message.startswith("API fetch completed run_id={} pid={} thread={}")
    assert isinstance(args[0], str)
    assert args[0].startswith("daily-")
    assert isinstance(args[1], int)
    assert isinstance(args[2], str)


def test_update_daily_background_logs_share_run_identity(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    api_run_ids = []
    parquet_run_ids = []

    class ServicesLogger:
        def info(self, message, *args, **kwargs) -> None:
            if message.startswith("API fetch completed"):
                api_run_ids.append(args[0])

        def warning(self, message, *args, **kwargs) -> None:
            return None

        def exception(self, message, *args, **kwargs) -> None:
            raise AssertionError(message.format(*args))

    class ParquetLogger:
        def info(self, message, *args, **kwargs) -> None:
            if message.startswith("Dataset Parquet stored"):
                parquet_run_ids.append(args[0])

        def warning(self, message, *args, **kwargs) -> None:
            return None

        def exception(self, message, *args, **kwargs) -> None:
            raise AssertionError(message.format(*args))

    monkeypatch.setattr(services_module, "logger", ServicesLogger())
    monkeypatch.setattr(parquet_store_module, "logger", ParquetLogger())

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_qfq",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
    )

    assert api_run_ids
    assert parquet_run_ids
    assert set(parquet_run_ids) == {api_run_ids[0]}


def test_update_daily_refreshes_registry_once_after_pipeline_completes(
    tmp_path,
    monkeypatch,
    daily_sample,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(baostock_cn_stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    publish_calls = []
    refresh_calls = []
    original_publish = DataRegistry.publish_dataframe_write
    original_refresh = DataRegistry.refresh_inventory

    def publish_spy(self, *args, **kwargs):
        publish_calls.append(args)
        return original_publish(self, *args, **kwargs)

    def refresh_spy(self, dataset_ids=None, status_rows=None):
        refresh_calls.append(tuple(dataset_ids or ()))
        return original_refresh(self, dataset_ids=dataset_ids, status_rows=status_rows)

    monkeypatch.setattr(DataRegistry, "publish_dataframe_write", publish_spy)
    monkeypatch.setattr(DataRegistry, "refresh_inventory", refresh_spy)

    update_daily_module.update_daily(
        dataset="baostock_cn_stock_daily_bar_unadjusted",
        mode="full",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000",),
        root=tmp_path,
        build_views=False,
        force=True,
        resume=False,
    )

    assert publish_calls == []
    assert refresh_calls == [
        (
            "baostock_cn_stock_daily_bar_unadjusted",
            "baostock_cn_trading_calendar",
        )
    ]
