from __future__ import annotations

import pandas as pd
import pytest

from src.api.baostock_provider import BaostockProvider
from src.api.market_data import DailyBarRequest, create_provider
from src.utils.config_mgr import ConfigError, ConfigManager


def test_create_provider_defaults_to_configured_baostock(tmp_path) -> None:
    _write_settings(tmp_path)

    provider = create_provider(ConfigManager(tmp_path))

    assert isinstance(provider, BaostockProvider)
    assert provider.name == "baostock"


def test_create_provider_rejects_unknown_provider(tmp_path) -> None:
    _write_settings(tmp_path)

    with pytest.raises(ConfigError, match="Unknown data provider: missing"):
        create_provider(ConfigManager(tmp_path), provider="missing")


def test_baostock_provider_maps_daily_request_to_client(tmp_path) -> None:
    _write_settings(tmp_path)
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, max_attempts: int = 3) -> None:
            captured["max_attempts"] = max_attempts

        def __enter__(self):
            captured["entered"] = True
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            captured["exited"] = True

        def query_history_k_data_plus(
            self,
            code: str,
            fields: str,
            start_date: str,
            end_date: str,
            frequency: str = "d",
            adjust_flag: str = "3",
        ) -> pd.DataFrame:
            captured["daily"] = {
                "code": code,
                "fields": fields,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": frequency,
                "adjust_flag": adjust_flag,
            }
            return pd.DataFrame([{"code": code}])

        def query_baostock_cn_stock_adjustment_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
            captured["baostock_cn_stock_adjustment_factor"] = {
                "code": code,
                "start_date": start_date,
                "end_date": end_date,
            }
            return pd.DataFrame([{"code": code}])

    provider = BaostockProvider(ConfigManager(tmp_path), client_factory=FakeClient)
    with provider as source:
        result = source.query_daily_bars(
            DailyBarRequest(
                dataset="baostock_cn_stock_daily_bar_qfq",
                code="sh.600000",
                start_date="2024-01-01",
                end_date="2024-01-31",
                fields="date,code,prev_close,turnover_rate,pct_change,is_st",
                frequency="d",
            )
        )

    assert len(result) == 1
    assert captured["max_attempts"] == 5
    assert captured["entered"] is True
    assert captured["exited"] is True
    assert captured["daily"] == {
        "code": "sh.600000",
        "fields": "date,code,preclose,turn,pctChg,isST",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "frequency": "d",
        "adjust_flag": "1",
    }


def test_baostock_provider_maps_baostock_cn_stock_adjustment_factor_to_client(tmp_path) -> None:
    _write_settings(tmp_path)
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, max_attempts: int = 3) -> None:
            captured["max_attempts"] = max_attempts

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def query_baostock_cn_stock_adjustment_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
            captured["baostock_cn_stock_adjustment_factor"] = {
                "code": code,
                "start_date": start_date,
                "end_date": end_date,
            }
            return pd.DataFrame([{"code": code}])

    provider = BaostockProvider(ConfigManager(tmp_path), client_factory=FakeClient)
    with provider as source:
        result = source.query_baostock_cn_stock_adjustment_factor("sh.600000", "1990-01-01", "2024-01-31")

    assert len(result) == 1
    assert captured["baostock_cn_stock_adjustment_factor"] == {
        "code": "sh.600000",
        "start_date": "1990-01-01",
        "end_date": "2024-01-31",
    }


def _write_settings(root) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "api:",
                "  provider: baostock",
                "  baostock:",
                "    adjust_flag_map:",
                '      unadjusted: "3"',
                '      qfq: "1"',
                '      hfq: "2"',
                "datasets:",
                "  daily_bar:",
                '    fields: "date,code,close"',
                "    frequency: d",
                "pipeline:",
                "  max_retries: 5",
                "",
            ]
        ),
        encoding="utf-8",
    )

