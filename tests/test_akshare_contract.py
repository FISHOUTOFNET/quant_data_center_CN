from __future__ import annotations

import os

import pytest

from src.api.akshare_client import AkShareClient

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_AKSHARE_CONTRACT") != "1",
    reason="AkShare contract tests require RUN_AKSHARE_CONTRACT=1",
)


class ContractConfig:
    def get(self, dotted_key: str, default=None):
        values = {
            "api.akshare.max_retries": 3,
            "api.akshare.jitter_seconds": [0, 0],
            "api.akshare.endpoints.akshare_cn_stock_valuation_eastmoney.failure_threshold": 3,
            "api.akshare.endpoints.akshare_cn_stock_valuation_eastmoney.cooldown_minutes": 1,
        }
        return values.get(dotted_key, default)


def test_akshare_contract_akshare_cn_stock_valuation_eastmoney_sample_stock() -> None:
    client = AkShareClient(config=ContractConfig())

    df = client.fetch_stock_valuation("300766").data

    assert not df.empty
    assert not df.duplicated(["code", "date"]).any()
    assert set(df["code"].dropna().astype(str)) == {"300766"}
