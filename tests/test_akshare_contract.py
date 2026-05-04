from __future__ import annotations

import os

import pandas as pd
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
            "api.akshare.endpoints.stock_value_em.failure_threshold": 3,
            "api.akshare.endpoints.stock_value_em.cooldown_minutes": 1,
        }
        return values.get(dotted_key, default)


def test_akshare_contract_stock_value_em_sample_stock() -> None:
    client = AkShareClient(config=ContractConfig(), stock_basic_df=_contract_stock_basic())

    df = client.query_stock_value("sz.300766")

    assert not df.empty
    assert not df.duplicated(["code", "date"]).any()
    assert set(df["code"].dropna().astype(str)) == {"300766"}


def _contract_stock_basic() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"code": "sz.300766", "code_name": "每日互动", "type": "1", "status": "1"},
        ]
    )
