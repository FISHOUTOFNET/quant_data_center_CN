from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.sources.akshare.client import AkShareResponse
from src.sources.akshare.pipeline import AkShareUpdateRequest, update_akshare
from src.sources.akshare.eastmoney.modules.capital_structure_em import plan_capital_structure_tasks
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


class FakeCapitalStructureClient:
    akshare_version = "fake-akshare"

    def __init__(self, sample) -> None:
        self.calls: list[str] = []
        self._sample = sample

    def fetch_capital_structure(self, code: str) -> AkShareResponse:
        self.calls.append(code)
        return AkShareResponse(
            endpoint="stock_zh_a_gbjg_em",
            params={"symbol": code},
            akshare_version=self.akshare_version,
            data=self._sample(code),
        )


def test_update_akshare_capital_structure_explicit_code_resume_and_force(
    tmp_path,
    akshare_cn_stock_capital_structure_em_sample,
) -> None:
    _write_settings(tmp_path)
    client = FakeCapitalStructureClient(akshare_cn_stock_capital_structure_em_sample)

    records = update_akshare(
        _request(
            code=("600000",),
            root=tmp_path,
            build_views=False,
            client=client,
        )
    )

    store = ParquetStore(root=tmp_path)
    assert client.calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    assert store.dataset_exists("akshare_cn_stock_capital_structure_em", {"code": "600000"})
    checkpoint_count = len(store.read_pipeline_checkpoints())

    records = update_akshare(
        _request(
            code=("600000",),
            root=tmp_path,
            build_views=False,
            client=client,
        )
    )

    assert records == []
    assert client.calls == ["600000"]
    assert len(store.read_pipeline_checkpoints()) == checkpoint_count

    records = update_akshare(
        _request(
            code=("600000",),
            root=tmp_path,
            build_views=False,
            force=True,
            client=client,
        )
    )

    assert [item["status"] for item in records] == ["success"]
    assert client.calls == ["600000", "600000"]


def test_capital_structure_partial_uses_active_universe_and_full_includes_delisted(
    tmp_path,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    _write_akshare_universe(store, spot_codes=("600000", "000001"), delisted_codes=("000001",))
    config = ConfigManager(tmp_path)

    partial = plan_capital_structure_tasks(config=config, store=store, mode="partial")
    full = plan_capital_structure_tasks(config=config, store=store, mode="full")

    assert [task.code for task in partial] == ["600000"]
    assert [task.code for task in full] == ["600000", "000001"]


def _request(**kwargs) -> AkShareUpdateRequest:
    return AkShareUpdateRequest(target="capital_structure", **kwargs)


def _write_settings(root) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "project:",
                "  timezone: Asia/Shanghai",
                "api:",
                "  akshare:",
                "    max_retries: 1",
                "    workers: 3",
                "    jitter_seconds: [0, 0]",
                "    endpoints:",
                "      stock_zh_a_gbjg_em:",
                "        failure_threshold: 2",
                "        cooldown_minutes: 1",
                "pipeline:",
                "  metadata_flush_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_akshare_universe(
    store: ParquetStore,
    spot_codes: tuple[str, ...],
    delisted_codes: tuple[str, ...],
) -> None:
    fetched_at = datetime(2024, 1, 3, 18, 0)
    store.write_dataset(
        "akshare_cn_stock_spot_quote_eastmoney",
        pd.DataFrame(
            [
                {
                    "trade_date": "2024-01-03",
                    "code": code,
                    "source_symbol": code,
                    "name": f"Stock {code}",
                    "last_price": 8.3,
                    "price_change": 0.1,
                    "pct_change": 1.2,
                    "open": 8.2,
                    "high": 8.4,
                    "low": 8.1,
                    "prev_close": 8.2,
                    "volume": 120000.0,
                    "amount": 9960.0,
                    "turnover_rate": 0.12,
                    "amplitude": 3.0,
                    "pe_dynamic": 5.1,
                    "pb": 0.71,
                    "total_market_cap": 101000000.0,
                    "float_market_cap": 81000000.0,
                    "source_endpoint": "stock_zh_a_spot_em",
                    "fetched_at": fetched_at,
                }
                for code in spot_codes
            ]
        ),
        {"trade_date": "2024-01-03"},
    )
    store.write_dataset(
        "akshare_cn_stock_delist_sh",
        pd.DataFrame(
            [
                {
                    "snapshot_date": "2024-01-03",
                    "exchange": "sh",
                    "market": "全部",
                    "code": code,
                    "source_symbol": code,
                    "name": f"Delisted {code}",
                    "list_date": "2000-01-01",
                    "delist_date": "2024-01-02",
                    "source_endpoint": "akshare_cn_stock_delist_sh",
                    "fetched_at": fetched_at,
                }
                for code in delisted_codes
            ]
        ),
        {"snapshot_date": "2024-01-03"},
    )
