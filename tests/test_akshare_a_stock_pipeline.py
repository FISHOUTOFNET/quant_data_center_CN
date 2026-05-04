from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest

from src.api.akshare_client import AkShareResponse, dataframe_hash
from src.pipeline.update_akshare_hist import update_akshare_hist
from src.pipeline.update_akshare_spot import update_akshare_spot
from src.pipeline.update_akshare_universe import update_akshare_universe
from src.storage.parquet_store import ParquetStore


class FakeAStockClient:
    akshare_version = "fake-a-stock"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail_spot_em = False

    def fetch_stock_info_sh_delist(self, symbol: str = "全部", snapshot_date: str | None = None) -> AkShareResponse:
        self.calls.append(("stock_info_sh_delist", {"symbol": symbol, "snapshot_date": snapshot_date}))
        data = pd.DataFrame(
            [
                {
                    "snapshot_date": snapshot_date,
                    "exchange": "sh",
                    "market": symbol,
                    "code": "600001",
                    "source_symbol": "600001",
                    "name": "Old Corp",
                    "list_date": "2000-01-01",
                    "delist_date": "2024-01-02",
                    "source_endpoint": "stock_info_sh_delist",
                    "fetched_at": datetime(2024, 1, 3, 16, 0),
                }
            ]
        )
        return _response("stock_info_sh_delist", {"symbol": symbol}, data)

    def fetch_stock_zh_a_spot_em(self, trade_date: str | None = None) -> AkShareResponse:
        self.calls.append(("stock_zh_a_spot_em", {"trade_date": trade_date}))
        if self.fail_spot_em:
            raise RuntimeError("planned spot_em failure")
        data = _spot_em_data(trade_date or "2024-01-03")
        return _response("stock_zh_a_spot_em", {"trade_date": trade_date}, data)

    def fetch_stock_zh_a_spot_sina(self, trade_date: str | None = None, fallback_reason: str = "") -> AkShareResponse:
        self.calls.append(
            ("stock_zh_a_spot", {"trade_date": trade_date, "fallback_reason": fallback_reason})
        )
        data = pd.DataFrame(
            [
                {
                    "trade_date": trade_date,
                    "code": "600000",
                    "source_symbol": "sh600000",
                    "name": "PF Bank",
                    "latest_price": 8.3,
                    "change_amount": 0.1,
                    "pct_chg": 1.2,
                    "bid": 8.29,
                    "ask": 8.31,
                    "preclose": 8.2,
                    "open": 8.2,
                    "high": 8.4,
                    "low": 8.1,
                    "volume": 120000.0,
                    "amount": 9960.0,
                    "source_timestamp": "15:00:00",
                    "source_endpoint": "stock_zh_a_spot",
                    "is_fallback": True,
                    "fallback_reason": fallback_reason,
                    "fetched_at": datetime(2024, 1, 3, 16, 0),
                }
            ]
        )
        return _response("stock_zh_a_spot", {"trade_date": trade_date}, data)

    def fetch_stock_zh_a_hist(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str,
    ) -> AkShareResponse:
        self.calls.append(
            (
                "stock_zh_a_hist",
                {"symbol": symbol, "start_date": start_date, "end_date": end_date, "adjust": adjust},
            )
        )
        data = pd.DataFrame([_hist_row(symbol, adjust, close=8.31)])
        return _response(
            "stock_zh_a_hist",
            {"symbol": symbol, "start_date": start_date, "end_date": end_date, "adjust": adjust},
            data,
        )


def test_update_akshare_universe_writes_manual_delist_snapshot(tmp_path) -> None:
    _write_settings(tmp_path)
    client = FakeAStockClient()

    records = update_akshare_universe(
        market="全部",
        snapshot_date="2024-01-03",
        root=tmp_path,
        build_views=False,
        client=client,
    )

    store = ParquetStore(root=tmp_path)
    loaded = store.read_stock_info_sh_delist("2024-01-03")
    assert [item["status"] for item in records] == ["success"]
    assert loaded.loc[0, "code"] == "600001"
    assert _manifest_rows(tmp_path)[-1]["dataset"] == "stock_info_sh_delist"


def test_update_akshare_spot_success_writes_snapshot_and_hist_spot_close(tmp_path) -> None:
    _write_settings(tmp_path)
    client = FakeAStockClient()

    records = update_akshare_spot(
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        client=client,
        now=lambda: datetime(2024, 1, 3, 18, 0),
    )

    store = ParquetStore(root=tmp_path)
    spot = store.read_stock_zh_a_spot_em("2024-01-03")
    hist = store.read_stock_zh_a_hist("none", "600000")
    assert [item["dataset"] for item in records] == ["stock_zh_a_spot_em", "stock_zh_a_hist_none"]
    assert spot.loc[0, "code"] == "600000"
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_spot_em"
    assert hist.loc[0, "quality_status"] == "spot_close"
    assert hist.loc[0, "close"] == 8.3

    client.calls.clear()
    skipped = update_akshare_spot(
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        client=client,
        now=lambda: datetime(2024, 1, 3, 18, 0),
    )
    assert skipped == []
    assert client.calls == []


def test_update_akshare_spot_fallback_writes_sina_and_hist(tmp_path) -> None:
    _write_settings(tmp_path)
    client = FakeAStockClient()
    client.fail_spot_em = True

    records = update_akshare_spot(
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        client=client,
        now=lambda: datetime(2024, 1, 3, 18, 0),
    )

    store = ParquetStore(root=tmp_path)
    fallback = store.read_stock_zh_a_spot_sina("2024-01-03")
    hist = store.read_stock_zh_a_hist("none", "600000")
    assert [item["dataset"] for item in records] == [
        "stock_zh_a_spot_em",
        "stock_zh_a_spot_sina",
        "stock_zh_a_hist_none",
    ]
    assert [item["status"] for item in records] == ["failed", "success", "success"]
    assert fallback.loc[0, "source_endpoint"] == "stock_zh_a_spot"
    assert "planned spot_em failure" in fallback.loc[0, "fallback_reason"]
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_spot"
    assert hist.loc[0, "quality_status"] == "spot_close"


def test_update_akshare_spot_rejects_realtime_window_before_fetch(tmp_path) -> None:
    _write_settings(tmp_path)
    client = FakeAStockClient()

    with pytest.raises(RuntimeError, match="can only write hist"):
        update_akshare_spot(
            end="2024-01-03",
            root=tmp_path,
            build_views=False,
            client=client,
            now=lambda: datetime(2024, 1, 3, 8, 0),
        )

    assert client.calls == []


def test_update_akshare_hist_incremental_overrides_spot_and_full_writes_adjust(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_zh_a_spot_em("2024-01-03", _spot_em_data("2024-01-03"))
    store.write_stock_zh_a_hist("none", "600000", pd.DataFrame([_hist_row("600000", "none", close=8.3, source_endpoint="stock_zh_a_spot_em", quality_status="spot_close")]))
    store.close()
    client = FakeAStockClient()

    incremental = update_akshare_hist(
        mode="incremental",
        adjust="none",
        start="2024-01-03",
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )
    read_store = ParquetStore(root=tmp_path)
    hist = read_store.read_stock_zh_a_hist("none", "600000")
    assert [item["status"] for item in incremental] == ["success"]
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "hist_confirmed"

    full = update_akshare_hist(
        mode="full",
        adjust="qfq",
        code=("sh.600000",),
        start="2024-01-01",
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )
    qfq = read_store.read_stock_zh_a_hist("qfq", "600000")
    assert [item["dataset"] for item in full] == ["stock_zh_a_hist_qfq"]
    assert qfq.loc[0, "adjust"] == "qfq"


def _response(endpoint: str, params: dict[str, object], data: pd.DataFrame) -> AkShareResponse:
    raw = data.copy()
    return AkShareResponse(
        endpoint=endpoint,
        params=params,
        akshare_version="fake-a-stock",
        raw_df=raw,
        data=data.copy(),
        data_hash=dataframe_hash(raw),
    )


def _spot_em_data(trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "code": "600000",
                "source_symbol": "600000",
                "name": "PF Bank",
                "latest_price": 8.3,
                "change_amount": 0.1,
                "pct_chg": 1.2,
                "open": 8.2,
                "high": 8.4,
                "low": 8.1,
                "preclose": 8.2,
                "volume": 120000.0,
                "amount": 9960.0,
                "turnover_rate": 0.12,
                "amplitude": 3.0,
                "pe_dynamic": 5.1,
                "pb": 0.71,
                "total_market_cap": 101000000.0,
                "float_market_cap": 81000000.0,
                "source_endpoint": "stock_zh_a_spot_em",
                "fetched_at": datetime(2024, 1, 3, 16, 0),
            }
        ]
    )


def _hist_row(
    code: str,
    adjust: str,
    close: float,
    source_endpoint: str = "stock_zh_a_hist",
    quality_status: str = "hist_confirmed",
) -> dict[str, object]:
    return {
        "date": "2024-01-03",
        "code": code,
        "source_symbol": code.split(".", 1)[-1],
        "open": 8.2,
        "high": 8.4,
        "low": 8.1,
        "close": close,
        "volume": 120000,
        "amount": 9960.0,
        "amplitude": 3.0,
        "pct_chg": 1.2,
        "change_amount": 0.1,
        "turnover_rate": 0.12,
        "adjust": adjust,
        "source_endpoint": source_endpoint,
        "quality_status": quality_status,
        "fetched_at": datetime(2024, 1, 3, 16, 0),
    }


def _manifest_rows(root) -> list[dict[str, object]]:
    path = root / "data" / "raw" / "akshare" / "manifest" / "fetch_runs.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


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
                "    workers: 1",
                "    jitter_seconds: [0, 0]",
                "datasets:",
                "  stock_zh_a_hist:",
                "    full_start: '1990-01-01'",
                "  stock_zh_a_spot:",
                "    update_hist_from_spot: true",
                "pipeline:",
                "  metadata_flush_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
