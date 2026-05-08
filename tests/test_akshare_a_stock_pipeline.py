from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest

import src.pipeline.update_akshare_delist as update_akshare_delist_module
import src.pipeline.update_akshare_daily_bar as update_akshare_daily_bar_module
import src.pipeline.update_akshare_spot as update_akshare_spot_module
from src.api.akshare_client import AkShareResponse, dataframe_hash
from src.pipeline.update_akshare_daily_bar import update_akshare_daily_bar
from src.pipeline.update_akshare_spot import update_akshare_spot
from src.pipeline.update_akshare_delist import update_akshare_delist
from src.storage.parquet_store import ParquetStore


class FakeLogger:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, tuple[object, ...]]] = []

    def clear(self) -> None:
        self.entries.clear()

    def info(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("info", message, args))

    def warning(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("warning", message, args))

    def error(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("error", message, args))

    def exception(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("exception", message, args))


class FakeAStockClient:
    akshare_version = "fake-a-stock"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail_spot_em = False

    def fetch_akshare_cn_stock_delist_sh(self, symbol: str = "全部", snapshot_date: str | None = None) -> AkShareResponse:
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

    def fetch_akshare_cn_stock_delist_sz(
        self,
        symbol: str = "终止上市公司",
        snapshot_date: str | None = None,
    ) -> AkShareResponse:
        self.calls.append(("stock_info_sz_delist", {"symbol": symbol, "snapshot_date": snapshot_date}))
        data = pd.DataFrame(
            [
                {
                    "snapshot_date": snapshot_date,
                    "exchange": "sz",
                    "market": symbol,
                    "code": "000001",
                    "source_symbol": "000001",
                    "name": "Old SZ Corp",
                    "list_date": "1991-04-03",
                    "delist_date": "2024-01-02",
                    "source_endpoint": "stock_info_sz_delist",
                    "fetched_at": datetime(2024, 1, 3, 16, 0),
                }
            ]
        )
        return _response("stock_info_sz_delist", {"symbol": symbol}, data)

    def fetch_spot_quote_eastmoney(self, trade_date: str | None = None) -> AkShareResponse:
        self.calls.append(("stock_zh_a_spot_em", {"trade_date": trade_date}))
        if self.fail_spot_em:
            raise RuntimeError("planned spot_em failure")
        data = _spot_em_data(trade_date or "2024-01-03")
        return _response("stock_zh_a_spot_em", {"trade_date": trade_date}, data)

    def fetch_spot_quote_sina(self, trade_date: str | None = None, fallback_reason: str = "") -> AkShareResponse:
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
                    "last_price": 8.3,
                    "price_change": 0.1,
                    "pct_change": 1.2,
                    "bid": 8.29,
                    "ask": 8.31,
                    "prev_close": 8.2,
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

    def fetch_daily_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjustment: str,
    ) -> AkShareResponse:
        self.calls.append(
            (
                "stock_zh_a_hist",
                {"symbol": symbol, "start_date": start_date, "end_date": end_date, "adjustment": adjustment},
            )
        )
        data = pd.DataFrame([_daily_bar_row(symbol, adjustment, close=8.31)])
        return _response(
            "stock_zh_a_hist",
            {"symbol": symbol, "start_date": start_date, "end_date": end_date, "adjustment": adjustment},
            data,
        )


def test_update_akshare_delist_writes_manual_delist_snapshot(tmp_path) -> None:
    _write_settings(tmp_path)
    client = FakeAStockClient()

    records = update_akshare_delist(
        market="全部",
        snapshot_date="2024-01-03",
        root=tmp_path,
        build_views=False,
        client=client,
    )

    store = ParquetStore(root=tmp_path)
    loaded = store.read_akshare_cn_stock_delist_sh("2024-01-03")
    loaded_sz = store.read_akshare_cn_stock_delist_sz("2024-01-03")
    assert [item["status"] for item in records] == ["success", "success"]
    assert loaded.loc[0, "code"] == "600001"
    assert loaded_sz.loc[0, "code"] == "000001"
    assert {item["dataset"] for item in _manifest_rows(tmp_path)} == {
        "akshare_cn_stock_delist_sh",
        "akshare_cn_stock_delist_sz",
    }


def test_update_akshare_spot_success_writes_snapshot_and_hist_spot_quote_close(tmp_path) -> None:
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
    spot = store.read_stock_spot_quote_eastmoney("2024-01-03")
    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert [item["dataset"] for item in records] == ["akshare_cn_stock_spot_quote_eastmoney", "akshare_cn_stock_daily_bar_unadjusted"]
    assert spot.loc[0, "code"] == "600000"
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_spot_em"
    assert hist.loc[0, "quality_status"] == "spot_quote_close"
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
    fallback = store.read_stock_spot_quote_sina("2024-01-03")
    hist = store.read_akshare_daily_bars("unadjusted", "600000")
    assert [item["dataset"] for item in records] == [
        "akshare_cn_stock_spot_quote_eastmoney",
        "akshare_cn_stock_spot_quote_sina",
        "akshare_cn_stock_daily_bar_unadjusted",
    ]
    assert [item["status"] for item in records] == ["failed", "success", "success"]
    assert fallback.loc[0, "source_endpoint"] == "stock_zh_a_spot"
    assert "planned spot_em failure" in fallback.loc[0, "fallback_reason"]
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_spot"
    assert hist.loc[0, "quality_status"] == "spot_quote_close"


def test_update_akshare_spot_rejects_realtime_window_before_fetch(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_trading_calendar(pd.DataFrame([{"calendar_date": "2024-01-03", "is_trading_day": "1"}]))
    client = FakeAStockClient()

    with pytest.raises(RuntimeError, match="can only write daily bars"):
        update_akshare_spot(
            end="2024-01-03",
            root=tmp_path,
            build_views=False,
            client=client,
            now=lambda: datetime(2024, 1, 3, 8, 0),
        )

    assert client.calls == []


def test_update_akshare_daily_bar_force_logs_progress_serial_and_parallel(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    fake_logger = FakeLogger()
    monkeypatch.setattr(update_akshare_daily_bar_module, "logger", fake_logger)
    client = FakeAStockClient()

    for workers in (1, 2):
        fake_logger.clear()
        records = update_akshare_daily_bar_module.update_akshare_daily_bar(
            mode="full",
            adjustment="unadjusted",
            code=("600000", "000001"),
            start="2024-01-01",
            end="2024-01-03",
            root=tmp_path,
            build_views=False,
            workers=workers,
            force=True,
            client=client,
        )

        progress_entries = _log_entries(
            fake_logger,
            "AkShare daily bar progress {}/{} code={} adjustment={} dataset={} status={} rows={}",
        )
        assert len(records) == 2
        assert len(progress_entries) == 2
        assert [entry[2][0] for entry in progress_entries] == [1, 2]
        assert all(entry[2][1] == 2 for entry in progress_entries)
        assert all(entry[2][5] == "success" for entry in progress_entries)
        assert _log_entries(
            fake_logger,
            "AkShare daily bar update started mode={} adjustment={} force={} workers={} planned_tasks={} processing_tasks={}",
        )
        assert _log_entries(
            fake_logger,
            "AkShare daily bar update completed processed={} success={} failed={}",
        )


def test_update_akshare_delist_and_spot_force_log_progress(tmp_path, monkeypatch) -> None:
    _write_settings(tmp_path)
    fake_logger = FakeLogger()
    monkeypatch.setattr(update_akshare_delist_module, "logger", fake_logger)
    monkeypatch.setattr(update_akshare_spot_module, "logger", fake_logger)
    client = FakeAStockClient()

    update_akshare_delist_module.update_akshare_delist(
        market="全部",
        snapshot_date="2024-01-03",
        root=tmp_path,
        build_views=False,
        force=True,
        exchanges=["sh", "sz"],
        client=client,
    )

    delist_progress = _log_entries(
        fake_logger,
        "AkShare delist progress {}/{} exchange={} code={} dataset={} status={} rows={}",
    )
    assert len(delist_progress) == 2
    assert [entry[2][0] for entry in delist_progress] == [1, 2]
    assert all(entry[2][1] == 2 for entry in delist_progress)
    assert all(entry[2][5] == "success" for entry in delist_progress)
    assert _log_entries(
        fake_logger,
        "AkShare delist update started market={} snapshot_date={} force={} planned_tasks={} processing_tasks={}",
    )
    assert _log_entries(
        fake_logger,
        "AkShare delist update completed processed={} success={} failed={} skipped={}",
    )

    fake_logger.clear()
    update_akshare_spot_module.update_akshare_spot(
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        force=True,
        client=client,
        now=lambda: datetime(2024, 1, 3, 18, 0),
    )

    spot_progress = _log_entries(
        fake_logger,
        "AkShare spot progress {}/{} code={} dataset={} status={} rows={}",
    )
    assert len(spot_progress) == 1
    assert spot_progress[0][2][0:2] == (1, 1)
    assert spot_progress[0][2][4] == "success"
    assert _log_entries(fake_logger, "AkShare spot update started trade_date={} force={} resume={}")
    assert _log_entries(
        fake_logger,
        "AkShare spot update completed processed={} success={} failed={} skipped={}",
    )


def test_update_akshare_daily_bar_full_ignores_spot_quote_close_in_prefilter(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_trading_calendar(pd.DataFrame([{"calendar_date": "2024-01-03", "is_trading_day": "1"}]))
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame(
            [
                _daily_bar_row(
                    "600000",
                    "unadjusted",
                    close=8.3,
                    source_endpoint="stock_zh_a_spot_em",
                    quality_status="spot_quote_close",
                )
            ]
        ),
    )
    store.close()
    client = FakeAStockClient()

    records = update_akshare_daily_bar(
        mode="full",
        adjustment="unadjusted",
        code=("600000",),
        start="2024-01-01",
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert [item[0] for item in client.calls] == ["stock_zh_a_hist"]
    assert [item["status"] for item in records] == ["success"]
    read_store = ParquetStore(root=tmp_path)
    hist = read_store.read_akshare_daily_bars("unadjusted", "600000")
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "daily_bar_confirmed"


def test_update_akshare_daily_bar_prefilter_skips_when_latest_row_is_hist_and_covers_baostock_cn_trading_calendar(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_trading_calendar(pd.DataFrame([{"calendar_date": "2024-01-03", "is_trading_day": "1"}]))
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame([_daily_bar_row("600000", "unadjusted", close=8.31, daily_bar_date="2024-01-03")]),
    )
    store.close()
    client = FakeAStockClient()

    records = update_akshare_daily_bar(
        mode="full",
        adjustment="unadjusted",
        code=("600000",),
        start="2024-01-01",
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert records == []
    assert client.calls == []


def test_update_akshare_daily_bar_prefilter_keeps_when_latest_row_is_spot_even_if_hist_covers_baostock_cn_trading_calendar(
    tmp_path,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_trading_calendar(pd.DataFrame([{"calendar_date": "2024-01-04", "is_trading_day": "1"}]))
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame(
            [
                _daily_bar_row("600000", "unadjusted", close=8.31, daily_bar_date="2024-01-04"),
                _daily_bar_row(
                    "600000",
                    "unadjusted",
                    close=8.3,
                    source_endpoint="stock_zh_a_spot_em",
                    quality_status="spot_quote_close",
                    daily_bar_date="2024-01-05",
                ),
            ]
        ),
    )
    store.close()
    client = FakeAStockClient()

    records = update_akshare_daily_bar(
        mode="full",
        adjustment="unadjusted",
        code=("600000",),
        start="2024-01-01",
        end="2024-01-05",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert [item[0] for item in client.calls] == ["stock_zh_a_hist"]
    assert [item["status"] for item in records] == ["success"]


def test_update_akshare_daily_bar_prefilter_keeps_when_latest_hist_is_before_baostock_cn_trading_calendar(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_trading_calendar(pd.DataFrame([{"calendar_date": "2024-01-04", "is_trading_day": "1"}]))
    store.write_akshare_daily_bars(
        "unadjusted",
        "600000",
        pd.DataFrame([_daily_bar_row("600000", "unadjusted", close=8.31, daily_bar_date="2024-01-03")]),
    )
    store.close()
    client = FakeAStockClient()

    records = update_akshare_daily_bar(
        mode="full",
        adjustment="unadjusted",
        code=("600000",),
        start="2024-01-01",
        end="2024-01-04",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert [item[0] for item in client.calls] == ["stock_zh_a_hist"]
    assert [item["status"] for item in records] == ["success"]


def test_update_akshare_daily_bar_incremental_overrides_spot_and_full_writes_adjust(tmp_path) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_spot_quote_eastmoney("2024-01-03", _spot_em_data("2024-01-03"))
    store.write_akshare_daily_bars("unadjusted", "600000", pd.DataFrame([_daily_bar_row("600000", "unadjusted", close=8.3, source_endpoint="stock_zh_a_spot_em", quality_status="spot_quote_close")]))
    store.close()
    client = FakeAStockClient()

    incremental = update_akshare_daily_bar(
        mode="incremental",
        adjustment="unadjusted",
        start="2024-01-03",
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )
    read_store = ParquetStore(root=tmp_path)
    hist = read_store.read_akshare_daily_bars("unadjusted", "600000")
    assert [item["status"] for item in incremental] == ["success"]
    assert hist.loc[0, "close"] == 8.31
    assert hist.loc[0, "source_endpoint"] == "stock_zh_a_hist"
    assert hist.loc[0, "quality_status"] == "daily_bar_confirmed"

    full = update_akshare_daily_bar(
        mode="full",
        adjustment="qfq",
        code=("600000",),
        start="2024-01-01",
        end="2024-01-03",
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )
    qfq = read_store.read_akshare_daily_bars("qfq", "600000")
    assert [item["dataset"] for item in full] == ["akshare_cn_stock_daily_bar_qfq"]
    assert qfq.loc[0, "adjustment"] == "qfq"


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
                "fetched_at": datetime(2024, 1, 3, 16, 0),
            }
        ]
    )


def _daily_bar_row(
    code: str,
    adjustment: str,
    close: float,
    source_endpoint: str = "stock_zh_a_hist",
    quality_status: str = "daily_bar_confirmed",
    daily_bar_date: str = "2024-01-03",
) -> dict[str, object]:
    return {
        "date": daily_bar_date,
        "code": code,
        "source_symbol": code.split(".", 1)[-1],
        "open": 8.2,
        "high": 8.4,
        "low": 8.1,
        "close": close,
        "volume": 120000,
        "amount": 9960.0,
        "amplitude": 3.0,
        "pct_change": 1.2,
        "price_change": 0.1,
        "turnover_rate": 0.12,
        "adjustment": adjustment,
        "source_endpoint": source_endpoint,
        "quality_status": quality_status,
        "fetched_at": datetime(2024, 1, 3, 16, 0),
    }


def _manifest_rows(root) -> list[dict[str, object]]:
    path = root / "data" / "raw" / "akshare" / "manifest" / "fetch_runs.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _log_entries(logger: FakeLogger, message: str) -> list[tuple[str, str, tuple[object, ...]]]:
    return [entry for entry in logger.entries if entry[1] == message]


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
                "  akshare_cn_stock_daily_bar:",
                "    full_start: '1990-01-01'",
                "  akshare_cn_stock_spot_quote:",
                "    update_daily_bar_from_spot: true",
                "pipeline:",
                "  metadata_flush_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )




