from __future__ import annotations

import threading
import time
from datetime import datetime

import pandas as pd
import pytest

import src.pipeline.akshare.modules.valuation_eastmoney as valuation_module
from src.api.akshare_client import AkShareCircuitOpen, AkShareResponse
from src.pipeline.akshare import AkShareUpdateRequest, update_akshare
from src.pipeline.akshare.modules.valuation_eastmoney import (
    AdaptiveConcurrencyController,
    plan_valuation_tasks,
)
from src.pipeline.common import write_checkpoint
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


class FakeAkShareClient:
    akshare_version = "fake-akshare"

    def __init__(self, akshare_cn_stock_valuation_eastmoney_sample) -> None:
        self.value_calls: list[str] = []
        self._akshare_cn_stock_valuation_eastmoney_sample = akshare_cn_stock_valuation_eastmoney_sample

    def fetch_stock_valuation(self, code: str) -> AkShareResponse:
        self.value_calls.append(code)
        data = self._akshare_cn_stock_valuation_eastmoney_sample(code)
        return _response("akshare_cn_stock_valuation_eastmoney", {"symbol": code}, data)


class OverlapAkShareClient(FakeAkShareClient):
    def __init__(self, akshare_cn_stock_valuation_eastmoney_sample, fail_codes: set[str] | None = None) -> None:
        super().__init__(akshare_cn_stock_valuation_eastmoney_sample)
        self._fail_codes = fail_codes or set()
        self._lock = threading.Lock()
        self._overlap_seen = threading.Event()
        self._active_fetches = 0
        self.max_active_fetches = 0

    def fetch_stock_valuation(self, code: str) -> AkShareResponse:
        with self._lock:
            self.value_calls.append(code)
            self._active_fetches += 1
            self.max_active_fetches = max(self.max_active_fetches, self._active_fetches)
            if self._active_fetches >= 2:
                self._overlap_seen.set()
        self._overlap_seen.wait(timeout=0.5)
        time.sleep(0.01)
        try:
            if code in self._fail_codes:
                raise RuntimeError(f"planned failure for {code}")
            data = self._akshare_cn_stock_valuation_eastmoney_sample(code)
            return _response("akshare_cn_stock_valuation_eastmoney", {"symbol": code}, data)
        finally:
            with self._lock:
                self._active_fetches -= 1


class CircuitOpenAkShareClient(FakeAkShareClient):
    def __init__(self, akshare_cn_stock_valuation_eastmoney_sample) -> None:
        super().__init__(akshare_cn_stock_valuation_eastmoney_sample)
        self._lock = threading.Lock()

    def fetch_stock_valuation(self, code: str) -> AkShareResponse:
        with self._lock:
            self.value_calls.append(code)
        if code == "600000":
            raise AkShareCircuitOpen("planned circuit open")
        time.sleep(0.05)
        data = self._akshare_cn_stock_valuation_eastmoney_sample(code)
        return _response("akshare_cn_stock_valuation_eastmoney", {"symbol": code}, data)


class ProgressLogger:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, tuple[object, ...]]] = []

    def info(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("info", message, args))

    def warning(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("warning", message, args))

    def error(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("error", message, args))

    def exception(self, message: str, *args, **kwargs) -> None:
        self.entries.append(("exception", message, args))


def _request(**kwargs) -> AkShareUpdateRequest:
    return AkShareUpdateRequest(target="valuation", **kwargs)


def test_update_akshare_stock_valuation_partial_active_only_resume_and_force(
    tmp_path,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    _write_akshare_universe(store)
    _write_baostock_cn_trading_calendar(store, "2024-01-03")
    client = FakeAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)

    records = update_akshare(
        _request(
            mode="partial",
            root=tmp_path,
            build_views=False,
            client=client,
        )
    )

    assert client.value_calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    assert store.dataset_exists("akshare_cn_stock_valuation_eastmoney", {"code": "600000"})
    assert not (tmp_path / "data" / "raw").exists()
    checkpoint_count = len(store.read_pipeline_checkpoints())

    client.value_calls.clear()
    records = update_akshare(
        _request(
            mode="partial",
            root=tmp_path,
            build_views=False,
            client=client,
        )
    )

    assert client.value_calls == []
    assert records == []
    assert len(store.read_pipeline_checkpoints()) == checkpoint_count
    assert not (tmp_path / "data" / "raw").exists()

    records = update_akshare(
        _request(
            mode="partial",
            root=tmp_path,
            build_views=False,
            force=True,
            client=client,
        )
    )

    assert client.value_calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    checkpoints = store.read_pipeline_checkpoints()
    latest = checkpoints.sort_values("updated_at").iloc[-1]
    assert latest["pipeline"] == "update_akshare"
    assert latest["dataset"] == "akshare_cn_stock_valuation_eastmoney"
    assert latest["code"] == "600000"
    assert latest["status"] == "success"
    assert not (tmp_path / "data" / "raw").exists()


def test_update_akshare_force_logs_stock_valuation_progress(
    tmp_path,
    monkeypatch,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    logger = ProgressLogger()
    monkeypatch.setattr(valuation_module, "logger", logger)
    client = FakeAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)

    records = update_akshare(
        _request(
            mode="partial",
            code=("600000", "000001", "000002"),
            root=tmp_path,
            build_views=False,
            workers=2,
            force=True,
            client=client,
        )
    )

    progress_entries = _log_entries(
        logger,
        "AkShare update progress {}/{} code={} dataset={} status={} rows={}",
    )
    assert len(records) == 3
    assert len(progress_entries) == 3
    assert sorted(entry[2][2] for entry in progress_entries) == ["000001", "000002", "600000"]
    assert [entry[2][0] for entry in progress_entries] == [1, 2, 3]
    assert all(entry[2][1] == 3 for entry in progress_entries)
    assert all(entry[2][4] == "success" for entry in progress_entries)
    assert _log_entries(
        logger,
        "AkShare update started dataset={} mode={} force={} workers={} planned_tasks={} processing_tasks={}",
    )
    assert _log_entries(
        logger,
        "AkShare update completed processed={} success={} failed={} skipped={}",
    )


def test_stock_valuation_prefilter_kept_task_ignores_checkpoint_and_logs_unchanged(
    tmp_path,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    _write_akshare_universe(store)
    _write_baostock_cn_trading_calendar(store, "2024-01-04")
    existing_path = store.write_dataset(
        "akshare_cn_stock_valuation_eastmoney",
        akshare_cn_stock_valuation_eastmoney_sample("600000"),
        {"code": "600000"},
    ).primary_path
    write_checkpoint(
        store,
        valuation_module.PIPELINE_UPDATE_AKSHARE,
        "akshare_cn_stock_valuation_eastmoney",
        "600000",
        "2024-01-02",
        "2024-01-03",
        "success",
        2,
        existing_path,
    )
    client = FakeAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)
    logs = []

    class FakeLogger:
        def info(self, message, *args, **kwargs) -> None:
            logs.append((message, args))

        def warning(self, message, *args, **kwargs) -> None:
            return None

        def error(self, message, *args, **kwargs) -> None:
            return None

        def exception(self, message, *args, **kwargs) -> None:
            return None

    monkeypatch.setattr(valuation_module, "logger", FakeLogger())

    records = update_akshare(
        _request(
            mode="partial",
            code=("600000",),
            root=tmp_path,
            build_views=False,
            workers=1,
            client=client,
        )
    )

    assert client.value_calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    assert (
        "AkShare akshare_cn_stock_valuation_eastmoney unchanged code={} rows={} path={}",
        ("600000", 2, existing_path),
    ) in logs


def test_update_akshare_stock_valuation_full_and_max_tasks(
    tmp_path,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    _write_akshare_universe(store)
    client = FakeAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)

    update_akshare(
        _request(
            mode="full",
            max_tasks=2,
            root=tmp_path,
            build_views=False,
            workers=1,
            client=client,
        )
    )

    assert client.value_calls == ["600000", "000001"]


def test_stock_valuation_task_pool_uses_akshare_universe_and_filters_delisted_incremental(
    tmp_path,
    baostock_cn_stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    _write_akshare_universe(store, spot_codes=("600000", "000001"), delisted_codes=("000001",))
    config = ConfigManager(tmp_path)

    partial = plan_valuation_tasks(config=config, store=store, mode="partial")
    include_inactive = plan_valuation_tasks(
        config=config,
        store=store,
        mode="partial",
        include_inactive=True,
    )
    full = plan_valuation_tasks(config=config, store=store, mode="full")

    assert [task.code for task in partial] == ["600000"]
    assert [task.code for task in include_inactive] == ["600000", "000001"]
    assert [task.code for task in full] == ["600000", "000001"]


def test_update_akshare_accepts_repeated_six_digit_code_option(
    tmp_path,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    client = FakeAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)

    update_akshare(
        _request(
            mode="partial",
            code=("600000", "000001"),
            root=tmp_path,
            build_views=False,
            workers=1,
            client=client,
        )
    )

    assert client.value_calls == ["600000", "000001"]


def test_update_akshare_rejects_non_six_digit_explicit_code_shapes(
    tmp_path,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    client = FakeAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)

    for code in ["sh.600000", "sh600000", "600000.0"]:
        with pytest.raises(ValueError, match="must be 6 digits"):
            update_akshare(
                _request(
                    mode="partial",
                    code=(code,),
                    root=tmp_path,
                    build_views=False,
                    workers=1,
                    client=client,
                )
            )


def test_update_akshare_stock_valuation_fetches_concurrently_but_writes_serially(
    tmp_path,
    monkeypatch,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    client = OverlapAkShareClient(akshare_cn_stock_valuation_eastmoney_sample, fail_codes={"000003"})
    original_write = ParquetStore.write_dataset
    write_lock = threading.Lock()
    active_writes = 0
    max_active_writes = 0

    def observing_write(self, dataset_id: str, df: pd.DataFrame, partition=None, mode=None, skip_existing=False):
        nonlocal active_writes, max_active_writes
        if dataset_id != "akshare_cn_stock_valuation_eastmoney":
            return original_write(self, dataset_id, df, partition, mode, skip_existing)
        with write_lock:
            active_writes += 1
            max_active_writes = max(max_active_writes, active_writes)
        time.sleep(0.01)
        try:
            return original_write(self, dataset_id, df, partition, mode, skip_existing)
        finally:
            with write_lock:
                active_writes -= 1

    monkeypatch.setattr(ParquetStore, "write_dataset", observing_write)

    records = update_akshare(
        _request(
            mode="partial",
            code=("600000", "000001", "000002", "000003"),
            root=tmp_path,
            build_views=False,
            workers=3,
            client=client,
        )
    )

    assert client.max_active_fetches >= 2
    assert max_active_writes == 1
    statuses = {item["code"]: item["status"] for item in records}
    assert statuses == {
        "600000": "success",
        "000001": "success",
        "000002": "success",
        "000003": "failed",
    }
    checkpoints = store.read_pipeline_checkpoints()
    attempted = checkpoints.loc[checkpoints["dataset"] == "akshare_cn_stock_valuation_eastmoney"]
    assert set(attempted["code"].astype(str)) == set(statuses)
    assert not (tmp_path / "data" / "raw").exists()


def test_adaptive_concurrency_controller_reduces_and_recovers() -> None:
    controller = AdaptiveConcurrencyController(max_workers=3)
    for index in range(20):
        controller.record_fetch_result(index not in {0, 5, 10, 15})

    assert controller.target_workers == 2

    for index in range(20):
        controller.record_fetch_result(index not in {1, 6, 11, 16})

    assert controller.target_workers == 1

    for _ in range(50):
        controller.record_fetch_result(True)

    assert controller.target_workers == 2

    for _ in range(50):
        controller.record_fetch_result(True)

    assert controller.target_workers == 3


def test_adaptive_concurrency_controller_reduces_on_consecutive_failures() -> None:
    controller = AdaptiveConcurrencyController(max_workers=3)

    for _ in range(3):
        controller.record_fetch_result(False)

    assert controller.target_workers == 2


def test_update_akshare_stock_valuation_stops_submitting_after_circuit_open(
    tmp_path,
    baostock_cn_stock_basic_sample,
    akshare_cn_stock_valuation_eastmoney_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())
    client = CircuitOpenAkShareClient(akshare_cn_stock_valuation_eastmoney_sample)

    records = update_akshare(
        _request(
            mode="partial",
            code=("600000", "000001", "000002", "000003", "000004"),
            root=tmp_path,
            build_views=False,
            workers=3,
            client=client,
        )
    )

    assert set(client.value_calls).issubset({"600000", "000001", "000002"})
    assert "000003" not in client.value_calls
    assert "000004" not in client.value_calls
    records_by_code = {item["code"]: item for item in records}
    assert set(records_by_code) == {*client.value_calls, "000003", "000004"}
    assert records_by_code["000003"]["status"] == "skipped_circuit_open"
    assert records_by_code["000004"]["status"] == "skipped_circuit_open"
    checkpoints = store.read_pipeline_checkpoints()
    checkpoint_codes = set(checkpoints["code"].astype(str))
    assert checkpoint_codes == set(records_by_code)
    assert not (tmp_path / "data" / "raw").exists()


def _response(endpoint: str, params: dict[str, object], data: pd.DataFrame) -> AkShareResponse:
    return AkShareResponse(
        endpoint=endpoint,
        params=params,
        akshare_version="fake-akshare",
        data=data.copy(),
    )


def _log_entries(logger: ProgressLogger, message: str) -> list[tuple[str, str, tuple[object, ...]]]:
    return [entry for entry in logger.entries if entry[1] == message]


def _write_baostock_cn_trading_calendar(store: ParquetStore, latest_date: str) -> None:
    dates = list(dict.fromkeys(["2024-01-02", "2024-01-03", latest_date]))
    store.write_dataset(
        "baostock_cn_trading_calendar", pd.DataFrame([{"calendar_date": item, "is_trading_day": "1"} for item in dates])
    )


def _write_akshare_universe(
    store: ParquetStore,
    spot_codes: tuple[str, ...] = ("600000",),
    delisted_codes: tuple[str, ...] = ("000001",),
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
    if delisted_codes:
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
                "      akshare_cn_stock_valuation_eastmoney:",
                "        failure_threshold: 2",
                "        cooldown_minutes: 1",
                "datasets:",
                "  akshare_cn_stock_valuation_eastmoney:",
                "    active_only: true",
                "pipeline:",
                "  metadata_flush_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
