from __future__ import annotations

import json
import threading
import time
from datetime import date

import pandas as pd

import src.pipeline.update_akshare as update_akshare_module
from src.api.akshare_client import AkShareCircuitOpen, AkShareResponse, dataframe_hash
from src.pipeline.akshare_tasks import plan_akshare_tasks
from src.pipeline.common import write_checkpoint
from src.pipeline.update_akshare import _AdaptiveConcurrencyController, update_akshare
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


class FakeAkShareClient:
    akshare_version = "fake-akshare"

    def __init__(self, stock_value_em_sample, stock_institute_hold_sample) -> None:
        self.value_calls: list[str] = []
        self.hold_calls: list[str] = []
        self._stock_value_em_sample = stock_value_em_sample
        self._stock_institute_hold_sample = stock_institute_hold_sample

    def fetch_stock_value(self, code: str) -> AkShareResponse:
        self.value_calls.append(code)
        data = self._stock_value_em_sample(code)
        return _response("stock_value_em", {"symbol": code}, data)

    def fetch_stock_institute_hold(self, period: str) -> AkShareResponse:
        self.hold_calls.append(period)
        data = self._stock_institute_hold_sample().assign(report_period=period)
        return _response("stock_institute_hold", {"symbol": period.replace("Q", "")}, data)


class OverlapAkShareClient(FakeAkShareClient):
    def __init__(self, stock_value_em_sample, stock_institute_hold_sample, fail_codes: set[str] | None = None) -> None:
        super().__init__(stock_value_em_sample, stock_institute_hold_sample)
        self._fail_codes = fail_codes or set()
        self._lock = threading.Lock()
        self._overlap_seen = threading.Event()
        self._active_fetches = 0
        self.max_active_fetches = 0

    def fetch_stock_value(self, code: str) -> AkShareResponse:
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
            data = self._stock_value_em_sample(code)
            return _response("stock_value_em", {"symbol": code}, data)
        finally:
            with self._lock:
                self._active_fetches -= 1


class CircuitOpenAkShareClient(FakeAkShareClient):
    def __init__(self, stock_value_em_sample, stock_institute_hold_sample) -> None:
        super().__init__(stock_value_em_sample, stock_institute_hold_sample)
        self._lock = threading.Lock()

    def fetch_stock_value(self, code: str) -> AkShareResponse:
        with self._lock:
            self.value_calls.append(code)
        if code == "600000":
            raise AkShareCircuitOpen("planned circuit open")
        time.sleep(0.05)
        data = self._stock_value_em_sample(code)
        return _response("stock_value_em", {"symbol": code}, data)


def test_update_akshare_stock_value_partial_active_only_resume_and_force(
    tmp_path,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    _write_calendar(store, "2024-01-03")
    client = FakeAkShareClient(stock_value_em_sample, stock_institute_hold_sample)

    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        root=tmp_path,
        build_views=False,
        client=client,
    )

    assert client.value_calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    assert store.stock_value_em_path("600000").exists()
    manifest_count = len(_manifest_rows(tmp_path))
    checkpoint_count = len(store.read_pipeline_checkpoints())

    client.value_calls.clear()
    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        root=tmp_path,
        build_views=False,
        client=client,
    )

    assert client.value_calls == []
    assert records == []
    assert len(_manifest_rows(tmp_path)) == manifest_count
    assert len(store.read_pipeline_checkpoints()) == checkpoint_count

    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        root=tmp_path,
        build_views=False,
        force=True,
        client=client,
    )

    assert client.value_calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]

    manifest_rows = _manifest_rows(tmp_path)
    assert manifest_rows[-1]["pipeline"] == "update_akshare"
    assert manifest_rows[-1]["dataset"] == "stock_value_em"
    assert manifest_rows[-1]["endpoint"] == "stock_value_em"
    assert manifest_rows[-1]["code"] == "600000"
    assert manifest_rows[-1]["params"] == {"symbol": "600000"}
    assert manifest_rows[-1]["status"] == "success"
    assert manifest_rows[-1]["raw_path"]


def test_stock_value_prefilter_kept_task_ignores_checkpoint_and_logs_unchanged(
    tmp_path,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
    monkeypatch,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    _write_calendar(store, "2024-01-04")
    existing_path = store.write_stock_value_em("600000", stock_value_em_sample("600000"))
    write_checkpoint(
        store,
        update_akshare_module.PIPELINE_UPDATE_AKSHARE,
        "stock_value_em",
        "600000",
        "2024-01-02",
        "2024-01-03",
        "success",
        2,
        existing_path,
    )
    client = FakeAkShareClient(stock_value_em_sample, stock_institute_hold_sample)
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

    monkeypatch.setattr(update_akshare_module, "logger", FakeLogger())

    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        code=("600000",),
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert client.value_calls == ["600000"]
    assert [item["status"] for item in records] == ["success"]
    assert (
        "AkShare stock_value_em unchanged code={} rows={} path={}",
        ("600000", 2, existing_path),
    ) in logs


def test_update_akshare_stock_value_full_and_max_tasks(
    tmp_path,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    client = FakeAkShareClient(stock_value_em_sample, stock_institute_hold_sample)

    update_akshare(
        dataset="stock_value_em",
        mode="full",
        max_tasks=2,
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert client.value_calls == ["600000", "000001"]


def test_stock_value_task_pool_excludes_non_common_types_in_full_and_include_inactive(
    tmp_path,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    stock_basic = pd.concat(
        [
            stock_basic_sample(),
            pd.DataFrame(
                [
                    {
                        "code": "sh.000044",
                        "code_name": "SSE Midcap",
                        "ipoDate": date(2009, 7, 3),
                        "outDate": None,
                        "type": "2",
                        "status": "1",
                    },
                ]
            ),
        ],
        ignore_index=True,
    )
    store.write_stock_basic(stock_basic)
    config = ConfigManager(tmp_path)

    partial = plan_akshare_tasks(config=config, store=store, dataset="stock_value_em", mode="partial")
    include_inactive = plan_akshare_tasks(
        config=config,
        store=store,
        dataset="stock_value_em",
        mode="partial",
        include_inactive=True,
    )
    full = plan_akshare_tasks(config=config, store=store, dataset="stock_value_em", mode="full")

    assert [task.code for task in partial] == ["600000"]
    assert [task.code for task in include_inactive] == ["600000", "000001"]
    assert [task.code for task in full] == ["600000", "000001"]


def test_update_akshare_stock_institute_hold_partial_and_full_task_selection(
    tmp_path,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
) -> None:
    _write_settings(tmp_path, lookback_quarters=2)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    client = FakeAkShareClient(stock_value_em_sample, stock_institute_hold_sample)

    update_akshare(
        dataset="stock_institute_hold",
        mode="partial",
        end_quarter="2024Q2",
        max_tasks=1,
        root=tmp_path,
        build_views=False,
        client=client,
    )

    assert client.hold_calls == ["2024Q1"]
    assert store.stock_institute_hold_path("2024Q1").exists()

    tasks = plan_akshare_tasks(
        config=ConfigManager(tmp_path),
        store=store,
        dataset="stock_institute_hold",
        mode="full",
        start_quarter="2024Q1",
        end_quarter="2024Q2",
    )
    assert [task.report_period for task in tasks] == ["2024Q1", "2024Q2"]


def test_update_akshare_accepts_repeated_code_option(
    tmp_path,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    client = FakeAkShareClient(stock_value_em_sample, stock_institute_hold_sample)

    update_akshare(
        dataset="stock_value_em",
        mode="partial",
        code=("600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
        workers=1,
        client=client,
    )

    assert client.value_calls == ["600000", "000001"]


def test_update_akshare_stock_value_fetches_concurrently_but_writes_serially(
    tmp_path,
    monkeypatch,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    client = OverlapAkShareClient(stock_value_em_sample, stock_institute_hold_sample, fail_codes={"000003"})
    original_write = ParquetStore.write_stock_value_em
    write_lock = threading.Lock()
    active_writes = 0
    max_active_writes = 0

    def observing_write(self, code: str, df: pd.DataFrame):
        nonlocal active_writes, max_active_writes
        with write_lock:
            active_writes += 1
            max_active_writes = max(max_active_writes, active_writes)
        time.sleep(0.01)
        try:
            return original_write(self, code, df)
        finally:
            with write_lock:
                active_writes -= 1

    monkeypatch.setattr(ParquetStore, "write_stock_value_em", observing_write)

    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        code=("600000", "000001", "000002", "000003"),
        root=tmp_path,
        build_views=False,
        workers=3,
        client=client,
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
    manifest_rows = _manifest_rows(tmp_path)
    assert {item["code"] for item in manifest_rows} == set(statuses)
    checkpoints = store.read_pipeline_checkpoints()
    attempted = checkpoints.loc[checkpoints["dataset"] == "stock_value_em"]
    assert set(attempted["code"].astype(str)) == set(statuses)


def test_adaptive_concurrency_controller_reduces_and_recovers() -> None:
    controller = _AdaptiveConcurrencyController(max_workers=3)
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
    controller = _AdaptiveConcurrencyController(max_workers=3)

    for _ in range(3):
        controller.record_fetch_result(False)

    assert controller.target_workers == 2


def test_update_akshare_stock_value_stops_submitting_after_circuit_open(
    tmp_path,
    stock_basic_sample,
    stock_value_em_sample,
    stock_institute_hold_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_stock_basic(stock_basic_sample())
    client = CircuitOpenAkShareClient(stock_value_em_sample, stock_institute_hold_sample)

    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        code=("600000", "000001", "000002", "000003", "000004"),
        root=tmp_path,
        build_views=False,
        workers=3,
        client=client,
    )

    assert set(client.value_calls).issubset({"600000", "000001", "000002"})
    assert "000003" not in client.value_calls
    assert "000004" not in client.value_calls
    attempted_codes = {item["code"] for item in records}
    assert attempted_codes == set(client.value_calls)
    assert "000003" not in attempted_codes
    assert "000004" not in attempted_codes
    manifest_codes = {item["code"] for item in _manifest_rows(tmp_path)}
    assert manifest_codes == attempted_codes
    checkpoints = store.read_pipeline_checkpoints()
    checkpoint_codes = set(checkpoints["code"].astype(str))
    assert "000003" not in checkpoint_codes
    assert "000004" not in checkpoint_codes


def _response(endpoint: str, params: dict[str, object], data: pd.DataFrame) -> AkShareResponse:
    raw = data.copy()
    return AkShareResponse(
        endpoint=endpoint,
        params=params,
        akshare_version="fake-akshare",
        raw_df=raw,
        data=data.copy(),
        data_hash=dataframe_hash(raw),
    )


def _manifest_rows(root) -> list[dict[str, object]]:
    path = root / "data" / "raw" / "akshare" / "manifest" / "fetch_runs.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _write_calendar(store: ParquetStore, latest_date: str) -> None:
    dates = list(dict.fromkeys(["2024-01-02", "2024-01-03", latest_date]))
    store.write_calendar(
        pd.DataFrame(
            [{"calendar_date": item, "is_trading_day": "1"} for item in dates]
        )
    )


def _write_settings(root, lookback_quarters: int = 2) -> None:
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
                f"    lookback_quarters: {lookback_quarters}",
                "    endpoints:",
                "      stock_institute_hold:",
                "        failure_threshold: 2",
                "        cooldown_minutes: 1",
                "      stock_value_em:",
                "        failure_threshold: 2",
                "        cooldown_minutes: 1",
                "datasets:",
                "  stock_institute_hold:",
                "    start_quarter: 2024Q1",
                "  stock_value_em:",
                "    active_only: true",
                "pipeline:",
                "  metadata_flush_size: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
