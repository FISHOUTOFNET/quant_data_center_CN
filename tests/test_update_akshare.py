from __future__ import annotations

import json
from datetime import date

import pandas as pd

from src.api.akshare_client import AkShareResponse, dataframe_hash
from src.pipeline.akshare_tasks import plan_akshare_tasks
from src.pipeline.update_akshare import update_akshare
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

    client.value_calls.clear()
    records = update_akshare(
        dataset="stock_value_em",
        mode="partial",
        root=tmp_path,
        build_views=False,
        client=client,
    )

    assert client.value_calls == []
    assert [item["status"] for item in records] == ["skipped_checkpoint"]

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
        client=client,
    )

    assert client.value_calls == ["600000", "000001"]


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
