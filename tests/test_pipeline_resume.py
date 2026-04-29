from __future__ import annotations

import threading

import pandas as pd

import src.pipeline.init_history as init_history_module
import src.pipeline.update_daily as update_daily_module
from src.storage.parquet_store import ParquetStore


def test_init_history_resumes_failed_code(tmp_path, monkeypatch, daily_sample, stock_basic_sample) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample(), fail_once={"sz.000001"})
    monkeypatch.setattr(init_history_module, "create_provider", provider_factory)

    first = init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    second = init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in first if item["dataset"] == "daily_k_qfq"] == [
        "success",
        "success",
        "failed",
    ]
    assert first_history_calls == ["sh.000001", "sh.600000", "sz.000001"]
    assert state["history_calls"][len(first_history_calls) :] == ["sz.000001"]
    assert [item["status"] for item in second if item["dataset"] == "daily_k_qfq"] == [
        "skipped",
        "skipped",
        "success",
    ]


def test_init_history_resumes_write_failure(tmp_path, monkeypatch, daily_sample, stock_basic_sample) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(init_history_module, "create_provider", provider_factory)

    original_write_daily_k = ParquetStore.write_daily_k
    failed_once = {"value": False}

    def flaky_write_daily_k(self, dataset: str, code: str, df: pd.DataFrame):
        if code == "sz.000001" and not failed_once["value"]:
            failed_once["value"] = True
            raise RuntimeError("temporary parquet write failure")
        return original_write_daily_k(self, dataset, code, df)

    monkeypatch.setattr(ParquetStore, "write_daily_k", flaky_write_daily_k)

    first = init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
    )
    second = init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
    )

    assert [item["status"] for item in first if item["dataset"] == "daily_k_qfq"] == ["success", "failed"]
    assert [item["status"] for item in second if item["dataset"] == "daily_k_qfq"] == ["skipped", "success"]
    assert state["history_calls"] == ["sh.600000", "sz.000001", "sz.000001"]


def test_init_history_fetches_next_code_while_previous_write_is_pending(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, _state = _fake_provider_factory(stock_basic_sample(), daily_sample())

    first_write_started = threading.Event()
    release_first_write = threading.Event()
    second_fetch_seen = threading.Event()
    original_write_daily_k = ParquetStore.write_daily_k

    def slow_first_write(self, dataset: str, code: str, df: pd.DataFrame):
        if code == "sh.600000":
            first_write_started.set()
            release_first_write.wait(timeout=5)
        return original_write_daily_k(self, dataset, code, df)

    class ObservingProvider(provider_factory.provider_cls):
        def query_daily_k(
            self,
            request,
        ) -> pd.DataFrame:
            result = super().query_daily_k(request)
            if request.code == "sz.000001":
                second_fetch_seen.set()
            return result

    monkeypatch.setattr(init_history_module, "create_provider", _provider_factory_for(ObservingProvider))
    monkeypatch.setattr(ParquetStore, "write_daily_k", slow_first_write)

    errors = []

    def run_pipeline() -> None:
        try:
            init_history_module.init_history(
                dataset="daily_k_qfq",
                start="2024-01-01",
                end="2024-01-31",
                code=("sh.600000", "sz.000001"),
                root=tmp_path,
                build_views=False,
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_pipeline)
    thread.start()
    try:
        assert first_write_started.wait(timeout=2)
        assert second_fetch_seen.wait(timeout=2)
    finally:
        release_first_write.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []


def test_init_history_resolves_non_trading_end_to_trading_bound(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(init_history_module, "create_provider", provider_factory)

    init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-06",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert state["history_params"] == [
        {
            "code": "sh.600000",
            "start_date": "1990-01-01",
            "end_date": "2024-01-05",
            "adjustflag": "2",
        }
    ]


def test_init_history_checkpoint_lookup_reads_checkpoints_once_per_run(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(init_history_module, "create_provider", provider_factory)

    init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    read_calls = {"count": 0}
    original_read_pipeline_checkpoints = ParquetStore.read_pipeline_checkpoints

    def counted_read_pipeline_checkpoints(self):
        read_calls["count"] += 1
        return original_read_pipeline_checkpoints(self)

    monkeypatch.setattr(ParquetStore, "read_pipeline_checkpoints", counted_read_pipeline_checkpoints)

    init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        code="sh.600000",
        root=tmp_path,
        build_views=False,
    )

    assert read_calls["count"] == 1
    assert state["history_calls"] == first_history_calls


def test_init_history_batches_daily_checkpoints_by_flush_size(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path, metadata_flush_size=2)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(init_history_module, "create_provider", provider_factory)

    flush_sizes = []
    original_persist_update_metadata = ParquetStore.persist_update_metadata

    def counted_persist_update_metadata(self, run_rows, status_rows, checkpoint_rows):
        flush_sizes.append(len(checkpoint_rows))
        return original_persist_update_metadata(self, run_rows, status_rows, checkpoint_rows)

    monkeypatch.setattr(ParquetStore, "persist_update_metadata", counted_persist_update_metadata)

    records = init_history_module.init_history(
        dataset="daily_k_qfq",
        start="2024-01-01",
        end="2024-01-31",
        code=("sh.000001", "sh.600000", "sz.000001"),
        root=tmp_path,
        build_views=False,
    )

    daily_records = [item for item in records if item["dataset"] == "daily_k_qfq"]
    assert [item["status"] for item in daily_records] == ["success", "success", "success"]
    assert state["history_calls"] == ["sh.000001", "sh.600000", "sz.000001"]
    assert flush_sizes == [2, 1]

    checkpoints = ParquetStore(root=tmp_path).read_pipeline_checkpoints()
    assert set(checkpoints["code"].astype(str)) == {"sh.000001", "sh.600000", "sz.000001"}


def test_update_daily_uses_active_stock_basic_codes_and_resumes(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    update_daily_module.update_daily(
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert first_history_calls == ["sh.600000", "sh.600000", "sh.600000"]
    assert "sz.000001" not in first_history_calls
    assert state["history_calls"] == first_history_calls
    assert state["stock_basic_calls"] == 1


def test_update_daily_checkpoint_lookup_reads_checkpoints_once_per_run(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    first_history_calls = list(state["history_calls"])

    read_calls = {"count": 0}
    original_read_pipeline_checkpoints = ParquetStore.read_pipeline_checkpoints

    def counted_read_pipeline_checkpoints(self):
        read_calls["count"] += 1
        return original_read_pipeline_checkpoints(self)

    monkeypatch.setattr(ParquetStore, "read_pipeline_checkpoints", counted_read_pipeline_checkpoints)

    update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert read_calls["count"] == 1
    assert state["history_calls"] == first_history_calls


def test_update_daily_resolves_weekend_end_to_previous_trading_day(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    update_daily_module.update_daily(
        end="2024-01-06",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )
    first_history_params = list(state["history_params"])

    update_daily_module.update_daily(
        end="2024-01-06",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert {item["start_date"] for item in first_history_params} == {"1990-01-01"}
    assert {item["end_date"] for item in first_history_params} == {"2024-01-05"}
    assert state["history_params"] == first_history_params
    assert state["stock_basic_calls"] == 1

    store = ParquetStore(root=tmp_path)
    assert store.stock_basic_path().exists()
    checkpoints = store.read_pipeline_checkpoints()
    end_dates = pd.to_datetime(checkpoints["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    assert set(end_dates.dropna()) == {"2024-01-05"}


def test_update_daily_refetches_full_history_on_lookback_mismatch(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    adjustflags = {"daily_k_none": "3", "daily_k_qfq": "2", "daily_k_hfq": "1"}
    for dataset, adjustflag in adjustflags.items():
        existing = daily_sample().assign(code="sh.600000", adjustflag=adjustflag)
        existing.loc[0, "close"] = 99.0
        store.write_daily_k(dataset, "sh.600000", existing)

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_sample())
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    history_starts = [item["start_date"] for item in state["history_params"]]
    assert history_starts == [
        "2024-01-02",
        "1990-01-01",
        "2024-01-02",
        "1990-01-01",
        "2024-01-02",
        "1990-01-01",
    ]
    daily_records = [item for item in records if item["dataset"].startswith("daily_k_")]
    assert [item["start_date"] for item in daily_records] == ["1990-01-01", "1990-01-01", "1990-01-01"]
    assert store.read_daily_k("daily_k_qfq", "sh.600000").loc[0, "close"] == 8.2


def test_update_daily_refetches_full_history_when_lookback_is_empty(
    tmp_path,
    monkeypatch,
    daily_sample,
    stock_basic_sample,
) -> None:
    _write_settings(tmp_path)
    empty_daily = daily_sample().iloc[0:0]

    def daily_by_start(**kwargs) -> pd.DataFrame:
        if kwargs["start_date"] == "1990-01-01":
            return daily_sample()
        return empty_daily

    provider_factory, state = _fake_provider_factory(stock_basic_sample(), daily_by_start)
    monkeypatch.setattr(update_daily_module, "create_provider", provider_factory)

    records = update_daily_module.update_daily(
        code="sh.600000",
        end="2024-01-03",
        lookback_days=1,
        root=tmp_path,
        build_views=False,
    )

    assert [item["start_date"] for item in state["history_params"]] == [
        "1990-01-01",
        "1990-01-01",
        "1990-01-01",
    ]
    daily_records = [item for item in records if item["dataset"].startswith("daily_k_")]
    assert [item["row_count"] for item in daily_records] == [2, 2, 2]


def _fake_provider_factory(stock_basic_df: pd.DataFrame, daily_df: pd.DataFrame, fail_once: set[str] | None = None):
    state = {
        "history_calls": [],
        "history_params": [],
        "calendar_params": [],
        "stock_basic_calls": 0,
        "fail_once": set(fail_once or set()),
    }

    class FakeProvider:
        name = "fake"

        def __init__(self, config=None) -> None:
            self.config = config

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def query_trade_dates(
            self,
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> pd.DataFrame:
            resolved_start = start_date or "1990-01-01"
            resolved_end = end_date or "2024-12-31"
            state["calendar_params"].append({"start_date": resolved_start, "end_date": resolved_end})
            dates = pd.date_range(resolved_start, resolved_end, freq="D")
            return pd.DataFrame(
                [
                    {
                        "calendar_date": item.date(),
                        "is_trading_day": "1" if item.weekday() < 5 else "0",
                    }
                    for item in dates
                ]
            )

        def query_stock_basic(self) -> pd.DataFrame:
            state["stock_basic_calls"] += 1
            return stock_basic_df.copy()

        def query_daily_k(
            self,
            request,
        ) -> pd.DataFrame:
            code = request.code
            start_date = request.start_date
            end_date = request.end_date
            adjustflag = _adjustflag_for_dataset(request.dataset)
            state["history_calls"].append(code)
            state["history_params"].append(
                {
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "adjustflag": adjustflag,
                }
            )
            if code in state["fail_once"]:
                state["fail_once"].remove(code)
                raise RuntimeError(f"temporary failure for {code}")
            source = daily_df(
                code=code,
                fields=request.fields,
                start_date=start_date,
                end_date=end_date,
                frequency=request.frequency,
                adjustflag=adjustflag,
            ) if callable(daily_df) else daily_df
            return source.assign(code=code, adjustflag=adjustflag).copy()

    factory = _provider_factory_for(FakeProvider)
    factory.provider_cls = FakeProvider
    return factory, state


def _provider_factory_for(provider_cls):
    def create_provider(config, provider: str | None = None):
        return provider_cls(config)

    create_provider.provider_cls = provider_cls
    return create_provider


def _adjustflag_for_dataset(dataset: str) -> str:
    return {"daily_k_none": "3", "daily_k_qfq": "2", "daily_k_hfq": "1"}[dataset]


def _write_settings(root, metadata_flush_size: int | None = None) -> None:
    config_dir = root / "config"
    config_dir.mkdir()
    pipeline_lines = [
        "pipeline:",
        "  lookback_days: 1",
        "  max_retries: 1",
    ]
    if metadata_flush_size is not None:
        pipeline_lines.append(f"  metadata_flush_size: {metadata_flush_size}")
    (config_dir / "settings.yaml").write_text(
        "\n".join(
            [
                "api:",
                "  baostock:",
                "    adjustflag_map:",
                '      none: "3"',
                '      qfq: "2"',
                '      hfq: "1"',
                "datasets:",
                "  daily_k:",
                '    fields: "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"',
                "    frequency: d",
                *pipeline_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )
