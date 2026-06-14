from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.sources.derived import update as update_module
from src.sources.derived.common import BuildDerivedLockError, build_derived_file_lock
from src.sources.derived.stock_daily_bar import build_cn_stock_daily_bar
from src.storage.parquet_store import ParquetStore

NOW = datetime(2024, 1, 5, 12, 0)


@pytest.mark.parametrize(
    ("targets", "expected"),
    [
        (("daily_bar",), ["security_master", "daily_bar"]),
        (("valuation",), ["security_master", "valuation"]),
        (("all",), ["security_master", "daily_bar", "valuation"]),
        (("security_master",), ["security_master"]),
    ],
)
def test_build_derived_datasets_uses_safe_target_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    targets: tuple[str, ...],
    expected: list[str],
) -> None:
    calls: list[str] = []

    def fake_builder(target: str, dataset: str):
        def build(**kwargs):
            del kwargs
            calls.append(target)
            return {"dataset": dataset, "status": "success", "rows": 1}

        return build

    monkeypatch.setattr(update_module, "build_security_master", fake_builder("security_master", "cn_security_master"))
    monkeypatch.setattr(update_module, "build_cn_stock_daily_bar", fake_builder("daily_bar", "cn_stock_daily_bar"))
    monkeypatch.setattr(update_module, "build_cn_stock_valuation", fake_builder("valuation", "cn_stock_valuation"))

    update_module.build_derived_datasets(
        root=tmp_path,
        targets=targets,
        build_views=False,
        refresh_registry=False,
    )

    assert calls == expected


def test_build_derived_file_lock_blocks_concurrent_builds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_security_master(**kwargs):
        del kwargs
        return {"dataset": "cn_security_master", "status": "success", "rows": 1}

    monkeypatch.setattr(update_module, "build_security_master", fake_security_master)

    with (
        build_derived_file_lock(tmp_path, ("security_master",)),
        pytest.raises(BuildDerivedLockError, match="build-derived is already running"),
    ):
        update_module.build_derived_datasets(
            root=tmp_path,
            targets=("security_master",),
            build_views=False,
            refresh_registry=False,
        )

    result = update_module.build_derived_datasets(
        root=tmp_path,
        targets=("security_master",),
        build_views=False,
        refresh_registry=False,
    )

    assert result == [{"dataset": "cn_security_master", "status": "success", "rows": 1}]


def test_build_derived_file_lock_writes_process_lock_owner(tmp_path: Path) -> None:
    with build_derived_file_lock(tmp_path, ("security_master",), stale_after_seconds=123) as lock_dir:
        owner = json.loads((lock_dir / "owner.json").read_text(encoding="utf-8"))
        assert owner["lock_name"] == "build-derived"
        assert owner["purpose"] == "build-derived"
        assert owner["stale_after_seconds"] == 123
        assert owner["target"] == ["security_master"]

    assert not lock_dir.exists()


def test_build_derived_incremental_passes_explicit_security_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[str, ...] | None]] = []

    def fake_builder(target: str, dataset: str):
        def build(**kwargs):
            calls.append((target, kwargs["security_ids"]))
            return {"dataset": dataset, "status": "success", "rows": 1}

        return build

    monkeypatch.setattr(update_module, "build_security_master", fake_builder("security_master", "cn_security_master"))
    monkeypatch.setattr(update_module, "build_cn_stock_daily_bar", fake_builder("daily_bar", "cn_stock_daily_bar"))

    update_module.build_derived_datasets(
        root=tmp_path,
        targets=("daily_bar",),
        mode="incremental",
        security_ids=("sh.600000",),
        build_views=False,
        refresh_registry=False,
    )

    assert calls == [("security_master", None), ("daily_bar", ("SH.600000",))]


def test_build_derived_full_ignores_incremental_partition_planning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[str, ...] | None]] = []

    def fake_builder(target: str, dataset: str):
        def build(**kwargs):
            calls.append((target, kwargs["security_ids"]))
            return {"dataset": dataset, "status": "success", "rows": 1}

        return build

    monkeypatch.setattr(update_module, "build_security_master", fake_builder("security_master", "cn_security_master"))
    monkeypatch.setattr(update_module, "build_cn_stock_daily_bar", fake_builder("daily_bar", "cn_stock_daily_bar"))

    update_module.build_derived_datasets(
        root=tmp_path,
        targets=("daily_bar",),
        mode="full",
        build_views=False,
        refresh_registry=False,
    )

    assert calls == [("security_master", None), ("daily_bar", None)]


def test_build_derived_incremental_falls_back_to_full_when_target_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[str, ...] | None]] = []

    def fake_security_master(**kwargs):
        calls.append(("security_master", kwargs["security_ids"]))
        return {"dataset": "cn_security_master", "status": "success", "rows": 1}

    def fake_daily_bar(**kwargs):
        calls.append(("daily_bar", kwargs["security_ids"]))
        return {"dataset": "cn_stock_daily_bar", "status": "success", "rows": 1}

    monkeypatch.setattr(update_module, "build_security_master", fake_security_master)
    monkeypatch.setattr(update_module, "build_cn_stock_daily_bar", fake_daily_bar)

    update_module.build_derived_datasets(
        root=tmp_path,
        targets=("daily_bar",),
        mode="incremental",
        build_views=False,
        refresh_registry=False,
    )

    assert calls == [("security_master", None), ("daily_bar", None)]


def test_incremental_detects_source_business_change_with_old_mtime(tmp_path: Path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_dataset("cn_security_master", _master())
    source_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"}
    ).primary_path
    build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    old_mtime = source_path.stat().st_mtime

    changed_source = daily_sample()
    changed_source.loc[0, "close"] = 9.9
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", changed_source, {"code": "sh.600000"})
    os.utime(source_path, (old_mtime, old_mtime))

    changed = update_module._changed_security_ids_for_target(
        store, store.read_dataset("cn_security_master"), "daily_bar", None
    )

    assert changed == ("SH.600000",)


def test_incremental_ignores_touch_when_semantic_hash_unchanged(tmp_path: Path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_dataset("cn_security_master", _master())
    source_path = store.write_dataset(
        "baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"}
    ).primary_path
    build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    os.utime(source_path, None)

    changed = update_module._changed_security_ids_for_target(
        store, store.read_dataset("cn_security_master"), "daily_bar", None
    )

    assert changed == ()


def test_incremental_ignores_fetched_at_only_change(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    master = _master().assign(baostock_code="", akshare_code="600000")
    store.write_dataset("cn_security_master", master)
    store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted", _akshare_daily("2024-01-03 12:00:00"), {"code": "600000"}
    )
    build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)

    store.write_dataset(
        "akshare_cn_stock_daily_bar_unadjusted", _akshare_daily("2024-01-04 12:00:00"), {"code": "600000"}
    )

    changed = update_module._changed_security_ids_for_target(
        store, store.read_dataset("cn_security_master"), "daily_bar", None
    )

    assert changed == ()


def test_incremental_detects_master_row_hash_change(tmp_path: Path, daily_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_dataset("cn_security_master", _master())
    store.write_dataset("baostock_cn_stock_daily_bar_unadjusted", daily_sample(), {"code": "sh.600000"})
    build_cn_stock_daily_bar(root=tmp_path, build_views=False, refresh_registry=False, now=lambda: NOW)
    changed_master = store.read_dataset("cn_security_master")
    changed_master.loc[0, "name"] = "Renamed Bank"

    changed = update_module._changed_security_ids_for_target(store, changed_master, "daily_bar", None)

    assert changed == ("SH.600000",)


def _master() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "SH.600000",
                "code": "600000",
                "exchange": "SH",
                "name": "PF Bank",
                "security_type": "1",
                "board": "main",
                "baostock_code": "sh.600000",
                "akshare_code": "600000",
                "qlib_symbol": "sh600000",
                "ipo_date": date(1999, 11, 10),
                "delist_date": None,
                "listing_status": "active",
                "is_active": True,
                "source_priority": "mixed",
                "latest_source_date": date(2024, 1, 5),
                "updated_at": NOW,
            }
        ]
    )


def _akshare_daily(fetched_at: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": date(2024, 1, 2),
                "code": "600000",
                "source_symbol": "600000",
                "open": 8.1,
                "high": 8.4,
                "low": 8.0,
                "close": 8.2,
                "volume": 1000,
                "amount": 8200.0,
                "amplitude": 1.0,
                "pct_change": 2.5,
                "price_change": 0.2,
                "turnover_rate": 0.1,
                "adjustment": "unadjusted",
                "source_endpoint": "stock_zh_a_hist",
                "quality_status": "daily_bar_confirmed",
                "fetched_at": pd.Timestamp(fetched_at),
            }
        ]
    )
