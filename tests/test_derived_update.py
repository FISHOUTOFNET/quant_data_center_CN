from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.sources.derived import update as update_module
from src.sources.derived.common import BuildDerivedLockError, build_derived_file_lock


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
