from __future__ import annotations

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
