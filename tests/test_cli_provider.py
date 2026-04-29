from __future__ import annotations

from click.testing import CliRunner

import src.cli as cli_module


def test_update_daily_full_cli_passes_explicit_provider(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "calendar", "code": "*", "status": "success", "row_count": 1}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-daily", "--mode", "full", "--dataset", "calendar", "--provider", "baostock", "--no-build-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] == "baostock"
    assert captured["mode"] == "full"
    assert captured["dataset"] == "calendar"


def test_update_daily_cli_keeps_provider_optional(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_update_daily(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "daily_k_qfq", "code": "sh.600000", "status": "success", "row_count": 2}]

    monkeypatch.setattr(cli_module, "run_update_daily", fake_update_daily)

    result = CliRunner().invoke(
        cli_module.cli,
        ["update-daily", "--code", "sh.600000", "--no-build-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] is None
    assert captured["mode"] == "partial"
    assert captured["dataset"] == "all"
