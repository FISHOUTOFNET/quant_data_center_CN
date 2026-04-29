from __future__ import annotations

from click.testing import CliRunner

import src.cli as cli_module


def test_init_history_cli_passes_explicit_provider(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_init_history(**kwargs):
        captured.update(kwargs)
        return [{"dataset": "calendar", "code": "*", "status": "success", "rows": 1, "path": "calendar.parquet"}]

    monkeypatch.setattr(cli_module, "run_init_history", fake_init_history)

    result = CliRunner().invoke(
        cli_module.cli,
        ["init-history", "--dataset", "calendar", "--provider", "baostock", "--no-build-views"],
    )

    assert result.exit_code == 0
    assert captured["provider"] == "baostock"


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
