from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_update_daily.bat"


def _script_text() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_run_update_daily_bat_delegates_to_python_orchestrator() -> None:
    text = _script_text()

    assert 'if not exist "logs" mkdir "logs"' in text
    assert "Get-Date -Format yyyyMMdd_HHmmss" in text
    assert 'set "QDC_RUN_LOG=logs\\run_update_daily_!QDC_RUN_STAMP!.log"' in text
    assert "python -m src.cli run-update-daily" in text
    assert "--run-log" in text
    assert "!QDC_RUN_LOG!" in text
    assert "%*" in text
    assert 'run-update-daily --run-log "!QDC_RUN_LOG!" %* >> "!QDC_RUN_LOG!" 2>&1' not in text


def test_run_update_daily_bat_preserves_orchestrator_exit_code() -> None:
    text = _script_text()

    assert 'set "QDC_EXIT_CODE=!errorlevel!"' in text
    assert "endlocal & exit /b %QDC_EXIT_CODE%" in text
    assert ":run_step" not in text
    assert ":run_optional_step" not in text
