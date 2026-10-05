from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_update_daily.bat"


def _script_text() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_run_update_daily_bat_delegates_to_python_orchestrator() -> None:
    """The BAT must NOT compute a log path itself. It only forwards to the
    Python orchestrator, which owns the unified log-path resolution chain
    (CLI --run-log > QDC_LOG_DIR > settings.yaml > OS default)."""

    text = _script_text()

    # The BAT delegates to the Python orchestrator and forwards all args.
    assert "python -m src.cli run-update-daily" in text
    assert "%*" in text

    # The BAT must NOT hardcode %LOCALAPPDATA% as the log root — that would
    # bypass QDC_LOG_DIR and settings.yaml. Log-path resolution is Python's
    # responsibility now.
    assert "LOCALAPPDATA" not in text
    # The BAT must NOT compute QDC_RUN_LOG_DIR or QDC_RUN_LOG itself.
    assert "QDC_RUN_LOG_DIR" not in text
    assert "QDC_RUN_LOG" not in text
    # The BAT must NOT use PowerShell Get-Date for timestamps — Python owns
    # the run-id and log filename.
    assert "Get-Date" not in text
    # The BAT must NOT redirect stdout/stderr to a log file — the Python
    # orchestrator opens the log file itself (RunLogContext) so it can record
    # orchestrator/child PIDs and handle log rotation.
    assert ">>" not in text.replace(">>", "<<") or "2>&1" not in text
    # The BAT must NOT create a logs/ directory inside the repo.
    assert 'if not exist "logs" mkdir "logs"' not in text
    assert 'set "QDC_RUN_LOG=logs' not in text


def test_run_update_daily_bat_preserves_orchestrator_exit_code() -> None:
    """The BAT must capture and forward the Python orchestrator's exit code."""

    text = _script_text()

    assert 'set "QDC_EXIT_CODE=!errorlevel!"' in text
    assert "endlocal & exit /b %QDC_EXIT_CODE%" in text
    assert ":run_step" not in text
    assert ":run_optional_step" not in text
