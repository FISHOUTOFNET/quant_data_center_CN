"""Centralized filesystem paths for the local data center."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

LOG_DIR_ENV = "QDC_LOG_DIR"
DEFAULT_LOG_APP_NAME = "QuantDataCenter"


def project_root() -> Path:
    """Return the repository root.

    QDC_ROOT is useful for tests and one-off scripts; otherwise this file lives
    under <root>/src/utils/paths.py, so parents[2] is the project root.
    """

    env_root = os.getenv("QDC_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


ROOT = project_root()
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
METADATA_DIR = DATA_DIR / "metadata"
DUCKDB_DIR = DATA_DIR / "duckdb"
DUCKDB_FILE = DUCKDB_DIR / "quant.duckdb"
METADATA_DUCKDB_FILE = METADATA_DIR / "qdc_metadata.duckdb"


def resolve_path(path: str | Path, base: Path | None = None) -> Path:
    """Resolve a config path relative to the project root unless absolute."""

    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return ((base or ROOT) / candidate).resolve()


def ensure_dir(path: str | Path) -> Path:
    """Create a directory and return it as a resolved Path."""

    directory = resolve_path(path) if not isinstance(path, Path) else path
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def parquet_dataset_dir(dataset: str, root: Path | None = None) -> Path:
    """Return the Parquet directory for a named dataset."""

    base = root or PARQUET_DIR
    return base / dataset


def baostock_daily_bar_file(dataset: str, code: str, root: Path | None = None) -> Path:
    """Return the single Parquet file for a daily_bar dataset and stock code."""

    return parquet_dataset_dir(dataset, root) / f"code={code}" / "data.parquet"


def baostock_cn_trading_calendar_file(root: Path | None = None) -> Path:
    """Return the trading baostock_cn_trading_calendar Parquet file."""

    return parquet_dataset_dir("baostock_cn_trading_calendar", root) / "data.parquet"


def default_app_data_log_root() -> Path:
    """Return the OS default log root for the project.

    Defaults to ``%LOCALAPPDATA%\\QuantDataCenter\\logs`` on Windows, which is
    outside the Git workspace and survives ``git clean`` / IDE cleanups. Falls
    back to ``~/.local/share/QuantDataCenter/logs`` on POSIX when
    ``LOCALAPPDATA`` is not set.
    """

    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data).expanduser().resolve() / DEFAULT_LOG_APP_NAME / "logs"
    xdg_data_home = os.getenv("XDG_DATA_HOME")
    if xdg_data_home:
        return Path(xdg_data_home).expanduser().resolve() / DEFAULT_LOG_APP_NAME / "logs"
    return Path.home().resolve() / ".local" / "share" / DEFAULT_LOG_APP_NAME / "logs"


def _config_log_dir(root: Path | None) -> Path | None:
    """Return the logs_dir setting from settings.yaml, or ``None`` if missing.

    Imported lazily so ``paths`` stays free of YAML dependencies at import time.
    A configured path that resolves inside the Git workspace is rejected
    (returns ``None``) so that logs always land outside the repo by default.
    """

    try:
        from src.utils.config_mgr import ConfigError, ConfigManager

        manager = ConfigManager(root)
        if not (manager.config_dir / "settings.yaml").exists():
            return None
        configured = manager.get("paths.logs_dir")
        if not configured:
            configured = manager.get("logging.file")
            if configured:
                configured_path = Path(str(configured)).expanduser()
                # Treat logging.file as the application log path; the parent
                # directory is the log root.
                resolved_candidate = configured_path.parent if configured_path.parent else None
                if resolved_candidate is not None:
                    resolved_absolute = (
                        resolved_candidate
                        if resolved_candidate.is_absolute()
                        else (manager.root / resolved_candidate)
                    ).resolve()
                    if not is_path_inside(resolved_absolute, manager.root):
                        return resolved_absolute
                return None
        if configured:
            resolved = resolve_path(str(configured), manager.root)
            # Refuse to put logs back inside the Git workspace: that defeats
            # the whole purpose of the migration.
            if is_path_inside(resolved, manager.root):
                return None
            return resolved
    except (ConfigError, OSError, ValueError):
        return None
    return None


@dataclass(frozen=True)
class RuntimePaths:
    """Immutable runtime path resolution result.

    Provides a single source of truth for log-related paths consumed across
    the CLI, the orchestrator and ``log_cleanup``. Resolution priority is:

    1. ``explicit_log_dir`` argument (CLI ``--log-dir`` / ``--run-log`` parent).
    2. ``QDC_LOG_DIR`` environment variable.
    3. ``paths.logs_dir`` (or ``logging.file``) setting in ``settings.yaml``.
    4. Operating-system default directory outside the Git workspace.

    The application log and per-run logs both live under ``logs_dir`` so that
    cleanup has a single managed root, but they are kept in separate
    subdirectories to make ownership clear (application log is owned by
    Loguru; per-run logs are owned by the orchestrator).
    """

    app_data_dir: Path
    logs_dir: Path
    application_log_path: Path
    run_logs_dir: Path

    @property
    def application_log_dir(self) -> Path:
        return self.application_log_path.parent


def resolve_runtime_paths(
    *,
    explicit_log_dir: str | Path | None = None,
    root: Path | None = None,
) -> RuntimePaths:
    """Resolve the runtime log paths for the current process.

    Args:
        explicit_log_dir: Explicit log directory passed via the CLI. When
            provided it wins over every other source.
        root: Repository root used for ``settings.yaml`` discovery. Defaults to
            :func:`project_root`.

    The returned object's ``logs_dir`` is always resolved to an absolute path.
    The directory is *not* created here; callers (``configure_logging`` and
    ``RunLogContext``) create it lazily so that read-only commands do not
    produce side effects.
    """

    base_root = (root or ROOT).resolve()
    resolved: Path | None = None
    if explicit_log_dir:
        resolved = Path(explicit_log_dir).expanduser().resolve()
    if resolved is None:
        env_dir = os.getenv(LOG_DIR_ENV)
        if env_dir:
            resolved = Path(env_dir).expanduser().resolve()
    if resolved is None:
        config_dir = _config_log_dir(base_root)
        if config_dir is not None:
            resolved = config_dir.resolve()
    if resolved is None:
        resolved = default_app_data_log_root()

    app_data_dir = resolved
    application_log_path = resolved / "application" / "qdc.log"
    run_logs_dir = resolved / "runs"
    return RuntimePaths(
        app_data_dir=app_data_dir,
        logs_dir=resolved,
        application_log_path=application_log_path,
        run_logs_dir=run_logs_dir,
    )


def is_path_inside(path: Path, parent: Path) -> bool:
    """Return ``True`` when ``path`` lives inside ``parent`` (both resolved)."""

    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True
