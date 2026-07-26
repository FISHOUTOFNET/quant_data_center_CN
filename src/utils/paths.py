"""Centralized filesystem paths for the local data center."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOG_DIR_ENV = "QDC_LOG_DIR"
DEFAULT_LOG_APP_NAME = "QuantDataCenter"

# ---------------------------------------------------------------------------
# Managed log-root marker (single authority for the authorization boundary).
#
# ``log_cleanup`` MUST NOT delete files from any directory that does not carry
# a valid marker. The marker is created by ``ensure_managed_log_root`` (called
# from ``resolve_runtime_paths`` / ``create_run_log_context``) and validated by
# ``validate_managed_log_root`` (called from ``cleanup_logs``). Keeping both
# functions here means ``log_cleanup`` and ``run_logging`` share exactly one
# marker format instead of each writing their own.
# ---------------------------------------------------------------------------

MANAGED_ROOT_MARKER = ".qdc-managed-log-root"
MANAGED_ROOT_LAYOUT_VERSION = 1
MANAGED_ROOT_APPLICATION = "QuantDataCenter"


class LogRootAuthorizationError(RuntimeError):
    """Raised when a log root is not authorized for cleanup.

    This is the single exception type used by both ``ensure_managed_log_root``
    (when initialization is unsafe) and ``validate_managed_log_root`` (when the
    marker is missing/invalid/wrong). ``log_cleanup`` surfaces it as a
    ``LogCleanupError`` for CLI consistency.
    """


def ensure_managed_log_root(log_root: Path) -> None:
    """Create the managed-root marker in ``log_root`` if it is safe to do so.

    Called by :func:`resolve_runtime_paths` (when resolving the default log
    root) and by :func:`src.tools.run_logging.create_run_log_context` (when
    creating the per-run log). The directory is created if missing, and the
    marker is written atomically. Unsafe roots (repo-internal, home, drive
    root, any symlink, junction) are rejected with
    :class:`LogRootAuthorizationError`.

    When a marker already exists, it is VALIDATED (not blindly trusted) so a
    corrupt or wrong-application marker cannot silently pass as authorized.
    This also makes ``ensure_managed_log_root`` idempotent for legitimate
    markers and fail-closed for tampered ones.

    This is the ONLY function that creates the marker. ``cleanup_logs`` must
    NOT create it — cleanup must only validate an existing marker.
    """

    root = Path(log_root).expanduser()
    _reject_unsafe_log_root(root)
    resolved = root.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    marker = resolved / MANAGED_ROOT_MARKER
    if marker.exists():
        # Marker already present — validate it so a corrupt or wrong-application
        # marker cannot silently pass as authorized. This is the P0-3 contract:
        # ``ensure`` must not blindly trust an existing marker, and
        # ``--initialize-managed-root`` must not overwrite a tampered one.
        validate_managed_log_root(root)
        return
    payload = {
        "application": MANAGED_ROOT_APPLICATION,
        "layout_version": MANAGED_ROOT_LAYOUT_VERSION,
    }
    tmp_path = marker.with_name(f".{marker.name}.{os.getpid()}.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(tmp_path, marker)


def validate_managed_log_root(log_root: Path) -> None:
    """Validate that ``log_root`` is authorized for cleanup.

    Raises :class:`LogRootAuthorizationError` when ANY of the following hold:

    * The marker file does not exist (cleanup must NOT auto-create it).
    * The marker is a symlink (could point at an attacker-controlled file).
    * The marker JSON is unreadable or not a mapping.
    * ``application`` is missing or != ``QuantDataCenter``.
    * ``layout_version`` is missing or unsupported.
    * The resolved root itself is a dangerous location (drive root, home,
      repo root, repo-internal path, junction, or symlink that escapes).

    This function is called by ``cleanup_logs`` BEFORE any deletion. It is
    strictly fail-closed: any doubt → reject.
    """

    root = Path(log_root).expanduser()
    _reject_unsafe_log_root(root)
    resolved = root.resolve()
    marker = resolved / MANAGED_ROOT_MARKER
    if not marker.exists():
        raise LogRootAuthorizationError(
            f"Refusing to clean log root without a managed-root marker: {resolved}. "
            f"Run 'python -m src.tools.log_cleanup --initialize-managed-root --log-dir {resolved}' "
            f"to authorize this directory first."
        )
    # A symlink marker could be redirected at an attacker-controlled file to
    # bypass the application/version check. Reject it.
    if marker.is_symlink():
        raise LogRootAuthorizationError(
            f"Refusing to clean log root with a symlink marker: {marker} -> {marker.resolve()}"
        )
    try:
        raw = marker.read_text(encoding="utf-8")
        payload: Any = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise LogRootAuthorizationError(f"Managed-root marker is unreadable/corrupt at {marker}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LogRootAuthorizationError(
            f"Managed-root marker must be a JSON object at {marker}; got {type(payload).__name__}"
        )
    application = str(payload.get("application") or "")
    if application != MANAGED_ROOT_APPLICATION:
        raise LogRootAuthorizationError(
            f"Managed-root marker application mismatch at {marker}: "
            f"expected {MANAGED_ROOT_APPLICATION!r}, got {application!r}"
        )
    layout_version = payload.get("layout_version")
    try:
        version_int = int(str(layout_version)) if layout_version is not None else -1
    except (TypeError, ValueError) as exc:
        raise LogRootAuthorizationError(
            f"Managed-root marker layout_version is not an integer at {marker}: {layout_version!r}"
        ) from exc
    if version_int != MANAGED_ROOT_LAYOUT_VERSION:
        raise LogRootAuthorizationError(
            f"Managed-root marker layout_version {version_int} is unsupported at {marker} "
            f"(expected {MANAGED_ROOT_LAYOUT_VERSION})"
        )


def _reject_unsafe_log_root(root: Path) -> None:
    """Reject log roots that are inherently dangerous to clean.

    This is the shared safety check used by both ``ensure_managed_log_root``
    and ``validate_managed_log_root``. It mirrors the checks previously
    scattered in ``log_cleanup._reject_dangerous_root`` but lives here so the
    marker authority and the cleanup authority agree on what is "safe".

    P0-3 contract: ALL root symlinks are rejected (not just those that escape
    the parent). Rationale: the marker binds to a directory identity; a
    symlink target can be swapped after authorization, so the deletion
    boundary must never depend on link resolution. Windows junctions/reparse
    points are rejected for the same reason.
    """

    # ANY symlink at the root — reject unconditionally. The marker binds to a
    # directory identity; a symlink target can be swapped after authorization,
    # so the deletion boundary must not depend on link resolution.
    if root.is_symlink():
        target = root.resolve()
        raise LogRootAuthorizationError(f"Refusing to authorize symlink log root: {root} -> {target}")

    # Windows junction/reparse point — reject.
    is_junction = getattr(root, "is_junction", lambda: False)()
    if is_junction:
        raise LogRootAuthorizationError(f"Refusing to authorize Windows junction/reparse log root: {root}")

    resolved = root.resolve()
    resolved_parent = resolved.parent
    # Filesystem root: parent is itself.
    if resolved == resolved_parent:
        raise LogRootAuthorizationError(f"Refusing to authorize filesystem root: {root}")

    # Bare Windows drive root (e.g. C:\).
    drive = resolved.anchor
    if drive and resolved == Path(drive):
        raise LogRootAuthorizationError(f"Refusing to authorize drive root: {root}")

    # User home directory.
    import pathlib

    home = pathlib.Path.home().resolve()
    if resolved == home:
        raise LogRootAuthorizationError(f"Refusing to authorize user home directory: {root}")

    # Repository root or any path inside the repository. Use ``project_root()``
    # (derived from ``__file__``) rather than the module-level ``ROOT`` so that
    # test monkeypatching of ``ROOT`` does not bypass this safety check.
    repo_root = project_root().resolve()
    if resolved == repo_root or is_path_inside(resolved, repo_root):
        raise LogRootAuthorizationError(f"Refusing to authorize repository or repo-internal path: {root}")


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
                        resolved_candidate if resolved_candidate.is_absolute() else (manager.root / resolved_candidate)
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
