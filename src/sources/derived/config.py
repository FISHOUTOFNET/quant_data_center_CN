"""Derived build runtime configuration loaded from ``settings.yaml``.

This module is the single source of truth for the user-facing tunables that
control the derived build pipeline:

* ``max_workers`` — thread pool size for the partition executor (1–8).
* ``max_in_flight_multiplier`` — bounded sliding-window cap multiplier (1–4).
* ``heartbeat_seconds`` — progress/journal heartbeat interval (5–300).
* ``stall_seconds`` — heartbeat age threshold before the orchestrator declares
  the build stalled (must be > ``heartbeat_seconds`` × a reasonable factor).

Resolution priority (highest first):

1. CLI explicit override (``--max-workers``) — only ``max_workers`` is exposed
   on the CLI to avoid configuration-surface bloat.
2. ``settings.yaml`` (``derived.*`` keys).
3. Code defaults (:data:`DEFAULTS`).

Configuration loading is fail-fast: an out-of-range or inconsistent value
raises :class:`DerivedConfigError` so the build aborts with a clear message
instead of silently degrading. Callers that want a non-fatal fallback (e.g.
the orchestrator's stall detector, which must keep running even when
``settings.yaml`` is missing in a test fixture) can catch the exception.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.utils import paths
from src.utils.config_mgr import ConfigError, ConfigManager
from src.utils.logging import logger

# Code defaults — the last-resort fallback when settings.yaml is absent or
# the ``derived`` section is missing. These intentionally match the values
# documented in ``config/settings.yaml`` so production behaviour is stable
# even when the config file is not present (e.g. fresh checkout, tests).
DEFAULT_MAX_WORKERS = 4
DEFAULT_MAX_IN_FLIGHT_MULTIPLIER = 2
DEFAULT_HEARTBEAT_SECONDS = 30.0
DEFAULT_STALL_SECONDS = 1800  # 30 minutes; > 25 min default heartbeat threshold.

# Validation ranges. ``stall_seconds`` must be at least
# ``heartbeat_seconds * STALL_HEARTBEAT_RATIO`` so a single missed heartbeat
# cannot immediately declare the build stalled.
MAX_WORKERS_RANGE = (1, 8)
MAX_IN_FLIGHT_MULTIPLIER_RANGE = (1, 4)
HEARTBEAT_SECONDS_RANGE = (5.0, 300.0)
STALL_HEARTBEAT_MIN_RATIO = 2.0  # stall >= 2x heartbeat


class DerivedConfigError(ValueError):
    """Raised when derived configuration is invalid or inconsistent."""


@dataclass(frozen=True)
class DerivedRuntimeConfig:
    """Immutable derived build runtime configuration.

    All values are validated at construction time. The orchestrator, the
    streaming coordinator, and ``build_cn_stock_daily_bar`` all read from a
    single instance so there is exactly one set of values in flight — no
    "daily-bar 30 min, orchestrator 25 min, YAML 30 min" divergence.
    """

    max_workers: int
    max_in_flight_multiplier: int
    heartbeat_seconds: float
    stall_seconds: int

    @classmethod
    def with_overrides(
        cls,
        *,
        max_workers_override: int | None = None,
        root: Path | None = None,
    ) -> DerivedRuntimeConfig:
        """Load from settings.yaml, applying explicit CLI overrides.

        Priority: ``max_workers_override`` (CLI) > settings.yaml > code default.
        Only ``max_workers`` is overridable via CLI; the other three values
        always come from settings.yaml (or code default) to prevent
        configuration-surface bloat.
        """

        loaded = _load_raw_config(root=root)
        effective_max_workers = max_workers_override or loaded.get(
            "max_workers", DEFAULT_MAX_WORKERS
        )
        effective_multiplier = loaded.get(
            "max_in_flight_multiplier", DEFAULT_MAX_IN_FLIGHT_MULTIPLIER
        )
        effective_heartbeat = loaded.get(
            "heartbeat_seconds", DEFAULT_HEARTBEAT_SECONDS
        )
        effective_stall = loaded.get("stall_seconds", DEFAULT_STALL_SECONDS)
        return cls(
            max_workers=int(effective_max_workers),
            max_in_flight_multiplier=int(effective_multiplier),
            heartbeat_seconds=float(effective_heartbeat),
            stall_seconds=int(effective_stall),
        )

    def validate(self) -> None:
        """Raise :class:`DerivedConfigError` if any value is out of range."""

        lo, hi = MAX_WORKERS_RANGE
        if not (lo <= self.max_workers <= hi):
            raise DerivedConfigError(
                f"derived.max_workers={self.max_workers} out of range [{lo}, {hi}]"
            )
        lo, hi = MAX_IN_FLIGHT_MULTIPLIER_RANGE
        if not (lo <= self.max_in_flight_multiplier <= hi):
            raise DerivedConfigError(
                f"derived.max_in_flight_multiplier={self.max_in_flight_multiplier} "
                f"out of range [{lo}, {hi}]"
            )
        lo, hi = HEARTBEAT_SECONDS_RANGE
        if not (lo <= self.heartbeat_seconds <= hi):
            raise DerivedConfigError(
                f"derived.heartbeat_seconds={self.heartbeat_seconds} out of range [{lo}, {hi}]"
            )
        if self.stall_seconds < int(self.heartbeat_seconds * STALL_HEARTBEAT_MIN_RATIO):
            raise DerivedConfigError(
                f"derived.stall_seconds={self.stall_seconds} must be at least "
                f"heartbeat_seconds({self.heartbeat_seconds}) * "
                f"{STALL_HEARTBEAT_MIN_RATIO} = "
                f"{int(self.heartbeat_seconds * STALL_HEARTBEAT_MIN_RATIO)}"
            )


def load_derived_runtime_config(
    *,
    root: Path | None = None,
    max_workers_override: int | None = None,
) -> DerivedRuntimeConfig:
    """Load and validate the derived runtime config.

    Fail-fast: invalid values raise :class:`DerivedConfigError`. Callers that
    need a non-fatal fallback (e.g. the orchestrator's stall detector, which
    must keep running even when settings.yaml is missing in a test fixture)
    should catch the exception and use :data:`DEFAULTS` directly.
    """

    config = DerivedRuntimeConfig.with_overrides(
        root=root, max_workers_override=max_workers_override
    )
    config.validate()
    return config


def load_derived_runtime_config_or_default(
    *,
    root: Path | None = None,
    max_workers_override: int | None = None,
) -> DerivedRuntimeConfig:
    """Like :func:`load_derived_runtime_config` but falls back to defaults.

    Used by the orchestrator's stall detector, which must keep running even
    when settings.yaml is missing or invalid (e.g. in tests). The fallback is
    logged so the operator can see why the default was used.
    """

    try:
        return load_derived_runtime_config(
            root=root, max_workers_override=max_workers_override
        )
    except (DerivedConfigError, ConfigError, OSError, ValueError) as exc:
        logger.warning(
            "Derived config load failed; falling back to code defaults: {}", exc
        )
        fallback = DerivedRuntimeConfig(
            max_workers=max_workers_override or DEFAULT_MAX_WORKERS,
            max_in_flight_multiplier=DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
            heartbeat_seconds=DEFAULT_HEARTBEAT_SECONDS,
            stall_seconds=DEFAULT_STALL_SECONDS,
        )
        return fallback


def _load_raw_config(*, root: Path | None = None) -> dict[str, object]:
    """Read the ``derived`` section from settings.yaml as a plain dict.

    Returns an empty dict when settings.yaml is absent (e.g. fresh test
    fixture) so callers fall through to code defaults. A present-but-invalid
    ``derived`` section raises :class:`DerivedConfigError`.
    """

    base_root = (root or paths.ROOT).resolve()
    try:
        manager = ConfigManager(base_root)
    except ConfigError:
        return {}
    if not (manager.config_dir / "settings.yaml").exists():
        return {}
    raw = manager.get("derived", {}) or {}
    if not isinstance(raw, dict):
        raise DerivedConfigError(
            f"settings.yaml: 'derived' section must be a mapping, got {type(raw).__name__}"
        )
    return dict(raw)


__all__ = [
    "DEFAULT_HEARTBEAT_SECONDS",
    "DEFAULT_MAX_IN_FLIGHT_MULTIPLIER",
    "DEFAULT_MAX_WORKERS",
    "DEFAULT_STALL_SECONDS",
    "DerivedConfigError",
    "DerivedRuntimeConfig",
    "load_derived_runtime_config",
    "load_derived_runtime_config_or_default",
]
