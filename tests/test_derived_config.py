"""P0-9: Derived runtime configuration tests.

Verifies that ``settings.yaml`` ``derived.*`` keys are actually wired into
the runtime (not dead config) and that the resolution priority is:

    CLI explicit override  >  settings.yaml  >  code default

Sections covered (spec §14.7):

* YAML max_workers takes effect
* CLI override overrides YAML max_workers
* in-flight multiplier takes effect
* heartbeat takes effect
* stall threshold takes effect
* invalid config fails fast (out of range, stall < heartbeat * 2)
* missing settings.yaml falls back to code defaults
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.sources.derived.config import (
    DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
    DEFAULT_MAX_WORKERS,
    DEFAULT_STALL_SECONDS,
    MAX_WORKERS_RANGE,
    STALL_HEARTBEAT_MIN_RATIO,
    DerivedConfigError,
    DerivedRuntimeConfig,
    load_derived_runtime_config,
    load_derived_runtime_config_or_default,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_settings(root: Path, derived_block: str) -> None:
    """Write a ``config/settings.yaml`` with the given ``derived:`` block."""

    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text(
        "project:\n  name: test\n" + derived_block,
        encoding="utf-8",
    )


_VALID_BLOCK = """\
derived:
  max_workers: 3
  max_in_flight_multiplier: 2
  heartbeat_seconds: 20
  stall_seconds: 1200
"""


# ---------------------------------------------------------------------------
# 14.7a: YAML max_workers takes effect
# ---------------------------------------------------------------------------


def test_yaml_max_workers_takes_effect(tmp_path: Path) -> None:
    _write_settings(tmp_path, _VALID_BLOCK)
    config = load_derived_runtime_config(root=tmp_path)
    assert config.max_workers == 3
    assert config.max_in_flight_multiplier == 2
    assert config.heartbeat_seconds == 20.0
    assert config.stall_seconds == 1200


# ---------------------------------------------------------------------------
# 14.7b: CLI override overrides YAML max_workers
# ---------------------------------------------------------------------------


def test_cli_override_overrides_yaml_max_workers(tmp_path: Path) -> None:
    _write_settings(tmp_path, _VALID_BLOCK)
    config = load_derived_runtime_config(root=tmp_path, max_workers_override=6)
    assert config.max_workers == 6
    # Other values still come from YAML.
    assert config.max_in_flight_multiplier == 2
    assert config.heartbeat_seconds == 20.0


def test_cli_override_overrides_yaml_even_when_yaml_absent(tmp_path: Path) -> None:
    """CLI override applies even when settings.yaml has no derived section."""

    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text(
        "project:\n  name: test\n", encoding="utf-8"
    )
    config = load_derived_runtime_config(root=tmp_path, max_workers_override=7)
    assert config.max_workers == 7


# ---------------------------------------------------------------------------
# 14.7c: in-flight multiplier takes effect
# ---------------------------------------------------------------------------


def test_in_flight_multiplier_takes_effect(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 2\n  max_in_flight_multiplier: 4\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 1800\n",
    )
    config = load_derived_runtime_config(root=tmp_path)
    assert config.max_in_flight_multiplier == 4
    config.validate()  # 4 is within [1, 4]


# ---------------------------------------------------------------------------
# 14.7d: heartbeat takes effect
# ---------------------------------------------------------------------------


def test_heartbeat_takes_effect(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 2\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 15\n  stall_seconds: 1800\n",
    )
    config = load_derived_runtime_config(root=tmp_path)
    assert config.heartbeat_seconds == 15.0


# ---------------------------------------------------------------------------
# 14.7e: stall threshold takes effect
# ---------------------------------------------------------------------------


def test_stall_threshold_takes_effect(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 2\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 900\n",
    )
    config = load_derived_runtime_config(root=tmp_path)
    assert config.stall_seconds == 900
    config.validate()  # 900 >= 30 * 2 = 60


# ---------------------------------------------------------------------------
# 14.7f: invalid config fails fast
# ---------------------------------------------------------------------------


def test_invalid_max_workers_too_low_fails_fast(tmp_path: Path) -> None:
    lo, _ = MAX_WORKERS_RANGE
    _write_settings(
        tmp_path,
        f"derived:\n  max_workers: {lo - 1}\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 1800\n",
    )
    with pytest.raises(DerivedConfigError, match="max_workers"):
        load_derived_runtime_config(root=tmp_path)


def test_invalid_max_workers_too_high_fails_fast(tmp_path: Path) -> None:
    _, hi = MAX_WORKERS_RANGE
    _write_settings(
        tmp_path,
        f"derived:\n  max_workers: {hi + 1}\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 1800\n",
    )
    with pytest.raises(DerivedConfigError, match="max_workers"):
        load_derived_runtime_config(root=tmp_path)


def test_invalid_in_flight_multiplier_fails_fast(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 2\n  max_in_flight_multiplier: 5\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 1800\n",
    )
    with pytest.raises(DerivedConfigError, match="max_in_flight_multiplier"):
        load_derived_runtime_config(root=tmp_path)


def test_invalid_heartbeat_too_low_fails_fast(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 2\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 1\n  stall_seconds: 1800\n",
    )
    with pytest.raises(DerivedConfigError, match="heartbeat_seconds"):
        load_derived_runtime_config(root=tmp_path)


def test_stall_below_heartbeat_ratio_fails_fast(tmp_path: Path) -> None:
    """stall_seconds must be >= heartbeat_seconds * STALL_HEARTBEAT_MIN_RATIO."""

    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 2\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 45\n",  # 45 < 30 * 2 = 60
    )
    with pytest.raises(DerivedConfigError, match="stall_seconds"):
        load_derived_runtime_config(root=tmp_path)


def test_non_mapping_derived_section_fails_fast(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text(
        "project:\n  name: test\nderived: not-a-mapping\n",
        encoding="utf-8",
    )
    with pytest.raises(DerivedConfigError, match="mapping"):
        load_derived_runtime_config(root=tmp_path)


# ---------------------------------------------------------------------------
# 14.7g: missing settings.yaml falls back to code defaults
# ---------------------------------------------------------------------------


def test_missing_settings_yaml_falls_back_to_defaults(tmp_path: Path) -> None:
    """When settings.yaml is absent, code defaults are used (no error)."""

    # No config dir / settings.yaml created.
    config = load_derived_runtime_config(root=tmp_path)
    assert config.max_workers == DEFAULT_MAX_WORKERS
    assert config.max_in_flight_multiplier == DEFAULT_MAX_IN_FLIGHT_MULTIPLIER
    assert config.heartbeat_seconds == DEFAULT_HEARTBEAT_SECONDS
    assert config.stall_seconds == DEFAULT_STALL_SECONDS


def test_missing_derived_section_falls_back_to_defaults(tmp_path: Path) -> None:
    """When settings.yaml exists but has no derived section, defaults apply."""

    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text(
        "project:\n  name: test\n", encoding="utf-8"
    )
    config = load_derived_runtime_config(root=tmp_path)
    assert config.max_workers == DEFAULT_MAX_WORKERS


# ---------------------------------------------------------------------------
# 14.7h: load_derived_runtime_config_or_default fallback
# ---------------------------------------------------------------------------


def test_load_or_default_falls_back_on_invalid_config(tmp_path: Path) -> None:
    """On invalid config, the *_or_default variant falls back to defaults
    instead of raising (used by the orchestrator stall detector which must
    keep running even with a broken settings.yaml)."""

    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 99\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 1800\n",
    )
    config = load_derived_runtime_config_or_default(root=tmp_path)
    # Invalid max_workers=99 → fallback to default 4.
    assert config.max_workers == DEFAULT_MAX_WORKERS
    assert config.max_in_flight_multiplier == DEFAULT_MAX_IN_FLIGHT_MULTIPLIER


def test_load_or_default_preserves_cli_override_on_invalid_config(
    tmp_path: Path,
) -> None:
    """Even when falling back, an explicit CLI override is honored."""

    _write_settings(
        tmp_path,
        "derived:\n  max_workers: 99\n  max_in_flight_multiplier: 2\n"
        "  heartbeat_seconds: 30\n  stall_seconds: 1800\n",
    )
    config = load_derived_runtime_config_or_default(
        root=tmp_path, max_workers_override=5
    )
    assert config.max_workers == 5


# ---------------------------------------------------------------------------
# 14.7i: immutability and validate() semantics
# ---------------------------------------------------------------------------


def test_config_is_frozen() -> None:
    config = DerivedRuntimeConfig(
        max_workers=2,
        max_in_flight_multiplier=2,
        heartbeat_seconds=30.0,
        stall_seconds=1800,
    )
    with pytest.raises(Exception):
        config.max_workers = 4  # type: ignore[misc]


def test_validate_passes_for_default_values() -> None:
    config = DerivedRuntimeConfig(
        max_workers=DEFAULT_MAX_WORKERS,
        max_in_flight_multiplier=DEFAULT_MAX_IN_FLIGHT_MULTIPLIER,
        heartbeat_seconds=DEFAULT_HEARTBEAT_SECONDS,
        stall_seconds=DEFAULT_STALL_SECONDS,
    )
    # Must not raise.
    config.validate()


def test_boundary_stall_equals_ratio_is_valid() -> None:
    """stall_seconds == heartbeat * ratio is valid (>= is inclusive)."""

    heartbeat = 30.0
    stall = int(heartbeat * STALL_HEARTBEAT_MIN_RATIO)
    config = DerivedRuntimeConfig(
        max_workers=2,
        max_in_flight_multiplier=2,
        heartbeat_seconds=heartbeat,
        stall_seconds=stall,
    )
    config.validate()  # Must not raise.
