"""YAML configuration loader."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:
    yaml = None

from src.utils import paths


class ConfigError(RuntimeError):
    """Raised when configuration cannot be loaded or interpreted."""


class ConfigManager:
    """Read project YAML files and expose small convenience helpers."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = (root or paths.ROOT).resolve()
        self.config_dir = self.root / "config"

    @cached_property
    def settings(self) -> dict[str, Any]:
        return self._load_yaml(self.config_dir / "settings.yaml")

    @cached_property
    def universe(self) -> dict[str, Any]:
        return self._load_yaml(self.config_dir / "universe.yaml")

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as fh:
            if yaml is not None:
                data = yaml.safe_load(fh) or {}
            else:
                data = _load_yaml_subset(fh.read())
        if not isinstance(data, dict):
            raise ConfigError(f"Config root must be a mapping: {path}")
        return data

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Read a nested setting using dot notation."""

        value: Any = self.settings
        for part in dotted_key.split("."):
            if not isinstance(value, dict) or part not in value:
                return default
            value = value[part]
        return value

    def path(self, dotted_key: str, default: str | Path | None = None) -> Path:
        value = self.get(dotted_key, default)
        if value is None:
            raise ConfigError(f"Missing path setting: {dotted_key}")
        return paths.resolve_path(value, self.root)

    def daily_k_fields(self) -> str:
        fields = self.get("datasets.daily_k.fields")
        if not fields:
            raise ConfigError("Missing datasets.daily_k.fields")
        return str(fields)

    def stock_basic_fields(self) -> str:
        fields = self.get("datasets.stock_basic.fields")
        if not fields:
            raise ConfigError("Missing datasets.stock_basic.fields")
        return str(fields)

    def adjustflag_for_dataset(self, dataset: str) -> str:
        suffix = dataset.replace("daily_k_", "")
        mapping = self.get("api.baostock.adjustflag_map", {})
        try:
            return str(mapping[suffix])
        except KeyError as exc:
            raise ConfigError(f"Unsupported daily_k dataset: {dataset}") from exc

    def universe_codes(self, universe_name: str = "default") -> list[str]:
        raw = self.universe.get("universe", {})
        codes = raw.get(universe_name) if isinstance(raw, dict) else None
        if codes is None and universe_name == "default":
            codes = self.universe.get("codes")
        if not isinstance(codes, list) or not codes:
            raise ConfigError(f"Universe not found or empty: {universe_name}")
        return [str(code) for code in codes]


def load_settings(root: Path | None = None) -> dict[str, Any]:
    return ConfigManager(root).settings


def load_universe(root: Path | None = None) -> dict[str, Any]:
    return ConfigManager(root).universe


def _load_yaml_subset(text: str) -> dict[str, Any]:
    """Parse the small YAML subset used by config/*.yaml when PyYAML is absent."""

    lines: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        lines.append((indent, line.strip()))
    if not lines:
        return {}
    value, index = _parse_yaml_block(lines, 0, lines[0][0])
    if index != len(lines) or not isinstance(value, dict):
        raise ConfigError("Fallback YAML parser could not parse config")
    return value


def _parse_yaml_block(lines: list[tuple[int, str]], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return {}, index
    is_list = lines[index][1].startswith("- ")
    if is_list:
        values: list[Any] = []
        while index < len(lines):
            current_indent, stripped = lines[index]
            if current_indent != indent or not stripped.startswith("- "):
                break
            item = stripped[2:].strip()
            if item:
                values.append(_parse_scalar(item))
                index += 1
            else:
                nested, index = _parse_yaml_block(lines, index + 1, lines[index + 1][0])
                values.append(nested)
        return values, index

    mapping: dict[str, Any] = {}
    while index < len(lines):
        current_indent, stripped = lines[index]
        if current_indent != indent or stripped.startswith("- "):
            break
        key, separator, rest = stripped.partition(":")
        if not separator:
            raise ConfigError(f"Invalid YAML line: {stripped}")
        rest = rest.strip()
        if rest:
            mapping[key.strip()] = _parse_scalar(rest)
            index += 1
        else:
            if index + 1 >= len(lines) or lines[index + 1][0] <= current_indent:
                mapping[key.strip()] = {}
                index += 1
            else:
                nested, index = _parse_yaml_block(lines, index + 1, lines[index + 1][0])
                mapping[key.strip()] = nested
    return mapping, index


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        return value
