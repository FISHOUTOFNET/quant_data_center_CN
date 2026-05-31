"""YAML configuration loader."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import Any

import yaml

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

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
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

    def daily_bar_fields(self) -> str:
        fields = self.get("datasets.daily_bar.fields")
        if not fields:
            raise ConfigError("Missing datasets.daily_bar.fields")
        return str(fields)

    def baostock_cn_stock_basic_fields(self) -> str:
        fields = self.get("datasets.baostock_cn_stock_basic.fields")
        if not fields:
            raise ConfigError("Missing datasets.baostock_cn_stock_basic.fields")
        return str(fields)

    def adjust_flag_for_dataset(self, dataset: str) -> str:
        prefix = "baostock_cn_stock_daily_bar_"
        if not dataset.startswith(prefix):
            raise ConfigError(f"Unsupported daily_bar dataset: {dataset}")
        suffix = dataset.removeprefix(prefix)
        mapping = self.get("api.baostock.adjust_flag_map", {})
        try:
            return str(mapping[suffix])
        except KeyError as exc:
            raise ConfigError(f"Unsupported daily_bar dataset: {dataset}") from exc


def load_settings(root: Path | None = None) -> dict[str, Any]:
    return ConfigManager(root).settings
