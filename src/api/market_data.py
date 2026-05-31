"""Provider-neutral market data interfaces and registry."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import pandas as pd

from src.utils.config_mgr import ConfigError, ConfigManager


@dataclass(frozen=True)
class DailyBarRequest:
    """Provider-neutral request for one daily bar dataset/code/date window."""

    dataset: str
    code: str
    start_date: str
    end_date: str
    fields: str
    frequency: str


class MarketDataProvider(Protocol):
    """Context-managed source of normalized market data frames."""

    name: str

    def __enter__(self) -> MarketDataProvider: ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None: ...

    def query_trade_dates(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame: ...

    def query_baostock_cn_stock_basic(
        self,
        code: str | None = None,
        code_name: str | None = None,
    ) -> pd.DataFrame: ...

    def query_daily_bars(self, request: DailyBarRequest) -> pd.DataFrame: ...

    def query_baostock_cn_stock_adjustment_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame: ...


ProviderFactory = Callable[[ConfigManager], MarketDataProvider]

_PROVIDER_FACTORIES: dict[str, ProviderFactory] = {}
_BUILTINS_REGISTERED = False


def register_provider(name: str, factory: ProviderFactory) -> None:
    """Register a provider factory by a stable lowercase name."""

    normalized = _normalize_provider_name(name)
    if not normalized:
        raise ConfigError("Provider name cannot be empty")
    _PROVIDER_FACTORIES[normalized] = factory


def registered_provider_names() -> tuple[str, ...]:
    _ensure_builtin_providers()
    return tuple(sorted(_PROVIDER_FACTORIES))


def create_provider(config: ConfigManager, provider: str | None = None) -> MarketDataProvider:
    """Create the configured provider, defaulting to api.provider/baostock."""

    _ensure_builtin_providers()
    provider_name = _normalize_provider_name(provider or str(config.get("api.provider", "baostock")))
    try:
        factory = _PROVIDER_FACTORIES[provider_name]
    except KeyError as exc:
        available = ", ".join(registered_provider_names()) or "<none>"
        raise ConfigError(f"Unknown data provider: {provider_name}. Available providers: {available}") from exc
    return factory(config)


def _ensure_builtin_providers() -> None:
    global _BUILTINS_REGISTERED
    if _BUILTINS_REGISTERED:
        return
    from src.api.baostock_provider import BaostockProvider

    register_provider(BaostockProvider.name, lambda config: BaostockProvider(config))
    _BUILTINS_REGISTERED = True


def _normalize_provider_name(name: str) -> str:
    return str(name).strip().lower()
