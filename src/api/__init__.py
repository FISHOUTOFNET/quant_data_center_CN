"""Data source API clients and provider-neutral interfaces."""

from src.api.akshare_client import AkShareClient
from src.api.market_data import (
    DailyKRequest,
    MarketDataProvider,
    create_provider,
    register_provider,
    registered_provider_names,
)

__all__ = [
    "AkShareClient",
    "DailyKRequest",
    "MarketDataProvider",
    "create_provider",
    "register_provider",
    "registered_provider_names",
]
