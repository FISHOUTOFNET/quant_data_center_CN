"""Baostock market-session scheduling helpers."""

from __future__ import annotations

from datetime import date


def should_run_adjusted_market_session(
    natural_date: date,
    candidate_date: date,
    market_date: date,
    market_date_overridden: bool = False,
) -> bool:
    """Return whether the daily market-session should include adjusted bars."""

    return (
        natural_date.weekday() in {4, 5, 6}
        or candidate_date != market_date
        or market_date_overridden
    )
