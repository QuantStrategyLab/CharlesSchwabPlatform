"""Cloud Run request helpers for CharlesSchwabPlatform."""

from __future__ import annotations

import pandas_market_calendars as mcal
from quant_platform_kit.markets import (
    is_market_open_now as _qpk_is_market_open_now,
    is_market_open_today as _qpk_is_market_open_today,
)


def is_market_open_now(*, calendar_name="NASDAQ", timezone_name="America/New_York"):
    """Return whether the US equity regular session is open right now."""
    return _qpk_is_market_open_now(
        calendar_name=calendar_name,
        timezone_name=timezone_name,
        calendar_factory=mcal.get_calendar,
    )


def is_market_open_today(*, calendar_name="NASDAQ", timezone_name="America/New_York") -> bool:
    """Return whether today is a US equity trading session (legacy helper)."""
    return _qpk_is_market_open_today(
        calendar_name=calendar_name,
        timezone_name=timezone_name,
        calendar_factory=mcal.get_calendar,
    )
