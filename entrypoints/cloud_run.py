"""Cloud Run request helpers for CharlesSchwabPlatform."""

from __future__ import annotations

from datetime import datetime

import pandas_market_calendars as mcal
import pytz


def is_market_open_now(*, calendar_name="NASDAQ", timezone_name="America/New_York"):
    """Return whether the US equity regular session is open right now."""
    try:
        calendar = mcal.get_calendar(calendar_name)
        market_tz = pytz.timezone(timezone_name)
        now_market = datetime.now(market_tz)
        schedule = calendar.schedule(start_date=now_market.date(), end_date=now_market.date())
        if schedule.empty:
            return False, None
        return calendar.open_at_time(schedule, now_market), None
    except Exception as exc:
        return False, exc


def is_market_open_today(*, calendar_name="NASDAQ", timezone_name="America/New_York") -> bool:
    """Return whether today is a US equity trading session (legacy helper)."""
    tz_ny = pytz.timezone(timezone_name)
    now_ny = datetime.now(tz_ny)
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=now_ny.date(), end_date=now_ny.date())
    return not schedule.empty
