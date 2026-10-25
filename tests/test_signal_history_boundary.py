"""Dated-history boundary before the existing intraday quote overlay."""
from datetime import datetime, timedelta, timezone

import pytest

from application.runtime_broker_adapters import SchwabRuntimeBrokerAdapters
from quant_platform_kit.common.models import QuoteSnapshot

NOW = datetime(2026, 3, 31, 19, 45, tzinfo=timezone.utc)


def candle(day, price):
    return {"datetime": int(day.timestamp() * 1000), "close": price}


def port(candles):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("NO_ACCOUNT_OR_ORDER_CALL")
    adapters = SchwabRuntimeBrokerAdapters(
        managed_symbols=("SOXL",), fetch_account_snapshot_fn=forbidden,
        fetch_quotes_fn=lambda *_a, **_k: {"SOXL": QuoteSnapshot("SOXL", NOW, 42.)},
        fetch_daily_price_history_fn=lambda *_a, **_k: candles,
        submit_equity_order_fn=forbidden, clock=lambda: NOW)
    return adapters.build_market_data_port(None)


def test_signal_history_orders_and_clips_before_current_quote_overlay():
    day = NOW.replace(hour=4, minute=0)
    old, current = candle(day - timedelta(days=1), 30.), candle(day, 31.)
    future = candle(day + timedelta(days=1), 9999.)
    known = port([old, current]).get_price_series("SOXL")
    mixed = port([current, future, old]).get_price_series("SOXL")
    assert mixed.points == known.points
    assert mixed.points[-1].close == 42.


def test_future_only_signal_history_is_rejected_before_quote_can_fund_history():
    with pytest.raises(ValueError, match="Price history unavailable at observation"):
        port([candle(NOW + timedelta(days=1), 9999.)]).get_price_series("SOXL")


def test_same_session_intraday_quote_overlay_remains_available():
    day = NOW.replace(hour=4, minute=0)
    series = port([candle(day - timedelta(days=1), 30.), candle(day, 31.)]).get_price_series("SOXL")
    assert [p.close for p in series.points] == [30., 42.]
    assert series.points[-1].as_of == NOW
