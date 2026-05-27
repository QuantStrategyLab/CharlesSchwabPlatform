from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from application.runtime_broker_adapters import build_runtime_broker_adapters


def _candle(ts: datetime, close: float) -> dict[str, float]:
    return {"datetime": int(ts.timestamp() * 1000), "close": close}


def test_price_history_appends_current_market_quote_when_daily_history_is_previous_day():
    adapters = build_runtime_broker_adapters(
        managed_symbols=("SOXL", "SOXX"),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=lambda _client, symbols: {
            symbol: SimpleNamespace(last_price=112.0, bid_price=111.9, ask_price=112.1)
            for symbol in symbols
        },
        fetch_daily_price_history_fn=lambda _client, _symbol: [
            _candle(datetime(2026, 5, 25, 4, tzinfo=timezone.utc), 100.0),
            _candle(datetime(2026, 5, 26, 4, tzinfo=timezone.utc), 105.0),
        ],
        submit_equity_order_fn=None,
        clock=lambda: datetime(2026, 5, 27, 19, 45, tzinfo=timezone.utc),
    )

    history = adapters.build_price_history(adapters.build_market_data_port(object()), "SOXX")

    assert [point["close"] for point in history] == [100.0, 105.0, 112.0]


def test_price_history_replaces_current_market_day_candle_with_current_quote():
    adapters = build_runtime_broker_adapters(
        managed_symbols=("SOXL", "SOXX"),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=lambda _client, symbols: {
            symbol: SimpleNamespace(last_price=112.0, bid_price=111.9, ask_price=112.1)
            for symbol in symbols
        },
        fetch_daily_price_history_fn=lambda _client, _symbol: [
            _candle(datetime(2026, 5, 26, 4, tzinfo=timezone.utc), 105.0),
            _candle(datetime(2026, 5, 27, 4, tzinfo=timezone.utc), 110.0),
        ],
        submit_equity_order_fn=None,
        clock=lambda: datetime(2026, 5, 27, 19, 45, tzinfo=timezone.utc),
    )

    history = adapters.build_price_history(adapters.build_market_data_port(object()), "SOXX")

    assert [point["close"] for point in history] == [105.0, 112.0]


def test_price_history_falls_back_to_daily_history_when_quote_is_unavailable():
    def fail_quotes(_client, _symbols):
        raise RuntimeError("quotes unavailable")

    adapters = build_runtime_broker_adapters(
        managed_symbols=("SOXL", "SOXX"),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=fail_quotes,
        fetch_daily_price_history_fn=lambda _client, _symbol: [
            _candle(datetime(2026, 5, 26, 4, tzinfo=timezone.utc), 105.0),
        ],
        submit_equity_order_fn=None,
        clock=lambda: datetime(2026, 5, 27, 19, 45, tzinfo=timezone.utc),
    )

    history = adapters.build_price_history(adapters.build_market_data_port(object()), "SOXX")

    assert [point["close"] for point in history] == [105.0]
