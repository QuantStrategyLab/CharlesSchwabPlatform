from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from application import runtime_broker_adapters as broker_adapters_module
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


def test_market_data_port_batches_managed_quotes_and_caches_results():
    observed_calls = []

    adapters = build_runtime_broker_adapters(
        managed_symbols=("SOXL", "SOXX", "BOXX"),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=lambda _client, symbols: (
            observed_calls.append(tuple(symbols)),
            {
                symbol: SimpleNamespace(
                    last_price=float(index + 1),
                    bid_price=None,
                    ask_price=None,
                )
                for index, symbol in enumerate(symbols)
            },
        )[-1],
        fetch_daily_price_history_fn=lambda _client, _symbol: [],
        submit_equity_order_fn=None,
        clock=lambda: datetime(2026, 5, 27, 19, 45, tzinfo=timezone.utc),
    )

    market_data_port = adapters.build_market_data_port(object())
    quote_a = market_data_port.get_quote("SOXL")
    quote_b = market_data_port.get_quote("BOXX")

    assert quote_a.symbol == "SOXL"
    assert quote_b.symbol == "BOXX"
    assert observed_calls == [("SOXL", "SOXX", "BOXX")]


def test_market_data_port_retries_rate_limited_quote_batch(monkeypatch):
    observed_calls = []
    sleeps = []

    def fetch_quotes(_client, symbols):
        observed_calls.append(tuple(symbols))
        if len(observed_calls) == 1:
            raise RuntimeError("Quotes failed: 429")
        return {
            symbol: SimpleNamespace(last_price=10.0, bid_price=None, ask_price=None)
            for symbol in symbols
        }

    monkeypatch.setattr(
        broker_adapters_module.time,
        "sleep",
        lambda seconds: sleeps.append(seconds),
    )
    adapters = build_runtime_broker_adapters(
        managed_symbols=("SOXL", "SOXX"),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=fetch_quotes,
        fetch_daily_price_history_fn=lambda _client, _symbol: [],
        submit_equity_order_fn=None,
        clock=lambda: datetime(2026, 5, 27, 19, 45, tzinfo=timezone.utc),
    )

    quote = adapters.build_market_data_port(object()).get_quote("SOXX")

    assert quote.last_price == 10.0
    assert observed_calls == [("SOXL", "SOXX"), ("SOXL", "SOXX")]
    assert sleeps == [0.5]


def test_build_order_status_fetcher_curries_client_and_account_hash():
    observed = {}

    def fetch_order_status(client, account_hash, order_id):
        observed["call"] = (client, account_hash, order_id)
        return {"status": "FILLED", "executed_qty": 1.0, "executed_price": 100.0}

    adapters = build_runtime_broker_adapters(
        managed_symbols=("SOXL",),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=lambda _client, _symbols: {},
        fetch_daily_price_history_fn=lambda _client, _symbol: [],
        submit_equity_order_fn=None,
        fetch_order_status_fn=fetch_order_status,
    )

    fetcher = adapters.build_order_status_fetcher("client-1", "acct-1")

    assert fetcher is not None
    fetcher("order-1")
    assert observed["call"] == ("client-1", "acct-1", "order-1")
