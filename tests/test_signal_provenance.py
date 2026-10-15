from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from application import signal_provenance
from application.runtime_broker_adapters import build_runtime_broker_adapters


@pytest.fixture(autouse=True)
def _reset():
    signal_provenance.reset()
    yield
    signal_provenance.reset()


def _candle(ts: datetime, close: float) -> dict[str, float]:
    return {"datetime": int(ts.timestamp() * 1000), "close": close}


def _adapters():
    return build_runtime_broker_adapters(
        managed_symbols=("SOXL", "SOXX"),
        fetch_account_snapshot_fn=None,
        fetch_quotes_fn=lambda _client, symbols: {
            symbol: SimpleNamespace(last_price=112.0, bid_price=111.9, ask_price=112.1) for symbol in symbols
        },
        fetch_daily_price_history_fn=lambda _client, _symbol: [
            _candle(datetime(2026, 5, 25, 4, tzinfo=timezone.utc), 100.0),
            _candle(datetime(2026, 5, 26, 4, tzinfo=timezone.utc), 105.0),
        ],
        submit_equity_order_fn=None,
        clock=lambda: datetime(2026, 5, 27, 19, 45, tzinfo=timezone.utc),
    )


def test_price_history_records_provenance_without_changing_history():
    adapters = _adapters()
    history = adapters.build_price_history(adapters.build_market_data_port(object()), "SOXX")
    assert [point["close"] for point in history] == [100.0, 105.0, 112.0]
    entry = signal_provenance.snapshot()["symbols"]["SOXX"]
    assert entry["source"] == "schwab_price_history"
    assert entry["price_basis"] == "split_only"
    assert entry["rows"] == 3
    assert entry["first_date"] == "2026-05-25"
    assert entry["last_date"] == "2026-05-27"
    assert set(entry) == {"source", "price_basis", "rows", "first_date", "last_date", "note", "fetch_count"}


def test_market_history_loader_records_provenance():
    adapters = _adapters()
    loader = adapters.build_market_history_loader(adapters.build_market_data_port(object()))
    series = loader(None, "soxl")
    assert list(series.values) == [100.0, 105.0, 112.0]
    assert signal_provenance.snapshot()["symbols"]["SOXL"]["rows"] == 3


def test_recorder_never_raises_and_resets():
    signal_provenance.record("X", source="s", price_basis="bogus", dates=object())
    assert signal_provenance.snapshot() == {}
    signal_provenance.record("X", source="s", price_basis="bogus", dates=[datetime(2026, 1, 2)])
    assert signal_provenance.snapshot()["symbols"]["X"]["price_basis"] == "unknown"


def test_main_attaches_provenance():
    from test_request_handling import load_module

    module = load_module()
    signal_provenance.record("SOXL", source="schwab_price_history", price_basis="split_only",
                             dates=[datetime(2026, 10, 9)])
    report = {"diagnostics": {"runtime_revision": "r"}}
    module.attach_signal_data_provenance(report)
    assert report["diagnostics"]["runtime_revision"] == "r"
    assert report["diagnostics"]["signal_data_provenance"]["symbols"]["SOXL"]["rows"] == 1
    signal_provenance.reset()
    empty: dict = {}
    module.attach_signal_data_provenance(empty)
    assert empty == {}
