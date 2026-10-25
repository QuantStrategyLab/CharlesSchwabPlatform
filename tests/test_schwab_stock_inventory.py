"""Synthetic current holdings from the read-only native account endpoint."""
from copy import deepcopy
from unittest.mock import Mock

import pytest

from application.stock_inventory import collect_stock_inventory, project_stock_inventory


def payload(positions=None):
    return {"securitiesAccount": {"positions": [] if positions is None else positions}}


def stock(**changes):
    return {"instrument": {"assetType": "EQUITY", "symbol": "OTHER"},
            "longQuantity": "2", "shortQuantity": "0", "marketValue": "53.21", **changes}


def test_unfiltered_stock_quantities_short_and_optional_amounts():
    raw = payload([stock(), stock(instrument={"assetType": "EQUITY", "symbol": "SHORT"},
                                 longQuantity="0", shortQuantity="3", marketValue="-75"),
                   stock(instrument={"assetType": "EQUITY", "symbol": "NOQUOTE"}, marketValue=None),
                   {"instrument": {"assetType": "OPTION", "symbol": "OPTION-EXAMPLE"}}])
    original = deepcopy(raw)
    rows = project_stock_inventory(raw, currency="USD")
    assert raw == original
    assert [r["symbol"] for r in rows] == ["NOQUOTE", "OTHER", "SHORT"]
    assert rows[0]["market_value"] is None
    assert rows[1]["quantity"] == "2" and rows[2]["quantity"] == "-3"
    assert all(r["currency"] == "USD" and r["currency_source"] == "owner_confirmed" for r in rows)
    assert all("avg_cost" not in r for r in rows)


def test_explicit_empty_stocks_is_known_empty_but_missing_is_unknown():
    assert project_stock_inventory(payload(), currency="USD") == []
    assert project_stock_inventory(payload([{"instrument": {"assetType": "OPTION"}}]), currency="USD") == []
    assert project_stock_inventory({"securitiesAccount": {}}, currency="USD") is None
    assert project_stock_inventory(payload(), currency=None) is None


@pytest.mark.parametrize("rows", [None, {}, [stock(longQuantity=True)],
    [stock(longQuantity="NaN")], [stock(shortQuantity="-1")],
    [stock(longQuantity="1.000000001")], [stock(), stock()],
    [{"instrument": {"symbol": "MISSING-TYPE"}}],
    [stock(instrument={"assetType": "EQUITY", "symbol": "bad"})],
    [stock(longQuantity="1e999999")], [stock(shortQuantity=None)],
    [{"instrument": {"assetType": [], "symbol": "BADTYPE"}}]])
def test_malformed_is_unknown_not_empty(rows):
    assert project_stock_inventory({"securitiesAccount": {"positions": rows}}, currency="USD") is None


def test_native_read_uses_only_selected_account_and_requests_positions():
    response = Mock(); response.json.return_value = payload([stock()])
    client = Mock(); client.get_account.return_value = response
    result = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")
    assert result["broker_reported_positions_scope"] == "stocks_only"
    assert result["broker_reported_positions_complete"] is True
    assert result["broker_reported_positions"][0]["quantity"] == "2"
    assert result["broker_reported_positions_observed_at"].endswith("Z")
    assert client.get_account.call_count == 1
    assert client.get_account.call_args.args == ("synthetic-bound-hash",)
    assert client.mock_calls[0].args == ("synthetic-bound-hash",)
    assert response.raise_for_status.call_count == 1


def test_missing_identity_or_currency_does_not_request_and_failure_not_empty():
    client = Mock()
    assert collect_stock_inventory(client, account_hash="", currency="USD") is None
    assert collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency=None) is None
    client.get_account.assert_not_called()
    client.get_account.side_effect = RuntimeError("synthetic provider failure")
    with pytest.raises(RuntimeError):
        collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")


@pytest.mark.parametrize("read_fails", [False, True])
def test_probe_attaches_inventory_without_changing_strategy_snapshot_or_health(read_fails):
    from types import SimpleNamespace
    from test_request_handling import load_module

    module = load_module()
    snapshot = SimpleNamespace(buying_power=123.0, total_equity=456.0, positions=())
    module.fetch_account_snapshot = lambda client, **kwargs: snapshot
    observation = {"account_hash": "synthetic-bound-hash", "net_assets_currency": "USD"}
    module.build_account_observation = lambda *args, **kwargs: dict(observation)
    inventory = {"broker_reported_positions": [], "broker_reported_positions_scope": "stocks_only",
                 "broker_reported_positions_complete": True,
                 "broker_reported_positions_observed_at": "2026-10-25T05:00:00Z"}
    module.collect_stock_inventory = Mock(side_effect=RuntimeError("synthetic failure")) if read_fails else Mock(return_value=inventory)
    saved = {}
    module.build_execution_report = lambda *args, **kwargs: {"status": "pending"}
    module.persist_execution_report = lambda report, **kwargs: saved.update(report) or "/synthetic/report.json"
    module.emit_runtime_log = lambda *args, **kwargs: None
    module.log_runtime_event = lambda *args, **kwargs: None
    client = object()
    module.build_composer = lambda: SimpleNamespace(
        build_client=lambda: client,
        build_reporting_adapters=lambda: SimpleNamespace(build_log_context=lambda: SimpleNamespace(run_id="synthetic-run")),
    )
    body, status = module._handle_schwab_probe()
    assert status == 200 and body == "Probe OK"
    assert saved["status"] == "ok" and snapshot.positions == ()
    module.collect_stock_inventory.assert_called_once_with(client, account_hash="synthetic-bound-hash", currency="USD")
    recorded = saved["summary"]["account_observation"]
    assert recorded["account_hash"] == observation["account_hash"]
    assert ("broker_reported_positions_complete" in recorded) is not read_fails
    if not read_fails:
        assert recorded["broker_reported_positions"] == []
