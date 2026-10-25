"""Synthetic current holdings from the read-only native account endpoint."""
from copy import deepcopy
import json
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
    diagnostics = result["broker_stock_inventory_diagnostics"]
    assert diagnostics["fallback_attempted"] is False
    assert diagnostics["fallback_reason"] == "not_needed"
    assert diagnostics["linked_positions_field_present"] is None


def test_missing_identity_or_currency_does_not_request_and_failure_not_empty():
    client = Mock()
    assert collect_stock_inventory(client, account_hash="", currency="USD") is None
    assert collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency=None) is None
    client.get_account.assert_not_called()
    client.get_account.side_effect = RuntimeError("synthetic provider failure")
    with pytest.raises(RuntimeError):
        collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")


@pytest.mark.parametrize("positions", [[], [stock()]])
def test_missing_field_can_use_explicit_inventory_for_the_single_verified_linked_account(positions):
    client = Mock()
    client.get_account.return_value.json.return_value = {"securitiesAccount": {}}
    client.get_account_numbers.return_value.json.return_value = [
        {"accountNumber": "synthetic-account", "hashValue": "synthetic-bound-hash"}]
    client.get_accounts.return_value.json.return_value = [
        {"securitiesAccount": {"accountNumber": "synthetic-account", "positions": positions}}]
    result = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")
    assert result["broker_reported_positions_complete"] is True
    assert len(result["broker_reported_positions"]) == len(positions)
    diagnostics = result["broker_stock_inventory_diagnostics"]
    assert diagnostics["fallback_attempted"] is True
    assert diagnostics["fallback_reason"] == "linked_read_completed"
    assert diagnostics["linked_positions_field_present"] is True
    client.get_account_numbers.assert_called_once_with()
    client.get_accounts.assert_called_once_with(fields=client.get_account.call_args.kwargs["fields"])


@pytest.mark.parametrize("mapping,reason", [(None, "mapping_unusable"), ([], "mapping_not_single"), ([
    {"accountNumber": "synthetic-account", "hashValue": "wrong-bound-hash"}], "mapping_unverified"), ([
    {"accountNumber": "synthetic-account", "hashValue": "synthetic-bound-hash"},
    {"accountNumber": "other-account", "hashValue": "other-hash"}], "mapping_not_single")])
def test_missing_inventory_never_broadens_the_authorized_account_scope(mapping, reason):
    client = Mock()
    client.get_account.return_value.json.return_value = {"securitiesAccount": {}}
    client.get_account_numbers.return_value.json.return_value = mapping
    result = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")
    assert result["broker_stock_inventory_status"] == "unknown"
    assert "broker_reported_positions_complete" not in result
    diagnostics = result["broker_stock_inventory_diagnostics"]
    assert diagnostics["reason"] == "positions_field_missing"
    assert diagnostics["fallback_attempted"] is False
    assert diagnostics["fallback_reason"] == reason
    assert diagnostics["linked_positions_field_present"] is None
    client.get_accounts.assert_not_called()


@pytest.mark.parametrize("linked,reason,present", [([], "linked_payload_unusable", None), ([
    {"securitiesAccount": {"accountNumber": "other-account", "positions": []}}], "linked_identity_unverified", None), ([
    {"securitiesAccount": {"accountNumber": "synthetic-account"}}], "linked_read_completed", False), ([
    {"securitiesAccount": {"accountNumber": "synthetic-account", "positions": None}}], "linked_read_completed", True)])
def test_linked_account_omission_or_identity_mismatch_remains_unknown(linked, reason, present):
    client = Mock()
    client.get_account.return_value.json.return_value = {"securitiesAccount": {}}
    client.get_account_numbers.return_value.json.return_value = [
        {"accountNumber": "synthetic-account", "hashValue": "synthetic-bound-hash"}]
    client.get_accounts.return_value.json.return_value = linked
    result = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")
    assert result["broker_stock_inventory_status"] == "unknown"
    assert "broker_reported_positions_complete" not in result
    diagnostics = result["broker_stock_inventory_diagnostics"]
    assert diagnostics["fallback_attempted"] is True
    assert diagnostics["fallback_reason"] == reason
    assert diagnostics["linked_positions_field_present"] is present
    safe = json.dumps(diagnostics)
    for private in ("synthetic-account", "other-account", "synthetic-bound-hash"):
        assert private not in safe


@pytest.mark.parametrize("missing_method", ["get_account_numbers", "get_accounts"])
def test_unavailable_fallback_method_is_distinct_from_a_missing_linked_field(missing_method):
    client = Mock()
    client.get_account.return_value.json.return_value = {"securitiesAccount": {}}
    setattr(client, missing_method, None)
    diagnostics = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")["broker_stock_inventory_diagnostics"]
    assert diagnostics["fallback_attempted"] is False
    assert diagnostics["fallback_reason"] == "reader_unavailable"
    assert diagnostics["linked_positions_field_present"] is None


def test_real_sdk_requests_positions_for_selected_and_linked_reads_without_network():
    from schwab.client import Client

    client = Client("synthetic-api-key", Mock())
    replies = [
        {"securitiesAccount": {}},
        [{"accountNumber": "synthetic-account", "hashValue": "synthetic-bound-hash"}],
        [{"securitiesAccount": {"accountNumber": "synthetic-account"}}],
    ]
    def response(_path, _params):
        result = Mock()
        result.json.return_value = replies.pop(0)
        return result
    client._get_request = Mock(side_effect=response)
    result = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")
    assert [call.args for call in client._get_request.call_args_list] == [
        ("/trader/v1/accounts/synthetic-bound-hash", {"fields": "positions"}),
        ("/trader/v1/accounts/accountNumbers", {}),
        ("/trader/v1/accounts", {"fields": "positions"}),
    ]
    assert result["broker_stock_inventory_diagnostics"]["fallback_attempted"] is True
    assert result["broker_stock_inventory_diagnostics"]["linked_positions_field_present"] is False


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
                 "broker_reported_positions_observed_at": "2026-10-25T05:00:00Z",
                 "broker_stock_inventory_diagnostics": {"fallback_attempted": True,
                     "fallback_reason": "linked_read_completed", "linked_positions_field_present": True}}
    module.collect_stock_inventory = Mock(side_effect=RuntimeError("synthetic failure")) if read_fails else Mock(return_value=inventory)
    saved = {}
    module.build_execution_report = lambda *args, **kwargs: {"status": "pending"}
    module.persist_execution_report = lambda report, **kwargs: saved.update(report) or "/synthetic/report.json"
    module.emit_runtime_log = lambda *args, **kwargs: None
    module.log_runtime_event = Mock()
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
        events = [call.kwargs for call in module.log_runtime_event.call_args_list
                  if call.args[1] == "stock_inventory_observed"]
        assert len(events) == 1
        assert events[0]["inventory_fallback_attempted"] is True
        assert events[0]["inventory_fallback_reason"] == "linked_read_completed"
        assert events[0]["linked_positions_present"] is True
        assert "synthetic-bound-hash" not in json.dumps(events)


@pytest.mark.parametrize("native,reason,present,count", [
    ({"securitiesAccount": {}}, "positions_field_missing", False, None),
    (payload([]), "available", True, 0),
    (payload([stock(shortQuantity=None)]), "stock_quantity_unusable", True, 1),
])
def test_native_shape_diagnostics_distinguish_missing_empty_and_malformed_without_values(native, reason, present, count):
    client = Mock(); client.get_account.return_value.json.return_value = native
    result = collect_stock_inventory(client, account_hash="synthetic-bound-hash", currency="USD")
    info = result["broker_stock_inventory_diagnostics"]
    assert info["reason"] == reason and info["positions_field_present"] is present
    assert info["native_positions_count"] == count
    assert set(info) <= {"reason", "positions_field_present", "native_positions_count", "projected_stock_count",
                        "fallback_attempted", "fallback_reason", "linked_positions_field_present"}
    assert (result["broker_stock_inventory_status"] == "available") is (reason == "available")
    assert ("broker_reported_positions_complete" in result) is (reason == "available")
