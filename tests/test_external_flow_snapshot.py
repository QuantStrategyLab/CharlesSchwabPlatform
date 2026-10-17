from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from application.external_flow_snapshot import (
    attach_external_flow_snapshot,
    collect_external_flow_snapshot,
    external_flow_enabled,
    project_external_flow_snapshot,
)
from scripts import publish_account_facts_from_reports as publisher
from test_publish_account_facts_from_reports import _valid_report


START = "2026-09-29T22:21:00Z"
END = "2026-09-30T22:21:00Z"
TX = {"activityId": "synthetic-flow", "type": "ACH_RECEIPT", "status": "VALID", "netAmount": "100.25",
      "time": "2026-09-30T10:21:00Z", "settlementDate": "2026-09-30",
      "transferItems": [{"instrument": {"assetType": "CURRENCY"}}]}
KWARGS = dict(expected_account_hash="synthetic-account-hash", observed_account_hash="synthetic-account-hash",
              period_start=START, period_end=END, currency="USD", source_complete=True, enabled=True)


def project(rows, **overrides):
    return project_external_flow_snapshot(rows, **(KWARGS | overrides))


def test_default_off_has_no_client_calls_or_history_change():
    assert external_flow_enabled({}) is False
    assert external_flow_enabled({"SCHWAB_EXTERNAL_FLOW_EXPORT_ENABLED": "true"}) is True
    for value in [True, "1", "TRUE", "false"]:
        assert external_flow_enabled({"SCHWAB_EXTERNAL_FLOW_EXPORT_ENABLED": value}) is False
    assert project([TX], enabled=False) is None
    client = Mock()
    assert collect_external_flow_snapshot(client, account_hash="synthetic", period_start=START,
                                         period_end=END, currency="USD") is None
    client.get_transactions.assert_not_called()
    assert attach_external_flow_snapshot({"old": "unchanged"}, {"invalid": True}) == {"old": "unchanged"}


def test_projection_preserves_signed_native_money_hashes_refs_and_dedupes():
    result = project([TX, TX])
    window = result["windows"][0]
    assert window["coverage"] == "complete"
    assert len(window["records"]) == 1
    record = window["records"][0]
    assert record["amount"] == "100.25"
    assert record["type"] == "deposit"
    assert len(record["source_ref"]) == 64
    assert "synthetic-flow" not in str(result)
    assert "synthetic-account-hash" not in str(result)
    assert project([TX | {"type": "WIRE_OUT", "netAmount": "-2"}])["windows"][0]["records"][0]["type"] == "withdrawal"
    assert project([])["windows"][0]["coverage"] == "complete"
    assert project([], source_complete=False)["windows"][0]["coverage"] == "incomplete"
    assert project(None)["windows"][0]["coverage"] == "incomplete"


@pytest.mark.parametrize("patch", [
    {"type": "JOURNAL"}, {"type": "RECEIVE_AND_DELIVER"}, {"type": "ELECTRONIC_FUND"}, {"type": "NEW_UNKNOWN"},
    {"time": None}, {"time": "2026-09-30T10:00:00"}, {"status": "PENDING"}, {"status": "REVERSED"},
    {"netAmount": "NaN"}, {"netAmount": True}, {"netAmount": "-100"}, {"netAmount": "0"},
    {"activityId": None}, {"transferItems": []}, {"settlementDate": "2026-02-30"},
    {"transferItems": [{"instrument": {"assetType": "EQUITY"}}]},
    {"fees": [{"amount": "1"}]}, {"commission": "1"},
    {"transferItems": [{"instrument": {"assetType": "CURRENCY"}, "feeType": "synthetic-fee"}]},
])
def test_unknown_or_unverified_sources_never_become_zero_flow(patch):
    assert project([TX | patch])["windows"][0]["coverage"] == "incomplete"


def test_internal_dividend_interest_and_trade_are_not_external():
    assert project([{"type": "DIVIDEND_OR_INTEREST"}, {"type": "TRADE"}])["windows"][0]["records"] == []


def test_timestamp_boundaries_identity_currency_and_conflict():
    assert project([TX | {"time": START}])["windows"][0]["records"] == []
    assert len(project([TX | {"time": END}])["windows"][0]["records"]) == 1
    with pytest.raises(ValueError, match="account_mismatch"):
        project([TX], observed_account_hash="another-synthetic-account")
    with pytest.raises(ValueError, match="currency"):
        project([TX], currency=None)
    with pytest.raises(ValueError, match="conflict"):
        project([TX, TX | {"netAmount": "101"}])


def test_fake_client_reuses_existing_read_method_without_assuming_complete():
    client = Mock()
    client.get_transactions.return_value = SimpleNamespace(status_code=200, json=lambda: [TX])
    result = collect_external_flow_snapshot(client, account_hash="synthetic-account-hash", period_start=START,
                                           period_end=END, currency="USD", enabled=True)
    assert result["source_complete"] is False
    assert attach_external_flow_snapshot({"account_hash": "synthetic-account-hash", "observed_finished_at": END}, result, enabled=True)["externalFlow"]["windows"][0]["coverage"] == "incomplete"
    client.get_transactions.assert_called_once()
    assert client.get_transactions.call_args.args == ("synthetic-account-hash",)
    client.get_transactions.return_value.status_code = 403
    with pytest.raises(ValueError, match="unavailable"):
        collect_external_flow_snapshot(client, account_hash="synthetic-account-hash", period_start=START,
                                       period_end=END, currency="USD", enabled=True)


def test_actual_publisher_default_unchanged_and_optional_upload_contract():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    kwargs = dict(source_report_uri=prefix + "2026-09/20260930T222000Z.json", report_prefix=prefix,
                  expected_service_name="synthetic-service", expected_runtime_revision="service-00007-abc",
                  expected_target_id="synthetic-target", now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc))
    report = _valid_report()
    legacy = publisher.project_schwab_account_facts_history(report, **kwargs)
    report["account_external_flows"] = {"account_hash": "synthetic-account-hash", "period_start": START,
                                        "period_end": END, "currency": "USD", "source_complete": True, "transactions": [TX]}
    assert publisher.project_schwab_account_facts_history(report, **kwargs) == legacy
    assert publisher.project_schwab_account_facts_history(_valid_report(), external_flow_enabled=True, **kwargs) == legacy
    projected = publisher.project_schwab_account_facts_history(report, external_flow_enabled=True, **kwargs)
    assert projected["externalFlow"] == project([TX])
    assert publisher.prepare_sync_payload(projected, include_external_flow=True)["externalFlow"] == project([TX])
    invalid = deepcopy(report)
    invalid["account_external_flows"]["account_hash"] = "wrong-synthetic"
    assert publisher.project_schwab_account_facts_history(invalid, external_flow_enabled=True, **kwargs) == {"status": "skipped", "reason": "external_flow_invalid"}


def test_collector_raw_envelope_attaches_without_adapter():
    client = Mock()
    client.get_transactions.return_value = SimpleNamespace(status_code=200, json=lambda: [TX])
    raw = collect_external_flow_snapshot(
        client, account_hash="synthetic-account-hash", period_start=START,
        period_end=END, currency="USD", enabled=True,
    )
    history = {"account_hash": "synthetic-account-hash", "observed_finished_at": END}
    attached = attach_external_flow_snapshot(history, raw, enabled=True)
    assert attached["externalFlow"] == project([TX], source_complete=False)
    assert set(raw) == {"account_hash", "period_start", "period_end", "currency", "source_complete", "transactions"}
    assert raw["source_complete"] is False
    assert raw["transactions"] == [TX]


@pytest.mark.parametrize("rows", [[], [TX]])
@pytest.mark.parametrize("attestation", [False, None, "true", 1, True])
def test_raw_source_attestation_is_never_inferred(rows, attestation):
    client = Mock()
    client.get_transactions.return_value = SimpleNamespace(status_code=200, json=lambda: rows)
    raw = collect_external_flow_snapshot(
        client, account_hash="synthetic-account-hash", period_start=START, period_end=END,
        currency="USD", enabled=True, source_complete=attestation,
    )
    out = attach_external_flow_snapshot(
        {"account_hash": "synthetic-account-hash", "observed_finished_at": END}, raw, enabled=True,
    )
    assert out["externalFlow"]["windows"][0]["coverage"] == ("complete" if attestation is True else "incomplete")
    assert client.get_transactions.call_count == 1


@pytest.mark.parametrize("status", [206, 403, 429, 500])
def test_failed_or_partial_source_does_not_return_raw(status):
    client = Mock()
    client.get_transactions.return_value = SimpleNamespace(status_code=status, json=Mock())
    with pytest.raises(ValueError, match="source_unavailable"):
        collect_external_flow_snapshot(client, account_hash="synthetic-account-hash", period_start=START,
                                       period_end=END, currency="USD", enabled=True)
    client.get_transactions.return_value.json.assert_not_called()


@pytest.mark.parametrize("rows", [{}, {"transactions": [TX]}, None, "unknown"])
def test_non_list_source_is_rejected(rows):
    client = Mock()
    client.get_transactions.return_value = SimpleNamespace(status_code=200, json=lambda: rows)
    with pytest.raises(ValueError, match="source_invalid"):
        collect_external_flow_snapshot(client, account_hash="synthetic-account-hash", period_start=START,
                                       period_end=END, currency="USD", enabled=True)


def test_json_failure_propagates_without_raw_snapshot():
    client = Mock()
    client.get_transactions.return_value = SimpleNamespace(status_code=200, json=Mock(side_effect=ValueError("synthetic invalid JSON")))
    with pytest.raises(ValueError, match="invalid JSON"):
        collect_external_flow_snapshot(client, account_hash="synthetic-account-hash", period_start=START,
                                       period_end=END, currency="USD", enabled=True)


@pytest.mark.parametrize("start,end", [(END, START), (START, START), ("2026-09-27T00:00:00Z", END), ("2026-09-29T22:21:00", END), ("invalid", END)])
def test_invalid_windows_are_rejected_before_sdk(start, end):
    client = Mock()
    with pytest.raises(ValueError):
        collect_external_flow_snapshot(client, account_hash="synthetic-account-hash", period_start=start,
                                       period_end=end, currency="USD", enabled=True)
    client.get_transactions.assert_not_called()


def test_offsets_dst_and_record_limit_preserve_projector_rules():
    kwargs = dict(period_start="2026-03-07T12:00:00Z", period_end="2026-03-08T12:00:00Z")
    for when, expected in [("2026-03-08T01:30:00-05:00", "2026-03-08T06:30:00.000Z"),
                           ("2026-03-08T03:30:00-04:00", "2026-03-08T07:30:00.000Z")]:
        out = project([TX | {"time": when}], **kwargs)
        assert out["windows"][0]["records"][0]["effective_at"] == expected
    with pytest.raises(ValueError, match="size_limit"):
        project([TX | {"activityId": str(i)} for i in range(257)])
