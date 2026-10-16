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
    assert result["windows"][0]["coverage"] == "incomplete"
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
    assert publisher.strip_archive_only_keys(projected)["externalFlow"] == project([TX])
    invalid = deepcopy(report)
    invalid["account_external_flows"]["account_hash"] = "wrong-synthetic"
    assert publisher.project_schwab_account_facts_history(invalid, external_flow_enabled=True, **kwargs) == {"status": "skipped", "reason": "external_flow_invalid"}
