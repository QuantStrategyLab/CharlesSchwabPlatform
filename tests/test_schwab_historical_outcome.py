from __future__ import annotations

import copy
import json

import pytest

from scripts import schwab_historical_outcome as history


PREFIX = "gs://synthetic-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
TARGET = {"service_name": "synthetic-service", "platform_id": "schwab", "account_scope": "live",
          "account_selector": ["live"], "deployment_selector": "live", "strategy_profile": "soxl_soxx_trend_income",
          "live_continuity": {"state": "PAUSED"}}
ACCOUNT = "a" * 64
OUTCOME = {"marker_key": "private-marker-canary", "metadata": {
    "signal_date": "2026-10-23", "effective_date": "2026-10-24", "execution_status": ""}}


def report():
    return {"schema_version": "runtime_report.v1", "run_id": "20261024T120000Z", "project_id": "synthetic-project",
            "platform": "charles_schwab", "service_name": TARGET["service_name"], "strategy_profile": TARGET["strategy_profile"],
            "dry_run": False, "runtime_target": copy.deepcopy(TARGET),
            "summary": {"signal_date": "2026-10-23", "effective_date": "2026-10-24",
                        "account_observation": {"account_hash": ACCOUNT}, "orders_submitted_count": 0},
            "diagnostics": {"runtime_revision": "synthetic-service-00001-abc"}}


class Blob:
    generation = 7
    content_encoding = None

    def __init__(self, payload=None, name=None):
        self.body = json.dumps(report() if payload is None else payload).encode()
        self.size = len(self.body)
        self.name = name or "execution-reports/charles_schwab/soxl_soxx_trend_income/2026-10/20261024T120000Z.json"
        self.calls = []

    def download_as_bytes(self, **kwargs):
        self.calls.append(kwargs)
        return self.body


class Client:
    def __init__(self, blobs=(), error=None):
        self.blobs = blobs
        self.error = error
        self.calls = []

    def list_blobs(self, bucket, **kwargs):
        self.calls.append((bucket, kwargs))
        if self.error:
            raise self.error
        return iter(self.blobs)


def inspect(client, **kwargs):
    options = dict(outcomes=[copy.deepcopy(OUTCOME)], env={"EXECUTION_REPORT_GCS_URI": {"value": PREFIX}},
                   state_uri="gs://synthetic-bucket/private-state", target=TARGET, account=ACCOUNT,
                   project="synthetic-project", client=client)
    options.update(kwargs)
    return history.inspect_history(**options)


def test_one_date_identity_revision_candidate_is_still_unknown_even_with_claimed_marker():
    payload = report()
    payload["marker_key"] = OUTCOME["marker_key"]
    payload["summary"]["execution_marker_key"] = OUTCOME["marker_key"]
    blob = Blob(payload)
    client = Client([blob])
    result = inspect(client)
    assert result["status"] == "unknown"
    assert result["reason"] == "exact_execution_link_unavailable"
    assert result["window_reports_count"] == result["date_candidates_count"] == result["identity_date_candidates_count"] == 1
    assert result["candidates_with_revision_count"] == 1
    for key in ("exact_execution_link_supported", "writer_source_verified", "broker_order_mapping_verified", "recovery_qualified"):
        assert result[key] is False
    _, options = client.calls[0]
    assert options["start_offset"].endswith("2026-10/20261023T000000Z.json")
    assert options["end_offset"].endswith("2026-10/20261026T000000Z.json")
    assert options["max_results"] == options["page_size"] == 21 and options["retry"] is None
    assert blob.calls == [{"start": 0, "end": 1024 * 1024, "if_generation_match": 7, "timeout": 15, "retry": None}]
    encoded = json.dumps(result)
    for private in (OUTCOME["marker_key"], ACCOUNT, PREFIX, "2026-10-24", "synthetic-service-00001-abc"):
        assert private not in encoded


@pytest.mark.parametrize("fault", ["wrong_account", "unicode_account", "missing_account", "other_strategy", "other_selector"])
def test_same_date_never_establishes_identity(fault):
    payload = report()
    if fault == "wrong_account":
        payload["summary"]["account_observation"]["account_hash"] = "b" * 64
    elif fault == "unicode_account":
        payload["summary"]["account_observation"]["account_hash"] = "Ａ" * 64
    elif fault == "missing_account":
        payload["summary"].pop("account_observation")
    elif fault == "other_strategy":
        payload["runtime_target"]["strategy_profile"] = "other"
    else:
        payload["runtime_target"]["account_selector"] = ["other"]
    result = inspect(Client([Blob(payload)]))
    assert result["date_candidates_count"] == 1
    assert result["identity_date_candidates_count"] == result["candidates_with_revision_count"] == 0
    assert result["reason"] == "qualified_report_not_found"


def test_hex_representation_remains_the_existing_account_contract():
    payload = report()
    payload["summary"]["account_observation"]["account_hash"] = ACCOUNT.upper()
    payload["diagnostics"] = {}
    result = inspect(Client([Blob(payload)]))
    assert result["identity_date_candidates_count"] == 1
    assert result["candidates_with_revision_count"] == 0
    assert not result["writer_source_verified"]


@pytest.mark.parametrize("field,value", [("dry_run", True), ("run_id", "other"), ("project_id", "other"), ("platform", "other")])
def test_unqualified_report_is_not_a_date_candidate(field, value):
    payload = report()
    payload[field] = value
    result = inspect(Client([Blob(payload)]))
    assert result["window_exhausted"] is True and result["date_candidates_count"] == 0


def test_date_mismatch_and_multiple_candidates_remain_unknown():
    payload = report()
    payload["summary"]["effective_date"] = "2026-10-25"
    assert inspect(Client([Blob(payload)]))["date_candidates_count"] == 0
    result = inspect(Client([Blob(), Blob()]))
    assert result["identity_date_candidates_count"] == 2
    assert result["status"] == "unknown" and not result["exact_execution_link_supported"]


@pytest.mark.parametrize("outcomes", [[], [OUTCOME, OUTCOME]])
def test_ambiguous_rejected_record_does_not_read_reports(outcomes):
    client = Client()
    result = inspect(client, outcomes=outcomes)
    assert result["reason"] == "rejected_outcome_not_unique"
    assert not result["report_read_attempted"] and not client.calls


@pytest.mark.parametrize("date", ["", "no-effective-date", "20261024", "2026-10-24T12:00:00Z", "0001-01-01", None])
def test_invalid_or_overflow_date_has_no_guessed_window(date):
    value = copy.deepcopy(OUTCOME)
    value["metadata"]["effective_date"] = date
    client = Client()
    assert inspect(client, outcomes=[value])["reason"] == "outcome_date_window_unavailable"
    assert not client.calls


@pytest.mark.parametrize("env", [{}, {"EXECUTION_REPORT_GCS_URI": {"value": PREFIX.replace("synthetic-bucket", "other-bucket")}},
                                {"EXECUTION_REPORT_GCS_URI": {"value": PREFIX.replace("execution-reports", "other")}},
                                {"EXECUTION_REPORT_GCS_URI": {"valueFrom": {"secretKeyRef": {"name": "private"}}}}])
def test_missing_redirected_or_secret_source_never_reads(env):
    client = Client()
    assert inspect(client, env=env)["reason"] == "report_source_unavailable"
    assert not client.calls


def test_explicit_blank_current_report_source_does_not_fall_back_to_legacy():
    client = Client()
    result = inspect(client, env={"EXECUTION_REPORT_CLOUD_URI": {"value": ""},
                                  "EXECUTION_REPORT_GCS_URI": {"value": PREFIX}})
    assert result["reason"] == "report_source_unavailable" and not client.calls


def test_changed_continuity_does_not_read_history():
    target = copy.deepcopy(TARGET)
    target["live_continuity"]["state"] = "ACTIVE_LKG"
    client = Client()
    assert inspect(client, target=target)["reason"] == "continuity_not_paused"
    assert not client.calls


def test_truncated_window_does_not_download_twenty_first_body():
    blobs = [Blob() for _ in range(21)]
    result = inspect(Client(blobs))
    assert result["reason"] == "report_window_truncated" and result["window_exhausted"] is False
    assert result["window_reports_count"] == 20 and not blobs[-1].calls


def test_read_denial_is_unknown_and_never_an_empty_report_set(capsys):
    result = inspect(Client(error=PermissionError("private-upstream-canary")))
    assert result["reason"] == "report_read_failed" and result["window_exhausted"] is None
    assert result["window_reports_count"] is result["identity_date_candidates_count"] is None
    assert capsys.readouterr().out == capsys.readouterr().err == ""


@pytest.mark.parametrize("fault", ["oversize", "encoded", "missing_generation", "outside_window", "invalid_json"])
def test_malformed_or_unbounded_object_does_not_qualify(fault):
    blob = Blob()
    if fault == "oversize": blob.size = 1024 * 1024 + 1
    elif fault == "encoded": blob.content_encoding = "gzip"
    elif fault == "missing_generation": blob.generation = None
    elif fault == "outside_window": blob.name = blob.name.replace("20261024", "20261030")
    else: blob.body = b'{"private":"canary",'
    result = inspect(Client([blob]))
    assert result["reason"] == "report_read_failed" and result["window_reports_count"] is None


def test_exhausted_empty_window_does_not_prove_absent_historical_orders():
    result = inspect(Client())
    assert result["window_exhausted"] is True and result["window_reports_count"] == 0
    assert result["reason"] == "qualified_report_not_found"
    assert result["status"] == "unknown" and not result["broker_order_mapping_verified"]
