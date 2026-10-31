import datetime as dt
import json
import subprocess
from types import SimpleNamespace

import pytest

from scripts import schwab_outcome_origin as origin


PRIVATE = "private-account-order-canary"
ROOT = "gs://synthetic-private/state"
URI = ROOT + "/execution_outcomes/" + PRIVATE + ".json"


def fixture(*, checksum=True, metadata=None, events=None, code=0, stderr=""):
    item = origin.locator(URI, PRIVATE)
    calls = []
    stamp = dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)
    blob = SimpleNamespace(generation=10, time_created=stamp, updated=stamp,
        md5_hash=item["md5"] if checksum else "changed", size=item["size"], metadata=metadata)
    blob.reload = lambda **kwargs: calls.append(("reload", kwargs))
    client = SimpleNamespace(bucket=lambda bucket: SimpleNamespace(blob=lambda path: blob))
    def run(command, **kwargs):
        calls.append((command, kwargs))
        assert command[:3] == ("gcloud", "logging", "read")
        assert "--limit=21" in command and "--order=asc" in command
        assert "protoPayload.resourceName=" in command[3] and "timestamp>=" in command[3]
        assert kwargs["check"] is False and kwargs["timeout"] == 30
        assert kwargs["env"]["CLOUDSDK_CORE_DISABLE_FILE_LOGGING"] == "true"
        return subprocess.CompletedProcess(command, code, json.dumps([] if events is None else events), stderr)
    return item, blob, client, run, calls


def test_exact_object_metadata_and_empty_audit_window_do_not_identify_application_writer():
    item, _, client, run, calls = fixture(metadata={"source_sha": "a" * 40, "github_run_id": "123", PRIVATE: PRIVATE})
    result = origin.inspect_origin(outcomes=[item], state_uri=ROOT, project="synthetic", client=client, run=run)
    assert result["object_checksum_matches_scan"] is True
    assert result["declared_source_sha"] == "a" * 40 and result["declared_run_id"] == "123"
    assert result["audit_status"] == "no_matching_events" and result["audit_matching_object_events"] == 0
    assert calls[0] == ("reload", {"timeout": 15, "retry": None})
    assert result["application_writer_verified"] is False and result["business_outcome_verified"] is False
    assert result["recovery_qualified"] is False and PRIVATE not in json.dumps(result)


def test_matching_storage_principal_is_not_application_writer_or_business_result():
    event = {"protoPayload": {"resourceName": "projects/_/buckets/synthetic-private/objects/state/execution_outcomes/" + PRIVATE + ".json",
        "methodName": "storage.objects.create", "authenticationInfo": {"principalEmail": PRIVATE}}}
    item, _, client, run, _ = fixture(events=[event])
    result = origin.inspect_origin(outcomes=[item], state_uri=ROOT, project="synthetic", client=client, run=run)
    assert result["audit_status"] == "matching_events" and result["audit_principal_present"] is True
    assert result["application_writer_verified"] is False and PRIVATE not in json.dumps(result)


@pytest.mark.parametrize("change,reason", [("checksum", "object_generation_or_checksum_unverified"),
    ("generation", "object_generation_or_checksum_unverified"), ("time", "object_audit_time_unavailable"),
    ("foreign", "object_binding_invalid"), ("multiple", "rejected_outcome_not_unique")])
def test_unsafe_or_changed_object_does_not_query_audit(change, reason):
    item, blob, client, run, calls = fixture(checksum=change != "checksum")
    if change == "generation": blob.generation = None
    if change == "time": blob.time_created = None
    if change == "foreign": item["uri"] = "gs://foreign/private"
    result = origin.inspect_origin(outcomes=[item, item] if change == "multiple" else [item],
        state_uri=ROOT, project="synthetic", client=client, run=run)
    assert result["reason"] == reason and result["audit_read_attempted"] is False
    assert all(event[0] == "reload" for event in calls)
    assert PRIVATE not in json.dumps(result)


@pytest.mark.parametrize("events,code,stderr,status", [
    ([], 1, "PERMISSION_DENIED " + PRIVATE, "permission_denied"),
    ([], 1, PRIVATE, "read_failed"),
    ([{}] * 21, 0, "", "truncated_or_invalid"),
    ([{"protoPayload": {"resourceName": PRIVATE, "methodName": "storage.objects.create"}}], 0, "", "invalid_scope"),
])
def test_audit_failure_stays_unknown_without_raw_provider_output(events, code, stderr, status):
    item, _, client, run, _ = fixture(events=events, code=code, stderr=stderr)
    result = origin.inspect_origin(outcomes=[item], state_uri=ROOT, project="synthetic", client=client, run=run)
    assert result["audit_status"] == status and result["audit_matching_object_events"] is None
    assert result["application_writer_verified"] is False and PRIVATE not in json.dumps(result)
