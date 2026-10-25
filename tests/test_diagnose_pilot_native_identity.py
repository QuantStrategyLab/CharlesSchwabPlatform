from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import diagnose_pilot_native_identity as reader


ROOT = Path(__file__).resolve().parents[1]
SERVICE = "synthetic-serving-service"
REVISION = SERVICE + "-00001-probe"
ACCOUNT = "synthetic-serving-private-account"
NOW = dt.datetime(2026, 10, 25, 8, tzinfo=dt.timezone.utc)


@pytest.fixture(autouse=True)
def runner_mode(monkeypatch):
    # Exercise the actual Actions mask path; assertions distinguish commands
    # consumed by the runner from the fixed safe result artifact.
    monkeypatch.setenv("GITHUB_ACTIONS", "true")


@pytest.fixture
def target():
    return {
        "service_name": SERVICE, "account_scope": "private-scope-canary",
        "account_selector": ["private-selector-canary"], "deployment_selector": "live",
        "strategy_profile": "soxl_soxx_trend_income", "platform_id": "schwab",
        "execution_mode": "live", "dry_run_only": False,
        "runtime_risk_limits": {"binding": {"account_hash": ACCOUNT}, "financial_canary": 123456.78},
        "strategy_release": {"strategy_revision": "a" * 40},
        "live_continuity": {"state": "PAUSED", "baseline_id": "private-baseline-canary"},
    }


@pytest.fixture
def source(target):
    return {"GCP_PROJECT_ID": "synthetic-project", "CLOUD_RUN_REGION": "synthetic-region1", "CLOUD_RUN_SERVICE": SERVICE, "RUNTIME_TARGET_JSON": json.dumps(target)}


def _service(revision=REVISION):
    return {"metadata": {"name": SERVICE}, "status": {"traffic": [{"revisionName": revision, "percent": 100}], "latestReadyRevisionName": SERVICE + "-candidate"}}


def _revision(target):
    return {"metadata": {"name": REVISION, "labels": {"serving.knative.dev/service": SERVICE}}, "spec": {"containers": [{"env": [{"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)}]}]}}


def _probe(source, target, *, replies=None, native_status="verified", native=None, observed_at=NOW):
    events = []
    replies = [_service(), _revision(target), _service(), _service()] if replies is None else replies

    def describe(resource, name, project, region):
        events.append(("metadata", resource, name, project, region))
        response = replies.pop(0)
        if isinstance(response, Exception):
            raise response
        return copy.deepcopy(response)

    def identity(**kwargs):
        events.append(("native", kwargs))
        if native is not None:
            return native(**kwargs)
        if isinstance(native_status, Exception):
            raise native_status
        return native_status

    result = reader.diagnose_native_identity(source, describe=describe, identity_reader=identity, observed_at=observed_at)
    return result, events


def _no_authority(result):
    for key in ("reports_read", "token_refresh_attempted", "secret_write_attempted", "orders_submitted", "notifications_sent", "cloud_write_attempted", "trading_state_changed", "breaker_reset", "account_policy_qualified", "spendable_cash_verified", "execution_enabled_by_check"):
        assert result[key] is False


def test_native_identity_is_bound_to_actual_serving_and_preserves_pause(source, target, capsys):
    result, events = _probe(source, target)
    assert result["status"] == "verified_native_identity"
    assert result["serving_bound_identity_verified"] is True
    assert result["serving_stable_before_native"] is True and result["serving_stable_after_native"] is True
    assert all(result["identity_matches"].values())
    assert result["binding_account_matches_expected"] is True
    assert result["serving_revision_sha256"] == hashlib.sha256(REVISION.encode()).hexdigest()
    assert result["continuity"] == {"state": "PAUSED", "paused": True, "structured_reason_field_present": False, "reason_qualified": False}
    assert [event[:2] for event in events] == [("metadata", "services"), ("metadata", "revisions"), ("metadata", "services"), ("native", events[3][1]), ("metadata", "services")]
    assert events[3][1] == {"environ": {"GCP_PROJECT_ID": "synthetic-project"}, "expected_account_hash": ACCOUNT, "observed_at": NOW}
    output = capsys.readouterr()
    assert output.err == "" and all(line.startswith("::add-mask::") for line in output.out.splitlines())
    encoded = json.dumps(result)
    for value in (SERVICE, REVISION, ACCOUNT, "private-scope-canary", "private-selector-canary", "private-baseline-canary", "123456.78", "a" * 40):
        assert value not in encoded
    _no_authority(result)


def test_daily_declaration_cannot_override_actual_binding(source, target):
    source["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] = json.dumps({**target, "runtime_risk_limits": {"binding": {"account_hash": "stale-daily-account-canary"}}})
    result, events = _probe(source, target)
    assert result["serving_bound_identity_verified"] is True
    assert events[3][1]["expected_account_hash"] == ACCOUNT
    assert "SCHWAB_RUNTIME_DAILY_TARGET_JSON" not in events[3][1]["environ"]


@pytest.mark.parametrize("field,value", [
    ("account_scope", "other-scope-canary"), ("account_selector", ["other-selector-canary"]),
    ("strategy_profile", "tqqq_growth_income"), ("deployment_selector", "other-deployment-canary"),
    ("service_name", "other-service"), ("platform_id", "other-platform"),
])
def test_all_six_identity_fields_gate_native_request(source, target, field, value):
    observed = {**target, field: value}
    result, events = _probe(source, observed)
    assert result["status"] == "unavailable" and result["native_check_attempted"] is False
    assert not any(event[0] == "native" for event in events)
    assert len(events) == 2
    _no_authority(result)


@pytest.mark.parametrize("side", ["source", "serving"])
@pytest.mark.parametrize("binding", [None, {}, {"account_hash": ""}, {"account_hash": " whitespace "}, {"account_hash": False}, {"account_hash": "x" * 513}])
def test_missing_or_invalid_account_binding_does_not_consume_token(source, target, side, binding):
    observed = copy.deepcopy(target)
    changed = target if side == "source" else observed
    changed["runtime_risk_limits"]["binding"] = binding
    source["RUNTIME_TARGET_JSON"] = json.dumps(target)
    result, events = _probe(source, observed)
    assert result["reason"] == "serving_account_binding_unavailable"
    assert result["native_check_attempted"] is False and len(events) == 2


def test_different_serving_hash_is_not_adopted_as_expected(source, target):
    observed = copy.deepcopy(target)
    observed["runtime_risk_limits"]["binding"]["account_hash"] = "other-serving-account-canary"
    result, events = _probe(source, observed)
    assert result["reason"] == "serving_account_binding_mismatch"
    assert result["binding_account_matches_expected"] is False
    assert len(events) == 2 and result["native_check_attempted"] is False


@pytest.mark.parametrize("when", ["before", "after"])
def test_serving_drift_cannot_be_overridden_by_account_membership(source, target, when):
    replies = [_service(), _revision(target), _service(), _service()]
    replies[2 if when == "before" else 3] = _service(SERVICE + "-changed")
    result, events = _probe(source, target, replies=replies)
    assert result["reason"] == "serving_changed_" + when + "_native"
    assert result["serving_bound_identity_verified"] is False
    assert sum(event[0] == "native" for event in events) == (0 if when == "before" else 1)
    assert result["native_identity_matches"] is (None if when == "before" else True)


@pytest.mark.parametrize("status", sorted(reader._NATIVE_STATUSES))
def test_fixed_native_failure_stops_without_retry_or_final_cloud_read(source, target, status):
    result, events = _probe(source, target, native_status=status)
    assert result["reason"] == status and result["native_status"] == status
    assert result["serving_bound_identity_verified"] is False
    assert result["serving_stable_after_native"] is None
    assert [event[0] for event in events] == ["metadata", "metadata", "metadata", "native"]
    _no_authority(result)


@pytest.mark.parametrize("status", ["private-upstream-status-canary", None, {"private": "canary"}, RuntimeError("private-error-canary")])
def test_untrusted_native_errors_never_escape_or_trigger_fallback(source, target, status, capsys):
    result, events = _probe(source, target, native_status=status)
    assert result["reason"] == "native_identity_unavailable"
    assert result["serving_bound_identity_verified"] is False
    assert sum(event[0] == "native" for event in events) == 1
    output = capsys.readouterr()
    assert "private-upstream" not in output.out and "private-error" not in output.out
    assert output.err == "" and "canary" not in json.dumps(result)


def test_sdk_output_is_suppressed_while_masks_remain(source, target, capsys):
    def identity(**_kwargs):
        import sys

        print("private-native-stdout-canary")
        print("private-native-stderr-canary", file=sys.stderr)
        return "verified"

    result, _ = _probe(source, target, native=identity)
    output = capsys.readouterr()
    assert "native-stdout" not in output.out and output.err == ""
    assert result["status"] == "verified_native_identity"


@pytest.mark.parametrize("error,reason", [(RuntimeError("private-cloud-error-canary"), "metadata_unavailable"), (subprocess.TimeoutExpired("private-command-canary", 30), "metadata_timeout")])
def test_metadata_failure_after_native_does_not_qualify_identity(source, target, error, reason):
    result, events = _probe(source, target, replies=[_service(), _revision(target), _service(), error])
    assert result["reason"] == reason and result["serving_bound_identity_verified"] is False
    assert result["native_identity_matches"] is True
    assert len(events) == 5


@pytest.mark.parametrize("extra,present", [
    ({"reason": "private-free-text-canary"}, False), ({"reason_code": "private-code-canary"}, True),
    ({"reason_codes": ["private-code-canary"]}, True), ({"reason": {"private": "free-text-canary"}}, True),
    ({"reason_codes": []}, False), ({"reason_codes": [None]}, False),
    ({"reason_code": "  "}, False), ({"reason": {}}, False),
])
def test_reason_field_presence_is_not_a_cause_or_a_reset(source, target, extra, present):
    target["live_continuity"].update(extra)
    source["RUNTIME_TARGET_JSON"] = json.dumps(target)
    result, _ = _probe(source, target)
    assert result["continuity"]["state"] == "PAUSED"
    assert result["continuity"]["structured_reason_field_present"] is present
    assert result["continuity"]["reason_qualified"] is False
    assert "canary" not in json.dumps(result)
    _no_authority(result)


class _Response:
    def __init__(self, payload, *, status=200):
        self.status_code = status
        self.body = json.dumps(payload).encode()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def iter_content(self, chunk_size):
        assert chunk_size == 8192
        yield self.body


@pytest.mark.parametrize("mode,status,gets", [
    ("matched", "verified_native_identity", 1), ("expired", "token_expired", 0),
    ("denied", "native_account_access_denied", 1), ("mismatch", "native_identity_mismatch", 1),
    ("permission", "token_permission_denied", 0), ("timeout", "native_identity_unavailable", 1),
])
def test_reuses_original_secret_reader_and_single_native_get(source, target, monkeypatch, mode, status, gets):
    import requests
    import socket

    from google.api_core.exceptions import PermissionDenied
    from google.cloud import secretmanager_v1
    from scripts import schwab_native_identity as native

    monkeypatch.setattr(native, "PROJECT_ID", source["GCP_PROJECT_ID"])
    secret_reads = []
    expires = NOW.timestamp() + (-60 if mode == "expired" else 600)
    token = json.dumps({"creation_timestamp": NOW.timestamp() - 120, "token": {"access_token": "synthetic-private-token-canary", "expires_at": expires, "refresh_token": "synthetic-private-refresh-canary"}}).encode()

    class Secrets:
        def access_secret_version(self, *, request, retry, timeout):
            secret_reads.append(request)
            assert request == {"name": f"projects/{source['GCP_PROJECT_ID']}/secrets/{native.SECRET_ID}/versions/latest"}
            assert retry is None and timeout == 15
            if mode == "permission":
                raise PermissionDenied("private-secret-error-canary")
            return SimpleNamespace(payload=SimpleNamespace(data=token))

    monkeypatch.setattr(secretmanager_v1, "SecretManagerServiceClient", Secrets)
    session = Mock()
    response = _Response([{"accountNumber": "private-native-number-canary", "hashValue": "other-canary" if mode == "mismatch" else ACCOUNT}], status=403 if mode == "denied" else 200)
    session.get.return_value = response
    if mode == "timeout":
        session.get.side_effect = requests.Timeout("private-transport-error-canary")
    monkeypatch.setattr(requests, "Session", lambda: session)
    monkeypatch.setattr(socket, "socket", lambda *_a, **_k: pytest.fail("unexpected network"))
    result, events = _probe(source, target, native=reader._existing_native_reader)
    assert len(secret_reads) == 1 and session.get.call_count == gets
    assert result["status"] == "verified_native_identity" if mode == "matched" else result["reason"] == status
    assert result["serving_bound_identity_verified"] is (mode == "matched")
    assert sum(event[0] == "native" for event in events) == 1
    session.post.assert_not_called()
    if gets:
        assert session.get.call_args.args == (native.API_URL,)
        assert session.get.call_args.kwargs["allow_redirects"] is False
        assert session.get.call_args.kwargs["timeout"] == (5, 15)
        assert session.trust_env is False
        session.close.assert_called_once()
    assert "canary" not in json.dumps(result)
    _no_authority(result)


def test_cli_writes_only_private_create_only_safe_artifact(monkeypatch, tmp_path, capsys):
    result = {"schema_version": reader.SCHEMA, "status": "verified_native_identity", "reason": "observed"}
    monkeypatch.setattr(reader, "diagnose_native_identity", lambda _: result)
    monkeypatch.setattr(reader.sys, "argv", ["diagnose_pilot_native_identity"])
    output = tmp_path / "safe.json"
    monkeypatch.setenv("PILOT_NATIVE_RESULT_PATH", str(output))
    assert reader.main() == 0 and json.loads(output.read_text()) == result
    assert output.stat().st_mode & 0o777 == 0o600
    assert reader.main() == 1 and json.loads(output.read_text()) == result
    assert "result_write_failed" in capsys.readouterr().out
    monkeypatch.delenv("PILOT_NATIVE_RESULT_PATH")
    monkeypatch.setattr(reader.sys, "argv", ["diagnose", "--account=private-override-canary"])
    assert reader.main() == 1
    assert "private-override-canary" not in capsys.readouterr().out


def test_workflow_is_protected_and_uses_same_sources_without_daily_or_writes():
    wrapper = (ROOT / ".github/workflows/pilot-native-identity.yml").read_text()
    protected = (ROOT / ".github/workflows/pilot-native-identity-protected.yml").read_text()
    assert "workflow_dispatch:" in wrapper and "inputs:" not in wrapper and "env:" not in wrapper
    assert "uses: ./.github/workflows/pilot-native-identity-protected.yml" in wrapper
    assert "inventory: ${{ vars.CLOUD_RUN_SERVICE_TARGETS_JSON || secrets.CLOUD_RUN_SERVICE_TARGETS_JSON }}" in wrapper
    assert "runtime_target: ${{ vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}" in wrapper
    assert "workflow_call:" in protected and "workflow_dispatch:" not in protected and "vars." not in protected
    assert protected.count("    env:") == 1
    assert "RUNTIME_TARGET_JSON: ${{ secrets.runtime_target }}" in protected
    assert "python3 -m scripts.diagnose_pilot_metadata --bootstrap-cloud-source" in protected
    assert "python -m scripts.diagnose_pilot_native_identity" in protected
    for text in (wrapper, protected):
        for guard in ("github.repository == 'QuantStrategyLab/CharlesSchwabPlatform'", "github.ref == 'refs/heads/main'", "github.run_attempt == 1"):
            assert guard in text
        for forbidden in ("SCHWAB_RUNTIME_DAILY_TARGET_JSON", "SCHWAB_API_KEY", "SCHWAB_APP_SECRET", "TELEGRAM", "projects/", ".iam.gserviceaccount.com", "/run", "/reconcile", "scheduler jobs", "services update", "--publish", "--renew"):
            assert forbidden not in text
