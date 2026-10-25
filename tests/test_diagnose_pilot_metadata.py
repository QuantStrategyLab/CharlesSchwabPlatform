from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

from scripts import diagnose_pilot_metadata as reader


ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "account-private-canary-918273"
SERVICE = "selected-service"
REVISION = SERVICE + "-00001-test"


@pytest.fixture(autouse=True)
def isolate_runner_environment(monkeypatch):
    # Most unit tests exercise a local call. Actions masking and bootstrap tests
    # select their own runner mode explicitly rather than inheriting the host.
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)


@pytest.fixture
def target():
    return {
        "service_name": SERVICE, "account_scope": "private-scope-canary",
        "account_selector": [ACCOUNT], "deployment_selector": "private-deployment-canary",
        "strategy_profile": "soxl_soxx_trend_income", "platform_id": "schwab",
        "execution_mode": "live", "dry_run_only": False,
        "live_continuity": {"state": "RECONCILE_ONLY", "baseline_id": "private-baseline-canary"},
        "strategy_release": {"strategy_revision": "f" * 40},
        "runtime_risk_limits": {"binding": {"account_hash": ACCOUNT}, "financial_canary": 123456.78},
    }


@pytest.fixture
def source(target):
    return {
        "GCP_PROJECT_ID": "synthetic-project", "CLOUD_RUN_REGION": "test-region1",
        "CLOUD_RUN_SERVICE": SERVICE, "RUNTIME_TARGET_JSON": json.dumps(target),
        "RUNTIME_TARGET_ENABLED": "false", "SCHWAB_DRY_RUN_ONLY": "false",
        "SCHWAB_CASH_ONLY_EXECUTION": "true",
    }


def _service(revision=REVISION):
    return {"metadata": {"name": SERVICE}, "status": {
        "traffic": [{"revisionName": revision, "percent": 100}],
        "latestReadyRevisionName": SERVICE + "-unrelated-ready",
        "latestCreatedRevisionName": SERVICE + "-unrelated-created",
    }}


def _revision(target):
    return {
        "metadata": {"name": REVISION, "labels": {"serving.knative.dev/service": SERVICE}},
        "spec": {"containers": [{"env": [
            {"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)},
            {"name": "RUNTIME_TARGET_ENABLED", "value": "false"},
            {"name": "SCHWAB_DRY_RUN_ONLY", "value": "false"},
            {"name": "SCHWAB_CASH_ONLY_EXECUTION", "value": "true"},
        ]}]},
    }


def _read(source, revision, *, before=None, after=None):
    calls = []
    responses = [before or _service(), revision, after or _service()]

    def describe(*args):
        calls.append(args)
        return copy.deepcopy(responses[len(calls) - 1])

    return reader.diagnose_metadata(source, describe=describe), calls


def _entry(revision, name):
    return next(e for e in revision["spec"]["containers"][0]["env"] if e["name"] == name)


def _assert_no_authority(result):
    for key in (
        "broker_requested", "reports_read", "cloud_write_attempted", "notifications_sent",
        "account_policy_qualified", "spendable_cash_verified", "execution_authorized",
        "runtime_consumption_verified", "breaker_reset",
    ):
        assert result[key] is False


def test_reads_only_exact_serving_and_keeps_reconcile_only(source, target, capsys):
    result, calls = _read(source, _revision(target))
    assert result["status"] == "verified_metadata"
    assert result["serving_stable"] is True
    assert result["serving_revision_sha256"] == hashlib.sha256(REVISION.encode()).hexdigest()
    assert all(result["identity_matches"].values())
    assert result["declarations"]["live_continuity"]["state"] == "RECONCILE_ONLY"
    assert result["declarations"]["RUNTIME_TARGET_ENABLED"]["value"] is False
    assert [call[:2] for call in calls] == [("services", SERVICE), ("revisions", REVISION), ("services", SERVICE)]
    _assert_no_authority(result)
    assert capsys.readouterr() == ("", "")
    encoded = json.dumps(result)
    for private in (ACCOUNT, SERVICE, REVISION, "private-scope-canary", "private-deployment-canary", "private-baseline-canary", "123456.78", "f" * 40):
        assert private not in encoded


@pytest.mark.parametrize("shape", ["list", "object"])
def test_uses_original_inventory_parser_and_exact_service(source, target, shape):
    sibling = {**target, "service_name": "unrelated-service", "account_selector": ["unrelated-private-account"]}
    entries = [{"service_name": sibling["service_name"], "runtime_target": sibling}, {"service_name": SERVICE, "runtime_target": target, "env": {"RUNTIME_TARGET_ENABLED": False}}]
    source["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps(entries if shape == "list" else {"targets": entries})
    result, calls = _read(source, _revision(target))
    assert result["status"] == "verified_metadata"
    assert len(calls) == 3
    assert result["declarations"]["RUNTIME_TARGET_ENABLED"]["matches_expected"] is True


def test_original_environment_target_precedes_legacy_inventory_for_selected_service(source, target):
    stale = {**target, "account_selector": ["stale-private-canary"]}
    source["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps({"targets": [{"service": SERVICE, "runtime_target": stale}]})
    result, _ = _read(source, _revision(target))
    assert result["status"] == "verified_metadata"


@pytest.mark.parametrize("value", ["", "missing-service", "a,b", " has-spaces "])
def test_does_not_guess_service_or_accept_cli_style_override(source, target, value):
    source["CLOUD_RUN_SERVICE"] = value
    source["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps({"targets": [{"service": SERVICE, "runtime_target": target}]})
    result, calls = _read(source, _revision(target))
    assert result["status"] == "unavailable"
    assert calls == []


def test_duplicate_selected_inventory_is_not_arbitrarily_chosen(source, target):
    item = {"service": SERVICE, "runtime_target": target}
    source["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps([item, item])
    result, calls = _read(source, _revision(target))
    assert result["reason"] == "selected_service_not_unique" and not calls


@pytest.mark.parametrize("selectors", [[], [ACCOUNT, "second-private-canary"], ACCOUNT, None])
def test_requires_single_explicit_account_before_cloud_reads(source, target, selectors):
    target["account_selector"] = selectors
    source["RUNTIME_TARGET_JSON"] = json.dumps(target)
    result, calls = _read(source, _revision(target))
    assert result["reason"] == "single_account_required" and not calls


@pytest.mark.parametrize("traffic", [
    [], [{"revisionName": REVISION, "percent": 0}],
    [{"revisionName": REVISION, "percent": 99}],
    [{"revisionName": REVISION, "percent": 50}, {"revisionName": SERVICE + "-other", "percent": 50}],
    [{"revisionName": REVISION, "percent": 100}, {"revisionName": REVISION, "percent": 100}],
    [{"revisionName": REVISION, "percent": True}],
    [{"revisionName": REVISION, "percent": "100"}],
    [{"revisionName": REVISION, "percent": -1}],
    [{"revisionName": REVISION, "percent": 101}],
    [{"revisionName": "unrelated-service-00001", "percent": 100}],
    [{"latestRevision": True, "percent": 100}],
])
def test_refuses_ambiguous_traffic_and_latest_fallback(source, target, traffic):
    before = _service()
    before["status"]["traffic"] = traffic
    result, calls = _read(source, _revision(target), before=before)
    assert result["status"] == "unavailable" and len(calls) == 1
    assert result["serving_stable"] is None


def test_zero_traffic_candidate_does_not_replace_serving(source, target):
    before = _service()
    before["status"]["traffic"].append({"revisionName": SERVICE + "-candidate", "percent": 0})
    result, calls = _read(source, _revision(target), before=before)
    assert result["status"] == "verified_metadata" and calls[1][1] == REVISION


def test_service_drift_after_read_is_not_success(source, target):
    result, calls = _read(source, _revision(target), after=_service(SERVICE + "-changed"))
    assert result["reason"] == "serving_changed" and result["serving_stable"] is False
    assert len(calls) == 3
    _assert_no_authority(result)


@pytest.mark.parametrize("field,value", [
    ("account_scope", "other-scope-canary"), ("account_selector", ["other-account-canary"]),
    ("strategy_profile", "tqqq_growth_income"), ("deployment_selector", "other-deployment-canary"),
])
def test_observed_identity_never_replaces_expected(source, target, field, value):
    observed = {**target, field: value}
    result, calls = _read(source, _revision(observed))
    assert result["reason"] == "runtime_identity_mismatch"
    assert result["identity_matches"][field] is False
    assert result["serving_stable"] is True and len(calls) == 3
    assert "other-" not in json.dumps(result)


@pytest.mark.parametrize("mutation,reason", [
    (lambda r: r["metadata"].update(name=SERVICE + "-wrong"), "revision_identity_mismatch"),
    (lambda r: r["metadata"]["labels"].update({"serving.knative.dev/service": "wrong"}), "revision_identity_mismatch"),
    (lambda r: r["spec"].update(containers=[]), "single_container_required"),
    (lambda r: r["spec"]["containers"].append({}), "single_container_required"),
    (lambda r: r["spec"]["containers"][0]["env"].append({"name": "RUNTIME_TARGET_ENABLED", "value": "true"}), "invalid_environment"),
    (lambda r: _entry(r, "RUNTIME_TARGET_JSON").update(valueFrom={"secretKeyRef": {"name": "secret-private-canary"}}), "invalid_environment"),
    (lambda r: r["spec"]["containers"][0].update(env={}), "invalid_environment"),
])
def test_revision_and_environment_are_unambiguous(source, target, mutation, reason):
    revision = _revision(target)
    mutation(revision)
    result, calls = _read(source, revision)
    assert result["reason"] == reason and len(calls) == 2


@pytest.mark.parametrize("alias", ["same", "only", "empty-primary", "conflict", "secret"])
def test_runtime_aliases_and_secret_references(source, target, alias):
    revision = _revision(target)
    entries = revision["spec"]["containers"][0]["env"]
    alternate = {"name": "QSL_RUNTIME_TARGET_JSON", "value": json.dumps(target, indent=2)}
    if alias == "only":
        entries.remove(_entry(revision, "RUNTIME_TARGET_JSON"))
    elif alias == "empty-primary":
        _entry(revision, "RUNTIME_TARGET_JSON")["value"] = ""
    elif alias == "conflict":
        alternate["value"] = json.dumps({**target, "account_selector": ["other-canary"]})
    elif alias == "secret":
        alternate = {"name": "QSL_RUNTIME_TARGET_JSON", "valueFrom": {"secretKeyRef": {"name": "private-secret-canary"}}}
    entries.append(alternate)
    result, _ = _read(source, revision)
    expected_reason = {"conflict": "runtime_target_alias_conflict", "secret": "runtime_target_secret_reference"}.get(alias, "observed")
    assert result["reason"] == expected_reason


@pytest.mark.parametrize("where", ["source", "inventory", "revision"])
def test_duplicate_json_keys_fail_closed(source, target, where):
    revision = _revision(target)
    raw = json.dumps(target)[:-1] + ', "account_scope": "private-duplicate-canary"}'
    if where == "source":
        source["RUNTIME_TARGET_JSON"] = raw
    elif where == "inventory":
        source.pop("RUNTIME_TARGET_JSON")
        source["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps([{"service": SERVICE, "runtime_target_json": raw}])
    else:
        _entry(revision, "RUNTIME_TARGET_JSON")["value"] = raw
    result, _ = _read(source, revision)
    assert result["reason"] == "invalid_json"


@pytest.mark.parametrize("raw,status,value", [
    (None, "absent", None), ("", "blank", None), ("  ", "blank", None),
    ("false", "explicit", False), ("true", "explicit", True),
    ("0", "explicit", False), ("1", "explicit", True),
    ("FALSE", "explicit", False), ("not-a-bool-canary", "invalid", None),
    (False, "invalid", None), (42, "invalid", None),
    ({"name": "secret-private-canary"}, "secret_reference", None),
])
def test_flag_false_is_not_missing_or_a_default(source, target, raw, status, value):
    revision = _revision(target)
    entry = _entry(revision, "RUNTIME_TARGET_ENABLED")
    if raw is None:
        revision["spec"]["containers"][0]["env"].remove(entry)
    elif isinstance(raw, dict):
        entry.pop("value")
        entry["valueFrom"] = {"secretKeyRef": raw}
    else:
        entry["value"] = raw
    result, _ = _read(source, revision)
    flag = result["declarations"]["RUNTIME_TARGET_ENABLED"]
    assert flag["status"] == status and flag["value"] is value
    assert result["status"] == "verified_metadata"
    _assert_no_authority(result)


def test_policy_numbers_release_and_open_state_not_qualified(source, target):
    observed = copy.deepcopy(target)
    observed["runtime_risk_limits"]["financial_canary"] = 987654.32
    observed["live_continuity"]["state"] = "PAUSED"
    observed["strategy_release"]["strategy_revision"] = "b" * 40
    result, _ = _read(source, _revision(observed))
    assert result["declarations"]["runtime_risk_limits"]["matches_expected"] is False
    assert result["declarations"]["strategy_release"]["matches_expected"] is False
    assert result["declarations"]["live_continuity"]["state"] == "PAUSED"
    assert "987654.32" not in json.dumps(result)
    _assert_no_authority(result)


@pytest.mark.parametrize("field", ["strategy_release", "runtime_risk_limits"])
@pytest.mark.parametrize("value,status", [(None, "absent"), ({}, "invalid"), ("private-invalid-canary", "invalid")])
def test_absent_and_invalid_policies_are_not_success_empty(source, target, field, value, status):
    observed = copy.deepcopy(target)
    observed[field] = value
    result, _ = _read(source, _revision(observed))
    assert result["declarations"][field]["status"] == status
    assert result["declarations"][field]["present"] is False
    _assert_no_authority(result)


def test_alias_policy_boolean_cannot_match_numeric_value(source, target):
    target["runtime_risk_limits"]["financial_canary"] = 1
    source["RUNTIME_TARGET_JSON"] = json.dumps(target)
    alternate = copy.deepcopy(target)
    alternate["runtime_risk_limits"]["financial_canary"] = True
    revision = _revision(target)
    revision["spec"]["containers"][0]["env"].append({"name": "QSL_RUNTIME_TARGET_JSON", "value": json.dumps(alternate)})
    result, _ = _read(source, revision)
    assert result["reason"] == "runtime_target_alias_conflict"


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_financial_declarations_are_invalid(source, target, literal):
    source["RUNTIME_TARGET_JSON"] = json.dumps(target)[:-1] + ', "bad_number": ' + literal + '}'
    result, calls = _read(source, _revision(target))
    assert result["reason"] == "invalid_json" and not calls


@pytest.mark.parametrize("error,reason", [
    (RuntimeError("upstream-private-error-canary"), "metadata_unavailable"),
    (subprocess.TimeoutExpired(["private-command-canary"], 30), "metadata_timeout"),
    (reader._Unavailable("upstream-private-reason-canary"), "metadata_unavailable"),
])
def test_errors_and_timeouts_are_fixed_and_never_retried(source, error, reason, capsys):
    calls = []

    def fail(*args):
        calls.append(args)
        raise error

    result = reader.diagnose_metadata(source, describe=fail)
    assert result["reason"] == reason and len(calls) == 1
    assert "canary" not in json.dumps(result)
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("returncode,stdout,reason", [(0, "{}", None), (1, "private-output-canary", "metadata_read_failed"), (0, "[1]", "invalid_metadata"), (0, "bad-private-json-canary", "invalid_json")])
def test_gcloud_transport_is_bounded_captured_and_metadata_only(monkeypatch, returncode, stdout, reason):
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, returncode, stdout=stdout, stderr="private-stderr-canary")

    monkeypatch.setattr(reader.subprocess, "run", run)
    if reason:
        with pytest.raises(reader._Unavailable, match=reason):
            reader._describe("services", SERVICE, "synthetic-project", "test-region1")
    else:
        assert reader._describe("services", SERVICE, "synthetic-project", "test-region1") == {}
    argv, options = calls[0]
    assert argv[:4] == ["gcloud", "run", "services", "describe"]
    assert options["timeout"] == 30 and options["capture_output"] is True
    assert options["stdin"] == subprocess.DEVNULL and "shell" not in options
    assert options["env"]["CLOUDSDK_CORE_DISABLE_FILE_LOGGING"] == "true"
    assert options["env"]["CLOUDSDK_CORE_LOG_HTTP"] == "false" and len(calls) == 1


def test_masks_private_derived_identifiers_before_describe(source, target, monkeypatch):
    events = []
    monkeypatch.setattr(reader, "_mask", lambda text: events.append(("mask", text)))

    def describe(resource, *_args):
        events.append(("describe", resource))
        return _revision(target) if resource == "revisions" else _service()

    result = reader.diagnose_metadata(source, describe=describe)
    first_read = events.index(("describe", "services"))
    assert ("mask", ACCOUNT) in events[:first_read]
    assert ("mask", SERVICE) in events[:first_read]
    assert result["status"] == "verified_metadata"


def test_mask_escapes_workflow_commands_and_only_runs_in_actions(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    reader._mask("private-local-canary")
    assert capsys.readouterr().out == ""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    reader._mask("private%\r\ncanary")
    assert capsys.readouterr().out == "::add-mask::private%25%0D%0Acanary\n"


@pytest.mark.parametrize("runner_mode", ["false", "true"])
def test_diagnostic_runner_mode_only_changes_mask_commands(source, target, runner_mode, monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_ACTIONS", runner_mode)
    result, calls = _read(source, _revision(target))
    output = capsys.readouterr()
    assert result["status"] == "verified_metadata" and len(calls) == 3
    assert output.err == ""
    if runner_mode == "true":
        lines = output.out.splitlines()
        assert lines and all(line.startswith("::add-mask::") for line in lines)
        assert f"::add-mask::{ACCOUNT}" in lines
        assert f"::add-mask::{REVISION}" in lines
    else:
        assert output.out == ""
    assert ACCOUNT not in json.dumps(result) and REVISION not in json.dumps(result)
    _assert_no_authority(result)


def test_cli_safe_artifact_is_private_create_only_and_no_override(monkeypatch, tmp_path, capsys):
    result = {"schema_version": reader.SCHEMA, "status": "verified_metadata", "reason": "observed"}
    monkeypatch.setattr(reader, "diagnose_metadata", lambda _: result)
    monkeypatch.setattr(reader.sys, "argv", ["diagnose_pilot_metadata"])
    output = tmp_path / "safe.json"
    monkeypatch.setenv("PILOT_METADATA_RESULT_PATH", str(output))
    assert reader.main() == 0
    assert json.loads(output.read_text()) == result
    assert output.stat().st_mode & 0o777 == 0o600
    assert reader.main() == 1
    assert json.loads(output.read_text()) == result
    assert "result_write_failed" in capsys.readouterr().out
    monkeypatch.delenv("PILOT_METADATA_RESULT_PATH")
    monkeypatch.setattr(reader.sys, "argv", ["diagnose_pilot_metadata", "--service=private-override-canary"])
    assert reader.main() == 1
    captured = capsys.readouterr().out
    assert "arguments_not_supported" in captured and "private-override-canary" not in captured


def test_wrapper_masks_original_variable_precedence_before_step_headers():
    wrapper = (ROOT / ".github/workflows/pilot-metadata-readback.yml").read_text()
    protected = (ROOT / ".github/workflows/pilot-metadata-readback-protected.yml").read_text()
    assert "workflow_dispatch:" in wrapper and "inputs:" not in wrapper
    assert "uses: ./.github/workflows/pilot-metadata-readback-protected.yml" in wrapper
    assert "inventory: ${{ vars.CLOUD_RUN_SERVICE_TARGETS_JSON || secrets.CLOUD_RUN_SERVICE_TARGETS_JSON }}" in wrapper
    assert "runtime_target: ${{ vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}" in wrapper
    assert "steps:" not in wrapper and "env:" not in wrapper
    assert "workflow_call:" in protected and "workflow_dispatch:" not in protected
    assert "vars." not in protected
    assert protected.count("    env:") == 1
    assert "CLOUD_RUN_SERVICE_TARGETS_JSON: ${{ secrets.inventory }}" in protected
    assert "RUNTIME_TARGET_JSON: ${{ secrets.runtime_target }}" in protected
    for text in (wrapper, protected):
        assert "github.repository == 'QuantStrategyLab/CharlesSchwabPlatform'" in text
        assert "github.ref == 'refs/heads/main'" in text
        assert "github.run_attempt == 1" in text
    assert "SCHWAB_API_KEY" not in protected and "TELEGRAM" not in protected
    assert "retention-days: 3" in protected
    assert "python -m scripts.diagnose_pilot_metadata" in protected
    assert "python3 -m scripts.diagnose_pilot_metadata --bootstrap-cloud-source" in protected
    assert "workload_identity_provider: ${{ env.GCP_WORKLOAD_IDENTITY_PROVIDER }}" in protected
    assert "service_account: ${{ env.GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT }}" in protected
    assert "project_id: ${{ env.GCP_PROJECT_ID }}" in protected
    assert "projects/" not in protected and ".iam.gserviceaccount.com" not in protected


def test_workflow_has_no_mutating_or_broker_steps():
    for path in ("pilot-metadata-readback.yml", "pilot-metadata-readback-protected.yml"):
        text = (ROOT / ".github/workflows" / path).read_text()
        for forbidden in ("/run", "/reconcile", "/probe", "scheduler jobs", "services update", "storage", "logging read", "secrets versions", "sendMessage", "--publish", "--renew"):
            assert forbidden not in text


_SYNTHETIC_CLOUD_SOURCE = """name: Existing reader
env:
  GCP_PROJECT_ID: source-project
  GCP_WORKLOAD_IDENTITY_PROVIDER: projects/12345/locations/global/workloadIdentityPools/source-pool/providers/source-provider
  GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT: source-reader@source-project.iam.gserviceaccount.com
jobs:
  unrelated:
    env:
      GCP_PROJECT_ID: ignored-project
"""


def test_bootstrap_reads_only_original_fixed_source_masks_before_env_write(monkeypatch, tmp_path, capsys):
    source_file = tmp_path / "original.yml"
    source_file.write_text(_SYNTHETIC_CLOUD_SOURCE)
    env_file = tmp_path / "job.env"
    monkeypatch.setattr(reader, "_CLOUD_SOURCE", source_file)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    monkeypatch.setenv("GCP_PROJECT_ID", "untrusted-override")
    monkeypatch.setenv("PILOT_CLOUD_SOURCE_PATH", "untrusted-path")
    masked = []

    def mask(value):
        assert not env_file.exists()
        masked.append(value)

    monkeypatch.setattr(reader, "_mask", mask)
    assert reader.bootstrap_cloud_source() == 0
    assert len(masked) == 3 and "source-project" in masked
    assert "GCP_PROJECT_ID=source-project\n" in env_file.read_text()
    assert "untrusted" not in env_file.read_text() and "ignored-project" not in env_file.read_text()
    assert capsys.readouterr().out == '{"cloud_metadata_source_ready":true}\n'


@pytest.mark.parametrize("change", [
    "missing", "duplicate-key", "duplicate-top-env", "expression", "wrong-account-project",
    "invalid-provider", "symlink", "not-actions", "missing-env-output",
])
def test_bootstrap_failure_is_fixed_and_does_not_copy_or_guess_cloud_metadata(monkeypatch, tmp_path, capsys, change):
    source_file = tmp_path / "original.yml"
    raw = _SYNTHETIC_CLOUD_SOURCE
    if change == "missing":
        raw = raw.replace("  GCP_PROJECT_ID: source-project\n", "")
    elif change == "duplicate-key":
        raw = raw.replace("env:\n", "env:\n  GCP_PROJECT_ID: private-duplicate-canary\n", 1)
    elif change == "duplicate-top-env":
        raw += "\nenv:\n  GCP_PROJECT_ID: private-duplicate-canary\n"
    elif change == "expression":
        raw = raw.replace("GCP_PROJECT_ID: source-project", "GCP_PROJECT_ID: ${{ vars.private_canary }}")
    elif change == "wrong-account-project":
        raw = raw.replace("source-reader@source-project", "source-reader@private-other-project")
    elif change == "invalid-provider":
        raw = raw.replace("projects/12345/", "private-invalid-provider/")
    source_file.write_text(raw)
    if change == "symlink":
        link = tmp_path / "link.yml"
        link.symlink_to(source_file)
        source_file = link
    env_file = tmp_path / "job.env"
    monkeypatch.setattr(reader, "_CLOUD_SOURCE", source_file)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    if change == "not-actions":
        monkeypatch.delenv("GITHUB_ACTIONS")
    elif change == "missing-env-output":
        monkeypatch.delenv("GITHUB_ENV")
    monkeypatch.setattr(reader, "_mask", lambda _value: None)
    assert reader.bootstrap_cloud_source() == 1 and not env_file.exists()
    output = capsys.readouterr().out
    assert json.loads(output) == {"cloud_metadata_source_ready": False, "reason": "bootstrap_source_unavailable"}
    assert "private" not in output


def test_bootstrap_source_has_no_path_override_or_dependency_loading():
    assert reader._CLOUD_SOURCE == ROOT / ".github/workflows/env-sync-drift-check.yml"
    import inspect

    text = inspect.getsource(reader.bootstrap_cloud_source)
    for forbidden in ("subprocess", "_expected", "_describe", "secretmanager", "getenv(", "os.environ.get(\"PILOT_"):
        assert forbidden not in text
