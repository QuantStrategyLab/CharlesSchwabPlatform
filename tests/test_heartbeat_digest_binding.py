from copy import deepcopy
import json
from unittest.mock import Mock

import pytest

from scripts import with_heartbeat_digest_binding as binding


TARGET = dict(platform_id="synthetic", strategy_profile="core", service_name="synthetic-service",
              account_scope="synthetic-scope", account_selector=["synthetic-account"],
              deployment_selector="synthetic-deployment", dry_run_only=True,
              scheduler={"main_time": "0 20 * * 1-5", "timezone": "UTC"})
VALUES = dict(DIGEST_OPAQUE_ACCOUNT_UID="synthetic-uid", DIGEST_TARGET_ID="synthetic-target",
              DIGEST_STORE_TARGET="synthetic-slot", DIGEST_CANDIDATES_GCS_URI="gs://synthetic-bucket/candidates")
REVISION = "synthetic-service-00001-test"


def environment():
    return dict(RUNTIME_TARGET_ENABLED="true", RUNTIME_TARGET_JSON=json.dumps(TARGET),
                CLOUD_RUN_SERVICE=TARGET["service_name"], GCP_PROJECT_ID="synthetic-project",
                CLOUD_RUN_REGION="synthetic-region", GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT="synthetic@example.invalid")


def metadata():
    service = dict(metadata={"name": TARGET["service_name"]}, status={
        "latestReadyRevisionName": "synthetic-service-00002-candidate",
        "traffic": [{"revisionName": REVISION, "percent": 100}]})
    values = {**VALUES, "RUNTIME_TARGET_JSON": json.dumps(TARGET), "RUNTIME_TARGET_ENABLED": "true"}
    revision = dict(metadata={"name": REVISION}, spec={"containers": [
        {"env": [{"name": k, "value": v} for k, v in values.items()]}]})
    return service, revision


def reader():
    service, revision = metadata()
    return Mock(side_effect=[service, revision, deepcopy(service)])


def test_missing_binding_uses_serving_revision_and_keeps_values_in_memory(capsys):
    read = reader()
    env = environment()
    result = binding.resolve(env, read)
    assert {k: result[k] for k in VALUES} == VALUES
    assert result["RUNTIME_TARGET_JSON"] == env["RUNTIME_TARGET_JSON"]
    assert read.call_args_list[1].args[0] == ["run", "revisions", "describe", REVISION]
    assert json.loads(result["DIGEST_NOTIFICATION_SCHEDULE_JSON"])["scheduler"] == TARGET["scheduler"]
    assert "DIGEST_TARGET_ID" not in env
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("mutation,reason", [
    ("identity", "digest_runtime_identity_mismatch"),
    ("missing", "digest_identity_owner_input_required"),
    ("secret_reference", "digest_identity_owner_input_required"),
    ("duplicate", "digest_metadata_invalid"),
])
def test_unqualified_metadata_never_supplies_identity(mutation, reason):
    service, revision = metadata()
    rows = revision["spec"]["containers"][0]["env"]
    if mutation == "identity":
        changed = {**TARGET, "account_selector": ["another-synthetic-account"]}
        next(r for r in rows if r["name"] == "RUNTIME_TARGET_JSON")["value"] = json.dumps(changed)
    if mutation in {"missing", "secret_reference"}:
        rows[:] = [r for r in rows if r["name"] != "DIGEST_OPAQUE_ACCOUNT_UID"]
        if mutation == "secret_reference":
            rows.append({"name": "DIGEST_OPAQUE_ACCOUNT_UID", "valueFrom": {"secretKeyRef": {"name": "synthetic-secret"}}})
    if mutation == "duplicate":
        rows.append(rows[0])
    with pytest.raises(binding.BindingError, match=reason):
        binding.resolve(environment(), Mock(side_effect=[service, revision]))


def test_partial_conflict_is_not_overwritten():
    with pytest.raises(binding.BindingError, match="digest_configured_identity_mismatch"):
        binding.resolve({**environment(), "DIGEST_TARGET_ID": "another-target"}, reader())


def test_split_or_changed_traffic_cannot_select_a_binding():
    service, revision = metadata()
    split = deepcopy(service)
    split["status"]["traffic"] = [{"revisionName": REVISION, "percent": 50},
                                  {"revisionName": "synthetic-service-00002-candidate", "percent": 50}]
    with pytest.raises(binding.BindingError, match="digest_serving_revision_ambiguous"):
        binding.resolve(environment(), Mock(return_value=split))
    with pytest.raises(binding.BindingError, match="digest_serving_revision_changed"):
        binding.resolve(environment(), Mock(side_effect=[service, revision, split]))


def test_complete_disabled_and_multi_target_bindings_do_not_read_cloud():
    read = Mock(side_effect=AssertionError("unexpected cloud read"))
    for env in ({**environment(), **VALUES}, {"RUNTIME_TARGET_ENABLED": "false"},
                {"DIGEST_TARGET_BINDINGS_JSON": "[]"}):
        binding.resolve(env, read)
    read.assert_not_called()


def test_schedule_is_not_invented():
    target = {**TARGET, "scheduler": {}}
    with pytest.raises(binding.BindingError, match="digest_schedule_owner_input_required"):
        binding.resolve({**environment(), **VALUES, "RUNTIME_TARGET_JSON": json.dumps(target)})


def test_cloud_reader_has_explicit_account_and_suppresses_provider_errors(monkeypatch):
    run = Mock(return_value=Mock(returncode=1, stdout="private-provider-output", stderr="private-error"))
    monkeypatch.setattr(binding.subprocess, "run", run)
    with pytest.raises(binding.BindingError, match="^digest_metadata_unavailable$"):
        binding.cloud_json(["run", "services", "describe", TARGET["service_name"]], environment())
    args = run.call_args.args[0]
    assert "--account=synthetic@example.invalid" in args
    assert run.call_args.kwargs["capture_output"] is True


def test_blocked_resolution_never_runs_a_command_or_prints_private_data(monkeypatch, capsys):
    monkeypatch.setattr(binding.sys, "argv", ["wrapper", "heartbeat"])
    monkeypatch.setattr(binding, "resolve", Mock(side_effect=binding.BindingError("digest_identity_owner_input_required")))
    run = Mock(side_effect=AssertionError("blocked command must not run"))
    monkeypatch.setattr(binding.subprocess, "run", run)
    assert binding.main() == 1
    assert json.loads(capsys.readouterr().out) == {"status": "blocked", "reason": "digest_identity_owner_input_required"}
    run.assert_not_called()
