import copy
import importlib.util
import json
import os
import subprocess
import textwrap
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "paused_admission", ROOT / "scripts/verify_deployed_runtime_target_admission.py"
)
admission = importlib.util.module_from_spec(spec)
spec.loader.exec_module(admission)


@pytest.fixture
def recovery(monkeypatch):
    from quant_platform_kit.common.live_continuity import runtime_target_fingerprint

    target = {
        "platform_id": "schwab", "service_name": "synthetic-service",
        "strategy_profile": "soxl_soxx_trend_income", "execution_mode": "live",
        "dry_run_only": False, "account_scope": "synthetic-account",
        "strategy_release": {"strategy_revision": "9d1544d1476dd19b1da61b16f37cee7f9126caf4"},
        "runtime_risk_limits": {"binding": {"ues_revision": "9d1544d1476dd19b1da61b16f37cee7f9126caf4"}},
    }
    target["live_continuity"] = {
        "state": "PAUSED", "baseline_kind": "legacy_authorized",
        "baseline_id": "synthetic-baseline", "captured_at": "2026-10-22",
        "baseline_target_sha256": runtime_target_fingerprint(target),
    }
    old = copy.deepcopy(target)
    old["live_continuity"]["baseline_target_sha256"] = "0" * 64
    old["live_continuity"]["state"] = "ACTIVE_LKG"
    service = {
        "metadata": {"name": "synthetic-service", "uid": "synthetic-uid", "generation": 1},
        "spec": {"traffic": [{"latestRevision": True, "percent": 100}], "template": {"spec": {"containers": [{
            "image": "synthetic/image@sha256:" + "b" * 64,
            "startupProbe": {"tcpSocket": {"port": 8080}},
            "env": [
                {"name": "RUNTIME_TARGET_JSON", "value": json.dumps(old)},
                {"name": "STRATEGY_PROFILE", "value": target["strategy_profile"]},
                {"name": "SCHWAB_DRY_RUN_ONLY", "value": "false"},
                {"name": "RUNTIME_TARGET_ENABLED", "value": "true"},
                {"name": "UNCHANGED", "value": "private-sentinel"},
                {"name": "SECRET_REF", "valueFrom": {"secretKeyRef": {"name": "private-ref", "key": "1"}}},
            ],
        }]}}},
        "status": {"observedGeneration": 1, "latestReadyRevisionName": "synthetic-old", "traffic": [{"revisionName": "synthetic-old", "percent": 100, "latestRevision": True}]},
    }
    plan = {"targets": [{"service_name": "synthetic-service", "env": {
        "RUNTIME_TARGET_JSON": json.dumps(target), "UNCHANGED": "private-sentinel",
    }, "remove_env_vars": ["ABSENT"]}]}
    def read_reference(command):
        assert command[:5] == ["gcloud", "run", "revisions", "describe", "synthetic-old"]
        return json.dumps({
            "metadata": {"name": "synthetic-old", "generation": 1, "labels": {
                "serving.knative.dev/service": "synthetic-service",
                "serving.knative.dev/serviceUid": "synthetic-uid",
            }},
            "spec": dict(copy.deepcopy(service["spec"]["template"]["spec"]), containers=[
                dict(copy.deepcopy(service["spec"]["template"]["spec"]["containers"][0]), name="traced-container"),
            ]),
            "status": {"observedGeneration": 1, "conditions": [{"type": "Ready", "status": "True"}]},
        })
    monkeypatch.setattr(admission, "_run", read_reference)
    return service, plan


def project(service, plan):
    return admission.project_paused_env(
        service="synthetic-service", service_json=service, plan=plan, repository=ROOT,
    )


def test_valid_projection_checks_old_target_and_preserves_configuration(recovery):
    service, plan = recovery
    original = copy.deepcopy(service)
    projected, result = project(service, plan)
    assert service == original
    assert result["old_continuity"] == "baseline_mismatch"
    assert result["state"] == "PAUSED"
    assert admission.verify_service(service="synthetic-service", service_json=projected)["enabled"]
    assert projected["spec"]["template"]["spec"]["containers"][0]["startupProbe"] == {"tcpSocket": {"port": 8080}}
    assert projected["spec"]["template"]["spec"]["containers"][0]["env"][-1] == original["spec"]["template"]["spec"]["containers"][0]["env"][-1]


@pytest.mark.parametrize("field", ["strategy_release", "runtime_risk_limits", "account_scope", "service_name", "strategy_profile", "execution_mode", "dry_run_only"])
def test_even_self_consistent_unapproved_target_change_is_rejected(recovery, field):
    from quant_platform_kit.common.live_continuity import runtime_target_fingerprint

    service, plan = recovery
    target = json.loads(plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"])
    target[field] = {"unapproved": True} if isinstance(target[field], dict) else "unapproved"
    target["live_continuity"]["baseline_target_sha256"] = runtime_target_fingerprint(target)
    plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"] = json.dumps(target)
    with pytest.raises(admission.AdmissionError, match="unapproved_target_change"):
        project(service, plan)


@pytest.mark.parametrize("mutation,category", [
    ("old_profile", "STRATEGY_PROFILE"), ("old_invalid_json", "target_json"),
    ("old_invalid_continuity", "continuity_fields"), ("old_missing_continuity", "continuity_fields"),
    ("new_missing_continuity", "paused_required"), ("new_active", "paused_required"),
    ("new_bad_baseline", "continuity"), ("env_change", "unapproved_env_change"),
    ("env_removal", "unapproved_env_change"), ("secret_overwrite", "unapproved_env_change"),
    ("secret_removal", "unapproved_env_change"), ("image_tag", "immutable_image"),
    ("wrong_service", "service_identity"), ("duplicate_target", "plan_selection"),
    ("bad_plan", "plan_selection"), ("duplicate_env", "environment"),
])
def test_recovery_fails_closed_without_sensitive_diagnostics(recovery, mutation, category):
    service, plan = recovery
    container = service["spec"]["template"]["spec"]["containers"][0]
    entries = container["env"]
    target = json.loads(plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"])
    if mutation == "old_profile":
        entries[1]["value"] = "private-sentinel"
    elif mutation == "old_invalid_json":
        entries[0]["value"] = "private-sentinel{"
    elif mutation in {"old_invalid_continuity", "old_missing_continuity"}:
        old = json.loads(entries[0]["value"])
        if mutation == "old_invalid_continuity":
            old["live_continuity"]["state"] = "private-sentinel"
        else:
            del old["live_continuity"]
        entries[0]["value"] = json.dumps(old)
    elif mutation == "new_missing_continuity":
        del target["live_continuity"]
    elif mutation == "new_active":
        target["live_continuity"]["state"] = "ACTIVE_LKG"
    elif mutation == "new_bad_baseline":
        target["live_continuity"]["baseline_target_sha256"] = "0" * 64
    elif mutation == "env_change":
        plan["targets"][0]["env"]["UNCHANGED"] = "changed"
    elif mutation in {"env_removal", "secret_removal"}:
        plan["targets"][0]["remove_env_vars"] = ["UNCHANGED" if mutation == "env_removal" else "SECRET_REF"]
        plan["targets"][0]["env"].pop("UNCHANGED", None)
    elif mutation == "secret_overwrite":
        plan["targets"][0]["env"]["SECRET_REF"] = "private-sentinel"
    elif mutation == "image_tag":
        container["image"] = "synthetic/image:mutable"
    elif mutation == "wrong_service":
        service["metadata"]["name"] = "private-sentinel"
    elif mutation == "duplicate_target":
        plan["targets"].append(copy.deepcopy(plan["targets"][0]))
    elif mutation == "bad_plan":
        plan["targets"] = "private-sentinel"
    elif mutation == "duplicate_env":
        entries.append(copy.deepcopy(entries[0]))
    if isinstance(plan["targets"], list):
        plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"] = json.dumps(target)
    with pytest.raises(admission.AdmissionError, match=category) as caught:
        project(service, plan)
    assert "private-sentinel" not in str(caught.value)
    assert "private-ref" not in str(caught.value)
    assert "synthetic-account" not in str(caught.value)


def test_old_pin_is_checked_even_when_continuity_is_stale(recovery, monkeypatch):
    service, plan = recovery
    def reject(**kwargs):
        raise admission.AdmissionError("old pin rejected")
    monkeypatch.setattr(admission, "verify_production_ues_pin", reject)
    with pytest.raises(admission.AdmissionError, match="old pin rejected"):
        project(service, plan)


def test_image_and_traffic_path_still_reject_old_continuity(recovery):
    service, _plan = recovery
    with pytest.raises(admission.AdmissionError, match="continuity"):
        admission.verify_service(service="synthetic-service", service_json=service)


def test_workflow_keeps_recovery_separate_from_general_sync():
    workflow = (ROOT / ".github/workflows/sync-cloud-run-env.yml").read_text()
    assert "paused_env_recovery:" in workflow
    assert "--paused-env-only" in workflow
    assert "--check-before-apply" in workflow
    assert "--verify-paused-revision" in workflow
    recovery_block = workflow.split("      - name: Apply PAUSED env-only recovery", 1)[1].split("      - name:", 1)[0]
    assert "--no-traffic" in recovery_block
    for forbidden in ("--image", "--startup-probe", "--update-secrets", "--remove-secrets", "update-traffic"):
        assert forbidden not in recovery_block
    assert "env_sync_enabled == 'true' && inputs.paused_env_recovery != true" in workflow


@pytest.mark.parametrize("approval", [
    "APPROVE_ENV_SECRET_SYNC", "APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT",
    "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP",
])
def test_recovery_requires_exclusive_env_authorization(monkeypatch, approval):
    monkeypatch.setenv("APPROVE_ENV_SECRET_SYNC", "true")
    for name in ("APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP"):
        monkeypatch.setenv(name, "false")
    admission._require_env_only()
    monkeypatch.setenv(approval, "false" if approval == "APPROVE_ENV_SECRET_SYNC" else "true")
    with pytest.raises(admission.AdmissionError, match="operation_scope"):
        admission._require_env_only()


@pytest.mark.parametrize("mutation", ["unchanged", "plan", "current", "traffic", "service_metadata", "template_metadata", "pending_generation"])
def test_capture_and_immediate_preapply_compare_the_admitted_snapshot(recovery, tmp_path, monkeypatch, mutation):
    from types import SimpleNamespace

    service, plan = recovery
    monkeypatch.setenv("APPROVE_ENV_SECRET_SYNC", "true")
    monkeypatch.setenv("SYNC_PLAN_JSON", json.dumps(plan))
    for name in ("APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP"):
        monkeypatch.setenv(name, "false")
    args = SimpleNamespace(
        service="synthetic-service", project="synthetic", region="synthetic",
        no_traffic=True, baseline_file=tmp_path / "baseline.json",
        verify_paused_revision=None, check_before_apply=False,
    )
    admission._paused_operation(args, service)
    assert args.baseline_file.stat().st_mode & 0o777 == 0o600
    assert "private-sentinel" not in args.baseline_file.read_text()
    args.check_before_apply = True
    if mutation == "plan":
        target = json.loads(plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"])
        target["live_continuity"]["baseline_id"] = "different-baseline"
        plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"] = json.dumps(target)
        monkeypatch.setenv("SYNC_PLAN_JSON", json.dumps(plan))
    elif mutation == "current":
        service["spec"]["template"]["spec"]["timeoutSeconds"] = 123
    elif mutation == "traffic":
        service["status"]["traffic"][0]["revisionName"] = "different-old"
    elif mutation == "service_metadata":
        service["metadata"]["annotations"] = {"run.googleapis.com/secrets": "private-alias:private-other-secret"}
    elif mutation == "template_metadata":
        service["spec"]["template"]["metadata"] = {"annotations": {"run.googleapis.com/secrets": "private-alias:private-other-secret"}}
    elif mutation == "pending_generation":
        service["metadata"]["generation"] = 2
    if mutation == "unchanged":
        admission._paused_operation(args, service)
    else:
        with pytest.raises(admission.AdmissionError, match="baseline_changed|traffic_readback|reconciliation_pending"):
            admission._paused_operation(args, service)


@pytest.mark.parametrize("mutation,category", [
    (None, None), ("image", "revision_projection"), ("resolved_image", "image_readback"),
    ("target", "revision_projection"), ("risk", "revision_projection"),
    ("traffic", "traffic_readback"), ("revision", "revision_identity"),
    ("not_ready", "revision_readiness"), ("service_spec", "revision_projection"),
    ("service_metadata", "service_identity"), ("template_metadata", "revision_projection"),
    ("revision_metadata", "revision_projection"), ("pending_generation", "reconciliation_pending"),
    ("pending_traffic", "traffic_readback"),
    ("system_metadata", None), ("unnamed_candidate", None), ("wrong_uid", "revision_identity"),
    ("missing_uid", "revision_identity"), ("wrong_owner", "revision_identity"),
    ("service_uid_missing", "revision_identity"),
    ("untraced_name", "revision_projection"), ("multi_container", "revision_projection"),
    ("dependencies", "revision_projection"), ("depends_on", "revision_projection"),
    ("explicit_name", "revision_projection"), ("explicit_name_equal", None),
    ("missing_provenance", "revision_projection"), ("nonce", "revision_projection"),
    ("env", "revision_projection"), ("secret", "revision_projection"),
    ("continuity_baseline", "revision_projection"), ("probe", "revision_projection"),
    ("unknown_metadata", "revision_projection"),
    ("bad_template_uid", "revision_identity"),
])
def test_exact_revision_readback(recovery, tmp_path, monkeypatch, capsys, mutation, category):
    from types import SimpleNamespace

    service, plan = recovery
    if mutation in {"explicit_name", "explicit_name_equal"}:
        service["spec"]["template"]["spec"]["containers"][0]["name"] = "explicit-container"
    projected, _result = project(service, plan)
    args = SimpleNamespace(
        service="synthetic-service", project="synthetic", region="synthetic", no_traffic=True,
        baseline_file=tmp_path / "baseline.json", verify_paused_revision=None, check_before_apply=False,
    )
    monkeypatch.setenv("APPROVE_ENV_SECRET_SYNC", "true")
    monkeypatch.setenv("SYNC_PLAN_JSON", json.dumps(plan))
    for name in ("APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP"):
        monkeypatch.setenv(name, "false")
    admission._paused_operation(args, service)
    args.verify_paused_revision = "synthetic-service-paused-123-1"
    revision = {
        "metadata": {"name": args.verify_paused_revision, "generation": 1, "labels": {
            "serving.knative.dev/service": "synthetic-service",
            "serving.knative.dev/serviceUid": "synthetic-uid",
        }},
        "spec": copy.deepcopy(projected["spec"]["template"]["spec"]),
        "status": {"observedGeneration": 1, "conditions": [{"type": "Ready", "status": "True"}], "imageDigest": "sha256:" + "b" * 64},
    }
    container = revision["spec"]["containers"][0]
    if mutation != "unnamed_candidate" and "name" not in container:
        container["name"] = "traced-container"
    if mutation == "system_metadata":
        # A validated serviceUid may also appear on the template.
        projected["spec"]["template"]["metadata"] = {"labels": {"serving.knative.dev/serviceUid": "synthetic-uid"}}
    elif mutation == "bad_template_uid":
        projected["spec"]["template"]["metadata"] = {"labels": {"serving.knative.dev/serviceUid": "private-other-uid"}}
    elif mutation == "wrong_uid":
        revision["metadata"]["labels"]["serving.knative.dev/serviceUid"] = "private-other-uid"
    elif mutation == "missing_uid":
        del revision["metadata"]["labels"]["serving.knative.dev/serviceUid"]
    elif mutation == "wrong_owner":
        revision["metadata"]["labels"]["serving.knative.dev/service"] = "private-other-service"
    elif mutation == "service_uid_missing":
        del projected["metadata"]["uid"]
    elif mutation == "untraced_name":
        container["name"] = "private-untraced-container"
    elif mutation == "multi_container":
        revision["spec"]["containers"].append(copy.deepcopy(container))
    elif mutation == "dependencies":
        revision["metadata"]["annotations"] = {"run.googleapis.com/container-dependencies": "{}"}
    elif mutation == "depends_on":
        container["dependsOn"] = []
    elif mutation == "explicit_name":
        container["name"] = "changed-container"
    elif mutation == "missing_provenance":
        baseline = json.loads(args.baseline_file.read_text())
        baseline.pop("container_name_provenance", None)
        args.baseline_file.write_text(json.dumps(baseline))
    elif mutation == "nonce":
        revision["metadata"]["labels"]["client.knative.dev/nonce"] = "private-nonce"
    elif mutation == "env":
        container["env"][4]["value"] = "private-changed"
    elif mutation == "secret":
        container["env"][5]["valueFrom"]["secretKeyRef"]["key"] = "2"
    elif mutation == "continuity_baseline":
        target = json.loads(container["env"][0]["value"])
        target["live_continuity"]["baseline_target_sha256"] = "1" * 64
        container["env"][0]["value"] = json.dumps(target)
    elif mutation == "probe":
        container["startupProbe"]["tcpSocket"]["port"] = 9999
    elif mutation == "unknown_metadata":
        revision["metadata"]["labels"]["unknown.example/field"] = "private-value"
    elif mutation == "image":
        revision["spec"]["containers"][0]["image"] = "synthetic/image@sha256:" + "c" * 64
    elif mutation == "resolved_image":
        revision["status"]["imageDigest"] = "sha256:" + "c" * 64
    elif mutation in {"target", "risk"}:
        entry = revision["spec"]["containers"][0]["env"][0]
        target = json.loads(entry["value"])
        if mutation == "target":
            target["live_continuity"]["state"] = "ACTIVE_LKG"
        else:
            target["runtime_risk_limits"]["binding"]["ues_revision"] = "c" * 40
        entry["value"] = json.dumps(target)
    elif mutation == "traffic":
        projected["status"]["traffic"][0]["revisionName"] = args.verify_paused_revision
    elif mutation == "revision":
        revision["metadata"]["name"] = "different-revision"
    elif mutation == "not_ready":
        revision["status"]["conditions"] = []
    elif mutation == "service_spec":
        projected["spec"]["template"]["spec"]["timeoutSeconds"] = 123
    elif mutation == "service_metadata":
        projected["metadata"]["annotations"] = {"run.googleapis.com/secrets": "private-alias:private-other-secret"}
    elif mutation == "template_metadata":
        projected["spec"]["template"]["metadata"] = {"annotations": {"run.googleapis.com/secrets": "private-alias:private-other-secret"}}
    elif mutation == "revision_metadata":
        revision["metadata"]["annotations"] = {"run.googleapis.com/secrets": "private-alias:private-other-secret"}
    elif mutation == "pending_generation":
        revision["metadata"]["generation"] = 2
    elif mutation == "pending_traffic":
        projected["spec"]["traffic"] = [{"revisionName": args.verify_paused_revision, "percent": 100}]
    else:
        projected["status"]["traffic"][0]["latestRevision"] = False
        projected["spec"]["traffic"] = [{"revisionName": "synthetic-old", "percent": 100}]
        projected["status"]["latestReadyRevisionName"] = args.verify_paused_revision
        projected["status"]["traffic"].append({"revisionName": args.verify_paused_revision, "percent": 0})
    calls = []
    def read(command):
        calls.append(command)
        return json.dumps(revision)
    monkeypatch.setattr(admission, "_run", read)
    if category:
        with pytest.raises(admission.AdmissionError, match=category):
            admission._paused_operation(args, projected)
    else:
        admission._paused_operation(args, projected)
        assert "traffic_unchanged=true" in capsys.readouterr().out
    assert calls[0][0:4] == ["gcloud", "run", "revisions", "describe"]
    assert calls[0][4] == args.verify_paused_revision


def test_cloud_read_errors_never_echo_provider_response(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(admission.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        returncode=1, stdout="private-sentinel", stderr="private-ref",
    ))
    with pytest.raises(admission.AdmissionError, match="cloud_read") as caught:
        admission._run(["gcloud", "run", "services", "describe", "synthetic-service"])
    assert "private" not in str(caught.value)


def test_cli_invalid_input_error_is_sanitized(recovery, monkeypatch, capsys):
    service, _plan = recovery
    monkeypatch.setattr(admission, "_describe_service", lambda **kwargs: service)
    monkeypatch.setattr(admission.sys, "argv", [
        "admission", "--project=synthetic", "--region=synthetic", "--service=synthetic-service",
        "--paused-env-only", "--no-traffic", "--baseline-file=synthetic-file",
    ])
    monkeypatch.setenv("APPROVE_ENV_SECRET_SYNC", "true")
    monkeypatch.setenv("SYNC_PLAN_JSON", "private-sentinel{")
    for name in ("APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP"):
        monkeypatch.setenv(name, "false")
    assert admission.main() == 1
    error = capsys.readouterr().err
    assert "invalid_input" in error
    assert "private-sentinel" not in error


@pytest.mark.parametrize("approval", [None, "APPROVE_ENV_SECRET_SYNC", "APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP", "NOTIFICATION_ONLY"])
def test_real_workflow_config_rejects_combined_approvals(tmp_path, approval):
    workflow = (ROOT / ".github/workflows/sync-cloud-run-env.yml").read_text()
    config_step = workflow.split("      - name: Check whether Cloud Run automation is enabled", 1)[1].split("      - name:", 1)[0]
    script = textwrap.dedent(config_step.split("        run: |\n", 1)[1])
    env = {
        "PATH": os.defpath, "GITHUB_OUTPUT": str(tmp_path / "output"),
        "PAUSED_ENV_RECOVERY": "true", "ENABLE_GITHUB_ENV_SYNC": "true",
        "APPROVE_ENV_SECRET_SYNC": "true", "APPROVE_IMAGE_DEPLOY": "false",
        "APPROVE_TRAFFIC_SHIFT": "false", "APPROVE_SCHEDULER_IAM_SYNC": "false",
        "APPROVE_CLEANUP": "false", "NOTIFICATION_ONLY": "false",
        "ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION": "false", "GITHUB_EVENT_NAME": "workflow_dispatch",
    }
    if approval:
        env[approval] = "false" if approval == "APPROVE_ENV_SECRET_SYNC" else "true"
    result = subprocess.run(["bash", "-c", script], env=env, text=True, capture_output=True)
    assert result.returncode == (1 if approval else 0), result.stderr
    if approval is None:
        outputs = (tmp_path / "output").read_text()
        assert "env_sync_enabled=true" in outputs
        for key in ("deploy_enabled", "traffic_shift_enabled", "scheduler_iam_sync_enabled", "cleanup_enabled"):
            assert key + "=false" in outputs


@pytest.mark.parametrize("kind", ["valid_old", "disabled", "delimiter", "missing_no_traffic"])
def test_recovery_boundaries(recovery, tmp_path, monkeypatch, kind):
    from types import SimpleNamespace

    service, plan = recovery
    if kind == "valid_old":
        entries = service["spec"]["template"]["spec"]["containers"][0]["env"]
        entries[0]["value"] = plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"]
        assert project(service, plan)[1]["old_continuity"] == "valid"
    elif kind == "disabled":
        service["spec"]["template"]["spec"]["containers"][0]["env"][3]["value"] = "false"
        with pytest.raises(admission.AdmissionError, match="operation_scope"):
            project(service, plan)
    elif kind == "delimiter":
        plan["targets"][0]["env"]["UNCHANGED"] = "private-sentinel|BREAK=true"
        with pytest.raises(admission.AdmissionError, match="environment"):
            project(service, plan)
    else:
        monkeypatch.setenv("APPROVE_ENV_SECRET_SYNC", "true")
        for name in ("APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP"):
            monkeypatch.setenv(name, "false")
        with pytest.raises(admission.AdmissionError, match="operation_scope"):
            admission._paused_operation(SimpleNamespace(no_traffic=False, baseline_file=tmp_path / "baseline.json"), service)


def test_numeric_false_cannot_change_protected_target_fingerprint(recovery):
    from quant_platform_kit.common.live_continuity import runtime_target_fingerprint

    service, plan = recovery
    target = json.loads(plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"])
    target["dry_run_only"] = 0
    target["live_continuity"]["baseline_target_sha256"] = runtime_target_fingerprint(target)
    plan["targets"][0]["env"]["RUNTIME_TARGET_JSON"] = json.dumps(target)
    with pytest.raises(admission.AdmissionError, match="unapproved_target_change"):
        project(service, plan)


@pytest.mark.parametrize("location", ["service", "template", "revision"])
def test_protected_secret_alias_metadata_changes_fingerprint(recovery, location):
    service, _plan = recovery
    changed = copy.deepcopy(service)
    metadata = changed["metadata"] if location == "service" else changed["spec"]["template"].setdefault("metadata", {})
    metadata["annotations"] = {"run.googleapis.com/secrets": "private-alias:private-other-secret"}
    digest = admission._service_identity_digest if location == "service" else admission._spec_digest
    assert digest(service) != digest(changed)


@pytest.mark.parametrize("mutation", ["pending_generation", "desired_route", "unresolved_latest"])
def test_pending_or_different_desired_traffic_is_rejected(recovery, mutation):
    service, _plan = recovery
    if mutation == "pending_generation":
        service["metadata"]["generation"] = 2
    elif mutation == "desired_route":
        service["spec"]["traffic"] = [{"revisionName": "synthetic-unapproved", "percent": 100}]
    else:
        service["status"].pop("latestReadyRevisionName")
    with pytest.raises(admission.AdmissionError, match="reconciliation_pending|traffic_readback"):
        admission._traffic(service)


@pytest.mark.parametrize("mutation,category", [
    ("valid", None), ("explicit_template", None), ("template_dependencies", None),
    ("service_dependencies", None), ("template_depends_on", None),
    ("wrong_uid", "revision_identity"), ("wrong_service", "revision_identity"),
    ("wrong_revision", "revision_identity"), ("unresolved", "revision_identity"),
    ("pending", "reconciliation_pending"),
    ("wrong_image", None), ("source_dependencies", None), ("source_depends_on", None),
    ("multi_source", None), ("missing_name", None), ("invalid_name", None),
    ("not_ready", None),
])
def test_container_name_source_is_owned_and_unambiguous(recovery, monkeypatch, mutation, category):
    from types import SimpleNamespace

    service, _plan = recovery
    args = SimpleNamespace(project="synthetic", region="synthetic")
    original = copy.deepcopy(service)
    read_reference = admission._run
    calls = []

    def read(command):
        calls.append(command)
        source = json.loads(read_reference(command))
        container = source["spec"]["containers"][0]
        if mutation == "wrong_uid":
            source["metadata"]["labels"]["serving.knative.dev/serviceUid"] = "private-other-uid"
        elif mutation == "wrong_service":
            source["metadata"]["labels"]["serving.knative.dev/service"] = "private-other-service"
        elif mutation == "wrong_revision":
            source["metadata"]["name"] = "private-other-revision"
        elif mutation == "pending":
            source["status"]["observedGeneration"] = 0
        elif mutation == "wrong_image":
            container["image"] = "synthetic/image@sha256:" + "c" * 64
        elif mutation == "source_dependencies":
            source["metadata"]["annotations"] = {"run.googleapis.com/container-dependencies": "{}"}
        elif mutation == "source_depends_on":
            container["dependsOn"] = []
        elif mutation == "multi_source":
            source["spec"]["containers"].append(copy.deepcopy(container))
        elif mutation == "missing_name":
            del container["name"]
        elif mutation == "invalid_name":
            container["name"] = ""
        elif mutation == "not_ready":
            source["status"]["conditions"] = []
        return json.dumps(source)

    monkeypatch.setattr(admission, "_run", read)
    template = service["spec"]["template"]
    if mutation == "explicit_template":
        template["spec"]["containers"][0]["name"] = "explicit-name"
    elif mutation == "template_dependencies":
        template["metadata"] = {"annotations": {"run.googleapis.com/container-dependencies": "{}"}}
    elif mutation == "service_dependencies":
        service["metadata"]["annotations"] = {"run.googleapis.com/container-dependencies": "{}"}
    elif mutation == "template_depends_on":
        template["spec"]["containers"][0]["dependsOn"] = []
    elif mutation == "unresolved":
        service["status"].pop("latestReadyRevisionName")
    if category:
        with pytest.raises(admission.AdmissionError, match=category) as caught:
            admission._container_name_provenance(args, service)
        assert "private-" not in str(caught.value)
    else:
        result = admission._container_name_provenance(args, service)
        if mutation == "valid":
            assert result == {"source_revision": "synthetic-old", "name_sha256": admission._digest("traced-container")}
            assert service == original
        else:
            assert result is None
    if mutation in {"explicit_template", "template_dependencies", "service_dependencies", "template_depends_on", "unresolved"}:
        assert not calls
    else:
        assert len(calls) == 1
        assert calls[0][-3:] == ["--project=synthetic", "--region=synthetic", "--format=json"]


@pytest.mark.parametrize("change", ["equal", "add", "remove", "change"])
def test_nonce_is_always_protected_without_historical_evidence(recovery, change):
    service, _plan = recovery
    original = copy.deepcopy(service)
    metadata = service["spec"]["template"].setdefault("metadata", {})
    metadata["labels"] = {"client.knative.dev/nonce": "synthetic-nonce"}
    other = copy.deepcopy(service)
    if change == "add":
        other = original
    elif change == "remove":
        del other["spec"]["template"]["metadata"]["labels"]["client.knative.dev/nonce"]
    elif change == "change":
        other["spec"]["template"]["metadata"]["labels"]["client.knative.dev/nonce"] = "changed-nonce"
    assert (admission._paused_spec_digest(service) == admission._paused_spec_digest(other)) == (change == "equal")


def test_paused_uid_normalization_does_not_change_shared_comparison(recovery):
    service, _plan = recovery
    other = copy.deepcopy(service)
    other["spec"]["template"]["metadata"] = {"labels": {"serving.knative.dev/serviceUid": "synthetic-uid"}}
    assert admission._spec_digest(service) != admission._spec_digest(other)
    assert admission._paused_spec_digest(service) == admission._paused_spec_digest(other)



def test_preapply_does_not_rewrite_baseline_when_name_source_changes(recovery, tmp_path, monkeypatch):
    from types import SimpleNamespace

    service, plan = recovery
    args = SimpleNamespace(
        service="synthetic-service", project="synthetic", region="synthetic", no_traffic=True,
        baseline_file=tmp_path / "baseline.json", verify_paused_revision=None, check_before_apply=False,
    )
    monkeypatch.setenv("APPROVE_ENV_SECRET_SYNC", "true")
    monkeypatch.setenv("SYNC_PLAN_JSON", json.dumps(plan))
    for name in ("APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP"):
        monkeypatch.setenv(name, "false")
    admission._paused_operation(args, service)
    original_baseline = args.baseline_file.read_bytes()
    baseline = json.loads(original_baseline)
    assert baseline["container_name_provenance"]["source_revision"] == "synthetic-old"
    assert "traced-container" not in original_baseline.decode()
    read_reference = admission._run

    def changed(command):
        source = json.loads(read_reference(command))
        source["spec"]["containers"][0]["name"] = "different-source-name"
        return json.dumps(source)

    monkeypatch.setattr(admission, "_run", changed)
    args.check_before_apply = True
    with pytest.raises(admission.AdmissionError, match="baseline_changed"):
        admission._paused_operation(args, service)
    assert args.baseline_file.read_bytes() == original_baseline
