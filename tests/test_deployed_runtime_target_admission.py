import importlib.util
import json
from pathlib import Path

import pytest


path = Path(__file__).resolve().parents[1] / "scripts" / "verify_deployed_runtime_target_admission.py"
spec = importlib.util.spec_from_file_location("deployed_target_admission", path)
assert spec is not None and spec.loader is not None
admission = importlib.util.module_from_spec(spec)
spec.loader.exec_module(admission)


def payload(target, profile, dry_run="true"):
    return {"spec": {"template": {"spec": {"containers": [{"env": [
        {"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)},
        {"name": "STRATEGY_PROFILE", "value": profile},
        {"name": "SCHWAB_DRY_RUN_ONLY", "value": dry_run},
        {"name": "RUNTIME_TARGET_ENABLED", "value": "true"},
    ]}]}}}}


def target(profile="soxl_soxx_trend_income", dry_run=True):
    return {"platform_id": "schwab", "service_name": "paper-service", "strategy_profile": profile, "execution_mode": "paper" if dry_run else "live", "dry_run_only": dry_run}


def test_admitted_shadow_target_passes():
    result = admission.verify_service(service="paper-service", service_json=payload(target(), "soxl_soxx_trend_income"))
    assert result["profile"] == "soxl_soxx_trend_income"


def test_paper_broker_submission_target_passes():
    configured = target(dry_run=False) | {"execution_mode": "paper"}
    assert admission.verify_service(service="paper-service", service_json=payload(configured, "soxl_soxx_trend_income", "false"))["dry_run_only"] is False


@pytest.mark.parametrize(
    ("configured", "profile", "message"),
    [
        (target(), "different_profile", "STRATEGY_PROFILE does not match"),
        (target() | {"execution_mode": "live"}, "soxl_soxx_trend_income", "dry-run/shadow target"),
        (target("retired_profile"), "retired_profile", "not admitted"),
    ],
)
def test_target_drift_fails_closed(configured, profile, message):
    with pytest.raises(admission.AdmissionError, match=message):
        admission.verify_service(service="paper-service", service_json=payload(configured, profile))


@pytest.mark.parametrize("mismatch", [None, "image", "source"])
def test_no_traffic_readback_projects_container_array_and_checks_identity(tmp_path, monkeypatch, mismatch):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback
    sha, digest = "a" * 40, "sha256:" + "b" * 64
    baseline = {key: "synthetic" for key in ("traffic", "configuration", "iam", "scheduler")}
    before = tmp_path / "before.json"
    before.write_text(json.dumps(baseline))
    args = SimpleNamespace(before=before, project="synthetic", region="synthetic",
                           service="synthetic", expected_sha=sha, expected_image_digest=digest)
    monkeypatch.setattr(readback, "_snapshot", lambda _args: baseline)
    monkeypatch.setattr(readback.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout="synthetic-revision", stderr=""))
    def projected(command):
        result = {"metadata": {"labels": {"commit-sha": "c" * 40 if mismatch == "source" else sha}}}
        # Verified gcloud behavior: missing [] omits the whole containers subtree.
        if "spec.containers[].image" in command[-1]:
            value = "sha256:" + "d" * 64 if mismatch == "image" else digest
            result["spec"] = {"containers": [{"image": "synthetic/image@" + value}]}
        return result
    monkeypatch.setattr(readback, "_run_json", projected)
    if mismatch:
        with pytest.raises(RuntimeError, match="commit SHA" if mismatch == "source" else "image digest"):
            readback._verify(args)
    else:
        readback._verify(args)


def test_no_traffic_capture_keeps_resource_secret_reference_and_iam_arrays(monkeypatch):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback
    service_spec = {"template": {"spec": {"containers": [{
        "resources": {"limits": {"cpu": "1", "memory": "512Mi"}},
        "env": [{"name": "SYNTHETIC_SECRET", "valueFrom": {"secretKeyRef": {"name": "synthetic", "key": "1"}}}],
    }]}}}
    policy = {"bindings": [{"role": "roles/run.invoker", "members": ["serviceAccount:synthetic"]}]}
    def projected(command):
        expression = command[-1]
        assert 'env.value,' not in expression and 'env[].value,' not in expression
        if "get-iam-policy" in command:
            return policy if "bindings[].members" in expression else None
        if "scheduler" in command:
            return []
        result = {"status": {"traffic": [{"revisionName": "synthetic", "percent": 100}]}}
        if all(value in expression for value in ("containers[].resources", "containers[].env[].name", "containers[].env[].valueFrom.secretKeyRef")):
            result["spec"] = service_spec
        return result
    monkeypatch.setattr(readback, "_run_json", projected)
    result = readback._snapshot(SimpleNamespace(service="synthetic", project="synthetic", region="synthetic", scheduler_location="synthetic"))
    assert result["configuration"] == readback._digest(service_spec)
    assert result["iam"] == readback._digest(policy)
