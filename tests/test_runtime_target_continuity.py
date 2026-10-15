import copy
import json

import pytest

from quant_platform_kit.common.live_continuity import runtime_target_fingerprint
from scripts.runtime_target_continuity import verify_target_continuity, verify_traffic_revision


def frozen_target():
    target = {
        "service_name": "unit-service", "strategy_profile": "soxl_soxx_trend_income",
        "execution_mode": "live", "dry_run_only": False,
        "strategy_release": {"strategy_revision": "a" * 40},
        "runtime_risk_limits": {"binding": {"ues_revision": "a" * 40}},
    }
    target["live_continuity"] = {
        "state": "ACTIVE_LKG", "baseline_kind": "legacy_authorized",
        "baseline_id": "synthetic-baseline", "captured_at": "2026-10-01",
        "baseline_target_sha256": runtime_target_fingerprint(target),
    }
    return target


def candidate():
    return {
        "metadata": {"name": "unit-service-00001", "labels": {"commit-sha": "a" * 40}},
        "spec": {"containers": [{
            "startupProbe": {"httpGet": {"path": "/health", "port": 8080}},
            "env": [{"name": "RUNTIME_TARGET_JSON", "value": json.dumps(frozen_target())}],
        }]},
        "status": {"conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "ContainerHealthy", "status": "True"},
        ]},
    }


@pytest.mark.parametrize("path", [
    ("strategy_release", "strategy_revision"), ("runtime_risk_limits", "binding", "ues_revision"),
])
def test_changed_version_reference_rejects_without_rewriting_baseline(path):
    target = frozen_target()
    node = target
    for name in path[:-1]:
        node = node[name]
    node[path[-1]] = "b" * 40
    before = copy.deepcopy(target)
    with pytest.raises(ValueError, match="continuity"):
        verify_target_continuity(target)
    assert target == before

@pytest.mark.parametrize("failure", ["tcp", "stale_fingerprint"])
def test_reconciler_refuses_candidate_before_updating_traffic(monkeypatch, failure):
    from scripts import reconcile_cloud_runtime as reconciler

    revision = candidate()
    if failure == "tcp":
        revision["spec"]["containers"][0]["startupProbe"] = {"tcpSocket": {"port": 8080}}
    else:
        target = frozen_target()
        target["strategy_release"]["strategy_revision"] = "b" * 40
        revision["spec"]["containers"][0]["env"][0]["value"] = json.dumps(target)
    calls = []

    def fake_cloud(args, **kwargs):
        calls.append(args)
        if "update-traffic" in args:
            pytest.fail("unverified candidate reached traffic mutation")
        if "describe" in args and "revisions" in args:
            return revision
        if "describe" in args and "services" in args:
            return {"status": {"traffic": [{"revisionName": "unit-service-old", "percent": 100}]}}
        if "list" in args and "revisions" in args:
            return [revision]
        pytest.fail("unexpected cloud command")

    monkeypatch.setattr(reconciler, "_gcloud_json", fake_cloud)
    monkeypatch.setattr(reconciler, "_gcloud", fake_cloud)
    with pytest.raises(ValueError):
        reconciler.reconcile_traffic({
            "CLOUD_RUN_SERVICE": "unit-service", "GCP_PROJECT_ID": "unit-project",
            "CLOUD_RUN_REGION": "unit-region", "GITHUB_SHA": "a" * 40,
        })
    assert any("revisions" in cmd and "describe" in cmd for cmd in calls)
    assert not any("update-traffic" in cmd for cmd in calls)



def test_continuity_uses_shared_canonical_hash_and_allows_safe_state_change():
    target = frozen_target()
    target["live_continuity"]["state"] = "PAUSED"
    assert verify_target_continuity(target) == runtime_target_fingerprint(target)


def test_target_without_continuity_keeps_runtime_contract():
    target = {"execution_mode": "paper"}
    assert verify_target_continuity(target) == runtime_target_fingerprint(target)
    assert "live_continuity" not in target


def test_healthy_http_candidate_passes():
    verify_traffic_revision(candidate(), expected_revision="unit-service-00001", expected_commit="a" * 40)


@pytest.mark.parametrize("failure", ["tcp", "path", "port", "unhealthy", "not_ready", "commit", "name", "stale_target", "missing_target"])
def test_traffic_gate_rejects_unbootable_or_unverified_revision(failure):
    revision = candidate()
    container = revision["spec"]["containers"][0]
    if failure == "tcp":
        container["startupProbe"] = {"tcpSocket": {"port": 8080}}
    elif failure in {"path", "port"}:
        container["startupProbe"]["httpGet"][failure] = "/run" if failure == "path" else 9090
    elif failure in {"unhealthy", "not_ready"}:
        revision["status"]["conditions"][1 if failure == "unhealthy" else 0]["status"] = "False"
    elif failure == "commit":
        revision["metadata"]["labels"]["commit-sha"] = "b" * 40
    elif failure == "name":
        revision["metadata"]["name"] = "different-revision"
    elif failure == "missing_target":
        container["env"] = []
    else:
        target = frozen_target()
        target["strategy_release"]["strategy_revision"] = "b" * 40
        container["env"][0]["value"] = json.dumps(target)
    with pytest.raises(ValueError):
        verify_traffic_revision(revision, expected_revision="unit-service-00001", expected_commit="a" * 40)


def test_sync_plan_rejects_stale_baseline_before_serializing_target():
    from scripts import build_cloud_run_env_sync_plan as planner

    target = frozen_target()
    target["strategy_release"]["strategy_revision"] = "b" * 40
    before = copy.deepcopy(target)
    with pytest.raises(ValueError, match="continuity"):
        planner._build_target_plan(
            target={"service_name": "unit-service", "runtime_target": target},
            defaults={}, env={}, per_service_mode=True, platform_config={},
            status_rows={"soxl_soxx_trend_income": {"eligible": True, "enabled": True}},
        )
    assert target == before
