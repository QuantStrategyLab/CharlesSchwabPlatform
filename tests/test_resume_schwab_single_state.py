import copy
import json

import pytest
from quant_platform_kit.common.live_continuity import runtime_target_fingerprint

from scripts import resume_schwab_single_state as switch

REQUEST = "2c953159-63a4-4fc9-8945-c912099cb7e1"


@pytest.fixture
def case(monkeypatch):
    target = {"service_name": switch.SERVICE, "risk": {"synthetic": 1}, "live_continuity": {}}
    target["live_continuity"] = {"state": "ACTIVE_LKG", "baseline_kind": "legacy_authorized",
        "baseline_id": "synthetic-baseline", "captured_at": "2026-10-01",
        "baseline_target_sha256": runtime_target_fingerprint(target)}
    spec = {"containers": [{"image": "registry.invalid/app@sha256:" + "a" * 64,
                            "env": [{"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)},
                                    {"name": "RUNTIME_TARGET_ENABLED", "value": "true"}]}]}
    revision_name = switch.SERVICE + "-synthetic"
    service = {"metadata": {"name": switch.SERVICE, "resourceVersion": "one"},
               "spec": {"template": {"spec": copy.deepcopy(spec)}},
               "status": {"url": "https://synthetic.invalid", "traffic": [{"revisionName": revision_name, "percent": 100}]}}
    revision = {"metadata": {"name": revision_name, "labels": {"serving.knative.dev/service": switch.SERVICE}}, "spec": spec}
    declaration = {"RUNTIME_TARGET_ENABLED": "true"}
    monkeypatch.setattr(switch.metadata, "_expected", lambda _: (switch.SERVICE, switch.PROJECT, switch.REGION, copy.deepcopy(target), declaration))
    monkeypatch.setattr(switch.metadata, "_runtime_target", lambda env, _: json.loads(env["RUNTIME_TARGET_JSON"]["value"]))
    return service, revision, target, declaration


def environment(mode):
    return {"RESUME_OPERATION": mode, "RUNTIME_SWITCH_REQUEST_ID": REQUEST,
            "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1"}


def test_same_enabled_value_reads_back_without_changing_service(case):
    service, revision, _, _ = case
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    def command(args):
        assert args[1:4] == ["scheduler", "jobs", "list"]
        return json.dumps([{"state": "ENABLED", "httpTarget": {"uri": "https://synthetic.invalid/run"}}])
    result = switch.execute(environment("enable"), describe=describe, command=command)
    assert result["status"] == "applied" and result["cloud_write"] == "none"
    assert result["operator_state"] == result["effective_state"] == "enabled"
    assert result["request_id"] == REQUEST and result["run_id"] == "123" and result["operation"] == "enable"


def test_enable_is_partial_when_scheduler_cannot_be_confirmed(case):
    service, revision, _, _ = case
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    result = switch.execute(environment("enable"), describe=describe, command=lambda _: "[]")
    assert result["status"] == "partial" and result["effective_state"] == "enabled"
    assert result["scheduler_state"] == "unknown" and result["cloud_write"] == "none"


def test_inspect_reports_actual_even_when_declaration_differs(case):
    service, revision, _, declaration = case
    declaration["RUNTIME_TARGET_ENABLED"] = "false"
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    result = switch.execute(environment("inspect"), describe=describe, command=lambda _: "[]")
    assert result["operator_state"] == "enabled" and result["declaration_state"] == "disabled"
    assert result["cloud_write"] == "none"


def test_enabled_target_resumes_only_its_paused_run_scheduler(case):
    service, revision, _, _ = case
    calls = []
    job = {"state": "PAUSED", "name": "projects/charlesschwabquant/locations/us-central1/jobs/synthetic",
           "httpTarget": {"uri": "https://synthetic.invalid/run"}}
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    def command(args):
        calls.append(args)
        if args[3] == "list": return json.dumps([job])
        if args[3] == "resume": return ""
        if args[3] == "describe": return json.dumps({**job, "state": "ENABLED"})
        pytest.fail("unexpected command")
    result = switch.execute(environment("enable"), describe=describe, command=command)
    assert result["cloud_write"] == "none" and result["scheduler_write"] == "attempted"
    assert result["scheduler_state"] == "enabled"
    assert [args[3] for args in calls] == ["list", "resume", "describe"]


@pytest.mark.parametrize("state", ["PAUSED", "RECONCILE_ONLY"])
def test_enable_does_not_clear_continuity_or_resume_scheduler(case, state):
    service, revision, target, _ = case
    target["live_continuity"]["state"] = state
    revision["spec"]["containers"][0]["env"][0]["value"] = json.dumps(target)
    service["spec"]["template"]["spec"] = copy.deepcopy(revision["spec"])
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    def command(args):
        assert args[1:4] == ["scheduler", "jobs", "list"]
        return json.dumps([{"state": "PAUSED", "name": "projects/charlesschwabquant/locations/us-central1/jobs/synthetic",
                            "httpTarget": {"uri": "https://synthetic.invalid/run"}}])
    result = switch.execute(environment("enable"), describe=describe, command=command)
    assert result["status"] == "protected" and result["continuity_state"] == state
    assert result["effective_state"] == "disabled" and result["scheduler_state"] == "paused"


def test_disable_only_changes_enabled_then_independent_readback(case):
    service, revision, target, declaration = case
    declaration["RUNTIME_TARGET_ENABLED"] = "false"
    phase, calls = [0], []
    candidate = switch.SERVICE + "-switch-123"
    expected_spec = copy.deepcopy(revision["spec"])
    expected_spec["containers"][0]["env"][1]["value"] = "false"
    def command(args):
        if args[1:4] == ["scheduler", "jobs", "list"]: return "[]"
        calls.append(args)
        phase[0] += 1
        return ""
    def describe(kind, name, *_):
        if kind == "revisions" and name == candidate:
            return {"spec": expected_spec, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        if kind == "revisions": return copy.deepcopy(revision)
        result = copy.deepcopy(service)
        if phase[0]: result["metadata"]["resourceVersion"] = "two"
        if phase[0] == 2: result["status"]["traffic"] = [{"revisionName": candidate, "percent": 100}]
        return result
    result = switch.execute(environment("disable"), describe=describe, command=command)
    assert result["status"] == "applied" and result["effective_state"] == "disabled"
    assert result["cloud_write"] == "attempted" and len(calls) == 2
    assert "--update-env-vars=RUNTIME_TARGET_ENABLED=false" in calls[0]
    assert json.loads(expected_spec["containers"][0]["env"][0]["value"]) == target


def test_unknown_write_is_never_retried(case):
    service, revision, _, declaration = case
    declaration["RUNTIME_TARGET_ENABLED"] = "false"
    calls = []
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    def command(args): calls.append(args); raise TimeoutError()
    with pytest.raises(RuntimeError, match="candidate_write_uncertain_do_not_retry"):
        switch.execute(environment("disable"), describe=describe, command=command)
    assert len(calls) == 1


def test_invalid_frozen_baseline_does_not_prevent_disable(case):
    service, revision, target, declaration = case
    declaration["RUNTIME_TARGET_ENABLED"] = "false"
    target["live_continuity"]["baseline_target_sha256"] = "0" * 64
    revision["spec"]["containers"][0]["env"][0]["value"] = json.dumps(target)
    revision["spec"]["containers"][0]["env"][1]["value"] = "false"
    service["spec"]["template"]["spec"] = copy.deepcopy(revision["spec"])
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    result = switch.execute(environment("disable"), describe=describe, command=lambda _: "[]")
    assert result["status"] == "applied" and result["effective_state"] == "disabled"
    assert result["continuity_state"] == "UNKNOWN" and result["cloud_write"] == "none"


def test_runtime_comparison_ignores_known_server_representation_only():
    first = {"containers": [{"name": "generated", "image": "immutable", "env": [{"name": "B"}, {"name": "A"}]}],
             "servingState": "active", "timeoutSeconds": 300}
    second = {"containers": [{"image": "immutable", "env": [{"name": "A"}, {"name": "B"}]}], "timeoutSeconds": 300}
    assert switch._runtime_spec(first) == switch._runtime_spec(second)
    second["containers"][0]["image"] = "other"
    assert switch._runtime_spec(first) != switch._runtime_spec(second)
