import copy
import json

import pytest
from quant_platform_kit.common.live_continuity import runtime_target_fingerprint

from scripts import resume_schwab_single_state as resume


@pytest.fixture
def case(monkeypatch):
    target = {"service_name": resume.SERVICE, "risk": {"synthetic": 1}, "live_continuity": {}}
    target["live_continuity"] = {"state": "PAUSED", "baseline_kind": "legacy_authorized",
        "baseline_id": "synthetic-baseline", "captured_at": "2026-10-01",
        "baseline_target_sha256": runtime_target_fingerprint(target)}
    spec = {"containers": [{"image": "registry.invalid/app@" + resume.IMAGE,
                            "env": [{"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)}]}]}
    service = {"metadata": {"name": resume.SERVICE, "resourceVersion": "one"},
               "spec": {"template": {"spec": copy.deepcopy(spec)}},
               "status": {"traffic": [{"revisionName": resume.REVISION, "percent": 100}]}}
    revision = {"metadata": {"name": resume.REVISION, "labels": {"serving.knative.dev/service": resume.SERVICE}},
                "spec": spec}
    monkeypatch.setattr(resume.metadata, "_expected", lambda _: (resume.SERVICE, resume.PROJECT, resume.REGION, copy.deepcopy(target), {}))
    monkeypatch.setattr(resume.metadata, "_runtime_target", lambda env, _: json.loads(env["RUNTIME_TARGET_JSON"]["value"]))
    return service, revision, target


def test_inspect_is_read_only_and_detects_template_drift(case):
    service, revision, _ = case
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    def command(args):
        assert args[1:4] == ["scheduler", "jobs", "list"]
        return "[]"
    result = resume.execute({"RESUME_OPERATION": "inspect"}, describe=describe, command=command)
    assert result["status"] == "ready_for_review"
    service["spec"]["template"]["spec"]["containers"][0]["image"] = "different"
    with pytest.raises(ValueError, match="template_differs"):
        resume.execute({"RESUME_OPERATION": "inspect"}, describe=describe)


def test_apply_writes_only_state_then_adopts_and_reads_back(case, monkeypatch):
    service, revision, target = case
    declared = copy.deepcopy(target)
    declared["live_continuity"]["state"] = "ACTIVE_LKG"
    monkeypatch.setattr(resume.metadata, "_expected", lambda _: (resume.SERVICE, resume.PROJECT, resume.REGION, declared, {}))
    state = {"phase": "before", "calls": []}
    candidate = resume.SERVICE + "-resume-123"
    active_spec = copy.deepcopy(revision["spec"])
    active_spec["containers"][0]["env"][0]["value"] = json.dumps(declared, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    def command(args):
        state["calls"].append(args)
        state["phase"] = "staged" if len(state["calls"]) == 1 else "adopted"
    def describe(kind, name, *_):
        if kind == "revisions" and name == candidate:
            return {"spec": active_spec, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        if kind == "revisions": return copy.deepcopy(revision)
        result = copy.deepcopy(service)
        if state["phase"] != "before": result["metadata"]["resourceVersion"] = "two"
        if state["phase"] == "adopted": result["status"]["traffic"] = [{"revisionName": candidate, "percent": 100}]
        return result
    result = resume.execute({"RESUME_OPERATION": "apply", "GITHUB_RUN_ID": "123"}, describe=describe, command=command)
    assert result["status"] == "active_readback" and not result["natural_cycle_observed"]
    assert len(state["calls"]) == 2
    assert "--no-traffic" in state["calls"][0]
    update = next(x for x in state["calls"][0] if x.startswith("--update-env-vars="))
    assert json.loads(update.split("RUNTIME_TARGET_JSON=", 1)[1]) == declared


def test_uncertain_stage_never_adopts(case, monkeypatch):
    service, revision, target = case
    declared = copy.deepcopy(target)
    declared["live_continuity"]["state"] = "ACTIVE_LKG"
    monkeypatch.setattr(resume.metadata, "_expected", lambda _: (resume.SERVICE, resume.PROJECT, resume.REGION, declared, {}))
    calls = []
    def describe(kind, *_): return copy.deepcopy(service if kind == "services" else revision)
    def command(args): calls.append(args); raise TimeoutError()
    with pytest.raises(RuntimeError, match="candidate_write_uncertain_do_not_retry"):
        resume.execute({"RESUME_OPERATION": "apply", "GITHUB_RUN_ID": "123"}, describe=describe, command=command)
    assert len(calls) == 1
