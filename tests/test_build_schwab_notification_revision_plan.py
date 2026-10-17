"""Handwritten synthetic snapshots; no real cloud, environment or account data."""

import builtins
import copy
import importlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/build_schwab_notification_revision_plan.py"
BASE = "demo-service-00781-base"
DESIRED = "demo-service-00715-old"
CANDIDATE = "demo-service-00782-review"
OLD_IMAGE = "registry.invalid/demo/app@sha256:" + "a" * 64
NEW_IMAGE = "registry.invalid/demo/app@sha256:" + "b" * 64
SENTINEL = "PRIVATE_SENTINEL_NEVER_PRINT_42"
RAW_TARGET = ('{ "platform_id":"schwab", "service_name":"demo-service",\r\n'
              '"account_id":"fictional-account", "strategy_profile":"fictional", '
              '"dry_run_only":false, "execution_mode":"live", '
              '"runtime_risk_limits":{"max_order_value":17}, '
              '"strategy_release":{"strategy_revision":"frozen-fiction"}, '
              '"live_continuity":{"signature":"never-re-sign"}, "note":"虚构" }')
REAL_POPEN = subprocess.Popen


@pytest.fixture(autouse=True)
def no_external_activity(monkeypatch):
    calls = []

    def denied(*args, **kwargs):
        calls.append("external-activity")
        raise AssertionError("External activity forbidden")

    for module, names in ((socket, ("socket", "create_connection", "getaddrinfo")),
                          (subprocess, ("Popen", "run")), (os, ("system", "posix_spawn"))):
        for name in names:
            monkeypatch.setattr(module, name, denied)
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] in {"main", "application", "schwab", "notifications", "strategy_registry",
                                 "strategy_runtime", "strategy_loader", "quant_platform_kit", "us_equity_strategies"}:
            calls.append("business-import")
            raise AssertionError("Business import forbidden")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    yield calls
    assert calls == []


def planner():
    return importlib.import_module("scripts.build_schwab_notification_revision_plan")


def labels(purpose):
    return {"commit-sha": "c" * 40, "run-id": "fictional-run", "purpose": purpose}


def snapshot():
    identity = {"project": "demo-project", "region": "demo-region", "service": "demo-service"}
    return {
        "service": {
            "identity": dict(identity),
            "metadata": {"name": "demo-service", "uid": "fictional-service-uid",
                         "resourceVersion": "opaque/RV+781==", "labels": labels("service")},
            "provenance": {"commit": "c" * 40, "run": "fictional-service-run"},
            "traffic": [{"revisionName": BASE, "percent": 90, "tag": "stable"},
                        {"revisionName": "demo-service-00780-other", "percent": 10},
                        {"revisionName": DESIRED, "percent": 0, "tag": "old-preview"}],
        },
        "revision": {
            "identity": identity | {"revision": BASE},
            "metadata": {"name": BASE, "uid": "fictional-revision-uid", "labels": labels("baseline")},
            "provenance": {"commit": "c" * 40, "run": "fictional-baseline-run"},
            "runtime": {
                "serviceAccountName": "fictional@demo-project.iam.gserviceaccount.com",
                "ingress": "internal", "executionEnvironment": "gen2", "containerConcurrency": 1,
                "timeoutSeconds": 300, "scaling": {"minInstanceCount": 0, "maxInstanceCount": 1},
                "network": {"vpcAccessConnector": "fictional-connector", "egress": "private-ranges-only"},
                "volumes": [{"name": "auth", "secret": {"secretName": "fictional-volume-secret",
                             "items": [{"key": "3", "path": "token"}], "defaultMode": 256}},
                            {"name": "scratch", "emptyDir": {"medium": "Memory", "sizeLimit": "8Mi"}}],
                "containers": [{
                    "name": "app", "image": OLD_IMAGE, "ports": [{"name": "http1", "containerPort": 8080}],
                    "command": [], "args": [],
                    "resources": {"limits": {"cpu": "1", "memory": "512Mi"},
                                  "cpuIdle": True, "startupCpuBoost": False},
                    "startupProbe": {"httpGet": {"path": "/health", "port": 8080}, "timeoutSeconds": 5,
                                     "periodSeconds": 10, "failureThreshold": 24},
                    "livenessProbe": {"httpGet": {"path": "/health", "port": 8080}},
                    "volumeMounts": [{"name": "auth", "mountPath": "/auth", "readOnly": True},
                                     {"name": "scratch", "mountPath": "/tmp"}],
                    "env": [{"name": "RUNTIME_TARGET_JSON", "value": RAW_TARGET},
                            {"name": "STRATEGY_PROFILE", "value": "fictional"},
                            {"name": "RUNTIME_TARGET_ENABLED", "value": "true"},
                            {"name": "SCHWAB_DRY_RUN_ONLY", "value": "false"},
                            {"name": "SCHWAB_CASH_CURRENCY", "value": "USD"},
                            {"name": "EXECUTION_REPORT_GCS_URI", "value": "gs://fictional/reports"},
                            {"name": "FICTIONAL_SECRET", "valueFrom": {
                                "secretKeyRef": {"name": "fictional-env-secret", "key": "7"}}}],
                }, {
                    "name": "sidecar", "image": OLD_IMAGE, "ports": [], "command": [], "args": [],
                    "resources": {"limits": {"cpu": "100m", "memory": "64Mi"}},
                    "env": [], "volumeMounts": [],
                }],
            },
        },
    }


@pytest.fixture
def case():
    before = snapshot()
    desired = snapshot()["revision"]
    desired["identity"]["revision"] = DESIRED
    desired["metadata"]["name"] = DESIRED
    desired["runtime"]["containers"][0]["env"] = [{"name": "UNKNOWN_DESIRED_LITERAL", "value": SENTINEL}]
    return {
        "model": "schwab-local-review-v1",
        "expected": {"project": "demo-project", "region": "demo-region", "service": "demo-service",
                     "baseline_revision": BASE, "candidate_revision": CANDIDATE,
                     "candidate_image": NEW_IMAGE, "resource_version": "opaque/RV+781=="},
        "before": before, "current": copy.deepcopy(before), "excluded_desired_template": desired,
    }


def main_container(payload):
    return payload["revision"]["runtime"]["containers"][0]


def test_only_image_and_revision_change_from_00781_not_desired_00715(case):
    frozen = copy.deepcopy(case)
    candidate, review = planner().build_plan(case)
    expected = copy.deepcopy(case["before"])
    expected["revision"]["identity"]["revision"] = CANDIDATE
    expected["revision"]["metadata"]["name"] = CANDIDATE
    main_container(expected)["image"] = NEW_IMAGE
    assert candidate == expected
    assert candidate["service"] == case["current"]["service"]
    assert main_container(candidate)["env"][0]["value"].encode() == RAW_TARGET.encode()
    assert case == frozen
    assert review["mode"] == "review_only"
    assert review["deployment_ready"] is False


def test_independent_current_version_is_compared_with_expected_and_before(case):
    case["current"]["service"]["metadata"]["resourceVersion"] = "concurrent-version"
    with pytest.raises(planner().PlanError, match="^RESOURCE_VERSION_MISMATCH$"):
        planner().build_plan(case)


def test_baseline_tcp_guard_cannot_be_repaired_by_constructor(case):
    for side in ("before", "current"):
        main_container(case[side])["startupProbe"] = {"tcpSocket": {"port": 8080}}
    with pytest.raises(planner().PlanError, match="^HTTP_STARTUP_GUARD_REQUIRED$"):
        planner().build_plan(case)


def stop(case, reason):
    frozen = copy.deepcopy(case)
    with pytest.raises(planner().PlanError) as caught:
        planner().build_plan(case)
    assert str(caught.value) == reason
    assert SENTINEL not in str(caught.value)
    assert case == frozen


def test_deterministic_redacted_review_and_detached_candidate(case):
    candidate, review = planner().build_plan(case)
    again, review_again = planner().build_plan(copy.deepcopy(case))
    assert candidate == again
    encoded = json.dumps(review, sort_keys=True, separators=(",", ":"))
    assert encoded == json.dumps(review_again, sort_keys=True, separators=(",", ":"))
    assert review["old_installation"] == "UNKNOWN"
    assert review["startup_side_effects"] == "UNVERIFIED"
    assert review["source"] == review["admission"] == "NOT_VERIFIED"
    assert {row["path"] for row in review["changes"]} == {
        "revision.identity.revision", "revision.metadata.name", "revision.runtime.containers[0].image"}
    for value in (RAW_TARGET, SENTINEL, "fictional-env-secret", "fictional-volume-secret", NEW_IMAGE,
                  "fictional-service-uid", "opaque/RV+781==", "fictional@demo-project", "never-re-sign"):
        assert value not in encoded
    main_container(candidate)["env"].reverse()
    candidate["service"]["metadata"]["labels"]["purpose"] = "changed-detached-object"
    assert case["before"] == case["current"]
    assert main_container(again)["env"][0]["value"] == RAW_TARGET


def test_all_positive_traffic_and_zero_tags_remain_in_original_order(case):
    for side in ("before", "current"):
        case[side]["service"]["traffic"].append({"revisionName": BASE, "percent": 0, "tag": "second-tag"})
    candidate, _ = planner().build_plan(case)
    assert candidate["service"]["traffic"] == case["before"]["service"]["traffic"]
    assert [row["percent"] for row in candidate["service"]["traffic"]] == [90, 10, 0, 0]
    assert all(row["revisionName"] != CANDIDATE for row in candidate["service"]["traffic"])


@pytest.mark.parametrize("side", ["before", "current"])
@pytest.mark.parametrize("part,field", [("service", "project"), ("service", "region"), ("service", "service"),
                                       ("revision", "project"), ("revision", "region"), ("revision", "service")])
def test_explicit_ownership_is_required(case, side, part, field):
    case[side][part]["identity"][field] = "wrong-identity"
    stop(case, "IDENTITY_MISMATCH")


@pytest.mark.parametrize("side", ["before", "current"])
@pytest.mark.parametrize("revision", [DESIRED, "latest", "latest-created", "latest-ready", ""])
def test_explicit_selected_baseline_must_match(case, side, revision):
    case[side]["revision"]["identity"]["revision"] = revision
    stop(case, "BASELINE_IDENTITY_MISMATCH")


@pytest.mark.parametrize("field,value,reason", [
    ("project", "", "INVALID_EXPECTED_IDENTITY"),
    ("region", "Wrong", "INVALID_EXPECTED_IDENTITY"),
    ("service", "wrong-service", "INVALID_EXPECTED_REVISION"),
    ("baseline_revision", "latest", "INVALID_EXPECTED_REVISION"),
    ("baseline_revision", "latest-ready", "INVALID_EXPECTED_REVISION"),
    ("baseline_revision", "latest-created", "INVALID_EXPECTED_REVISION"),
    ("baseline_revision", "", "INVALID_EXPECTED_REVISION"),
    ("baseline_revision", DESIRED, "BASELINE_IDENTITY_MISMATCH"),
    ("candidate_revision", BASE, "AMBIGUOUS_REVISION_IDENTITY"),
    ("candidate_revision", "latest", "INVALID_EXPECTED_REVISION"),
    ("candidate_revision", "demo-service-latest", "INVALID_EXPECTED_REVISION"),
    ("candidate_revision", "other-service-00782", "INVALID_EXPECTED_REVISION"),
    ("candidate_revision", "demo-service-invalid_", "INVALID_EXPECTED_REVISION"),
    ("candidate_revision", "demo-service-" + "x" * 60, "INVALID_EXPECTED_REVISION"),
    ("candidate_image", OLD_IMAGE, "IMAGE_UNCHANGED"),
    ("candidate_image", "registry.invalid/demo/app:latest", "INVALID_IMMUTABLE_IMAGE"),
    ("candidate_image", "registry.invalid/demo/app@sha256:" + "A" * 64, "INVALID_IMMUTABLE_IMAGE"),
    ("candidate_image", NEW_IMAGE + "suffix", "INVALID_IMMUTABLE_IMAGE"),
    ("candidate_image", "sha256:" + "b" * 64, "INVALID_IMMUTABLE_IMAGE"),
    ("resource_version", "wrong-version", "RESOURCE_VERSION_MISMATCH"),
    ("resource_version", "", "INVALID_RESOURCE_VERSION"),
    ("resource_version", "  ", "INVALID_RESOURCE_VERSION"),
    ("resource_version", None, "INVALID_STRUCTURE"),
    ("resource_version", 781, "INVALID_STRUCTURE"),
    ("candidate_revision", None, "INVALID_STRUCTURE"),
])
def test_invalid_explicit_expectations_stop(case, field, value, reason):
    case["expected"][field] = value
    stop(case, reason)


@pytest.mark.parametrize("field", ["project", "region", "service", "baseline_revision", "candidate_revision",
                                  "candidate_image", "resource_version"])
def test_expectations_have_no_defaults(case, field):
    del case["expected"][field]
    stop(case, "MISSING_FIELD")


@pytest.mark.parametrize("side", ["before", "current"])
@pytest.mark.parametrize("value,reason", [("", "INVALID_RESOURCE_VERSION"), ("  ", "INVALID_RESOURCE_VERSION"),
                                        (None, "INVALID_STRUCTURE"), (781, "INVALID_STRUCTURE"),
                                        (False, "INVALID_STRUCTURE"), ("opaque/RV+782==", "RESOURCE_VERSION_MISMATCH")])
def test_resource_versions_are_independent_opaque_strings(case, side, value, reason):
    case[side]["service"]["metadata"]["resourceVersion"] = value
    stop(case, reason)


@pytest.mark.parametrize("side", ["before", "current"])
def test_missing_snapshot_version_is_not_equality(case, side):
    del case[side]["service"]["metadata"]["resourceVersion"]
    stop(case, "MISSING_FIELD")


def test_before_version_not_replaced_by_matching_current_and_expected(case):
    case["expected"]["resource_version"] = "new-version"
    case["current"]["service"]["metadata"]["resourceVersion"] = "new-version"
    stop(case, "RESOURCE_VERSION_MISMATCH")


def test_in_memory_alias_is_not_an_independent_current_snapshot(case):
    case["current"] = case["before"]
    stop(case, "INDEPENDENT_SNAPSHOTS_REQUIRED")


@pytest.mark.parametrize("part", ["service", "revision"])
def test_recreated_objects_are_rejected_even_if_version_matches(case, part):
    case["current"][part]["metadata"]["uid"] = "replacement-uid"
    stop(case, "OBJECT_RECREATED")


@pytest.mark.parametrize("part", ["service", "revision"])
@pytest.mark.parametrize("field", ["name", "uid"])
def test_invalid_object_identity_is_not_ignored(case, part, field):
    case["current"][part]["metadata"][field] = ""
    stop(case, "INVALID_UID" if field == "uid" else "IDENTITY_MISMATCH")


def set_path(value, path, replacement):
    for key in path[:-1]:
        value = value[key]
    value[path[-1]] = replacement


PROTECTED_CHANGES = [
    (("service", "metadata", "labels", "commit-sha"), "d" * 40),
    (("service", "provenance", "run"), "another-run"),
    (("revision", "metadata", "labels", "purpose"), "another-purpose"),
    (("revision", "provenance", "commit"), "d" * 40),
    (("revision", "runtime", "serviceAccountName"), "other@example.invalid"),
    (("revision", "runtime", "ingress"), "all"),
    (("revision", "runtime", "executionEnvironment"), "gen1"),
    (("revision", "runtime", "containerConcurrency"), 2),
    (("revision", "runtime", "timeoutSeconds"), 301),
    (("revision", "runtime", "scaling", "maxInstanceCount"), 2),
    (("revision", "runtime", "network", "vpcAccessConnector"), "other-connector"),
    (("revision", "runtime", "containers", 0, "resources", "limits", "cpu"), "2"),
    (("revision", "runtime", "containers", 0, "resources", "limits", "memory"), "1Gi"),
    (("revision", "runtime", "containers", 0, "command"), ["python"]),
    (("revision", "runtime", "containers", 0, "args"), ["--fictional"]),
    (("revision", "runtime", "containers", 0, "startupProbe", "failureThreshold"), 25),
    (("revision", "runtime", "containers", 0, "env", 0, "value"), RAW_TARGET + " "),
    (("revision", "runtime", "containers", 0, "env", 2, "value"), "false"),
    (("revision", "runtime", "containers", 0, "env", 3, "value"), "true"),
    (("revision", "runtime", "containers", 0, "env", 6, "valueFrom", "secretKeyRef", "key"), "8"),
    (("revision", "runtime", "volumes", 0, "secret", "items", 0, "key"), "4"),
    (("revision", "runtime", "containers", 0, "volumeMounts", 0, "readOnly"), False),
    (("revision", "runtime", "containers", 1, "image"), NEW_IMAGE),
]


@pytest.mark.parametrize("path,value", PROTECTED_CHANGES)
def test_concurrent_protected_configuration_changes_stop(case, path, value):
    set_path(case["current"], path, value)
    stop(case, "CONCURRENT_SNAPSHOT_CHANGED")


@pytest.mark.parametrize("path,value", PROTECTED_CHANGES)
def test_candidate_cannot_change_protected_configuration(case, path, value):
    candidate, _ = planner().build_plan(case)
    set_path(candidate, path, value)
    with pytest.raises(planner().PlanError, match="^PROTECTED_CONFIGURATION_CHANGED$"):
        planner().review_candidate(case["before"], candidate, case["expected"])


@pytest.mark.parametrize("mutation", ["traffic-order", "traffic-tag", "traffic-split", "env-order", "container-count"])
def test_ordered_and_routing_changes_during_snapshot_window_stop(case, mutation):
    current = case["current"]
    if mutation == "traffic-order": current["service"]["traffic"].reverse()
    elif mutation == "traffic-tag": current["service"]["traffic"][-1]["tag"] = "another-tag"
    elif mutation == "traffic-split":
        current["service"]["traffic"][0]["percent"] = 80
        current["service"]["traffic"][1]["percent"] = 20
    elif mutation == "env-order": main_container(current)["env"].reverse()
    elif mutation == "container-count": current["revision"]["runtime"]["containers"].pop()
    stop(case, "CONCURRENT_SNAPSHOT_CHANGED")


@pytest.mark.parametrize("mutation,reason", [
    ("latest", "INVALID_TRAFFIC"), ("latest-field", "UNKNOWN_FIELD"),
    ("missing-revision", "MISSING_FIELD"), ("missing-percent", "MISSING_FIELD"),
    ("duplicate-route", "AMBIGUOUS_TRAFFIC"), ("duplicate-tag", "AMBIGUOUS_TRAFFIC"),
    ("candidate-zero", "CANDIDATE_ROUTE_PRESENT"), ("candidate-positive", "CANDIDATE_ROUTE_PRESENT"),
    ("candidate-tag", "CANDIDATE_ROUTE_PRESENT"), ("boolean-percent", "INVALID_STRUCTURE"),
    ("negative", "INVALID_TRAFFIC"), ("wrong-total", "BASELINE_NOT_SERVING"),
    ("baseline-zero", "BASELINE_NOT_SERVING"), ("unresolved", "INVALID_TRAFFIC"),
])
def test_unsafe_traffic_never_passes_even_identically(case, mutation, reason):
    for side in ("before", "current"):
        rows = case[side]["service"]["traffic"]
        if mutation == "latest": rows[0]["revisionName"] = "latest"
        elif mutation == "latest-field": rows[0]["latestRevision"] = True
        elif mutation == "missing-revision": del rows[0]["revisionName"]
        elif mutation == "missing-percent": del rows[0]["percent"]
        elif mutation == "duplicate-route": rows.append(copy.deepcopy(rows[-1]))
        elif mutation == "duplicate-tag": rows[1]["tag"] = rows[0]["tag"]
        elif mutation.startswith("candidate-"):
            rows.append({"revisionName": CANDIDATE, "percent": 1 if mutation == "candidate-positive" else 0})
            if mutation == "candidate-tag": rows[-1]["tag"] = "forbidden-tag"
        elif mutation == "boolean-percent": rows[0]["percent"] = True
        elif mutation == "negative": rows[0]["percent"] = -1
        elif mutation == "wrong-total": rows[0]["percent"] = 89
        elif mutation == "baseline-zero": rows[0]["percent"], rows[1]["percent"] = 0, 100
        elif mutation == "unresolved": rows[0]["revisionName"] = ""
    stop(case, reason)


@pytest.mark.parametrize("side", ["before", "current", "candidate"])
@pytest.mark.parametrize("mutation,reason", [
    ("missing", "HTTP_STARTUP_GUARD_REQUIRED"), ("tcp", "HTTP_STARTUP_GUARD_REQUIRED"),
    ("path", "HTTP_STARTUP_GUARD_REQUIRED"), ("port", "HTTP_STARTUP_GUARD_REQUIRED"),
    ("port-type", "INVALID_STRUCTURE"), ("bool-port", "INVALID_STRUCTURE"),
    ("missing-http-port", "MISSING_FIELD"), ("empty", "INVALID_PROBE"),
    ("negative-period", "INVALID_PROBE"),
])
def test_baseline_and_candidate_share_strict_a_probe_guard(case, side, mutation, reason):
    candidate, _ = planner().build_plan(case)
    payload = candidate if side == "candidate" else case[side]
    container = main_container(payload)
    probe = container["startupProbe"]
    if mutation == "missing": del container["startupProbe"]
    elif mutation == "tcp": container["startupProbe"] = {"tcpSocket": {"port": 8080}}
    elif mutation == "path": probe["httpGet"]["path"] = "/other"
    elif mutation == "port": probe["httpGet"]["port"] = 8081
    elif mutation == "port-type": probe["httpGet"]["port"] = "8080"
    elif mutation == "bool-port": probe["httpGet"]["port"] = True
    elif mutation == "missing-http-port": del probe["httpGet"]["port"]
    elif mutation == "empty": container["startupProbe"] = {}
    elif mutation == "negative-period": probe["periodSeconds"] = -1
    with pytest.raises(planner().PlanError) as caught:
        if side == "candidate": planner().review_candidate(case["before"], candidate, case["expected"])
        else: planner().build_plan(case)
    assert str(caught.value) == reason


@pytest.mark.parametrize("path", [
    (), ("expected",), ("before",), ("before", "service"), ("before", "service", "metadata"),
    ("before", "service", "metadata", "labels"), ("before", "revision", "provenance"),
    ("before", "revision", "runtime"), ("before", "revision", "runtime", "network"),
    ("before", "revision", "runtime", "containers", 0),
    ("before", "revision", "runtime", "containers", 0, "resources"),
    ("before", "revision", "runtime", "containers", 0, "startupProbe", "httpGet"),
    ("before", "revision", "runtime", "containers", 0, "env", 6, "valueFrom", "secretKeyRef"),
    ("before", "revision", "runtime", "volumes", 0, "secret"),
])
def test_unknown_structured_fields_stop(case, path):
    node = case
    for key in path:
        node = node[key]
    node["unknown_field"] = SENTINEL
    stop(case, "UNKNOWN_FIELD")


class LiteralTrap(dict):
    def __getitem__(self, key):
        if key == "value":
            raise AssertionError("Literal was read before inventory finished")
        return super().__getitem__(key)

    def __deepcopy__(self, memo):
        raise AssertionError("Uninventoried literal was copied")


@pytest.mark.parametrize("side", ["before", "current"])
def test_unknown_literal_stops_before_any_allowed_literal_access(case, side):
    for snapshot_side in ("before", "current"):
        entries = main_container(case[snapshot_side])["env"]
        entries[0] = LiteralTrap(entries[0])
    main_container(case[side])["env"].append(LiteralTrap(name="UNKNOWN_LITERAL", value=SENTINEL))
    with pytest.raises(planner().PlanError, match="^UNKNOWN_ENV_LITERAL$"):
        planner().build_plan(case)


class DesiredTrap(dict):
    def __getitem__(self, key):
        if key == "excluded_desired_template":
            raise AssertionError("Excluded desired template was accessed")
        return super().__getitem__(key)


def test_desired_template_value_is_never_accessed_or_copied(case):
    request = DesiredTrap(case)
    candidate, review = planner().build_plan(request)
    assert main_container(candidate)["env"][0]["value"] == RAW_TARGET
    assert review["excluded_desired_template"] == "NOT_ACCESSED"


def test_pure_functions_read_no_files_environment_or_full_a_preflight(case, monkeypatch):
    module = planner()
    calls = []

    def denied(*args, **kwargs):
        calls.append("impure-call")
        raise AssertionError("Pure function accessed external state")

    class EnvironmentTrap(dict):
        __getitem__ = get = __iter__ = __contains__ = denied

    with monkeypatch.context() as scoped:
        scoped.setattr(builtins, "open", denied)
        scoped.setattr(io, "open", denied)
        scoped.setattr(os, "getenv", denied)
        scoped.setattr(os, "environ", EnvironmentTrap())
        for name in ("_preflight_local", "_run_json", "_dependency_projection", "_runtime_values"):
            scoped.setattr(module.a, name, denied)
        candidate, review = module.build_plan(case)
        assert module.review_candidate(case["before"], candidate, case["expected"]) == review
    assert calls == []


@pytest.mark.parametrize("field,value", [
    ("account_id", "other-fictional-account"), ("runtime_risk_limits", {"max_order_value": 18}),
    ("strategy_release", {"strategy_revision": "other-frozen-fiction"}),
    ("live_continuity", {"signature": "different-frozen-signature"}), ("dry_run_only", True),
])
def test_encoded_risk_and_frozen_target_drift_is_not_normalized_or_resigned(case, field, value):
    changed = json.loads(RAW_TARGET)
    changed[field] = value
    main_container(case["current"])["env"][0]["value"] = json.dumps(changed)
    stop(case, "CONCURRENT_SNAPSHOT_CHANGED")


def test_even_semantically_equal_target_reformatting_is_protected(case):
    main_container(case["current"])["env"][0]["value"] = json.dumps(json.loads(RAW_TARGET), sort_keys=True)
    stop(case, "CONCURRENT_SNAPSHOT_CHANGED")


@pytest.mark.parametrize("field,value", [("image", OLD_IMAGE), ("image", "app:mutable")])
def test_review_checks_actual_candidate_image(case, field, value):
    candidate, _ = planner().build_plan(case)
    main_container(candidate)[field] = value
    reason = "CANDIDATE_IMAGE_MISMATCH" if value == OLD_IMAGE else "INVALID_IMMUTABLE_IMAGE"
    with pytest.raises(planner().PlanError, match="^" + reason + "$"):
        planner().review_candidate(case["before"], candidate, case["expected"])


def test_review_refuses_candidate_zero_percent_tag(case):
    candidate, _ = planner().build_plan(case)
    candidate["service"]["traffic"].append({"revisionName": CANDIDATE, "percent": 0, "tag": "no-routing"})
    with pytest.raises(planner().PlanError, match="^CANDIDATE_ROUTE_PRESENT$"):
        planner().review_candidate(case["before"], candidate, case["expected"])


@pytest.mark.parametrize("mutation,reason", [
    ("non-string-literal", "INVALID_ENV_LITERAL"), ("duplicate-env", "DUPLICATE_ENV_NAME"),
    ("ambiguous-env", "AMBIGUOUS_ENV_KIND"), ("unknown-env", "UNKNOWN_ENV_LITERAL"),
    ("mutable-sidecar", "INVALID_IMMUTABLE_IMAGE"), ("wrong-resource", "INVALID_RESOURCE_LITERAL"),
    ("missing-target", "AMBIGUOUS_OR_MISSING_TARGET"), ("missing-protection", "REQUIRED_LITERAL_MISSING"),
    ("bad-secret", "INVALID_SECRET_REFERENCE"),
])
def test_identically_invalid_supported_configuration_stops(case, mutation, reason):
    for side in ("before", "current"):
        container = main_container(case[side])
        if mutation == "non-string-literal": container["env"][0]["value"] = {"secret": SENTINEL}
        elif mutation == "duplicate-env": container["env"].append(copy.deepcopy(container["env"][0]))
        elif mutation == "ambiguous-env": container["env"][0]["valueFrom"] = {"secretKeyRef": {"name": "fictional", "key": "1"}}
        elif mutation == "unknown-env": container["env"].append({"name": "UNKNOWN", "value": SENTINEL})
        elif mutation == "mutable-sidecar": case[side]["revision"]["runtime"]["containers"][1]["image"] = "app:latest"
        elif mutation == "wrong-resource": container["resources"]["limits"]["cpu"] = "0"
        elif mutation == "missing-target": container["env"].pop(0)
        elif mutation == "missing-protection": container["env"].pop(2)
        elif mutation == "bad-secret": container["env"][-1]["valueFrom"]["secretKeyRef"]["key"] = "bad-version"
    stop(case, reason)


# The guard lives in the actual CLI interpreter, before product import. It also
# constrains reads/import origins, not merely a grep or a module-name blacklist.
CHILD_GUARD = r'''
import builtins
import io
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys

script, input_file, mode = sys.argv[1:4]
script = Path(script).resolve()
input_file = Path(input_file).resolve()
events = []
stdlib = Path(sys.base_prefix).resolve()
allowed_files = {script, script.with_name("verify_cloud_run_no_traffic_deploy.py"), input_file}

def deny(kind):
    events.append(kind)
    raise AssertionError("LOCAL_GUARD_" + kind)

def audit(event, args):
    if event.startswith("socket."):
        deny("network")
    if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.spawn", "os.exec", "pty.spawn"}:
        deny("command")
    if event in {"os.mkdir", "os.remove", "os.rmdir", "os.rename", "os.link", "os.symlink", "os.chmod", "os.truncate"}:
        deny("write")
    if event == "open":
        path, open_mode, flags = args
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            deny("write")
        if isinstance(path, (str, bytes, os.PathLike)):
            path = Path(os.fsdecode(path)).resolve()
            cached_a = (path.parent == script.parent / "__pycache__"
                        and path.name.startswith("verify_cloud_run_no_traffic_deploy."))
            if path not in allowed_files and not path.is_relative_to(stdlib) and not cached_a:
                deny("unapproved-read")

original_import = builtins.__import__
def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    # Relative imports within the standard library retain their package root.
    root = (globals or {}).get("__package__", "").split(".")[0] if level else name.split(".")[0]
    if root not in sys.stdlib_module_names | {"verify_cloud_run_no_traffic_deploy"}:
        deny("business-import")
    return original_import(name, globals, locals, fromlist, level)

def no_network(*args, **kwargs):
    deny("network")
def no_command(*args, **kwargs):
    deny("command")

sys.addaudithook(audit)
builtins.__import__ = guarded_import
socket.socket = socket.create_connection = socket.getaddrinfo = no_network
subprocess.Popen = subprocess.run = no_command
os.system = no_command
sys.path.insert(0, str(script.parent))
try:
    if mode == "network":
        socket.create_connection(("example.invalid", 443))
    elif mode == "command":
        subprocess.run(["gcloud", "run", "services", "replace", "forbidden"])
    elif mode == "write":
        Path("forbidden-output").write_text("forbidden")
    elif mode == "business-import":
        __import__("application")
    elif mode == "unapproved-read":
        Path("unapproved-private-file").read_text()
    else:
        sys.argv = [str(script)] + (["--input", str(input_file)] if mode != "no-args" else [])
        runpy.run_path(str(script), run_name="__main__")
finally:
    sys.stderr.write("LOCAL_GUARD_EVENTS=" + repr(events) + "\n")
'''


def python_process(command, **kwargs):
    """Parent driver exception: only this Python interpreter, never a shell."""
    assert command[0] == sys.executable
    process = REAL_POPEN(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **kwargs)
    stdout, stderr = process.communicate(timeout=20)
    return SimpleNamespace(returncode=process.returncode, stdout=stdout, stderr=stderr)


def cli(tmp_path, case=None, *, raw=None, mode="cli"):
    directory = tmp_path / "cli"
    directory.mkdir()
    input_file = directory / "explicit-input.json"
    input_file.write_text(raw if raw is not None else json.dumps(case), encoding="utf-8")
    fake_bin = directory / "fake-bin"
    fake_bin.mkdir()
    call_log = directory / "fake-calls"
    for name in ("gcloud", "docker", "gh", "curl"):
        fake = fake_bin / name
        fake.write_text("#!" + sys.executable + "\nfrom pathlib import Path\n"
                        + "with Path(" + repr(str(call_log)) + ").open('a') as log: log.write('" + name + "\\n')\n"
                        + "raise SystemExit(97)\n", encoding="utf-8")
        fake.chmod(0o700)
    guard = directory / "guard.py"
    guard.write_text(CHILD_GUARD, encoding="utf-8")
    before_files = {str(path.relative_to(directory)): path.read_bytes()
                    for path in directory.rglob("*") if path.is_file()}
    completed = python_process(
        [sys.executable, str(guard), str(SCRIPT), str(input_file), mode], cwd=directory,
        env={"PATH": str(fake_bin), "PYTHONDONTWRITEBYTECODE": "1", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
             "PYTHONPATH": str(ROOT)},
    )
    after_files = {str(path.relative_to(directory)): path.read_bytes()
                   for path in directory.rglob("*") if path.is_file()}
    assert before_files == after_files
    assert not call_log.exists()
    return completed


def test_real_cli_has_no_side_effects_or_complete_service_output(tmp_path, case):
    result = cli(tmp_path, case)
    assert result.returncode == 0, result.stderr
    assert result.stderr == "LOCAL_GUARD_EVENTS=[]\n"
    review = json.loads(result.stdout)
    assert review == planner().build_plan(case)[1]
    assert result.stdout == json.dumps(review, sort_keys=True, separators=(",", ":")) + "\n"
    for forbidden in (SENTINEL, RAW_TARGET, "fictional-env-secret", "fictional-volume-secret", "runtime_risk_limits",
                      '"containers"', '"spec"', '"metadata"', '"traffic"', "apiVersion", "kind: Service"):
        assert forbidden not in result.stdout + result.stderr


@pytest.mark.parametrize("mode", ["network", "command", "write", "business-import", "unapproved-read"])
def test_child_guard_is_effective_when_actively_challenged(tmp_path, case, mode):
    result = cli(tmp_path, case, mode=mode)
    assert result.returncode != 0
    assert "LOCAL_GUARD_EVENTS=['" + mode + "']" in result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("raw,reason", [
    ('{"model":"first","model":"' + SENTINEL + '"}', "DUPLICATE_JSON_KEY"),
    ('{"nested":{"same":1,"same":"' + SENTINEL + '"}}', "DUPLICATE_JSON_KEY"),
    ('{"value":NaN}', "INVALID_JSON"), ('{"value":Infinity}', "INVALID_JSON"),
    ('{"value":-Infinity}', "INVALID_JSON"), (SENTINEL, "INVALID_INPUT_FILE"),
    ('["' + SENTINEL + '"]', "INVALID_STRUCTURE"), ('null', "INVALID_STRUCTURE"),
    ('{"model":', "INVALID_INPUT_FILE"), ('"' + SENTINEL + '"', "INVALID_STRUCTURE"),
])
def test_real_cli_invalid_json_reports_fixed_reason_without_input(tmp_path, raw, reason):
    result = cli(tmp_path, raw=raw)
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "Review stopped: " + reason + "\nLOCAL_GUARD_EVENTS=[]\n"
    assert SENTINEL not in result.stderr


@pytest.mark.parametrize("failure,reason", [
    ("unknown-literal", "UNKNOWN_ENV_LITERAL"), ("unknown-field", "UNKNOWN_FIELD"),
    ("wrong-version", "RESOURCE_VERSION_MISMATCH"), ("bad-probe", "HTTP_STARTUP_GUARD_REQUIRED"),
])
def test_real_cli_validation_failure_never_leaks_literals(tmp_path, case, failure, reason):
    if failure == "unknown-literal": main_container(case["current"])["env"].append({"name": "UNKNOWN", "value": SENTINEL})
    elif failure == "unknown-field": case["before"]["service"]["secret_field"] = SENTINEL
    elif failure == "wrong-version": case["expected"]["resource_version"] = SENTINEL
    elif failure == "bad-probe": main_container(case["before"])["startupProbe"] = {"tcpSocket": {"port": 8080}}
    result = cli(tmp_path, case)
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "Review stopped: " + reason + "\nLOCAL_GUARD_EVENTS=[]\n"
    assert SENTINEL not in result.stderr


def test_real_cli_requires_explicit_input_with_no_fallback(tmp_path, case):
    result = cli(tmp_path, case, mode="no-args")
    assert result.returncode == 1
    assert result.stderr == "Review stopped: INVALID_ARGUMENTS\nLOCAL_GUARD_EVENTS=[]\n"


def test_cli_determinism_across_separate_interpreters(tmp_path, case):
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    left, right = cli(first, case), cli(second, case)
    assert left.returncode == right.returncode == 0
    assert left.stdout == right.stdout
    assert left.stderr == right.stderr == "LOCAL_GUARD_EVENTS=[]\n"


@pytest.mark.parametrize("argument", [SENTINEL, "--override=" + SENTINEL, "--output=" + SENTINEL])
def test_unknown_cli_arguments_do_not_echo_values(capsys, argument):
    assert planner().main([argument]) == 1
    result = capsys.readouterr()
    assert result.out == ""
    assert result.err == "Review stopped: INVALID_ARGUMENTS\n"


def test_missing_or_non_utf8_input_file_is_redacted(tmp_path, capsys):
    path = tmp_path / SENTINEL
    assert planner().main(["--input", str(path)]) == 1
    assert capsys.readouterr().err == "Review stopped: INVALID_INPUT_FILE\n"
    path.write_bytes(b"\xff\xfe")
    assert planner().main(["--input", str(path)]) == 1
    assert capsys.readouterr().err == "Review stopped: INVALID_INPUT_FILE\n"


def test_recursive_python_input_is_a_fixed_error_without_raw_details(case):
    case["before"]["revision"]["runtime"]["network"] = case["before"]
    # Unknown keys stop before recursive data can be serialized or copied.
    with pytest.raises(planner().PlanError, match="^UNKNOWN_FIELD$"):
        planner().build_plan(case)


def test_non_string_python_mapping_key_has_fixed_error(case):
    case["before"]["revision"]["runtime"]["network"][17] = SENTINEL
    with pytest.raises(planner().PlanError, match="^UNKNOWN_FIELD$"):
        planner().build_plan(case)
