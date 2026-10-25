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


# Only these public, explicitly authorized identity declarations are real. All
# UID/RV, env, targets, accounts, secret references and candidate data stay fake.
TCP_POLICY = "schwab-00781-tcp-preserve"
HTTP_POLICY = "http-health-8080"
TCP_SERVICE = "charles-schwab-quant-service"
TCP_BASE = TCP_SERVICE + "-00781-lel"
TCP_IMAGE = ("us-central1-docker.pkg.dev/charlesschwabquant/cloud-run-source-deploy/"
             "charlesschwabplatform/charles-schwab-quant-service@sha256:"
             "cd4779376abf0ec4a54c0b04c690adc953756cda782ecd8adcf773e659a1733a")
TCP_SOURCE = "6c890e3fdfbc0f8dcd7f96b5a634cc482ba3606c"
TCP_PROBE = {"failureThreshold": 1, "periodSeconds": 240, "tcpSocket": {"port": 8080}, "timeoutSeconds": 240}


@pytest.fixture
def tcp_case(case):
    case["startup_policy"] = TCP_POLICY
    case["expected"].update(project="charlesschwabquant", region="us-central1", service=TCP_SERVICE,
                            baseline_revision=TCP_BASE, candidate_revision=TCP_SERVICE + "-00782-review")
    for side in ("before", "current"):
        payload = case[side]
        for part in ("service", "revision"):
            payload[part]["identity"].update(project="charlesschwabquant", region="us-central1", service=TCP_SERVICE)
        payload["service"]["metadata"]["name"] = TCP_SERVICE
        for row in payload["service"]["traffic"]:
            row["revisionName"] = row["revisionName"].replace("demo-service", TCP_SERVICE)
        payload["service"]["traffic"][0]["revisionName"] = TCP_BASE
        payload["revision"]["identity"]["revision"] = TCP_BASE
        payload["revision"]["metadata"]["name"] = TCP_BASE
        payload["revision"]["metadata"]["labels"]["commit-sha"] = TCP_SOURCE
        payload["revision"]["provenance"]["commit"] = TCP_SOURCE
        main_container(payload)["image"] = TCP_IMAGE
        main_container(payload)["startupProbe"] = copy.deepcopy(TCP_PROBE)
    return case


def tcp_candidate(case):
    """Hand derivation lets negative review tests run independently of build_plan."""
    candidate = copy.deepcopy(case["before"])
    candidate["revision"]["identity"]["revision"] = case["expected"]["candidate_revision"]
    candidate["revision"]["metadata"]["name"] = case["expected"]["candidate_revision"]
    main_container(candidate)["image"] = case["expected"]["candidate_image"]
    return candidate


def tcp_review(case, candidate):
    return planner().review_candidate(case["before"], candidate, case["expected"], startup_policy=TCP_POLICY)


def test_tcp_exact_00781_preserves_complete_context_without_mutation(tcp_case):
    frozen = copy.deepcopy(tcp_case)
    candidate, review = planner().build_plan(tcp_case)
    assert candidate == tcp_candidate(tcp_case)
    assert tcp_case == frozen
    assert candidate["service"] == tcp_case["current"]["service"]
    assert main_container(candidate)["startupProbe"] == TCP_PROBE
    assert main_container(candidate)["env"][0]["value"].encode() == RAW_TARGET.encode()
    assert tcp_review(tcp_case, candidate) == review
    assert review["mode"] == "review_only" and review["deployment_ready"] is False
    assert review["startup_side_effects"] == "UNVERIFIED"
    assert review["source"] == review["cloud_configuration"] == "NOT_VERIFIED"
    assert review["concurrency"] == "MATCHED_LOCAL_SNAPSHOTS_NO_CAS"
    assert review["startup_assurance"] == {
        "policy": TCP_POLICY, "readiness": "TCP_PORT_READINESS_ONLY",
        "http_application_health": "NOT_VERIFIED", "runtime_startup": "NOT_EXECUTED",
        "baseline_binding": "LOCAL_DECLARATION_ONLY_NOT_CLOUD_PROVENANCE",
    }
    again, repeated = planner().build_plan(copy.deepcopy(tcp_case))
    assert again == candidate and repeated == review
    main_container(candidate)["env"].reverse()
    candidate["service"]["metadata"]["labels"]["purpose"] = "detached"
    assert tcp_case == frozen


@pytest.mark.parametrize("policy", [None, True, False, [], {}, 1, "allow_tcp", "skip", TCP_POLICY + "-other"])
@pytest.mark.parametrize("entry", ["build", "review"])
def test_tcp_policy_is_a_closed_explicit_enum(tcp_case, policy, entry):
    candidate = tcp_candidate(tcp_case)
    with pytest.raises(planner().PlanError, match="^INVALID_STARTUP_POLICY$"):
        if entry == "build":
            tcp_case["startup_policy"] = policy
            planner().build_plan(tcp_case)
        else:
            planner().review_candidate(tcp_case["before"], candidate, tcp_case["expected"], startup_policy=policy)


@pytest.mark.parametrize("entry", ["build", "review"])
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("handler", ["tcpSocket", "grpc"])
def test_tcp_or_grpc_exact_identity_cannot_change_default_http_policy(tcp_case, entry, explicit, handler):
    for side in ("before", "current"):
        main_container(tcp_case[side])["startupProbe"] = {handler: {"port": 8080}}
    candidate = tcp_candidate(tcp_case)
    with pytest.raises(planner().PlanError, match="^HTTP_STARTUP_GUARD_REQUIRED$"):
        if entry == "build":
            if explicit:
                tcp_case["startup_policy"] = HTTP_POLICY
            else:
                del tcp_case["startup_policy"]
            planner().build_plan(tcp_case)
        else:
            kwargs = {"startup_policy": HTTP_POLICY} if explicit else {}
            planner().review_candidate(tcp_case["before"], candidate, tcp_case["expected"], **kwargs)


def test_explicit_http_policy_is_identical_to_default(case):
    default_candidate, default_review = planner().build_plan(case)
    case["startup_policy"] = HTTP_POLICY
    assert planner().build_plan(case) == (default_candidate, default_review)
    assert planner().review_candidate(case["before"], default_candidate, case["expected"],
                                      startup_policy=HTTP_POLICY) == default_review
    assert "startup_assurance" not in default_review


@pytest.mark.parametrize("field,value", [
    ("project", "other-project"), ("region", "other-region"), ("service", "other-service"),
    ("baseline_revision", TCP_SERVICE + "-00781-other"),
    ("baseline_revision", TCP_SERVICE + "-00715-old"),
])
@pytest.mark.parametrize("entry", ["build", "review"])
def test_tcp_rejects_coherently_rebound_unknown_baselines(tcp_case, field, value, entry):
    expected = tcp_case["expected"]
    previous_service = expected["service"]
    expected[field] = value
    if field == "service":
        expected["baseline_revision"] = value + "-00781-lel"
        expected["candidate_revision"] = value + "-00782-review"
    for side in ("before", "current"):
        payload = tcp_case[side]
        for part in ("service", "revision"):
            payload[part]["identity"].update({key: expected[key] for key in ("project", "region", "service")})
        payload["service"]["metadata"]["name"] = expected["service"]
        payload["revision"]["identity"]["revision"] = expected["baseline_revision"]
        payload["revision"]["metadata"]["name"] = expected["baseline_revision"]
        for row in payload["service"]["traffic"]:
            row["revisionName"] = row["revisionName"].replace(previous_service, expected["service"])
        payload["service"]["traffic"][0]["revisionName"] = expected["baseline_revision"]
    with pytest.raises(planner().PlanError, match="^TCP_BASELINE_IDENTITY_MISMATCH$"):
        if entry == "build": planner().build_plan(tcp_case)
        else: tcp_review(tcp_case, tcp_candidate(tcp_case))


@pytest.mark.parametrize("side", ["before", "current"])
@pytest.mark.parametrize("image", [OLD_IMAGE, TCP_IMAGE.replace("charlesschwabplatform/", "other/"),
                                  TCP_IMAGE[:-1] + "0"])
def test_tcp_old_image_binding_is_exact(tcp_case, side, image):
    main_container(tcp_case[side])["image"] = image
    stop(tcp_case, "TCP_BASELINE_IMAGE_MISMATCH")


@pytest.mark.parametrize("side", ["before", "current", "candidate"])
@pytest.mark.parametrize("path", [("revision", "provenance", "commit"),
                                  ("revision", "metadata", "labels", "commit-sha")])
def test_tcp_source_declaration_must_match_on_every_snapshot(tcp_case, side, path):
    candidate = tcp_candidate(tcp_case)
    set_path(candidate if side == "candidate" else tcp_case[side], path, "d" * 40)
    with pytest.raises(planner().PlanError, match="^TCP_BASELINE_SOURCE_MISMATCH$"):
        if side == "candidate": tcp_review(tcp_case, candidate)
        else: planner().build_plan(tcp_case)


TCP_BAD_PROBES = [
    (None, "TCP_BASELINE_PROBE_MISMATCH"), ({}, "INVALID_PROBE"),
    *[({key: copy.deepcopy(value) for key, value in TCP_PROBE.items() if key != missing},
       "INVALID_PROBE" if missing == "tcpSocket" else "TCP_BASELINE_PROBE_MISMATCH")
      for missing in TCP_PROBE],
    (TCP_PROBE | {"initialDelaySeconds": 0}, "TCP_BASELINE_PROBE_MISMATCH"),
    (TCP_PROBE | {"successThreshold": 1}, "TCP_BASELINE_PROBE_MISMATCH"),
    (TCP_PROBE | {"unknown": SENTINEL}, "UNKNOWN_FIELD"),
    *[(TCP_PROBE | {field: value}, "TCP_BASELINE_PROBE_MISMATCH") for field, value in
      [("failureThreshold", 2), ("periodSeconds", 239), ("timeoutSeconds", 239),
       ("tcpSocket", {"port": 8081}), ("tcpSocket", {"port": 1}), ("tcpSocket", {"port": 65535})]],
    (TCP_PROBE | {"timeoutSeconds": 0}, "INVALID_PROBE"),
    (TCP_PROBE | {"tcpSocket": {"port": True}}, "INVALID_STRUCTURE"),
    (TCP_PROBE | {"tcpSocket": {"port": "8080"}}, "INVALID_STRUCTURE"),
    (TCP_PROBE | {"tcpSocket": {}}, "MISSING_FIELD"),
    (TCP_PROBE | {"tcpSocket": {"port": 8080, "unknown": SENTINEL}}, "UNKNOWN_FIELD"),
    (TCP_PROBE | {"grpc": {"port": 8080}}, "INVALID_PROBE"),
    (TCP_PROBE | {"httpGet": {"path": "/health", "port": 8080}}, "INVALID_PROBE"),
    ({"grpc": {"port": 8080}}, "TCP_BASELINE_PROBE_MISMATCH"),
    ({"httpGet": {"path": "/health", "port": 8080}}, "TCP_BASELINE_PROBE_MISMATCH"),
]


@pytest.mark.parametrize("side", ["before", "current", "candidate"])
@pytest.mark.parametrize("probe,reason", TCP_BAD_PROBES)
def test_tcp_complete_probe_is_never_defaulted_or_repaired(tcp_case, side, probe, reason):
    candidate = tcp_candidate(tcp_case)
    payload = candidate if side == "candidate" else tcp_case[side]
    if probe is None: del main_container(payload)["startupProbe"]
    else: main_container(payload)["startupProbe"] = copy.deepcopy(probe)
    with pytest.raises(planner().PlanError, match="^" + reason + "$"):
        if side == "candidate": tcp_review(tcp_case, candidate)
        else: planner().build_plan(tcp_case)


@pytest.mark.parametrize("side", ["current", "candidate"])
@pytest.mark.parametrize("path,value", PROTECTED_CHANGES)
def test_tcp_protected_context_cannot_drift(tcp_case, side, path, value):
    candidate = tcp_candidate(tcp_case)
    set_path(candidate if side == "candidate" else tcp_case[side], path, value)
    reason = "CONCURRENT_SNAPSHOT_CHANGED" if side == "current" else "PROTECTED_CONFIGURATION_CHANGED"
    if path == ("revision", "provenance", "commit"): reason = "TCP_BASELINE_SOURCE_MISMATCH"
    if "startupProbe" in path: reason = "TCP_BASELINE_PROBE_MISMATCH"
    with pytest.raises(planner().PlanError, match="^" + reason + "$"):
        if side == "candidate": tcp_review(tcp_case, candidate)
        else: planner().build_plan(tcp_case)


@pytest.mark.parametrize("side", ["current", "candidate"])
@pytest.mark.parametrize("mutation", ["traffic-order", "traffic-tag", "traffic-split", "env-order",
                                      "container-count", "mount-order", "volume-order", "liveness"])
def test_tcp_preserves_order_routing_and_other_probes(tcp_case, side, mutation):
    candidate = tcp_candidate(tcp_case)
    payload = candidate if side == "candidate" else tcp_case[side]
    container = main_container(payload)
    if mutation == "traffic-order": payload["service"]["traffic"].reverse()
    elif mutation == "traffic-tag": payload["service"]["traffic"][-1]["tag"] = "other-tag"
    elif mutation == "traffic-split":
        payload["service"]["traffic"][0]["percent"] = 80
        payload["service"]["traffic"][1]["percent"] = 20
    elif mutation == "env-order": container["env"].reverse()
    elif mutation == "container-count": payload["revision"]["runtime"]["containers"].pop()
    elif mutation == "mount-order": container["volumeMounts"].reverse()
    elif mutation == "volume-order": payload["revision"]["runtime"]["volumes"].reverse()
    elif mutation == "liveness": container["livenessProbe"]["periodSeconds"] = 11
    reason = "CONCURRENT_SNAPSHOT_CHANGED" if side == "current" else "PROTECTED_CONFIGURATION_CHANGED"
    with pytest.raises(planner().PlanError, match="^" + reason + "$"):
        if side == "candidate": tcp_review(tcp_case, candidate)
        else: planner().build_plan(tcp_case)


@pytest.mark.parametrize("mutation,reason", [
    ("alias", "INDEPENDENT_SNAPSHOTS_REQUIRED"), ("service-uid", "OBJECT_RECREATED"),
    ("revision-uid", "OBJECT_RECREATED"), ("before-rv", "RESOURCE_VERSION_MISMATCH"),
    ("current-rv", "RESOURCE_VERSION_MISMATCH"), ("expected-rv", "RESOURCE_VERSION_MISMATCH"),
    ("missing-rv", "MISSING_FIELD"), ("baseline-zero", "BASELINE_NOT_SERVING"),
    ("candidate-zero", "CANDIDATE_ROUTE_PRESENT"), ("candidate-tag", "CANDIDATE_ROUTE_PRESENT"),
    ("duplicate-tag", "AMBIGUOUS_TRAFFIC"), ("missing-port", "TCP_BASELINE_PROBE_MISMATCH"),
])
def test_tcp_existing_snapshot_and_traffic_guards_remain(tcp_case, mutation, reason):
    if mutation == "alias": tcp_case["current"] = tcp_case["before"]
    elif mutation.endswith("-uid"):
        tcp_case["current"][mutation[:-4]]["metadata"]["uid"] = "replacement-uid"
    elif mutation in {"before-rv", "current-rv"}:
        tcp_case[mutation[:-3]]["service"]["metadata"]["resourceVersion"] = "other-rv"
    elif mutation == "expected-rv": tcp_case["expected"]["resource_version"] = "other-rv"
    elif mutation == "missing-rv": del tcp_case["before"]["service"]["metadata"]["resourceVersion"]
    elif mutation == "missing-port": main_container(tcp_case["before"])["ports"] = []
    else:
        for side in ("before", "current"):
            rows = tcp_case[side]["service"]["traffic"]
            if mutation == "baseline-zero": rows[0]["percent"], rows[1]["percent"] = 0, 100
            elif mutation == "duplicate-tag": rows[1]["tag"] = rows[0]["tag"]
            else:
                row = {"revisionName": tcp_case["expected"]["candidate_revision"], "percent": 0}
                if mutation == "candidate-tag": row["tag"] = "forbidden-tag"
                rows.append(row)
    stop(tcp_case, reason)


@pytest.mark.parametrize("side", ["before", "current", "candidate"])
@pytest.mark.parametrize("path", [("service", "identity"), ("service", "metadata"),
                                  ("revision", "provenance"), ("revision", "runtime"),
                                  ("revision", "runtime", "network"),
                                  ("revision", "runtime", "containers", 0, "resources")])
def test_tcp_context_unknown_fields_still_stop(tcp_case, side, path):
    candidate = tcp_candidate(tcp_case)
    payload = candidate if side == "candidate" else tcp_case[side]
    for key in path: payload = payload[key]
    payload["unknown"] = SENTINEL
    with pytest.raises(planner().PlanError, match="^UNKNOWN_FIELD$"):
        if side == "candidate": tcp_review(tcp_case, candidate)
        else: planner().build_plan(tcp_case)


@pytest.mark.parametrize("side", ["before", "current", "candidate"])
def test_tcp_unknown_literal_precedes_literal_access_or_copy(tcp_case, side):
    candidate = tcp_candidate(tcp_case)
    for payload in (tcp_case["before"], tcp_case["current"], candidate):
        entries = main_container(payload)["env"]
        entries[0] = LiteralTrap(entries[0])
    main_container(candidate if side == "candidate" else tcp_case[side])["env"].append(
        LiteralTrap(name="UNKNOWN_LITERAL", value=SENTINEL))
    with pytest.raises(planner().PlanError, match="^UNKNOWN_ENV_LITERAL$"):
        if side == "candidate": tcp_review(tcp_case, candidate)
        else: planner().build_plan(tcp_case)


def test_tcp_desired_template_is_never_accessed(tcp_case):
    candidate, review = planner().build_plan(DesiredTrap(tcp_case))
    assert main_container(candidate)["env"][0]["value"] == RAW_TARGET
    assert review["excluded_desired_template"] == "NOT_ACCESSED"


def test_tcp_pure_api_reads_no_environment_files_or_full_a_preflight(tcp_case, monkeypatch):
    module = planner()
    calls = []

    def denied(*args, **kwargs):
        calls.append("impure-call")
        raise AssertionError("Pure TCP API accessed external state")

    class EnvironmentTrap(dict):
        __getitem__ = get = __iter__ = __contains__ = denied

    with monkeypatch.context() as scoped:
        scoped.setattr(builtins, "open", denied)
        scoped.setattr(io, "open", denied)
        scoped.setattr(os, "getenv", denied)
        scoped.setattr(os, "environ", EnvironmentTrap())
        for name in ("_preflight_local", "_run_json", "_dependency_projection", "_runtime_values"):
            scoped.setattr(module.a, name, denied)
        candidate, review = module.build_plan(tcp_case)
        assert tcp_review(tcp_case, candidate) == review
    assert calls == []


def test_tcp_review_rechecks_old_binding_and_candidate_image(tcp_case):
    candidate = tcp_candidate(tcp_case)
    main_container(tcp_case["before"])["image"] = OLD_IMAGE
    with pytest.raises(planner().PlanError, match="^TCP_BASELINE_IMAGE_MISMATCH$"):
        tcp_review(tcp_case, candidate)
    main_container(tcp_case["before"])["image"] = TCP_IMAGE
    main_container(candidate)["image"] = OLD_IMAGE
    with pytest.raises(planner().PlanError, match="^CANDIDATE_IMAGE_MISMATCH$"):
        tcp_review(tcp_case, candidate)


def test_tcp_real_disk_cli_preserves_guard_and_redacted_assurance(tmp_path, tcp_case):
    result = cli(tmp_path, tcp_case)
    assert result.returncode == 0, result.stderr
    assert result.stderr == "LOCAL_GUARD_EVENTS=[]\n"
    review = json.loads(result.stdout)
    assert review == planner().build_plan(tcp_case)[1]
    assert result.stdout == json.dumps(review, sort_keys=True, separators=(",", ":")) + "\n"
    for forbidden in (SENTINEL, RAW_TARGET, "fictional-env-secret", "fictional-volume-secret", TCP_IMAGE,
                      TCP_SOURCE, TCP_BASE, "fictional-service-uid", "opaque/RV+781==", '"containers"',
                      '"metadata"', '"traffic"', "runtime_risk_limits", "apiVersion", "kind: Service"):
        assert forbidden not in result.stdout + result.stderr


@pytest.mark.parametrize("failure,reason", [
    ("policy", "INVALID_STARTUP_POLICY"), ("probe", "TCP_BASELINE_PROBE_MISMATCH"),
    ("image", "TCP_BASELINE_IMAGE_MISMATCH"), ("source", "TCP_BASELINE_SOURCE_MISMATCH"),
    ("uid", "OBJECT_RECREATED"), ("rv", "RESOURCE_VERSION_MISMATCH"),
    ("unknown-literal", "UNKNOWN_ENV_LITERAL"), ("unknown-field", "UNKNOWN_FIELD"),
])
def test_tcp_real_disk_cli_failures_are_fixed_and_redacted(tmp_path, tcp_case, failure, reason):
    if failure == "policy": tcp_case["startup_policy"] = SENTINEL
    elif failure == "probe": main_container(tcp_case["before"])["startupProbe"]["periodSeconds"] = 239
    elif failure == "image": main_container(tcp_case["before"])["image"] = OLD_IMAGE
    elif failure == "source": tcp_case["current"]["revision"]["provenance"]["commit"] = "d" * 40
    elif failure == "uid": tcp_case["current"]["service"]["metadata"]["uid"] = SENTINEL
    elif failure == "rv": tcp_case["expected"]["resource_version"] = SENTINEL
    elif failure == "unknown-literal": main_container(tcp_case["current"])["env"].append({"name": "UNKNOWN", "value": SENTINEL})
    elif failure == "unknown-field": tcp_case["before"]["service"]["secret_field"] = SENTINEL
    result = cli(tmp_path, tcp_case)
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "Review stopped: " + reason + "\nLOCAL_GUARD_EVENTS=[]\n"
    assert SENTINEL not in result.stderr


@pytest.fixture
def native_case(monkeypatch):
    b = planner()
    identity = dict(project="demo-project", region="demo-region", service="demo-service", baseline_revision=BASE)
    monkeypatch.setattr(b, "_TCP_00781_IDENTITY", identity)
    monkeypatch.setattr(b, "_TCP_00781_IMAGE", OLD_IMAGE)
    traffic = [{"revisionName": BASE, "percent": 100, "tag": "stable"},
               {"revisionName": DESIRED, "percent": 0, "tag": "keep-zero"}]
    meta = {"name": "demo-service", "namespace": "demo-project", "uid": "service-uid",
            "resourceVersion": "rv/original==", "generation": 7,
            "labels": {"managed-by": "synthetic"}, "annotations": {"run.googleapis.com/ingress": "internal"}}
    revision_meta = dict(meta, name=BASE, uid="revision-uid", resourceVersion="revision-rv", generation=1,
                         labels={"commit-sha": b._TCP_00781_SOURCE, "serving.knative.dev/service": "demo-service"},
                         annotations={"run.googleapis.com/health-check-disabled": "false", "autoscaling.knative.dev/maxScale": "1"})
    raw_target = json.dumps({"strategy_release": {"strategy_revision": b.OLD_UES_PIN},
                             "runtime_risk_limits": {"binding": {"ues_revision": b.OLD_UES_PIN}},
                             "live_continuity": {"signature": "unchanged-synthetic"}})
    spec = {"serviceAccountName": "synthetic@example.invalid", "containerConcurrency": 1, "timeoutSeconds": 300,
            "containers": [{"image": OLD_IMAGE, "ports": [{"containerPort": 8080}],
                            "startupProbe": copy.deepcopy(b._TCP_00781_PROBE),
                            "env": [{"name": "RUNTIME_TARGET_JSON", "value": raw_target},
                                    {"name": "CREDENTIAL", "valueFrom": {"secretKeyRef": {"name": "synthetic-ref", "key": "3"}}}],
                            "resources": {"limits": {"cpu": "1", "memory": "512Mi"}}},
                           {"name": "helper", "image": OLD_IMAGE, "env": []}], "volumes": []}
    snap = {"service": {"metadata": meta, "spec": {"traffic": traffic},
                        "status": {"traffic": copy.deepcopy(traffic), "observedGeneration": 7}},
            "revision": {"metadata": revision_meta, "spec": spec, "status": {"imageDigest": OLD_IMAGE, "observedGeneration": 1}}}
    expected = dict(identity, candidate_revision=CANDIDATE, candidate_image=NEW_IMAGE,
                    resource_version=meta["resourceVersion"], candidate_sha=b.NOTIFICATION_SOURCE, candidate_run="987")
    return b, snap, expected


def test_native_request_is_detached_and_keeps_config_and_zero_tags(native_case):
    b, before, expected = native_case
    frozen = copy.deepcopy(before)
    request, report = b.build_native_plan(before, copy.deepcopy(before), expected)
    assert before == frozen
    assert request["metadata"]["resourceVersion"] == "rv/original=="
    assert request["spec"]["traffic"] == before["service"]["spec"]["traffic"]
    template = request["spec"]["template"]
    assert template["metadata"]["name"] == CANDIDATE
    assert set(template["metadata"]) == {"name", "labels", "annotations"}
    assert template["metadata"]["labels"]["commit-sha"] == b.NOTIFICATION_SOURCE
    assert "serving.knative.dev/service" not in template["metadata"]["labels"]
    restored = copy.deepcopy(template["spec"])
    restored["containers"][0]["image"] = OLD_IMAGE
    assert restored == before["revision"]["spec"]
    assert report["deployment_ready"] is False
    assert "unchanged-synthetic" not in json.dumps(report)


@pytest.mark.parametrize("change", ["service-uid", "service-rv", "revision-uid", "zero-tag", "probe", "sidecar", "target"])
def test_native_stale_or_changed_snapshots_stop(native_case, change):
    b, before, expected = native_case
    current = copy.deepcopy(before)
    if change == "service-uid": current["service"]["metadata"]["uid"] = "recreated"
    elif change == "service-rv": current["service"]["metadata"]["resourceVersion"] = "later"
    elif change == "revision-uid": current["revision"]["metadata"]["uid"] = "recreated"
    elif change == "zero-tag": current["service"]["spec"]["traffic"][1]["tag"] = "changed"
    elif change == "probe": current["revision"]["spec"]["containers"][0]["startupProbe"]["timeoutSeconds"] = 1
    elif change == "sidecar": current["revision"]["spec"]["containers"][1]["image"] = NEW_IMAGE
    else: current["revision"]["spec"]["containers"][0]["env"][0]["value"] += " "
    with pytest.raises(b.PlanError): b.build_native_plan(before, current, expected)


@pytest.mark.parametrize("startup", ["absent-key", "absent-map", "explicit-false"])
def test_native_default_startup_check_preserves_baseline(native_case, startup):
    b, before, expected = native_case
    annotation = "run.googleapis.com/health-check-disabled"
    if startup == "absent-key":
        del before["revision"]["metadata"]["annotations"][annotation]
    elif startup == "absent-map":
        del before["revision"]["metadata"]["annotations"]
    frozen = copy.deepcopy(before)
    request, report = b.build_native_plan(before, copy.deepcopy(before), expected)
    assert before == frozen
    assert request["metadata"]["resourceVersion"] == expected["resource_version"]
    assert request["spec"]["traffic"] == before["service"]["spec"]["traffic"]
    template = request["spec"]["template"]
    annotations = template["metadata"].get("annotations", {})
    if startup == "explicit-false":
        assert annotations[annotation] == "false"
    else:
        assert annotation not in annotations
    restored = copy.deepcopy(template["spec"])
    restored["containers"][0]["image"] = OLD_IMAGE
    assert restored == before["revision"]["spec"]
    assert report["deployment_ready"] is False


@pytest.mark.parametrize("value", ["", None, False, "FALSE", "invalid"])
def test_native_invalid_startup_annotation_still_stops(native_case, value):
    b, before, expected = native_case
    before["revision"]["metadata"]["annotations"]["run.googleapis.com/health-check-disabled"] = value
    with pytest.raises(b.PlanError):
        b.build_native_plan(before, copy.deepcopy(before), expected)


@pytest.mark.parametrize("change", ["desired", "source", "old-image", "disabled-startup", "wrong-pin", "unknown-env", "latest-route"])
def test_native_unsupported_or_wrong_baseline_is_not_defaulted(native_case, change):
    b, before, expected = native_case
    if change == "desired": before["revision"]["metadata"]["name"] = DESIRED
    elif change == "source": before["revision"]["metadata"]["labels"]["commit-sha"] = "d" * 40
    elif change == "old-image": before["revision"]["spec"]["containers"][0]["image"] = NEW_IMAGE
    elif change == "disabled-startup": before["revision"]["metadata"]["annotations"]["run.googleapis.com/health-check-disabled"] = "true"
    elif change == "wrong-pin": before["revision"]["spec"]["containers"][0]["env"][0]["value"] = '{"strategy_release":{"strategy_revision":"wrong"}}'
    elif change == "unknown-env": before["revision"]["spec"]["containers"][0]["env"].append({"name": "PRIVATE", "value": SENTINEL})
    else: before["service"]["spec"]["traffic"][0]["latestRevision"] = True
    with pytest.raises(b.PlanError) as error: b.build_native_plan(before, copy.deepcopy(before), expected)
    assert SENTINEL not in str(error.value)


@pytest.mark.parametrize("status", [200, 409, 412, 500])
def test_native_transport_one_request_original_rv_no_body_read(native_case, status):
    b, before, expected = native_case
    request, _ = b.build_native_plan(before, copy.deepcopy(before), expected)
    calls = []
    class Response:
        status_code = status
        def close(self): pass
        def json(self): raise AssertionError("Never read response config or error text")
    class Session:
        def request(self, method, url, **kw):
            calls.append((method, url, kw)); return Response()
    if status == 200: b.replace_native_service(request, expected, session=Session())
    else:
        with pytest.raises(b.PlanError): b.replace_native_service(request, expected, session=Session())
    assert len(calls) == 1 and calls[0][0] == "PUT"
    assert calls[0][2]["json"]["metadata"]["resourceVersion"] == "rv/original=="
    assert calls[0][2]["allow_redirects"] is False and calls[0][2]["stream"] is True


def test_native_transport_uncertain_error_is_not_retried(native_case):
    b, before, expected = native_case
    request, _ = b.build_native_plan(before, copy.deepcopy(before), expected)
    calls = []
    class Session:
        def request(self, *args, **kwargs):
            calls.append(1); raise RuntimeError(SENTINEL)
    with pytest.raises(b.PlanError, match="NATIVE_WRITE_UNCERTAIN") as error:
        b.replace_native_service(request, expected, session=Session())
    assert len(calls) == 1 and SENTINEL not in str(error.value)


def ready_native(before, request):
    after = copy.deepcopy(before)
    after["revision"]["metadata"].update(request["spec"]["template"]["metadata"])
    after["revision"]["metadata"].update(uid="new-uid", resourceVersion="new-rv")
    after["revision"]["spec"] = copy.deepcopy(request["spec"]["template"]["spec"])
    after["revision"]["status"] = {"imageDigest": NEW_IMAGE, "observedGeneration": 1,
                                     "conditions": [{"type": k, "status": "True"} for k in ("Ready", "ContainerHealthy")]}
    after["service"]["metadata"]["resourceVersion"] = "after-write-rv"
    after["service"]["status"]["latestCreatedRevisionName"] = request["spec"]["template"]["metadata"]["name"]
    return after


def test_native_publisher_reads_exact_candidate_and_waits_only_control_plane(native_case):
    b, before, expected = native_case
    request, _ = b.build_native_plan(before, copy.deepcopy(before), expected)
    after = ready_native(before, request)
    pending = copy.deepcopy(after); pending["revision"]["status"]["conditions"] = []
    captures, writes, pauses = [], [], []
    states = iter([copy.deepcopy(before), pending, after])
    def capture(identity, **kwargs):
        captures.append(identity["baseline_revision"]); return next(states)
    result = b.publish_native(before, expected, read=lambda cmd: [], capture=capture,
                              replace=lambda req, exp: writes.append(req), pause=pauses.append)
    assert captures == [BASE, CANDIDATE, CANDIDATE]
    assert len(writes) == 1 and pauses == [15]
    assert result["startup"] == "CONTROL_PLANE_TCP_READY_ONLY"


@pytest.mark.parametrize("failure", ["conflict", "already-exists", "failed-startup", "pending-bound", "tag-changed"])
def test_native_publisher_never_redeploys_on_conflict_or_failed_readback(native_case, failure):
    b, before, expected = native_case
    request, _ = b.build_native_plan(before, copy.deepcopy(before), expected)
    after = ready_native(before, request)
    if failure == "failed-startup": after["revision"]["status"]["conditions"][0]["status"] = "False"
    if failure == "pending-bound": after["revision"]["status"]["conditions"] = []
    if failure == "tag-changed": after["service"]["spec"]["traffic"][1]["tag"] = "changed"
    states = iter([copy.deepcopy(before), after]); writes = []
    def replace(req, exp):
        writes.append(req)
        if failure == "conflict": raise b.PlanError("NATIVE_RESOURCE_VERSION_CONFLICT")
    with pytest.raises(b.PlanError):
        b.publish_native(before, expected, read=lambda cmd: ([{}] if failure == "already-exists" else []),
                         capture=lambda *a, **kw: next(states), replace=replace, pause=lambda _: None, attempts=1)
    assert len(writes) == (0 if failure == "already-exists" else 1)


def fake_overlay_git(b, *, omit=None, extra=None, symlink=False, changed_pin=False):
    paths = sorted(b._OVERLAY_REQUIRED - ({omit} if omit else set()))
    if extra: paths.append(extra)
    def git(*args):
        if args == ("rev-parse", "HEAD"): return b.NOTIFICATION_SOURCE.encode()
        if args[0] == "merge-base": return b._TCP_00781_SOURCE.encode()
        if args[0] == "diff": return "\n".join("M\t" + p for p in paths).encode()
        if args[0] == "ls-tree": return (("120000" if symlink else "100644") + " blob " + "a" * 40 + "\t" + args[-1]).encode()
        assert args[0] == "show"
        source, name = args[1].split(":", 1)
        return (("bad" if changed_pin and source == b.NOTIFICATION_SOURCE else "dependency") if name in b._DEPENDENCY_HASHES else "synthetic " + name).encode()
    return git


def test_overlay_exact_git_blobs_assets_and_no_install_commands(native_case, monkeypatch, tmp_path):
    b, _, _ = native_case
    monkeypatch.setattr(b, "_DEPENDENCY_HASHES", {n: __import__('hashlib').sha256(b"dependency").hexdigest() for n in b._DEPENDENCY_HASHES})
    out = tmp_path / "context"
    result = b.prepare_notification_overlay(tmp_path, out, git=fake_overlay_git(b))
    recipe = (out / "Dockerfile").read_text()
    assert recipe.startswith("FROM " + OLD_IMAGE + "\n")
    assert all(line.startswith("COPY [") for line in recipe.splitlines()[1:])
    assert set(result["files_sha256"]) == b._OVERLAY_REQUIRED
    assert (out / "notifications/display_names.json").exists()
    assert (out / "notifications/digest_snapshot/provenance.json").exists()
    assert (out / "scripts/execution_report_heartbeat.py").exists()
    assert not (out / ".venv").exists() and not (out / "gha-creds-synthetic.json").exists()


@pytest.mark.parametrize("case", ["missing-asset", "extra-runtime", "symlink", "pin-changed"])
def test_overlay_rejects_before_writing_context(native_case, monkeypatch, tmp_path, case):
    b, _, _ = native_case
    monkeypatch.setattr(b, "_DEPENDENCY_HASHES", {n: __import__('hashlib').sha256(b"dependency").hexdigest() for n in b._DEPENDENCY_HASHES})
    git = fake_overlay_git(b, omit="notifications/display_names.json" if case == "missing-asset" else None,
                           extra="application/unreviewed.py" if case == "extra-runtime" else None,
                           symlink=case == "symlink", changed_pin=case == "pin-changed")
    out = tmp_path / "context"
    with pytest.raises(b.PlanError): b.prepare_notification_overlay(tmp_path, out, git=git)
    assert not out.exists()


def test_native_numeric_project_namespace_is_preserved_not_rewritten(native_case):
    b, before, expected = native_case
    for part in ("service", "revision"): before[part]["metadata"]["namespace"] = "123456789"
    request, _ = b.build_native_plan(before, copy.deepcopy(before), expected)
    assert request["metadata"]["namespace"] == "123456789"
