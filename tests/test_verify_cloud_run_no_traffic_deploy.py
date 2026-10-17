"""Synthetic local preflight tests. Never import the application or use a cloud client."""

import builtins
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import verify_cloud_run_no_traffic_deploy as check


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/verify_cloud_run_no_traffic_deploy.py"
BASE_REV = "demo-service-baseline"
CANDIDATE_REV = "demo-service-candidate"
BASE_COMMIT, CANDIDATE_COMMIT, PIN = "a" * 40, "b" * 40, "c" * 40
BASE_IMAGE = "registry.invalid/demo/app@sha256:" + "d" * 64
CANDIDATE_IMAGE = "registry.invalid/demo/app@sha256:" + "e" * 64
SENTINEL = "SENTINEL_PRIVATE_VALUE_MUST_NEVER_APPEAR"
REAL_POPEN = subprocess.Popen


def _test_process(command, *, check=False, capture_output=True, **kwargs):
    """Launch only the test driver or read-only git inventory, outside product guards."""
    assert command[0] in {sys.executable, "git"}
    process = REAL_POPEN(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    stdout, stderr = process.communicate()
    if check:
        assert process.returncode == 0, stderr
    return SimpleNamespace(returncode=process.returncode, stdout=stdout, stderr=stderr)


@pytest.fixture(autouse=True)
def isolated_execution(monkeypatch):
    calls = []

    def forbidden(*args, **kwargs):
        calls.append("external-runner-or-socket")
        raise AssertionError("External execution/network is forbidden")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(check, "_run_json", forbidden)
    original_import = builtins.__import__

    def no_business(name, *args, **kwargs):
        if name.split(".")[0] in {"main", "application", "strategy_registry", "us_equity_strategies", "quant_platform_kit", "schwab", "notifications"}:
            calls.append("business-import")
            raise AssertionError("Business import is forbidden")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_business)
    yield calls
    assert calls == []


def target():
    return {
        "platform_id": "schwab", "service_name": "demo-service", "strategy_profile": "synthetic-profile",
        "execution_mode": "live", "dry_run_only": False,
        "account_id": "synthetic-account", "strategy_release": {"strategy_revision": PIN},
        "runtime_risk_limits": {"binding": {"ues_revision": PIN}, "max_order_value": 123},
        "live_continuity": {"baseline_target_sha256": "f" * 64},
    }


def projection(*, candidate=False):
    return {
        "identity": {"project": "demo-project", "region": "demo-region", "service": "demo-service",
                     "revision": CANDIDATE_REV if candidate else BASE_REV},
        "provenance": {"commit": CANDIDATE_COMMIT if candidate else BASE_COMMIT, "run": "run-candidate" if candidate else "run-baseline"},
        "traffic": [{"revisionName": BASE_REV, "percent": 100},
                    {"revisionName": "demo-service-old", "percent": 0, "tag": "old-tag"}],
        "runtime": {
            "serviceAccountName": "demo-runtime@demo-project.iam.gserviceaccount.com",
            "ingress": "internal", "executionEnvironment": "gen2", "containerConcurrency": 1, "timeoutSeconds": 300,
            "scaling": {"minInstanceCount": 0, "maxInstanceCount": 1}, "network": {}, "volumes": [],
            "containers": [{
                "name": "app", "image": CANDIDATE_IMAGE if candidate else BASE_IMAGE,
                "ports": [{"containerPort": 8080}], "command": [], "args": [], "volumeMounts": [],
                "resources": {"limits": {"cpu": "1", "memory": "512Mi"}},
                "startupProbe": {"httpGet": {"path": "/health", "port": 8080}, "periodSeconds": 10, "timeoutSeconds": 5, "failureThreshold": 24},
                "env": [
                    {"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target())},
                    {"name": "STRATEGY_PROFILE", "value": "synthetic-profile"},
                    {"name": "RUNTIME_TARGET_ENABLED", "value": "true"},
                    {"name": "SCHWAB_DRY_RUN_ONLY", "value": "false"},
                    {"name": "SCHWAB_CASH_CURRENCY", "value": "USD"},
                    {"name": "EXECUTION_REPORT_GCS_URI", "value": "gs://demo-reports/readonly"},
                    {"name": "SYNTHETIC_SECRET", "valueFrom": {"secretKeyRef": {"name": "demo-secret", "key": "1"}}},
                ],
            }],
        },
    }


def write_repository(directory):
    directory.mkdir()
    (directory / "pyproject.toml").write_text(
        '[project]\nname="synthetic-app"\ndependencies=["UsEquityStrategies @ git+https://example.invalid/UsEquityStrategies.git@' + PIN + '"]\n', encoding="utf-8")
    (directory / "qsl.toml").write_text('[qsl.requires]\nus_equity_strategies="' + PIN + '"\n', encoding="utf-8")
    (directory / "uv.lock").write_text(
        'version=1\n[[package]]\nname="us-equity-strategies"\nversion="0.0.0"\nsource={git="https://example.invalid/UsEquityStrategies.git?rev=' + PIN + '#' + PIN + '"}\n', encoding="utf-8")


@pytest.fixture
def case(tmp_path):
    baseline_repo, candidate_repo = tmp_path / "baseline-app", tmp_path / "candidate-app"
    for directory in (baseline_repo, candidate_repo):
        write_repository(directory)
    args = SimpleNamespace(
        project="demo-project", region="demo-region", service="demo-service",
        baseline_revision=BASE_REV, candidate_revision=CANDIDATE_REV,
        candidate_image=CANDIDATE_IMAGE, candidate_sha=CANDIDATE_COMMIT, candidate_run="run-candidate",
        baseline_repository=baseline_repo, candidate_repository=candidate_repo,
    )
    return args, projection(), projection(candidate=True)


def env(payload):
    return payload["runtime"]["containers"][0]["env"]


def literal(payload, name):
    return next(entry for entry in env(payload) if entry["name"] == name)


def stop(case, reason):
    args, baseline, candidate = case
    frozen = json.dumps([baseline, candidate], sort_keys=True)
    with pytest.raises(check.PreflightError) as caught:
        check._preflight_local(args, baseline, candidate)
    assert str(caught.value) == reason
    assert SENTINEL not in str(caught.value)
    assert frozen == json.dumps([baseline, candidate], sort_keys=True)


def test_image_and_explicit_provenance_only_pass_without_mutation(case):
    args, baseline, candidate = case
    frozen = copy.deepcopy((baseline, candidate))
    check._preflight_local(args, baseline, candidate)
    assert (baseline, candidate) == frozen
    assert literal(baseline, "RUNTIME_TARGET_JSON")["value"] == literal(candidate, "RUNTIME_TARGET_JSON")["value"]


def test_same_env_names_and_secret_references_do_not_hide_target_drift(case):
    args, baseline, candidate = case
    changed = target() | {"account_id": "another-synthetic-account"}
    literal(candidate, "RUNTIME_TARGET_JSON")["value"] = json.dumps(changed)
    assert [entry["name"] for entry in env(baseline)] == [entry["name"] for entry in env(candidate)]
    assert env(baseline)[-1] == env(candidate)[-1]
    # Demonstrate the old names/reference-only projection misses this drift.
    def old(p):
        return [{key: value for key, value in entry.items() if key != "value"} for entry in env(p)]
    assert check._digest(old(baseline)) == check._digest(old(candidate))
    stop(case, "RUNTIME_CONFIGURATION_CHANGED")


@pytest.mark.parametrize("side", [1, 2])
@pytest.mark.parametrize("field", ["project", "region", "service", "revision"])
def test_explicit_revision_ownership_must_match(case, side, field):
    case[side]["identity"][field] = "wrong-identity"
    stop(case, "BASELINE_IDENTITY_MISMATCH" if side == 1 else "CANDIDATE_IDENTITY_MISMATCH")


@pytest.mark.parametrize("field,value,reason", [
    ("candidate_image", BASE_IMAGE, "CANDIDATE_IMAGE_MISMATCH"),
    ("candidate_image", CANDIDATE_IMAGE + "suffix", "CANDIDATE_IMAGE_MISMATCH"),
    ("candidate_sha", BASE_COMMIT, "CANDIDATE_PROVENANCE_MISMATCH"),
    ("candidate_run", "wrong-run", "CANDIDATE_PROVENANCE_MISMATCH"),
    ("candidate_revision", BASE_REV, "AMBIGUOUS_REVISION_IDENTITY"),
    ("baseline_revision", "latest-created", "INVALID_EXPECTED_IDENTITY"),
])
def test_wrong_candidate_identity_never_falls_back(case, field, value, reason):
    setattr(case[0], field, value)
    stop(case, reason)


def test_missing_run_rejected_if_expected(case):
    del case[2]["provenance"]["run"]
    stop(case, "CANDIDATE_PROVENANCE_MISMATCH")


def test_optional_run_and_target_alias(case):
    case[0].candidate_run = None
    for payload in case[1:]:
        del payload["provenance"]["run"]
        literal(payload, "RUNTIME_TARGET_JSON")["name"] = "QSL_RUNTIME_TARGET_JSON"
    check._preflight_local(*case)


def test_new_untagged_zero_candidate_allowed(case):
    case[2]["traffic"].append({"revisionName": CANDIDATE_REV, "percent": 0})
    check._preflight_local(*case)


@pytest.mark.parametrize("change", ["tag", "remove-tag", "remove-old", "retarget-tag", "new-tag", "percent", "candidate-active", "desired-only", "duplicate", "latest"])
def test_all_existing_tags_and_percentages_protected(case, change):
    rows = case[2]["traffic"]
    reason = "TRAFFIC_OR_TAG_CHANGED"
    if change == "tag":
        rows[1]["tag"] = "changed-tag"
    elif change == "remove-tag":
        del rows[1]["tag"]
    elif change == "remove-old":
        rows.pop()
    elif change == "retarget-tag":
        rows[1]["revisionName"] = CANDIDATE_REV
    elif change == "new-tag":
        rows.append({"revisionName": CANDIDATE_REV, "percent": 0, "tag": "candidate-tag"})
    elif change == "percent":
        rows[0]["percent"], rows[1]["percent"] = 99, 1
    elif change == "candidate-active":
        rows[0]["percent"] = 99
        rows.append({"revisionName": CANDIDATE_REV, "percent": 1})
        reason = "CANDIDATE_HAS_TRAFFIC"
    elif change == "desired-only":
        rows[0]["revisionName"] = "demo-service-other"
        reason = "BASELINE_NOT_SERVING"
    elif change == "duplicate":
        rows.append(copy.deepcopy(rows[1]))
        reason = "AMBIGUOUS_TRAFFIC"
    elif change == "latest":
        rows[0]["latestRevision"] = True
        reason = "UNKNOWN_FIELD"
    # Even the allowed new row must not hide any earlier tag drift.
    if change in {"tag", "remove-tag", "remove-old", "retarget-tag"}:
        rows.append({"revisionName": CANDIDATE_REV, "percent": 0})
    stop(case, reason)


class LiteralAccessTrap(dict):
    def __getitem__(self, key):
        if key == "value":
            raise AssertionError("A literal value was accessed before inventory completed")
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key == "value":
            raise AssertionError("A literal value was accessed before inventory completed")
        return super().get(key, default)


def trap_all_literals(case):
    for payload in case[1:]:
        for container in payload["runtime"]["containers"]:
            container["env"] = [LiteralAccessTrap(entry) if "value" in entry else entry for entry in container["env"]]


@pytest.mark.parametrize("side", ["both", "candidate"])
@pytest.mark.parametrize("probe,reason", [
    ({}, "INVALID_PROBE"),
    ({"tcpSocket": {}}, "MISSING_FIELD"),
    ({"grpc": {}}, "MISSING_FIELD"),
    ({"httpGet": {"path": "/health"}}, "MISSING_FIELD"),
    ({"httpGet": {"port": 8080}}, "MISSING_FIELD"),
    ({"httpGet": {"path": "", "port": 8080}}, "INVALID_PROBE"),
    ({"httpGet": {"path": "health", "port": 8080}}, "INVALID_PROBE"),
    ({"tcpSocket": {"port": 8080}, "httpGet": {"path": "/health", "port": 8080}}, "INVALID_PROBE"),
    ({"tcpSocket": {"port": 8080}, "grpc": {"port": 8080}}, "INVALID_PROBE"),
    ({"tcpSocket": {"port": 8080}, "periodSeconds": 0}, "INVALID_PROBE"),
])
def test_probe_structure_stops_before_any_literal(case, side, probe, reason):
    for payload in case[1:] if side == "both" else case[2:]:
        payload["runtime"]["containers"][0]["livenessProbe"] = copy.deepcopy(probe)
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^" + reason + "$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("side", ["both", "candidate"])
@pytest.mark.parametrize("handler", ["httpGet", "tcpSocket", "grpc"])
@pytest.mark.parametrize("port,reason", [
    (-1, "INVALID_PROBE"), (0, "INVALID_PROBE"), (65536, "INVALID_PROBE"),
    (True, "INVALID_STRUCTURE"), (8080.0, "INVALID_STRUCTURE"), ("8080", "INVALID_STRUCTURE"),
])
def test_probe_ports_stop_before_any_literal(case, side, handler, port, reason):
    for payload in case[1:] if side == "both" else case[2:]:
        settings = {"port": port}
        if handler == "httpGet":
            settings["path"] = "/health"
        payload["runtime"]["containers"][0]["livenessProbe"] = {handler: settings}
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^" + reason + "$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("side", ["both", "candidate"])
@pytest.mark.parametrize("network,reason", [
    ({"egress": "UNKNOWN"}, "INVALID_NETWORK_EGRESS"),
    ({"network": "demo-network", "egress": ""}, "INVALID_NETWORK_EGRESS"),
    ({"vpcAccessConnector": "demo-connector", "egress": "UNKNOWN"}, "INVALID_NETWORK_EGRESS"),
    ({"egress": "all-traffic"}, "INVALID_NETWORK_CONFIGURATION"),
    ({"network": "demo-network", "vpcAccessConnector": "demo-connector"}, "AMBIGUOUS_NETWORK"),
    ({"subnetwork": "demo-subnet", "vpcAccessConnector": "demo-connector"}, "AMBIGUOUS_NETWORK"),
    ({"vpcAccessConnector": "demo-connector", "networkTags": ["demo-tag"]}, "AMBIGUOUS_NETWORK"),
    ({"networkTags": ["demo-tag"]}, "INVALID_NETWORK_CONFIGURATION"),
    ({"network": ""}, "INVALID_NETWORK_REFERENCE"),
    ({"subnetwork": ""}, "INVALID_NETWORK_REFERENCE"),
    ({"vpcAccessConnector": ""}, "INVALID_NETWORK_REFERENCE"),
    ({"network": "invalid network"}, "INVALID_NETWORK_REFERENCE"),
    ({"network": "projects/demo-project/regions/demo-region/subnetworks/demo-subnet"}, "INVALID_NETWORK_REFERENCE"),
    ({"network": "demo-network", "networkTags": [""]}, "INVALID_NETWORK_TAGS"),
    ({"network": "demo-network", "networkTags": ["demo-tag", "demo-tag"]}, "INVALID_NETWORK_TAGS"),
])
def test_network_metadata_stops_before_any_literal(case, side, network, reason):
    for payload in case[1:] if side == "both" else case[2:]:
        payload["runtime"]["network"] = copy.deepcopy(network)
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^" + reason + "$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("side", ["both", "candidate"])
@pytest.mark.parametrize("resources,reason", [
    ({}, "MISSING_FIELD"), ({"limits": {}}, "MISSING_FIELD"),
    ({"limits": {"cpu": "1"}}, "MISSING_FIELD"),
    ({"limits": {"memory": "512Mi"}}, "MISSING_FIELD"),
    ({"limits": {"cpu": "", "memory": ""}}, "INVALID_RESOURCE_LITERAL"),
    *[({"limits": {"cpu": cpu, "memory": "512Mi"}}, "INVALID_RESOURCE_LITERAL")
      for cpu in ("", "UNKNOWN", "0", "-1", "0.000", "0m", "-100m", "NaN", "inf", "1 CPU", " 1")],
    *[({"limits": {"cpu": "1", "memory": memory}}, "INVALID_RESOURCE_LITERAL")
      for memory in ("", "UNKNOWN", "0Mi", "-1Gi", "512", "512MB", "NaN", "512Mi ")],
    ({"limits": {"cpu": 1, "memory": "512Mi"}}, "INVALID_STRUCTURE"),
    ({"limits": {"cpu": "1", "memory": None}}, "INVALID_STRUCTURE"),
])
def test_resource_metadata_stops_before_any_literal(case, side, resources, reason):
    for payload in case[1:] if side == "both" else case[2:]:
        payload["runtime"]["containers"][0]["resources"] = copy.deepcopy(resources)
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^" + reason + "$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("field,value,reason", [
    ("startupProbe", {"tcpSocket": {}}, "MISSING_FIELD"),
    ("livenessProbe", {"grpc": {"port": 0}}, "INVALID_PROBE"),
    ("resources", {"limits": {"cpu": "1", "memory": ""}}, "INVALID_RESOURCE_LITERAL"),
])
def test_candidate_sidecar_metadata_stops_before_baseline_literals(case, field, value, reason):
    sidecar = copy.deepcopy(case[2]["runtime"]["containers"][0])
    sidecar.update(name="sidecar", env=[], **{field: value})
    case[2]["runtime"]["containers"].append(sidecar)
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^" + reason + "$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("network", [
    {"network": "demo-network"}, {"subnetwork": "demo-subnet"},
    {"network": "demo-network", "subnetwork": "demo-subnet", "networkTags": ["demo-tag"]},
    {"network": "projects/demo-project/global/networks/demo-network",
     "subnetwork": "projects/demo-project/regions/demo-region/subnetworks/demo-subnet"},
    {"vpcAccessConnector": "demo-connector"},
    {"vpcAccessConnector": "projects/demo-project/locations/demo-region/connectors/demo-connector"},
])
@pytest.mark.parametrize("egress", [None, "all-traffic", "private-ranges-only"])
def test_supported_network_combinations_pass_without_rewriting(case, network, egress):
    for payload in case[1:]:
        payload["runtime"]["network"] = copy.deepcopy(network)
        if egress is not None:
            payload["runtime"]["network"]["egress"] = egress
    frozen = copy.deepcopy(case[1:])
    check._preflight_local(*case)
    assert case[1:] == frozen


@pytest.mark.parametrize("cpu,memory", [("1", "512Mi"), ("2", "1Gi"), ("0.5", "256Mi"), ("1000m", "2Gi"), ("1.5", "1024Mi")])
def test_supported_resource_literals_pass_without_rewriting(case, cpu, memory):
    for payload in case[1:]:
        payload["runtime"]["containers"][0]["resources"]["limits"] = {"cpu": cpu, "memory": memory}
    frozen = copy.deepcopy(case[1:])
    check._preflight_local(*case)
    assert case[1:] == frozen


@pytest.mark.parametrize("handler", ["httpGet", "tcpSocket", "grpc"])
@pytest.mark.parametrize("port", [1, 65535])
def test_supported_probe_handlers_and_port_boundaries_pass(case, handler, port):
    for payload in case[1:]:
        settings = {"port": port}
        if handler == "httpGet":
            settings["path"] = "/health"
        elif handler == "grpc":
            settings["service"] = "synthetic-health"
        payload["runtime"]["containers"][0]["livenessProbe"] = {handler: settings, "initialDelaySeconds": 0}
    check._preflight_local(*case)


@pytest.mark.parametrize("location", ["baseline", "candidate", "sidecar"])
def test_unknown_literal_never_accessed_even_after_other_allowed_names(case, location):
    # Trap allowed baseline literals too: BOTH inventories must finish first.
    for payload in case[1:]:
        payload["runtime"]["containers"][0]["env"] = [LiteralAccessTrap(entry) if "value" in entry else entry for entry in env(payload)]
    selected = case[1] if location == "baseline" else case[2]
    container = selected["runtime"]["containers"][0]
    if location == "sidecar":
        container = copy.deepcopy(container)
        container["name"] = "sidecar"
        container["env"] = []
        selected["runtime"]["containers"].append(container)
    container["env"].append(LiteralAccessTrap(name="UNKNOWN_TOKEN", value=SENTINEL))
    with pytest.raises(check.PreflightError, match="^UNKNOWN_ENV_LITERAL$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("where", ["top", "identity", "runtime", "container", "probe", "secret"])
def test_unknown_fields_stop_before_literal_access(case, where):
    for payload in case[1:]:
        payload["runtime"]["containers"][0]["env"] = [LiteralAccessTrap(entry) if "value" in entry else entry for entry in env(payload)]
    candidate = case[2]
    container = candidate["runtime"]["containers"][0]
    selected = {"top": candidate, "identity": candidate["identity"], "runtime": candidate["runtime"],
                "container": container, "probe": container["startupProbe"], "secret": env(candidate)[-1]["valueFrom"]["secretKeyRef"]}[where]
    selected["unknown-field"] = SENTINEL
    with pytest.raises(check.PreflightError, match="^UNKNOWN_FIELD$"):
        check._preflight_local(*case)


@pytest.mark.parametrize("change,reason", [
    ("duplicate-env", "DUPLICATE_ENV_NAME"), ("both-kinds", "AMBIGUOUS_ENV_KIND"),
    ("missing-kind", "AMBIGUOUS_ENV_KIND"), ("target-alias", "AMBIGUOUS_OR_MISSING_TARGET"),
    ("secret-target", "REQUIRED_LITERAL_MISSING"), ("missing-target", "AMBIGUOUS_OR_MISSING_TARGET"),
    ("invalid-secret-name", "INVALID_SECRET_REFERENCE"), ("invalid-secret-version", "INVALID_SECRET_REFERENCE"),
    ("missing-reference", "MISSING_FIELD"), ("bad-target", "INVALID_TARGET"),
    ("target-array", "INVALID_TARGET"), ("target-mode-object", "INVALID_TARGET"),
    ("target-service", "INVALID_TARGET"), ("bad-bool", "INVALID_BOOLEAN_LITERAL"),
    ("non-string-literal", "INVALID_ENV_LITERAL"), ("invalid-env-name", "INVALID_ENV_NAME"),
    ("duplicate-container", "DUPLICATE_CONTAINER"), ("missing-percent", "MISSING_FIELD"),
    ("boolean-percent", "INVALID_STRUCTURE"), ("mutable-image", "INVALID_IMMUTABLE_IMAGE"),
])
def test_invalid_or_ambiguous_input_fixed_reasons(case, change, reason):
    candidate = case[2]
    entries = env(candidate)
    if change == "duplicate-env":
        entries.append(copy.deepcopy(entries[0]))
    elif change == "both-kinds":
        entries[0]["valueFrom"] = copy.deepcopy(entries[-1]["valueFrom"])
    elif change == "missing-kind":
        del entries[0]["value"]
    elif change == "target-alias":
        entries.append({"name": "QSL_RUNTIME_TARGET_JSON", "value": entries[0]["value"]})
    elif change == "secret-target":
        del entries[0]["value"]
        entries[0]["valueFrom"] = copy.deepcopy(entries[-1]["valueFrom"])
    elif change == "missing-target":
        entries.pop(0)
    elif change.startswith("invalid-secret"):
        key = "name" if change.endswith("name") else "key"
        entries[-1]["valueFrom"]["secretKeyRef"][key] = "invalid value"
    elif change == "missing-reference":
        del entries[-1]["valueFrom"]["secretKeyRef"]["key"]
    elif change == "bad-target":
        entries[0]["value"] = SENTINEL
    elif change == "target-array":
        entries[0]["value"] = "[]"
    elif change == "target-mode-object":
        entries[0]["value"] = json.dumps(target() | {"execution_mode": {}})
    elif change == "target-service":
        entries[0]["value"] = json.dumps(target() | {"service_name": "wrong-service"})
    elif change == "bad-bool":
        literal(candidate, "RUNTIME_TARGET_ENABLED")["value"] = SENTINEL
    elif change == "non-string-literal":
        literal(candidate, "STRATEGY_PROFILE")["value"] = 17
    elif change == "invalid-env-name":
        entries[0]["name"] = "INVALID NAME"
    elif change == "duplicate-container":
        candidate["runtime"]["containers"].append(copy.deepcopy(candidate["runtime"]["containers"][0]))
    elif change == "missing-percent":
        del candidate["traffic"][0]["percent"]
    elif change == "boolean-percent":
        candidate["traffic"][0]["percent"] = True
    elif change == "mutable-image":
        candidate["runtime"]["containers"][0]["image"] = "registry.invalid/demo/app:latest"
    stop(case, reason)


@pytest.mark.parametrize("name,value", [
    ("RUNTIME_TARGET_ENABLED", "false"), ("SCHWAB_CASH_CURRENCY", "EUR"),
    ("EXECUTION_REPORT_GCS_URI", "gs://demo-other/reports"),
])
def test_protected_literal_changes_stop(case, name, value):
    literal(case[2], name)["value"] = value
    stop(case, "RUNTIME_CONFIGURATION_CHANGED")


@pytest.mark.parametrize("field", ["account_id", "runtime_risk_limits", "strategy_release", "live_continuity", "dry_run_only"])
def test_target_risk_pin_fingerprint_and_dry_run_never_rebased(case, field):
    changed = target()
    if field == "dry_run_only":
        changed.update(dry_run_only=True, execution_mode="paper")
        literal(case[2], "SCHWAB_DRY_RUN_ONLY")["value"] = "true"
    else:
        changed[field] = SENTINEL
    literal(case[2], "RUNTIME_TARGET_JSON")["value"] = json.dumps(changed)
    stop(case, "RUNTIME_CONFIGURATION_CHANGED")


@pytest.mark.parametrize("change", ["account", "ingress", "network", "environment", "scale", "cpu", "memory", "concurrency", "timeout", "port", "command", "args", "probe", "liveness", "secret-version", "kind", "env-order", "container-count", "container-order", "sidecar-image", "mount"])
def test_runtime_fields_and_order_protected(case, change):
    # Include a sidecar to exercise both ordering and a non-primary image.
    for payload in case[1:]:
        sidecar = copy.deepcopy(payload["runtime"]["containers"][0])
        sidecar.update(name="sidecar", image=BASE_IMAGE, env=[])
        payload["runtime"]["containers"].append(sidecar)
    runtime = case[2]["runtime"]
    container = runtime["containers"][0]
    if change == "account": runtime["serviceAccountName"] = "other@example.invalid"
    elif change == "ingress": runtime["ingress"] = "all"
    elif change == "network": runtime["network"] = {"vpcAccessConnector": "demo-connector"}
    elif change == "environment": runtime["executionEnvironment"] = "gen1"
    elif change == "scale": runtime["scaling"]["maxInstanceCount"] = 2
    elif change in {"cpu", "memory"}: container["resources"]["limits"][change] = "2" if change == "cpu" else "1Gi"
    elif change == "concurrency": runtime["containerConcurrency"] = 2
    elif change == "timeout": runtime["timeoutSeconds"] = 301
    elif change == "port": container["ports"][0]["containerPort"] = 9090
    elif change == "command": container["command"] = ["python"]
    elif change == "args": container["args"] = ["--demo"]
    elif change == "probe": container["startupProbe"]["failureThreshold"] = 25
    elif change == "liveness": container["livenessProbe"] = {"httpGet": {"path": "/health", "port": 8080}}
    elif change == "secret-version": env(case[2])[-1]["valueFrom"]["secretKeyRef"]["key"] = "2"
    elif change == "kind": literal(case[2], "SCHWAB_CASH_CURRENCY").update(valueFrom={"secretKeyRef": {"name": "demo-secret", "key": "1"}}); del literal(case[2], "SCHWAB_CASH_CURRENCY")["value"]
    elif change == "env-order": container["env"].reverse()
    elif change == "container-count": runtime["containers"].pop()
    elif change == "container-order":
        # Keep valid target in the new primary, so this reaches exact config comparison.
        runtime["containers"][1]["env"] = container["env"]
        container["env"] = []
        runtime["containers"].reverse()
        runtime["containers"][0]["image"] = CANDIDATE_IMAGE
    elif change == "sidecar-image": runtime["containers"][1]["image"] = CANDIDATE_IMAGE
    elif change == "mount":
        runtime["volumes"] = [{"name": "cache", "emptyDir": {}}]
        container["volumeMounts"] = [{"name": "cache", "mountPath": "/cache"}]
    stop(case, "RUNTIME_CONFIGURATION_CHANGED")


@pytest.mark.parametrize("change", ["missing", "tcp", "wrong-path", "wrong-port"])
def test_http_startup_guard_cannot_be_downgraded_even_identically(case, change):
    for payload in case[1:]:
        container = payload["runtime"]["containers"][0]
        if change == "missing": del container["startupProbe"]
        elif change == "tcp": container["startupProbe"] = {"tcpSocket": {"port": 8080}}
        elif change == "wrong-path": container["startupProbe"]["httpGet"]["path"] = "/other"
        elif change == "wrong-port": container["startupProbe"]["httpGet"]["port"] = 9090
    stop(case, "HTTP_STARTUP_GUARD_REQUIRED")


@pytest.mark.parametrize("filename", ["pyproject.toml", "qsl.toml", "uv.lock"])
def test_dependency_changes_fail_without_install_or_repo_default(case, filename):
    path = case[0].candidate_repository / filename
    path.write_text(path.read_text().replace(PIN, "1" * 40), encoding="utf-8")
    stop(case, "DEPENDENCY_DECLARATIONS_CHANGED")


@pytest.mark.parametrize("declaration", [
    '[dependency-groups]\ntest=["synthetic-package==2"]\n',
    '[build-system]\nrequires=["synthetic-builder==2"]\n',
])
def test_dependency_groups_and_build_requirements_are_protected(case, declaration):
    path = case[0].candidate_repository / "pyproject.toml"
    path.write_text(path.read_text() + declaration, encoding="utf-8")
    stop(case, "DEPENDENCY_DECLARATIONS_CHANGED")


@pytest.mark.parametrize("filename", ["pyproject.toml", "qsl.toml", "uv.lock"])
def test_malformed_declarations_stop_with_fixed_reason(case, filename):
    (case[0].candidate_repository / filename).write_text(SENTINEL, encoding="utf-8")
    stop(case, "INVALID_DEPENDENCY_DECLARATIONS")


def test_unsupported_toml_value_has_no_raw_exception(case):
    for repository in (case[0].baseline_repository, case[0].candidate_repository):
        path = repository / "pyproject.toml"
        path.write_text(path.read_text() + '[tool.synthetic]\nunsupported=2026-10-01\n', encoding="utf-8")
    stop(case, "INVALID_DEPENDENCY_DECLARATIONS")


def test_identically_wrong_live_pins_still_fail_existing_admission(case):
    for repository in (case[0].baseline_repository, case[0].candidate_repository):
        for filename in ("pyproject.toml", "qsl.toml", "uv.lock"):
            path = repository / filename
            path.write_text(path.read_text().replace(PIN, "1" * 40), encoding="utf-8")
    stop(case, "LIVE_UES_PIN_NOT_ADMITTED")


def test_missing_explicit_application_checkout_never_uses_tool_checkout(case):
    case[0].candidate_repository = case[0].candidate_repository / "missing"
    stop(case, "INVALID_DEPENDENCY_DECLARATIONS")


def test_pin_helper_import_does_not_load_business_modules():
    path = ROOT / "scripts/verify_deployed_runtime_target_admission.py"
    spec = importlib.util.spec_from_file_location("isolated_pin_helper", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.verify_production_ues_pin)


def test_legacy_capture_and_verify_use_original_fake_gcloud_contract(tmp_path, monkeypatch):
    calls = []
    args = SimpleNamespace(project="demo-project", region="demo-region", service="demo-service",
                           scheduler_location="demo-region", expected_sha=CANDIDATE_COMMIT,
                           expected_image_digest="sha256:" + "e" * 64, before=tmp_path / "before.json")

    def fake_gcloud(command, **kwargs):
        calls.append(command)
        assert command[0] == "gcloud"
        if command[-1] == "--format=value(status.latestCreatedRevisionName)":
            result = CANDIDATE_REV
        elif command[1:4] == ["run", "revisions", "describe"]:
            result = json.dumps({"metadata": {"name": CANDIDATE_REV, "labels": {"commit-sha": CANDIDATE_COMMIT}},
                                 "spec": {"containers": [{"image": CANDIDATE_IMAGE}]}})
        elif "get-iam-policy" in command:
            result = '{"bindings":[]}'
        elif "scheduler" in command:
            result = "[]"
        else:
            assert command[-1] == "--format=" + check.SERVICE_FORMAT
            result = json.dumps({"status": {"traffic": [{"revisionName": BASE_REV, "percent": 100}]}, "spec": {}})
        return SimpleNamespace(returncode=0, stdout=result, stderr=SENTINEL)

    # Unwrap only the legacy JSON runner, leaving real subprocesses prohibited.
    def run_json(command):
        return json.loads(fake_gcloud(command).stdout)
    monkeypatch.setattr(check, "_run_json", run_json)
    monkeypatch.setattr(check.subprocess, "run", fake_gcloud)
    args.before.write_text(json.dumps(check._snapshot(args)), encoding="utf-8")
    check._verify(args)
    assert len(calls) == 8


def snapshot_files(directory):
    return {str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*") if path.is_file()}


@pytest.mark.parametrize("variant,reason", [
    ("pass", None), ("drift", "RUNTIME_CONFIGURATION_CHANGED"),
    ("unknown-env", "UNKNOWN_ENV_LITERAL"), ("unknown-field", "UNKNOWN_FIELD"),
    ("duplicate-json", "DUPLICATE_JSON_KEY"), ("invalid-json", "INVALID_PROJECTION_FILE"),
    ("duplicate-target-key", "INVALID_TARGET"),
    ("probe-missing-port", "MISSING_FIELD"), ("probe-negative-port", "INVALID_PROBE"),
    ("unknown-egress", "INVALID_NETWORK_EGRESS"), ("ambiguous-network", "AMBIGUOUS_NETWORK"),
    ("empty-resources", "INVALID_RESOURCE_LITERAL"),
])
def test_real_cli_in_readonly_sandbox_with_fake_tools(case, tmp_path, variant, reason):
    args, baseline, candidate = case
    if variant == "drift": literal(candidate, "EXECUTION_REPORT_GCS_URI")["value"] = SENTINEL
    elif variant == "unknown-env": env(candidate).append({"name": "UNKNOWN_TOKEN", "value": SENTINEL})
    elif variant == "unknown-field": candidate["runtime"]["unknown-field"] = SENTINEL
    elif variant == "duplicate-target-key": literal(candidate, "RUNTIME_TARGET_JSON")["value"] = '{"account":"' + SENTINEL + '","account":"duplicate"}'
    for payload in (baseline, candidate):
        runtime = payload["runtime"]
        container = runtime["containers"][0]
        if variant == "probe-missing-port": container["livenessProbe"] = {"tcpSocket": {}}
        elif variant == "probe-negative-port": container["livenessProbe"] = {"tcpSocket": {"port": -1}}
        elif variant == "unknown-egress": runtime["network"] = {"egress": "UNKNOWN"}
        elif variant == "ambiguous-network": runtime["network"] = {"network": "demo-network", "vpcAccessConnector": "demo-connector"}
        elif variant == "empty-resources": container["resources"]["limits"] = {"cpu": "", "memory": ""}
    baseline_file, candidate_file = tmp_path / "baseline.json", tmp_path / "candidate.json"
    baseline_file.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_file.write_text(json.dumps(candidate), encoding="utf-8")
    if variant == "duplicate-json": candidate_file.write_text('{"identity":{},"identity":"' + SENTINEL + '"}', encoding="utf-8")
    elif variant == "invalid-json": candidate_file.write_text(SENTINEL, encoding="utf-8")
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    fake_log = tmp_path / "fake-tool-calls.log"
    for name in ("gcloud", "docker", "gh", "curl", "uv", "pip"):
        tool = bin_dir / name
        tool.write_text('#!/bin/sh\nprintf "%s\\n" "$0" >> "$FAKE_TOOL_LOG"\nexit 91\n', encoding="utf-8")
        tool.chmod(0o700)
    argv = [str(SCRIPT), "preflight-local", "--project", args.project, "--region", args.region,
            "--service", args.service, "--baseline-revision", args.baseline_revision,
            "--candidate-revision", args.candidate_revision, "--candidate-image", args.candidate_image,
            "--candidate-sha", args.candidate_sha, "--candidate-run", args.candidate_run,
            "--baseline", str(baseline_file), "--candidate", str(candidate_file),
            "--baseline-repository", str(args.baseline_repository), "--candidate-repository", str(args.candidate_repository)]
    # Real Python CLI routing, with all network, subprocess and write entrances
    # intercepted in the child. PATH fakes provide a second boundary and log.
    driver = '''
import builtins, io, json, os, runpy, socket, subprocess, sys
events = []
def forbidden(kind):
    def call(*args, **kwargs):
        events.append(kind)
        raise AssertionError("forbidden operation")
    return call
socket.socket = forbidden("socket")
socket.create_connection = forbidden("socket")
subprocess.run = forbidden("runner")
subprocess.Popen = forbidden("runner")
os.system = forbidden("runner")
real_open, real_io_open, real_os_open = builtins.open, io.open, os.open
def guarded_open(original):
    def call(file, mode="r", *args, **kwargs):
        if any(flag in mode for flag in "wax+"):
            return forbidden("write")()
        return original(file, mode, *args, **kwargs)
    return call
builtins.open = guarded_open(real_open)
io.open = guarded_open(real_io_open)
def os_open(path, flags, *args, **kwargs):
    if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
        return forbidden("write")()
    return real_os_open(path, flags, *args, **kwargs)
os.open = os_open
for name in ("mkdir", "remove", "unlink", "rmdir", "rename", "replace", "chmod", "link", "symlink", "truncate"):
    setattr(os, name, forbidden("write"))
original_import = builtins.__import__
def safe_import(name, *args, **kwargs):
    if name.split(".")[0] in {"main", "application", "strategy_registry", "us_equity_strategies", "quant_platform_kit", "schwab", "notifications"}:
        return forbidden("business-import")()
    return original_import(name, *args, **kwargs)
builtins.__import__ = safe_import
sys.argv = sys.argv[1:]
sys.path.insert(0, os.path.dirname(sys.argv[0]))
# Exercise the product's helper-import protection even when the caller permits
# bytecode writes. The filesystem guard must still observe zero write attempts.
sys.dont_write_bytecode = False
try:
    runpy.run_path(sys.argv[0], run_name="__main__")
except SystemExit as exc:
    print("SANDBOX_EVENTS=" + json.dumps(events))
    raise SystemExit(exc.code)
'''
    before_inputs = snapshot_files(tmp_path)
    # All tracked non-target paths must remain unchanged, including QRS files.
    tracked = _test_process(["git", "-c", "core.fsmonitor=false", "ls-files", "-z"], cwd=ROOT,
                       env={"PATH": os.defpath, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull}, capture_output=True, check=True).stdout.decode().split("\0")
    non_target = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in tracked if name and name not in {
        "scripts/verify_cloud_run_no_traffic_deploy.py", "scripts/verify_deployed_runtime_target_admission.py",
        "tests/test_verify_cloud_run_no_traffic_deploy.py", "tests/test_deployed_runtime_target_admission.py"}}
    result = _test_process([sys.executable, "-B", "-c", driver, *argv], cwd=ROOT, capture_output=True, text=True,
                      env={"PATH": str(bin_dir), "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1",
                           "TMPDIR": str(tmp_path), "FAKE_TOOL_LOG": str(fake_log)})
    assert result.returncode == (0 if reason is None else 1), result.stderr
    assert "SANDBOX_EVENTS=[]" in result.stdout
    if reason:
        assert result.stderr.strip() == "Local projection preflight stopped: " + reason
    else:
        assert "cloud authenticity and installed dependencies remain unverified" in result.stdout
        assert result.stderr == ""
    assert SENTINEL not in result.stdout + result.stderr
    assert not fake_log.exists()
    assert before_inputs == snapshot_files(tmp_path)
    assert non_target == {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in non_target}


@pytest.mark.parametrize("handler", ["tcpSocket", "grpc"])
def test_structure_inventory_does_not_select_a_startup_policy(case, handler):
    for payload in case[1:]:
        payload["runtime"]["containers"][0]["startupProbe"] = {handler: {"port": 8080}}
    trap_all_literals(case)
    for payload in case[1:]:
        check._structure_inventory(payload)
        with pytest.raises(check.PreflightError, match="^HTTP_STARTUP_GUARD_REQUIRED$"):
            check._inventory(payload)


@pytest.mark.parametrize("handler", ["tcpSocket", "grpc"])
@pytest.mark.parametrize("exact_identity", [False, True])
def test_default_preflight_rejects_non_http_even_with_exact_00781_identity(case, handler, exact_identity):
    for payload in case[1:]:
        payload["runtime"]["containers"][0]["startupProbe"] = {
            "failureThreshold": 1, "periodSeconds": 240, handler: {"port": 8080}, "timeoutSeconds": 240,
        }
        if exact_identity:
            payload["identity"].update(project="charlesschwabquant", region="us-central1",
                                       service="charles-schwab-quant-service",
                                       revision="charles-schwab-quant-service-00781-lel")
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^HTTP_STARTUP_GUARD_REQUIRED$"):
        check._preflight_local(*case)


def test_default_http_failure_keeps_original_precedence_over_volume_semantics(case):
    for payload in case[1:]:
        payload["runtime"]["containers"][0]["startupProbe"] = {"tcpSocket": {"port": 8080}}
        payload["runtime"]["volumes"] = [{"name": "cache", "emptyDir": {}}, {"name": "cache", "emptyDir": {}}]
    trap_all_literals(case)
    with pytest.raises(check.PreflightError, match="^HTTP_STARTUP_GUARD_REQUIRED$"):
        check._preflight_local(*case)


def native_snapshot():
    traffic = [{"revisionName": BASE_REV, "percent": 100, "tag": "stable"},
               {"revisionName": "demo-service-old", "tag": "keep-zero"}]
    meta = dict(name="demo-service", namespace="demo-project", uid="service-uid", resourceVersion="service-rv", generation=1)
    spec = dict(serviceAccountName="demo@example.invalid", containers=[{
        "image": BASE_IMAGE, "env": [{"name": "SCHWAB_CASH_CURRENCY", "value": "USD"}],
        "ports": [{"containerPort": 8080}], "startupProbe": {"tcpSocket": {"port": 8080}},
    }])
    return {"service": {"metadata": meta, "spec": {"traffic": traffic},
                        "status": {"traffic": copy.deepcopy(traffic), "observedGeneration": 1}},
            "revision": {"metadata": dict(meta, name=BASE_REV, uid="revision-uid", resourceVersion="revision-rv"),
                         "spec": spec, "status": {"imageDigest": BASE_IMAGE, "observedGeneration": 1,
                         "conditions": [{"type": k, "status": "True"} for k in ("Ready", "ContainerHealthy")]}}}


def test_native_readback_keeps_zero_tags_and_exact_revision():
    before = native_snapshot(); after = copy.deepcopy(before)
    template = {"metadata": {"name": CANDIDATE_REV, "labels": {"commit-sha": CANDIDATE_COMMIT}},
                "spec": copy.deepcopy(before["revision"]["spec"])}
    template["spec"]["containers"][0]["image"] = CANDIDATE_IMAGE
    after["revision"]["metadata"].update(template["metadata"])
    after["revision"]["spec"] = template["spec"]
    after["revision"]["status"]["imageDigest"] = CANDIDATE_IMAGE
    check.verify_native_readback(before, {"spec": {"template": template}}, **after)
    after["service"]["status"]["traffic"][1]["tag"] = "different"
    with pytest.raises(check.PreflightError, match="NATIVE_TRAFFIC_CHANGED"):
        check.verify_native_readback(before, {"spec": {"template": template}}, **after)


@pytest.mark.parametrize("condition", ["pending", "false", "generation", "wrong-image", "wrong-candidate", "sidecar-drift"])
def test_native_startup_needs_exact_candidate_observed_generation_and_conditions(condition):
    before = native_snapshot(); after = copy.deepcopy(before)
    request = {"spec": {"template": {"metadata": {"name": BASE_REV}, "spec": copy.deepcopy(before["revision"]["spec"])}}}
    if condition == "pending": after["revision"]["status"]["conditions"] = []
    elif condition == "false": after["revision"]["status"]["conditions"][0]["status"] = "False"
    elif condition == "generation": after["service"]["status"]["observedGeneration"] = 0
    elif condition == "wrong-image": after["revision"]["status"]["imageDigest"] = CANDIDATE_IMAGE
    elif condition == "wrong-candidate": after["revision"]["metadata"]["name"] = CANDIDATE_REV
    else: after["revision"]["spec"]["containers"][0]["resources"] = {"limits": {"cpu": "2"}}
    with pytest.raises(check.PreflightError): check.verify_native_readback(before, request, **after)


def test_native_capture_unknown_literal_stops_before_any_value_projection():
    commands = []
    def read(command):
        commands.append(command)
        fmt = command[-1]
        if "env[].name" in fmt:
            return {"metadata": {"uid": "u", "resourceVersion": "r"},
                    "spec": {"containers": [{"env": [{"name": "PRIVATE_UNKNOWN"}]}]}}
        return {}
    identity = dict(project="demo-project", region="demo-region", service="demo-service", baseline_revision=BASE_REV)
    with pytest.raises(check.PreflightError, match="UNKNOWN_ENV_LITERAL"):
        check.capture_native_snapshot(identity, read=read)
    assert not any("extract(value)" in cmd[-1] or "json(metadata," in cmd[-1] for cmd in commands)
    assert not any("template.spec" in cmd[-1] for cmd in commands)


def test_native_capture_unknown_annotation_stops_before_metadata_values():
    commands = []
    def read(command):
        commands.append(command)
        if "annotations.list(show=keys)" in command[-1]:
            return {"metadata": {"annotations": "kubectl.kubernetes.io/last-applied-configuration"}}
        return {}
    identity = dict(project="demo-project", region="demo-region", service="demo-service", baseline_revision=BASE_REV)
    with pytest.raises(check.PreflightError, match="NATIVE_UNSUPPORTED_ANNOTATION"):
        check.capture_native_snapshot(identity, read=read)
    assert not any("json(metadata," in cmd[-1] for cmd in commands)


def test_native_capture_unknown_config_field_stops_name_inventory():
    def read(command):
        return {"spec": "containers,unsupported"} if command[-1] == "--format=json(spec.list(show=keys))" else {}
    identity = dict(project="demo-project", region="demo-region", service="demo-service", baseline_revision=BASE_REV)
    with pytest.raises(check.PreflightError, match="NATIVE_UNSUPPORTED_CONFIGURATION_FIELD"):
        check.capture_native_snapshot(identity, read=read)


@pytest.mark.parametrize("drift", [None, "revision", "service"])
def test_native_capture_value_projection_is_allowlisted_and_uid_rv_bracketed(drift):
    expected = native_snapshot(); commands = []
    def read(command):
        commands.append(command)
        fmt = command[-1]
        revision = command[2] == "revisions"
        obj = expected["revision" if revision else "service"]
        if fmt == "--format=json(metadata.uid,metadata.resourceVersion)":
            meta = {k: obj["metadata"][k] for k in ("uid", "resourceVersion")}
            if drift == ("revision" if revision else "service"): meta["resourceVersion"] += "changed"
            return {"metadata": meta}
        if "json(metadata," in fmt:
            result = copy.deepcopy(obj)
            if revision: del result["spec"]["containers"][0]["env"][0]["value"]
            return result
        if "env[].name" in fmt:
            return {"metadata": {k: obj["metadata"][k] for k in ("uid", "resourceVersion")},
                    "spec": {"containers": [{"env": [{"name": "SCHWAB_CASH_CURRENCY"}]}]}}
        if "extract(value)" in fmt:
            assert 'filter("name=SCHWAB_CASH_CURRENCY")' in fmt
            return {"spec": {"containers": [{"env": [["USD"]]}]}}
        return {}
    identity = dict(project="demo-project", region="demo-region", service="demo-service", baseline_revision=BASE_REV)
    if drift:
        with pytest.raises(check.PreflightError, match="NATIVE_OBJECT_CHANGED_DURING_CAPTURE"):
            check.capture_native_snapshot(identity, read=read)
    else: assert check.capture_native_snapshot(identity, read=read) == expected
    assert not any("template.spec" in c[-1] or "env[].value," in c[-1] for c in commands)
