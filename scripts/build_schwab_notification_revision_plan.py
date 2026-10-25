#!/usr/bin/env python3
"""Build a review-only synthetic revision; never render an API Service or apply it.

The closed model is deliberately smaller than Cloud Run. Literal env payloads
are opaque, byte-preserved strings, not business admission evidence. The sole
excluded_desired_template slot is never accessed, even for hashing or copying.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import sys

if __package__:
    from . import verify_cloud_run_no_traffic_deploy as a
else:
    import verify_cloud_run_no_traffic_deploy as a


MODEL = "schwab-local-review-v1"
_IDENTITY = {"project": str, "region": str, "service": str}
_LABELS = {"commit-sha": str, "run-id": str, "purpose": str}
_PROVENANCE = {"commit": str, "run": str}
_METADATA = {"name": str, "uid": str, "labels": _LABELS}
_SERVICE = {
    "identity": _IDENTITY,
    "metadata": _METADATA | {"resourceVersion": str},
    "provenance": _PROVENANCE,
    "traffic": a._PROJECTION["traffic"],
}
_REVISION = {
    "identity": _IDENTITY | {"revision": str}, "metadata": _METADATA,
    "provenance": _PROVENANCE, "runtime": a._RUNTIME,
}
_SNAPSHOT = {"service": _SERVICE, "revision": _REVISION}
_EXPECTED = dict.fromkeys((
    "project", "region", "service", "baseline_revision", "candidate_revision",
    "candidate_image", "resource_version",
), str)
_ALIASES = {"latest", "latest-created", "latest-ready"}
HTTP_STARTUP_POLICY = "http-health-8080"
TCP_00781_STARTUP_POLICY = "schwab-00781-tcp-preserve"
_TCP_00781_IDENTITY = {
    "project": "charlesschwabquant", "region": "us-central1", "service": "charles-schwab-quant-service",
    "baseline_revision": "charles-schwab-quant-service-00781-lel",
}
_TCP_00781_IMAGE = (
    "us-central1-docker.pkg.dev/charlesschwabquant/cloud-run-source-deploy/"
    "charlesschwabplatform/charles-schwab-quant-service@sha256:"
    "cd4779376abf0ec4a54c0b04c690adc953756cda782ecd8adcf773e659a1733a"
)
_TCP_00781_SOURCE = "6c890e3fdfbc0f8dcd7f96b5a634cc482ba3606c"
_TCP_00781_PROBE = {
    "failureThreshold": 1, "periodSeconds": 240, "tcpSocket": {"port": 8080}, "timeoutSeconds": 240,
}


class PlanError(ValueError):
    """Only fixed, non-sensitive reason codes leave this module."""


def _stop(reason: str) -> None:
    raise PlanError(reason)


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("ascii")).hexdigest()


def _required(value: object, schema: dict) -> None:
    a._keys(value, schema, schema)


def _nonempty(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _name(value: object, limit: int = 63) -> bool:
    return (type(value) is str and len(value) <= limit
            and re.fullmatch(r"[a-z](?:[a-z0-9-]*[a-z0-9])?", value) is not None)


def _revision_name(value: object, service: str) -> bool:
    return (_name(value) and value not in _ALIASES
            and value.startswith(service + "-")
            and value[len(service) + 1:] not in _ALIASES)


def _image(value: object) -> bool:
    return (type(value) is str and re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9./_-]*@sha256:[0-9a-f]{64}", value) is not None)


def _startup_policy(policy: object) -> None:
    if type(policy) is not str or policy not in {HTTP_STARTUP_POLICY, TCP_00781_STARTUP_POLICY}:
        _stop("INVALID_STARTUP_POLICY")


def _expected(expected: object, startup_policy: str = HTTP_STARTUP_POLICY) -> None:
    _required(expected, _EXPECTED)
    a._shape(expected, _EXPECTED)
    if (not all(_name(expected[key]) for key in _IDENTITY)
            or not _name(expected["service"], 49)):
        _stop("INVALID_EXPECTED_IDENTITY")
    for key in ("baseline_revision", "candidate_revision"):
        if not _revision_name(expected[key], expected["service"]):
            _stop("INVALID_EXPECTED_REVISION")
    if expected["baseline_revision"] == expected["candidate_revision"]:
        _stop("AMBIGUOUS_REVISION_IDENTITY")
    if not _image(expected["candidate_image"]):
        _stop("INVALID_IMMUTABLE_IMAGE")
    if not _nonempty(expected["resource_version"]):
        _stop("INVALID_RESOURCE_VERSION")
    if startup_policy == TCP_00781_STARTUP_POLICY and any(
            expected[key] != value for key, value in _TCP_00781_IDENTITY.items()):
        _stop("TCP_BASELINE_IDENTITY_MISMATCH")


def _projection(snapshot: dict) -> dict:
    revision = snapshot["revision"]
    return {"identity": revision["identity"], "provenance": revision["provenance"],
            "runtime": revision["runtime"], "traffic": snapshot["service"]["traffic"]}


def _inventory(snapshot: object, startup_policy: str = HTTP_STARTUP_POLICY) -> None:
    # A never reads literal values during inventory. Inventory BOTH snapshots
    # before any subsequent equality, hash, deepcopy or literal type check.
    a._shape(snapshot, _SNAPSHOT)
    _required(snapshot, _SNAPSHOT)
    for key, schema in _SNAPSHOT.items():
        part = snapshot[key]
        _required(part, schema)
        for field in ("identity", "metadata", "provenance"):
            _required(part[field], schema[field])
        _required(part["metadata"]["labels"], _LABELS)
    projection = _projection(snapshot)
    if startup_policy == HTTP_STARTUP_POLICY:
        a._inventory(projection)
    else:
        a._structure_inventory(projection)
        if projection["runtime"]["containers"][0].get("startupProbe") != _TCP_00781_PROBE:
            _stop("TCP_BASELINE_PROBE_MISMATCH")


def _traffic(rows: list, expected: dict) -> None:
    seen, tags = set(), set()
    total, baseline_percent = 0, 0
    for row in rows:
        a._keys(row, {"revisionName", "percent", "tag"}, {"revisionName", "percent"})
        revision, percent = row["revisionName"], row["percent"]
        tag = row.get("tag")
        if (not _revision_name(revision, expected["service"]) or type(percent) is not int
                or not 0 <= percent <= 100 or (tag is not None and not _name(tag))):
            _stop("INVALID_TRAFFIC")
        if (revision, tag) in seen or (tag is not None and tag in tags):
            _stop("AMBIGUOUS_TRAFFIC")
        seen.add((revision, tag))
        if tag is not None:
            tags.add(tag)
        if revision == expected["candidate_revision"]:
            _stop("CANDIDATE_ROUTE_PRESENT")
        total += percent
        if revision == expected["baseline_revision"]:
            baseline_percent += percent
    if total != 100 or baseline_percent <= 0:
        _stop("BASELINE_NOT_SERVING")


def _snapshot_values(snapshot: dict, expected: dict, startup_policy: str = HTTP_STARTUP_POLICY, *, candidate: bool = False) -> None:
    revision_name = expected["candidate_revision" if candidate else "baseline_revision"]
    if snapshot["revision"]["identity"]["revision"] != revision_name:
        _stop("BASELINE_IDENTITY_MISMATCH" if not candidate else "CANDIDATE_IDENTITY_MISMATCH")
    for part_name in ("service", "revision"):
        part = snapshot[part_name]
        if any(part["identity"][key] != expected[key] for key in _IDENTITY):
            _stop("IDENTITY_MISMATCH")
        if part["metadata"]["name"] != (expected["service"] if part_name == "service" else revision_name):
            _stop("IDENTITY_MISMATCH")
        if not _nonempty(part["metadata"]["uid"]):
            _stop("INVALID_UID")
        if any(not _nonempty(value) for value in part["metadata"]["labels"].values()):
            _stop("INVALID_METADATA")
        if (not re.fullmatch(r"[0-9a-f]{40}", part["provenance"]["commit"])
                or not re.fullmatch(r"[A-Za-z0-9_-]+", part["provenance"]["run"])):
            _stop("INVALID_PROVENANCE")
    version = snapshot["service"]["metadata"]["resourceVersion"]
    if not _nonempty(version):
        _stop("INVALID_RESOURCE_VERSION")
    if version != expected["resource_version"]:
        _stop("RESOURCE_VERSION_MISMATCH")
    _traffic(snapshot["service"]["traffic"], expected)
    runtime = snapshot["revision"]["runtime"]
    if startup_policy == TCP_00781_STARTUP_POLICY:
        # These are exact LOCAL declarations, never proof of cloud origin or
        # installed source. UID/RV and full equality still bind both snapshots.
        revision = snapshot["revision"]
        if not candidate and runtime["containers"][0]["image"] != _TCP_00781_IMAGE:
            _stop("TCP_BASELINE_IMAGE_MISMATCH")
        if (revision["provenance"]["commit"] != _TCP_00781_SOURCE
                or revision["metadata"]["labels"]["commit-sha"] != _TCP_00781_SOURCE):
            _stop("TCP_BASELINE_SOURCE_MISMATCH")
    if (not re.fullmatch(r"[A-Za-z0-9._-]+@[A-Za-z0-9.-]+", runtime["serviceAccountName"])
            or runtime["ingress"] not in {"all", "internal", "internal-and-cloud-load-balancing"}
            or runtime["executionEnvironment"] not in {"gen1", "gen2"}
            or not 1 <= runtime["containerConcurrency"] <= 1000 or runtime["timeoutSeconds"] <= 0
            or runtime["scaling"]["minInstanceCount"] < 0
            or runtime["scaling"]["maxInstanceCount"] < max(1, runtime["scaling"]["minInstanceCount"])):
        _stop("INVALID_RUNTIME")
    for container in runtime["containers"]:
        if not _image(container["image"]):
            _stop("INVALID_IMMUTABLE_IMAGE")
        if any(not 1 <= port["containerPort"] <= 65535 for port in container["ports"]):
            _stop("INVALID_PORT")
        for entry in container["env"]:
            if "value" in entry and type(entry["value"]) is not str:
                _stop("INVALID_ENV_LITERAL")
    if 8080 not in [port["containerPort"] for port in runtime["containers"][0]["ports"]]:
        _stop("HTTP_STARTUP_GUARD_REQUIRED" if startup_policy == HTTP_STARTUP_POLICY else "TCP_BASELINE_PROBE_MISMATCH")


def _review_candidate(before: dict, candidate: dict, expected: dict, startup_policy: str = HTTP_STARTUP_POLICY) -> dict:
    _startup_policy(startup_policy)
    _expected(expected, startup_policy)
    _inventory(before, startup_policy)
    _inventory(candidate, startup_policy)
    _snapshot_values(before, expected, startup_policy)
    _snapshot_values(candidate, expected, startup_policy, candidate=True)
    old_image = before["revision"]["runtime"]["containers"][0]["image"]
    new_image = candidate["revision"]["runtime"]["containers"][0]["image"]
    if new_image != expected["candidate_image"]:
        _stop("CANDIDATE_IMAGE_MISMATCH")
    if new_image == old_image:
        _stop("IMAGE_UNCHANGED")
    restored = copy.deepcopy(candidate)
    restored["revision"]["identity"]["revision"] = expected["baseline_revision"]
    restored["revision"]["metadata"]["name"] = expected["baseline_revision"]
    restored["revision"]["runtime"]["containers"][0]["image"] = old_image
    if restored != before:
        _stop("PROTECTED_CONFIGURATION_CHANGED")
    changes = []
    for path, old, new in (
        ("revision.identity.revision", expected["baseline_revision"], expected["candidate_revision"]),
        ("revision.metadata.name", expected["baseline_revision"], expected["candidate_revision"]),
        ("revision.runtime.containers[0].image", old_image, new_image),
    ):
        changes.append({"path": path, "comparison": "changed", "before_sha256": _hash(old), "after_sha256": _hash(new)})
    review = {
        "model": MODEL, "mode": "review_only", "deployment_ready": False,
        "old_installation": "UNKNOWN", "startup_side_effects": "UNVERIFIED",
        "source": "NOT_VERIFIED", "admission": "NOT_VERIFIED", "cloud_configuration": "NOT_VERIFIED",
        "concurrency": "MATCHED_LOCAL_SNAPSHOTS_NO_CAS",
        "excluded_desired_template": "NOT_ACCESSED", "changes": changes,
        "preserved": [{"path": "service", "comparison": "equal", "sha256": _hash(before["service"])},
                      {"path": "revision_except_image_and_names", "comparison": "equal", "sha256": _hash(restored["revision"])}],
        "coverage": "CLOSED_SYNTHETIC_ENVELOPE_ONLY_OPAQUE_ENV_STRINGS",
    }
    if startup_policy == TCP_00781_STARTUP_POLICY:
        review["startup_assurance"] = {
            "policy": startup_policy, "readiness": "TCP_PORT_READINESS_ONLY",
            "http_application_health": "NOT_VERIFIED", "runtime_startup": "NOT_EXECUTED",
            "baseline_binding": "LOCAL_DECLARATION_ONLY_NOT_CLOUD_PROVENANCE",
        }
    return review


def review_candidate(before: dict, candidate: dict, expected: dict, *, startup_policy: str = HTTP_STARTUP_POLICY) -> dict:
    """Review an in-memory candidate without weakening probe or preservation guards."""
    try:
        return _review_candidate(before, candidate, expected, startup_policy)
    except PlanError:
        raise
    except a.PreflightError as exc:
        raise PlanError(str(exc)) from None
    except (ValueError, TypeError, KeyError, RecursionError):
        _stop("INVALID_INPUT")


def build_plan(request: dict) -> tuple[dict, dict]:
    """Return a detached candidate and redacted plan; never inspect desired data."""
    try:
        fields = {"model", "expected", "before", "current", "excluded_desired_template"}
        a._keys(request, fields | {"startup_policy"}, fields)
        if request["model"] != MODEL:
            _stop("INVALID_MODEL")
        startup_policy = request["startup_policy"] if "startup_policy" in request else HTTP_STARTUP_POLICY
        _startup_policy(startup_policy)
        expected = request["expected"]
        _expected(expected, startup_policy)
        before, current = request["before"], request["current"]
        if before is current:
            _stop("INDEPENDENT_SNAPSHOTS_REQUIRED")
        _inventory(before, startup_policy)
        _inventory(current, startup_policy)
        _snapshot_values(before, expected, startup_policy)
        _snapshot_values(current, expected, startup_policy)
        for key in ("service", "revision"):
            if before[key]["metadata"]["uid"] != current[key]["metadata"]["uid"]:
                _stop("OBJECT_RECREATED")
        if before != current:
            _stop("CONCURRENT_SNAPSHOT_CHANGED")
        candidate = {"service": copy.deepcopy(current["service"]), "revision": copy.deepcopy(before["revision"])}
        candidate["revision"]["identity"]["revision"] = expected["candidate_revision"]
        candidate["revision"]["metadata"]["name"] = expected["candidate_revision"]
        candidate["revision"]["runtime"]["containers"][0]["image"] = expected["candidate_image"]
        return candidate, _review_candidate(before, candidate, expected, startup_policy)
    except PlanError:
        raise
    except a.PreflightError as exc:
        raise PlanError(str(exc)) from None
    except (ValueError, TypeError, KeyError, RecursionError):
        _stop("INVALID_INPUT")


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            _stop("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _invalid_constant(_value: str) -> None:
    _stop("INVALID_JSON")


def load_request(path: Path) -> object:
    """Read one explicitly named local JSON file, with fixed diagnostics only."""
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_pairs, parse_constant=_invalid_constant)
    except PlanError:
        raise
    except (OSError, ValueError, RecursionError):
        _stop("INVALID_INPUT_FILE")


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        _stop("INVALID_ARGUMENTS")

# Native publishing is separate from the closed synthetic review model above.
# It never imports business code, changes a runtime target, or sends an app probe.
NOTIFICATION_SOURCE = "e159e6cc07f9a9eea417b810674f934eb5291e6f"
OLD_UES_PIN = "4a3943883cd6b5bbfe32a559e56a91b40a81b7ce"
_DEPENDENCY_HASHES = {
    "uv.lock": "5bead5ad0b79876f74dd6b02ab29538e5cc5ace2420c15f6222e77c7f458a66f",
    "pyproject.toml": "2a943c7217b46912e6da0ac1bc382e4a2c9fff644aadf0da5077ba1d911fe780",
    "qsl.toml": "979b31053ff222a496e6b39718b0f1b412fa317ad2db009807deb4efedaf0807",
}
_OVERLAY_EXISTING = frozenset({
    "application/account_new_risk_gate_support.py", "application/execution_service.py",
    "application/rebalance_service.py", "application/runtime_report_summary.py",
    "application/runtime_strategy_adapters.py", "main.py",
})
_OVERLAY_REQUIRED = _OVERLAY_EXISTING | {
    "notifications/display_names.json", "notifications/digest_snapshot/provenance.json",
    "scripts/execution_report_heartbeat.py", "scripts/emit_unified_digest_candidates.py",
}
_OUTPUT_LABELS = a.NATIVE_OUTPUT_LABELS
_OUTPUT_ANNOTATIONS = a.NATIVE_OUTPUT_ANNOTATIONS


def build_native_plan(before: dict, current: dict, expected: dict) -> tuple[dict, dict]:
    """Construct one native v1 Service guarded by its ORIGINAL resourceVersion.

    A supported native snapshot is required; a closed-model projection or a
    desired template is never a source for the new revision's configuration.
    """
    try:
        a.native_snapshot_inventory(before)
        a.native_snapshot_inventory(current)
        _required(expected, _EXPECTED | {"candidate_sha": str, "candidate_run": str})
        _expected({k: expected[k] for k in _EXPECTED}, TCP_00781_STARTUP_POLICY)
        if expected["candidate_sha"] != NOTIFICATION_SOURCE or not re.fullmatch(r"[0-9]+", expected["candidate_run"]):
            _stop("NATIVE_SOURCE_OR_RUN_MISMATCH")
        for snap in (before, current):
            for part, name in (("service", expected["service"]), ("revision", expected["baseline_revision"])):
                meta = snap[part]["metadata"]
                if (meta["name"] != name or not (meta["namespace"] == expected["project"] or re.fullmatch(r"[1-9][0-9]*", meta["namespace"]))
                        or not _nonempty(meta["uid"]) or not _nonempty(meta["resourceVersion"])):
                    _stop("NATIVE_IDENTITY_MISMATCH")
            if snap["service"]["metadata"]["namespace"] != snap["revision"]["metadata"]["namespace"]:
                _stop("NATIVE_NAMESPACE_MISMATCH")
            if snap["service"]["metadata"]["resourceVersion"] != expected["resource_version"]:
                _stop("RESOURCE_VERSION_MISMATCH")
            revision = snap["revision"]
            main_container = revision["spec"]["containers"][0]
            if (main_container["image"] != _TCP_00781_IMAGE
                    or revision["metadata"].get("labels", {}).get("commit-sha") != _TCP_00781_SOURCE):
                _stop("NATIVE_BASELINE_PROVENANCE_MISMATCH")
            if main_container["startupProbe"] != _TCP_00781_PROBE:
                _stop("TCP_BASELINE_PROBE_MISMATCH")
            # Missing means the default check is enabled; retain the old metadata.
            if revision["metadata"].get("annotations", {}).get("run.googleapis.com/health-check-disabled", "false") != "false":
                _stop("NATIVE_STARTUP_CHECK_NOT_EXPLICITLY_ENABLED")
            _traffic(a.native_routes(snap["service"]["spec"]["traffic"]), expected)
            if a.native_routes(snap["service"]["status"]["traffic"], status=True) != a.native_routes(snap["service"]["spec"]["traffic"]):
                _stop("NATIVE_TRAFFIC_NOT_OBSERVED")
            if sum(row["percent"] for row in a.native_routes(snap["service"]["spec"]["traffic"])
                   if row["revisionName"] == expected["baseline_revision"]) != 100:
                _stop("NATIVE_BASELINE_NOT_100_PERCENT")
            targets = [entry["value"] for entry in main_container["env"]
                       if entry["name"] in {"RUNTIME_TARGET_JSON", "QSL_RUNTIME_TARGET_JSON"} and "value" in entry]
            if len(targets) != 1:
                _stop("NATIVE_TARGET_BINDING_UNAVAILABLE")
            target = json.loads(targets[0], object_pairs_hook=_pairs, parse_constant=_invalid_constant)
            if (target.get("strategy_release", {}).get("strategy_revision") != OLD_UES_PIN
                    or target.get("runtime_risk_limits", {}).get("binding", {}).get("ues_revision") != OLD_UES_PIN):
                _stop("NATIVE_OLD_UES_BINDING_MISMATCH")
            for container in revision["spec"]["containers"]:
                if not _image(container["image"]): _stop("INVALID_IMMUTABLE_IMAGE")
                if container.get("command") or container.get("args"): _stop("NATIVE_COMMAND_OVERRIDE_UNSUPPORTED")
                for entry in container.get("env", []):
                    if "value" in entry and type(entry["value"]) is not str: _stop("INVALID_ENV_LITERAL")
        if before is current: _stop("INDEPENDENT_SNAPSHOTS_REQUIRED")
        if before != current: _stop("CONCURRENT_SNAPSHOT_CHANGED")
        revision_meta = current["revision"]["metadata"]
        labels = {k: v for k, v in revision_meta.get("labels", {}).items() if k not in _OUTPUT_LABELS}
        labels.update({"commit-sha": NOTIFICATION_SOURCE, "github-run-id": expected["candidate_run"]})
        if "run-id" in labels: labels["run-id"] = expected["candidate_run"]
        # Old build provenance describes the base, not this overlay source.
        annotations = {k: v for k, v in revision_meta.get("annotations", {}).items()
                       if k not in _OUTPUT_ANNOTATIONS and not k.startswith("run.googleapis.com/build-")}
        spec = copy.deepcopy(current["revision"]["spec"])
        spec["containers"][0]["image"] = expected["candidate_image"]
        if expected["candidate_image"] == _TCP_00781_IMAGE: _stop("IMAGE_UNCHANGED")
        service_meta = current["service"]["metadata"]
        metadata = {k: copy.deepcopy(service_meta[k]) for k in ("name", "namespace", "resourceVersion", "labels", "annotations") if k in service_meta}
        request = {"apiVersion": "serving.knative.dev/v1", "kind": "Service", "metadata": metadata,
                   "spec": {"template": {"metadata": {"name": expected["candidate_revision"], "labels": labels,
                                                         "annotations": annotations}, "spec": spec},
                            "traffic": copy.deepcopy(current["service"]["spec"]["traffic"])}}
        return request, {"mode": "native_original_rv", "deployment_ready": False,
            "source": NOTIFICATION_SOURCE, "source_is_tool_main": False,
            "protected_baseline_sha256": _hash(current), "request_sha256": _hash(request),
            "allowed_changes": ["template.name", "template.spec.containers[0].image", "template.source_provenance"],
            "service_desired_template": "REPLACED_FROM_EXPLICIT_BASELINE_NOT_FROM_DESIRED",
            "traffic_and_all_tags": "PRESERVED", "effective_notification_flags": "UNVERIFIED",
            "runtime_marker_CAS": "UNVERIFIED", "startup": "NOT_EXECUTED_TCP_ONLY"}
    except PlanError:
        raise
    except a.PreflightError as exc:
        raise PlanError(str(exc)) from None
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        _stop("INVALID_NATIVE_INPUT")


def replace_native_service(request: dict, expected: dict, *, session=None) -> None:
    """One native PUT. No gcloud replace, automatic retry, response dump or rebase."""
    owned = session is None
    if request.get("metadata", {}).get("resourceVersion") != expected["resource_version"]:
        _stop("RESOURCE_VERSION_MISMATCH")
    _expected({k: expected[k] for k in _EXPECTED}, TCP_00781_STARTUP_POLICY)
    response = None
    try:
        if owned:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession
            credentials, _project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            session = AuthorizedSession(credentials, max_refresh_attempts=0)
        url = (f"https://{expected['region']}-run.googleapis.com/apis/serving.knative.dev/v1/"
               f"namespaces/{expected['project']}/services/{expected['service']}")
        response = session.request("PUT", url, json=request, timeout=60, allow_redirects=False, stream=True)
        status = response.status_code
    except Exception:
        _stop("NATIVE_WRITE_UNCERTAIN_DO_NOT_RETRY")
    finally:
        if response is not None: response.close()
        if owned and session is not None: session.close()
    if status in {409, 412}: _stop("NATIVE_RESOURCE_VERSION_CONFLICT")
    if status != 200: _stop("NATIVE_WRITE_UNCERTAIN_DO_NOT_RETRY")


def publish_native(before: dict, expected: dict, *, read=None, capture=None, replace=None, pause=None, attempts: int = 24) -> dict:
    """Read fresh, write once, then wait only on exact candidate control-plane GETs."""
    import time
    read = read or a._run_json
    capture = capture or a.capture_native_snapshot
    replace = replace or replace_native_service
    pause = pause or time.sleep
    if type(attempts) is not int or not 1 <= attempts <= 24: _stop("INVALID_POLL_BOUND")
    current = capture(expected, read=read)
    request, review = build_native_plan(before, current, expected)
    candidates = read(["gcloud", "run", "revisions", "list", f"--project={expected['project']}",
                       f"--region={expected['region']}", f"--filter=metadata.name={expected['candidate_revision']}",
                       "--format=json(metadata.name)"])
    if candidates != []: _stop("NATIVE_CANDIDATE_ALREADY_EXISTS_OR_AMBIGUOUS")
    replace(request, expected)
    candidate_identity = dict(expected, baseline_revision=expected["candidate_revision"])
    for index in range(attempts):
        after = capture(candidate_identity, read=read)
        try:
            a.verify_native_readback(before, request, after["service"], after["revision"])
        except a.PreflightError as exc:
            if str(exc) != "NATIVE_STARTUP_PENDING": raise PlanError(str(exc)) from None
            if index == attempts - 1: _stop("NATIVE_STARTUP_NOT_OBSERVED_DO_NOT_REDEPLOY")
            pause(15)
        else:
            return review | {"startup": "CONTROL_PLANE_TCP_READY_ONLY", "application_delivery": "NOT_VERIFIED"}
    _stop("NATIVE_STARTUP_NOT_OBSERVED_DO_NOT_REDEPLOY")


def _git_bytes(repository: Path, *args: str) -> bytes:
    import subprocess
    result = subprocess.run(["git", "-C", str(repository), *args], capture_output=True, check=False)
    if result.returncode: _stop("NATIVE_SOURCE_GIT_UNAVAILABLE")
    return result.stdout


def prepare_notification_overlay(repository: Path, destination: Path, *, git=None) -> dict:
    """Only exact reviewed Git blobs enter the context; no workspace/auth files."""
    git = git or (lambda *args: _git_bytes(repository, *args))
    if destination.exists(): _stop("OVERLAY_DESTINATION_EXISTS")
    if git("rev-parse", "HEAD").decode().strip() != NOTIFICATION_SOURCE: _stop("OVERLAY_SOURCE_MISMATCH")
    if git("merge-base", _TCP_00781_SOURCE, NOTIFICATION_SOURCE).decode().strip() != _TCP_00781_SOURCE: _stop("OVERLAY_BASE_MISMATCH")
    for name, digest in _DEPENDENCY_HASHES.items():
        blob = git("show", NOTIFICATION_SOURCE + ":" + name)
        if hashlib.sha256(blob).hexdigest() != digest or blob != git("show", _TCP_00781_SOURCE + ":" + name):
            _stop("OVERLAY_DEPENDENCY_CHANGED")
    rows = git("diff", "--name-status", "--no-renames", _TCP_00781_SOURCE, NOTIFICATION_SOURCE).decode().splitlines()
    paths = []
    for row in rows:
        status, name = row.split("\t")
        runtime = (name in _OVERLAY_EXISTING or name in _OVERLAY_REQUIRED
                   or re.fullmatch(r"notifications/(?:[a-z_]+/)*[a-z_]+\.py", name))
        documentation = re.fullmatch(r"(?:tests|docs)/[A-Za-z0-9_./-]+", name) or name in {"README.md", "CHANGELOG.md", "MANIFEST.in", ".github/workflows/execution-report-heartbeat.yml"}
        if not runtime:
            if documentation: continue
            _stop("OVERLAY_UNREVIEWED_PATH")
        if status not in {"A", "M"}: _stop("OVERLAY_DELETE_OR_RENAME")
        if ".." in Path(name).parts: _stop("OVERLAY_INVALID_PATH")
        record = git("ls-tree", NOTIFICATION_SOURCE, "--", name).decode().split()
        if len(record) != 4 or record[0] not in {"100644", "100755"} or record[1] != "blob":
            _stop("OVERLAY_NOT_REGULAR_FILE")
        paths.append(name)
    if not _OVERLAY_REQUIRED <= set(paths): _stop("OVERLAY_REQUIRED_ASSET_MISSING")
    blobs = {name: git("show", NOTIFICATION_SOURCE + ":" + name) for name in paths}
    destination.mkdir(parents=True, mode=0o700)
    for name, blob in blobs.items():
        output = destination / name
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(blob)
    # Docker's JSON COPY form: no shell expansion, no RUN, no dependency install.
    recipe = "FROM " + _TCP_00781_IMAGE + "\n" + "".join(
        "COPY " + json.dumps([name, "/app/" + name]) + "\n" for name in sorted(blobs))
    (destination / "Dockerfile").write_text(recipe, encoding="utf-8")
    return {"source": NOTIFICATION_SOURCE, "base": _TCP_00781_SOURCE, "files_sha256": {
        name: hashlib.sha256(blob).hexdigest() for name, blob in sorted(blobs.items())},
        "build": "NOT_EXECUTED", "installed_dependencies": "PRESERVED_BY_BASE_IMAGE_NOT_RUNTIME_VERIFIED"}


def _native_main(argv: list[str]) -> int:
    parser = _Parser()
    modes = parser.add_subparsers(dest="mode", required=True)
    prepare = modes.add_parser("prepare-overlay")
    prepare.add_argument("--repository", required=True, type=Path)
    prepare.add_argument("--output", required=True, type=Path)
    publish = modes.add_parser("native-publish")
    publish.add_argument("--before", required=True, type=Path)
    for flag in ("project", "region", "service", "baseline-revision", "candidate-revision", "candidate-image", "candidate-run"):
        publish.add_argument("--" + flag, required=True)
    try:
        args = parser.parse_args(argv)
        if args.mode == "prepare-overlay":
            result = prepare_notification_overlay(args.repository, args.output)
        else:
            before = load_request(args.before)
            expected = {k: getattr(args, k) for k in _EXPECTED if k != "resource_version"}
            expected["resource_version"] = before["service"]["metadata"]["resourceVersion"]
            expected.update(candidate_sha=NOTIFICATION_SOURCE, candidate_run=args.candidate_run)
            result = publish_native(before, expected)
        print(_canonical(result))
    except (PlanError, a.PreflightError) as exc:
        print("Native operation stopped: " + str(exc), file=sys.stderr); return 1
    except (ValueError, TypeError, KeyError, OSError, AttributeError):
        print("Native operation stopped: INVALID_INPUT", file=sys.stderr); return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in {"prepare-overlay", "native-publish"}:
        return _native_main(argv)
    parser = _Parser(description="Review a closed local synthetic snapshot; emits hashes only.")
    parser.add_argument("--input", required=True, type=Path)
    try:
        args = parser.parse_args(argv)
        _candidate, review = build_plan(load_request(args.input))
        print(_canonical(review))
    except PlanError as exc:
        print("Review stopped: " + str(exc), file=sys.stderr)
        return 1
    except (ValueError, TypeError, KeyError, RecursionError, OSError):
        print("Review stopped: INVALID_INPUT", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
