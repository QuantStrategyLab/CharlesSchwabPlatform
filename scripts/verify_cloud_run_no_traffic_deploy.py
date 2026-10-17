#!/usr/bin/env python3
"""Read back a no-traffic deploy, or compare explicit local revision projections."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path


SERVICE_FORMAT = (
    "json(status.traffic,spec.template.spec.serviceAccountName,"
    "spec.template.spec.containerConcurrency,spec.template.spec.timeoutSeconds,"
    "spec.template.spec.containers[].resources,spec.template.spec.containers[].env[].name,"
    "spec.template.spec.containers[].env[].valueFrom.secretKeyRef)"
)
IAM_FORMAT = "json(bindings[].role,bindings[].members,bindings[].condition)"
SCHEDULER_FORMAT = "json(name,state,schedule,timeZone,httpTarget.uri,httpTarget.oidcToken)"
REVISION_FORMAT = "json(metadata.name,metadata.labels,spec.containers[].image)"
CASH_CURRENCY_FORMAT = (
    'json(spec.template.spec.containers[].env.always().filter("name=SCHWAB_CASH_CURRENCY").map().extract(value))'
)
CASH_REVISION_FORMAT = (
    'json(spec.containers[].env.always().filter("name=SCHWAB_CASH_CURRENCY").map().extract(value))'
)


def _run_json(command: list[str]) -> object:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError("read-only gcloud readback command failed")
    try:
        return json.loads(result.stdout or "null")
    except json.JSONDecodeError as exc:
        raise RuntimeError("gcloud readback returned invalid JSON") from exc


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _cash_values(value: object) -> list[object]:
    """Read only the explicitly projected non-secret cash-currency values."""
    if value is None:
        return []
    if isinstance(value, list):
        return [item for child in value for item in _cash_values(child)]
    if isinstance(value, dict):
        return [item for child in value.values() for item in _cash_values(child)]
    return [value]


def _cash_currency(args: argparse.Namespace, *, revision: bool = False) -> str | None:
    if not getattr(args, "cash_currency", None):
        return None
    if revision:
        payload = _run_json([
            "gcloud", "run", "revisions", "describe", args.revision_name,
            f"--project={args.project}", f"--region={args.region}",
            f"--format={CASH_REVISION_FORMAT}",
        ])
    else:
        payload = _run_json([
            "gcloud", "run", "services", "describe", args.service,
            f"--project={args.project}", f"--region={args.region}",
            f"--format={CASH_CURRENCY_FORMAT}",
        ])
    values = _cash_values(payload)
    if not values:
        return None
    if len(values) != 1 or not isinstance(values[0], str):
        raise RuntimeError("cash currency environment projection is ambiguous")
    return values[0]


def _configuration_projection(spec: object, *, allow_cash_currency: bool) -> object:
    if not allow_cash_currency or not isinstance(spec, dict):
        return spec
    # SERVICE_FORMAT returns only names and secret references, never plaintext
    # environment values. Remove the single explicitly allowed env name before
    # hashing so an absent-to-USD addition does not mask any other config drift.
    projected = json.loads(_canonical(spec))
    template = projected.get("template")
    template_spec = template.get("spec") if isinstance(template, dict) else None
    containers = template_spec.get("containers") if isinstance(template_spec, dict) else None
    if isinstance(containers, list):
        cash_entries = [
            entry
            for container in containers if isinstance(container, dict)
            for entry in (container.get("env") if isinstance(container.get("env"), list) else [])
            if isinstance(entry, dict) and entry.get("name") == "SCHWAB_CASH_CURRENCY"
        ]
        if len(cash_entries) > 1:
            raise RuntimeError("cash currency environment entry is duplicated")
        if cash_entries and "valueFrom" in cash_entries[0]:
            raise RuntimeError("cash currency environment entry must not use a secret reference")
        for container in containers:
            if not isinstance(container, dict) or not isinstance(container.get("env"), list):
                continue
            container["env"] = [
                entry for entry in container["env"]
                if not (isinstance(entry, dict) and entry.get("name") == "SCHWAB_CASH_CURRENCY")
            ]
    return projected


def _active_traffic(traffic: object) -> list[dict[str, object]]:
    if not isinstance(traffic, list):
        raise RuntimeError("Cloud Run traffic readback returned a non-list payload")
    active: list[dict[str, object]] = []
    for entry in traffic:
        if not isinstance(entry, dict):
            raise RuntimeError("Cloud Run traffic readback contained a non-object entry")
        try:
            percent = int(entry.get("percent", 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Cloud Run traffic readback contained an invalid percent") from exc
        if percent > 0:
            active.append({"revisionName": entry.get("revisionName"), "percent": percent})
    return sorted(active, key=_canonical)


def _snapshot(args: argparse.Namespace) -> dict[str, object]:
    service = _run_json([
        "gcloud", "run", "services", "describe", args.service,
        f"--project={args.project}", f"--region={args.region}", f"--format={SERVICE_FORMAT}",
    ])
    if not isinstance(service, dict):
        raise RuntimeError("Cloud Run service readback returned a non-object payload")
    iam = _run_json([
        "gcloud", "run", "services", "get-iam-policy", args.service,
        f"--project={args.project}", f"--region={args.region}", f"--format={IAM_FORMAT}",
    ])
    scheduler = _run_json([
        "gcloud", "scheduler", "jobs", "list", f"--project={args.project}",
        f"--location={args.scheduler_location}", f"--format={SCHEDULER_FORMAT}",
    ])
    status = service.get("status") if isinstance(service.get("status"), dict) else {}
    # Keep only digests in the on-runner baseline.  Service-account identities,
    # endpoint URIs, and secret-reference names are needed for comparison but
    # must not be persisted or printed by this verification helper.
    cash_currency = _cash_currency(args)
    if getattr(args, "cash_currency", None) and cash_currency not in (None, args.cash_currency):
        raise RuntimeError("cash currency baseline does not match the approved declaration")
    return {
        # A no-traffic revision is allowed to appear as a zero-percent status
        # entry.  Compare only effective traffic, otherwise a correct deploy
        # would fail its own readback merely because the new revision exists.
        "traffic": _digest(_active_traffic(status.get("traffic"))),
        "configuration": _digest(_configuration_projection(
            service.get("spec"), allow_cash_currency=bool(getattr(args, "cash_currency", None))
        )),
        **({"cash_currency": cash_currency} if getattr(args, "cash_currency", None) else {}),
        "iam": _digest(iam),
        "scheduler": _digest(scheduler),
    }


def _created_revision(args: argparse.Namespace) -> dict[str, object]:
    result = subprocess.run([
        "gcloud", "run", "services", "describe", args.service,
        f"--project={args.project}", f"--region={args.region}",
        "--format=value(status.latestCreatedRevisionName)",
    ], text=True, capture_output=True, check=False)
    revision_name = result.stdout.strip() if result.returncode == 0 else ""
    if not revision_name:
        raise RuntimeError("Cloud Run service did not report a created revision")
    revision = _run_json([
        "gcloud", "run", "revisions", "describe", revision_name,
        f"--project={args.project}", f"--region={args.region}", f"--format={REVISION_FORMAT}",
    ])
    if not isinstance(revision, dict):
        raise RuntimeError("Cloud Run revision readback returned a non-object payload")
    return revision


def _verify(args: argparse.Namespace) -> None:
    try:
        before = json.loads(args.before.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("deployment baseline is unreadable") from exc
    after = _snapshot(args)
    if not isinstance(before, dict):
        raise RuntimeError("deployment baseline is malformed")
    for key in ("traffic", "scheduler", "iam", "configuration"):
        if _canonical(before.get(key)) != _canonical(after.get(key)):
            raise RuntimeError(f"{key} changed during no-traffic deployment")

    revision = _created_revision(args)
    metadata = revision.get("metadata") if isinstance(revision.get("metadata"), dict) else {}
    labels = metadata.get("labels") if isinstance(metadata.get("labels"), dict) else {}
    containers = revision.get("spec", {}).get("containers", []) if isinstance(revision.get("spec"), dict) else []
    image = containers[0].get("image", "") if containers and isinstance(containers[0], dict) else ""
    if labels.get("commit-sha") != args.expected_sha:
        raise RuntimeError("created revision commit SHA does not match expected SHA")
    if f"@{args.expected_image_digest}" not in str(image):
        raise RuntimeError("created revision image digest does not match the pushed image")
    if getattr(args, "cash_currency", None):
        before_currency = before.get("cash_currency")
        if before_currency not in (None, args.cash_currency):
            raise RuntimeError("deployment baseline cash currency is not an allowed value")
        # Re-read only the explicitly named, non-secret value from the service
        # and created revision. Neither full env values nor the projection are
        # written into the baseline or emitted in diagnostics.
        revision_name = metadata.get("name")
        if not isinstance(revision_name, str) or not revision_name:
            raise RuntimeError("created revision name is missing")
        args.revision_name = revision_name
        if after.get("cash_currency") != args.cash_currency or _cash_currency(args, revision=True) != args.cash_currency:
            raise RuntimeError("cash currency declaration did not reach the created revision")
    message = (
        "Verified no-traffic deployment: commit SHA and image digest match; "
        "traffic, scheduler, IAM, and configuration digests are unchanged."
    )
    if getattr(args, "cash_currency", None):
        message += " The explicitly approved cash currency matches."
    print(message)


class PreflightError(RuntimeError):
    """A fixed, non-sensitive local preflight stop reason."""


LITERAL_ENV = frozenset({
    "RUNTIME_TARGET_JSON", "QSL_RUNTIME_TARGET_JSON", "STRATEGY_PROFILE",
    "RUNTIME_TARGET_ENABLED", "SCHWAB_DRY_RUN_ONLY", "SCHWAB_CASH_CURRENCY",
    "EXECUTION_REPORT_GCS_URI",
})
_ENV = object()
_PROBE = {
    "initialDelaySeconds": int, "timeoutSeconds": int, "periodSeconds": int,
    "failureThreshold": int, "successThreshold": int,
    "httpGet": {"path": str, "port": int},
    "tcpSocket": {"port": int}, "grpc": {"port": int, "service": str},
}
_CONTAINER = {
    "name": str, "image": str, "ports": [{"name": str, "containerPort": int}],
    "command": [str], "args": [str], "env": [_ENV],
    "resources": {"limits": {"cpu": str, "memory": str}, "cpuIdle": bool, "startupCpuBoost": bool},
    "startupProbe": _PROBE, "livenessProbe": _PROBE,
    "volumeMounts": [{"name": str, "mountPath": str, "subPath": str, "readOnly": bool}],
}
_RUNTIME = {
    "serviceAccountName": str, "ingress": str, "executionEnvironment": str,
    "containerConcurrency": int, "timeoutSeconds": int,
    "scaling": {"minInstanceCount": int, "maxInstanceCount": int},
    "network": {"network": str, "subnetwork": str, "vpcAccessConnector": str,
                "egress": str, "networkTags": [str]},
    "containers": [_CONTAINER],
    "volumes": [{"name": str,
                 "secret": {"secretName": str, "items": [{"key": str, "path": str}], "defaultMode": int},
                 "emptyDir": {"medium": str, "sizeLimit": str}}],
}
_PROJECTION = {
    "identity": {"project": str, "region": str, "service": str, "revision": str},
    "provenance": {"commit": str, "run": str},
    "traffic": [{"revisionName": str, "percent": int, "tag": str}],
    "runtime": _RUNTIME,
}


def _stop(reason: str) -> None:
    raise PreflightError(reason)


def _keys(value: object, allowed: object, required: object = ()) -> set[str]:
    if not isinstance(value, Mapping):
        _stop("INVALID_STRUCTURE")
    keys = set(value)
    if keys - set(allowed):
        _stop("UNKNOWN_FIELD")
    if set(required) - keys:
        _stop("MISSING_FIELD")
    return keys


def _secret_reference(value: object) -> None:
    _keys(value, {"name", "key"}, {"name", "key"})
    name, version = value["name"], value["key"]
    if (not isinstance(name, str) or not re.fullmatch(r"(?:projects/[a-z0-9-]+/secrets/)?[A-Za-z0-9_-]+", name)
            or not isinstance(version, str) or not re.fullmatch(r"latest|[1-9][0-9]*", version)):
        _stop("INVALID_SECRET_REFERENCE")


def _shape(value: object, schema: object) -> None:
    """Validate the entire metadata inventory without accessing env literal values.

    These are local projections, NOT full Cloud Run responses. A future reader
    must separately inventory metadata before fetching any allowlisted value.
    """
    if schema is _ENV:
        keys = _keys(value, {"name", "value", "valueFrom"}, {"name"})
        name = value["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            _stop("INVALID_ENV_NAME")
        if ("value" in keys) == ("valueFrom" in keys):
            _stop("AMBIGUOUS_ENV_KIND")
        if "value" in keys:
            if name not in LITERAL_ENV:
                _stop("UNKNOWN_ENV_LITERAL")
            # Deliberately do not get value here, even for an allowed name.
        else:
            source = value["valueFrom"]
            _keys(source, {"secretKeyRef"}, {"secretKeyRef"})
            _secret_reference(source["secretKeyRef"])
    elif isinstance(schema, dict):
        keys = _keys(value, schema)
        for key in sorted(keys):
            _shape(value[key], schema[key])
    elif isinstance(schema, list):
        if not isinstance(value, list):
            _stop("INVALID_STRUCTURE")
        for item in value:
            _shape(item, schema[0])
    elif type(value) is not schema:
        _stop("INVALID_STRUCTURE")


def _unique_json(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            _stop("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _strict_json(text: str) -> object:
    return json.loads(text, object_pairs_hook=_unique_json,
                      parse_constant=lambda _value: _stop("INVALID_JSON"))


def _load_projection(path: Path) -> object:
    try:
        return _strict_json(path.read_text(encoding="utf-8"))
    except PreflightError:
        raise
    except (OSError, ValueError, RecursionError):
        _stop("INVALID_PROJECTION_FILE")


def _inventory(projection: object) -> None:
    _shape(projection, _PROJECTION)
    _keys(projection, _PROJECTION, _PROJECTION)
    _keys(projection["identity"], _PROJECTION["identity"], _PROJECTION["identity"])
    _keys(projection["provenance"], _PROJECTION["provenance"], {"commit"})
    runtime = projection["runtime"]
    _keys(runtime, _RUNTIME, {
        "serviceAccountName", "ingress", "executionEnvironment", "scaling", "network",
        "containerConcurrency", "timeoutSeconds", "containers", "volumes",
    })
    _keys(runtime["scaling"], _RUNTIME["scaling"], _RUNTIME["scaling"])
    containers = runtime["containers"]
    if not containers:
        _stop("MISSING_CONTAINER")
    names = set()
    for container in containers:
        _keys(container, _CONTAINER, {"name", "image", "ports", "command", "args", "resources", "env", "volumeMounts"})
        if not container["name"] or container["name"] in names:
            _stop("DUPLICATE_CONTAINER")
        names.add(container["name"])
        _keys(container["resources"], _CONTAINER["resources"], {"limits"})
        _keys(container["resources"]["limits"], {"cpu", "memory"}, {"cpu", "memory"})
        for field, key in (("ports", "containerPort"), ("volumeMounts", "name")):
            identifiers = []
            for item in container[field]:
                _keys(item, _CONTAINER[field][0], {key})
                identifiers.append(item[key])
            if len(identifiers) != len(set(identifiers)):
                _stop("AMBIGUOUS_CONTAINER_CONFIGURATION")
        env_names = [entry["name"] for entry in container["env"]]
        if len(env_names) != len(set(env_names)):
            _stop("DUPLICATE_ENV_NAME")
    # Missing and ambiguous target declarations fail before any literal access.
    main_env = {entry["name"]: entry for entry in containers[0]["env"]}
    targets = {"RUNTIME_TARGET_JSON", "QSL_RUNTIME_TARGET_JSON"}
    if len(targets & main_env.keys()) != 1:
        _stop("AMBIGUOUS_OR_MISSING_TARGET")
    for name in (targets & main_env.keys()) | {"STRATEGY_PROFILE", "RUNTIME_TARGET_ENABLED", "SCHWAB_DRY_RUN_ONLY"}:
        if name not in main_env or "value" not in main_env[name]:
            _stop("REQUIRED_LITERAL_MISSING")
    if any(entry["name"] in targets for container in containers[1:] for entry in container["env"]):
        _stop("AMBIGUOUS_OR_MISSING_TARGET")
    volumes = runtime["volumes"]
    volume_names = []
    for volume in volumes:
        _keys(volume, _RUNTIME["volumes"][0], {"name"})
        if len({"secret", "emptyDir"} & volume.keys()) != 1 or not volume["name"]:
            _stop("AMBIGUOUS_VOLUME")
        volume_names.append(volume["name"])
        if "secret" in volume:
            secret = volume["secret"]
            _keys(secret, _RUNTIME["volumes"][0]["secret"], {"secretName", "items"})
            _secret_reference({"name": secret["secretName"], "key": "latest"})
            paths = []
            for item in secret["items"]:
                _keys(item, {"key", "path"}, {"key", "path"})
                _secret_reference({"name": secret["secretName"], "key": item["key"]})
                paths.append(item["path"])
            if len(paths) != len(set(paths)):
                _stop("AMBIGUOUS_VOLUME")
    if len(volume_names) != len(set(volume_names)):
        _stop("AMBIGUOUS_VOLUME")
    for container in containers:
        for mount in container["volumeMounts"]:
            _keys(mount, _CONTAINER["volumeMounts"][0], {"name", "mountPath"})
            if mount["name"] not in volume_names or not mount["mountPath"].startswith("/"):
                _stop("INVALID_VOLUME_MOUNT")


def _identity(projection: dict, args: argparse.Namespace, *, candidate: bool) -> None:
    identity = projection["identity"]
    expected_revision = args.candidate_revision if candidate else args.baseline_revision
    for key, expected in (("project", args.project), ("region", args.region),
                          ("service", args.service), ("revision", expected_revision)):
        if not isinstance(expected, str) or not re.fullmatch(r"[a-z][a-z0-9-]*", expected):
            _stop("INVALID_EXPECTED_IDENTITY")
        if key == "revision" and expected in {"latest", "latest-ready", "latest-created"}:
            _stop("INVALID_EXPECTED_IDENTITY")
        if identity[key] != expected:
            _stop("CANDIDATE_IDENTITY_MISMATCH" if candidate else "BASELINE_IDENTITY_MISMATCH")
    if args.baseline_revision == args.candidate_revision:
        _stop("AMBIGUOUS_REVISION_IDENTITY")
    provenance = projection["provenance"]
    if not re.fullmatch(r"[0-9a-f]{40}", provenance["commit"]):
        _stop("INVALID_COMMIT")
    if "run" in provenance and not re.fullmatch(r"[A-Za-z0-9_-]+", provenance["run"]):
        _stop("INVALID_RUN")
    if candidate and (provenance["commit"] != args.candidate_sha
                      or (args.candidate_run is not None and provenance.get("run") != args.candidate_run)):
        _stop("CANDIDATE_PROVENANCE_MISMATCH")
    containers = projection["runtime"]["containers"]
    for container in containers:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9./_-]*@sha256:[0-9a-f]{64}", container["image"]):
            _stop("INVALID_IMMUTABLE_IMAGE")
    if candidate and containers[0]["image"] != args.candidate_image:
        _stop("CANDIDATE_IMAGE_MISMATCH")


def _traffic(projection: dict, args: argparse.Namespace) -> list[dict]:
    rows = projection["traffic"]
    seen, tags = set(), set()
    total = 0
    baseline_percent = 0
    for row in rows:
        _keys(row, _PROJECTION["traffic"][0], {"revisionName", "percent"})
        revision, percent, tag = row["revisionName"], row["percent"], row.get("tag")
        if not re.fullmatch(r"[a-z][a-z0-9-]*", revision) or not 0 <= percent <= 100:
            _stop("INVALID_TRAFFIC")
        if tag is not None and not re.fullmatch(r"[a-z][a-z0-9-]*", tag):
            _stop("INVALID_TRAFFIC")
        if (revision, tag) in seen or (tag is not None and tag in tags):
            _stop("AMBIGUOUS_TRAFFIC")
        seen.add((revision, tag))
        if tag is not None:
            tags.add(tag)
        total += percent
        if revision == args.baseline_revision:
            baseline_percent += percent
        if revision == args.candidate_revision and percent:
            _stop("CANDIDATE_HAS_TRAFFIC")
    if total != 100 or baseline_percent <= 0:
        _stop("BASELINE_NOT_SERVING")
    return sorted(rows, key=_canonical)


def _runtime_values(projection: dict, args: argparse.Namespace) -> dict[str, str]:
    runtime = projection["runtime"]
    if (not re.fullmatch(r"[A-Za-z0-9._-]+@[A-Za-z0-9.-]+", runtime["serviceAccountName"])
            or runtime["ingress"] not in {"all", "internal", "internal-and-cloud-load-balancing"}
            or runtime["executionEnvironment"] not in {"gen1", "gen2"}
            or not 1 <= runtime["containerConcurrency"] <= 1000 or runtime["timeoutSeconds"] <= 0
            or runtime["scaling"]["minInstanceCount"] < 0
            or runtime["scaling"]["maxInstanceCount"] < max(1, runtime["scaling"]["minInstanceCount"])):
        _stop("INVALID_RUNTIME")
    startup = runtime["containers"][0].get("startupProbe", {})
    if (startup.get("httpGet") != {"path": "/health", "port": 8080}
            or "tcpSocket" in startup or "grpc" in startup):
        _stop("HTTP_STARTUP_GUARD_REQUIRED")
    for container in runtime["containers"]:
        for port in container["ports"]:
            if not 1 <= port["containerPort"] <= 65535:
                _stop("INVALID_PORT")
        for field in ("startupProbe", "livenessProbe"):
            if field not in container:
                continue
            probe = container[field]
            if len({"httpGet", "tcpSocket", "grpc"} & probe.keys()) != 1:
                _stop("INVALID_PROBE")
            for key in probe.keys() - {"httpGet", "tcpSocket", "grpc"}:
                if probe[key] < (0 if key == "initialDelaySeconds" else 1):
                    _stop("INVALID_PROBE")
        for entry in container["env"]:
            if "value" in entry and not isinstance(entry["value"], str):
                _stop("INVALID_ENV_LITERAL")
    env = {entry["name"]: entry["value"] for entry in runtime["containers"][0]["env"] if "value" in entry}
    raw_target = env.get("RUNTIME_TARGET_JSON", env.get("QSL_RUNTIME_TARGET_JSON"))
    try:
        target = _strict_json(raw_target)
    except (ValueError, RecursionError, PreflightError):
        _stop("INVALID_TARGET")
    if (not isinstance(target, dict) or target.get("service_name") != args.service
            or target.get("platform_id") != "schwab" or not isinstance(target.get("strategy_profile"), str)
            or not target["strategy_profile"] or target["strategy_profile"] != env["STRATEGY_PROFILE"]
            or not isinstance(target.get("execution_mode"), str) or target["execution_mode"] not in {"paper", "live"}
            or type(target.get("dry_run_only")) is not bool):
        _stop("INVALID_TARGET")
    # Same boolean vocabulary as deployed admission, with no missing-value default.
    def boolean(value: str) -> bool:
        if value.strip().lower() not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
            _stop("INVALID_BOOLEAN_LITERAL")
        return value.strip().lower() in {"1", "true", "yes", "on"}
    boolean(env["RUNTIME_TARGET_ENABLED"])
    if boolean(env["SCHWAB_DRY_RUN_ONLY"]) != target["dry_run_only"] or (target["dry_run_only"] and target["execution_mode"] != "paper"):
        _stop("TARGET_DRY_RUN_MISMATCH")
    return env


def _dependency_projection(repository: Path) -> object:
    """Read existing application declarations only; never infer installed pins."""
    try:
        project = tomllib.loads((repository / "pyproject.toml").read_text(encoding="utf-8"))
        config = tomllib.loads((repository / "qsl.toml").read_text(encoding="utf-8"))
        lock = tomllib.loads((repository / "uv.lock").read_text(encoding="utf-8"))
        dependencies = project["project"]["dependencies"]
        requires = config["qsl"]["requires"]
        if not isinstance(dependencies, list) or not all(isinstance(item, str) for item in dependencies) or not isinstance(requires, dict) or not lock:
            _stop("INVALID_DEPENDENCY_DECLARATIONS")
        return json.dumps({"dependencies": dependencies, "optional": project["project"].get("optional-dependencies", {}),
                           "python": project["project"].get("requires-python"), "dynamic": project["project"].get("dynamic", []),
                           "build": project.get("build-system", {}), "groups": project.get("dependency-groups", {}),
                           "requires": requires, "tool": project.get("tool", {}), "lock": lock},
                          sort_keys=True, allow_nan=False)
    except (OSError, ValueError, KeyError, TypeError):
        _stop("INVALID_DEPENDENCY_DECLARATIONS")


def _preflight_local(args: argparse.Namespace, baseline: object, candidate: object) -> None:
    """Compare caller-supplied projections. No cloud reader or subprocess fallback.

    Shape/name inventory for BOTH inputs precedes every literal value access.
    This checks declarations, not cloud authenticity, installed dependencies,
    strategy admission, continuity signatures, or application probe success.
    """
    _inventory(baseline)
    _inventory(candidate)
    _identity(baseline, args, candidate=False)
    _identity(candidate, args, candidate=True)
    before_traffic, after_traffic = _traffic(baseline, args), _traffic(candidate, args)
    # Only a newly appearing, untagged zero-percent candidate row may be added.
    addition = {"revisionName": args.candidate_revision, "percent": 0}
    if after_traffic != before_traffic and after_traffic != sorted(before_traffic + [addition], key=_canonical):
        _stop("TRAFFIC_OR_TAG_CHANGED")
    before_env = _runtime_values(baseline, args)
    after_env = _runtime_values(candidate, args)
    # No normalization of target data, risk bindings or frozen fingerprints.
    # All literal strings, reference names/versions and ordered lists compare exactly.
    before_runtime = baseline["runtime"]
    after_runtime = candidate["runtime"]
    comparable = dict(after_runtime)
    comparable["containers"] = [dict(container) for container in after_runtime["containers"]]
    comparable["containers"][0]["image"] = before_runtime["containers"][0]["image"]
    if _canonical(before_runtime) != _canonical(comparable):
        _stop("RUNTIME_CONFIGURATION_CHANGED")
    if _dependency_projection(args.baseline_repository) != _dependency_projection(args.candidate_repository):
        _stop("DEPENDENCY_DECLARATIONS_CHANGED")
    # Reuse the existing file-only live UES risk/release check, without importing
    # strategy_registry (which loads application adapters). Direct-script and
    # package imports use the same helper; neither path invokes its CLI.
    bytecode_setting = sys.dont_write_bytecode
    try:
        sys.dont_write_bytecode = True
        if __package__:
            from .verify_deployed_runtime_target_admission import verify_production_ues_pin
        else:
            from verify_deployed_runtime_target_admission import verify_production_ues_pin
    finally:
        sys.dont_write_bytecode = bytecode_setting
    for env, repository in ((before_env, args.baseline_repository), (after_env, args.candidate_repository)):
        service_json = {"spec": {"template": {"spec": {"containers": [{"env": [
            {"name": name, "value": value} for name, value in env.items()
        ]}]}}}}
        try:
            verify_production_ues_pin(service=args.service, service_json=service_json, repository=repository)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            _stop("LIVE_UES_PIN_NOT_ADMITTED")


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("capture", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--project", required=True)
        command.add_argument("--region", required=True)
        command.add_argument("--service", required=True)
        command.add_argument("--scheduler-location", required=True)
        command.add_argument("--cash-currency", choices=("USD",))
    subparsers.choices["capture"].add_argument("--output", required=True, type=Path)
    verify = subparsers.choices["verify"]
    verify.add_argument("--before", required=True, type=Path)
    verify.add_argument("--expected-sha", required=True)
    verify.add_argument("--expected-image-digest", required=True)
    local = subparsers.add_parser("preflight-local", help="Check local revision projections only; never reads Cloud Run")
    for flag in ("project", "region", "service", "baseline-revision", "candidate-revision", "candidate-image", "candidate-sha"):
        local.add_argument(f"--{flag}", required=True)
    local.add_argument("--candidate-run")
    for flag in ("baseline", "candidate", "baseline-repository", "candidate-repository"):
        local.add_argument(f"--{flag}", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.command == "capture":
            args.output.write_text(_canonical(_snapshot(args)), encoding="utf-8")
            print("Captured non-secret Cloud Run deployment baseline.")
        elif args.command == "verify":
            _verify(args)
        else:
            _preflight_local(args, _load_projection(args.baseline), _load_projection(args.candidate))
            print("Local projection preflight passed; cloud authenticity and installed dependencies remain unverified.")
    except PreflightError as exc:
        print(f"Local projection preflight stopped: {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"No-traffic deployment readback failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
