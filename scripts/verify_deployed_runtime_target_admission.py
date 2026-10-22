#!/usr/bin/env python3
"""Fail closed before a Cloud Run rollout reaches an unadmitted target.

Only non-sensitive target identity fields are read from Cloud Run.  This
checker never reads Secret Manager values and never mutates a service.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


class AdmissionError(ValueError):
    """A deployed runtime target is not safe to receive a new image."""


def _run(command: Sequence[str]) -> str:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        raise AdmissionError(f"[cloud_read] Cloud Run read failed (exit {result.returncode})")
    return result.stdout


def _describe_service(*, service: str, project: str, region: str) -> Mapping[str, Any]:
    payload = _run(["gcloud", "run", "services", "describe", service, f"--project={project}", f"--region={region}", "--format=json"])
    loaded = json.loads(payload)
    if not isinstance(loaded, Mapping):
        raise AdmissionError(f"{service}: Cloud Run describe returned a non-object payload")
    return loaded


def _container_env(service_json: Mapping[str, Any]) -> dict[str, str]:
    containers = service_json.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    if not isinstance(containers, list) or not containers:
        raise AdmissionError("Cloud Run service has no container configuration")
    entries = containers[0].get("env", [])
    if not isinstance(entries, list):
        raise AdmissionError("Cloud Run container environment is malformed")
    return {
        str(entry.get("name") or "").strip(): str(entry.get("value") or "").strip()
        for entry in entries
        if isinstance(entry, Mapping) and str(entry.get("name") or "").strip() and "value" in entry
    }


def _parse_bool(value: object, *, field: str, service: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise AdmissionError(f"{service}: {field} must be a boolean")


def verify_service(*, service: str, service_json: Mapping[str, Any]) -> dict[str, object]:
    """Validate one deployed service without printing account or secret data."""

    # The local projection preflight reuses only the file-based pin check below.
    # Importing this helper must not load strategies or application adapters.
    from strategy_registry import SCHWAB_PLATFORM, resolve_strategy_definition

    env = _container_env(service_json)
    raw_target = env.get("RUNTIME_TARGET_JSON") or env.get("QSL_RUNTIME_TARGET_JSON")
    if not raw_target:
        raise AdmissionError(f"{service}: RUNTIME_TARGET_JSON is required for image admission")
    try:
        target = json.loads(raw_target)
    except json.JSONDecodeError as exc:
        raise AdmissionError(f"{service}: RUNTIME_TARGET_JSON is invalid JSON") from exc
    if not isinstance(target, Mapping):
        raise AdmissionError(f"{service}: RUNTIME_TARGET_JSON must be an object")
    from scripts.runtime_target_continuity import verify_target_continuity

    try:
        verify_target_continuity(target)
    except ValueError as exc:
        raise AdmissionError(f"{service}: runtime target continuity validation failed") from exc
    target_service = str(target.get("service_name") or "").strip()
    if target_service and target_service != service:
        raise AdmissionError(f"{service}: runtime target service_name does not match the deployed service")

    raw_profile = str(target.get("strategy_profile") or "").strip()
    if not raw_profile:
        raise AdmissionError(f"{service}: runtime target strategy_profile is required")
    try:
        definition = resolve_strategy_definition(raw_profile, platform_id=SCHWAB_PLATFORM)
    except (TypeError, ValueError) as exc:
        raise AdmissionError(f"{service}: strategy profile is not admitted") from exc
    canonical_profile = definition.profile
    if str(env.get("STRATEGY_PROFILE") or "").strip() != canonical_profile:
        raise AdmissionError(f"{service}: STRATEGY_PROFILE does not match the admitted runtime target profile")

    execution_mode = str(target.get("execution_mode") or "").strip().lower()
    if execution_mode not in {"paper", "live"}:
        raise AdmissionError(f"{service}: execution_mode must be paper or live")
    if "dry_run_only" not in target:
        raise AdmissionError(f"{service}: runtime target dry_run_only is required")
    target_dry_run = _parse_bool(target["dry_run_only"], field="runtime target dry_run_only", service=service)
    configured_dry_run = env.get("SCHWAB_DRY_RUN_ONLY")
    if configured_dry_run is not None and _parse_bool(configured_dry_run, field="SCHWAB_DRY_RUN_ONLY", service=service) != target_dry_run:
        raise AdmissionError(f"{service}: SCHWAB_DRY_RUN_ONLY does not match runtime target dry_run_only")
    if target_dry_run and execution_mode != "paper":
        raise AdmissionError(f"{service}: a dry-run/shadow target must declare execution_mode=paper")

    enabled = _parse_bool(env.get("RUNTIME_TARGET_ENABLED", "true"), field="RUNTIME_TARGET_ENABLED", service=service)
    return {"service": service, "profile": canonical_profile, "execution_mode": execution_mode, "dry_run_only": target_dry_run, "enabled": enabled}


def verify_production_ues_pin(*, service: str, service_json: Mapping[str, Any], repository: Path) -> None:
    """Keep a live image on the UES revision admitted by its deployed risk binding."""

    env = _container_env(service_json)
    if not _parse_bool(env.get("RUNTIME_TARGET_ENABLED", "true"), field="RUNTIME_TARGET_ENABLED", service=service):
        return
    target = json.loads(env.get("RUNTIME_TARGET_JSON") or env.get("QSL_RUNTIME_TARGET_JSON") or "{}")
    if target.get("execution_mode") != "live":
        return
    release = target.get("strategy_release") or {}
    binding = (target.get("runtime_risk_limits") or {}).get("binding") or {}
    expected = release.get("strategy_revision")
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{40}", expected) or binding.get("ues_revision") != expected:
        raise AdmissionError(f"{service}: deployed live strategy release and risk binding lack one matching UES revision")

    project = tomllib.loads((repository / "pyproject.toml").read_text())
    dependencies = project.get("project", {}).get("dependencies", [])
    pins = [re.search(r"UsEquityStrategies\.git@([0-9a-f]{40})$", item, re.IGNORECASE) for item in dependencies if isinstance(item, str)]
    project_pins = [match.group(1) for match in pins if match]
    config_pin = tomllib.loads((repository / "qsl.toml").read_text()).get("qsl", {}).get("requires", {}).get("us_equity_strategies")
    packages = tomllib.loads((repository / "uv.lock").read_text()).get("package", [])
    locked = [item.get("source", {}).get("git", "") for item in packages if item.get("name") == "us-equity-strategies"]
    lock_pins = [re.search(r"[?&]rev=([0-9a-f]{40})#([0-9a-f]{40})$", item) for item in locked]
    locked_pins = [match.group(1) for match in lock_pins if match and match.group(1) == match.group(2)]
    if project_pins != [expected] or config_pin != expected or locked_pins != [expected]:
        raise AdmissionError(f"{service}: candidate UES pin differs from deployed live release and risk binding")


def _target(env: Mapping[str, str]) -> dict[str, Any]:
    try:
        target = json.loads(env.get("RUNTIME_TARGET_JSON") or env.get("QSL_RUNTIME_TARGET_JSON") or "null")
    except (TypeError, ValueError) as exc:
        raise AdmissionError("[target_json] Invalid runtime target JSON") from exc
    if not isinstance(target, dict):
        raise AdmissionError("[target_json] Runtime target must be an object")
    return target


def _env_entries(service_json: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    containers = service_json["spec"]["template"]["spec"]["containers"]
    if not isinstance(containers, list) or len(containers) != 1:
        raise AdmissionError("[environment] Recovery requires exactly one container")
    entries = containers[0].get("env", [])
    if not isinstance(entries, list):
        raise AdmissionError("[environment] Invalid environment entries")
    result = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise AdmissionError("[environment] Invalid environment entry")
        name = entry["name"]
        if not name or name in result or ("value" in entry) == ("valueFrom" in entry):
            raise AdmissionError("[environment] Duplicate or ambiguous environment entry")
        if "value" in entry and not isinstance(entry["value"], str):
            raise AdmissionError("[environment] Environment values must be strings")
        result[name] = copy.deepcopy(entry)
    return result


def _immutable_image(service_json: Mapping[str, Any]) -> str:
    image = service_json["spec"]["template"]["spec"]["containers"][0].get("image", "")
    if not isinstance(image, str) or not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
        raise AdmissionError("[immutable_image] Recovery requires a digest-pinned existing image")
    return image


def project_paused_env(
    *, service: str, service_json: Mapping[str, Any], plan: Mapping[str, Any], repository: Path,
) -> tuple[dict[str, Any], dict[str, object]]:
    from quant_platform_kit.common.live_continuity import build_live_continuity, runtime_target_fingerprint
    from scripts.reconcile_cloud_runtime import select_sync_target

    if service_json.get("metadata", {}).get("name") != service:
        raise AdmissionError("[service_identity] Cloud Run service identity does not match")
    entries = _env_entries(service_json)
    _immutable_image(service_json)
    old_target = _target(_container_env(service_json))
    try:
        old_continuity = build_live_continuity(old_target.get("live_continuity"))
    except (TypeError, ValueError) as exc:
        raise AdmissionError("[continuity_fields] Existing continuity fields are invalid or missing") from exc
    old_status = "valid" if old_continuity.baseline_target_sha256 == runtime_target_fingerprint(old_target) else "baseline_mismatch"
    old_without_continuity = copy.deepcopy(service_json)
    old_target_body = {key: value for key, value in old_target.items() if key != "live_continuity"}
    for entry in old_without_continuity["spec"]["template"]["spec"]["containers"][0]["env"]:
        if entry["name"] in {"RUNTIME_TARGET_JSON", "QSL_RUNTIME_TARGET_JSON"}:
            entry["value"] = json.dumps(old_target_body)
    old_result = verify_service(service=service, service_json=old_without_continuity)
    if old_target.get("service_name") != service or not old_result["enabled"] or old_result["execution_mode"] != "live":
        raise AdmissionError("[operation_scope] Recovery requires an enabled live target bound to this service")
    verify_production_ues_pin(service=service, service_json=service_json, repository=repository)
    try:
        selected = select_sync_target(plan, service)
    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        raise AdmissionError("[plan_selection] Expected exactly one matching plan target") from exc
    updates = selected.get("env")
    removals = selected.get("remove_env_vars", [])
    if not isinstance(updates, Mapping) or not isinstance(removals, list):
        raise AdmissionError("[environment] Invalid plan environment updates or removals")
    if any(
        not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
        or not isinstance(value, str) or any(separator in value for separator in ("|", "\n", "\r"))
        for name, value in updates.items()
    ):
        raise AdmissionError("[environment] Plan environment values must be strings")
    if any(not isinstance(name, str) or name in updates for name in removals):
        raise AdmissionError("[environment] Invalid or overlapping environment removals")
    for name in removals:
        if name in entries:
            raise AdmissionError("[unapproved_env_change] Recovery cannot remove existing environment or Secret references")
    for name, value in updates.items():
        if name != "RUNTIME_TARGET_JSON" and entries.get(name) != {"name": name, "value": value}:
            raise AdmissionError("[unapproved_env_change] Recovery cannot change other environment or Secret references")
        if name == "RUNTIME_TARGET_JSON" and name in entries and "valueFrom" in entries[name]:
            raise AdmissionError("[unapproved_env_change] Recovery cannot replace a Secret reference")
        entries[name] = {"name": name, "value": value}
    projected = copy.deepcopy(service_json)
    projected["spec"]["template"]["spec"]["containers"][0]["env"] = list(entries.values())
    candidate = _target(_container_env(projected))
    if runtime_target_fingerprint(candidate) != runtime_target_fingerprint(old_target):
        raise AdmissionError("[unapproved_target_change] Recovery cannot change release, risk, account, service or other target fields")
    if not isinstance(candidate.get("live_continuity"), Mapping) or candidate["live_continuity"].get("state") != "PAUSED":
        raise AdmissionError("[paused_required] Recovery requires explicit PAUSED continuity")
    result = verify_service(service=service, service_json=projected)
    verify_production_ues_pin(service=service, service_json=projected, repository=repository)
    return projected, result | {"old_continuity": old_status, "state": "PAUSED"}


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _spec_digest(service_json: Mapping[str, Any]) -> str:
    template = service_json["spec"]["template"]
    spec = copy.deepcopy(template["spec"])
    for container in spec["containers"]:
        container["env"] = sorted(container.get("env", []), key=lambda entry: entry["name"])
    return _digest({"spec": spec, "metadata": _protected_metadata(template.get("metadata", {}))})


def _protected_metadata(metadata: Mapping[str, Any]) -> dict[str, object]:
    from scripts.verify_cloud_run_no_traffic_deploy import NATIVE_OUTPUT_ANNOTATIONS, NATIVE_OUTPUT_LABELS

    return {
        field: {key: value for key, value in metadata.get(field, {}).items() if key not in outputs}
        for field, outputs in (("labels", NATIVE_OUTPUT_LABELS), ("annotations", NATIVE_OUTPUT_ANNOTATIONS))
    }


def _service_identity_digest(service_json: Mapping[str, Any]) -> str:
    metadata = service_json["metadata"]
    return _digest({"identity": {key: metadata.get(key) for key in ("name", "namespace", "uid")}, "configuration": _protected_metadata(metadata)})


def _require_reconciled(payload: Mapping[str, Any]) -> None:
    generation = payload.get("metadata", {}).get("generation")
    observed = payload.get("status", {}).get("observedGeneration")
    if type(generation) is not int or type(observed) is not int or generation != observed:
        raise AdmissionError("[reconciliation_pending] Cloud Run generation has not been observed")


def _traffic(service_json: Mapping[str, Any]) -> str:
    from scripts.verify_cloud_run_no_traffic_deploy import PreflightError, native_routes

    _require_reconciled(service_json)
    routes = []
    for kind in ("spec", "status"):
        entries = service_json.get(kind, {}).get("traffic")
        if not isinstance(entries, list) or not entries:
            raise AdmissionError("[traffic_readback] Existing traffic allocation is missing")
        resolved = copy.deepcopy(entries)
        for entry in resolved:
            if entry.get("latestRevision") is True:
                if kind == "spec":
                    entry["revisionName"] = service_json.get("status", {}).get("latestReadyRevisionName")
                entry["latestRevision"] = False
            if not entry.get("revisionName"):
                raise AdmissionError("[traffic_readback] Traffic revision is unresolved")
        try:
            allocations = native_routes(resolved, status=True)
        except PreflightError as exc:
            raise AdmissionError("[traffic_readback] Invalid traffic allocation") from exc
        routes.append([entry for entry in allocations if entry["percent"] > 0 or entry.get("tag")])
    if routes[0] != routes[1] or sum(entry["percent"] for entry in routes[1]) != 100:
        raise AdmissionError("[traffic_readback] Desired and observed traffic allocations differ")
    return _digest(routes[1])


def _require_env_only() -> None:
    if os.environ.get("APPROVE_ENV_SECRET_SYNC") != "true" or any(
        os.environ.get(name, "false") != "false" for name in (
            "APPROVE_IMAGE_DEPLOY", "APPROVE_TRAFFIC_SHIFT", "APPROVE_SCHEDULER_IAM_SYNC", "APPROVE_CLEANUP",
        )
    ):
        raise AdmissionError("[operation_scope] Recovery requires env-only authorization without image, traffic, IAM or cleanup")


def _paused_operation(args: argparse.Namespace, service_json: Mapping[str, Any]) -> dict[str, object]:
    _require_env_only()
    if not args.no_traffic or not args.baseline_file:
        raise AdmissionError("[operation_scope] Recovery requires --no-traffic and a baseline file")
    plan = json.loads(os.environ.get("SYNC_PLAN_JSON", "null"))
    repository = Path(__file__).resolve().parent.parent
    if args.verify_paused_revision:
        baseline = json.loads(args.baseline_file.read_text())
        revision = json.loads(_run([
            "gcloud", "run", "revisions", "describe", args.verify_paused_revision,
            f"--project={args.project}", f"--region={args.region}", "--format=json",
        ]))
        if revision.get("metadata", {}).get("name") != args.verify_paused_revision:
            raise AdmissionError("[revision_identity] Candidate revision identity does not match")
        _require_reconciled(revision)
        observed = {"spec": {"template": {"spec": revision["spec"], "metadata": revision.get("metadata", {})}}}
        if any(item.get("status") != "True" for item in revision.get("status", {}).get("conditions", []) if item.get("type") in {"Ready", "ContainerHealthy"}) or not any(
            item.get("type") == "Ready" and item.get("status") == "True" for item in revision.get("status", {}).get("conditions", [])
        ):
            raise AdmissionError("[revision_readiness] Candidate revision is not ready")
        if _spec_digest(observed) != baseline["projected_spec"] or _spec_digest(service_json) != baseline["projected_spec"]:
            raise AdmissionError("[revision_projection] Candidate or service configuration differs from admitted projection")
        if _service_identity_digest(service_json) != baseline["service_identity"]:
            raise AdmissionError("[service_identity] Protected service metadata changed")
        if _immutable_image(observed) != baseline["image"] or revision.get("status", {}).get("imageDigest", "").split("@")[-1] != baseline["image"].split("@")[-1]:
            raise AdmissionError("[image_readback] Candidate image digest changed or is unresolved")
        if _traffic(service_json) != baseline["traffic"]:
            raise AdmissionError("[traffic_readback] Existing traffic allocation changed")
        result = verify_service(service=args.service, service_json=observed)
        verify_production_ues_pin(service=args.service, service_json=observed, repository=repository)
        target = _target(_container_env(observed))
        if target.get("live_continuity", {}).get("state") != "PAUSED":
            raise AdmissionError("[paused_required] Candidate is not PAUSED")
        print(f"Verified PAUSED candidate: revision={args.verify_paused_revision}, target_sha256={_digest({key: value for key, value in target.items() if key != 'live_continuity'})}, image_digest={baseline['image'].split('@')[-1]}, risk_binding_matches=true, traffic_unchanged=true")
        return result
    projected, result = project_paused_env(service=args.service, service_json=service_json, plan=plan, repository=repository)
    baseline = {"current_spec": _spec_digest(service_json), "projected_spec": _spec_digest(projected), "traffic": _traffic(service_json), "image": _immutable_image(service_json), "service_identity": _service_identity_digest(service_json)}
    if args.check_before_apply:
        if json.loads(args.baseline_file.read_text()) != baseline:
            raise AdmissionError("[baseline_changed] Service, image, traffic or plan changed since admission")
    else:
        descriptor = os.open(args.baseline_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(baseline, stream)
    print(f"Verified PAUSED env projection: old_continuity={result['old_continuity']}, protected_target_unchanged=true")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--paused-env-only", action="store_true")
    parser.add_argument("--no-traffic", action="store_true")
    parser.add_argument("--baseline-file", type=Path)
    phase = parser.add_mutually_exclusive_group()
    phase.add_argument("--check-before-apply", action="store_true")
    phase.add_argument("--verify-paused-revision")
    args = parser.parse_args()
    try:
        service_json = _describe_service(service=args.service, project=args.project, region=args.region)
        if args.paused_env_only:
            result = _paused_operation(args, service_json)
        else:
            if args.check_before_apply or args.verify_paused_revision or args.no_traffic or args.baseline_file:
                raise AdmissionError("[operation_scope] Projection options require explicit --paused-env-only")
            result = verify_service(service=args.service, service_json=service_json)
            verify_production_ues_pin(service=args.service, service_json=service_json, repository=Path(__file__).resolve().parent.parent)
    except AdmissionError as exc:
        print(f"Deployed runtime target admission failed: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        print(f"Deployed runtime target admission failed: [invalid_input] {type(exc).__name__}; details withheld", file=sys.stderr)
        return 1
    print("Verified deployed runtime target admission: " f"service={result['service']}, profile={result['profile']}, " f"mode={result['execution_mode']}, dry_run_only={result['dry_run_only']}, enabled={result['enabled']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
