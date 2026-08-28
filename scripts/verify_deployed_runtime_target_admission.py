#!/usr/bin/env python3
"""Fail closed before a Cloud Run rollout reaches an unadmitted target.

Only non-sensitive target identity fields are read from Cloud Run.  This
checker never reads Secret Manager values and never mutates a service.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from strategy_registry import SCHWAB_PLATFORM, resolve_strategy_definition


class AdmissionError(ValueError):
    """A deployed runtime target is not safe to receive a new image."""


def _run(command: Sequence[str]) -> str:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise AdmissionError(detail or f"Command failed: {' '.join(command)}")
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--service", required=True)
    args = parser.parse_args()
    try:
        result = verify_service(service=args.service, service_json=_describe_service(service=args.service, project=args.project, region=args.region))
    except AdmissionError as exc:
        print(f"Deployed runtime target admission failed: {exc}", file=sys.stderr)
        return 1
    print("Verified deployed runtime target admission: " f"service={result['service']}, profile={result['profile']}, " f"mode={result['execution_mode']}, dry_run_only={result['dry_run_only']}, enabled={result['enabled']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
