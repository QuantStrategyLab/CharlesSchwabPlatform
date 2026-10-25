"""Read one existing Schwab serving declaration without broker or state writes.

The output attests deployment metadata only, never spendable cash, an effective
risk policy, breaker recovery, or permission to trade. All raw configuration
stays in memory; exceptions and low-entropy financial values are not logged.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


SCHEMA = "schwab.pilot-metadata-readback.v1"
_CLOUD_SOURCE = Path(__file__).resolve().parents[1] / ".github/workflows/env-sync-drift-check.yml"
_CLOUD_KEYS = ("GCP_PROJECT_ID", "GCP_WORKLOAD_IDENTITY_PROVIDER", "GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT")
_IDENTITY_FIELDS = (
    "service_name", "account_scope", "account_selector", "deployment_selector",
    "strategy_profile", "platform_id",
)
_STATES = {"ACTIVE_LKG", "ROLLBACK_LKG", "RECONCILE_ONLY", "PAUSED"}
_NAME = re.compile(r"[a-z][a-z0-9-]{0,62}\Z")
_REASONS = {
    "invalid_json", "invalid_declaration", "invalid_source_identity",
    "selected_service_not_unique", "invalid_runtime_target", "single_account_required",
    "metadata_read_failed", "invalid_metadata", "service_identity_mismatch",
    "serving_revision_not_unique", "invalid_traffic", "revision_identity_mismatch",
    "single_container_required", "invalid_environment", "runtime_target_secret_reference",
    "runtime_target_unavailable", "runtime_target_alias_conflict", "serving_changed",
    "runtime_identity_mismatch",
}


class _Unavailable(ValueError):
    """Internal fixed reason, never an upstream exception message."""


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _Unavailable("duplicate_json_key")
        result[key] = value
    return result


def _json(raw: str) -> Any:
    def invalid_constant(_value: str) -> None:
        raise _Unavailable("invalid_json")

    try:
        return json.loads(raw, object_pairs_hook=_object_pairs, parse_constant=invalid_constant)
    except (ValueError, TypeError):
        raise _Unavailable("invalid_json") from None


def _same(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _Unavailable("invalid_declaration")
    return value


def _expected(environ: Mapping[str, str]) -> tuple[str, str, str, dict[str, Any], dict[str, str | None]]:
    from scripts.build_cloud_run_env_sync_plan import (
        _first_non_empty,
        _load_target_entries,
        _resolve_runtime_target,
        _target_env_value,
        _target_field,
    )

    env = dict(environ)
    service = _text(env.get("CLOUD_RUN_SERVICE"))
    project = _text(env.get("GCP_PROJECT_ID"))
    if not _NAME.fullmatch(service) or not _NAME.fullmatch(project):
        raise _Unavailable("invalid_source_identity")
    for name in ("CLOUD_RUN_SERVICE_TARGETS_JSON", "RUNTIME_TARGET_JSON"):
        if env.get(name, "").strip():
            _json(env[name])
    entries, defaults, per_service = _load_target_entries(env)
    selected = [entry for entry in entries if _first_non_empty(
        _target_field(entry, defaults, "service"),
        _target_field(entry, defaults, "service_name"),
        _target_field(entry, defaults, "cloud_run_service"),
    ) == service]
    if len(selected) != 1:
        raise _Unavailable("selected_service_not_unique")
    entry = selected[0]
    # Validate embedded JSON strings before reusing the original pure parser.
    for source in (entry, defaults, entry.get("env", {}), defaults.get("env", {})):
        if isinstance(source, Mapping):
            for name in ("runtime_target", "runtime_target_json"):
                if isinstance(source.get(name), str):
                    _json(source[name])
    target = _resolve_runtime_target(entry, defaults, env, per_service)
    target.setdefault("service_name", service)
    _validate_target(target, service)
    region = _text(_first_non_empty(
        _target_field(entry, defaults, "region"),
        _target_field(entry, defaults, "cloud_run_region"),
        _target_field(entry, defaults, "location"),
        target.get("region"), target.get("cloud_run_region"), target.get("location"),
        env.get("CLOUD_RUN_REGION"),
    ))
    if not _NAME.fullmatch(region):
        raise _Unavailable("invalid_source_identity")
    declared = {}
    for name in ("RUNTIME_TARGET_ENABLED", "SCHWAB_DRY_RUN_ONLY", "SCHWAB_CASH_ONLY_EXECUTION", "CASH_ONLY_EXECUTION"):
        declared[name] = _target_env_value(
            entry, defaults, env, name, per_service_mode=per_service,
            allow_shared_fallback=False,
        )
    if declared["SCHWAB_DRY_RUN_ONLY"] is None and type(target.get("dry_run_only")) is bool:
        declared["SCHWAB_DRY_RUN_ONLY"] = str(target["dry_run_only"]).lower()
    return service, project, region, target, declared


def _validate_target(target: Any, service: str) -> None:
    if not isinstance(target, dict):
        raise _Unavailable("invalid_runtime_target")
    for field in _IDENTITY_FIELDS:
        if field == "account_selector":
            selectors = target.get(field)
            if not isinstance(selectors, list) or len(selectors) != 1:
                raise _Unavailable("single_account_required")
            _text(selectors[0])
        else:
            _text(target.get(field))
    if target["service_name"] != service or target["platform_id"] != "schwab":
        raise _Unavailable("invalid_runtime_target")


def _mask(value: str) -> None:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        escaped = value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::add-mask::{escaped}", flush=True)


def bootstrap_cloud_source() -> int:
    """Reuse only the existing drift workflow's fixed cloud identity metadata."""
    try:
        if os.environ.get("GITHUB_ACTIONS") != "true":
            raise _Unavailable("bootstrap_source_unavailable")
        if _CLOUD_SOURCE.is_symlink() or _CLOUD_SOURCE.stat().st_size > 131072:
            raise _Unavailable("bootstrap_source_unavailable")
        lines = _CLOUD_SOURCE.read_text(encoding="utf-8").splitlines()
        starts = [index for index, line in enumerate(lines) if line == "env:"]
        if len(starts) != 1:
            raise _Unavailable("bootstrap_source_unavailable")
        values = {}
        for line in lines[starts[0] + 1:]:
            if line and not line.startswith(" "):
                break
            match = re.fullmatch(r"  ([A-Z0-9_]+):(?: (.*))?", line)
            if match and match[1] in _CLOUD_KEYS:
                if match[1] in values:
                    raise _Unavailable("bootstrap_source_unavailable")
                values[match[1]] = match[2]
        if set(values) != set(_CLOUD_KEYS) or not _NAME.fullmatch(values["GCP_PROJECT_ID"]):
            raise _Unavailable("bootstrap_source_unavailable")
        if not re.fullmatch(r"projects/[0-9]+/locations/global/workloadIdentityPools/[a-z0-9-]+/providers/[a-z0-9-]+", values["GCP_WORKLOAD_IDENTITY_PROVIDER"]):
            raise _Unavailable("bootstrap_source_unavailable")
        if not re.fullmatch(r"[a-z0-9-]+@" + re.escape(values["GCP_PROJECT_ID"]) + r"\.iam\.gserviceaccount\.com", values["GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT"]):
            raise _Unavailable("bootstrap_source_unavailable")
        for value in values.values():
            _mask(value)
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
            output.writelines(f"{key}={values[key]}\n" for key in _CLOUD_KEYS)
        print('{"cloud_metadata_source_ready":true}')
        return 0
    except Exception:
        print('{"cloud_metadata_source_ready":false,"reason":"bootstrap_source_unavailable"}')
        return 1


def _mask_target(target: Mapping[str, Any]) -> None:
    for field in ("service_name", "account_scope", "deployment_selector"):
        if isinstance(target.get(field), str) and target[field] not in {"live", "paper", "US", "HK", "CN", "SG"}:
            _mask(target[field])
    for selector in target.get("account_selector", ()):
        if isinstance(selector, str):
            _mask(selector)
    binding = (target.get("runtime_risk_limits") or {}).get("binding", {}) if isinstance(target.get("runtime_risk_limits"), dict) else {}
    if isinstance(binding, dict) and isinstance(binding.get("account_hash"), str):
        _mask(binding["account_hash"])


def _describe(resource: str, name: str, project: str, region: str) -> dict[str, Any]:
    # The only external calls in this module are metadata describes, once each.
    result = subprocess.run(
        ["gcloud", "run", resource, "describe", name, f"--project={project}", f"--region={region}", "--format=json", "--quiet"],
        capture_output=True, text=True, check=False, timeout=30, stdin=subprocess.DEVNULL,
        env={**os.environ, "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true", "CLOUDSDK_CORE_LOG_HTTP": "false", "CLOUDSDK_CORE_DISABLE_PROMPTS": "1"},
    )
    if result.returncode:
        raise _Unavailable("metadata_read_failed")
    payload = _json(result.stdout)
    if not isinstance(payload, dict):
        raise _Unavailable("invalid_metadata")
    return payload


def _serving(payload: Mapping[str, Any], service: str) -> str:
    if (payload.get("metadata") or {}).get("name") != service:
        raise _Unavailable("service_identity_mismatch")
    traffic = (payload.get("status") or {}).get("traffic")
    if not isinstance(traffic, list) or not traffic:
        raise _Unavailable("serving_revision_not_unique")
    positive = []
    total = 0
    for item in traffic:
        if not isinstance(item, dict) or type(item.get("percent", 0)) is not int:
            raise _Unavailable("invalid_traffic")
        percent = item.get("percent", 0)
        if not 0 <= percent <= 100:
            raise _Unavailable("invalid_traffic")
        total += percent
        if percent:
            positive.append(item)
    if total != 100 or len(positive) != 1 or positive[0]["percent"] != 100:
        raise _Unavailable("serving_revision_not_unique")
    revision = _text(positive[0].get("revisionName"))
    if not _NAME.fullmatch(revision) or not revision.startswith(service + "-"):
        raise _Unavailable("revision_identity_mismatch")
    return revision


def _revision_env(payload: Mapping[str, Any], revision: str, service: str) -> dict[str, dict[str, Any]]:
    metadata = payload.get("metadata") or {}
    if metadata.get("name") != revision or (metadata.get("labels") or {}).get("serving.knative.dev/service") != service:
        raise _Unavailable("revision_identity_mismatch")
    containers = (payload.get("spec") or {}).get("containers")
    if not isinstance(containers, list) or len(containers) != 1 or not isinstance(containers[0], dict):
        raise _Unavailable("single_container_required")
    entries = containers[0].get("env", [])
    if not isinstance(entries, list):
        raise _Unavailable("invalid_environment")
    env = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str) or not entry["name"] or entry["name"] in env:
            raise _Unavailable("invalid_environment")
        if ("value" in entry) == ("valueFrom" in entry):
            raise _Unavailable("invalid_environment")
        env[entry["name"]] = entry
    return env


def _plain(env: Mapping[str, dict[str, Any]], name: str) -> tuple[str, Any]:
    if name not in env:
        return "absent", None
    if "valueFrom" in env[name]:
        return "secret_reference", None
    value = env[name]["value"]
    if not isinstance(value, str):
        return "invalid", None
    return ("explicit", value) if value.strip() else ("blank", None)


def _runtime_target(env: Mapping[str, dict[str, Any]], service: str) -> dict[str, Any]:
    declarations = [_plain(env, name) for name in ("RUNTIME_TARGET_JSON", "QSL_RUNTIME_TARGET_JSON")]
    if any(state == "secret_reference" for state, _ in declarations):
        raise _Unavailable("runtime_target_secret_reference")
    values = [value for state, value in declarations if state == "explicit"]
    if not values:
        raise _Unavailable("runtime_target_unavailable")
    if len(values) == 2 and not _same(_json(values[0]), _json(values[1])):
        raise _Unavailable("runtime_target_alias_conflict")
    target = _json(values[0])
    _validate_target(target, service)
    return target


def _boolean(env: Mapping[str, dict[str, Any]], name: str, expected: str | None) -> dict[str, Any]:
    state, raw = _plain(env, name)
    value = None
    if state == "explicit":
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            value = True
        elif normalized in {"0", "false", "no", "off"}:
            value = False
        else:
            state = "invalid"
    expected_value = None if expected is None else _boolean({name: {"value": expected}}, name, None)["value"]
    return {"status": state, "value": value, "matches_expected": None if expected_value is None or value is None else expected_value == value}


def _declarations(target: dict[str, Any], expected: dict[str, Any], env: dict[str, Any], declared: dict[str, str | None]) -> dict[str, Any]:
    flags = {name: _boolean(env, name, value) for name, value in declared.items()}
    mode = target.get("execution_mode")
    flags["execution_mode"] = {"status": "explicit" if mode in {"live", "paper"} else "invalid", "live": mode == "live", "paper": mode == "paper", "matches_expected": mode == expected.get("execution_mode")}
    continuity = target.get("live_continuity")
    state = continuity.get("state") if isinstance(continuity, dict) else None
    flags["live_continuity"] = {"status": "absent" if continuity is None else "explicit" if state in _STATES else "invalid", "state": state if state in _STATES else None, "matches_expected": _same(continuity, expected.get("live_continuity"))}
    release = target.get("strategy_release")
    release_present = isinstance(release, dict) and bool(release)
    flags["strategy_release"] = {"status": "absent" if release is None else "explicit" if release_present else "invalid", "present": release_present, "matches_expected": None if release is None or expected.get("strategy_release") is None else _same(release, expected["strategy_release"])}
    risk = target.get("runtime_risk_limits")
    binding = risk.get("binding") if isinstance(risk, dict) else None
    expected_risk = expected.get("runtime_risk_limits")
    risk_present = isinstance(risk, dict) and bool(risk)
    flags["runtime_risk_limits"] = {"status": "absent" if risk is None else "explicit" if risk_present else "invalid", "present": risk_present, "binding_present": isinstance(binding, dict) and bool(binding), "matches_expected": None if risk is None or expected_risk is None else _same(risk, expected_risk)}
    return flags


def diagnose_metadata(
    environ: Mapping[str, str], *, describe: Callable[..., dict[str, Any]] = _describe,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": SCHEMA, "status": "unavailable", "reason": "source_unavailable",
        "serving_stable": None, "serving_revision_sha256": None,
        "identity_matches": {}, "declarations": {},
        "broker_requested": False, "reports_read": False, "cloud_write_attempted": False,
        "notifications_sent": False, "account_policy_qualified": False,
        "spendable_cash_verified": False, "execution_authorized": False,
        "runtime_consumption_verified": False, "breaker_reset": False,
    }
    try:
        # Suppress helper/import diagnostics: only this fixed projection escapes.
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            service, project, region, expected, declared = _expected(environ)
        _mask_target(expected)
        before = describe("services", service, project, region)
        revision = _serving(before, service)
        _mask(revision)
        result["serving_revision_sha256"] = hashlib.sha256(revision.encode()).hexdigest()
        payload = describe("revisions", revision, project, region)
        env = _revision_env(payload, revision, service)
        target = _runtime_target(env, service)
        _mask_target(target)
        result["identity_matches"] = {field: target[field] == expected[field] for field in _IDENTITY_FIELDS}
        result["declarations"] = _declarations(target, expected, env, declared)
        after = describe("services", service, project, region)
        result["serving_stable"] = _serving(after, service) == revision
        if not result["serving_stable"]:
            raise _Unavailable("serving_changed")
        if not all(result["identity_matches"].values()):
            raise _Unavailable("runtime_identity_mismatch")
        result.update(status="verified_metadata", reason="observed")
    except _Unavailable as exc:
        result["reason"] = str(exc) if str(exc) in _REASONS else "metadata_unavailable"
    except (subprocess.TimeoutExpired, TimeoutError):
        result["reason"] = "metadata_timeout"
    except Exception:
        result["reason"] = "metadata_unavailable"
    return result


def main() -> int:
    if sys.argv[1:] == ["--bootstrap-cloud-source"]:
        return bootstrap_cloud_source()
    if len(sys.argv) != 1:
        result = {"schema_version": SCHEMA, "status": "unavailable", "reason": "arguments_not_supported"}
    else:
        result = diagnose_metadata(os.environ)
    encoded = json.dumps(result, sort_keys=True, allow_nan=False)
    output = os.environ.get("PILOT_METADATA_RESULT_PATH")
    if output:
        try:
            descriptor = os.open(Path(output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(encoded + "\n")
        except OSError:
            print(json.dumps({"schema_version": SCHEMA, "status": "unavailable", "reason": "result_write_failed"}))
            return 1
    print(encoded)
    return 0 if result["status"] == "verified_metadata" else 1


if __name__ == "__main__":
    raise SystemExit(main())
