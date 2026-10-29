"""Apply the existing Schwab runtime enabled declaration to the serving service."""
from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import os
import re
import subprocess
import uuid

from quant_platform_kit.common.live_continuity import build_live_continuity, runtime_target_fingerprint
from scripts import diagnose_pilot_metadata as metadata

SERVICE = "charles-schwab-quant-service"
PROJECT = "charlesschwabquant"
REGION = "us-central1"


def _runtime_spec(spec):
    spec = copy.deepcopy(spec)
    spec.pop("servingState", None)
    for container in spec["containers"]:
        container.pop("name", None)
        container["env"] = sorted(container.get("env", []), key=lambda item: item["name"])
    return spec


def _command(args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=180, check=False,
                            env={**os.environ, "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true",
                                 "CLOUDSDK_CORE_LOG_HTTP": "false", "CLOUDSDK_CORE_DISABLE_PROMPTS": "1"})
    if result.returncode:
        raise RuntimeError("cloud_command_failed")
    return result.stdout


def _enabled(raw):
    if not isinstance(raw, str):
        raise ValueError("enabled_unavailable")
    if raw.strip().lower() in {"true", "1", "yes", "on"}:
        return True
    if raw.strip().lower() in {"false", "0", "no", "off"}:
        return False
    raise ValueError("enabled_unavailable")


def _scheduler(service, *, activate, command):
    attempted = False
    try:
        jobs = json.loads(command(["gcloud", "scheduler", "jobs", "list", "--project=" + PROJECT,
                                   "--location=" + REGION, "--format=json", "--quiet"]))
        url = service.get("status", {}).get("url", "").rstrip("/")
        matches = [job for job in jobs if url and job.get("httpTarget", {}).get("uri") == url + "/run"]
        if len(matches) != 1:
            return "unknown", "none"
        job = matches[0]
        state = str(job.get("state", "")).upper()
        if state == "ENABLED":
            return "enabled", "none"
        if state != "PAUSED":
            return "unknown", "none"
        if not activate:
            return "paused", "none"
        name = job.get("name", "")
        if not re.fullmatch(r"projects/charlesschwabquant/locations/us-central1/jobs/[A-Za-z0-9_-]+", name):
            return "unknown", "none"
        try:
            attempted = True
            command(["gcloud", "scheduler", "jobs", "resume", name.rsplit("/", 1)[-1],
                     "--project=" + PROJECT, "--location=" + REGION, "--quiet"])
        except Exception:
            return "resume_unknown", "attempted"
        reread = json.loads(command(["gcloud", "scheduler", "jobs", "describe", name.rsplit("/", 1)[-1],
                                     "--project=" + PROJECT, "--location=" + REGION, "--format=json", "--quiet"]))
        return ("enabled" if reread.get("state") == "ENABLED" and reread.get("httpTarget", {}).get("uri") == url + "/run" else "resume_unknown"), "attempted"
    except Exception:
        return ("resume_unknown" if attempted else "unknown"), ("attempted" if attempted else "none")


def execute(environ, *, describe=metadata._describe, command=_command):
    mode = environ.get("RESUME_OPERATION")
    if mode not in {"inspect", "enable", "disable"}:
        raise ValueError("operation_invalid")
    request_id = environ.get("RUNTIME_SWITCH_REQUEST_ID", "")
    try:
        if str(uuid.UUID(request_id)) != request_id:
            raise ValueError
    except (ValueError, AttributeError):
        raise ValueError("request_id_invalid") from None
    run_id, attempt = environ.get("GITHUB_RUN_ID", ""), environ.get("GITHUB_RUN_ATTEMPT", "")
    if not run_id.isascii() or not run_id.isdigit() or attempt != "1":
        raise ValueError("run_identity_invalid")
    service, project, region, declared, declarations = metadata._expected(environ)
    if (service, project, region) != (SERVICE, PROJECT, REGION):
        raise ValueError("identity_mismatch")
    desired = _enabled(declarations.get("RUNTIME_TARGET_ENABLED"))
    if mode != "inspect" and desired != (mode == "enable"):
        raise ValueError("declaration_mismatch")
    before = describe("services", service, project, region)
    revision_name = metadata._serving(before, service)
    revision = describe("revisions", revision_name, project, region)
    env = metadata._revision_env(revision, revision_name, service)
    target = metadata._runtime_target(env, service)
    if any(target.get(field) != declared.get(field) for field in metadata._IDENTITY_FIELDS):
        raise ValueError("identity_mismatch")
    if mode == "enable" and target != declared:
        raise ValueError("target_mismatch")
    continuity = target.get("live_continuity")
    if continuity is None:
        continuity_state = "NONE"
    else:
        try:
            frozen = build_live_continuity(continuity)
            baseline_matches = frozen.baseline_target_sha256 == runtime_target_fingerprint(target)
            continuity_state = frozen.state if baseline_matches else "UNKNOWN"
        except (TypeError, ValueError):
            continuity_state = "UNKNOWN"
        if mode == "enable" and continuity_state == "UNKNOWN":
            raise ValueError("baseline_mismatch")
    entry = env.get("RUNTIME_TARGET_ENABLED")
    if not isinstance(entry, dict) or "value" not in entry or "valueFrom" in entry:
        raise ValueError("enabled_unavailable")
    actual = _enabled(entry["value"])
    desired_spec = before.get("spec", {}).get("template", {}).get("spec")
    spec = revision["spec"]
    if not isinstance(desired_spec, dict) or _runtime_spec(desired_spec) != _runtime_spec(spec):
        raise ValueError("template_differs")
    image = spec["containers"][0].get("image", "")
    if not re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("image_unavailable")
    confirmed = describe("services", service, project, region)
    if (confirmed.get("metadata", {}).get("resourceVersion") != before.get("metadata", {}).get("resourceVersion")
            or metadata._serving(confirmed, service) != revision_name):
        raise ValueError("serving_changed")
    changed = mode != "inspect" and actual != desired
    if changed:
        suffix = "switch-" + run_id
        candidate = SERVICE + "-" + suffix
        value = "true" if desired else "false"
        try:
            command(["gcloud", "run", "services", "update", SERVICE, "--project=" + PROJECT,
                     "--region=" + REGION, "--no-traffic", "--deploy-health-check", "--revision-suffix=" + suffix,
                     "--update-env-vars=RUNTIME_TARGET_ENABLED=" + value, "--quiet"])
        except Exception:
            raise RuntimeError("candidate_write_uncertain_do_not_retry") from None
        try:
            staged_service = describe("services", service, project, region)
            staged = describe("revisions", candidate, project, region)
        except Exception:
            raise RuntimeError("candidate_readback_unknown_do_not_retry") from None
        if metadata._serving(staged_service, service) != revision_name:
            raise ValueError("serving_changed_after_stage")
        expected_spec = copy.deepcopy(spec)
        for item in expected_spec["containers"][0]["env"]:
            if item.get("name") == "RUNTIME_TARGET_ENABLED":
                item["value"] = value
        if _runtime_spec(staged.get("spec", {})) != _runtime_spec(expected_spec):
            raise ValueError("candidate_configuration_mismatch")
        conditions = {row.get("type"): row.get("status") for row in staged.get("status", {}).get("conditions", [])}
        if conditions.get("Ready") != "True":
            raise ValueError("candidate_not_ready")
        fresh = describe("services", service, project, region)
        if (fresh.get("metadata", {}).get("resourceVersion") != staged_service.get("metadata", {}).get("resourceVersion")
                or metadata._serving(fresh, service) != revision_name):
            raise ValueError("service_changed_after_stage")
        try:
            command(["gcloud", "run", "services", "update-traffic", SERVICE, "--project=" + PROJECT,
                     "--region=" + REGION, "--to-revisions=" + candidate + "=100", "--quiet"])
        except Exception:
            raise RuntimeError("traffic_write_uncertain_do_not_retry") from None
        try:
            before = describe("services", service, project, region)
            adopted = describe("revisions", candidate, project, region)
            confirmed = describe("services", service, project, region)
        except Exception:
            raise RuntimeError("adoption_readback_unknown_do_not_retry") from None
        if (metadata._serving(before, service) != candidate or metadata._serving(confirmed, service) != candidate
                or _runtime_spec(adopted.get("spec", {})) != _runtime_spec(expected_spec)
                or before.get("metadata", {}).get("resourceVersion") != confirmed.get("metadata", {}).get("resourceVersion")):
            raise ValueError("adoption_readback_mismatch")
        revision_name = candidate
        actual = desired
    effective = actual and continuity_state in {"NONE", "ACTIVE_LKG", "ROLLBACK_LKG"}
    scheduler, scheduler_write = _scheduler(before, activate=(mode == "enable" and effective), command=command)
    status = ("observed" if mode == "inspect" else
              "protected" if mode == "enable" and not effective else
              "partial" if mode == "enable" and scheduler != "enabled" else "applied")
    return {"status": status, "operation": mode, "request_id": request_id, "run_id": run_id, "attempt": 1,
            "operator_state": "enabled" if actual else "disabled", "declaration_state": "enabled" if desired else "disabled",
            "continuity_state": continuity_state, "effective_state": "enabled" if effective else "disabled",
            "scheduler_state": scheduler, "scheduler_write": scheduler_write,
            "serving_revision_sha256": hashlib.sha256(revision_name.encode()).hexdigest(),
            "cloud_write": "attempted" if changed else "none", "natural_cycle": "not_observed"}


def main():
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = execute(os.environ)
    except Exception as exc:
        allowed = {"operation_invalid", "request_id_invalid", "run_identity_invalid", "identity_mismatch",
                   "enabled_unavailable", "declaration_mismatch", "target_mismatch", "baseline_mismatch",
                   "template_differs", "image_unavailable", "serving_changed", "serving_changed_after_stage",
                   "candidate_write_uncertain_do_not_retry", "candidate_readback_unknown_do_not_retry",
                   "candidate_configuration_mismatch", "candidate_not_ready", "service_changed_after_stage",
                   "traffic_write_uncertain_do_not_retry", "adoption_readback_unknown_do_not_retry", "adoption_readback_mismatch"}
        reason = str(exc) if str(exc) in allowed else "read_or_validation_failed"
        request_id = os.environ.get("RUNTIME_SWITCH_REQUEST_ID", "")
        try:
            request_id = str(uuid.UUID(request_id)) if str(uuid.UUID(request_id)) == request_id else None
        except (ValueError, AttributeError):
            request_id = None
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        mode = os.environ.get("RESUME_OPERATION")
        result = {"status": "stopped", "reason": reason, "operation": mode if mode in {"inspect", "enable", "disable"} else None, "request_id": request_id,
                  "run_id": run_id if run_id.isascii() and run_id.isdigit() else None,
                  "attempt": 1 if os.environ.get("GITHUB_RUN_ATTEMPT") == "1" else None}
    path = os.environ.get("RUNTIME_SWITCH_RESULT_PATH")
    if path:
        with open(path, "x", encoding="utf-8") as handle:
            json.dump(result, handle, sort_keys=True)
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] == "stopped" else 0


if __name__ == "__main__":
    raise SystemExit(main())
