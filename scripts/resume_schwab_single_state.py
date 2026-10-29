"""Existing WIF entry: inspect, then resume one Schwab service without new orders."""
from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import subprocess

from quant_platform_kit.common.live_continuity import build_live_continuity, runtime_target_fingerprint
from scripts import diagnose_pilot_metadata as metadata

SERVICE = "charles-schwab-quant-service"
PROJECT = "charlesschwabquant"
REGION = "us-central1"
REVISION = SERVICE + "-00805-buq"
IMAGE = "sha256:e5b96671086a3c3da8e8d2c88fd7a9b65a69b7d38d9aaa63a2e66da94170ba1f"


class TemplateDifference(ValueError):
    def __init__(self, desired, serving):
        super().__init__("template_differs")
        fields = ("containers", "containerConcurrency", "timeoutSeconds", "serviceAccountName", "volumes")
        self.comparisons = {name: "match" if desired.get(name) == serving.get(name) else "mismatch" for name in fields}
        self.comparisons["other_fields"] = "match" if {k: v for k, v in desired.items() if k not in fields} == {k: v for k, v in serving.items() if k not in fields} else "mismatch"
        left, right = desired.get("containers", []), serving.get("containers", [])
        if len(left) == len(right) == 1:
            names = ("image", "env", "resources", "ports", "startupProbe", "command", "args", "name")
            self.comparisons.update({"container_" + name: "match" if left[0].get(name) == right[0].get(name) else "mismatch" for name in names})
            self.comparisons["container_other_fields"] = "match" if {k: v for k, v in left[0].items() if k not in names} == {k: v for k, v in right[0].items() if k not in names} else "mismatch"


def _runtime_spec(spec):
    """Use the previously verified Cloud Run revision representation rules."""
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


def inspect(environ, *, describe=metadata._describe):
    service, project, region, declared, _ = metadata._expected(environ)
    if (service, project, region) != (SERVICE, PROJECT, REGION):
        raise ValueError("identity_mismatch")
    state = build_live_continuity(declared.get("live_continuity"))
    if state.state not in {"PAUSED", "ACTIVE_LKG"} or state.baseline_target_sha256 != runtime_target_fingerprint(declared):
        raise ValueError("declaration_invalid")
    before = describe("services", service, project, region)
    revision_name = metadata._serving(before, service)
    if revision_name != REVISION:
        raise ValueError("serving_changed")
    revision = describe("revisions", revision_name, project, region)
    env = metadata._revision_env(revision, revision_name, service)
    target = metadata._runtime_target(env, service)
    if "RUNTIME_TARGET_JSON" not in env or "QSL_RUNTIME_TARGET_JSON" in env:
        raise ValueError("target_alias_unsupported")
    paused = copy.deepcopy(declared)
    paused["live_continuity"]["state"] = "PAUSED"
    if target != paused or build_live_continuity(target["live_continuity"]).baseline_target_sha256 != runtime_target_fingerprint(target):
        raise ValueError("target_mismatch")
    desired = before.get("spec", {}).get("template", {}).get("spec")
    if not isinstance(desired, dict) or _runtime_spec(desired) != _runtime_spec(revision["spec"]):
        if not isinstance(desired, dict) or not isinstance(revision.get("spec"), dict):
            raise ValueError("template_differs")
        raise TemplateDifference(desired, revision["spec"])
    containers = revision["spec"]["containers"]
    if len(containers) != 1 or not containers[0]["image"].endswith("@" + IMAGE):
        raise ValueError("image_mismatch")
    after = describe("services", service, project, region)
    if before.get("metadata", {}).get("resourceVersion") != after.get("metadata", {}).get("resourceVersion") or metadata._serving(after, service) != revision_name:
        raise ValueError("serving_changed")
    return before, revision, target, state.state


def execute(environ, *, describe=metadata._describe, command=_command):
    before, revision, target, declared_state = inspect(environ, describe=describe)
    mode = environ.get("RESUME_OPERATION")
    if mode == "inspect":
        scheduler = "unknown"
        try:
            jobs = json.loads(command(["gcloud", "scheduler", "jobs", "list", "--project=" + PROJECT,
                                       "--location=" + REGION, "--format=json", "--quiet"]))
            service_url = before.get("status", {}).get("url", "").rstrip("/")
            matches = [job for job in jobs if service_url and
                       job.get("httpTarget", {}).get("uri") == service_url + "/run"]
            if len(matches) == 1:
                scheduler = str(matches[0].get("state", "UNKNOWN")).lower()
                if scheduler not in {"enabled", "paused", "disabled"}:
                    scheduler = "unknown"
        except Exception:
            pass
        return {"status": "ready_for_review", "template_comparison": "match",
                "continuity_state": "PAUSED", "declaration_state": declared_state, "scheduler_run": scheduler,
                "cloud_write": "none"}
    if mode != "apply" or declared_state != "ACTIVE_LKG":
        raise ValueError("active_declaration_required")
    run_id = environ.get("GITHUB_RUN_ID", "")
    if not run_id.isascii() or not run_id.isdigit():
        raise ValueError("run_id_invalid")
    target["live_continuity"]["state"] = "ACTIVE_LKG"
    value = json.dumps(target, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if "|" in value or "\n" in value:
        raise ValueError("target_encoding_unsupported")
    suffix = "resume-" + run_id
    candidate = SERVICE + "-" + suffix
    # Fail before writing if an external writer changed the observed service.
    fresh = describe("services", SERVICE, PROJECT, REGION)
    if fresh.get("metadata", {}).get("resourceVersion") != before.get("metadata", {}).get("resourceVersion"):
        raise ValueError("service_changed")
    try:
        command(["gcloud", "run", "services", "update", SERVICE, "--project=" + PROJECT,
                 "--region=" + REGION, "--no-traffic", "--deploy-health-check", "--revision-suffix=" + suffix,
                 "--update-env-vars=^|^RUNTIME_TARGET_JSON=" + value, "--quiet"])
    except Exception:
        raise RuntimeError("candidate_write_uncertain_do_not_retry") from None
    try:
        staged_service = describe("services", SERVICE, PROJECT, REGION)
        staged = describe("revisions", candidate, PROJECT, REGION)
    except Exception:
        raise RuntimeError("candidate_readback_unknown_do_not_retry") from None
    if metadata._serving(staged_service, SERVICE) != REVISION:
        raise ValueError("serving_changed_after_stage")
    expected_spec = copy.deepcopy(revision["spec"])
    for item in expected_spec["containers"][0]["env"]:
        if item.get("name") == "RUNTIME_TARGET_JSON":
            item["value"] = value
    if _runtime_spec(staged.get("spec", {})) != _runtime_spec(expected_spec):
        raise ValueError("candidate_configuration_mismatch")
    ready = {x.get("type"): x.get("status") for x in staged.get("status", {}).get("conditions", [])}
    if ready.get("Ready") != "True":
        raise ValueError("candidate_not_ready")
    fresh = describe("services", SERVICE, PROJECT, REGION)
    if (fresh.get("metadata", {}).get("resourceVersion") != staged_service.get("metadata", {}).get("resourceVersion")
            or metadata._serving(fresh, SERVICE) != REVISION):
        raise ValueError("service_changed_after_stage")
    try:
        command(["gcloud", "run", "services", "update-traffic", SERVICE, "--project=" + PROJECT,
                 "--region=" + REGION, "--to-revisions=" + candidate + "=100", "--quiet"])
    except Exception:
        raise RuntimeError("traffic_write_uncertain_do_not_retry") from None
    try:
        readback = describe("services", SERVICE, PROJECT, REGION)
        adopted = describe("revisions", candidate, PROJECT, REGION)
        confirmed = describe("services", SERVICE, PROJECT, REGION)
    except Exception:
        raise RuntimeError("adoption_readback_unknown_do_not_retry") from None
    if (metadata._serving(readback, SERVICE) != candidate or _runtime_spec(adopted.get("spec", {})) != _runtime_spec(expected_spec)
            or metadata._serving(confirmed, SERVICE) != candidate
            or readback.get("metadata", {}).get("resourceVersion") != confirmed.get("metadata", {}).get("resourceVersion")):
        raise ValueError("adoption_readback_mismatch")
    return {"status": "active_readback", "template_comparison": "match",
            "continuity_state": "ACTIVE_LKG", "declaration_state": declared_state, "cloud_write": "attempted",
            "broker_read": "none", "natural_cycle": "not_observed"}


def main():
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = execute(os.environ)
    except Exception as exc:
        allowed = {"identity_mismatch", "declaration_invalid", "serving_changed", "target_mismatch",
                   "template_differs", "image_mismatch", "active_declaration_required", "run_id_invalid",
                   "target_encoding_unsupported", "service_changed", "candidate_write_uncertain_do_not_retry",
                   "serving_changed_after_stage", "service_changed_after_stage", "target_alias_unsupported",
                   "candidate_configuration_mismatch", "candidate_not_ready",
                   "traffic_write_uncertain_do_not_retry", "adoption_readback_mismatch",
                   "candidate_readback_unknown_do_not_retry", "adoption_readback_unknown_do_not_retry"}
        reason = str(exc) if str(exc) in allowed else "read_or_validation_failed"
        result = {"status": "stopped", "reason": reason}
        if isinstance(exc, TemplateDifference):
            result["template_field_matches"] = exc.comparisons
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
