"""Run a heartbeat command with its existing serving digest binding in memory."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys

FIELDS = ("DIGEST_OPAQUE_ACCOUNT_UID", "DIGEST_TARGET_ID", "DIGEST_STORE_TARGET", "DIGEST_CANDIDATES_GCS_URI")
IDENTITY = ("platform_id", "strategy_profile", "service_name", "account_scope", "account_selector", "deployment_selector", "dry_run_only")
COMMANDS = {"heartbeat": "execution_report_heartbeat.py", "candidates": "emit_unified_digest_candidates.py"}


class BindingError(ValueError):
    """Only fixed public codes may leave the metadata boundary."""


def cloud_json(args, env):
    account = env.get("GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT", "")
    if not account:
        raise BindingError("digest_reader_account_missing")
    result = subprocess.run(["gcloud", *args, "--account=" + account, "--project=" + env["GCP_PROJECT_ID"],
                             "--region=" + env["CLOUD_RUN_REGION"], "--format=json", "--quiet"],
                            capture_output=True, text=True, timeout=60, check=False)
    if result.returncode or len(result.stdout) > 256 * 1024:
        raise BindingError("digest_metadata_unavailable")
    return json.loads(result.stdout)


def resolve(env, read=cloud_json):
    resolved = dict(env)
    if env.get("RUNTIME_TARGET_ENABLED") != "true" or env.get("DIGEST_TARGET_BINDINGS_JSON"):
        return resolved  # Existing multi-target/disabled contracts retain ownership.
    expected = json.loads(env.get("RUNTIME_TARGET_JSON") or "{}")
    if not isinstance(expected, dict) or any(k not in expected for k in IDENTITY):
        raise BindingError("digest_runtime_identity_missing")
    missing = [key for key in FIELDS if not env.get(key)]
    if missing:
        service = env.get("CLOUD_RUN_SERVICE", "")
        if not service or service != expected["service_name"]:
            raise BindingError("digest_runtime_identity_mismatch")
        metadata = read(["run", "services", "describe", service], env)
        if metadata.get("metadata", {}).get("name") != service:
            raise BindingError("digest_runtime_identity_mismatch")
        traffic = [row for row in metadata.get("status", {}).get("traffic", []) if row.get("percent", 0) > 0]
        if len(traffic) != 1 or type(traffic[0].get("percent")) is not int or traffic[0]["percent"] != 100:
            raise BindingError("digest_serving_revision_ambiguous")
        revision = traffic[0].get("revisionName", "")
        if not re.fullmatch(re.escape(service) + r"-[a-z0-9-]+", revision):
            raise BindingError("digest_serving_revision_ambiguous")
        payload = read(["run", "revisions", "describe", revision], env)
        if payload.get("metadata", {}).get("name") != revision:
            raise BindingError("digest_runtime_identity_mismatch")
        containers = payload.get("spec", {}).get("containers", [])
        if len(containers) != 1:
            raise BindingError("digest_metadata_invalid")
        values = {}
        for row in containers[0].get("env", []):
            name = row.get("name")
            if name in values:
                raise BindingError("digest_metadata_invalid")
            values[name] = row.get("value")  # Never access Secret Manager references.
        target = json.loads(values.get("RUNTIME_TARGET_JSON") or "{}")
        if (not isinstance(target, dict) or any(target.get(k) != expected[k] for k in IDENTITY)
                or values.get("RUNTIME_TARGET_ENABLED") != "true"):
            raise BindingError("digest_runtime_identity_mismatch")
        for key in FIELDS:
            value = values.get(key)
            if not isinstance(value, str) or not value or value != value.strip() or any(c in value for c in "\r\n\0"):
                raise BindingError("digest_identity_owner_input_required")
            if env.get(key) and env[key] != value:
                raise BindingError("digest_configured_identity_mismatch")
            resolved[key] = value
        # A traffic change invalidates this observation instead of adopting another target.
        after = read(["run", "services", "describe", service], env)
        if after.get("status", {}).get("traffic") != metadata.get("status", {}).get("traffic"):
            raise BindingError("digest_serving_revision_changed")
    if not resolved.get("DIGEST_NOTIFICATION_SCHEDULE_JSON"):
        scheduler = expected.get("scheduler") or {}
        if not scheduler.get("main_time") or not scheduler.get("timezone"):
            raise BindingError("digest_schedule_owner_input_required")
        resolved["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
            "main_time": scheduler["main_time"], "timezone": scheduler["timezone"]}})
    return resolved


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        return 2
    try:
        env = resolve(os.environ)
    except BindingError as error:
        print(json.dumps({"status": "blocked", "reason": str(error)}))
        return 1
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        print(json.dumps({"status": "blocked", "reason": "digest_metadata_invalid"}))
        return 1
    # Values are neither printed nor exported through GITHUB_ENV/output/artifacts.
    command = Path(__file__).with_name(COMMANDS[sys.argv[1]])
    return subprocess.run([sys.executable, str(command)], env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
