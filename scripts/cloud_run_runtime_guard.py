#!/usr/bin/env python3
"""Check Cloud Scheduler and Cloud Run logs, then notify Telegram on failures."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from typing import Any


ERROR_SEVERITIES = {"ERROR", "CRITICAL", "ALERT", "EMERGENCY"}
FAILURE_WORDS = (
    "DEADLINE_EXCEEDED",
    "INTERNAL_ERROR",
    "PERMISSION_DENIED",
    "UNAUTHENTICATED",
    "URL_ERROR",
    "URL_UNREACHABLE",
)


def _split_values(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in re.split(r"[,;\n]+", raw) if part.strip()]


def _env_bool(name: str, default: bool = False) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def _load_services() -> list[str]:
    services = []
    for name in (
        "RUNTIME_GUARD_CLOUD_RUN_SERVICES",
        "CLOUD_RUN_SERVICES",
        "CLOUD_RUN_SERVICE",
    ):
        services.extend(_split_values(os.environ.get(name)))

    raw_targets = (os.environ.get("CLOUD_RUN_SERVICE_TARGETS_JSON") or "").strip()
    if raw_targets:
        try:
            payload = json.loads(raw_targets)
            targets = payload.get("targets") if isinstance(payload, dict) else payload
            if isinstance(targets, list):
                for target in targets:
                    if not isinstance(target, dict):
                        continue
                    runtime_target = target.get("runtime_target") or target.get(
                        "runtime_target_json"
                    )
                    if isinstance(runtime_target, str):
                        try:
                            runtime_target = json.loads(runtime_target)
                        except json.JSONDecodeError:
                            runtime_target = {}
                    for key in ("service", "service_name", "cloud_run_service"):
                        value = target.get(key) or (
                            runtime_target.get(key)
                            if isinstance(runtime_target, dict)
                            else None
                        )
                        if value:
                            services.extend(_split_values(str(value)))
                            break
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"CLOUD_RUN_SERVICE_TARGETS_JSON is invalid: {exc}") from exc

    seen = set()
    unique = []
    for service in services:
        if service not in seen:
            seen.add(service)
            unique.append(service)
    return unique


def _run_gcloud_logging(project: str, log_filter: str, limit: int) -> list[dict[str, Any]]:
    command = [
        "gcloud",
        "logging",
        "read",
        log_filter,
        "--project",
        project,
        "--format=json",
        f"--limit={limit}",
    ]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(detail or "gcloud logging read failed")
    if not result.stdout.strip():
        return []
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"gcloud returned invalid JSON: {exc}") from exc
    return payload if isinstance(payload, list) else []


def _status(entry: dict[str, Any]) -> int | None:
    value = (entry.get("httpRequest") or {}).get("status")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _entry_text(entry: dict[str, Any]) -> str:
    chunks = []
    for key in ("textPayload", "message"):
        value = entry.get(key)
        if value:
            chunks.append(str(value))
    for key in ("jsonPayload", "protoPayload"):
        value = entry.get(key)
        if value:
            chunks.append(json.dumps(value, sort_keys=True))
    return " ".join(chunks)


def _is_failure(entry: dict[str, Any]) -> bool:
    severity = str(entry.get("severity") or "").upper()
    status = _status(entry)
    text = _entry_text(entry).upper()
    return (
        severity in ERROR_SEVERITIES
        or (status is not None and status >= 400)
        or any(word in text for word in FAILURE_WORDS)
    )


def _is_success(entry: dict[str, Any]) -> bool:
    status = _status(entry)
    return status is not None and 200 <= status < 400


def _labels(entry: dict[str, Any]) -> dict[str, Any]:
    resource = entry.get("resource") or {}
    labels = resource.get("labels") or {}
    return labels if isinstance(labels, dict) else {}


def _summarize(entry: dict[str, Any]) -> str:
    labels = _labels(entry)
    target = labels.get("service_name") or labels.get("job_id") or labels.get("job_name")
    timestamp = str(entry.get("timestamp") or "-")
    severity = str(entry.get("severity") or "-")
    status = _status(entry)
    status_text = f" status={status}" if status is not None else ""
    text = re.sub(r"\s+", " ", _entry_text(entry)).strip()
    if len(text) > 180:
        text = text[:177] + "..."
    suffix = f" {text}" if text else ""
    return f"- {timestamp} {target or '<unknown>'} severity={severity}{status_text}{suffix}"


def _send_telegram(message: str) -> bool:
    targets: list[tuple[str, str]] = []

    token = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TG_TOKEN")
    for chat_id in _split_values(os.environ.get("GLOBAL_TELEGRAM_CHAT_ID")):
        if token:
            targets.append((token, chat_id))

    unique_targets = list(dict.fromkeys(targets))
    if not unique_targets:
        print("No Telegram token/chat configured; unable to send runtime guard alert.", file=sys.stderr)
        return False

    ok = True
    base_url = "https://api.telegram.org"
    for token_value, chat_id in unique_targets:
        body = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
        request = urllib.request.Request(
            f"{base_url.rstrip('/')}/bot{token_value}/sendMessage",
            data=body,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                if response.status >= 400:
                    ok = False
                    print(f"Telegram returned HTTP {response.status}", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            ok = False
            print(f"Telegram send failed: {type(exc).__name__}", file=sys.stderr)
    return ok


def main() -> int:
    project = (
        os.environ.get("RUNTIME_GUARD_GCP_PROJECT_ID")
        or os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if not project:
        raise SystemExit("GCP_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required")

    name = os.environ.get("RUNTIME_GUARD_NAME") or os.environ.get("GITHUB_REPOSITORY") or "Cloud Run"
    lookback_minutes = int(os.environ.get("RUNTIME_GUARD_LOOKBACK_MINUTES") or "180")
    limit = int(os.environ.get("RUNTIME_GUARD_LOG_LIMIT") or "200")
    require_success = _env_bool("RUNTIME_GUARD_REQUIRE_SUCCESS", False)
    fail_workflow = _env_bool("RUNTIME_GUARD_FAIL_WORKFLOW_ON_ALERT", True)
    check_scheduler = _env_bool("RUNTIME_GUARD_CHECK_SCHEDULER", True)
    scheduler_pattern = os.environ.get("RUNTIME_GUARD_SCHEDULER_JOB_PATTERN") or ""

    since = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=lookback_minutes)
    ).replace(microsecond=0)
    since_text = since.isoformat().replace("+00:00", "Z")

    issues: list[str] = []
    details: list[str] = []
    success_count = 0

    try:
        services = _load_services()
    except RuntimeError as exc:
        services = []
        issues.append(f"service configuration error: {exc}")

    for service in services:
        log_filter = (
            'resource.type="cloud_run_revision" '
            f'AND resource.labels.service_name="{service}" '
            f'AND timestamp >= "{since_text}"'
        )
        try:
            entries = _run_gcloud_logging(project, log_filter, limit)
        except RuntimeError as exc:
            issues.append(f"Cloud Run log query failed for {service}: {exc}")
            continue
        failures = [entry for entry in entries if _is_failure(entry)]
        success_count += sum(1 for entry in entries if _is_success(entry))
        if failures:
            issues.append(f"{len(failures)} Cloud Run failure log(s) for {service}")
            details.extend(_summarize(entry) for entry in failures[:5])

    if services and require_success and success_count == 0:
        issues.append(
            f"no successful Cloud Run request found for {', '.join(services)} in the last {lookback_minutes} minutes"
        )

    if check_scheduler:
        log_filter = f'resource.type="cloud_scheduler_job" AND timestamp >= "{since_text}"'
        try:
            entries = _run_gcloud_logging(project, log_filter, limit)
            if scheduler_pattern:
                regex = re.compile(scheduler_pattern)
                entries = [
                    entry
                    for entry in entries
                    if regex.search(str(_labels(entry).get("job_id") or _labels(entry).get("job_name") or ""))
                ]
            failures = [entry for entry in entries if _is_failure(entry)]
            if failures:
                issues.append(f"{len(failures)} Cloud Scheduler failure log(s)")
                details.extend(_summarize(entry) for entry in failures[:5])
        except RuntimeError as exc:
            issues.append(f"Cloud Scheduler log query failed: {exc}")

    if not issues:
        service_text = ", ".join(services) if services else "<none configured>"
        print(
            f"Runtime guard OK for {name}: services={service_text}, lookback={lookback_minutes}m, successes={success_count}"
        )
        return 0

    run_url = ""
    if os.environ.get("GITHUB_SERVER_URL") and os.environ.get("GITHUB_REPOSITORY") and os.environ.get("GITHUB_RUN_ID"):
        run_url = (
            f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}"
            f"/actions/runs/{os.environ['GITHUB_RUN_ID']}"
        )
    message_lines = [
        f"[Runtime Guard] {name}",
        f"Project: {project}",
        f"Lookback: {lookback_minutes} minutes",
        "Issues:",
        *[f"- {issue}" for issue in issues],
    ]
    if details:
        message_lines.extend(["Details:", *details[:10]])
    if run_url:
        message_lines.append(f"Workflow: {run_url}")
    message = "\n".join(message_lines)
    print(message)
    _send_telegram(message[:3900])
    return 1 if fail_workflow else 0


if __name__ == "__main__":
    raise SystemExit(main())
