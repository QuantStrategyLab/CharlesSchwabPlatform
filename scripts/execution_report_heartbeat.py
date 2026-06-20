#!/usr/bin/env python3
"""Verify that an expected runtime report was written recently."""

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
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_ACCEPT_STATUSES = {"ok", "skipped", "success", "completed", "no_action"}
DEFAULT_REJECT_STATUSES = {"error", "failed", "failure", "cancelled", "canceled", "timed_out"}
DEFAULT_ACCEPT_STAGES = {
    "DRY_RUN_COMPLETED",
    "FUNDING_BLOCKED",
    "NO_ACTION",
    "ORDERS_PLANNED",
    "PARTIAL_SUBMITTED",
    "RECONCILED",
    "SUBMITTED",
    "COMPLETED",
}
DEFAULT_REJECT_STAGES = {"ERROR", "EXECUTION_BLOCKED", "FAILED", "FAILURE"}


def _split_values(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in re.split(r"[,;\n]+", raw) if part.strip()]


def _env_bool(name: str, default: bool = False) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def _enabled_value(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return True


def _parse_schedule_day_of_month_field(raw: str) -> set[int] | None:
    text = str(raw or "").strip()
    if not text or text in {"*", "?"}:
        return None
    days: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            try:
                step = max(1, int(step_text))
            except ValueError:
                return None
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            try:
                start = int(start_text)
                end = int(end_text)
            except ValueError:
                return None
            if start > end:
                return None
            days.update(day for day in range(start, end + 1, step) if 1 <= day <= 31)
            continue
        try:
            day = int(part)
        except ValueError:
            return None
        if 1 <= day <= 31:
            days.add(day)
    return days or None


def _runtime_target_payload() -> dict[str, Any]:
    raw = (os.environ.get("RUNTIME_TARGET_JSON") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_target_enabled() -> bool:
    value: Any = os.environ.get("RUNTIME_TARGET_ENABLED")
    if value is None:
        value = _runtime_target_payload().get("runtime_target_enabled")
    return _enabled_value(value, default=True)


def _runtime_target_scheduler() -> dict[str, Any]:
    payload = _runtime_target_payload()
    scheduler = payload.get("scheduler") if isinstance(payload, dict) else None
    return scheduler if isinstance(scheduler, dict) else {}


def _heartbeat_skip_reason_for_schedule(since: dt.datetime, now: dt.datetime) -> str | None:
    scheduler = _runtime_target_scheduler()
    cron = str(scheduler.get("main_time") or "").strip()
    fields = cron.split()
    if len(fields) != 5:
        return None
    expected_days = _parse_schedule_day_of_month_field(fields[2])
    if not expected_days:
        return None
    timezone_name = str(scheduler.get("timezone") or "UTC").strip() or "UTC"
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        timezone = dt.timezone.utc
        timezone_name = "UTC"
    local_since_date = since.astimezone(timezone).date()
    local_now_date = now.astimezone(timezone).date()
    cursor = local_since_date
    while cursor <= local_now_date:
        if cursor.day in expected_days:
            return None
        cursor += dt.timedelta(days=1)
    day_text = ",".join(str(day) for day in sorted(expected_days))
    return (
        f"runtime scheduler main_time is not due today "
        f"({timezone_name} date_window={local_since_date.isoformat()}.."
        f"{local_now_date.isoformat()}; expected day(s)={day_text})"
    )


def _parse_timestamp(value: Any) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _month_segments(start: dt.datetime, end: dt.datetime) -> list[str]:
    months = []
    cursor = dt.datetime(start.year, start.month, 1, tzinfo=dt.timezone.utc)
    end_cursor = dt.datetime(end.year, end.month, 1, tzinfo=dt.timezone.utc)
    while cursor <= end_cursor:
        months.append(f"{cursor.year:04d}-{cursor.month:02d}")
        if cursor.month == 12:
            cursor = dt.datetime(cursor.year + 1, 1, 1, tzinfo=dt.timezone.utc)
        else:
            cursor = dt.datetime(cursor.year, cursor.month + 1, 1, tzinfo=dt.timezone.utc)
    return months


def _base_report_uris() -> list[str]:
    uris = _split_values(os.environ.get("RUNTIME_HEARTBEAT_GCS_URIS"))
    uris.extend(_split_values(os.environ.get("EXECUTION_REPORT_GCS_URI")))

    state_bucket = (os.environ.get("FIRSTRADE_GCS_STATE_BUCKET") or "").strip()
    if state_bucket:
        state_prefix = (os.environ.get("FIRSTRADE_STATE_PREFIX") or "firstrade-platform").strip("/")
        base = f"gs://{state_bucket}"
        if state_prefix:
            base = f"{base}/{state_prefix}"
        uris.append(f"{base}/strategy-runs")

    seen = set()
    unique = []
    for uri in uris:
        clean = uri.rstrip("/")
        if clean and clean not in seen:
            seen.add(clean)
            unique.append(clean)
    return unique


def _load_required_services() -> list[str]:
    services = []
    for name in (
        "RUNTIME_HEARTBEAT_REQUIRED_SERVICES",
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
                    runtime_target = target.get("runtime_target") or target.get("runtime_target_json")
                    if isinstance(runtime_target, str):
                        try:
                            runtime_target = json.loads(runtime_target)
                        except json.JSONDecodeError:
                            runtime_target = {}
                    for key in ("service", "service_name", "cloud_run_service"):
                        value = target.get(key) or (
                            runtime_target.get(key) if isinstance(runtime_target, dict) else None
                        )
                        if value:
                            services.extend(_split_values(str(value)))
                            break
        except json.JSONDecodeError:
            pass

    seen = set()
    unique = []
    for service in services:
        if service not in seen:
            seen.add(service)
            unique.append(service)
    return unique


def _report_globs(since: dt.datetime, now: dt.datetime) -> list[str]:
    explicit = _split_values(os.environ.get("RUNTIME_HEARTBEAT_GCS_GLOBS"))
    if explicit:
        return explicit
    globs = []
    platform = (os.environ.get("RUNTIME_HEARTBEAT_REPORT_PLATFORM") or "").strip("/")
    for base in _base_report_uris():
        if platform and not base.rstrip("/").endswith(f"/{platform}"):
            base = f"{base}/{platform}"
        for month in _month_segments(since, now):
            globs.append(f"{base}/**/{month}/*.json")
    return globs


def _run_gcloud(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=False)


def _list_gcs_objects(gcs_glob: str, *, project: str | None) -> list[dict[str, Any]]:
    command = ["gcloud", "storage", "ls", "--json", gcs_glob]
    if project:
        command.extend(["--project", project])
    result = _run_gcloud(command)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        if "matched no objects" in detail.lower() or "no urls matched" in detail.lower():
            return []
        raise RuntimeError(detail or f"gcloud storage ls failed for {gcs_glob}")
    if not result.stdout.strip():
        return []
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"gcloud storage ls returned invalid JSON for {gcs_glob}: {exc}") from exc
    return payload if isinstance(payload, list) else []


def _cat_gcs_json(uri: str, *, project: str | None) -> dict[str, Any] | None:
    clean_uri = uri.split("#", 1)[0]
    command = ["gcloud", "storage", "cat", clean_uri]
    if project:
        command.extend(["--project", project])
    result = _run_gcloud(command)
    if result.returncode != 0:
        print(f"Unable to read report {clean_uri}: {(result.stderr or result.stdout).strip()}", file=sys.stderr)
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"Report is not valid JSON: {clean_uri}", file=sys.stderr)
        return None
    return payload if isinstance(payload, dict) else None


def _object_uri(entry: dict[str, Any]) -> str:
    return str(entry.get("url") or "").split("#", 1)[0]


def _object_updated_at(entry: dict[str, Any]) -> dt.datetime | None:
    metadata = entry.get("metadata") or {}
    return _parse_timestamp(metadata.get("updated") or metadata.get("timeCreated"))


def _report_errors(payload: dict[str, Any]) -> list[Any]:
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        return errors
    error_summary = payload.get("error_summary")
    if isinstance(error_summary, dict):
        nested = error_summary.get("errors")
        if isinstance(nested, list) and nested:
            return nested
    if payload.get("error"):
        return [payload.get("error")]
    return []


def _report_status(payload: dict[str, Any]) -> tuple[str, str]:
    status = str(payload.get("status") or payload.get("summary", {}).get("status") or "").strip()
    stage = str(payload.get("stage") or payload.get("summary", {}).get("stage") or "").strip()
    return status, stage


def _payload_service_name(payload: dict[str, Any]) -> str:
    runtime_target = payload.get("runtime_target")
    service = payload.get("service_name")
    if not service and isinstance(runtime_target, dict):
        service = runtime_target.get("service_name")
    return str(service or "").strip()


def _payload_account_scope(payload: dict[str, Any]) -> str:
    for key in ("account_scope", "account_group", "account_region"):
        value = payload.get(key)
        if value:
            return str(value).strip()
    return ""


def _payload_matches(payload: dict[str, Any], required_services: list[str]) -> tuple[bool, str, str]:
    expected_platform = (os.environ.get("RUNTIME_HEARTBEAT_REPORT_PLATFORM") or "").strip().lower()
    if expected_platform:
        platform = str(payload.get("platform") or "").strip().lower()
        if platform != expected_platform:
            return False, "", f"platform={platform or '-'}"

    expected_scope = (os.environ.get("RUNTIME_HEARTBEAT_ACCOUNT_SCOPE") or "").strip().lower()
    if expected_scope:
        account_scope = _payload_account_scope(payload).lower()
        if account_scope != expected_scope:
            return False, "", f"account_scope={account_scope or '-'}"

    service_name = _payload_service_name(payload)
    if required_services and service_name not in required_services:
        return False, service_name, f"service_name={service_name or '-'}"
    return True, service_name, "matched filters"


def _is_accepted_report(payload: dict[str, Any]) -> tuple[bool, str]:
    accepted_statuses = {
        value.lower()
        for value in (_split_values(os.environ.get("RUNTIME_HEARTBEAT_ACCEPT_STATUSES")) or DEFAULT_ACCEPT_STATUSES)
    }
    reject_statuses = {
        value.lower()
        for value in (_split_values(os.environ.get("RUNTIME_HEARTBEAT_REJECT_STATUSES")) or DEFAULT_REJECT_STATUSES)
    }
    accepted_stages = {
        value.upper()
        for value in (_split_values(os.environ.get("RUNTIME_HEARTBEAT_ACCEPT_STAGES")) or DEFAULT_ACCEPT_STAGES)
    }
    reject_stages = {
        value.upper()
        for value in (_split_values(os.environ.get("RUNTIME_HEARTBEAT_REJECT_STAGES")) or DEFAULT_REJECT_STAGES)
    }
    allow_errors = _env_bool("RUNTIME_HEARTBEAT_ACCEPT_REPORTS_WITH_ERRORS", False)

    status, stage = _report_status(payload)
    status_key = status.lower()
    stage_key = stage.upper()
    errors = _report_errors(payload)
    if errors and not allow_errors:
        return False, f"errors={len(errors)} status={status or '-'} stage={stage or '-'}"
    if status_key in reject_statuses or stage_key in reject_stages:
        return False, f"rejected status={status or '-'} stage={stage or '-'}"
    if status_key and status_key in accepted_statuses:
        return True, f"status={status}"
    if stage_key and stage_key in accepted_stages:
        return True, f"stage={stage}"
    if not status_key and not stage_key and not errors and not _env_bool("RUNTIME_HEARTBEAT_REQUIRE_STATUS", False):
        return True, "report exists"
    return False, f"unaccepted status={status or '-'} stage={stage or '-'}"


def _send_telegram(message: str) -> bool:
    targets: list[tuple[str, str]] = []
    token = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TG_TOKEN")
    for chat_id in _split_values(os.environ.get("GLOBAL_TELEGRAM_CHAT_ID")):
        if token:
            targets.append((token, chat_id))
    unique_targets = list(dict.fromkeys(targets))
    if not unique_targets:
        print("No Telegram token/chat configured; unable to send heartbeat alert.", file=sys.stderr)
        return False
    base_url = "https://api.telegram.org"
    ok = True
    for token_value, chat_id in unique_targets:
        body = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
        request = urllib.request.Request(
            f"{base_url.rstrip('/')}/bot{token_value}/sendMessage",
            data=body,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                ok = ok and response.status < 400
        except Exception as exc:  # noqa: BLE001
            ok = False
            print(f"Telegram send failed: {type(exc).__name__}", file=sys.stderr)
    return ok


def main(now: dt.datetime | None = None) -> int:
    project = (
        os.environ.get("RUNTIME_HEARTBEAT_GCP_PROJECT_ID")
        or os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    name = os.environ.get("RUNTIME_HEARTBEAT_NAME") or os.environ.get("GITHUB_REPOSITORY") or "runtime"
    if not _runtime_target_enabled():
        print(f"Execution report heartbeat skipped for {name}: runtime target is disabled")
        return 0
    lookback_hours = float(os.environ.get("RUNTIME_HEARTBEAT_LOOKBACK_HOURS") or "36")
    max_reports = int(os.environ.get("RUNTIME_HEARTBEAT_MAX_REPORTS_TO_READ") or "20")
    fail_workflow = _env_bool("RUNTIME_HEARTBEAT_FAIL_WORKFLOW_ON_ALERT", True)
    required_services = _load_required_services()

    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    now = now.astimezone(dt.timezone.utc)
    since = now - dt.timedelta(hours=lookback_hours)
    schedule_skip_reason = _heartbeat_skip_reason_for_schedule(since, now)
    if schedule_skip_reason:
        print(f"Execution report heartbeat skipped for {name}: {schedule_skip_reason}")
        return 0

    globs = _report_globs(since, now)
    if not globs:
        raise SystemExit("No heartbeat GCS report URI configured")

    objects: dict[str, tuple[str, dt.datetime]] = {}
    list_errors = []
    for gcs_glob in globs:
        try:
            for entry in _list_gcs_objects(gcs_glob, project=project):
                uri = _object_uri(entry)
                updated = _object_updated_at(entry)
                if uri and updated and updated >= since:
                    objects[uri] = (uri, updated)
        except RuntimeError as exc:
            list_errors.append(f"{gcs_glob}: {exc}")

    sorted_objects = sorted(objects.values(), key=lambda item: item[1], reverse=True)
    accepted = []
    accepted_by_service: dict[str, tuple[str, dt.datetime, str]] = {}
    inspected = []
    for uri, updated in sorted_objects[:max_reports]:
        payload = _cat_gcs_json(uri, project=project)
        if payload is None:
            inspected.append(f"- {updated.isoformat()} {uri} unreadable")
            continue
        matches, service_name, filter_reason = _payload_matches(payload, required_services)
        if not matches:
            inspected.append(f"- {updated.isoformat()} {uri} skipped {filter_reason}")
            continue
        ok, reason = _is_accepted_report(payload)
        inspected.append(f"- {updated.isoformat()} {uri} {reason}")
        if ok:
            if required_services:
                accepted_by_service[service_name] = (uri, updated, reason)
            else:
                accepted.append((uri, updated, reason))

    if required_services:
        missing = [service for service in required_services if service not in accepted_by_service]
        if not missing:
            details = ", ".join(
                f"{service}@{accepted_by_service[service][1].isoformat()}" for service in required_services
            )
            print(f"Execution report heartbeat OK for {name}: {details}")
            return 0
    if accepted:
        uri, updated, reason = accepted[0]
        print(
            f"Execution report heartbeat OK for {name}: {reason}, updated={updated.isoformat()}, uri={uri}"
        )
        return 0

    issues = []
    if list_errors:
        issues.extend(f"list failed: {item}" for item in list_errors[:3])
    if not sorted_objects:
        issues.append(f"no report object updated in the last {lookback_hours:g} hours")
    elif required_services:
        missing = [service for service in required_services if service not in accepted_by_service]
        issues.append(f"missing acceptable report for service(s): {', '.join(missing)}")
    else:
        issues.append(f"no acceptable report among {min(len(sorted_objects), max_reports)} recent report object(s)")

    run_url = ""
    if os.environ.get("GITHUB_SERVER_URL") and os.environ.get("GITHUB_REPOSITORY") and os.environ.get("GITHUB_RUN_ID"):
        run_url = (
            f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}"
            f"/actions/runs/{os.environ['GITHUB_RUN_ID']}"
        )
    message_lines = [
        f"[Execution Report Heartbeat] {name}",
        f"Lookback: {lookback_hours:g} hours",
        "Issues:",
        *[f"- {issue}" for issue in issues],
    ]
    if inspected:
        message_lines.extend(["Recent reports:", *inspected[:max_reports]])
    if run_url:
        message_lines.append(f"Workflow: {run_url}")
    message = "\n".join(message_lines)
    print(message)
    _send_telegram(message[:3900])
    return 1 if fail_workflow else 0


if __name__ == "__main__":
    raise SystemExit(main())
